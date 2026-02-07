package importer

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/wesm/msgvault/internal/fileutil"
	"github.com/wesm/msgvault/internal/mbox"
	"github.com/wesm/msgvault/internal/mime"
	"github.com/wesm/msgvault/internal/store"
	"github.com/wesm/msgvault/internal/textutil"
)

type MboxImportOptions struct {
	// SourceType is the sources.source_type value (e.g. "mbox" or "hey").
	SourceType string

	// Identifier is the sources.identifier (e.g. "you@hey.com").
	Identifier string

	// Label, if non-empty, is applied to all imported messages.
	Label string

	// NoResume forces a fresh import even if an active sync run exists for the source.
	NoResume bool

	// CheckpointInterval controls how often (in messages) to persist progress.
	// If zero, a default of 200 is used.
	CheckpointInterval int

	// AttachmentsDir controls where attachments are written.
	// If empty, attachments are not written to disk (but messages are still imported).
	AttachmentsDir string

	// Logger is optional; defaults to slog.Default().
	Logger *slog.Logger
}

type MboxImportSummary struct {
	WasResumed     bool
	ResumedOffset  int64
	FinalOffset    int64
	Duration       time.Duration
	BytesProcessed int64

	MessagesProcessed int64
	MessagesAdded     int64
	MessagesSkipped   int64
	Errors            int64
}

type mboxCheckpoint struct {
	File   string `json:"file"`
	Offset int64  `json:"offset"`
}

// ImportMbox imports a single MBOX file into the msgvault database.
//
// This is intended for services like HEY.com that provide an export in MBOX
// format but do not expose IMAP/POP. The importer stores the raw MIME,
// parsed bodies, participants, recipients, and (optionally) attachments.
func ImportMbox(ctx context.Context, st *store.Store, mboxPath string, opts MboxImportOptions) (*MboxImportSummary, error) {
	if opts.SourceType == "" {
		opts.SourceType = "mbox"
	}
	if opts.Identifier == "" {
		return nil, fmt.Errorf("identifier is required")
	}
	if opts.CheckpointInterval <= 0 {
		opts.CheckpointInterval = 200
	}
	log := opts.Logger
	if log == nil {
		log = slog.Default()
	}

	start := time.Now()
	summary := &MboxImportSummary{}

	absPath, err := filepath.Abs(mboxPath)
	if err != nil {
		return nil, fmt.Errorf("abs path: %w", err)
	}

	// Ensure / get the source.
	src, err := st.GetOrCreateSource(opts.SourceType, opts.Identifier)
	if err != nil {
		return nil, fmt.Errorf("get/create source: %w", err)
	}

	// Create or resume the sync run for this source.
	var (
		syncID int64
		cp     store.Checkpoint
		offset int64
	)

	if !opts.NoResume {
		active, err := st.GetActiveSync(src.ID)
		if err != nil {
			return nil, fmt.Errorf("check active sync: %w", err)
		}
		if active != nil {
			syncID = active.ID
			cp.MessagesProcessed = active.MessagesProcessed
			cp.MessagesAdded = active.MessagesAdded
			cp.MessagesUpdated = active.MessagesUpdated
			cp.ErrorsCount = active.ErrorsCount
			if active.CursorBefore.Valid && active.CursorBefore.String != "" {
				var mcp mboxCheckpoint
				if err := json.Unmarshal([]byte(active.CursorBefore.String), &mcp); err == nil {
					if mcp.File == absPath && mcp.Offset > 0 {
						offset = mcp.Offset
						summary.WasResumed = true
						summary.ResumedOffset = offset
						log.Info("resuming mbox import", "file", absPath, "offset", offset, "processed", cp.MessagesProcessed)
					} else if mcp.File != "" && mcp.File != absPath {
						return nil, fmt.Errorf("active mbox import is for a different file (%q), not %q; rerun with --no-resume to start fresh", mcp.File, absPath)
					}
				}
			}
		}
	}

	if syncID == 0 {
		syncID, err = st.StartSync(src.ID, "import-mbox")
		if err != nil {
			return nil, fmt.Errorf("start sync: %w", err)
		}
	}

	// Save an initial checkpoint so the active sync always records which file it's importing,
	// even if the run is interrupted before the first periodic checkpoint.
	_ = saveMboxCheckpoint(st, syncID, absPath, offset, &cp)

	// Ensure label (once).
	var labelIDs []int64
	if opts.Label != "" {
		labelID, err := st.EnsureLabel(src.ID, opts.Label, opts.Label, "user")
		if err != nil {
			_ = st.FailSync(syncID, err.Error())
			return nil, fmt.Errorf("ensure label: %w", err)
		}
		labelIDs = []int64{labelID}
	}

	// Open file and (if resuming) seek.
	f, err := os.Open(absPath)
	if err != nil {
		_ = st.FailSync(syncID, err.Error())
		return nil, fmt.Errorf("open mbox: %w", err)
	}
	defer f.Close()

	if offset > 0 {
		if _, err := f.Seek(offset, io.SeekStart); err != nil {
			_ = st.FailSync(syncID, err.Error())
			return nil, fmt.Errorf("seek mbox: %w", err)
		}
	}

	r := mbox.NewReader(f)

	// If we resumed at a saved offset, the reader's logical Offset() should now be that.
	// However, if the offset lands in the middle of a line (corrupt checkpoint), the
	// reader will scan to the next separator.
	var lastCheckpointOffset int64 = offset

	for {
		if err := ctx.Err(); err != nil {
			summary.FinalOffset = lastCheckpointOffset
			summary.Duration = time.Since(start)
			// Record best-effort checkpoint before returning.
			_ = saveMboxCheckpoint(st, syncID, absPath, lastCheckpointOffset, &cp)
			return summary, nil
		}

		msg, err := r.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			cp.ErrorsCount++
			summary.Errors++
			log.Warn("mbox read error", "error", err)
			continue
		}

		cp.MessagesProcessed++
		summary.MessagesProcessed++
		summary.BytesProcessed += int64(len(msg.Raw))

		// Compute a stable ID for dedup.
		sum := sha256.Sum256(msg.Raw)
		// Use a filesystem-safe ID (used by default output naming in export-eml).
		// Avoid ":" since it is invalid on Windows and historically problematic on macOS.
		hashHex := hex.EncodeToString(sum[:])
		sourceMsgID := hashHex

		// Fast existence check to skip parsing / attachment work on re-runs.
		existing, err := st.MessageExistsBatch(src.ID, []string{sourceMsgID})
		if err != nil {
			cp.ErrorsCount++
			summary.Errors++
			log.Warn("existence check failed", "error", err)
			continue
		}
		if _, ok := existing[sourceMsgID]; ok {
			cp.MessagesUpdated++
			summary.MessagesSkipped++

			// Update checkpoint offset even when skipping so resumption progresses.
			lastCheckpointOffset = r.NextFromOffset()
			if cp.MessagesProcessed%int64(opts.CheckpointInterval) == 0 {
				_ = saveMboxCheckpoint(st, syncID, absPath, lastCheckpointOffset, &cp)
			}
			continue
		}

		if err := ingestRawEmail(ctx, st, src.ID, opts.Identifier, opts.AttachmentsDir, labelIDs, sourceMsgID, hashHex, msg, log); err != nil {
			cp.ErrorsCount++
			summary.Errors++
			log.Warn("failed to ingest message", "error", err)
		} else {
			cp.MessagesAdded++
			summary.MessagesAdded++
		}

		lastCheckpointOffset = r.NextFromOffset()

		if cp.MessagesProcessed%int64(opts.CheckpointInterval) == 0 {
			_ = saveMboxCheckpoint(st, syncID, absPath, lastCheckpointOffset, &cp)
		}
	}

	summary.FinalOffset = r.Offset()
	summary.Duration = time.Since(start)

	// Final checkpoint and mark sync complete.
	_ = saveMboxCheckpoint(st, syncID, absPath, r.Offset(), &cp)
	if err := st.CompleteSync(syncID, fmt.Sprintf("offset:%d", r.Offset())); err != nil {
		return summary, fmt.Errorf("complete sync: %w", err)
	}

	return summary, nil
}

func saveMboxCheckpoint(st *store.Store, syncID int64, file string, offset int64, cp *store.Checkpoint) error {
	b, _ := json.Marshal(mboxCheckpoint{File: file, Offset: offset})
	cp.PageToken = string(b)
	return st.UpdateSyncCheckpoint(syncID, cp)
}

func normalizeMessageID(id string) string {
	id = strings.TrimSpace(id)
	id = strings.Trim(id, "<>")
	return id
}

func threadKey(parsed *mime.Message, rawHash string) string {
	// Prefer References[0] (root), then In-Reply-To, then Message-ID.
	if len(parsed.References) > 0 {
		if root := normalizeMessageID(parsed.References[0]); root != "" {
			return root
		}
	}
	if irt := normalizeMessageID(parsed.InReplyTo); irt != "" {
		return irt
	}
	if mid := normalizeMessageID(parsed.MessageID); mid != "" {
		return mid
	}
	return rawHash
}

func parseFromLineDate(fromLine string) (time.Time, bool) {
	// "From sender@example.com Sat Feb  7 12:34:56 2026"
	fields := strings.Fields(fromLine)
	if len(fields) < 7 || fields[0] != "From" {
		return time.Time{}, false
	}
	// Join the remainder after the email address.
	dateStr := strings.Join(fields[2:], " ")

	// Common mbox "From " date layouts.
	layouts := []string{
		"Mon Jan 2 15:04:05 2006",
		"Mon Jan 2 15:04:05 -0700 2006",
	}
	for _, layout := range layouts {
		if t, err := time.Parse(layout, dateStr); err == nil {
			// Normalize to UTC for consistent storage.
			return t.UTC(), true
		}
	}
	return time.Time{}, false
}

func snippetFromBody(body string) string {
	body = strings.TrimSpace(body)
	if body == "" {
		return ""
	}
	line := textutil.FirstLine(body)
	return textutil.TruncateRunes(line, 200)
}

func joinEmails(addrs []mime.Address) string {
	if len(addrs) == 0 {
		return ""
	}
	emails := make([]string, 0, len(addrs))
	for _, a := range addrs {
		if a.Email != "" {
			emails = append(emails, a.Email)
		}
	}
	return strings.Join(emails, " ")
}

func storeRecipients(st *store.Store, messageID int64, recipientType string, addresses []mime.Address, participantMap map[string]int64) error {
	if len(addresses) == 0 {
		return nil
	}

	// Prefer non-empty display names when duplicates exist.
	idToName := make(map[int64]string)
	var orderedIDs []int64

	for _, addr := range addresses {
		if addr.Email == "" {
			continue
		}
		id, ok := participantMap[addr.Email]
		if !ok {
			continue
		}
		name := textutil.EnsureUTF8(addr.Name)
		if _, seen := idToName[id]; !seen {
			orderedIDs = append(orderedIDs, id)
			idToName[id] = name
			continue
		}
		if idToName[id] == "" && name != "" {
			idToName[id] = name
		}
	}

	displayNames := make([]string, len(orderedIDs))
	for i, id := range orderedIDs {
		displayNames[i] = idToName[id]
	}

	return st.ReplaceMessageRecipients(messageID, recipientType, orderedIDs, displayNames)
}

func storeAttachment(st *store.Store, attachmentsDir string, messageID int64, att *mime.Attachment) error {
	if attachmentsDir == "" || len(att.Content) == 0 || att.ContentHash == "" {
		return nil
	}

	// Content-addressed storage: first 2 chars / full hash
	subdir := att.ContentHash[:2]
	storagePath := filepath.Join(subdir, att.ContentHash)
	fullPath := filepath.Join(attachmentsDir, storagePath)

	if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
		return err
	}

	if _, err := os.Stat(fullPath); errors.Is(err, os.ErrNotExist) {
		if err := fileutil.SecureWriteFile(fullPath, att.Content, 0600); err != nil {
			return err
		}
	}

	return st.UpsertAttachment(messageID, att.Filename, att.ContentType, storagePath, att.ContentHash, att.Size)
}

func ingestRawEmail(ctx context.Context, st *store.Store, sourceID int64, identifier string, attachmentsDir string, labelIDs []int64, sourceMsgID string, rawHash string, msg *mbox.Message, log *slog.Logger) error {
	parsed, parseErr := mime.Parse(msg.Raw)
	if parseErr != nil {
		errMsg := textutil.FirstLine(parseErr.Error())
		parsed = &mime.Message{
			Subject:  "(MIME parse error)",
			BodyText: fmt.Sprintf("[MIME parsing failed: %s]\n\nRaw MIME data is preserved in message_raw table.", errMsg),
		}
		// Best-effort date from separator.
		if t, ok := parseFromLineDate(msg.FromLine); ok {
			parsed.Date = t
		}
	}

	// Ensure all text fields are valid UTF-8.
	subject := textutil.EnsureUTF8(parsed.Subject)
	bodyText := textutil.EnsureUTF8(parsed.GetBodyText())
	bodyHTML := textutil.EnsureUTF8(parsed.BodyHTML)

	// Ensure participants exist.
	allAddresses := append(append(append(parsed.From, parsed.To...), parsed.Cc...), parsed.Bcc...)
	participantMap, err := st.EnsureParticipantsBatch(allAddresses)
	if err != nil {
		return fmt.Errorf("ensure participants: %w", err)
	}

	// Sender ID.
	var senderID sql.NullInt64
	if len(parsed.From) > 0 && parsed.From[0].Email != "" {
		if id, ok := participantMap[parsed.From[0].Email]; ok {
			senderID = sql.NullInt64{Int64: id, Valid: true}
		}
	}

	// is_from_me heuristic: first From matches the source identifier.
	isFromMe := false
	if len(parsed.From) > 0 {
		if strings.EqualFold(parsed.From[0].Email, identifier) {
			isFromMe = true
		}
	}

	threadID := threadKey(parsed, rawHash)

	convSubject := subject
	if convSubject == "" {
		convSubject = "(no subject)"
	}
	conversationID, err := st.EnsureConversation(sourceID, threadID, convSubject)
	if err != nil {
		return fmt.Errorf("ensure conversation: %w", err)
	}

	// Sent date (UTC).
	var sentAt sql.NullTime
	if !parsed.Date.IsZero() {
		sentAt = sql.NullTime{Time: parsed.Date.UTC(), Valid: true}
	} else if t, ok := parseFromLineDate(msg.FromLine); ok {
		sentAt = sql.NullTime{Time: t, Valid: true}
	}

	snippet := snippetFromBody(bodyText)

	rec := &store.Message{
		ConversationID:  conversationID,
		SourceID:        sourceID,
		SourceMessageID: sourceMsgID,
		MessageType:     "email",
		SentAt:          sentAt,
		SenderID:        senderID,
		IsFromMe:        isFromMe,
		Subject:         sql.NullString{String: subject, Valid: subject != ""},
		Snippet:         sql.NullString{String: snippet, Valid: snippet != ""},
		SizeEstimate:    int64(len(msg.Raw)),
		HasAttachments:  len(parsed.Attachments) > 0,
		AttachmentCount: len(parsed.Attachments),
	}

	messageID, err := st.UpsertMessage(rec)
	if err != nil {
		return fmt.Errorf("upsert message: %w", err)
	}

	if err := st.UpsertMessageBody(messageID,
		sql.NullString{String: bodyText, Valid: bodyText != ""},
		sql.NullString{String: bodyHTML, Valid: bodyHTML != ""},
	); err != nil {
		return fmt.Errorf("upsert body: %w", err)
	}

	if err := st.UpsertMessageRaw(messageID, msg.Raw); err != nil {
		return fmt.Errorf("store raw: %w", err)
	}

	// Recipients.
	if err := storeRecipients(st, messageID, "from", parsed.From, participantMap); err != nil {
		return fmt.Errorf("store from: %w", err)
	}
	if err := storeRecipients(st, messageID, "to", parsed.To, participantMap); err != nil {
		return fmt.Errorf("store to: %w", err)
	}
	if err := storeRecipients(st, messageID, "cc", parsed.Cc, participantMap); err != nil {
		return fmt.Errorf("store cc: %w", err)
	}
	if err := storeRecipients(st, messageID, "bcc", parsed.Bcc, participantMap); err != nil {
		return fmt.Errorf("store bcc: %w", err)
	}

	// Labels.
	if err := st.ReplaceMessageLabels(messageID, labelIDs); err != nil {
		return fmt.Errorf("store labels: %w", err)
	}

	// Attachments.
	for i := range parsed.Attachments {
		att := &parsed.Attachments[i]
		if err := storeAttachment(st, attachmentsDir, messageID, att); err != nil {
			// Non-fatal: keep importing.
			// (MIME decoding already happened in parser; failure here is filesystem/db.)
			if log != nil {
				log.Warn("failed to store attachment", "message", messageID, "filename", att.Filename, "error", err)
			}
		}
	}

	// FTS.
	if st.FTS5Available() {
		fromAddr := joinEmails(parsed.From)
		toAddrs := joinEmails(parsed.To)
		ccAddrs := joinEmails(parsed.Cc)
		if err := st.UpsertFTS(messageID, subject, bodyText, fromAddr, toAddrs, ccAddrs); err != nil {
			// Non-fatal.
			if log != nil {
				log.Warn("failed to upsert FTS", "message", messageID, "error", err)
			}
		}
	}

	return nil
}
