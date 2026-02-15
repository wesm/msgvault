package importer

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/wesm/msgvault/internal/export"
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

	// MaxMessageBytes limits the maximum size of a single message read from the MBOX.
	// If zero, a default of 128 MiB is used.
	MaxMessageBytes int64

	// IngestFunc allows callers (tests) to override message ingestion. If nil,
	// the default ingestRawEmail is used.
	IngestFunc func(ctx context.Context, st *store.Store, sourceID int64, identifier string, attachmentsDir string, labelIDs []int64, sourceMsgID string, rawHash string, msg *mbox.Message, log *slog.Logger) error

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
	MessagesUpdated   int64
	MessagesSkipped   int64
	Errors            int64
	HardErrors        bool
}

type mboxCheckpoint struct {
	File   string `json:"file"`
	Offset int64  `json:"offset"`
	Seq    int64  `json:"seq,omitempty"`
}

const defaultMaxMboxMessageBytes int64 = 128 << 20 // 128 MiB

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
	if opts.MaxMessageBytes <= 0 {
		opts.MaxMessageBytes = defaultMaxMboxMessageBytes
	}
	ingestFn := opts.IngestFunc
	if ingestFn == nil {
		ingestFn = ingestRawEmail
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
	cpFile := absPath
	if resolved, err := filepath.EvalSymlinks(absPath); err == nil {
		cpFile = resolved
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
		seq    int64
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
					sameFile := false
					if mcp.File == absPath || mcp.File == cpFile {
						sameFile = true
					} else if mcp.File != "" {
						if curInfo, err := os.Stat(absPath); err == nil {
							if cpInfo, err := os.Stat(mcp.File); err == nil && os.SameFile(curInfo, cpInfo) {
								sameFile = true
							}
						}
					}

					if sameFile && mcp.Offset > 0 {
						offset = mcp.Offset
						seq = mcp.Seq
						summary.WasResumed = true
						summary.ResumedOffset = offset
						log.Info("resuming mbox import", "file", absPath, "offset", offset, "processed", cp.MessagesProcessed)
					} else if mcp.File != "" && !sameFile {
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
	if err := saveMboxCheckpoint(st, syncID, cpFile, offset, seq, &cp); err != nil {
		cp.ErrorsCount++
		summary.Errors++
		log.Warn("failed to save initial checkpoint", "error", err)
	}

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

	fi, err := f.Stat()
	if err != nil {
		_ = st.FailSync(syncID, err.Error())
		return nil, fmt.Errorf("stat mbox: %w", err)
	}
	if offset > fi.Size() {
		resumeErr := fmt.Errorf("resume offset %d is beyond end of file (size %d) for %q; rerun with --no-resume to start fresh", offset, fi.Size(), absPath)
		_ = st.FailSync(syncID, resumeErr.Error())
		return nil, resumeErr
	}
	if offset > 0 && offset == fi.Size() {
		log.Info("resume offset at end of file; no work to do", "file", absPath, "offset", offset, "size", fi.Size())
	}

	if offset > 0 {
		if _, err := f.Seek(offset, io.SeekStart); err != nil {
			_ = st.FailSync(syncID, err.Error())
			return nil, fmt.Errorf("seek mbox: %w", err)
		}
	}

	if offset == 0 && cp.MessagesProcessed == 0 {
		if err := mbox.Validate(f, 8<<20); err != nil {
			_ = st.FailSync(syncID, err.Error())
			return summary, err
		}
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			_ = st.FailSync(syncID, err.Error())
			return nil, fmt.Errorf("seek mbox: %w", err)
		}
	}

	r := mbox.NewReaderWithMaxMessageBytes(f, opts.MaxMessageBytes)

	// If we resumed at a saved offset, the reader's logical Offset() should now be that.
	// However, if the offset lands in the middle of a line (corrupt checkpoint), the
	// reader will scan to the next separator.
	var lastCheckpointOffset int64 = offset
	var lastCheckpointSeq int64 = seq
	checkpointBlocked := false
	hardErrors := false

	type pendingMboxMessage struct {
		Msg        *mbox.Message
		RawHash    string
		SourceMsg  string
		Seq        int64
		NextOffset int64
	}

	const (
		existsCheckBatchSize  = 200
		existsCheckBatchBytes = 32 << 20 // 32 MiB
	)

	var pending []pendingMboxMessage
	var pendingBytes int64

	msgSeq := seq

	flushPending := func() (bool, error) {
		if len(pending) == 0 {
			return false, nil
		}

		ids := make([]string, len(pending))
		for i, p := range pending {
			ids[i] = p.SourceMsg
		}

		existingWithRaw, err := st.MessageExistsWithRawBatch(src.ID, ids)
		batchOK := err == nil
		if err != nil {
			cp.ErrorsCount++
			summary.Errors++
			log.Warn("existence check failed", "error", err)
		}

		existingAny, err := st.MessageExistsBatch(src.ID, ids)
		anyOK := err == nil
		if err != nil {
			cp.ErrorsCount++
			summary.Errors++
			log.Warn("existence check failed (any)", "error", err)
		}

		for _, p := range pending {
			if err := ctx.Err(); err != nil {
				summary.FinalOffset = lastCheckpointOffset
				summary.Duration = time.Since(start)
				if err := saveMboxCheckpoint(st, syncID, cpFile, lastCheckpointOffset, lastCheckpointSeq, &cp); err != nil {
					cp.ErrorsCount++
					summary.Errors++
					log.Warn("failed to save checkpoint", "error", err)
				}
				return true, nil
			}

			cp.MessagesProcessed++
			summary.MessagesProcessed++
			summary.BytesProcessed += int64(len(p.Msg.Raw))

			exists := false
			if batchOK {
				_, exists = existingWithRaw[p.SourceMsg]
			} else {
				one, err := st.MessageExistsWithRawBatch(src.ID, []string{p.SourceMsg})
				if err != nil {
					cp.ErrorsCount++
					summary.Errors++
					log.Warn("existence check failed; attempting ingest anyway", "error", err)
				} else {
					_, exists = one[p.SourceMsg]
				}
			}

			if exists {
				summary.MessagesSkipped++

				// Update checkpoint offset even when skipping so resumption progresses.
				if !checkpointBlocked {
					lastCheckpointOffset = p.NextOffset
					lastCheckpointSeq = p.Seq
				}
				if cp.MessagesProcessed%int64(opts.CheckpointInterval) == 0 {
					if err := saveMboxCheckpoint(st, syncID, cpFile, lastCheckpointOffset, lastCheckpointSeq, &cp); err != nil {
						cp.ErrorsCount++
						summary.Errors++
						log.Warn("failed to save checkpoint", "error", err)
					}
				}
				continue
			}

			alreadyExists := false
			if anyOK {
				_, alreadyExists = existingAny[p.SourceMsg]
			} else {
				one, err := st.MessageExistsBatch(src.ID, []string{p.SourceMsg})
				if err != nil {
					cp.ErrorsCount++
					summary.Errors++
					log.Warn("existence check failed (any); counting as added", "error", err)
				} else {
					_, alreadyExists = one[p.SourceMsg]
				}
			}

			if err := ingestFn(ctx, st, src.ID, opts.Identifier, opts.AttachmentsDir, labelIDs, p.SourceMsg, p.RawHash, p.Msg, log); err != nil {
				cp.ErrorsCount++
				summary.Errors++
				log.Warn("failed to ingest message", "source_msg", p.SourceMsg, "next_offset", p.NextOffset, "error", err)
				checkpointBlocked = true
				hardErrors = true
				continue
			}
			if alreadyExists {
				cp.MessagesUpdated++
				summary.MessagesUpdated++
			} else {
				cp.MessagesAdded++
				summary.MessagesAdded++
			}

			if !checkpointBlocked {
				lastCheckpointOffset = p.NextOffset
				lastCheckpointSeq = p.Seq
			}

			if cp.MessagesProcessed%int64(opts.CheckpointInterval) == 0 {
				if err := saveMboxCheckpoint(st, syncID, cpFile, lastCheckpointOffset, lastCheckpointSeq, &cp); err != nil {
					cp.ErrorsCount++
					summary.Errors++
					log.Warn("failed to save checkpoint", "error", err)
				}
			}
		}

		clear(pending)
		pending = pending[:0]
		pendingBytes = 0
		return false, nil
	}

	for {
		if err := ctx.Err(); err != nil {
			summary.FinalOffset = lastCheckpointOffset
			summary.Duration = time.Since(start)
			// Record best-effort checkpoint before returning.
			if err := saveMboxCheckpoint(st, syncID, cpFile, lastCheckpointOffset, lastCheckpointSeq, &cp); err != nil {
				cp.ErrorsCount++
				summary.Errors++
				log.Warn("failed to save checkpoint", "error", err)
			}
			return summary, nil
		}

		msg, err := r.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			nextOffset := r.NextFromOffset()
			if !checkpointBlocked {
				lastCheckpointOffset = nextOffset
			}
			cp.ErrorsCount++
			summary.Errors++
			log.Warn("mbox read error", "error", err, "next_offset", nextOffset)
			continue
		}

		// Compute stable IDs:
		// - rawHash: sha256(raw MIME), used for thread fallback and dedup
		// - sourceMsgID: based on stable message-level identity (rawHash + seq)
		//   so that re-importing the same mailbox after file changes (e.g. new
		//   export with appended messages) deduplicates unchanged messages.
		msgSeq++
		sum := sha256.Sum256(msg.Raw)
		rawHash := hex.EncodeToString(sum[:])
		nextOffset := r.NextFromOffset()
		sourceMsgID := fmt.Sprintf("mbox-%s-%d", rawHash, msgSeq)
		pending = append(pending, pendingMboxMessage{
			Msg:        msg,
			RawHash:    rawHash,
			SourceMsg:  sourceMsgID,
			Seq:        msgSeq,
			NextOffset: nextOffset,
		})
		pendingBytes += int64(len(msg.Raw))

		if len(pending) >= existsCheckBatchSize || pendingBytes >= existsCheckBatchBytes {
			stop, err := flushPending()
			if err != nil {
				return summary, err
			}
			if stop {
				return summary, nil
			}
		}
	}

	if stop, err := flushPending(); err != nil {
		return summary, err
	} else if stop {
		return summary, nil
	}

	finalOffset := r.Offset()
	if checkpointBlocked {
		finalOffset = lastCheckpointOffset
	}
	summary.FinalOffset = finalOffset
	summary.Duration = time.Since(start)

	// Final checkpoint and mark sync complete.
	if err := saveMboxCheckpoint(st, syncID, cpFile, finalOffset, lastCheckpointSeq, &cp); err != nil {
		cp.ErrorsCount++
		summary.Errors++
		log.Warn("failed to save checkpoint", "error", err)
	}

	summary.HardErrors = hardErrors
	if hardErrors {
		if err := st.FailSync(syncID, fmt.Sprintf("completed with %d errors", cp.ErrorsCount)); err != nil {
			return summary, fmt.Errorf("fail sync: %w", err)
		}
		return summary, nil
	}

	finalMsg := fmt.Sprintf("offset:%d", r.Offset())
	if cp.ErrorsCount > 0 {
		finalMsg = fmt.Sprintf("offset:%d errors:%d", r.Offset(), cp.ErrorsCount)
	}
	if err := st.CompleteSync(syncID, finalMsg); err != nil {
		return summary, fmt.Errorf("complete sync: %w", err)
	}

	return summary, nil
}

func saveMboxCheckpoint(st *store.Store, syncID int64, file string, offset int64, seq int64, cp *store.Checkpoint) error {
	b, err := json.Marshal(mboxCheckpoint{File: file, Offset: offset, Seq: seq})
	if err != nil {
		return fmt.Errorf("marshal checkpoint: %w", err)
	}
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
	if t, ok := mbox.ParseFromSeparatorDateStrict(fromLine); ok {
		return t.UTC(), true
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
	storagePath, err := export.StoreAttachmentFile(attachmentsDir, att)
	if err != nil || storagePath == "" {
		return err
	}
	return st.UpsertAttachment(messageID, att.Filename, att.ContentType, storagePath, att.ContentHash, len(att.Content))
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
	allAddresses := make([]mime.Address, 0, len(parsed.From)+len(parsed.To)+len(parsed.Cc)+len(parsed.Bcc))
	allAddresses = append(allAddresses, parsed.From...)
	allAddresses = append(allAddresses, parsed.To...)
	allAddresses = append(allAddresses, parsed.Cc...)
	allAddresses = append(allAddresses, parsed.Bcc...)
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

	hasAttachments := len(parsed.Attachments) > 0
	attachmentCount := len(parsed.Attachments)

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
		HasAttachments:  hasAttachments,
		AttachmentCount: attachmentCount,
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
			if log != nil {
				log.Warn("failed to store attachment", "message", messageID, "filename", att.Filename, "error", err)
			}
			return fmt.Errorf("store attachment %q: %w", att.Filename, err)
		}
	}

	if attachmentsDir != "" && len(parsed.Attachments) > 0 {
		var storedCount int
		if err := st.DB().QueryRow(`SELECT COUNT(*) FROM attachments WHERE message_id = ?`, messageID).Scan(&storedCount); err != nil {
			if log != nil {
				log.Warn("failed to count stored attachments", "message", messageID, "error", err)
			}
		} else if storedCount != attachmentCount {
			if _, err := st.DB().Exec(`UPDATE messages SET has_attachments = ?, attachment_count = ? WHERE id = ?`, storedCount > 0, storedCount, messageID); err != nil {
				if log != nil {
					log.Warn("failed to update attachment metadata", "message", messageID, "error", err)
				}
			}
		}
	}

	if err := st.UpsertMessageRaw(messageID, msg.Raw); err != nil {
		return fmt.Errorf("store raw: %w", err)
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
