// Package sync provides Gmail synchronization workflows.
package sync

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/gogs/chardet"
	"github.com/wesm/msgvault/internal/gmail"
	"github.com/wesm/msgvault/internal/mime"
	"github.com/wesm/msgvault/internal/store"
	"golang.org/x/text/encoding"
	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/encoding/japanese"
	"golang.org/x/text/encoding/korean"
	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/encoding/traditionalchinese"
)

// ensureUTF8 ensures a string is valid UTF-8.
// If already valid UTF-8, returns as-is.
// Otherwise attempts charset detection and conversion.
// Falls back to replacing invalid bytes with replacement character.
func ensureUTF8(s string) string {
	if utf8.ValidString(s) {
		return s
	}

	// Try charset detection and conversion
	data := []byte(s)

	// Try automatic charset detection (works better on longer samples,
	// but we try it even for short strings with lower confidence threshold)
	minConfidence := 30 // Lower threshold for shorter strings
	if len(data) > 50 {
		minConfidence = 50 // Higher threshold for longer strings
	}

	detector := chardet.NewTextDetector()
	result, err := detector.DetectBest(data)
	if err == nil && result.Confidence >= minConfidence {
		if enc := getEncodingByName(result.Charset); enc != nil {
			decoded, err := enc.NewDecoder().Bytes(data)
			if err == nil && utf8.Valid(decoded) {
				return string(decoded)
			}
		}
	}

	// Try common encodings in order of likelihood for email content.
	// Single-byte encodings first (Windows-1252/Latin-1 are most common in Western emails),
	// then multi-byte Asian encodings.
	encodings := []encoding.Encoding{
		charmap.Windows1252,     // Smart quotes, dashes common in Windows emails
		charmap.ISO8859_1,       // Latin-1 (Western European)
		charmap.ISO8859_15,      // Latin-9 (Western European with Euro)
		japanese.ShiftJIS,       // Japanese
		japanese.EUCJP,          // Japanese
		korean.EUCKR,            // Korean
		simplifiedchinese.GBK,   // Simplified Chinese
		traditionalchinese.Big5, // Traditional Chinese
	}

	for _, enc := range encodings {
		decoded, err := enc.NewDecoder().Bytes(data)
		if err == nil && utf8.Valid(decoded) {
			return string(decoded)
		}
	}

	// Last resort: replace invalid bytes
	return sanitizeUTF8(s)
}

// sanitizeUTF8 replaces invalid UTF-8 bytes with replacement character.
func sanitizeUTF8(s string) string {
	var sb strings.Builder
	sb.Grow(len(s))
	for i := 0; i < len(s); {
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError && size == 1 {
			sb.WriteRune('\ufffd')
			i++
		} else {
			sb.WriteRune(r)
			i += size
		}
	}
	return sb.String()
}

// getEncodingByName returns an encoding for the given IANA charset name.
func getEncodingByName(name string) encoding.Encoding {
	switch name {
	case "windows-1252", "CP1252", "cp1252":
		return charmap.Windows1252
	case "ISO-8859-1", "iso-8859-1", "latin1", "latin-1":
		return charmap.ISO8859_1
	case "ISO-8859-15", "iso-8859-15", "latin9":
		return charmap.ISO8859_15
	case "ISO-8859-2", "iso-8859-2", "latin2":
		return charmap.ISO8859_2
	case "Shift_JIS", "shift_jis", "shift-jis", "sjis":
		return japanese.ShiftJIS
	case "EUC-JP", "euc-jp", "eucjp":
		return japanese.EUCJP
	case "ISO-2022-JP", "iso-2022-jp":
		return japanese.ISO2022JP
	case "EUC-KR", "euc-kr", "euckr":
		return korean.EUCKR
	case "GB2312", "gb2312", "GBK", "gbk":
		return simplifiedchinese.GBK
	case "GB18030", "gb18030":
		return simplifiedchinese.GB18030
	case "Big5", "big5", "big-5":
		return traditionalchinese.Big5
	case "KOI8-R", "koi8-r":
		return charmap.KOI8R
	case "KOI8-U", "koi8-u":
		return charmap.KOI8U
	default:
		return nil
	}
}

// ErrHistoryExpired indicates that the Gmail history ID is too old and a full sync is required.
var ErrHistoryExpired = errors.New("history expired - run full sync")

// Options configures sync behavior.
type Options struct {
	// Query is an optional Gmail search query (e.g., "before:2020/01/01")
	Query string

	// NoResume forces a fresh sync even if a checkpoint exists
	NoResume bool

	// BatchSize is the number of messages to fetch in parallel (default: 10)
	BatchSize int

	// CheckpointInterval is how often to save progress (default: every 200 messages)
	CheckpointInterval int

	// AttachmentsDir is where to store attachments
	AttachmentsDir string
}

// DefaultOptions returns sensible defaults.
func DefaultOptions() *Options {
	return &Options{
		BatchSize:          10,
		CheckpointInterval: 200,
	}
}

// Syncer performs Gmail synchronization.
type Syncer struct {
	client   gmail.API
	store    *store.Store
	logger   *slog.Logger
	progress gmail.SyncProgress
	opts     *Options
}

// New creates a new Syncer.
func New(client gmail.API, store *store.Store, opts *Options) *Syncer {
	if opts == nil {
		opts = DefaultOptions()
	}

	return &Syncer{
		client:   client,
		store:    store,
		logger:   slog.Default(),
		progress: gmail.NullProgress{},
		opts:     opts,
	}
}

// WithLogger sets the logger.
func (s *Syncer) WithLogger(logger *slog.Logger) *Syncer {
	s.logger = logger
	return s
}

// WithProgress sets the progress reporter.
func (s *Syncer) WithProgress(p gmail.SyncProgress) *Syncer {
	s.progress = p
	return s
}

// Full performs a full synchronization.
func (s *Syncer) Full(ctx context.Context, email string) (*gmail.SyncSummary, error) {
	startTime := time.Now()
	summary := &gmail.SyncSummary{StartTime: startTime}

	// Get or create source
	source, err := s.store.GetOrCreateSource("gmail", email)
	if err != nil {
		return nil, fmt.Errorf("get/create source: %w", err)
	}

	// Check for active sync to resume
	var syncID int64
	var checkpoint *store.Checkpoint
	var pageToken string

	if !s.opts.NoResume {
		activeSync, err := s.store.GetActiveSync(source.ID)
		if err != nil {
			return nil, fmt.Errorf("check active sync: %w", err)
		}
		if activeSync != nil {
			syncID = activeSync.ID
			if activeSync.CursorBefore.Valid {
				pageToken = activeSync.CursorBefore.String
			}
			checkpoint = &store.Checkpoint{
				PageToken:         pageToken,
				MessagesProcessed: activeSync.MessagesProcessed,
				MessagesAdded:     activeSync.MessagesAdded,
				MessagesUpdated:   activeSync.MessagesUpdated,
				ErrorsCount:       activeSync.ErrorsCount,
			}
			summary.WasResumed = true
			summary.ResumedFromToken = pageToken
			s.logger.Info("resuming sync", "messages_processed", checkpoint.MessagesProcessed)
		}
	}

	// Start new sync if not resuming
	if syncID == 0 {
		syncID, err = s.store.StartSync(source.ID, "full")
		if err != nil {
			return nil, fmt.Errorf("start sync: %w", err)
		}
		checkpoint = &store.Checkpoint{}
	}

	// Defer failure handling
	defer func() {
		if r := recover(); r != nil {
			_ = s.store.FailSync(syncID, fmt.Sprintf("panic: %v", r))
			panic(r)
		}
	}()

	// Get profile to verify connection and get historyId
	profile, err := s.client.GetProfile(ctx)
	if err != nil {
		_ = s.store.FailSync(syncID, err.Error())
		return nil, fmt.Errorf("get profile: %w", err)
	}

	s.logger.Info("syncing account", "email", profile.EmailAddress, "messages", profile.MessagesTotal)

	// Sync labels
	labelMap, err := s.syncLabels(ctx, source.ID)
	if err != nil {
		_ = s.store.FailSync(syncID, err.Error())
		return nil, fmt.Errorf("sync labels: %w", err)
	}

	// List and sync messages
	var totalEstimate int64
	firstPage := true

	for {
		// List messages
		listResp, err := s.client.ListMessages(ctx, s.opts.Query, pageToken)
		if err != nil {
			_ = s.store.FailSync(syncID, err.Error())
			return nil, fmt.Errorf("list messages: %w", err)
		}

		if firstPage {
			totalEstimate = listResp.ResultSizeEstimate
			s.progress.OnStart(totalEstimate)
			firstPage = false
		}

		if len(listResp.Messages) == 0 {
			break
		}

		// Check which messages already exist
		messageIDs := make([]string, len(listResp.Messages))
		threadIDs := make(map[string]string) // messageID -> threadID
		for i, m := range listResp.Messages {
			messageIDs[i] = m.ID
			threadIDs[m.ID] = m.ThreadID
		}

		existingMap, err := s.store.MessageExistsBatch(source.ID, messageIDs)
		if err != nil {
			_ = s.store.FailSync(syncID, err.Error())
			return nil, fmt.Errorf("check existing: %w", err)
		}

		// Filter to new messages
		var newIDs []string
		for _, id := range messageIDs {
			if _, exists := existingMap[id]; !exists {
				newIDs = append(newIDs, id)
			}
		}

		checkpoint.MessagesProcessed += int64(len(messageIDs))
		skipped := int64(len(messageIDs) - len(newIDs))

		// Fetch new messages in batch
		var oldestDate time.Time // Track oldest date since Gmail returns newest-to-oldest
		if len(newIDs) > 0 {
			rawMessages, err := s.client.GetMessagesRawBatch(ctx, newIDs)
			if err != nil {
				_ = s.store.FailSync(syncID, err.Error())
				return nil, fmt.Errorf("fetch messages: %w", err)
			}

			// Ingest messages
			for i, raw := range rawMessages {
				if raw == nil {
					checkpoint.ErrorsCount++
					continue
				}

				// Track oldest message date for progress display
				// Gmail returns messages newest-to-oldest, so oldest shows where we've reached
				if raw.InternalDate > 0 {
					msgDate := time.UnixMilli(raw.InternalDate)
					if oldestDate.IsZero() || msgDate.Before(oldestDate) {
						oldestDate = msgDate
					}
				}

				threadID := threadIDs[newIDs[i]]
				err := s.ingestMessage(ctx, source.ID, raw, threadID, labelMap)
				if err != nil {
					s.logger.Warn("failed to ingest message", "id", raw.ID, "error", err)
					checkpoint.ErrorsCount++
					continue
				}

				checkpoint.MessagesAdded++
				summary.BytesDownloaded += int64(len(raw.Raw))
			}
		}

		// Report current position date before progress (so UI shows consistent state)
		if !oldestDate.IsZero() {
			if p, ok := s.progress.(gmail.SyncProgressWithDate); ok {
				p.OnLatestDate(oldestDate)
			}
		}

		// Report progress
		s.progress.OnProgress(checkpoint.MessagesProcessed, checkpoint.MessagesAdded, skipped)

		// Save checkpoint
		pageToken = listResp.NextPageToken
		checkpoint.PageToken = pageToken
		if err := s.store.UpdateSyncCheckpoint(syncID, checkpoint); err != nil {
			s.logger.Warn("failed to save checkpoint", "error", err)
		}

		// No more pages
		if pageToken == "" {
			break
		}
	}

	// Update source with final history ID
	historyIDStr := strconv.FormatUint(profile.HistoryID, 10)
	if err := s.store.UpdateSourceSyncCursor(source.ID, historyIDStr); err != nil {
		s.logger.Warn("failed to update sync cursor", "error", err)
	}

	// Mark sync complete
	if err := s.store.CompleteSync(syncID, historyIDStr); err != nil {
		s.logger.Warn("failed to complete sync", "error", err)
	}

	// Build summary
	summary.EndTime = time.Now()
	summary.Duration = summary.EndTime.Sub(summary.StartTime)
	summary.MessagesFound = checkpoint.MessagesProcessed
	summary.MessagesAdded = checkpoint.MessagesAdded
	summary.MessagesUpdated = checkpoint.MessagesUpdated
	summary.MessagesSkipped = checkpoint.MessagesProcessed - checkpoint.MessagesAdded - checkpoint.MessagesUpdated
	summary.Errors = checkpoint.ErrorsCount
	summary.FinalHistoryID = profile.HistoryID

	s.progress.OnComplete(summary)
	return summary, nil
}

// syncLabels syncs all labels and returns a map of Gmail label ID to internal ID.
func (s *Syncer) syncLabels(ctx context.Context, sourceID int64) (map[string]int64, error) {
	labels, err := s.client.ListLabels(ctx)
	if err != nil {
		return nil, err
	}

	labelNames := make(map[string]string)
	for _, l := range labels {
		labelNames[l.ID] = l.Name
	}

	return s.store.EnsureLabelsBatch(sourceID, labelNames)
}

// ingestMessage parses and stores a single message.
func (s *Syncer) ingestMessage(ctx context.Context, sourceID int64, raw *gmail.RawMessage, threadID string, labelMap map[string]int64) error {
	// Validate raw MIME data exists (Python sync: line 242-244)
	if len(raw.Raw) == 0 {
		return fmt.Errorf("missing raw MIME data for message %s", raw.ID)
	}

	// Fall back to raw.ThreadID if list response threadID is missing,
	// then to message ID as last resort (Python sync: line 232-234)
	if threadID == "" {
		threadID = raw.ThreadID
		if threadID == "" {
			threadID = raw.ID
		}
	}

	// Parse MIME - on failure, store with placeholder body
	parsed, parseErr := mime.Parse(raw.Raw)
	if parseErr != nil {
		// Extract just the first line of error (enmime includes full stack traces)
		errMsg := firstLine(parseErr.Error())

		// Create placeholder message for MIME parse failures
		// This preserves the raw data for potential future re-parsing
		parsed = &mime.Message{
			Subject:  extractSubjectFromSnippet(raw.Snippet),
			BodyText: fmt.Sprintf("[MIME parsing failed: %s]\n\nRaw MIME data is preserved in message_raw table.", errMsg),
		}
		// Set date from InternalDate if available
		if raw.InternalDate > 0 {
			parsed.Date = time.UnixMilli(raw.InternalDate).UTC()
		}
		s.logger.Warn("MIME parse failed, storing with placeholder",
			"id", raw.ID,
			"error", errMsg)
	}

	// Ensure conversation (thread)
	// Ensure subject is valid UTF-8
	subject := ensureUTF8(parsed.Subject)
	// Use placeholder for conversation matching only (subject can be empty for storage)
	convSubject := subject
	if convSubject == "" {
		convSubject = "(no subject)"
	}
	conversationID, err := s.store.EnsureConversation(sourceID, threadID, convSubject)
	if err != nil {
		return fmt.Errorf("ensure conversation: %w", err)
	}

	// Ensure participants
	allAddresses := append(append(append(parsed.From, parsed.To...), parsed.Cc...), parsed.Bcc...)
	participantMap, err := s.store.EnsureParticipantsBatch(allAddresses)
	if err != nil {
		return fmt.Errorf("ensure participants: %w", err)
	}

	// Get sender ID
	var senderID sql.NullInt64
	if len(parsed.From) > 0 && parsed.From[0].Email != "" {
		if id, ok := participantMap[parsed.From[0].Email]; ok {
			senderID = sql.NullInt64{Int64: id, Valid: true}
		}
	}

	// Build message record
	// Ensure all text fields are valid UTF-8 (detect encoding and convert if needed)
	// Note: subject was already sanitized above for conversation matching
	bodyText := ensureUTF8(parsed.GetBodyText())
	bodyHTML := ensureUTF8(parsed.BodyHTML)
	snippet := ensureUTF8(raw.Snippet)

	msg := &store.Message{
		ConversationID:  conversationID,
		SourceID:        sourceID,
		SourceMessageID: raw.ID,
		MessageType:     "email",
		SenderID:        senderID,
		Subject:         sql.NullString{String: subject, Valid: subject != ""},
		Snippet:         sql.NullString{String: snippet, Valid: snippet != ""},
		SizeEstimate:    raw.SizeEstimate,
		HasAttachments:  len(parsed.Attachments) > 0,
		AttachmentCount: len(parsed.Attachments),
	}

	// Set dates - always store in UTC for consistent querying
	if raw.InternalDate > 0 {
		t := time.UnixMilli(raw.InternalDate).UTC()
		msg.InternalDate = sql.NullTime{Time: t, Valid: true}
	}
	if !parsed.Date.IsZero() {
		// parseDate already returns UTC
		msg.SentAt = sql.NullTime{Time: parsed.Date, Valid: true}
	} else if msg.InternalDate.Valid {
		// Fall back to InternalDate if Date header couldn't be parsed
		msg.SentAt = msg.InternalDate
	}

	// Upsert message
	messageID, err := s.store.UpsertMessage(msg)
	if err != nil {
		return fmt.Errorf("upsert message: %w", err)
	}

	// Store message body in separate table
	if err := s.store.UpsertMessageBody(messageID,
		sql.NullString{String: bodyText, Valid: bodyText != ""},
		sql.NullString{String: bodyHTML, Valid: bodyHTML != ""},
	); err != nil {
		return fmt.Errorf("upsert message body: %w", err)
	}

	// Store raw MIME
	if err := s.store.UpsertMessageRaw(messageID, raw.Raw); err != nil {
		return fmt.Errorf("store raw: %w", err)
	}

	// Store recipients
	if err := s.storeRecipients(messageID, "from", parsed.From, participantMap); err != nil {
		return fmt.Errorf("store from: %w", err)
	}
	if err := s.storeRecipients(messageID, "to", parsed.To, participantMap); err != nil {
		return fmt.Errorf("store to: %w", err)
	}
	if err := s.storeRecipients(messageID, "cc", parsed.Cc, participantMap); err != nil {
		return fmt.Errorf("store cc: %w", err)
	}
	if err := s.storeRecipients(messageID, "bcc", parsed.Bcc, participantMap); err != nil {
		return fmt.Errorf("store bcc: %w", err)
	}

	// Store labels
	var labelIDs []int64
	for _, gmailLabelID := range raw.LabelIDs {
		if internalID, ok := labelMap[gmailLabelID]; ok {
			labelIDs = append(labelIDs, internalID)
		}
	}
	if err := s.store.ReplaceMessageLabels(messageID, labelIDs); err != nil {
		return fmt.Errorf("store labels: %w", err)
	}

	// Store attachments
	if s.opts.AttachmentsDir != "" {
		for _, att := range parsed.Attachments {
			if err := s.storeAttachment(messageID, &att); err != nil {
				s.logger.Warn("failed to store attachment", "message", messageID, "filename", att.Filename, "error", err)
			}
		}
	}

	return nil
}

// storeRecipients stores recipient records.
func (s *Syncer) storeRecipients(messageID int64, recipientType string, addresses []mime.Address, participantMap map[string]int64) error {
	if len(addresses) == 0 {
		return nil
	}

	// Track participant ID -> display name, preferring non-empty names
	// This handles duplicates where the first occurrence might have an empty name
	// but a later occurrence has a better display name
	idToName := make(map[int64]string)
	var orderedIDs []int64

	for _, addr := range addresses {
		if id, ok := participantMap[addr.Email]; ok {
			// Ensure display name is valid UTF-8
			name := ensureUTF8(addr.Name)
			if _, seen := idToName[id]; !seen {
				// First occurrence - record the ID order and initial name
				orderedIDs = append(orderedIDs, id)
				idToName[id] = name
			} else if idToName[id] == "" && name != "" {
				// Duplicate with better name - prefer non-empty
				idToName[id] = name
			}
		}
	}

	// Build slices in original order
	participantIDs := orderedIDs
	displayNames := make([]string, len(orderedIDs))
	for i, id := range orderedIDs {
		displayNames[i] = idToName[id]
	}

	return s.store.ReplaceMessageRecipients(messageID, recipientType, participantIDs, displayNames)
}

// storeAttachment stores an attachment to disk and records it in the database.
func (s *Syncer) storeAttachment(messageID int64, att *mime.Attachment) error {
	if len(att.Content) == 0 {
		return nil
	}

	// Content-addressed storage: first 2 chars / full hash
	subdir := att.ContentHash[:2]
	storagePath := filepath.Join(subdir, att.ContentHash)
	fullPath := filepath.Join(s.opts.AttachmentsDir, storagePath)

	// Create directory if needed
	if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
		return err
	}

	// Write file if it doesn't exist (deduplication)
	if _, err := os.Stat(fullPath); os.IsNotExist(err) {
		if err := os.WriteFile(fullPath, att.Content, 0600); err != nil {
			return err
		}
	}

	// Record in database
	return s.store.UpsertAttachment(messageID, att.Filename, att.ContentType, storagePath, att.ContentHash, att.Size)
}

// extractSubjectFromSnippet attempts to extract a subject from the message snippet.
// Used as fallback when MIME parsing fails.
func extractSubjectFromSnippet(snippet string) string {
	if snippet == "" {
		return "(MIME parse error)"
	}
	// Use first line of snippet, truncated
	line := snippet
	if idx := strings.Index(snippet, "\n"); idx > 0 {
		line = snippet[:idx]
	}
	return truncateRunes(line, 80)
}

// truncateRunes truncates a string to maxRunes runes (not bytes), adding "..." if truncated.
// This is UTF-8 safe and won't split multi-byte characters.
func truncateRunes(s string, maxRunes int) string {
	runes := []rune(s)
	if len(runes) <= maxRunes {
		return s
	}
	if maxRunes <= 3 {
		return string(runes[:maxRunes])
	}
	return string(runes[:maxRunes-3]) + "..."
}

// firstLine returns the first line of a string.
// Used to extract clean error messages from enmime's stack-trace-laden errors.
func firstLine(s string) string {
	if idx := strings.Index(s, "\n"); idx >= 0 {
		return s[:idx]
	}
	return s
}
