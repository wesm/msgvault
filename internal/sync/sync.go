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
	"time"

	"github.com/wesm/msgvault/internal/gmail"
	"github.com/wesm/msgvault/internal/mime"
	"github.com/wesm/msgvault/internal/store"
	"github.com/wesm/msgvault/internal/textutil"
)

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

// syncState holds the state for a sync operation.
type syncState struct {
	syncID     int64
	checkpoint *store.Checkpoint
	pageToken  string
	wasResumed bool
}

// initSyncState initializes sync state, resuming from checkpoint if possible.
func (s *Syncer) initSyncState(sourceID int64) (*syncState, error) {
	state := &syncState{
		checkpoint: &store.Checkpoint{},
	}

	if !s.opts.NoResume {
		activeSync, err := s.store.GetActiveSync(sourceID)
		if err != nil {
			return nil, fmt.Errorf("check active sync: %w", err)
		}
		if activeSync != nil {
			state.syncID = activeSync.ID
			if activeSync.CursorBefore.Valid {
				state.pageToken = activeSync.CursorBefore.String
			}
			state.checkpoint = &store.Checkpoint{
				PageToken:         state.pageToken,
				MessagesProcessed: activeSync.MessagesProcessed,
				MessagesAdded:     activeSync.MessagesAdded,
				MessagesUpdated:   activeSync.MessagesUpdated,
				ErrorsCount:       activeSync.ErrorsCount,
			}
			state.wasResumed = true
			s.logger.Info("resuming sync", "messages_processed", state.checkpoint.MessagesProcessed)
			return state, nil
		}
	}

	// Start new sync
	syncID, err := s.store.StartSync(sourceID, "full")
	if err != nil {
		return nil, fmt.Errorf("start sync: %w", err)
	}
	state.syncID = syncID
	return state, nil
}

// batchResult holds the result of processing a batch.
type batchResult struct {
	processed  int64
	added      int64
	skipped    int64
	oldestDate time.Time
}

// processBatch processes a single batch of messages from a list response.
func (s *Syncer) processBatch(ctx context.Context, sourceID int64, listResp *gmail.MessageListResponse, labelMap map[string]int64, checkpoint *store.Checkpoint, summary *gmail.SyncSummary) (*batchResult, error) {
	result := &batchResult{}

	if len(listResp.Messages) == 0 {
		return result, nil
	}

	// Build message ID list and thread ID map
	messageIDs := make([]string, len(listResp.Messages))
	threadIDs := make(map[string]string) // messageID -> threadID
	for i, m := range listResp.Messages {
		messageIDs[i] = m.ID
		threadIDs[m.ID] = m.ThreadID
	}

	// Check which messages already exist
	existingMap, err := s.store.MessageExistsBatch(sourceID, messageIDs)
	if err != nil {
		return nil, fmt.Errorf("check existing: %w", err)
	}

	// Filter to new messages
	var newIDs []string
	for _, id := range messageIDs {
		if _, exists := existingMap[id]; !exists {
			newIDs = append(newIDs, id)
		}
	}

	result.processed = int64(len(messageIDs))
	result.skipped = int64(len(messageIDs) - len(newIDs))

	// Fetch and ingest new messages
	if len(newIDs) > 0 {
		rawMessages, err := s.client.GetMessagesRawBatch(ctx, newIDs)
		if err != nil {
			return nil, fmt.Errorf("fetch messages: %w", err)
		}

		for i, raw := range rawMessages {
			if raw == nil {
				checkpoint.ErrorsCount++
				continue
			}

			// Track oldest message date for progress display
			// Gmail returns messages newest-to-oldest, so oldest shows where we've reached
			if raw.InternalDate > 0 {
				msgDate := time.UnixMilli(raw.InternalDate)
				if result.oldestDate.IsZero() || msgDate.Before(result.oldestDate) {
					result.oldestDate = msgDate
				}
			}

			threadID := threadIDs[newIDs[i]]
			if err := s.ingestMessage(ctx, sourceID, raw, threadID, labelMap); err != nil {
				s.logger.Warn("failed to ingest message", "id", raw.ID, "error", err)
				checkpoint.ErrorsCount++
				continue
			}

			result.added++
			summary.BytesDownloaded += int64(len(raw.Raw))
		}
	}

	return result, nil
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

	// Initialize sync state (resume or start new)
	state, err := s.initSyncState(source.ID)
	if err != nil {
		return nil, err
	}
	summary.WasResumed = state.wasResumed
	summary.ResumedFromToken = state.pageToken

	// Defer failure handling
	defer func() {
		if r := recover(); r != nil {
			_ = s.store.FailSync(state.syncID, fmt.Sprintf("panic: %v", r))
			panic(r)
		}
	}()

	// Get profile to verify connection and get historyId
	profile, err := s.client.GetProfile(ctx)
	if err != nil {
		_ = s.store.FailSync(state.syncID, err.Error())
		return nil, fmt.Errorf("get profile: %w", err)
	}

	s.logger.Info("syncing account", "email", profile.EmailAddress, "messages", profile.MessagesTotal)

	// Sync labels
	labelMap, err := s.syncLabels(ctx, source.ID)
	if err != nil {
		_ = s.store.FailSync(state.syncID, err.Error())
		return nil, fmt.Errorf("sync labels: %w", err)
	}

	// List and sync messages
	var totalEstimate int64
	firstPage := true
	pageToken := state.pageToken

	for {
		// List messages
		listResp, err := s.client.ListMessages(ctx, s.opts.Query, pageToken)
		if err != nil {
			_ = s.store.FailSync(state.syncID, err.Error())
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

		// Process batch
		result, err := s.processBatch(ctx, source.ID, listResp, labelMap, state.checkpoint, summary)
		if err != nil {
			_ = s.store.FailSync(state.syncID, err.Error())
			return nil, err
		}

		state.checkpoint.MessagesProcessed += result.processed
		state.checkpoint.MessagesAdded += result.added

		// Report current position date before progress (so UI shows consistent state)
		if !result.oldestDate.IsZero() {
			if p, ok := s.progress.(gmail.SyncProgressWithDate); ok {
				p.OnLatestDate(result.oldestDate)
			}
		}

		// Report progress
		s.progress.OnProgress(state.checkpoint.MessagesProcessed, state.checkpoint.MessagesAdded, result.skipped)

		// Save checkpoint
		pageToken = listResp.NextPageToken
		state.checkpoint.PageToken = pageToken
		if err := s.store.UpdateSyncCheckpoint(state.syncID, state.checkpoint); err != nil {
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
	if err := s.store.CompleteSync(state.syncID, historyIDStr); err != nil {
		s.logger.Warn("failed to complete sync", "error", err)
	}

	// Build summary
	summary.EndTime = time.Now()
	summary.Duration = summary.EndTime.Sub(summary.StartTime)
	summary.MessagesFound = state.checkpoint.MessagesProcessed
	summary.MessagesAdded = state.checkpoint.MessagesAdded
	summary.MessagesUpdated = state.checkpoint.MessagesUpdated
	summary.MessagesSkipped = state.checkpoint.MessagesProcessed - state.checkpoint.MessagesAdded - state.checkpoint.MessagesUpdated
	summary.Errors = state.checkpoint.ErrorsCount
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

	labelInfos := make(map[string]store.LabelInfo)
	for _, l := range labels {
		labelType := "user"
		if store.IsSystemLabel(l.ID) {
			labelType = "system"
		}
		labelInfos[l.ID] = store.LabelInfo{Name: l.Name, Type: labelType}
	}

	return s.store.EnsureLabelsBatch(sourceID, labelInfos)
}

// messageData holds all parsed data for a message before persistence.
type messageData struct {
	message        *store.Message
	bodyText       string
	bodyHTML       string
	rawMIME        []byte
	from           []mime.Address
	to             []mime.Address
	cc             []mime.Address
	bcc            []mime.Address
	gmailLabelIDs  []string
	attachments    []mime.Attachment
	participantMap map[string]int64
}

// parseToModel parses a raw Gmail message into a messageData struct.
func (s *Syncer) parseToModel(sourceID int64, raw *gmail.RawMessage, threadID string) (*messageData, error) {
	// Validate raw MIME data exists
	if len(raw.Raw) == 0 {
		return nil, fmt.Errorf("missing raw MIME data for message %s", raw.ID)
	}

	// Fall back to raw.ThreadID if list response threadID is missing,
	// then to message ID as last resort
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
		errMsg := textutil.FirstLine(parseErr.Error())

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

	// Ensure all text fields are valid UTF-8
	subject := textutil.EnsureUTF8(parsed.Subject)
	bodyText := textutil.EnsureUTF8(parsed.GetBodyText())
	bodyHTML := textutil.EnsureUTF8(parsed.BodyHTML)
	snippet := textutil.EnsureUTF8(raw.Snippet)

	// Ensure participants exist in database
	allAddresses := append(append(append(parsed.From, parsed.To...), parsed.Cc...), parsed.Bcc...)
	participantMap, err := s.store.EnsureParticipantsBatch(allAddresses)
	if err != nil {
		return nil, fmt.Errorf("ensure participants: %w", err)
	}

	// Get sender ID
	var senderID sql.NullInt64
	if len(parsed.From) > 0 && parsed.From[0].Email != "" {
		if id, ok := participantMap[parsed.From[0].Email]; ok {
			senderID = sql.NullInt64{Int64: id, Valid: true}
		}
	}

	// Use placeholder for conversation matching only (subject can be empty for storage)
	convSubject := subject
	if convSubject == "" {
		convSubject = "(no subject)"
	}
	conversationID, err := s.store.EnsureConversation(sourceID, threadID, convSubject)
	if err != nil {
		return nil, fmt.Errorf("ensure conversation: %w", err)
	}

	// Build message record
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
		msg.SentAt = sql.NullTime{Time: parsed.Date, Valid: true}
	} else if msg.InternalDate.Valid {
		// Fall back to InternalDate if Date header couldn't be parsed
		msg.SentAt = msg.InternalDate
	}

	return &messageData{
		message:        msg,
		bodyText:       bodyText,
		bodyHTML:       bodyHTML,
		rawMIME:        raw.Raw,
		from:           parsed.From,
		to:             parsed.To,
		cc:             parsed.Cc,
		bcc:            parsed.Bcc,
		gmailLabelIDs:  raw.LabelIDs,
		attachments:    parsed.Attachments,
		participantMap: participantMap,
	}, nil
}

// persistMessage stores a parsed message and all related data.
func (s *Syncer) persistMessage(data *messageData, labelMap map[string]int64) error {
	// Upsert message
	messageID, err := s.store.UpsertMessage(data.message)
	if err != nil {
		return fmt.Errorf("upsert message: %w", err)
	}

	// Store message body in separate table
	if err := s.store.UpsertMessageBody(messageID,
		sql.NullString{String: data.bodyText, Valid: data.bodyText != ""},
		sql.NullString{String: data.bodyHTML, Valid: data.bodyHTML != ""},
	); err != nil {
		return fmt.Errorf("upsert message body: %w", err)
	}

	// Store raw MIME
	if err := s.store.UpsertMessageRaw(messageID, data.rawMIME); err != nil {
		return fmt.Errorf("store raw: %w", err)
	}

	// Store recipients
	if err := s.storeRecipients(messageID, "from", data.from, data.participantMap); err != nil {
		return fmt.Errorf("store from: %w", err)
	}
	if err := s.storeRecipients(messageID, "to", data.to, data.participantMap); err != nil {
		return fmt.Errorf("store to: %w", err)
	}
	if err := s.storeRecipients(messageID, "cc", data.cc, data.participantMap); err != nil {
		return fmt.Errorf("store cc: %w", err)
	}
	if err := s.storeRecipients(messageID, "bcc", data.bcc, data.participantMap); err != nil {
		return fmt.Errorf("store bcc: %w", err)
	}

	// Store labels
	var labelIDs []int64
	for _, gmailLabelID := range data.gmailLabelIDs {
		if internalID, ok := labelMap[gmailLabelID]; ok {
			labelIDs = append(labelIDs, internalID)
		}
	}
	if err := s.store.ReplaceMessageLabels(messageID, labelIDs); err != nil {
		return fmt.Errorf("store labels: %w", err)
	}

	// Store attachments
	if s.opts.AttachmentsDir != "" {
		for _, att := range data.attachments {
			if err := s.storeAttachment(messageID, &att); err != nil {
				s.logger.Warn("failed to store attachment", "message", messageID, "filename", att.Filename, "error", err)
			}
		}
	}

	return nil
}

// ingestMessage parses and stores a single message.
func (s *Syncer) ingestMessage(ctx context.Context, sourceID int64, raw *gmail.RawMessage, threadID string, labelMap map[string]int64) error {
	data, err := s.parseToModel(sourceID, raw, threadID)
	if err != nil {
		return err
	}

	return s.persistMessage(data, labelMap)
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
			name := textutil.EnsureUTF8(addr.Name)
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
	line := textutil.FirstLine(snippet)
	return textutil.TruncateRunes(line, 80)
}
