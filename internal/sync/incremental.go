package sync

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/wesm/msgvault/internal/gmail"
	"github.com/wesm/msgvault/internal/store"
)

// Incremental performs an incremental sync using the Gmail History API.
// Falls back to full sync if history is too old (404 error).
func (s *Syncer) Incremental(ctx context.Context, email string) (*gmail.SyncSummary, error) {
	startTime := time.Now()
	summary := &gmail.SyncSummary{StartTime: startTime}

	// Get source - must already exist for incremental sync
	source, err := s.store.GetSourceByIdentifier(email)
	if err != nil {
		return nil, fmt.Errorf("get source: %w", err)
	}
	if source == nil {
		return nil, fmt.Errorf("no source found for %s - run full sync first", email)
	}

	// Get last history ID
	if !source.SyncCursor.Valid || source.SyncCursor.String == "" {
		return nil, fmt.Errorf("no history ID for %s - run full sync first", email)
	}

	startHistoryID, err := strconv.ParseUint(source.SyncCursor.String, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid history ID %q: %w", source.SyncCursor.String, err)
	}

	// Start sync
	syncID, err := s.store.StartSync(source.ID, "incremental")
	if err != nil {
		return nil, fmt.Errorf("start sync: %w", err)
	}

	// Defer failure handling
	defer func() {
		if r := recover(); r != nil {
			_ = s.store.FailSync(syncID, fmt.Sprintf("panic: %v", r))
			panic(r)
		}
	}()

	// Get profile for current history ID
	profile, err := s.client.GetProfile(ctx)
	if err != nil {
		_ = s.store.FailSync(syncID, err.Error())
		return nil, fmt.Errorf("get profile: %w", err)
	}

	s.logger.Info("incremental sync", "email", email, "start_history", startHistoryID, "current_history", profile.HistoryID)

	// If history IDs match, nothing to do
	if startHistoryID >= profile.HistoryID {
		s.logger.Info("already up to date")
		_ = s.store.CompleteSync(syncID, strconv.FormatUint(profile.HistoryID, 10))
		summary.EndTime = time.Now()
		summary.Duration = summary.EndTime.Sub(summary.StartTime)
		summary.FinalHistoryID = profile.HistoryID
		return summary, nil
	}

	// Sync labels first (new labels may have been created)
	labelMap, err := s.syncLabels(ctx, source.ID)
	if err != nil {
		_ = s.store.FailSync(syncID, err.Error())
		return nil, fmt.Errorf("sync labels: %w", err)
	}

	// Process history
	checkpoint := &store.Checkpoint{}
	pageToken := ""

	for {
		historyResp, err := s.client.ListHistory(ctx, startHistoryID, pageToken)
		if err != nil {
			// Check for 404 - history too old
			var notFound *gmail.NotFoundError
			if errors.As(err, &notFound) {
				s.logger.Warn("history too old, falling back to full sync")
				_ = s.store.FailSync(syncID, "history too old")
				// Caller should trigger full sync
				return nil, ErrHistoryExpired
			}
			_ = s.store.FailSync(syncID, err.Error())
			return nil, fmt.Errorf("list history: %w", err)
		}

		// Process each history record
		for _, record := range historyResp.History {
			s.processMessagesAdded(ctx, source.ID, record.MessagesAdded, labelMap, checkpoint, summary)
			s.processMessagesDeleted(source.ID, record.MessagesDeleted, checkpoint)
			s.processLabelChanges(ctx, source.ID, record, labelMap)
			checkpoint.MessagesProcessed++
		}

		// Report progress
		s.progress.OnProgress(checkpoint.MessagesProcessed, checkpoint.MessagesAdded, 0)

		// Save checkpoint
		pageToken = historyResp.NextPageToken
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
	summary.Errors = checkpoint.ErrorsCount
	summary.FinalHistoryID = profile.HistoryID

	s.progress.OnComplete(summary)
	return summary, nil
}

// processMessagesAdded fetches and ingests newly added messages.
func (s *Syncer) processMessagesAdded(ctx context.Context, sourceID int64, added []gmail.HistoryMessage, labelMap map[string]int64, checkpoint *store.Checkpoint, summary *gmail.SyncSummary) {
	for _, msg := range added {
		raw, err := s.client.GetMessageRaw(ctx, msg.Message.ID)
		if err != nil {
			var notFound *gmail.NotFoundError
			if errors.As(err, &notFound) {
				// Message was deleted before we could fetch it
				continue
			}
			s.logger.Warn("failed to fetch added message", "id", msg.Message.ID, "error", err)
			checkpoint.ErrorsCount++
			continue
		}

		if err := s.ingestMessage(ctx, sourceID, raw, msg.Message.ThreadID, labelMap); err != nil {
			s.logger.Warn("failed to ingest added message", "id", msg.Message.ID, "error", err)
			checkpoint.ErrorsCount++
			continue
		}

		checkpoint.MessagesAdded++
		summary.BytesDownloaded += int64(len(raw.Raw))
	}
}

// processMessagesDeleted marks deleted messages in the local store.
func (s *Syncer) processMessagesDeleted(sourceID int64, deleted []gmail.HistoryMessage, checkpoint *store.Checkpoint) {
	for _, msg := range deleted {
		if err := s.store.MarkMessageDeleted(sourceID, msg.Message.ID); err != nil {
			s.logger.Warn("failed to mark message deleted", "id", msg.Message.ID, "error", err)
			checkpoint.ErrorsCount++
		}
	}
}

// processLabelChanges handles label additions and removals for messages.
func (s *Syncer) processLabelChanges(ctx context.Context, sourceID int64, record gmail.HistoryRecord, labelMap map[string]int64) {
	for _, item := range record.LabelsAdded {
		if err := s.handleLabelChange(ctx, sourceID, item.Message.ID, item.Message.ThreadID, item.LabelIDs, labelMap, true); err != nil {
			s.logLabelChangeError("add", item.Message.ID, err)
		}
	}
	for _, item := range record.LabelsRemoved {
		if err := s.handleLabelChange(ctx, sourceID, item.Message.ID, item.Message.ThreadID, item.LabelIDs, labelMap, false); err != nil {
			s.logLabelChangeError("remove", item.Message.ID, err)
		}
	}
}

// handleLabelChange processes a label addition or removal.
// If the message doesn't exist locally, it may need to be fetched.
func (s *Syncer) handleLabelChange(ctx context.Context, sourceID int64, messageID, threadID string, gmailLabelIDs []string, labelMap map[string]int64, isAdd bool) error {
	// Check if message exists
	existing, err := s.store.MessageExistsBatch(sourceID, []string{messageID})
	if err != nil {
		return err
	}

	internalID, exists := existing[messageID]

	if !exists {
		// Message doesn't exist locally - if adding labels, we should fetch it
		if isAdd {
			raw, err := s.client.GetMessageRaw(ctx, messageID)
			if err != nil {
				return err
			}
			return s.ingestMessage(ctx, sourceID, raw, threadID, labelMap)
		}
		// Removing labels from non-existent message is a no-op
		return nil
	}

	// Get current labels
	// For simplicity, we'll just re-fetch and update all labels
	// A more efficient approach would track individual adds/removes
	raw, err := s.client.GetMessageRaw(ctx, messageID)
	if err != nil {
		return err
	}

	// Convert Gmail label IDs to internal IDs
	var labelIDs []int64
	for _, gmailID := range raw.LabelIDs {
		if id, ok := labelMap[gmailID]; ok {
			labelIDs = append(labelIDs, id)
		}
	}

	return s.store.ReplaceMessageLabels(internalID, labelIDs)
}

// logLabelChangeError logs label change errors, downgrading "not found"
// to a debug-level message since deleted messages are expected during
// incremental sync (e.g., spam auto-deleted between sync runs).
func (s *Syncer) logLabelChangeError(action, messageID string, err error) {
	var notFound *gmail.NotFoundError
	if errors.As(err, &notFound) {
		s.logger.Debug("skipping label "+action+": message deleted from Gmail", "id", messageID)
	} else {
		s.logger.Warn("failed to handle label "+action, "id", messageID, "error", err)
	}
}
