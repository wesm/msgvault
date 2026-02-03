package deletion

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/wesm/msgvault/internal/gmail"
	"github.com/wesm/msgvault/internal/store"
)

// isNotFoundError checks if an error indicates the message was already deleted.
// Treating 404 as success makes deletion idempotent.
func isNotFoundError(err error) bool {
	var notFound *gmail.NotFoundError
	return errors.As(err, &notFound)
}

// isInsufficientScopeError checks if an error is due to missing OAuth scopes.
func isInsufficientScopeError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "ACCESS_TOKEN_SCOPE_INSUFFICIENT") ||
		strings.Contains(msg, "insufficient authentication scopes") ||
		strings.Contains(msg, "Insufficient Permission")
}

// Progress reports deletion progress.
type Progress interface {
	OnStart(total int)
	OnProgress(processed, succeeded, failed int)
	OnComplete(succeeded, failed int)
}

// NullProgress is a no-op progress reporter.
type NullProgress struct{}

func (NullProgress) OnStart(total int)                           {}
func (NullProgress) OnProgress(processed, succeeded, failed int) {}
func (NullProgress) OnComplete(succeeded, failed int)            {}

// Executor performs deletion operations.
type Executor struct {
	manager  *Manager
	store    *store.Store
	client   gmail.API
	logger   *slog.Logger
	progress Progress
}

// NewExecutor creates a deletion executor.
func NewExecutor(manager *Manager, store *store.Store, client gmail.API) *Executor {
	return &Executor{
		manager:  manager,
		store:    store,
		client:   client,
		logger:   slog.Default(),
		progress: NullProgress{},
	}
}

// WithLogger sets the logger.
func (e *Executor) WithLogger(logger *slog.Logger) *Executor {
	e.logger = logger
	return e
}

// WithProgress sets the progress reporter.
func (e *Executor) WithProgress(p Progress) *Executor {
	e.progress = p
	return e
}

// ExecuteOptions configures deletion execution.
type ExecuteOptions struct {
	Method    Method // Trash or permanent delete
	BatchSize int    // Messages per batch for batch delete API
	Resume    bool   // Resume from last checkpoint
}

// DefaultExecuteOptions returns sensible defaults.
func DefaultExecuteOptions() *ExecuteOptions {
	return &ExecuteOptions{
		Method:    MethodTrash,
		BatchSize: 100, // Gmail batch delete supports up to 1000
		Resume:    true,
	}
}

// Execute performs the deletion for a manifest.
func (e *Executor) Execute(ctx context.Context, manifestID string, opts *ExecuteOptions) error {
	if opts == nil {
		opts = DefaultExecuteOptions()
	}

	// Load manifest
	manifest, _, err := e.manager.GetManifest(manifestID)
	if err != nil {
		return fmt.Errorf("load manifest: %w", err)
	}

	// Check status
	if manifest.Status != StatusPending && manifest.Status != StatusInProgress {
		return fmt.Errorf("manifest %s is %s, cannot execute", manifestID, manifest.Status)
	}

	// Move to in_progress if pending
	if manifest.Status == StatusPending {
		if err := e.manager.MoveManifest(manifestID, StatusPending, StatusInProgress); err != nil {
			return fmt.Errorf("move to in_progress: %w", err)
		}
		manifest.Status = StatusInProgress

		// Initialize execution
		manifest.Execution = &Execution{
			StartedAt: time.Now(),
			Method:    opts.Method,
		}
	}

	// Path is always in in_progress location during execution
	path := e.manager.InProgressDir() + "/" + manifestID + ".json"

	// Determine starting point
	startIndex := 0
	if opts.Resume && manifest.Execution != nil {
		startIndex = manifest.Execution.LastProcessedIndex
	}

	e.logger.Info("executing deletion",
		"manifest", manifestID,
		"total", len(manifest.GmailIDs),
		"start_index", startIndex,
		"method", opts.Method,
	)

	e.progress.OnStart(len(manifest.GmailIDs))

	// Execute deletions
	succeeded := manifest.Execution.Succeeded
	failed := manifest.Execution.Failed
	failedIDs := manifest.Execution.FailedIDs

	for i := startIndex; i < len(manifest.GmailIDs); i++ {
		select {
		case <-ctx.Done():
			// Interrupted - save checkpoint
			manifest.Execution.LastProcessedIndex = i
			manifest.Execution.Succeeded = succeeded
			manifest.Execution.Failed = failed
			manifest.Execution.FailedIDs = failedIDs
			if err := manifest.Save(path); err != nil {
				e.logger.Warn("failed to save checkpoint", "error", err)
			}
			return ctx.Err()
		default:
		}

		gmailID := manifest.GmailIDs[i]

		var err error
		if opts.Method == MethodTrash {
			err = e.client.TrashMessage(ctx, gmailID)
		} else {
			err = e.client.DeleteMessage(ctx, gmailID)
		}

		if err != nil {
			// Treat 404 (already deleted) as success - makes deletion idempotent
			if isNotFoundError(err) {
				e.logger.Debug("message already deleted", "gmail_id", gmailID)
				succeeded++
				// Mark as deleted in local database even if already gone from server
				if markErr := e.store.MarkMessageDeletedByGmailID(manifest.Execution.Method == MethodDelete, gmailID); markErr != nil {
					e.logger.Warn("failed to mark deleted in DB", "gmail_id", gmailID, "error", markErr)
				}
			} else if isInsufficientScopeError(err) {
				// Scope errors should propagate immediately — every subsequent
				// message will fail for the same reason. Save checkpoint first.
				manifest.Execution.LastProcessedIndex = i
				manifest.Execution.Succeeded = succeeded
				manifest.Execution.Failed = failed
				manifest.Execution.FailedIDs = failedIDs
				if saveErr := manifest.Save(path); saveErr != nil {
					e.logger.Warn("failed to save checkpoint", "error", saveErr)
				}
				return fmt.Errorf("delete message: %w", err)
			} else {
				e.logger.Warn("failed to delete message", "gmail_id", gmailID, "error", err)
				failed++
				failedIDs = append(failedIDs, gmailID)
			}
		} else {
			succeeded++
			// Mark as deleted in local database
			if markErr := e.store.MarkMessageDeletedByGmailID(manifest.Execution.Method == MethodDelete, gmailID); markErr != nil {
				e.logger.Warn("failed to mark deleted in DB", "gmail_id", gmailID, "error", markErr)
			}
		}

		// Save checkpoint periodically
		if (i+1)%opts.BatchSize == 0 {
			manifest.Execution.LastProcessedIndex = i + 1
			manifest.Execution.Succeeded = succeeded
			manifest.Execution.Failed = failed
			manifest.Execution.FailedIDs = failedIDs
			if err := manifest.Save(path); err != nil {
				e.logger.Warn("failed to save checkpoint", "error", err)
			}
			e.progress.OnProgress(i+1, succeeded, failed)
		}
	}

	// Mark complete
	now := time.Now()
	manifest.Execution.CompletedAt = &now
	manifest.Execution.LastProcessedIndex = len(manifest.GmailIDs)
	manifest.Execution.Succeeded = succeeded
	manifest.Execution.Failed = failed
	manifest.Execution.FailedIDs = failedIDs

	// Move to completed or failed
	var targetStatus Status
	if failed == 0 {
		targetStatus = StatusCompleted
	} else if succeeded == 0 {
		targetStatus = StatusFailed
	} else {
		// Partial success - still mark as completed but keep failed IDs
		targetStatus = StatusCompleted
	}

	manifest.Status = targetStatus
	if err := manifest.Save(path); err != nil {
		e.logger.Warn("failed to save final state", "error", err)
	}

	if err := e.manager.MoveManifest(manifestID, StatusInProgress, targetStatus); err != nil {
		e.logger.Warn("failed to move manifest", "error", err)
	}

	e.progress.OnComplete(succeeded, failed)

	e.logger.Info("deletion complete",
		"manifest", manifestID,
		"succeeded", succeeded,
		"failed", failed,
	)

	return nil
}

// ExecuteBatch performs batch deletion (more efficient but permanent).
func (e *Executor) ExecuteBatch(ctx context.Context, manifestID string) error {
	// Load manifest
	manifest, _, err := e.manager.GetManifest(manifestID)
	if err != nil {
		return fmt.Errorf("load manifest: %w", err)
	}

	if manifest.Status != StatusPending && manifest.Status != StatusInProgress {
		return fmt.Errorf("manifest %s is %s, cannot execute", manifestID, manifest.Status)
	}

	// Move to in_progress if pending
	if manifest.Status == StatusPending {
		if err := e.manager.MoveManifest(manifestID, StatusPending, StatusInProgress); err != nil {
			return fmt.Errorf("move to in_progress: %w", err)
		}
		manifest.Status = StatusInProgress

		// Initialize execution
		manifest.Execution = &Execution{
			StartedAt: time.Now(),
			Method:    MethodDelete, // Batch delete is permanent
		}
	} else {
		// Resuming in_progress
		if manifest.Execution == nil {
			manifest.Execution = &Execution{
				StartedAt: time.Now(),
				Method:    MethodDelete,
			}
		}
	}

	// Path is always in in_progress location during execution
	path := e.manager.InProgressDir() + "/" + manifestID + ".json"
	if err := manifest.Save(path); err != nil {
		return fmt.Errorf("save manifest: %w", err)
	}

	// Resume from checkpoint if available
	startIndex := 0
	succeeded := 0
	failed := 0
	var retryIDs []string
	if manifest.Execution != nil {
		startIndex = manifest.Execution.LastProcessedIndex
		succeeded = manifest.Execution.Succeeded
		// Retry previously failed IDs instead of carrying forward the count
		if len(manifest.Execution.FailedIDs) > 0 {
			retryIDs = manifest.Execution.FailedIDs
			// Don't carry forward the old failed count — we're retrying them
			failed = 0
			succeeded = manifest.Execution.Succeeded
		} else {
			failed = manifest.Execution.Failed
		}
	}

	// Bounds check to handle corrupted manifests
	if startIndex < 0 {
		startIndex = 0
	}
	if startIndex > len(manifest.GmailIDs) {
		startIndex = len(manifest.GmailIDs)
	}

	e.logger.Info("executing batch deletion",
		"manifest", manifestID,
		"total", len(manifest.GmailIDs),
		"start_index", startIndex,
		"retry_ids", len(retryIDs),
	)

	e.progress.OnStart(len(manifest.GmailIDs))

	var failedIDs []string

	// Retry previously failed IDs before continuing with remaining messages
	if len(retryIDs) > 0 {
		e.logger.Info("retrying previously failed messages", "count", len(retryIDs))
		for ri, gmailID := range retryIDs {
			if delErr := e.client.DeleteMessage(ctx, gmailID); delErr != nil {
				if isNotFoundError(delErr) {
					e.logger.Debug("message already deleted", "gmail_id", gmailID)
					succeeded++
					if markErr := e.store.MarkMessageDeletedByGmailID(true, gmailID); markErr != nil {
						e.logger.Warn("failed to mark deleted in DB", "gmail_id", gmailID, "error", markErr)
					}
				} else if isInsufficientScopeError(delErr) {
					// Save only unattempted + already-failed IDs
					remaining := append(failedIDs, retryIDs[ri:]...)
					manifest.Execution.LastProcessedIndex = startIndex
					manifest.Execution.Succeeded = succeeded
					manifest.Execution.Failed = len(remaining)
					manifest.Execution.FailedIDs = remaining
					if saveErr := manifest.Save(path); saveErr != nil {
						e.logger.Warn("failed to save checkpoint", "error", saveErr)
					}
					return fmt.Errorf("delete message: %w", delErr)
				} else {
					e.logger.Warn("retry failed", "gmail_id", gmailID, "error", delErr)
					failed++
					failedIDs = append(failedIDs, gmailID)
				}
			} else {
				succeeded++
				if markErr := e.store.MarkMessageDeletedByGmailID(true, gmailID); markErr != nil {
					e.logger.Warn("failed to mark deleted in DB", "gmail_id", gmailID, "error", markErr)
				}
			}
		}
		e.logger.Info("retry complete", "succeeded_now", succeeded-manifest.Execution.Succeeded, "still_failed", len(failedIDs))
	}

	// Execute in batches of 1000 (Gmail API limit)
	const batchSize = 1000

	for i := startIndex; i < len(manifest.GmailIDs); i += batchSize {
		select {
		case <-ctx.Done():
			// Save checkpoint
			manifest.Execution.LastProcessedIndex = i
			manifest.Execution.Succeeded = succeeded
			manifest.Execution.Failed = failed
			manifest.Execution.FailedIDs = failedIDs
			if err := manifest.Save(path); err != nil {
				e.logger.Warn("failed to save checkpoint", "error", err)
			}
			return ctx.Err()
		default:
		}

		end := i + batchSize
		if end > len(manifest.GmailIDs) {
			end = len(manifest.GmailIDs)
		}

		batch := manifest.GmailIDs[i:end]

		e.logger.Info("deleting batch", "start", i, "end", end, "size", len(batch))

		if err := e.client.BatchDeleteMessages(ctx, batch); err != nil {
			// If it's a permission/scope error, save checkpoint and return
			// immediately — falling back to individual deletes would fail
			// for the same reason.
			if isInsufficientScopeError(err) {
				manifest.Execution.LastProcessedIndex = i
				manifest.Execution.Succeeded = succeeded
				manifest.Execution.Failed = failed
				manifest.Execution.FailedIDs = failedIDs
				if saveErr := manifest.Save(path); saveErr != nil {
					e.logger.Warn("failed to save checkpoint", "error", saveErr)
				}
				return fmt.Errorf("batch delete: %w", err)
			}
			e.logger.Warn("batch delete failed, falling back to individual deletes", "start_index", i, "error", err)
			// Fall back to individual deletes
			for j, gmailID := range batch {
				if delErr := e.client.DeleteMessage(ctx, gmailID); delErr != nil {
					// Treat 404 (already deleted) as success - makes deletion idempotent
					if isNotFoundError(delErr) {
						e.logger.Debug("message already deleted", "gmail_id", gmailID)
						succeeded++
						if markErr := e.store.MarkMessageDeletedByGmailID(true, gmailID); markErr != nil {
							e.logger.Warn("failed to mark message as deleted in DB", "gmail_id", gmailID, "error", markErr)
						}
					} else if isInsufficientScopeError(delErr) {
						manifest.Execution.LastProcessedIndex = i + j
						manifest.Execution.Succeeded = succeeded
						manifest.Execution.Failed = failed
						manifest.Execution.FailedIDs = failedIDs
						if saveErr := manifest.Save(path); saveErr != nil {
							e.logger.Warn("failed to save checkpoint", "error", saveErr)
						}
						return fmt.Errorf("delete message: %w", delErr)
					} else {
						failed++
						failedIDs = append(failedIDs, gmailID)
					}
				} else {
					succeeded++
					if markErr := e.store.MarkMessageDeletedByGmailID(true, gmailID); markErr != nil {
						e.logger.Warn("failed to mark message as deleted in DB", "gmail_id", gmailID, "error", markErr)
					}
				}
				e.progress.OnProgress(i+j+1, succeeded, failed)
			}
		} else {
			succeeded += len(batch)
			// Mark all as deleted in DB
			for _, gmailID := range batch {
				if markErr := e.store.MarkMessageDeletedByGmailID(true, gmailID); markErr != nil {
					e.logger.Warn("failed to mark message as deleted in DB", "gmail_id", gmailID, "error", markErr)
				}
			}
		}

		e.progress.OnProgress(end, succeeded, failed)
	}

	// Mark complete
	now := time.Now()
	manifest.Execution.CompletedAt = &now
	manifest.Execution.Succeeded = succeeded
	manifest.Execution.Failed = failed
	manifest.Execution.FailedIDs = failedIDs

	var targetStatus Status
	if failed == 0 {
		targetStatus = StatusCompleted
	} else {
		targetStatus = StatusCompleted // Still completed, just with some failures
	}

	manifest.Status = targetStatus
	if err := manifest.Save(path); err != nil {
		e.logger.Warn("failed to save manifest", "manifest", manifestID, "error", err)
	}
	if err := e.manager.MoveManifest(manifestID, StatusInProgress, targetStatus); err != nil {
		e.logger.Warn("failed to move manifest", "manifest", manifestID, "error", err)
	}

	e.progress.OnComplete(succeeded, failed)

	e.logger.Info("batch deletion complete",
		"manifest", manifestID,
		"succeeded", succeeded,
		"failed", failed,
	)

	return nil
}
