package gmail

import (
	"context"
	"fmt"
	"sync"
)

// Operation constants for call tracking.
const (
	OpTrash       = "trash"
	OpDelete      = "delete"
	OpBatchDelete = "batch_delete"
)

// DeletionMockAPI is a mock Gmail API specifically designed for testing deletion
// scenarios with comprehensive error injection and call tracking.
type DeletionMockAPI struct {
	mu sync.Mutex

	// Per-message error injection for Trash operations
	// Key: messageID, Value: error to return (or nil for success)
	TrashErrors map[string]error

	// Per-message error injection for Delete operations
	DeleteErrors map[string]error

	// Batch delete error - returned when BatchDeleteMessages is called
	BatchDeleteError error

	// Transient errors - fail N times then succeed
	// Key: messageID, Value: number of failures remaining
	TransientTrashFailures  map[string]int
	TransientDeleteFailures map[string]int

	// Rate limit simulation
	RateLimitAfterCalls int // Trigger rate limit after this many calls (0 = disabled)
	RateLimitDuration   int // Seconds to suggest retry (for 429 Retry-After)
	rateLimitCallCount  int // protected by mu

	// Call tracking
	TrashCalls       []string   // Message IDs passed to TrashMessage
	DeleteCalls      []string   // Message IDs passed to DeleteMessage
	BatchDeleteCalls [][]string // Batches passed to BatchDeleteMessages

	// Call sequence tracking (for verifying retry behavior)
	CallSequence []DeletionCall

	// Hooks for custom behavior
	BeforeTrash       func(messageID string) error // Called before processing trash
	BeforeDelete      func(messageID string) error // Called before processing delete
	BeforeBatchDelete func(messageIDs []string) error
}

// DeletionCall represents a single API call for sequence tracking.
type DeletionCall struct {
	Operation string   // OpTrash, OpDelete, or OpBatchDelete
	MessageID string   // For single operations
	BatchIDs  []string // For batch operations
	Error     error    // Error returned (nil for success)
}

// NewDeletionMockAPI creates a new deletion mock with empty state.
func NewDeletionMockAPI() *DeletionMockAPI {
	return &DeletionMockAPI{
		TrashErrors:             make(map[string]error),
		DeleteErrors:            make(map[string]error),
		TransientTrashFailures:  make(map[string]int),
		TransientDeleteFailures: make(map[string]int),
	}
}

// TrashMessage simulates trashing a message with error injection.
func (m *DeletionMockAPI) TrashMessage(ctx context.Context, messageID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.checkRateLimit(); err != nil {
		m.recordCall(OpTrash, messageID, nil, err)
		return err
	}

	if m.BeforeTrash != nil {
		if err := m.BeforeTrash(messageID); err != nil {
			m.recordCall(OpTrash, messageID, nil, err)
			return err
		}
	}

	m.TrashCalls = append(m.TrashCalls, messageID)

	err := m.checkErrors(messageID, m.TransientTrashFailures, m.TrashErrors)
	m.recordCall(OpTrash, messageID, nil, err)
	return err
}

// DeleteMessage simulates permanently deleting a message with error injection.
func (m *DeletionMockAPI) DeleteMessage(ctx context.Context, messageID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.checkRateLimit(); err != nil {
		m.recordCall(OpDelete, messageID, nil, err)
		return err
	}

	if m.BeforeDelete != nil {
		if err := m.BeforeDelete(messageID); err != nil {
			m.recordCall(OpDelete, messageID, nil, err)
			return err
		}
	}

	m.DeleteCalls = append(m.DeleteCalls, messageID)

	err := m.checkErrors(messageID, m.TransientDeleteFailures, m.DeleteErrors)
	m.recordCall(OpDelete, messageID, nil, err)
	return err
}

// checkErrors checks transient and permanent error maps for a message.
// Must be called with mutex held.
func (m *DeletionMockAPI) checkErrors(messageID string, transientFailures map[string]int, permanentErrors map[string]error) error {
	if failures, ok := transientFailures[messageID]; ok && failures > 0 {
		transientFailures[messageID] = failures - 1
		return fmt.Errorf("transient error (retries remaining: %d)", failures-1)
	}
	if err, ok := permanentErrors[messageID]; ok {
		return err
	}
	return nil
}

// BatchDeleteMessages simulates batch deletion with error injection.
func (m *DeletionMockAPI) BatchDeleteMessages(ctx context.Context, messageIDs []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check rate limit
	if err := m.checkRateLimit(); err != nil {
		m.recordCall(OpBatchDelete, "", messageIDs, err)
		return err
	}

	// Run hook if set
	if m.BeforeBatchDelete != nil {
		if err := m.BeforeBatchDelete(messageIDs); err != nil {
			m.recordCall(OpBatchDelete, "", messageIDs, err)
			return err
		}
	}

	m.BatchDeleteCalls = append(m.BatchDeleteCalls, messageIDs)

	if m.BatchDeleteError != nil {
		m.recordCall(OpBatchDelete, "", messageIDs, m.BatchDeleteError)
		return m.BatchDeleteError
	}

	m.recordCall(OpBatchDelete, "", messageIDs, nil)
	return nil
}

// checkRateLimit checks if a rate limit should be triggered.
// Must be called with mutex held.
func (m *DeletionMockAPI) checkRateLimit() error {
	if m.RateLimitAfterCalls <= 0 {
		return nil
	}

	m.rateLimitCallCount++
	if m.rateLimitCallCount > m.RateLimitAfterCalls {
		return &RateLimitError{
			RetryAfter: m.RateLimitDuration,
		}
	}
	return nil
}

// recordCall records a call to the sequence log.
// Must be called with mutex held.
func (m *DeletionMockAPI) recordCall(op, messageID string, batchIDs []string, err error) {
	m.CallSequence = append(m.CallSequence, DeletionCall{
		Operation: op,
		MessageID: messageID,
		BatchIDs:  batchIDs,
		Error:     err,
	})
}

// RateLimitError represents a rate limit response.
type RateLimitError struct {
	RetryAfter int // Seconds to wait before retry
}

func (e *RateLimitError) Error() string {
	return fmt.Sprintf("rate limited, retry after %d seconds", e.RetryAfter)
}

// SetNotFoundError sets a 404 error for a message ID.
func (m *DeletionMockAPI) SetNotFoundError(messageID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	err := &NotFoundError{Path: "/users/me/messages/" + messageID}
	m.TrashErrors[messageID] = err
	m.DeleteErrors[messageID] = err
}

// SetTransientFailure sets a message to fail N times then succeed.
func (m *DeletionMockAPI) SetTransientFailure(messageID string, failCount int, forTrash bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if forTrash {
		m.TransientTrashFailures[messageID] = failCount
	} else {
		m.TransientDeleteFailures[messageID] = failCount
	}
}

// Reset clears all state and call tracking.
func (m *DeletionMockAPI) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.TrashErrors = make(map[string]error)
	m.DeleteErrors = make(map[string]error)
	m.TransientTrashFailures = make(map[string]int)
	m.TransientDeleteFailures = make(map[string]int)
	m.BatchDeleteError = nil
	m.RateLimitAfterCalls = 0
	m.RateLimitDuration = 0
	m.rateLimitCallCount = 0

	m.TrashCalls = nil
	m.DeleteCalls = nil
	m.BatchDeleteCalls = nil
	m.CallSequence = nil

	m.BeforeTrash = nil
	m.BeforeDelete = nil
	m.BeforeBatchDelete = nil
}

// GetTrashCallCount returns the number of trash calls for a message.
func (m *DeletionMockAPI) GetTrashCallCount(messageID string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	count := 0
	for _, id := range m.TrashCalls {
		if id == messageID {
			count++
		}
	}
	return count
}

// GetDeleteCallCount returns the number of delete calls for a message.
func (m *DeletionMockAPI) GetDeleteCallCount(messageID string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	count := 0
	for _, id := range m.DeleteCalls {
		if id == messageID {
			count++
		}
	}
	return count
}

// Stub out the other API methods required by the interface.
// These panic if called since DeletionMockAPI is only for deletion testing.

func (m *DeletionMockAPI) GetProfile(ctx context.Context) (*Profile, error) {
	panic("DeletionMockAPI.GetProfile not implemented - use MockAPI for full API testing")
}

func (m *DeletionMockAPI) ListLabels(ctx context.Context) ([]*Label, error) {
	panic("DeletionMockAPI.ListLabels not implemented - use MockAPI for full API testing")
}

func (m *DeletionMockAPI) ListMessages(ctx context.Context, query string, pageToken string) (*MessageListResponse, error) {
	panic("DeletionMockAPI.ListMessages not implemented - use MockAPI for full API testing")
}

func (m *DeletionMockAPI) GetMessageRaw(ctx context.Context, messageID string) (*RawMessage, error) {
	panic("DeletionMockAPI.GetMessageRaw not implemented - use MockAPI for full API testing")
}

func (m *DeletionMockAPI) GetMessagesRawBatch(ctx context.Context, messageIDs []string) ([]*RawMessage, error) {
	panic("DeletionMockAPI.GetMessagesRawBatch not implemented - use MockAPI for full API testing")
}

func (m *DeletionMockAPI) ListHistory(ctx context.Context, startHistoryID uint64, pageToken string) (*HistoryResponse, error) {
	panic("DeletionMockAPI.ListHistory not implemented - use MockAPI for full API testing")
}

func (m *DeletionMockAPI) Close() error {
	return nil
}

// Ensure DeletionMockAPI implements API interface.
var _ API = (*DeletionMockAPI)(nil)
