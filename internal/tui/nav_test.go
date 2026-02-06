package tui

import (
	"testing"

	"github.com/wesm/msgvault/internal/query"
)

// =============================================================================
// Async Response Handling Tests
// =============================================================================

func TestStaleAsyncResponsesIgnored(t *testing.T) {
	model := NewBuilder().
		WithLevel(levelMessageList).
		Build()
	model.loadRequestID = 5 // Current request ID

	// Simulate a stale response with old request ID
	staleMsg := messagesLoadedMsg{
		messages:  []query.MessageSummary{{ID: 99, Subject: "Stale"}},
		requestID: 3, // Old request ID
	}

	m, _ := sendMsg(t, model, staleMsg)

	// Stale response should be ignored - messages should be unchanged (empty)
	if len(m.messages) != 0 {
		t.Errorf("stale response should be ignored, got %d messages", len(m.messages))
	}

	// Now send a valid response with current request ID
	validMsg := messagesLoadedMsg{
		messages:  []query.MessageSummary{{ID: 1, Subject: "Valid"}},
		requestID: 5, // Current request ID
	}

	m, _ = sendMsg(t, m, validMsg)

	// Valid response should be processed
	if len(m.messages) != 1 {
		t.Errorf("valid response should be processed, got %d messages", len(m.messages))
	}
	if m.messages[0].Subject != "Valid" {
		t.Errorf("expected subject 'Valid', got %s", m.messages[0].Subject)
	}
}

func TestStaleDetailResponsesIgnored(t *testing.T) {
	model := NewBuilder().
		WithLevel(levelMessageDetail).
		WithSize(100, 30).
		WithPageSize(20).
		Build()
	model.detailRequestID = 10 // Current request ID

	// Simulate a stale response with old request ID
	staleMsg := messageDetailLoadedMsg{
		detail:    &query.MessageDetail{ID: 99, Subject: "Stale Detail"},
		requestID: 8, // Old request ID
	}

	m, _ := sendMsg(t, model, staleMsg)

	// Stale response should be ignored
	if m.messageDetail != nil {
		t.Error("stale detail response should be ignored")
	}

	// Now send a valid response with current request ID
	validMsg := messageDetailLoadedMsg{
		detail:    &query.MessageDetail{ID: 1, Subject: "Valid Detail"},
		requestID: 10, // Current request ID
	}

	m, _ = sendMsg(t, m, validMsg)

	// Valid response should be processed
	if m.messageDetail == nil {
		t.Error("valid detail response should be processed")
	}
	if m.messageDetail.Subject != "Valid Detail" {
		t.Errorf("expected subject 'Valid Detail', got %s", m.messageDetail.Subject)
	}
}

// =============================================================================
// Window Size and Page Size Tests
// =============================================================================

func TestWindowSizeClampNegative(t *testing.T) {
	model := NewBuilder().Build()

	// Simulate negative window size (can happen during terminal resize)
	m := resizeModel(t, model, -1, -1)

	if m.width < 0 {
		t.Errorf("expected width >= 0, got %d", m.width)
	}
	if m.height < 0 {
		t.Errorf("expected height >= 0, got %d", m.height)
	}
	if m.pageSize < 1 {
		t.Errorf("expected pageSize >= 1, got %d", m.pageSize)
	}
}

func TestDefaultLoadingWithNoData(t *testing.T) {
	// Build with no rows/messages and no explicit loading override.
	// The builder should preserve New()'s default loading=true.
	model := NewBuilder().WithPageSize(10).WithSize(100, 20).Build()

	if !model.loading {
		t.Errorf("expected loading=true (New default) when no data provided, got false")
	}
}

func TestPageSizeRawZeroAndNegative(t *testing.T) {
	tests := []struct {
		name     string
		pageSize int
	}{
		{"zero page size", 0},
		{"negative page size", -1},
		{"large negative page size", -100},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Should not panic when building or rendering with raw zero/negative page sizes.
			model := NewBuilder().
				WithPageSizeRaw(tc.pageSize).
				WithRows(testAggregateRows...).
				WithSize(100, 20).
				Build()

			if model.pageSize != tc.pageSize {
				t.Errorf("expected pageSize=%d, got %d", tc.pageSize, model.pageSize)
			}

			// Rendering should not panic even with unusual page sizes.
			_ = model.View()
		})
	}
}

func TestWithPageSizeClearsRawFlag(t *testing.T) {
	// WithPageSizeRaw followed by WithPageSize should clear the raw flag,
	// so the normal clamping logic applies.
	model := NewBuilder().
		WithPageSizeRaw(0).
		WithPageSize(10).
		WithRows(testAggregateRows...).
		WithSize(100, 20).
		Build()

	if model.pageSize != 10 {
		t.Errorf("expected pageSize=10 after WithPageSize cleared raw flag, got %d", model.pageSize)
	}
}

// =============================================================================
// List Navigation Helper Tests
// =============================================================================

func TestNavigateList(t *testing.T) {
	tests := []struct {
		name        string
		key         string
		itemCount   int
		initCursor  int
		wantCursor  int
		wantHandled bool
	}{
		{"down from top", "j", 5, 0, 1, true},
		{"up from second", "k", 5, 1, 0, true},
		{"down at end", "j", 5, 4, 4, true},
		{"up at top", "k", 5, 0, 0, true},
		{"unhandled key", "x", 5, 0, 0, false},
		{"empty list down", "j", 0, 0, 0, true},
		{"empty list up", "k", 0, 0, 0, true},
		{"home", "home", 5, 3, 0, true},
		{"end", "end", 5, 0, 4, true},
		{"end empty list", "end", 0, 0, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := NewBuilder().WithRows(
				query.AggregateRow{Key: "a"},
			).Build()
			m.cursor = tt.initCursor

			handled := m.navigateList(tt.key, tt.itemCount)
			if handled != tt.wantHandled {
				t.Errorf("navigateList(%q, %d) handled = %v, want %v", tt.key, tt.itemCount, handled, tt.wantHandled)
			}
			if m.cursor != tt.wantCursor {
				t.Errorf("navigateList(%q, %d) cursor = %d, want %d", tt.key, tt.itemCount, m.cursor, tt.wantCursor)
			}
		})
	}
}
