package tui

import (
	"context"
	"fmt"
	"time"

	"github.com/wesm/msgvault/internal/deletion"
	"github.com/wesm/msgvault/internal/query"
)

// SearchState encapsulates search-related state.
type SearchState struct {
	Query       string
	Mode        SearchMode
	Filter      query.MessageFilter // Context filter applied to search
	TotalCount  int64               // Total matching messages
	Offset      int                 // Current offset for pagination
	LoadingMore bool                // True when loading additional results
	RequestID   uint64              // To detect stale responses
}

// AggregateModel manages state for aggregate views (senders, lists, etc.).
type AggregateModel struct {
	Rows            []query.AggregateRow
	ViewType        query.ViewType
	TimeGranularity query.TimeGranularity
	SortField       query.SortField
	SortDirection   query.SortDirection
	Cursor          int
	ScrollOffset    int
	RequestID       uint64
	// FilterKey is the aggregate key used to filter messages when drilling down
	FilterKey       string 
	// Selection tracks selected aggregate keys
	Selection       map[string]bool
}

// MessageListModel manages state for the message list view.
type MessageListModel struct {
	Messages      []query.MessageSummary
	Cursor        int
	ScrollOffset  int
	SortField     query.MessageSortField
	SortDirection query.SortDirection
	Filter        query.MessageFilter
	DrillFilter   query.MessageFilter // Filter from parent drill-down
	DrillViewType query.ViewType      // ViewType that created the drill filter
	AllMessages   bool                // True when showing all messages (not filtered by aggregate)
	RequestID     uint64
	// Selection tracks selected message IDs
	Selection     map[int64]bool
}

// ThreadModel manages state for the thread/conversation view.
type ThreadModel struct {
	Messages       []query.MessageSummary
	ConversationID int64
	Cursor         int
	ScrollOffset   int
	Truncated      bool
	RequestID      uint64
}

// DetailModel manages state for the single message detail view.
type DetailModel struct {
	Message       *query.MessageDetail
	Scroll        int
	LineCount     int
	MessageIndex  int  // Index in the parent list
	FromThread    bool // True if navigated from thread view
	PendingSubject string
	RequestID     uint64
}