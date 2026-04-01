package tui

import "github.com/wesm/msgvault/internal/query"

// tuiMode represents the top-level mode: Email or Texts.
type tuiMode int

const (
	modeEmail tuiMode = iota
	modeTexts
)

// textViewLevel represents the navigation depth within Texts mode.
type textViewLevel int

const (
	textLevelConversations      textViewLevel = iota // Top-level conversation list
	textLevelAggregate                               // Aggregate view (contacts, sources, etc.)
	textLevelDrillConversations                      // Conversations filtered by aggregate drill-down
	textLevelTimeline                                // Message timeline within a conversation
)

// textState holds all state for the Texts mode TUI.
type textState struct {
	viewType       query.TextViewType
	level          textViewLevel
	conversations  []query.ConversationRow
	aggregateRows  []query.AggregateRow
	messages       []query.MessageSummary
	cursor         int
	scrollOffset   int
	selectedConvID int64
	filter         query.TextFilter
	stats          *query.TotalStats
	breadcrumbs    []textNavSnapshot
}

// textNavSnapshot stores state for text mode navigation history.
type textNavSnapshot struct {
	level          textViewLevel
	viewType       query.TextViewType
	cursor         int
	scrollOffset   int
	filter         query.TextFilter
	selectedConvID int64
}
