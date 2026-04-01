package query

import "context"

// TextEngine provides query operations for text message data.
// This is a separate interface from Engine to avoid rippling text
// query methods through remote/API/MCP/mock layers.
// DuckDBEngine and SQLiteEngine implement both Engine and TextEngine.
type TextEngine interface {
	// ListConversations returns conversations matching the filter.
	ListConversations(ctx context.Context,
		filter TextFilter) ([]ConversationRow, error)

	// TextAggregate aggregates text messages by the given view type.
	TextAggregate(ctx context.Context, viewType TextViewType,
		opts TextAggregateOptions) ([]AggregateRow, error)

	// ListConversationMessages returns messages within a conversation.
	ListConversationMessages(ctx context.Context, convID int64,
		filter TextFilter) ([]MessageSummary, error)

	// TextSearch performs plain full-text search over text messages.
	TextSearch(ctx context.Context, query string,
		limit, offset int) ([]MessageSummary, error)

	// GetTextStats returns aggregate stats for text messages.
	GetTextStats(ctx context.Context,
		opts TextStatsOptions) (*TotalStats, error)
}
