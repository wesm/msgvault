package query

import (
	"context"

	"github.com/wesm/msgvault/internal/search"
)

// Engine provides query operations for msgvault data.
// This interface can be implemented by different backends:
// - SQLiteEngine: Direct SQLite queries (flexible, moderate performance)
// - ParquetEngine: Arrow/Parquet queries (fast aggregates, read-only)
type Engine interface {
	// Aggregate performs grouping based on the provided ViewType (Sender, Domain, etc.)
	Aggregate(ctx context.Context, groupBy ViewType, opts AggregateOptions) ([]AggregateRow, error)

	// SubAggregate performs aggregation on a filtered subset of messages.
	// This is used for sub-grouping after drill-down, e.g., drilling into
	// "Sender: foo@example.com" and then sub-grouping by Recipients or Labels.
	// The filter specifies the parent context (sender, domain, etc.) and
	// groupBy specifies what dimension to aggregate by.
	SubAggregate(ctx context.Context, filter MessageFilter, groupBy ViewType, opts AggregateOptions) ([]AggregateRow, error)

	// Message queries
	ListMessages(ctx context.Context, filter MessageFilter) ([]MessageSummary, error)
	GetMessage(ctx context.Context, id int64) (*MessageDetail, error)
	GetMessageBySourceID(ctx context.Context, sourceMessageID string) (*MessageDetail, error)
	GetAttachment(ctx context.Context, id int64) (*AttachmentInfo, error)

	// Search - full-text search using FTS5 (includes message body)
	Search(ctx context.Context, query *search.Query, limit, offset int) ([]MessageSummary, error)

	// SearchFast searches message metadata only (no body text).
	// This is much faster for large archives as it queries Parquet files directly.
	// Searches: subject, sender email/name (case-insensitive).
	// The filter parameter allows contextual search within a drill-down.
	SearchFast(ctx context.Context, query *search.Query, filter MessageFilter, limit, offset int) ([]MessageSummary, error)

	// SearchFastCount returns the total count of messages matching a search query.
	// This is used for pagination UI to show "N of M results".
	SearchFastCount(ctx context.Context, query *search.Query, filter MessageFilter) (int64, error)

	// GetGmailIDsByFilter returns Gmail message IDs (source_message_id) matching a filter.
	// This is useful for batch operations like staging messages for deletion.
	GetGmailIDsByFilter(ctx context.Context, filter MessageFilter) ([]string, error)

	// Account queries
	ListAccounts(ctx context.Context) ([]AccountInfo, error)

	// Stats
	GetTotalStats(ctx context.Context, opts StatsOptions) (*TotalStats, error)

	// Close releases any resources held by the engine.
	Close() error
}

// TotalStats provides overall database statistics.
type TotalStats struct {
	MessageCount    int64
	TotalSize       int64
	AttachmentCount int64
	AttachmentSize  int64
	LabelCount      int64
	AccountCount    int64
}
