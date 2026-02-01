// Package query provides a reusable query layer for msgvault.
// It supports aggregation queries for TUI views and message retrieval for detail views.
// The package is designed with a backend-agnostic interface to support both
// SQLite (for flexibility) and Parquet (for performance) data sources.
package query

import "time"

// AggregateRow represents a single row in an aggregate view.
// Used for Senders, Recipients, Domains, Labels, and Time views.
type AggregateRow struct {
	Key             string // email, domain, label name, or time period
	Count           int64  // number of messages
	TotalSize       int64  // sum of size_estimate in bytes
	AttachmentSize  int64  // sum of attachment sizes in bytes
	AttachmentCount int64  // number of attachments
	TotalUnique     int64  // total unique keys (same for all rows, computed via COUNT(*) OVER())
}

// MessageSummary represents a message in list views.
// Contains enough information for display without fetching the full body.
type MessageSummary struct {
	ID              int64
	SourceMessageID string
	ConversationID  int64
	Subject         string
	Snippet         string
	FromEmail       string
	FromName        string
	SentAt          time.Time
	SizeEstimate    int64
	HasAttachments  bool
	AttachmentCount int
	Labels          []string
	DeletedAt       *time.Time // When message was deleted from server (nil if not deleted)
}

// MessageDetail represents a full message with body and attachments.
type MessageDetail struct {
	ID              int64
	SourceMessageID string
	ConversationID  int64
	Subject         string
	Snippet         string
	SentAt          time.Time
	ReceivedAt      *time.Time
	SizeEstimate    int64
	HasAttachments  bool

	// Participants
	From []Address
	To   []Address
	Cc   []Address
	Bcc  []Address

	// Content
	BodyText string
	BodyHTML string

	// Metadata
	Labels      []string
	Attachments []AttachmentInfo
}

// Address represents an email address with optional display name.
type Address struct {
	Email string
	Name  string
}

// AttachmentInfo represents attachment metadata.
type AttachmentInfo struct {
	ID          int64
	Filename    string
	MimeType    string
	Size        int64
	ContentHash string
}

// ViewType represents the type of aggregate view.
type ViewType int

const (
	ViewSenders ViewType = iota
	ViewSenderNames
	ViewRecipients
	ViewRecipientNames
	ViewDomains
	ViewLabels
	ViewTime
)

func (v ViewType) String() string {
	switch v {
	case ViewSenders:
		return "Senders"
	case ViewSenderNames:
		return "Sender Names"
	case ViewRecipients:
		return "Recipients"
	case ViewRecipientNames:
		return "Recipient Names"
	case ViewDomains:
		return "Domains"
	case ViewLabels:
		return "Labels"
	case ViewTime:
		return "Time"
	default:
		return "Unknown"
	}
}

// TimeGranularity represents the time grouping level.
type TimeGranularity int

const (
	TimeYear TimeGranularity = iota
	TimeMonth
	TimeDay
)

func (g TimeGranularity) String() string {
	switch g {
	case TimeYear:
		return "Year"
	case TimeMonth:
		return "Month"
	case TimeDay:
		return "Day"
	default:
		return "Unknown"
	}
}

// SortField represents the field to sort aggregates by.
type SortField int

const (
	SortByCount SortField = iota
	SortBySize
	SortByAttachmentSize
	SortByName
)

func (f SortField) String() string {
	switch f {
	case SortByCount:
		return "Count"
	case SortBySize:
		return "Size"
	case SortByAttachmentSize:
		return "Attachments"
	case SortByName:
		return "Name"
	default:
		return "Unknown"
	}
}

// SortDirection represents ascending or descending sort order.
type SortDirection int

const (
	SortDesc SortDirection = iota
	SortAsc
)

// MessageSortField represents the field to sort messages by.
type MessageSortField int

const (
	MessageSortByDate MessageSortField = iota
	MessageSortBySize
	MessageSortBySubject
)

// MessageFilter specifies which messages to retrieve.
type MessageFilter struct {
	// Filter by aggregate key
	Sender        string // filter by sender email
	SenderName    string // filter by sender display name (COALESCE(display_name, email))
	Recipient     string // filter by recipient email
	RecipientName string // filter by recipient display name (COALESCE(display_name, email))
	Domain        string // filter by sender domain
	Label         string // filter by label name

	// Filter by conversation (thread)
	ConversationID *int64 // filter by conversation/thread ID

	// MatchEmpty* flags change how empty filter values are interpreted for each field.
	// When false (default): empty string means "no filter" (return all)
	// When true: empty string means "filter for NULL/empty values"
	// This enables drilldown into empty-bucket aggregates (e.g., messages with no sender).
	//
	// IMPORTANT: Only set ONE MatchEmpty* flag at a time. Setting multiple flags
	// creates an AND condition that may return no results (e.g., messages with
	// no sender AND no recipient AND no domain). The TUI sets exactly one flag
	// based on the current view type when drilling into an empty aggregate bucket.
	MatchEmptySender        bool
	MatchEmptySenderName    bool
	MatchEmptyRecipient     bool
	MatchEmptyRecipientName bool
	MatchEmptyDomain        bool
	MatchEmptyLabel         bool

	// Time range
	TimePeriod      string // e.g., "2024", "2024-01", "2024-01-15"
	TimeGranularity TimeGranularity

	// Account filter
	SourceID *int64 // nil means all accounts

	// Date range
	After  *time.Time
	Before *time.Time

	// Content filter
	WithAttachmentsOnly bool // only return messages with attachments

	// Pagination
	Limit  int
	Offset int

	// Sorting
	SortField     MessageSortField
	SortDirection SortDirection
}

// AggregateOptions configures an aggregate query.
type AggregateOptions struct {
	// Account filter
	SourceID *int64 // nil means all accounts

	// Date range
	After  *time.Time
	Before *time.Time

	// Sorting
	SortField     SortField
	SortDirection SortDirection

	// Limit results (0 means default, typically 100)
	Limit int

	// Time-specific options
	TimeGranularity TimeGranularity

	// Filter options
	WithAttachmentsOnly bool

	// Text search filter (filters aggregates to only include messages matching search)
	SearchQuery string
}

// DefaultAggregateOptions returns sensible defaults.
func DefaultAggregateOptions() AggregateOptions {
	return AggregateOptions{
		SortField:       SortByCount,
		SortDirection:   SortDesc,
		Limit:           100,
		TimeGranularity: TimeMonth,
	}
}

// AccountInfo represents a source account.
type AccountInfo struct {
	ID          int64
	SourceType  string
	Identifier  string // email address
	DisplayName string
}

// StatsOptions configures a stats query.
type StatsOptions struct {
	SourceID            *int64 // nil means all accounts
	WithAttachmentsOnly bool   // only count messages with attachments
}
