package query

import "time"

// TextViewType represents the type of view in Texts mode.
type TextViewType int

const (
	TextViewConversations TextViewType = iota
	TextViewContacts
	TextViewContactNames
	TextViewSources
	TextViewLabels
	TextViewTime
	TextViewTypeCount
)

func (v TextViewType) String() string {
	switch v {
	case TextViewConversations:
		return "Conversations"
	case TextViewContacts:
		return "Contacts"
	case TextViewContactNames:
		return "Contact Names"
	case TextViewSources:
		return "Sources"
	case TextViewLabels:
		return "Labels"
	case TextViewTime:
		return "Time"
	default:
		return "Unknown"
	}
}

// ConversationRow represents a conversation in the Conversations view.
type ConversationRow struct {
	ConversationID   int64
	Title            string
	SourceType       string
	MessageCount     int64
	ParticipantCount int64
	LastMessageAt    time.Time
	LastPreview      string
}

// TextFilter specifies which text messages to retrieve.
type TextFilter struct {
	SourceID       *int64
	ConversationID *int64
	ContactPhone   string
	ContactName    string
	SourceType     string
	Label          string
	TimeRange      TimeRange
	After          *time.Time
	Before         *time.Time
	Pagination     Pagination
	SortField      SortField
	SortDirection  SortDirection
}

// TextAggregateOptions configures a text aggregate query.
type TextAggregateOptions struct {
	SourceID        *int64
	After           *time.Time
	Before          *time.Time
	SortField       SortField
	SortDirection   SortDirection
	Limit           int
	TimeGranularity TimeGranularity
	SearchQuery     string
}

// TextStatsOptions configures a text stats query.
type TextStatsOptions struct {
	SourceID    *int64
	SearchQuery string
}

// TextMessageTypes lists the message_type values included in Texts mode.
var TextMessageTypes = []string{
	"whatsapp", "imessage", "sms", "google_voice_text",
}

// IsTextMessageType returns true if the given type is a text message type.
func IsTextMessageType(mt string) bool {
	for _, t := range TextMessageTypes {
		if t == mt {
			return true
		}
	}
	return false
}
