// Package imessage provides an iMessage client that reads from macOS's chat.db
// and implements the gmail.API interface for use with the existing sync infrastructure.
package imessage

// messageRow holds a row from the iMessage chat.db message table
// joined with chat and handle info.
type messageRow struct {
	ROWID           int64
	GUID            string
	Text            *string // nullable - some messages only have attributedBody
	Date            int64   // Apple epoch timestamp (seconds or nanoseconds)
	IsFromMe        int
	Service         string // "iMessage" or "SMS"
	HasAttachments  int
	HandleID        *string // handle.id (phone or email), NULL for is_from_me
	ChatROWID       *int64  // chat.ROWID for participant lookup
	ChatGUID        *string // chat.guid, used as conversation/thread ID
	ChatDisplayName *string // chat.display_name (set for group chats)
	ChatIdentifier  *string // chat.chat_identifier
}
