package imessage

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/wesm/msgvault/internal/gmail"
)

const defaultPageSize = 500

// Client reads from macOS's iMessage chat.db and implements the gmail.API
// interface so it can be used with the existing sync infrastructure.
type Client struct {
	db             *sql.DB
	identifier     string    // source identifier for GetProfile (e.g., "local")
	myAddress      string    // normalized email-like address for the device owner
	afterDate      time.Time // only sync messages after this date
	beforeDate     time.Time // only sync messages before this date
	limit          int       // max total messages to return (0 = unlimited)
	returned       int       // messages returned so far (for limit tracking)
	useNanoseconds bool      // whether chat.db uses nanosecond timestamps
	logger         *slog.Logger
	pageSize       int
}

// ClientOption configures a Client.
type ClientOption func(*Client)

// WithAfterDate filters to messages on or after this date.
func WithAfterDate(t time.Time) ClientOption {
	return func(c *Client) { c.afterDate = t }
}

// WithBeforeDate filters to messages before this date.
func WithBeforeDate(t time.Time) ClientOption {
	return func(c *Client) { c.beforeDate = t }
}

// WithLimit sets the maximum number of messages to return across all pages.
func WithLimit(n int) ClientOption {
	return func(c *Client) { c.limit = n }
}

// WithMyAddress sets the owner's email-like address for MIME From headers
// on is_from_me messages.
func WithMyAddress(addr string) ClientOption {
	return func(c *Client) { c.myAddress = addr }
}

// WithImessageLogger sets the logger for the client.
func WithImessageLogger(l *slog.Logger) ClientOption {
	return func(c *Client) { c.logger = l }
}

// NewClient opens a read-only connection to an iMessage chat.db file
// and returns a Client that implements gmail.API.
func NewClient(dbPath string, identifier string, opts ...ClientOption) (*Client, error) {
	// Open chat.db read-only
	dsn := fmt.Sprintf("file:%s?mode=ro&_journal_mode=WAL&_busy_timeout=5000", dbPath)
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("open chat.db: %w", err)
	}

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("connect to chat.db: %w (check Full Disk Access permissions)", err)
	}

	c := &Client{
		db:         db,
		identifier: identifier,
		myAddress:  "me@imessage.local",
		logger:     slog.Default(),
		pageSize:   defaultPageSize,
	}

	for _, opt := range opts {
		opt(c)
	}

	// Detect timestamp format (nanoseconds vs seconds)
	if err := c.detectTimestampFormat(); err != nil {
		db.Close()
		return nil, fmt.Errorf("detect timestamp format: %w", err)
	}

	return c, nil
}

// Close closes the database connection.
func (c *Client) Close() error {
	return c.db.Close()
}

// detectTimestampFormat checks whether chat.db uses nanosecond timestamps
// (macOS High Sierra+) or second timestamps (older macOS).
func (c *Client) detectTimestampFormat() error {
	var maxDate sql.NullInt64
	err := c.db.QueryRow("SELECT MAX(date) FROM message WHERE date > 0").Scan(&maxDate)
	if err != nil {
		return fmt.Errorf("query max date: %w", err)
	}
	if maxDate.Valid {
		c.useNanoseconds = maxDate.Int64 > 1_000_000_000_000
	}
	return nil
}

// GetProfile returns a profile with the message count and max ROWID as history ID.
func (c *Client) GetProfile(ctx context.Context) (*gmail.Profile, error) {
	var count int64
	if err := c.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM message").Scan(&count); err != nil {
		return nil, fmt.Errorf("count messages: %w", err)
	}

	var maxROWID sql.NullInt64
	if err := c.db.QueryRowContext(ctx, "SELECT MAX(ROWID) FROM message").Scan(&maxROWID); err != nil {
		return nil, fmt.Errorf("get max rowid: %w", err)
	}

	historyID := uint64(0)
	if maxROWID.Valid {
		historyID = uint64(maxROWID.Int64)
	}

	return &gmail.Profile{
		EmailAddress:  c.identifier,
		MessagesTotal: count,
		HistoryID:     historyID,
	}, nil
}

// ListLabels returns iMessage and SMS as labels.
func (c *Client) ListLabels(ctx context.Context) ([]*gmail.Label, error) {
	return []*gmail.Label{
		{ID: "iMessage", Name: "iMessage", Type: "user"},
		{ID: "SMS", Name: "SMS", Type: "user"},
	}, nil
}

// ListMessages returns a page of message IDs from chat.db, ordered by ROWID.
// The pageToken is the string representation of the last seen ROWID.
// The query parameter is ignored (date filtering is done via client options).
func (c *Client) ListMessages(ctx context.Context, query string, pageToken string) (*gmail.MessageListResponse, error) {
	// Check limit
	if c.limit > 0 && c.returned >= c.limit {
		return &gmail.MessageListResponse{}, nil
	}

	lastROWID := int64(0)
	if pageToken != "" {
		var err error
		lastROWID, err = strconv.ParseInt(pageToken, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid page token: %w", err)
		}
	}

	// Build query
	sqlQuery := `
		SELECT m.ROWID, COALESCE(c.guid, 'no-chat-' || CAST(m.ROWID AS TEXT)) as chat_guid
		FROM message m
		LEFT JOIN chat_message_join cmj ON cmj.message_id = m.ROWID
		LEFT JOIN chat c ON c.ROWID = cmj.chat_id
		WHERE m.ROWID > ?`
	args := []interface{}{lastROWID}

	if !c.afterDate.IsZero() {
		appleTS := timeToAppleTimestamp(c.afterDate, c.useNanoseconds)
		sqlQuery += " AND m.date >= ?"
		args = append(args, appleTS)
	}
	if !c.beforeDate.IsZero() {
		appleTS := timeToAppleTimestamp(c.beforeDate, c.useNanoseconds)
		sqlQuery += " AND m.date < ?"
		args = append(args, appleTS)
	}

	sqlQuery += " ORDER BY m.ROWID ASC LIMIT ?"

	// Calculate page size respecting limit
	pageSize := c.pageSize
	if c.limit > 0 {
		remaining := c.limit - c.returned
		if remaining < pageSize {
			pageSize = remaining
		}
	}
	args = append(args, pageSize)

	rows, err := c.db.QueryContext(ctx, sqlQuery, args...)
	if err != nil {
		return nil, fmt.Errorf("list messages: %w", err)
	}
	defer rows.Close()

	var messages []gmail.MessageID
	var maxRowID int64
	for rows.Next() {
		var rowID int64
		var chatGUID string
		if err := rows.Scan(&rowID, &chatGUID); err != nil {
			return nil, fmt.Errorf("scan message: %w", err)
		}
		messages = append(messages, gmail.MessageID{
			ID:       strconv.FormatInt(rowID, 10),
			ThreadID: chatGUID,
		})
		maxRowID = rowID
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate messages: %w", err)
	}

	c.returned += len(messages)

	// Determine next page token
	var nextPageToken string
	if len(messages) == pageSize {
		nextPageToken = strconv.FormatInt(maxRowID, 10)
	}

	// Get total estimate for progress reporting
	totalEstimate := int64(len(messages))
	if pageToken == "" {
		totalEstimate = c.countFilteredMessages(ctx)
	}

	return &gmail.MessageListResponse{
		Messages:           messages,
		NextPageToken:      nextPageToken,
		ResultSizeEstimate: totalEstimate,
	}, nil
}

// countFilteredMessages returns the total count of messages matching the date filters.
func (c *Client) countFilteredMessages(ctx context.Context) int64 {
	sqlQuery := "SELECT COUNT(*) FROM message WHERE 1=1"
	var args []interface{}

	if !c.afterDate.IsZero() {
		appleTS := timeToAppleTimestamp(c.afterDate, c.useNanoseconds)
		sqlQuery += " AND date >= ?"
		args = append(args, appleTS)
	}
	if !c.beforeDate.IsZero() {
		appleTS := timeToAppleTimestamp(c.beforeDate, c.useNanoseconds)
		sqlQuery += " AND date < ?"
		args = append(args, appleTS)
	}

	var count int64
	if err := c.db.QueryRowContext(ctx, sqlQuery, args...).Scan(&count); err != nil {
		return 0
	}
	return count
}

// GetMessageRaw fetches a single message and builds synthetic MIME data.
func (c *Client) GetMessageRaw(ctx context.Context, messageID string) (*gmail.RawMessage, error) {
	rowID, err := strconv.ParseInt(messageID, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid message ID: %w", err)
	}

	// Query message with handle and chat info
	var msg messageRow
	err = c.db.QueryRowContext(ctx, `
		SELECT
			m.ROWID, m.guid, m.text, m.date, m.is_from_me, m.service,
			m.cache_has_attachments,
			h.id,
			c.ROWID, c.guid, c.display_name, c.chat_identifier
		FROM message m
		LEFT JOIN handle h ON h.ROWID = m.handle_id
		LEFT JOIN chat_message_join cmj ON cmj.message_id = m.ROWID
		LEFT JOIN chat c ON c.ROWID = cmj.chat_id
		WHERE m.ROWID = ?
	`, rowID).Scan(
		&msg.ROWID, &msg.GUID, &msg.Text, &msg.Date, &msg.IsFromMe, &msg.Service,
		&msg.HasAttachments,
		&msg.HandleID,
		&msg.ChatROWID, &msg.ChatGUID, &msg.ChatDisplayName, &msg.ChatIdentifier,
	)
	if err == sql.ErrNoRows {
		return nil, &gmail.NotFoundError{Path: "/messages/" + messageID}
	}
	if err != nil {
		return nil, fmt.Errorf("get message %s: %w", messageID, err)
	}

	// Determine sender and recipients
	fromAddr, toAddrs := c.resolveParticipants(ctx, &msg)

	// Convert Apple timestamp to time
	msgDate := appleTimestampToTime(msg.Date)

	// Get message body
	body := ""
	if msg.Text != nil {
		body = *msg.Text
	}

	// Build MIME
	mimeData := buildMIME(fromAddr, toAddrs, msgDate, msg.GUID, body)

	// Determine thread ID
	threadID := "no-chat-" + messageID
	if msg.ChatGUID != nil {
		threadID = *msg.ChatGUID
	}

	// Build label based on service
	var labelIDs []string
	if msg.Service != "" {
		labelIDs = []string{msg.Service}
	}

	// InternalDate as Unix milliseconds
	internalDate := int64(0)
	if !msgDate.IsZero() {
		internalDate = msgDate.UnixMilli()
	}

	return &gmail.RawMessage{
		ID:           messageID,
		ThreadID:     threadID,
		LabelIDs:     labelIDs,
		Snippet:      snippet(body, 100),
		HistoryID:    uint64(msg.ROWID),
		InternalDate: internalDate,
		SizeEstimate: int64(len(mimeData)),
		Raw:          mimeData,
	}, nil
}

// resolveParticipants determines the From and To addresses for a message.
func (c *Client) resolveParticipants(ctx context.Context, msg *messageRow) (from []string, to []string) {
	if msg.IsFromMe != 0 {
		// Sender is the device owner
		from = []string{c.myAddress}
		// Recipients are the chat participants
		if msg.ChatROWID != nil {
			to = c.getChatParticipants(ctx, *msg.ChatROWID)
		} else if msg.HandleID != nil {
			email, _, _ := normalizeIdentifier(*msg.HandleID)
			if email != "" {
				to = []string{email}
			}
		}
	} else {
		// Sender is from the handle table
		if msg.HandleID != nil {
			email, _, _ := normalizeIdentifier(*msg.HandleID)
			if email != "" {
				from = []string{email}
			}
		}
		// Recipient is the device owner (and possibly other participants in group chats)
		to = []string{c.myAddress}
		if msg.ChatROWID != nil {
			others := c.getChatParticipants(ctx, *msg.ChatROWID)
			// Add other participants (exclude the sender)
			senderAddr := ""
			if len(from) > 0 {
				senderAddr = from[0]
			}
			for _, addr := range others {
				if addr != senderAddr && addr != c.myAddress {
					to = append(to, addr)
				}
			}
		}
	}
	return from, to
}

// getChatParticipants returns the normalized email addresses of all participants
// in a chat (excluding the device owner).
func (c *Client) getChatParticipants(ctx context.Context, chatROWID int64) []string {
	rows, err := c.db.QueryContext(ctx, `
		SELECT h.id
		FROM chat_handle_join chj
		JOIN handle h ON h.ROWID = chj.handle_id
		WHERE chj.chat_id = ?
	`, chatROWID)
	if err != nil {
		c.logger.Warn("failed to get chat participants", "chat_id", chatROWID, "error", err)
		return nil
	}
	defer rows.Close()

	var addrs []string
	for rows.Next() {
		var handleID string
		if err := rows.Scan(&handleID); err != nil {
			continue
		}
		email, _, _ := normalizeIdentifier(handleID)
		if email != "" {
			addrs = append(addrs, email)
		}
	}
	return addrs
}

// GetMessagesRawBatch fetches multiple messages sequentially.
// Since we're reading from a local database, parallelism adds no benefit.
func (c *Client) GetMessagesRawBatch(ctx context.Context, messageIDs []string) ([]*gmail.RawMessage, error) {
	results := make([]*gmail.RawMessage, len(messageIDs))
	for i, id := range messageIDs {
		msg, err := c.GetMessageRaw(ctx, id)
		if err != nil {
			c.logger.Warn("failed to fetch message", "id", id, "error", err)
			continue
		}
		results[i] = msg
	}
	return results, nil
}

// ListHistory is not supported for iMessage (no incremental sync yet).
func (c *Client) ListHistory(ctx context.Context, startHistoryID uint64, pageToken string) (*gmail.HistoryResponse, error) {
	return &gmail.HistoryResponse{
		HistoryID: startHistoryID,
	}, nil
}

// TrashMessage is not supported for iMessage.
func (c *Client) TrashMessage(ctx context.Context, messageID string) error {
	return fmt.Errorf("trash not supported for iMessage")
}

// DeleteMessage is not supported for iMessage.
func (c *Client) DeleteMessage(ctx context.Context, messageID string) error {
	return fmt.Errorf("delete not supported for iMessage")
}

// BatchDeleteMessages is not supported for iMessage.
func (c *Client) BatchDeleteMessages(ctx context.Context, messageIDs []string) error {
	return fmt.Errorf("batch delete not supported for iMessage")
}

// Ensure Client implements gmail.API.
var _ gmail.API = (*Client)(nil)
