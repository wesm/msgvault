package gvoice

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/wesm/msgvault/internal/gmail"
)

const defaultPageSize = 500

// Client reads from a Google Voice Takeout export and implements the gmail.API
// interface so it can be used with the existing sync infrastructure.
type Client struct {
	takeoutDir string
	owner      ownerPhones
	identifier string // GV phone number used as source identifier
	afterDate  time.Time
	beforeDate time.Time
	limit      int
	returned   int
	index      []indexEntry
	indexBuilt bool
	logger     *slog.Logger
	pageSize   int

	// LRU cache for parsed HTML files (avoid re-parsing when consecutive
	// messages come from the same file)
	lastFilePath string
	lastMessages []textMessage
	lastGroupPar []string
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

// WithLogger sets the logger for the client.
func WithLogger(l *slog.Logger) ClientOption {
	return func(c *Client) { c.logger = l }
}

// NewClient creates a Client from a Google Voice Takeout directory.
// The directory should be the "Voice" folder containing "Calls/" and "Phones.vcf".
func NewClient(takeoutDir string, opts ...ClientOption) (*Client, error) {
	// Validate directory exists
	info, err := os.Stat(takeoutDir)
	if err != nil {
		return nil, fmt.Errorf("takeout directory: %w", err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("not a directory: %s", takeoutDir)
	}

	// Check for Calls subdirectory
	callsDir := filepath.Join(takeoutDir, "Calls")
	if _, err := os.Stat(callsDir); err != nil {
		return nil, fmt.Errorf("Calls directory not found in %s: %w", takeoutDir, err)
	}

	// Parse Phones.vcf
	vcfPath := filepath.Join(takeoutDir, "Phones.vcf")
	vcfData, err := os.ReadFile(vcfPath)
	if err != nil {
		return nil, fmt.Errorf("read Phones.vcf: %w", err)
	}

	owner, err := parseVCF(vcfData)
	if err != nil {
		return nil, fmt.Errorf("parse Phones.vcf: %w", err)
	}

	c := &Client{
		takeoutDir: takeoutDir,
		owner:      owner,
		identifier: owner.GoogleVoice,
		logger:     slog.Default(),
		pageSize:   defaultPageSize,
	}

	for _, opt := range opts {
		opt(c)
	}

	return c, nil
}

// Identifier returns the Google Voice phone number used as source identifier.
func (c *Client) Identifier() string {
	return c.identifier
}

// Close is a no-op for the Takeout client.
func (c *Client) Close() error {
	return nil
}

// buildIndex walks the Calls directory, parses each HTML file, and builds
// a sorted index of all messages and call records. This is done lazily
// on the first call to ListMessages.
func (c *Client) buildIndex() error {
	if c.indexBuilt {
		return nil
	}

	callsDir := filepath.Join(c.takeoutDir, "Calls")
	entries, err := os.ReadDir(callsDir)
	if err != nil {
		return fmt.Errorf("read Calls directory: %w", err)
	}

	c.logger.Info("building index", "files", len(entries))

	var index []indexEntry
	skipped := 0

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name, ft, err := classifyFile(entry.Name())
		if err != nil {
			skipped++
			continue
		}

		filePath := filepath.Join(callsDir, entry.Name())

		switch ft {
		case fileTypeText, fileTypeGroup:
			entries, err := c.indexTextFile(filePath, name, ft)
			if err != nil {
				c.logger.Warn("failed to index text file", "file", entry.Name(), "error", err)
				continue
			}
			index = append(index, entries...)

		case fileTypeReceived, fileTypePlaced, fileTypeMissed, fileTypeVoicemail:
			entry, err := c.indexCallFile(filePath, name, ft)
			if err != nil {
				c.logger.Warn("failed to index call file", "file", entry.ID, "error", err)
				continue
			}
			index = append(index, *entry)
		}
	}

	// Apply date filters
	var filtered []indexEntry
	for _, e := range index {
		if !c.afterDate.IsZero() && e.Timestamp.Before(c.afterDate) {
			continue
		}
		if !c.beforeDate.IsZero() && !e.Timestamp.Before(c.beforeDate) {
			continue
		}
		filtered = append(filtered, e)
	}

	// Sort by timestamp
	sort.Slice(filtered, func(i, j int) bool {
		return filtered[i].Timestamp.Before(filtered[j].Timestamp)
	})

	c.index = filtered
	c.indexBuilt = true

	c.logger.Info("index built",
		"total_entries", len(index),
		"filtered_entries", len(filtered),
		"skipped_files", skipped,
	)

	return nil
}

// indexTextFile parses a text/group conversation HTML and returns index entries
// for each individual message within it.
func (c *Client) indexTextFile(filePath, contactName string, ft fileType) ([]indexEntry, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	messages, groupParticipants, err := parseTextHTML(f)
	if err != nil {
		return nil, err
	}

	var entries []indexEntry
	for i, msg := range messages {
		// Compute deterministic message ID
		bodyPrefix := msg.Body
		if len(bodyPrefix) > 50 {
			bodyPrefix = bodyPrefix[:50]
		}
		id := computeMessageID(msg.SenderPhone, msg.Timestamp.Format(time.RFC3339Nano), bodyPrefix)

		// Compute thread ID
		var threadID string
		if ft == fileTypeGroup {
			threadID = computeThreadID(c.owner.Cell, fileTypeGroup, "", groupParticipants)
		} else {
			// For 1:1 texts, use the non-owner phone
			otherPhone := msg.SenderPhone
			if msg.IsMe {
				// Need to find the other party — look through all messages
				for _, m := range messages {
					if !m.IsMe {
						otherPhone = m.SenderPhone
						break
					}
				}
			}
			threadID = computeThreadID(c.owner.Cell, fileTypeText, otherPhone, nil)
		}

		label := labelForFileType(ft)

		entries = append(entries, indexEntry{
			ID:           id,
			ThreadID:     threadID,
			FilePath:     filePath,
			MessageIndex: i,
			Timestamp:    msg.Timestamp,
			FileType:     ft,
			Labels:       []string{label},
		})
	}

	return entries, nil
}

// indexCallFile parses a call log HTML and returns a single index entry.
func (c *Client) indexCallFile(filePath, contactName string, ft fileType) (*indexEntry, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	record, err := parseCallHTML(f)
	if err != nil {
		return nil, err
	}

	// Override file type from HTML if the filename didn't include it
	if record.CallType != 0 {
		ft = record.CallType
	}

	id := computeMessageID(ft.String(), record.Phone, record.Timestamp.Format(time.RFC3339Nano))
	threadID := computeThreadID(c.owner.Cell, ft, record.Phone, nil)
	label := labelForFileType(ft)

	return &indexEntry{
		ID:       id,
		ThreadID: threadID,
		FilePath: filePath,
		Timestamp: record.Timestamp,
		FileType: ft,
		Labels:   []string{label},
	}, nil
}

// GetProfile returns a profile with the GV phone as identifier and index size as total.
func (c *Client) GetProfile(ctx context.Context) (*gmail.Profile, error) {
	if err := c.buildIndex(); err != nil {
		return nil, err
	}

	return &gmail.Profile{
		EmailAddress:  c.identifier,
		MessagesTotal: int64(len(c.index)),
		HistoryID:     uint64(len(c.index)),
	}, nil
}

// ListLabels returns the set of labels used for Google Voice messages.
func (c *Client) ListLabels(ctx context.Context) ([]*gmail.Label, error) {
	return []*gmail.Label{
		{ID: "sms", Name: "SMS", Type: "user"},
		{ID: "call_received", Name: "Call Received", Type: "user"},
		{ID: "call_placed", Name: "Call Placed", Type: "user"},
		{ID: "call_missed", Name: "Call Missed", Type: "user"},
		{ID: "voicemail", Name: "Voicemail", Type: "user"},
		{ID: "mms", Name: "MMS", Type: "user"},
	}, nil
}

// ListMessages returns a page of message IDs from the sorted index.
// The pageToken is the string representation of the offset into the index.
func (c *Client) ListMessages(ctx context.Context, query string, pageToken string) (*gmail.MessageListResponse, error) {
	if err := c.buildIndex(); err != nil {
		return nil, fmt.Errorf("build index: %w", err)
	}

	// Check limit
	if c.limit > 0 && c.returned >= c.limit {
		return &gmail.MessageListResponse{}, nil
	}

	offset := 0
	if pageToken != "" {
		var err error
		offset, err = strconv.Atoi(pageToken)
		if err != nil {
			return nil, fmt.Errorf("invalid page token: %w", err)
		}
	}

	if offset >= len(c.index) {
		return &gmail.MessageListResponse{}, nil
	}

	// Calculate page size respecting limit
	pageSize := c.pageSize
	if c.limit > 0 {
		remaining := c.limit - c.returned
		if remaining < pageSize {
			pageSize = remaining
		}
	}

	end := offset + pageSize
	if end > len(c.index) {
		end = len(c.index)
	}

	page := c.index[offset:end]
	messages := make([]gmail.MessageID, len(page))
	for i, entry := range page {
		messages[i] = gmail.MessageID{
			ID:       entry.ID,
			ThreadID: entry.ThreadID,
		}
	}

	c.returned += len(messages)

	var nextPageToken string
	if end < len(c.index) && (c.limit <= 0 || c.returned < c.limit) {
		nextPageToken = strconv.Itoa(end)
	}

	totalEstimate := int64(len(c.index))

	return &gmail.MessageListResponse{
		Messages:           messages,
		NextPageToken:      nextPageToken,
		ResultSizeEstimate: totalEstimate,
	}, nil
}

// GetMessageRaw fetches a single message by ID and builds synthetic MIME data.
func (c *Client) GetMessageRaw(ctx context.Context, messageID string) (*gmail.RawMessage, error) {
	if err := c.buildIndex(); err != nil {
		return nil, fmt.Errorf("build index: %w", err)
	}

	// Linear scan for the entry (index is typically <300k entries)
	var entry *indexEntry
	for i := range c.index {
		if c.index[i].ID == messageID {
			entry = &c.index[i]
			break
		}
	}
	if entry == nil {
		return nil, &gmail.NotFoundError{Path: "/messages/" + messageID}
	}

	switch entry.FileType {
	case fileTypeText, fileTypeGroup:
		return c.buildTextMessage(entry)
	case fileTypeReceived, fileTypePlaced, fileTypeMissed, fileTypeVoicemail:
		return c.buildCallMessage(entry)
	default:
		return nil, fmt.Errorf("unknown file type for message %s", messageID)
	}
}

// buildTextMessage constructs a RawMessage from a text/group conversation entry.
func (c *Client) buildTextMessage(entry *indexEntry) (*gmail.RawMessage, error) {
	messages, groupParticipants, err := c.getCachedMessages(entry.FilePath)
	if err != nil {
		return nil, err
	}

	if entry.MessageIndex >= len(messages) {
		return nil, fmt.Errorf("message index %d out of range (file has %d messages)", entry.MessageIndex, len(messages))
	}

	msg := messages[entry.MessageIndex]

	// Determine from and to addresses
	var fromAddrs, toAddrs []string

	ownerEmail, _ := normalizeIdentifier(c.owner.GoogleVoice)

	if msg.IsMe {
		fromAddrs = []string{ownerEmail}
		if entry.FileType == fileTypeGroup {
			for _, phone := range groupParticipants {
				email, _ := normalizeIdentifier(phone)
				toAddrs = append(toAddrs, email)
			}
		} else {
			// 1:1 text — find the other party
			for _, m := range messages {
				if !m.IsMe {
					email, _ := normalizeIdentifier(m.SenderPhone)
					toAddrs = []string{email}
					break
				}
			}
		}
	} else {
		senderEmail, _ := normalizeIdentifier(msg.SenderPhone)
		fromAddrs = []string{senderEmail}
		toAddrs = []string{ownerEmail}
		// In group conversations, add other participants
		if entry.FileType == fileTypeGroup {
			for _, phone := range groupParticipants {
				email, _ := normalizeIdentifier(phone)
				if email != senderEmail {
					toAddrs = append(toAddrs, email)
				}
			}
		}
	}

	mimeData := buildMIME(fromAddrs, toAddrs, msg.Timestamp, entry.ID, msg.Body)

	internalDate := int64(0)
	if !msg.Timestamp.IsZero() {
		internalDate = msg.Timestamp.UnixMilli()
	}

	// Check for MMS attachments
	labels := entry.Labels
	if len(msg.Attachments) > 0 {
		labels = append(labels, "mms")
	}

	return &gmail.RawMessage{
		ID:           entry.ID,
		ThreadID:     entry.ThreadID,
		LabelIDs:     labels,
		Snippet:      snippet(msg.Body, 100),
		HistoryID:    uint64(entry.Timestamp.UnixNano()),
		InternalDate: internalDate,
		SizeEstimate: int64(len(mimeData)),
		Raw:          mimeData,
	}, nil
}

// buildCallMessage constructs a RawMessage from a call record entry.
func (c *Client) buildCallMessage(entry *indexEntry) (*gmail.RawMessage, error) {
	f, err := os.Open(entry.FilePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	record, err := parseCallHTML(f)
	if err != nil {
		return nil, err
	}

	// Build a descriptive body for the call
	var body strings.Builder
	switch record.CallType {
	case fileTypeReceived:
		fmt.Fprintf(&body, "Received call from %s", record.Name)
	case fileTypePlaced:
		fmt.Fprintf(&body, "Placed call to %s", record.Name)
	case fileTypeMissed:
		fmt.Fprintf(&body, "Missed call from %s", record.Name)
	case fileTypeVoicemail:
		fmt.Fprintf(&body, "Voicemail from %s", record.Name)
	}
	if record.Duration != "" {
		fmt.Fprintf(&body, " (%s)", formatDuration(record.Duration))
	}

	ownerEmail, _ := normalizeIdentifier(c.owner.GoogleVoice)
	contactEmail, _ := normalizeIdentifier(record.Phone)

	var fromAddrs, toAddrs []string
	switch record.CallType {
	case fileTypeReceived, fileTypeMissed, fileTypeVoicemail:
		fromAddrs = []string{contactEmail}
		toAddrs = []string{ownerEmail}
	case fileTypePlaced:
		fromAddrs = []string{ownerEmail}
		toAddrs = []string{contactEmail}
	}

	mimeData := buildMIME(fromAddrs, toAddrs, record.Timestamp, entry.ID, body.String())

	internalDate := int64(0)
	if !record.Timestamp.IsZero() {
		internalDate = record.Timestamp.UnixMilli()
	}

	return &gmail.RawMessage{
		ID:           entry.ID,
		ThreadID:     entry.ThreadID,
		LabelIDs:     entry.Labels,
		Snippet:      snippet(body.String(), 100),
		HistoryID:    uint64(record.Timestamp.UnixNano()),
		InternalDate: internalDate,
		SizeEstimate: int64(len(mimeData)),
		Raw:          mimeData,
	}, nil
}

// getCachedMessages returns parsed messages for a file, using a simple cache.
func (c *Client) getCachedMessages(filePath string) ([]textMessage, []string, error) {
	if c.lastFilePath == filePath {
		return c.lastMessages, c.lastGroupPar, nil
	}

	f, err := os.Open(filePath)
	if err != nil {
		return nil, nil, err
	}
	defer f.Close()

	messages, groupParticipants, err := parseTextHTML(f)
	if err != nil {
		return nil, nil, err
	}

	c.lastFilePath = filePath
	c.lastMessages = messages
	c.lastGroupPar = groupParticipants

	return messages, groupParticipants, nil
}

// GetMessagesRawBatch fetches multiple messages sequentially.
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

// ListHistory is not supported for Google Voice Takeout (static export).
func (c *Client) ListHistory(ctx context.Context, startHistoryID uint64, pageToken string) (*gmail.HistoryResponse, error) {
	return &gmail.HistoryResponse{
		HistoryID: startHistoryID,
	}, nil
}

// TrashMessage is not supported for Google Voice Takeout.
func (c *Client) TrashMessage(ctx context.Context, messageID string) error {
	return fmt.Errorf("trash not supported for Google Voice Takeout")
}

// DeleteMessage is not supported for Google Voice Takeout.
func (c *Client) DeleteMessage(ctx context.Context, messageID string) error {
	return fmt.Errorf("delete not supported for Google Voice Takeout")
}

// BatchDeleteMessages is not supported for Google Voice Takeout.
func (c *Client) BatchDeleteMessages(ctx context.Context, messageIDs []string) error {
	return fmt.Errorf("batch delete not supported for Google Voice Takeout")
}

// formatDuration converts ISO 8601 duration (PT1M23S) to human-readable format.
func formatDuration(iso string) string {
	// Parse PT{hours}H{minutes}M{seconds}S
	iso = strings.TrimPrefix(iso, "PT")
	var parts []string

	if i := strings.Index(iso, "H"); i >= 0 {
		parts = append(parts, iso[:i]+"h")
		iso = iso[i+1:]
	}
	if i := strings.Index(iso, "M"); i >= 0 {
		parts = append(parts, iso[:i]+"m")
		iso = iso[i+1:]
	}
	if i := strings.Index(iso, "S"); i >= 0 {
		parts = append(parts, iso[:i]+"s")
	}

	if len(parts) == 0 {
		return "0s"
	}
	return strings.Join(parts, " ")
}

// Ensure Client implements gmail.API.
var _ gmail.API = (*Client)(nil)
