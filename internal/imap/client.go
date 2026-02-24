package imap

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"

	imap "github.com/emersion/go-imap/v2"
	"github.com/emersion/go-imap/v2/imapclient"
	gmailapi "github.com/wesm/msgvault/internal/gmail"
)

// Option is a functional option for Client.
type Option func(*Client)

// WithLogger sets the logger.
func WithLogger(logger *slog.Logger) Option {
	return func(c *Client) { c.logger = logger }
}

// fetchChunkSize is the maximum number of UIDs per UID FETCH command.
// Large FETCH sets cause server-side timeouts on big mailboxes; chunking
// keeps each round-trip short.
const fetchChunkSize = 50

// listPageSize is the number of message IDs returned per ListMessages call.
// Matches typical Gmail page size so the sync loop checkpoints frequently.
const listPageSize = 500

// Client implements gmail.API for IMAP servers.
type Client struct {
	config   *Config
	password string
	logger   *slog.Logger

	mu               sync.Mutex
	conn             *imapclient.Client
	selectedMailbox  string               // currently selected mailbox
	mailboxCache     []string             // cached list of selectable mailboxes
	messageListCache []gmailapi.MessageID // full message ID list, built once per session
	trashMailbox     string               // cached trash mailbox name
}

// NewClient creates a new IMAP client.
func NewClient(cfg *Config, password string, opts ...Option) *Client {
	c := &Client{
		config:   cfg,
		password: password,
		logger:   slog.Default(),
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// connect establishes and authenticates the IMAP connection. Caller must hold mu.
func (c *Client) connect(ctx context.Context) error {
	if c.conn != nil {
		return nil
	}

	addr := c.config.Addr()
	c.logger.Debug("connecting to IMAP server", "addr", addr, "tls", c.config.TLS, "starttls", c.config.STARTTLS)

	imapOpts := &imapclient.Options{}
	var (
		conn *imapclient.Client
		err  error
	)
	if c.config.TLS {
		conn, err = imapclient.DialTLS(addr, imapOpts)
	} else if c.config.STARTTLS {
		conn, err = imapclient.DialStartTLS(addr, imapOpts)
	} else {
		conn, err = imapclient.DialInsecure(addr, imapOpts)
	}
	if err != nil {
		return fmt.Errorf("dial IMAP %s: %w", addr, err)
	}

	if err := conn.Login(c.config.Username, c.password).Wait(); err != nil {
		_ = conn.Close()
		return fmt.Errorf("IMAP login: %w", err)
	}

	c.conn = conn
	c.selectedMailbox = ""
	c.logger.Debug("connected and authenticated", "user", c.config.Username)
	return nil
}

// reconnect closes the current connection and re-establishes it. Caller must hold mu.
func (c *Client) reconnect(ctx context.Context) error {
	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
	}
	c.selectedMailbox = ""
	c.mailboxCache = nil
	c.messageListCache = nil
	c.logger.Debug("reconnecting to IMAP server", "addr", c.config.Addr())
	return c.connect(ctx)
}

// withConn runs fn with the active connection, connecting if necessary.
// It holds the mutex for the duration of fn.
// If fn returns a network error the dead connection is cleared so the next
// call reconnects cleanly.
func (c *Client) withConn(ctx context.Context, fn func(*imapclient.Client) error) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.connect(ctx); err != nil {
		return err
	}
	err := fn(c.conn)
	if err != nil && isNetworkError(err) {
		if c.conn != nil {
			_ = c.conn.Close()
		}
		c.conn = nil
		c.selectedMailbox = ""
	}
	return err
}

// selectMailbox selects a mailbox if not already selected. Caller must hold mu.
func (c *Client) selectMailbox(mailbox string) error {
	if c.selectedMailbox == mailbox {
		return nil
	}
	if _, err := c.conn.Select(mailbox, nil).Wait(); err != nil {
		return fmt.Errorf("SELECT %q: %w", mailbox, err)
	}
	c.selectedMailbox = mailbox
	return nil
}

// listMailboxesLocked returns all selectable mailboxes, caching the result.
// Caller must hold mu.
func (c *Client) listMailboxesLocked() ([]string, error) {
	if c.mailboxCache != nil {
		return c.mailboxCache, nil
	}

	items, err := c.conn.List("", "*", nil).Collect()
	if err != nil {
		return nil, fmt.Errorf("LIST: %w", err)
	}

	var names []string
	for _, item := range items {
		if hasAttr(item.Attrs, imap.MailboxAttrNoSelect) {
			continue
		}
		names = append(names, item.Mailbox)
		if c.trashMailbox == "" && hasAttr(item.Attrs, imap.MailboxAttrTrash) {
			c.trashMailbox = item.Mailbox
		}
	}

	// Fallback: look for common trash folder names
	if c.trashMailbox == "" {
		for _, candidate := range []string{"Trash", "[Gmail]/Trash", "Deleted Items", "Deleted Messages"} {
			for _, mb := range names {
				if strings.EqualFold(mb, candidate) {
					c.trashMailbox = mb
					break
				}
			}
			if c.trashMailbox != "" {
				break
			}
		}
	}

	c.mailboxCache = names
	return names, nil
}

// buildMessageListCache enumerates all mailboxes and populates c.messageListCache.
// Caller must hold mu and have an active connection.
func (c *Client) buildMessageListCache(ctx context.Context) error {
	mailboxes, err := c.listMailboxesLocked()
	if err != nil {
		if isNetworkError(err) {
			if reconErr := c.reconnect(ctx); reconErr != nil {
				return fmt.Errorf("reconnect after LIST error: %w", reconErr)
			}
			mailboxes, err = c.listMailboxesLocked()
		}
		if err != nil {
			return err
		}
	}

	var messages []gmailapi.MessageID
	for _, mailbox := range mailboxes {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		if err := c.selectMailbox(mailbox); err != nil {
			if isNetworkError(err) {
				c.logger.Warn("network error selecting mailbox, reconnecting", "mailbox", mailbox, "error", err)
				if reconErr := c.reconnect(ctx); reconErr != nil {
					c.logger.Warn("reconnect failed, aborting list", "error", reconErr)
					break
				}
				if err := c.selectMailbox(mailbox); err != nil {
					c.logger.Warn("skipping mailbox after reconnect", "mailbox", mailbox, "error", err)
					continue
				}
			} else {
				c.logger.Warn("skipping mailbox", "mailbox", mailbox, "error", err)
				continue
			}
		}

		searchData, err := c.conn.UIDSearch(&imap.SearchCriteria{}, &imap.SearchOptions{ReturnAll: true}).Wait()
		if err != nil {
			if isNetworkError(err) {
				c.logger.Warn("network error during UID SEARCH, reconnecting", "mailbox", mailbox, "error", err)
				if reconErr := c.reconnect(ctx); reconErr != nil {
					c.logger.Warn("reconnect failed, aborting list", "error", reconErr)
					break
				}
				if selErr := c.selectMailbox(mailbox); selErr != nil {
					c.logger.Warn("skipping mailbox after reconnect", "mailbox", mailbox, "error", selErr)
					continue
				}
				searchData, err = c.conn.UIDSearch(&imap.SearchCriteria{}, &imap.SearchOptions{ReturnAll: true}).Wait()
				if err != nil {
					c.logger.Warn("UID SEARCH failed after reconnect", "mailbox", mailbox, "error", err)
					continue
				}
			} else {
				c.logger.Warn("UID SEARCH failed, skipping mailbox", "mailbox", mailbox, "error", err)
				continue
			}
		}

		uidSet, ok := searchData.All.(imap.UIDSet)
		if !ok {
			continue
		}
		uids, _ := uidSet.Nums()
		for _, uid := range uids {
			messages = append(messages, gmailapi.MessageID{
				ID:       compositeID(mailbox, uid),
				ThreadID: "",
			})
		}
		c.logger.Debug("listed mailbox", "mailbox", mailbox, "count", len(uids))
	}

	c.messageListCache = messages
	return nil
}

// isNetworkError reports whether err indicates the underlying TCP connection
// was closed or timed out, meaning the IMAP session must be re-established.
func isNetworkError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "use of closed network connection") ||
		strings.Contains(msg, "connection reset by peer") ||
		strings.Contains(msg, "broken pipe") ||
		strings.Contains(msg, "operation timed out") ||
		strings.Contains(msg, "EOF")
}

// hasAttr checks whether attr is in the attrs list.
func hasAttr(attrs []imap.MailboxAttr, attr imap.MailboxAttr) bool {
	for _, a := range attrs {
		if a == attr {
			return true
		}
	}
	return false
}

// compositeID builds a message identifier as "mailbox|uid".
func compositeID(mailbox string, uid imap.UID) string {
	return mailbox + "|" + strconv.FormatUint(uint64(uid), 10)
}

// parseCompositeID splits a composite message ID into mailbox and UID.
func parseCompositeID(id string) (mailbox string, uid imap.UID, err error) {
	idx := strings.LastIndexByte(id, '|')
	if idx < 0 {
		return "", 0, fmt.Errorf("invalid IMAP message ID %q (expected mailbox|uid)", id)
	}
	n, parseErr := strconv.ParseUint(id[idx+1:], 10, 32)
	if parseErr != nil {
		return "", 0, fmt.Errorf("invalid UID in message ID %q: %w", id, parseErr)
	}
	return id[:idx], imap.UID(n), nil
}

// GetProfile returns the IMAP account profile.
// Uses STATUS INBOX to get the message count; the username is used as the email address.
func (c *Client) GetProfile(ctx context.Context) (*gmailapi.Profile, error) {
	var profile gmailapi.Profile
	err := c.withConn(ctx, func(conn *imapclient.Client) error {
		statusData, err := conn.Status("INBOX", &imap.StatusOptions{NumMessages: true}).Wait()
		if err != nil {
			return fmt.Errorf("STATUS INBOX: %w", err)
		}
		var total int64
		if statusData.NumMessages != nil {
			total = int64(*statusData.NumMessages)
		}
		profile = gmailapi.Profile{
			EmailAddress:  c.config.Username,
			MessagesTotal: total,
			HistoryID:     0,
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &profile, nil
}

// ListLabels returns all IMAP mailboxes as labels.
func (c *Client) ListLabels(ctx context.Context) ([]*gmailapi.Label, error) {
	var labels []*gmailapi.Label
	err := c.withConn(ctx, func(conn *imapclient.Client) error {
		items, err := conn.List("", "*", nil).Collect()
		if err != nil {
			return fmt.Errorf("LIST: %w", err)
		}
		for _, item := range items {
			labelType := "user"
			if item.Mailbox == "INBOX" {
				labelType = "system"
			}
			labels = append(labels, &gmailapi.Label{
				ID:   item.Mailbox,
				Name: item.Mailbox,
				Type: labelType,
			})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return labels, nil
}

// ListMessages returns a page of message IDs from all IMAP mailboxes.
//
// The first call (pageToken == "") enumerates all mailboxes and caches the full
// list of message IDs; subsequent calls return successive pages of listPageSize
// using the returned NextPageToken as a numeric offset. This matches the Gmail
// pagination contract so the sync loop checkpoints and reports progress
// frequently on large mailboxes.
func (c *Client) ListMessages(ctx context.Context, query string, pageToken string) (*gmailapi.MessageListResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.connect(ctx); err != nil {
		return nil, err
	}

	// Build the full message ID list once per session.
	if c.messageListCache == nil {
		if err := c.buildMessageListCache(ctx); err != nil {
			return nil, err
		}
	}

	// Parse page offset from token.
	offset := 0
	if pageToken != "" {
		n, err := strconv.Atoi(pageToken)
		if err != nil || n < 0 {
			return &gmailapi.MessageListResponse{}, nil
		}
		offset = n
	}

	all := c.messageListCache
	total := int64(len(all))

	if offset >= len(all) {
		return &gmailapi.MessageListResponse{ResultSizeEstimate: total}, nil
	}

	end := offset + listPageSize
	if end > len(all) {
		end = len(all)
	}

	nextToken := ""
	if end < len(all) {
		nextToken = strconv.Itoa(end)
	}

	return &gmailapi.MessageListResponse{
		Messages:           all[offset:end],
		NextPageToken:      nextToken,
		ResultSizeEstimate: total,
	}, nil
}

// GetMessageRaw fetches a single IMAP message by composite ID.
func (c *Client) GetMessageRaw(ctx context.Context, messageID string) (*gmailapi.RawMessage, error) {
	msgs, err := c.GetMessagesRawBatch(ctx, []string{messageID})
	if err != nil {
		return nil, err
	}
	if len(msgs) == 0 || msgs[0] == nil {
		return nil, fmt.Errorf("message %s not found", messageID)
	}
	return msgs[0], nil
}

// GetMessagesRawBatch fetches multiple messages, grouping by mailbox for efficiency.
// Results are returned in the same order as messageIDs; nil entries indicate failures.
//
// UIDs per mailbox are fetched in chunks of fetchChunkSize to avoid huge FETCH
// commands that time out on large mailboxes. On network errors the connection is
// re-established and the failed chunk is retried once; if reconnect itself fails
// the function returns immediately with whatever results were collected.
func (c *Client) GetMessagesRawBatch(ctx context.Context, messageIDs []string) ([]*gmailapi.RawMessage, error) {
	type idxUID struct {
		idx int
		uid imap.UID
	}
	byMailbox := make(map[string][]idxUID, 4)
	for i, id := range messageIDs {
		mailbox, uid, err := parseCompositeID(id)
		if err != nil {
			c.logger.Warn("invalid message ID in batch", "id", id, "error", err)
			continue
		}
		byMailbox[mailbox] = append(byMailbox[mailbox], idxUID{i, uid})
	}

	results := make([]*gmailapi.RawMessage, len(messageIDs))
	fetchOpts := &imap.FetchOptions{
		UID:          true,
		InternalDate: true,
		RFC822Size:   true,
		BodySection:  []*imap.FetchItemBodySection{{}}, // empty section = entire message
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.connect(ctx); err != nil {
		return nil, err
	}

	for mailbox, items := range byMailbox {
		if ctx.Err() != nil {
			return results, ctx.Err()
		}

		if err := c.selectMailbox(mailbox); err != nil {
			if isNetworkError(err) {
				c.logger.Warn("network error selecting mailbox, reconnecting", "mailbox", mailbox, "error", err)
				if reconErr := c.reconnect(ctx); reconErr != nil {
					c.logger.Warn("reconnect failed, aborting batch", "error", reconErr)
					return results, nil
				}
				if err := c.selectMailbox(mailbox); err != nil {
					c.logger.Warn("skipping mailbox batch after reconnect", "mailbox", mailbox, "error", err)
					continue
				}
			} else {
				c.logger.Warn("skipping mailbox batch", "mailbox", mailbox, "error", err)
				continue
			}
		}

		// Build UID→result-index map for all items in this mailbox.
		uidToIdx := make(map[imap.UID]int, len(items))
		for _, item := range items {
			uidToIdx[item.uid] = item.idx
		}

		// Fetch in chunks to avoid huge UID FETCH commands that time out on
		// large mailboxes.
	chunkLoop:
		for chunkStart := 0; chunkStart < len(items); chunkStart += fetchChunkSize {
			if ctx.Err() != nil {
				return results, ctx.Err()
			}

			chunk := items[chunkStart:]
			if len(chunk) > fetchChunkSize {
				chunk = chunk[:fetchChunkSize]
			}

			var uidSet imap.UIDSet
			for _, item := range chunk {
				uidSet.AddNum(item.uid)
			}

			msgs, err := c.conn.Fetch(uidSet, fetchOpts).Collect()
			if err != nil {
				if isNetworkError(err) {
					c.logger.Warn("network error during UID FETCH, reconnecting", "mailbox", mailbox, "error", err)
					if reconErr := c.reconnect(ctx); reconErr != nil {
						c.logger.Warn("reconnect failed, aborting batch", "error", reconErr)
						return results, nil
					}
					if selErr := c.selectMailbox(mailbox); selErr != nil {
						c.logger.Warn("skipping remaining chunks after reconnect", "mailbox", mailbox, "error", selErr)
						break chunkLoop
					}
					msgs, err = c.conn.Fetch(uidSet, fetchOpts).Collect()
					if err != nil {
						c.logger.Warn("UID FETCH failed after reconnect", "mailbox", mailbox, "error", err)
						break chunkLoop
					}
				} else {
					c.logger.Warn("UID FETCH failed", "mailbox", mailbox, "error", err)
					break chunkLoop
				}
			}

			for _, msgBuf := range msgs {
				idx, ok := uidToIdx[msgBuf.UID]
				if !ok {
					continue
				}
				var rawMIME []byte
				if len(msgBuf.BodySection) > 0 {
					rawMIME = msgBuf.BodySection[0].Bytes
				}
				if len(rawMIME) == 0 {
					continue
				}
				msgID := compositeID(mailbox, msgBuf.UID)
				results[idx] = &gmailapi.RawMessage{
					ID:           msgID,
					ThreadID:     msgID,
					LabelIDs:     []string{mailbox},
					InternalDate: msgBuf.InternalDate.UnixMilli(),
					SizeEstimate: msgBuf.RFC822Size,
					Raw:          rawMIME,
				}
			}
		}
	}
	return results, nil
}

// ListHistory is not supported for IMAP servers.
// Callers should run a full sync instead of incremental sync for IMAP sources.
func (c *Client) ListHistory(_ context.Context, _ uint64, _ string) (*gmailapi.HistoryResponse, error) {
	return nil, fmt.Errorf("IMAP does not support history-based incremental sync")
}

// TrashMessage moves a message to the server's Trash folder.
func (c *Client) TrashMessage(ctx context.Context, messageID string) error {
	mailbox, uid, err := parseCompositeID(messageID)
	if err != nil {
		return err
	}
	return c.withConn(ctx, func(conn *imapclient.Client) error {
		if err := c.selectMailbox(mailbox); err != nil {
			return err
		}
		trashMailbox := c.trashMailbox
		if trashMailbox == "" {
			trashMailbox = "Trash"
		}
		var uidSet imap.UIDSet
		uidSet.AddNum(uid)
		if _, err := conn.Move(uidSet, trashMailbox).Wait(); err != nil {
			return fmt.Errorf("MOVE to %q: %w", trashMailbox, err)
		}
		return nil
	})
}

// DeleteMessage permanently deletes a message using UID STORE \Deleted + UID EXPUNGE.
func (c *Client) DeleteMessage(ctx context.Context, messageID string) error {
	mailbox, uid, err := parseCompositeID(messageID)
	if err != nil {
		return err
	}
	return c.withConn(ctx, func(conn *imapclient.Client) error {
		if err := c.selectMailbox(mailbox); err != nil {
			return err
		}
		var uidSet imap.UIDSet
		uidSet.AddNum(uid)
		if err := conn.Store(uidSet, &imap.StoreFlags{
			Op:     imap.StoreFlagsAdd,
			Silent: true,
			Flags:  []imap.Flag{imap.FlagDeleted},
		}, nil).Close(); err != nil {
			return fmt.Errorf("UID STORE \\Deleted: %w", err)
		}
		if err := conn.UIDExpunge(uidSet).Close(); err != nil {
			return fmt.Errorf("UID EXPUNGE: %w", err)
		}
		return nil
	})
}

// BatchDeleteMessages permanently deletes multiple messages.
func (c *Client) BatchDeleteMessages(ctx context.Context, messageIDs []string) error {
	for _, id := range messageIDs {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := c.DeleteMessage(ctx, id); err != nil {
			c.logger.Warn("failed to delete message", "id", id, "error", err)
		}
	}
	return nil
}

// Close logs out and disconnects from the IMAP server.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		return nil
	}
	conn := c.conn
	c.conn = nil
	c.selectedMailbox = ""
	return conn.Logout().Wait()
}
