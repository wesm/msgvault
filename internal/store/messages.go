package store

import (
	"bytes"
	"compress/zlib"
	"database/sql"
	"fmt"
	"io"
	"math/rand"
	"strings"
	"time"

	"github.com/wesm/msgvault/internal/mime"
)

// querier is satisfied by both *sql.DB and *sql.Tx, allowing
// helpers to run inside or outside a transaction.
type querier interface {
	Exec(query string, args ...any) (sql.Result, error)
	QueryRow(query string, args ...any) *sql.Row
}

// RecipientSet groups participant IDs and display names for one
// recipient type (from, to, cc, bcc).
type RecipientSet struct {
	Type           string
	ParticipantIDs []int64
	DisplayNames   []string
}

// MessagePersistData bundles everything needed to atomically
// persist a message and its related rows in a single transaction.
type MessagePersistData struct {
	Message    *Message
	BodyText   sql.NullString
	BodyHTML   sql.NullString
	RawMIME    []byte
	Recipients []RecipientSet
	LabelIDs   []int64
}

// Message represents a message in the database.
type Message struct {
	ID              int64
	ConversationID  int64
	SourceID        int64
	SourceMessageID string
	MessageType     string // "email"
	SentAt          sql.NullTime
	ReceivedAt      sql.NullTime
	InternalDate    sql.NullTime
	SenderID        sql.NullInt64
	IsFromMe        bool
	Subject         sql.NullString
	Snippet         sql.NullString
	SizeEstimate    int64
	HasAttachments  bool
	AttachmentCount int
	DeletedAt       sql.NullTime
	ArchivedAt      time.Time
}

// MessageExistsBatch checks which message IDs already exist in the database.
// Returns a map of source_message_id -> internal message_id for existing messages.
func (s *Store) MessageExistsBatch(sourceID int64, sourceMessageIDs []string) (map[string]int64, error) {
	if len(sourceMessageIDs) == 0 {
		return make(map[string]int64), nil
	}

	result := make(map[string]int64)
	err := queryInChunks(s.db, sourceMessageIDs, []interface{}{sourceID},
		`SELECT source_message_id, id FROM messages WHERE source_id = ? AND source_message_id IN (%s)`,
		func(rows *sql.Rows) error {
			var srcID string
			var id int64
			if err := rows.Scan(&srcID, &id); err != nil {
				return err
			}
			result[srcID] = id
			return nil
		})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// MessageExistsWithRawBatch checks which message IDs already exist in the database
// and have raw MIME data stored.
// Returns a map of source_message_id -> internal message_id.
func (s *Store) MessageExistsWithRawBatch(sourceID int64, sourceMessageIDs []string) (map[string]int64, error) {
	if len(sourceMessageIDs) == 0 {
		return make(map[string]int64), nil
	}

	result := make(map[string]int64)
	err := queryInChunks(s.db, sourceMessageIDs, []interface{}{sourceID},
		`SELECT m.source_message_id, m.id
		 FROM messages m
		 JOIN message_raw mr ON mr.message_id = m.id
		 WHERE m.source_id = ? AND m.source_message_id IN (%s)`,
		func(rows *sql.Rows) error {
			var srcID string
			var id int64
			if err := rows.Scan(&srcID, &id); err != nil {
				return err
			}
			result[srcID] = id
			return nil
		})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// EnsureConversation gets or creates a conversation (thread) for a message.
func (s *Store) EnsureConversation(sourceID int64, sourceConversationID, title string) (int64, error) {
	// Try to get existing
	var id int64
	err := s.db.QueryRow(`
		SELECT id FROM conversations
		WHERE source_id = ? AND source_conversation_id = ?
	`, sourceID, sourceConversationID).Scan(&id)

	if err == nil {
		return id, nil
	}
	if err != sql.ErrNoRows {
		return 0, err
	}

	// Create new
	result, err := s.db.Exec(`
		INSERT INTO conversations (source_id, source_conversation_id, conversation_type, title, created_at, updated_at)
		VALUES (?, ?, 'email_thread', ?, datetime('now'), datetime('now'))
	`, sourceID, sourceConversationID, title)
	if err != nil {
		return 0, err
	}

	return result.LastInsertId()
}

const upsertMessageSQL = `
	INSERT INTO messages (
		conversation_id, source_id, source_message_id, message_type,
		sent_at, received_at, internal_date, sender_id, is_from_me,
		subject, snippet, size_estimate,
		has_attachments, attachment_count, archived_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
	ON CONFLICT(source_id, source_message_id) DO UPDATE SET
		conversation_id = excluded.conversation_id,
		sent_at = excluded.sent_at,
		received_at = excluded.received_at,
		internal_date = excluded.internal_date,
		sender_id = excluded.sender_id,
		is_from_me = excluded.is_from_me,
		subject = excluded.subject,
		snippet = excluded.snippet,
		size_estimate = excluded.size_estimate,
		has_attachments = excluded.has_attachments,
		attachment_count = excluded.attachment_count`

// UpsertMessage inserts or updates a message.
func (s *Store) UpsertMessage(msg *Message) (int64, error) {
	return upsertMessage(s.db, msg)
}

func upsertMessage(q querier, msg *Message) (int64, error) {
	args := []any{
		msg.ConversationID, msg.SourceID, msg.SourceMessageID, msg.MessageType,
		msg.SentAt, msg.ReceivedAt, msg.InternalDate, msg.SenderID, msg.IsFromMe,
		msg.Subject, msg.Snippet, msg.SizeEstimate,
		msg.HasAttachments, msg.AttachmentCount,
	}

	// Use RETURNING to avoid an extra SELECT per message when supported.
	var id int64
	err := q.QueryRow(upsertMessageSQL+"\n\t\tRETURNING id\n\t", args...).Scan(&id)

	if err != nil {
		// SQLite < 3.35 does not support RETURNING. Fall back to an Exec + SELECT.
		if !isSQLiteError(err, "RETURNING") {
			return 0, err
		}

		if _, execErr := q.Exec(upsertMessageSQL, args...); execErr != nil {
			return 0, execErr
		}

		if err := q.QueryRow(
			`SELECT id FROM messages WHERE source_id = ? AND source_message_id = ?`,
			msg.SourceID, msg.SourceMessageID,
		).Scan(&id); err != nil {
			return 0, err
		}
	}
	return id, nil
}

// UpsertMessageBody stores the body text and HTML for a message in the separate message_bodies table.
func (s *Store) UpsertMessageBody(messageID int64, bodyText, bodyHTML sql.NullString) error {
	return upsertMessageBody(s.db, messageID, bodyText, bodyHTML)
}

func upsertMessageBody(q querier, messageID int64, bodyText, bodyHTML sql.NullString) error {
	_, err := q.Exec(`
		INSERT INTO message_bodies (message_id, body_text, body_html)
		VALUES (?, ?, ?)
		ON CONFLICT(message_id) DO UPDATE SET
			body_text = excluded.body_text,
			body_html = excluded.body_html
	`, messageID, bodyText, bodyHTML)
	return err
}

// UpsertMessageRaw stores the compressed raw MIME data for a message.
func (s *Store) UpsertMessageRaw(messageID int64, rawData []byte) error {
	return upsertMessageRaw(s.db, messageID, rawData)
}

func upsertMessageRaw(q querier, messageID int64, rawData []byte) error {
	// Compress with zlib
	var compressed bytes.Buffer
	w := zlib.NewWriter(&compressed)
	if _, err := w.Write(rawData); err != nil {
		return fmt.Errorf("compress: %w", err)
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("close compressor: %w", err)
	}

	_, err := q.Exec(`
		INSERT INTO message_raw (message_id, raw_data, raw_format, compression)
		VALUES (?, ?, 'mime', 'zlib')
		ON CONFLICT(message_id) DO UPDATE SET
			raw_data = excluded.raw_data,
			raw_format = excluded.raw_format,
			compression = excluded.compression
	`, messageID, compressed.Bytes())
	return err
}

// GetMessageRaw retrieves and decompresses the raw MIME data for a message.
func (s *Store) GetMessageRaw(messageID int64) ([]byte, error) {
	var compressed []byte
	var compression sql.NullString

	err := s.db.QueryRow(`
		SELECT raw_data, compression FROM message_raw WHERE message_id = ?
	`, messageID).Scan(&compressed, &compression)
	if err != nil {
		return nil, err
	}

	if compression.Valid && compression.String == "zlib" {
		r, err := zlib.NewReader(bytes.NewReader(compressed))
		if err != nil {
			return nil, fmt.Errorf("zlib reader: %w", err)
		}
		defer r.Close()
		return io.ReadAll(r)
	}

	return compressed, nil
}

// PersistMessage atomically stores a message plus its body, raw MIME,
// recipients, and labels in a single transaction. Returns the message ID.
func (s *Store) PersistMessage(data *MessagePersistData) (int64, error) {
	var messageID int64
	err := s.withTx(func(tx *sql.Tx) error {
		id, err := upsertMessage(tx, data.Message)
		if err != nil {
			return fmt.Errorf("upsert message: %w", err)
		}
		messageID = id

		if err := upsertMessageBody(tx, messageID, data.BodyText, data.BodyHTML); err != nil {
			return fmt.Errorf("upsert body: %w", err)
		}

		if len(data.RawMIME) > 0 {
			if err := upsertMessageRaw(tx, messageID, data.RawMIME); err != nil {
				return fmt.Errorf("store raw: %w", err)
			}
		}

		for _, rs := range data.Recipients {
			if err := replaceMessageRecipientsTx(tx, messageID, rs.Type, rs.ParticipantIDs, rs.DisplayNames); err != nil {
				return fmt.Errorf("store %s recipients: %w", rs.Type, err)
			}
		}

		if err := replaceMessageLabelsTx(tx, messageID, data.LabelIDs); err != nil {
			return fmt.Errorf("store labels: %w", err)
		}

		return nil
	})
	return messageID, err
}

// Participant represents a person in the participants table.
type Participant struct {
	ID           int64
	EmailAddress sql.NullString
	DisplayName  sql.NullString
	Domain       sql.NullString
}

// EnsureParticipant gets or creates a participant by email.
func (s *Store) EnsureParticipant(email, displayName, domain string) (int64, error) {
	// Try to get existing
	var id int64
	err := s.db.QueryRow(`
		SELECT id FROM participants WHERE email_address = ?
	`, email).Scan(&id)

	if err == nil {
		return id, nil
	}
	if err != sql.ErrNoRows {
		return 0, err
	}

	// Create new
	result, err := s.db.Exec(`
		INSERT INTO participants (email_address, display_name, domain, created_at, updated_at)
		VALUES (?, ?, ?, datetime('now'), datetime('now'))
	`, email, displayName, domain)
	if err != nil {
		return 0, err
	}

	return result.LastInsertId()
}

// EnsureParticipantsBatch gets or creates participants in batch.
// Returns a map of email -> participant ID.
func (s *Store) EnsureParticipantsBatch(addresses []mime.Address) (map[string]int64, error) {
	if len(addresses) == 0 {
		return make(map[string]int64), nil
	}

	result := make(map[string]int64)

	// First, try to insert all (ignoring conflicts)
	for _, addr := range addresses {
		if addr.Email == "" {
			continue
		}
		_, err := s.db.Exec(`
			INSERT OR IGNORE INTO participants (email_address, display_name, domain, created_at, updated_at)
			VALUES (?, ?, ?, datetime('now'), datetime('now'))
		`, addr.Email, addr.Name, addr.Domain)
		if err != nil {
			return nil, err
		}
	}

	// Then fetch all IDs
	emails := make([]string, 0, len(addresses))
	for _, addr := range addresses {
		if addr.Email != "" {
			emails = append(emails, addr.Email)
		}
	}

	if len(emails) == 0 {
		return result, nil
	}

	err := queryInChunks(s.db, emails, nil,
		`SELECT email_address, id FROM participants WHERE email_address IN (%s)`,
		func(rows *sql.Rows) error {
			var email string
			var id int64
			if err := rows.Scan(&email, &id); err != nil {
				return err
			}
			result[email] = id
			return nil
		})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// ReplaceMessageRecipients replaces all recipients for a message atomically.
func (s *Store) ReplaceMessageRecipients(messageID int64, recipientType string, participantIDs []int64, displayNames []string) error {
	return s.withTx(func(tx *sql.Tx) error {
		return replaceMessageRecipientsTx(tx, messageID, recipientType, participantIDs, displayNames)
	})
}

func replaceMessageRecipientsTx(tx *sql.Tx, messageID int64, recipientType string, participantIDs []int64, displayNames []string) error {
	_, err := tx.Exec(`
		DELETE FROM message_recipients WHERE message_id = ? AND recipient_type = ?
	`, messageID, recipientType)
	if err != nil {
		return err
	}

	if len(participantIDs) == 0 {
		return nil
	}

	return insertInChunks(tx, len(participantIDs), 4,
		"INSERT INTO message_recipients (message_id, participant_id, recipient_type, display_name) VALUES ",
		func(start, end int) ([]string, []interface{}) {
			values := make([]string, end-start)
			args := make([]interface{}, 0, (end-start)*4)
			for i := start; i < end; i++ {
				values[i-start] = "(?, ?, ?, ?)"
				displayName := ""
				if i < len(displayNames) {
					displayName = displayNames[i]
				}
				args = append(args, messageID, participantIDs[i], recipientType, displayName)
			}
			return values, args
		})
}

// Label represents a Gmail label.
type Label struct {
	ID            int64
	SourceID      sql.NullInt64
	SourceLabelID sql.NullString
	Name          string
	LabelType     sql.NullString
}

// EnsureLabel gets or creates a label.
func (s *Store) EnsureLabel(sourceID int64, sourceLabelID, name, labelType string) (int64, error) {
	// Try to get existing
	var id int64
	err := s.db.QueryRow(`
		SELECT id FROM labels WHERE source_id = ? AND source_label_id = ?
	`, sourceID, sourceLabelID).Scan(&id)

	if err == nil {
		return id, nil
	}
	if err != sql.ErrNoRows {
		return 0, err
	}

	// Create new
	result, err := s.db.Exec(`
		INSERT INTO labels (source_id, source_label_id, name, label_type)
		VALUES (?, ?, ?, ?)
	`, sourceID, sourceLabelID, name, labelType)
	if err != nil {
		return 0, err
	}

	return result.LastInsertId()
}

// LabelInfo holds the name and type for a label to be ensured.
type LabelInfo struct {
	Name string
	Type string // "system" or "user"
}

// IsSystemLabel returns true if the given Gmail label ID represents a system label.
func IsSystemLabel(sourceLabelID string) bool {
	switch sourceLabelID {
	case "INBOX", "SENT", "TRASH", "SPAM", "DRAFT", "UNREAD", "STARRED", "IMPORTANT":
		return true
	}
	return strings.HasPrefix(sourceLabelID, "CATEGORY_")
}

// EnsureLabelsBatch ensures all labels exist and returns a map of source_label_id -> internal ID.
func (s *Store) EnsureLabelsBatch(sourceID int64, labels map[string]LabelInfo) (map[string]int64, error) {
	result := make(map[string]int64)

	for sourceLabelID, info := range labels {
		id, err := s.EnsureLabel(sourceID, sourceLabelID, info.Name, info.Type)
		if err != nil {
			return nil, err
		}
		result[sourceLabelID] = id
	}

	return result, nil
}

// ReplaceMessageLabels replaces all labels for a message atomically.
func (s *Store) ReplaceMessageLabels(messageID int64, labelIDs []int64) error {
	return s.withTx(func(tx *sql.Tx) error {
		return replaceMessageLabelsTx(tx, messageID, labelIDs)
	})
}

func replaceMessageLabelsTx(tx *sql.Tx, messageID int64, labelIDs []int64) error {
	_, err := tx.Exec(`
		DELETE FROM message_labels WHERE message_id = ?
	`, messageID)
	if err != nil {
		return err
	}

	if len(labelIDs) == 0 {
		return nil
	}

	return insertInChunks(tx, len(labelIDs), 2,
		"INSERT INTO message_labels (message_id, label_id) VALUES ",
		func(start, end int) ([]string, []interface{}) {
			values := make([]string, end-start)
			args := make([]interface{}, 0, (end-start)*2)
			for i := start; i < end; i++ {
				values[i-start] = "(?, ?)"
				args = append(args, messageID, labelIDs[i])
			}
			return values, args
		})
}

// AddMessageLabels adds labels to a message without removing existing ones.
// Uses INSERT OR IGNORE to skip labels that already exist.
func (s *Store) AddMessageLabels(messageID int64, labelIDs []int64) error {
	if len(labelIDs) == 0 {
		return nil
	}
	return s.withTx(func(tx *sql.Tx) error {
		return insertInChunks(tx, len(labelIDs), 2,
			"INSERT OR IGNORE INTO message_labels (message_id, label_id) VALUES ",
			func(start, end int) ([]string, []interface{}) {
				values := make([]string, end-start)
				args := make([]interface{}, 0, (end-start)*2)
				for i := start; i < end; i++ {
					values[i-start] = "(?, ?)"
					args = append(args, messageID, labelIDs[i])
				}
				return values, args
			})
	})
}

// RemoveMessageLabels removes specific labels from a message.
func (s *Store) RemoveMessageLabels(messageID int64, labelIDs []int64) error {
	if len(labelIDs) == 0 {
		return nil
	}
	return execInChunks(s.db, labelIDs, []interface{}{messageID},
		`DELETE FROM message_labels WHERE message_id = ? AND label_id IN (%s)`)
}

// MarkMessageDeleted marks a message as deleted from the source.
func (s *Store) MarkMessageDeleted(sourceID int64, sourceMessageID string) error {
	_, err := s.db.Exec(`
		UPDATE messages
		SET deleted_from_source_at = datetime('now')
		WHERE source_id = ? AND source_message_id = ?
	`, sourceID, sourceMessageID)
	return err
}

// MarkMessagesDeletedBatch marks multiple messages as deleted from the source in a single transaction.
func (s *Store) MarkMessagesDeletedBatch(sourceID int64, sourceMessageIDs []string) error {
	if len(sourceMessageIDs) == 0 {
		return nil
	}
	return execInChunks(s.db, sourceMessageIDs, []interface{}{sourceID},
		`UPDATE messages SET deleted_from_source_at = datetime('now') WHERE source_id = ? AND source_message_id IN (%s)`)
}

// MarkMessageDeletedByGmailID marks a message as deleted by its Gmail ID.
// This is used by the deletion executor which only has the Gmail message ID.
// When permanent is true, the message row is deleted entirely; otherwise it is
// soft-deleted by setting deleted_from_source_at.
func (s *Store) MarkMessageDeletedByGmailID(permanent bool, gmailID string) error {
	if permanent {
		_, err := s.db.Exec(`DELETE FROM messages WHERE source_message_id = ?`, gmailID)
		return err
	}
	_, err := s.db.Exec(`
		UPDATE messages
		SET deleted_from_source_at = datetime('now')
		WHERE source_message_id = ?
	`, gmailID)
	return err
}

// MarkMessagesDeletedByGmailIDBatch marks multiple messages as deleted by their Gmail IDs
// in batched UPDATE statements. Much faster than individual MarkMessageDeletedByGmailID calls
// because it issues one UPDATE per chunk instead of one per message.
//
// Uses best-effort semantics: if a chunk fails, it falls back to individual updates
// for that chunk and continues with remaining chunks. Returns the first error encountered
// (if any) after processing all IDs.
func (s *Store) MarkMessagesDeletedByGmailIDBatch(gmailIDs []string) error {
	if len(gmailIDs) == 0 {
		return nil
	}

	const chunkSize = 500
	var firstErr error

	for i := 0; i < len(gmailIDs); i += chunkSize {
		end := i + chunkSize
		if end > len(gmailIDs) {
			end = len(gmailIDs)
		}
		chunk := gmailIDs[i:end]

		placeholders := make([]string, len(chunk))
		args := make([]interface{}, len(chunk))
		for j, id := range chunk {
			placeholders[j] = "?"
			args[j] = id
		}

		query := fmt.Sprintf(
			`UPDATE messages SET deleted_from_source_at = datetime('now') WHERE source_message_id IN (%s)`,
			strings.Join(placeholders, ","))

		if _, err := s.db.Exec(query, args...); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			// Fall back to individual updates for this chunk
			for _, id := range chunk {
				s.MarkMessageDeletedByGmailID(false, id) //nolint:errcheck // best-effort
			}
		}
	}

	return firstErr
}

// CountMessagesForSource returns the count of messages for a specific source (account).
func (s *Store) CountMessagesForSource(sourceID int64) (int64, error) {
	var count int64
	err := s.db.QueryRow(`
		SELECT COUNT(*) FROM messages WHERE source_id = ? AND deleted_from_source_at IS NULL
	`, sourceID).Scan(&count)
	return count, err
}

// CountMessagesWithRaw returns the count of messages that have raw MIME stored.
func (s *Store) CountMessagesWithRaw(sourceID int64) (int64, error) {
	var count int64
	err := s.db.QueryRow(`
		SELECT COUNT(*) FROM messages m
		JOIN message_raw mr ON m.id = mr.message_id
		WHERE m.source_id = ? AND m.deleted_from_source_at IS NULL
	`, sourceID).Scan(&count)
	return count, err
}

// GetRandomMessageIDs returns a random sample of message IDs for a source.
// Uses reservoir sampling with random offsets for O(limit) performance on large tables,
// falling back to ORDER BY RANDOM() for small tables where the overhead isn't significant.
func (s *Store) GetRandomMessageIDs(sourceID int64, limit int) ([]int64, error) {
	// Get total count first
	var total int64
	err := s.db.QueryRow(s.Rebind(`
		SELECT COUNT(*) FROM messages
		WHERE source_id = ? AND deleted_from_source_at IS NULL
	`), sourceID).Scan(&total)
	if err != nil {
		return nil, err
	}

	if total == 0 {
		return nil, nil
	}

	// For small tables or when limit >= total, use simple ORDER BY RANDOM()
	// The threshold of 10000 balances query overhead vs. scan cost
	if total < 10000 || int64(limit) >= total {
		rows, err := s.db.Query(s.Rebind(`
			SELECT id FROM messages
			WHERE source_id = ? AND deleted_from_source_at IS NULL
			ORDER BY RANDOM()
			LIMIT ?
		`), sourceID, limit)
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		var ids []int64
		for rows.Next() {
			var id int64
			if err := rows.Scan(&id); err != nil {
				return nil, err
			}
			ids = append(ids, id)
		}
		return ids, rows.Err()
	}

	// For large tables, use random offset sampling
	// This is O(limit) instead of O(n) for ORDER BY RANDOM()
	// Generate random offsets in Go for dialect portability (SQLite vs Postgres)
	// Use explicitly seeded RNG for true randomness across process runs
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	ids := make([]int64, 0, limit)
	seen := make(map[int64]bool)

	for len(ids) < limit {
		// Generate random offset in Go (portable across SQLite/Postgres)
		offset := rng.Int63n(total)

		var id int64
		err := s.db.QueryRow(s.Rebind(`
			SELECT id FROM messages
			WHERE source_id = ? AND deleted_from_source_at IS NULL
			ORDER BY id
			LIMIT 1 OFFSET ?
		`), sourceID, offset).Scan(&id)
		if err != nil {
			if err == sql.ErrNoRows {
				continue // Race condition with deletions, retry
			}
			return nil, err
		}

		if !seen[id] {
			seen[id] = true
			ids = append(ids, id)
		}
	}

	return ids, nil
}

// UpsertFTS inserts or replaces an FTS row for a message.
// No-op if FTS5 is not available.
func (s *Store) UpsertFTS(messageID int64, subject, bodyText, fromAddr, toAddrs, ccAddrs string) error {
	if !s.fts5Available {
		return nil
	}
	_, err := s.db.Exec(`
		INSERT OR REPLACE INTO messages_fts(rowid, message_id, subject, body, from_addr, to_addr, cc_addr)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`, messageID, messageID, subject, bodyText, fromAddr, toAddrs, ccAddrs)
	return err
}

// BackfillFTS populates the FTS table from existing message data.
// Processes in batches to avoid blocking for minutes on large archives.
// The progress callback (if non-nil) is called after each batch with
// (position in ID range, total ID range). Each batch is committed
// independently so partial progress is preserved if interrupted.
// Returns the number of rows inserted. No-op if FTS5 is not available.
func (s *Store) BackfillFTS(progress func(done, total int64)) (int64, error) {
	if !s.fts5Available {
		return 0, nil
	}

	const batchSize = 5000

	// Use MIN/MAX (instant B-tree lookups) instead of COUNT(*) (full scan)
	var minID, maxID int64
	err := s.db.QueryRow("SELECT COALESCE(MIN(id),0), COALESCE(MAX(id),0) FROM messages").Scan(&minID, &maxID)
	if err != nil {
		return 0, fmt.Errorf("get message ID range: %w", err)
	}
	if maxID == 0 {
		return 0, nil
	}
	idRange := maxID - minID + 1

	// Clear existing FTS data
	if _, err := s.db.Exec("DELETE FROM messages_fts"); err != nil {
		return 0, fmt.Errorf("clear FTS: %w", err)
	}

	var indexed int64
	cursor := minID

	for cursor <= maxID {
		batchEnd := cursor + batchSize
		n, err := s.backfillFTSBatch(cursor, batchEnd)
		if err != nil {
			return indexed, fmt.Errorf("backfill batch [%d,%d): %w", cursor, batchEnd, err)
		}
		indexed += n
		cursor = batchEnd

		if progress != nil {
			pos := cursor - minID
			if pos > idRange {
				pos = idRange
			}
			progress(pos, idRange)
		}
	}

	return indexed, nil
}

// backfillFTSBatch inserts FTS rows for messages with id in [fromID, toID).
func (s *Store) backfillFTSBatch(fromID, toID int64) (int64, error) {
	result, err := s.db.Exec(`
		INSERT OR REPLACE INTO messages_fts (rowid, message_id, subject, body, from_addr, to_addr, cc_addr)
		SELECT m.id, m.id, COALESCE(m.subject, ''), COALESCE(mb.body_text, ''),
			COALESCE((SELECT GROUP_CONCAT(p.email_address, ' ') FROM message_recipients mr JOIN participants p ON p.id = mr.participant_id WHERE mr.message_id = m.id AND mr.recipient_type = 'from'), ''),
			COALESCE((SELECT GROUP_CONCAT(p.email_address, ' ') FROM message_recipients mr JOIN participants p ON p.id = mr.participant_id WHERE mr.message_id = m.id AND mr.recipient_type = 'to'), ''),
			COALESCE((SELECT GROUP_CONCAT(p.email_address, ' ') FROM message_recipients mr JOIN participants p ON p.id = mr.participant_id WHERE mr.message_id = m.id AND mr.recipient_type = 'cc'), '')
		FROM messages m
		LEFT JOIN message_bodies mb ON mb.message_id = m.id
		WHERE m.id >= ? AND m.id < ?
	`, fromID, toID)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// EnsureConversationWithType gets or creates a conversation with an explicit conversation_type.
// Unlike EnsureConversation (which hardcodes 'email_thread'), this accepts the type as a parameter,
// making it suitable for WhatsApp and other messaging platforms.
func (s *Store) EnsureConversationWithType(sourceID int64, sourceConversationID, conversationType, title string) (int64, error) {
	// Try to get existing
	var id int64
	err := s.db.QueryRow(`
		SELECT id FROM conversations
		WHERE source_id = ? AND source_conversation_id = ?
	`, sourceID, sourceConversationID).Scan(&id)

	if err == nil {
		return id, nil
	}
	if err != sql.ErrNoRows {
		return 0, err
	}

	// Create new
	result, err := s.db.Exec(`
		INSERT INTO conversations (source_id, source_conversation_id, conversation_type, title, created_at, updated_at)
		VALUES (?, ?, ?, ?, datetime('now'), datetime('now'))
	`, sourceID, sourceConversationID, conversationType, title)
	if err != nil {
		return 0, err
	}

	return result.LastInsertId()
}

// EnsureParticipantByPhone gets or creates a participant by phone number.
// Phone must start with "+" (E.164 format). Returns an error for empty or
// invalid phone numbers to prevent database pollution.
// Also creates a participant_identifiers row with identifier_type='whatsapp'.
func (s *Store) EnsureParticipantByPhone(phone, displayName string) (int64, error) {
	if phone == "" {
		return 0, fmt.Errorf("phone number is required")
	}
	if !strings.HasPrefix(phone, "+") {
		return 0, fmt.Errorf("phone number must be in E.164 format (starting with +), got %q", phone)
	}

	// Try to get existing by phone
	var id int64
	err := s.db.QueryRow(`
		SELECT id FROM participants WHERE phone_number = ?
	`, phone).Scan(&id)

	if err == nil {
		// Update display name if provided and currently empty
		if displayName != "" {
			s.db.Exec(`
				UPDATE participants SET display_name = ?
				WHERE id = ? AND (display_name IS NULL OR display_name = '')
			`, displayName, id) //nolint:errcheck // best-effort display name update
		}
		return id, nil
	}
	if err != sql.ErrNoRows {
		return 0, err
	}

	// Create new participant
	result, err := s.db.Exec(`
		INSERT INTO participants (phone_number, display_name, created_at, updated_at)
		VALUES (?, ?, datetime('now'), datetime('now'))
	`, phone, displayName)
	if err != nil {
		return 0, fmt.Errorf("insert participant: %w", err)
	}

	id, err = result.LastInsertId()
	if err != nil {
		return 0, err
	}

	// Also create a participant_identifiers row
	_, err = s.db.Exec(`
		INSERT OR IGNORE INTO participant_identifiers (participant_id, identifier_type, identifier_value, is_primary)
		VALUES (?, 'whatsapp', ?, TRUE)
	`, id, phone)
	if err != nil {
		return 0, fmt.Errorf("insert participant identifier: %w", err)
	}

	return id, nil
}

// UpdateParticipantDisplayNameByPhone updates the display_name for an existing
// participant identified by phone number. Only updates if display_name is currently
// empty. Returns true if a participant was found and updated, false if not found
// or name was already set. Does NOT create new participants.
func (s *Store) UpdateParticipantDisplayNameByPhone(phone, displayName string) (bool, error) {
	if phone == "" || displayName == "" {
		return false, nil
	}

	result, err := s.db.Exec(`
		UPDATE participants SET display_name = ?, updated_at = datetime('now')
		WHERE phone_number = ? AND (display_name IS NULL OR display_name = '')
	`, displayName, phone)
	if err != nil {
		return false, err
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return rows > 0, nil
}

// EnsureConversationParticipant adds a participant to a conversation.
// Uses INSERT OR IGNORE to be idempotent.
func (s *Store) EnsureConversationParticipant(conversationID, participantID int64, role string) error {
	_, err := s.db.Exec(`
		INSERT OR IGNORE INTO conversation_participants (conversation_id, participant_id, role, joined_at)
		VALUES (?, ?, ?, datetime('now'))
	`, conversationID, participantID, role)
	return err
}

// UpsertReaction inserts or ignores a reaction.
func (s *Store) UpsertReaction(messageID, participantID int64, reactionType, reactionValue string, createdAt time.Time) error {
	_, err := s.db.Exec(`
		INSERT OR IGNORE INTO reactions (message_id, participant_id, reaction_type, reaction_value, created_at)
		VALUES (?, ?, ?, ?, ?)
	`, messageID, participantID, reactionType, reactionValue, createdAt)
	return err
}

// UpsertMessageRawWithFormat stores compressed raw data with an explicit format.
// Unlike UpsertMessageRaw (which hardcodes 'mime'), this accepts the format as a parameter.
func (s *Store) UpsertMessageRawWithFormat(messageID int64, rawData []byte, format string) error {
	// Compress with zlib
	var compressed bytes.Buffer
	w := zlib.NewWriter(&compressed)
	if _, err := w.Write(rawData); err != nil {
		return fmt.Errorf("compress: %w", err)
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("close compressor: %w", err)
	}

	_, err := s.db.Exec(`
		INSERT INTO message_raw (message_id, raw_data, raw_format, compression)
		VALUES (?, ?, ?, 'zlib')
		ON CONFLICT(message_id) DO UPDATE SET
			raw_data = excluded.raw_data,
			raw_format = excluded.raw_format,
			compression = excluded.compression
	`, messageID, compressed.Bytes(), format)
	return err
}

// UpsertAttachment stores an attachment record.
func (s *Store) UpsertAttachment(messageID int64, filename, mimeType, storagePath, contentHash string, size int) error {
	// Check if attachment already exists (by message_id and content_hash)
	var existingID int64
	err := s.db.QueryRow(`
		SELECT id FROM attachments WHERE message_id = ? AND content_hash = ?
	`, messageID, contentHash).Scan(&existingID)

	if err == nil {
		// Already exists, nothing to do
		return nil
	}
	if err != sql.ErrNoRows {
		return err
	}

	// Insert new attachment
	_, err = s.db.Exec(`
		INSERT INTO attachments (message_id, filename, mime_type, storage_path, content_hash, size, created_at)
		VALUES (?, ?, ?, ?, ?, ?, datetime('now'))
	`, messageID, filename, mimeType, storagePath, contentHash, size)
	return err
}
