package store

import (
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// APIMessage represents a message for API responses.
type APIMessage struct {
	ID             int64
	Subject        string
	From           string
	To             []string
	SentAt         time.Time
	Snippet        string
	Labels         []string
	HasAttachments bool
	SizeEstimate   int64
	Body           string
	Headers        map[string]string
	Attachments    []APIAttachment
}

// APIAttachment represents attachment metadata for API responses.
type APIAttachment struct {
	Filename string
	MimeType string
	Size     int64
}

// ListMessages returns a paginated list of messages with batch-loaded recipients and labels.
func (s *Store) ListMessages(offset, limit int) ([]APIMessage, int64, error) {
	// Get total count
	var total int64
	err := s.db.QueryRow("SELECT COUNT(*) FROM messages WHERE deleted_from_source_at IS NULL").Scan(&total)
	if err != nil {
		return nil, 0, err
	}

	// Query messages with sender info
	query := `
		SELECT
			m.id,
			COALESCE(m.subject, '') as subject,
			COALESCE(p.email_address, '') as from_email,
			COALESCE(m.sent_at, m.received_at, m.internal_date) as sent_at,
			COALESCE(m.snippet, '') as snippet,
			m.has_attachments,
			m.size_estimate
		FROM messages m
		LEFT JOIN message_recipients mr ON mr.message_id = m.id AND mr.recipient_type = 'from'
		LEFT JOIN participants p ON p.id = mr.participant_id
		WHERE m.deleted_from_source_at IS NULL
		ORDER BY COALESCE(m.sent_at, m.received_at, m.internal_date) DESC
		LIMIT ? OFFSET ?
	`

	rows, err := s.db.Query(query, limit, offset)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var messages []APIMessage
	var ids []int64
	for rows.Next() {
		var m APIMessage
		var sentAt sql.NullTime
		err := rows.Scan(&m.ID, &m.Subject, &m.From, &sentAt, &m.Snippet, &m.HasAttachments, &m.SizeEstimate)
		if err != nil {
			return nil, 0, err
		}
		if sentAt.Valid {
			m.SentAt = sentAt.Time
		}
		messages = append(messages, m)
		ids = append(ids, m.ID)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}

	if len(ids) == 0 {
		return messages, total, nil
	}

	// Batch-load recipients for all messages
	recipientMap, err := s.batchGetRecipients(ids, "to")
	if err != nil {
		return nil, 0, err
	}

	// Batch-load labels for all messages
	labelMap, err := s.batchGetLabels(ids)
	if err != nil {
		return nil, 0, err
	}

	for i := range messages {
		messages[i].To = recipientMap[messages[i].ID]
		messages[i].Labels = labelMap[messages[i].ID]
	}

	return messages, total, nil
}

// GetMessage returns a single message with full details.
// Only this method accesses message_bodies (single PK lookup).
func (s *Store) GetMessage(id int64) (*APIMessage, error) {
	query := `
		SELECT
			m.id,
			COALESCE(m.subject, '') as subject,
			COALESCE(p.email_address, '') as from_email,
			COALESCE(m.sent_at, m.received_at, m.internal_date) as sent_at,
			COALESCE(m.snippet, '') as snippet,
			m.has_attachments,
			m.size_estimate
		FROM messages m
		LEFT JOIN message_recipients mr ON mr.message_id = m.id AND mr.recipient_type = 'from'
		LEFT JOIN participants p ON p.id = mr.participant_id
		WHERE m.id = ? AND m.deleted_from_source_at IS NULL
	`

	var m APIMessage
	var sentAt sql.NullTime
	err := s.db.QueryRow(query, id).Scan(&m.ID, &m.Subject, &m.From, &sentAt, &m.Snippet, &m.HasAttachments, &m.SizeEstimate)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if sentAt.Valid {
		m.SentAt = sentAt.Time
	}

	// Get recipients (single message, per-row is fine)
	m.To, err = s.getRecipients(m.ID, "to")
	if err != nil {
		return nil, err
	}

	// Get labels (single message, per-row is fine)
	m.Labels, err = s.getLabels(m.ID)
	if err != nil {
		return nil, err
	}

	// Get body (single PK lookup — only place we touch message_bodies)
	var bodyText, bodyHTML sql.NullString
	err = s.db.QueryRow("SELECT body_text, body_html FROM message_bodies WHERE message_id = ?", id).Scan(&bodyText, &bodyHTML)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("get message body: %w", err)
	}
	if bodyText.Valid {
		m.Body = bodyText.String
	} else if bodyHTML.Valid {
		m.Body = bodyHTML.String
	}

	// Get attachments
	attRows, err := s.db.Query("SELECT filename, mime_type, size FROM attachments WHERE message_id = ?", id)
	if err == nil {
		defer attRows.Close()
		for attRows.Next() {
			var att APIAttachment
			if err := attRows.Scan(&att.Filename, &att.MimeType, &att.Size); err == nil {
				m.Attachments = append(m.Attachments, att)
			}
		}
	}

	m.Headers = make(map[string]string)

	return &m, nil
}

// SearchMessages searches messages using FTS5, with batch-loaded recipients and labels.
func (s *Store) SearchMessages(query string, offset, limit int) ([]APIMessage, int64, error) {
	// First try FTS5 search
	ftsQuery := `
		SELECT
			m.id,
			COALESCE(m.subject, '') as subject,
			COALESCE(p.email_address, '') as from_email,
			COALESCE(m.sent_at, m.received_at, m.internal_date) as sent_at,
			COALESCE(m.snippet, '') as snippet,
			m.has_attachments,
			m.size_estimate
		FROM messages_fts fts
		JOIN messages m ON m.id = fts.rowid
		LEFT JOIN message_recipients mr ON mr.message_id = m.id AND mr.recipient_type = 'from'
		LEFT JOIN participants p ON p.id = mr.participant_id
		WHERE messages_fts MATCH ? AND m.deleted_from_source_at IS NULL
		ORDER BY rank
		LIMIT ? OFFSET ?
	`

	rows, err := s.db.Query(ftsQuery, query, limit, offset)
	if err != nil {
		// FTS5 might not be available, fall back to LIKE search
		return s.searchMessagesLike(query, offset, limit)
	}
	defer rows.Close()

	messages, ids, err := scanMessageRows(rows)
	if err != nil {
		return nil, 0, err
	}

	if len(ids) == 0 {
		return []APIMessage{}, 0, nil
	}

	// Get total count
	var total int64
	countQuery := `
		SELECT COUNT(*)
		FROM messages_fts fts
		JOIN messages m ON m.id = fts.rowid
		WHERE messages_fts MATCH ? AND m.deleted_from_source_at IS NULL
	`
	if err := s.db.QueryRow(countQuery, query).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count FTS results: %w", err)
	}

	// Batch-load recipients and labels
	if err := s.batchPopulate(messages, ids); err != nil {
		return nil, 0, err
	}

	return messages, total, nil
}

// escapeLike escapes SQL LIKE special characters (%, _) so they are
// matched literally. The escaped string should be used with ESCAPE '\'.
func escapeLike(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `%`, `\%`)
	s = strings.ReplaceAll(s, `_`, `\_`)
	return s
}

// searchMessagesLike is a fallback search using LIKE with batch-loaded recipients and labels.
func (s *Store) searchMessagesLike(query string, offset, limit int) ([]APIMessage, int64, error) {
	likePattern := "%" + escapeLike(query) + "%"

	countQuery := `
		SELECT COUNT(*) FROM messages
		WHERE deleted_from_source_at IS NULL
		AND (subject LIKE ? ESCAPE '\' OR snippet LIKE ? ESCAPE '\')
	`
	var total int64
	if err := s.db.QueryRow(countQuery, likePattern, likePattern).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count search results: %w", err)
	}

	searchQuery := `
		SELECT
			m.id,
			COALESCE(m.subject, '') as subject,
			COALESCE(p.email_address, '') as from_email,
			COALESCE(m.sent_at, m.received_at, m.internal_date) as sent_at,
			COALESCE(m.snippet, '') as snippet,
			m.has_attachments,
			m.size_estimate
		FROM messages m
		LEFT JOIN message_recipients mr ON mr.message_id = m.id AND mr.recipient_type = 'from'
		LEFT JOIN participants p ON p.id = mr.participant_id
		WHERE m.deleted_from_source_at IS NULL
		AND (m.subject LIKE ? ESCAPE '\' OR m.snippet LIKE ? ESCAPE '\')
		ORDER BY COALESCE(m.sent_at, m.received_at, m.internal_date) DESC
		LIMIT ? OFFSET ?
	`

	rows, err := s.db.Query(searchQuery, likePattern, likePattern, limit, offset)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	messages, ids, err := scanMessageRows(rows)
	if err != nil {
		return nil, 0, err
	}

	if len(ids) == 0 {
		return messages, total, nil
	}

	// Batch-load recipients and labels
	if err := s.batchPopulate(messages, ids); err != nil {
		return nil, 0, err
	}

	return messages, total, nil
}

// scanMessageRows scans the standard 7-column message row set.
func scanMessageRows(rows *sql.Rows) ([]APIMessage, []int64, error) {
	var messages []APIMessage
	var ids []int64
	for rows.Next() {
		var m APIMessage
		var sentAt sql.NullTime
		err := rows.Scan(&m.ID, &m.Subject, &m.From, &sentAt, &m.Snippet, &m.HasAttachments, &m.SizeEstimate)
		if err != nil {
			return nil, nil, err
		}
		if sentAt.Valid {
			m.SentAt = sentAt.Time
		}
		messages = append(messages, m)
		ids = append(ids, m.ID)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("iterate messages: %w", err)
	}
	return messages, ids, nil
}

// batchPopulate batch-loads recipients and labels for a slice of messages.
func (s *Store) batchPopulate(messages []APIMessage, ids []int64) error {
	recipientMap, err := s.batchGetRecipients(ids, "to")
	if err != nil {
		return err
	}
	labelMap, err := s.batchGetLabels(ids)
	if err != nil {
		return err
	}
	for i := range messages {
		messages[i].To = recipientMap[messages[i].ID]
		messages[i].Labels = labelMap[messages[i].ID]
	}
	return nil
}

// batchGetRecipients loads recipients for multiple messages in a single query.
func (s *Store) batchGetRecipients(messageIDs []int64, recipientType string) (map[int64][]string, error) {
	if len(messageIDs) == 0 {
		return nil, nil
	}

	placeholders := make([]string, len(messageIDs))
	args := make([]interface{}, 0, len(messageIDs)+1)
	for i, id := range messageIDs {
		placeholders[i] = "?"
		args = append(args, id)
	}
	args = append(args, recipientType)

	query := fmt.Sprintf(`
		SELECT mr.message_id, COALESCE(p.email_address, '')
		FROM message_recipients mr
		JOIN participants p ON p.id = mr.participant_id
		WHERE mr.message_id IN (%s) AND mr.recipient_type = ?
	`, strings.Join(placeholders, ","))

	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("batch get recipients: %w", err)
	}
	defer rows.Close()

	result := make(map[int64][]string, len(messageIDs))
	for rows.Next() {
		var msgID int64
		var email string
		if err := rows.Scan(&msgID, &email); err != nil {
			return nil, fmt.Errorf("scan recipient: %w", err)
		}
		if email != "" {
			result[msgID] = append(result[msgID], email)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate recipients: %w", err)
	}
	return result, nil
}

// batchGetLabels loads labels for multiple messages in a single query.
func (s *Store) batchGetLabels(messageIDs []int64) (map[int64][]string, error) {
	if len(messageIDs) == 0 {
		return nil, nil
	}

	placeholders := make([]string, len(messageIDs))
	args := make([]interface{}, 0, len(messageIDs))
	for i, id := range messageIDs {
		placeholders[i] = "?"
		args = append(args, id)
	}

	query := fmt.Sprintf(`
		SELECT ml.message_id, l.name
		FROM message_labels ml
		JOIN labels l ON l.id = ml.label_id
		WHERE ml.message_id IN (%s)
	`, strings.Join(placeholders, ","))

	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("batch get labels: %w", err)
	}
	defer rows.Close()

	result := make(map[int64][]string, len(messageIDs))
	for rows.Next() {
		var msgID int64
		var name string
		if err := rows.Scan(&msgID, &name); err != nil {
			return nil, fmt.Errorf("scan label: %w", err)
		}
		result[msgID] = append(result[msgID], name)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate labels: %w", err)
	}
	return result, nil
}

// Single-message helpers (still used by GetMessage for single PK lookups)

func (s *Store) getRecipients(messageID int64, recipientType string) ([]string, error) {
	query := `
		SELECT COALESCE(p.email_address, '')
		FROM message_recipients mr
		JOIN participants p ON p.id = mr.participant_id
		WHERE mr.message_id = ? AND mr.recipient_type = ?
	`
	rows, err := s.db.Query(query, messageID, recipientType)
	if err != nil {
		return nil, fmt.Errorf("get recipients: %w", err)
	}
	defer rows.Close()

	var recipients []string
	for rows.Next() {
		var email string
		if err := rows.Scan(&email); err != nil {
			return nil, fmt.Errorf("scan recipient: %w", err)
		}
		if email != "" {
			recipients = append(recipients, email)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate recipients: %w", err)
	}
	return recipients, nil
}

func (s *Store) getLabels(messageID int64) ([]string, error) {
	query := `
		SELECT l.name
		FROM message_labels ml
		JOIN labels l ON l.id = ml.label_id
		WHERE ml.message_id = ?
	`
	rows, err := s.db.Query(query, messageID)
	if err != nil {
		return nil, fmt.Errorf("get labels: %w", err)
	}
	defer rows.Close()

	var labels []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("scan label: %w", err)
		}
		labels = append(labels, name)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate labels: %w", err)
	}
	return labels, nil
}
