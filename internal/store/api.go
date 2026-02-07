package store

import (
	"database/sql"
	"fmt"
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

// ListMessages returns a paginated list of messages.
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

		// Get To recipients
		m.To, err = s.getRecipients(m.ID, "to")
		if err != nil {
			return nil, 0, err
		}

		// Get labels
		m.Labels, err = s.getLabels(m.ID)
		if err != nil {
			return nil, 0, err
		}

		messages = append(messages, m)
	}

	return messages, total, rows.Err()
}

// GetMessage returns a single message with full details.
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

	// Get recipients
	m.To, err = s.getRecipients(m.ID, "to")
	if err != nil {
		return nil, err
	}

	// Get labels
	m.Labels, err = s.getLabels(m.ID)
	if err != nil {
		return nil, err
	}

	// Get body
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

// SearchMessages searches messages using FTS5.
func (s *Store) SearchMessages(query string, offset, limit int) ([]APIMessage, int64, error) {
	// First try FTS5 search
	ftsQuery := `
		SELECT m.id
		FROM messages_fts fts
		JOIN messages m ON m.id = fts.rowid
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

	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, 0, err
		}
		ids = append(ids, id)
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

	// Get message details
	messages := make([]APIMessage, 0, len(ids))
	for _, id := range ids {
		m, err := s.GetMessage(id)
		if err != nil {
			return nil, 0, err
		}
		if m != nil {
			messages = append(messages, *m)
		}
	}

	return messages, total, nil
}

// searchMessagesLike is a fallback search using LIKE.
func (s *Store) searchMessagesLike(query string, offset, limit int) ([]APIMessage, int64, error) {
	likePattern := "%" + query + "%"

	countQuery := `
		SELECT COUNT(*) FROM messages
		WHERE deleted_from_source_at IS NULL
		AND (subject LIKE ? OR snippet LIKE ?)
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
		AND (m.subject LIKE ? OR m.snippet LIKE ?)
		ORDER BY COALESCE(m.sent_at, m.received_at, m.internal_date) DESC
		LIMIT ? OFFSET ?
	`

	rows, err := s.db.Query(searchQuery, likePattern, likePattern, limit, offset)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var messages []APIMessage
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
		m.To, err = s.getRecipients(m.ID, "to")
		if err != nil {
			return nil, 0, err
		}
		m.Labels, err = s.getLabels(m.ID)
		if err != nil {
			return nil, 0, err
		}
		messages = append(messages, m)
	}

	return messages, total, rows.Err()
}

// Helper functions

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
	return recipients, rows.Err()
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
	return labels, rows.Err()
}
