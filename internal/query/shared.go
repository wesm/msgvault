package query

import (
	"bytes"
	"compress/zlib"
	"context"
	"database/sql"
	"fmt"
	"io"
	"strings"

	"github.com/wesm/msgvault/internal/mime"
)

// fetchLabelsForMessageList adds labels to message summaries using a batch query.
// tablePrefix is "" for direct SQLite or "sqlite_db." for DuckDB's sqlite_scan.
func fetchLabelsForMessageList(ctx context.Context, db *sql.DB, tablePrefix string, messages []MessageSummary) error {
	if len(messages) == 0 {
		return nil
	}

	ids := make([]interface{}, len(messages))
	placeholders := make([]string, len(messages))
	idToIndex := make(map[int64]int)
	for i, msg := range messages {
		ids[i] = msg.ID
		placeholders[i] = "?"
		idToIndex[msg.ID] = i
	}

	query := fmt.Sprintf(`
		SELECT ml.message_id, l.name
		FROM %smessage_labels ml
		JOIN %slabels l ON l.id = ml.label_id
		WHERE ml.message_id IN (%s)
	`, tablePrefix, tablePrefix, strings.Join(placeholders, ","))

	rows, err := db.QueryContext(ctx, query, ids...)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var msgID int64
		var labelName string
		if err := rows.Scan(&msgID, &labelName); err != nil {
			return err
		}
		if idx, ok := idToIndex[msgID]; ok {
			messages[idx].Labels = append(messages[idx].Labels, labelName)
		}
	}

	return rows.Err()
}

// fetchMessageLabelsDetail fetches labels for a single message detail.
// tablePrefix is "" for direct SQLite or "sqlite_db." for DuckDB's sqlite_scan.
func fetchMessageLabelsDetail(ctx context.Context, db *sql.DB, tablePrefix string, msg *MessageDetail) error {
	rows, err := db.QueryContext(ctx, fmt.Sprintf(`
		SELECT l.name
		FROM %smessage_labels ml
		JOIN %slabels l ON l.id = ml.label_id
		WHERE ml.message_id = ?
	`, tablePrefix, tablePrefix), msg.ID)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return err
		}
		msg.Labels = append(msg.Labels, name)
	}

	return rows.Err()
}

// fetchParticipantsShared fetches participants for a single message detail.
// tablePrefix is "" for direct SQLite or "sqlite_db." for DuckDB's sqlite_scan.
func fetchParticipantsShared(ctx context.Context, db *sql.DB, tablePrefix string, msg *MessageDetail) error {
	rows, err := db.QueryContext(ctx, fmt.Sprintf(`
		SELECT mr.recipient_type, p.email_address, COALESCE(mr.display_name, p.display_name, '')
		FROM %smessage_recipients mr
		JOIN %sparticipants p ON p.id = mr.participant_id
		WHERE mr.message_id = ?
	`, tablePrefix, tablePrefix), msg.ID)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var recipType, email, name string
		if err := rows.Scan(&recipType, &email, &name); err != nil {
			return err
		}
		addr := Address{Email: email, Name: name}
		switch recipType {
		case "from":
			msg.From = append(msg.From, addr)
		case "to":
			msg.To = append(msg.To, addr)
		case "cc":
			msg.Cc = append(msg.Cc, addr)
		case "bcc":
			msg.Bcc = append(msg.Bcc, addr)
		}
	}

	return rows.Err()
}

// fetchAttachmentsShared fetches attachments for a single message detail.
// tablePrefix is "" for direct SQLite or "sqlite_db." for DuckDB's sqlite_scan.
func fetchAttachmentsShared(ctx context.Context, db *sql.DB, tablePrefix string, msg *MessageDetail) error {
	rows, err := db.QueryContext(ctx, fmt.Sprintf(`
		SELECT id, COALESCE(filename, ''), COALESCE(mime_type, ''), COALESCE(size, 0), COALESCE(content_hash, '')
		FROM %sattachments
		WHERE message_id = ?
	`, tablePrefix), msg.ID)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var att AttachmentInfo
		if err := rows.Scan(&att.ID, &att.Filename, &att.MimeType, &att.Size, &att.ContentHash); err != nil {
			return err
		}
		msg.Attachments = append(msg.Attachments, att)
	}

	return rows.Err()
}

// extractBodyFromRawShared extracts text body from compressed MIME data.
// tablePrefix is "" for direct SQLite or "sqlite_db." for DuckDB's sqlite_scan.
func extractBodyFromRawShared(ctx context.Context, db *sql.DB, tablePrefix string, messageID int64) (string, error) {
	var compressed []byte
	var compression sql.NullString

	err := db.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT raw_data, compression FROM %smessage_raw WHERE message_id = ?
	`, tablePrefix), messageID).Scan(&compressed, &compression)
	if err != nil {
		return "", err
	}

	var rawData []byte
	if compression.Valid && compression.String == "zlib" {
		r, err := zlib.NewReader(bytes.NewReader(compressed))
		if err != nil {
			return "", err
		}
		defer r.Close()
		rawData, err = io.ReadAll(r)
		if err != nil {
			return "", err
		}
	} else {
		rawData = compressed
	}

	parsed, err := mime.Parse(rawData)
	if err != nil {
		return "", err
	}

	return parsed.GetBodyText(), nil
}

// getMessageByQueryShared retrieves a full message detail by an arbitrary WHERE clause.
// tablePrefix is "" for direct SQLite or "sqlite_db." for DuckDB's sqlite_scan.
func getMessageByQueryShared(ctx context.Context, db *sql.DB, tablePrefix string, whereClause string, args ...interface{}) (*MessageDetail, error) {
	query := fmt.Sprintf(`
		SELECT
			m.id,
			m.source_message_id,
			m.conversation_id,
			COALESCE(conv.source_conversation_id, ''),
			COALESCE(m.subject, ''),
			COALESCE(m.snippet, ''),
			m.sent_at,
			m.received_at,
			COALESCE(m.size_estimate, 0),
			m.has_attachments
		FROM %smessages m
		LEFT JOIN %sconversations conv ON conv.id = m.conversation_id
		WHERE %s
	`, tablePrefix, tablePrefix, whereClause)

	var msg MessageDetail
	var sentAt, receivedAt sql.NullTime
	err := db.QueryRowContext(ctx, query, args...).Scan(
		&msg.ID,
		&msg.SourceMessageID,
		&msg.ConversationID,
		&msg.SourceConversationID,
		&msg.Subject,
		&msg.Snippet,
		&sentAt,
		&receivedAt,
		&msg.SizeEstimate,
		&msg.HasAttachments,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get message: %w", err)
	}

	if sentAt.Valid {
		msg.SentAt = sentAt.Time
	}
	if receivedAt.Valid {
		t := receivedAt.Time
		msg.ReceivedAt = &t
	}

	// Fetch body from separate table (PK lookup, avoids scanning large body B-tree)
	var bodyText, bodyHTML sql.NullString
	err = db.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT body_text, body_html FROM %smessage_bodies WHERE message_id = ?
	`, tablePrefix), msg.ID).Scan(&bodyText, &bodyHTML)
	if err == nil {
		if bodyText.Valid {
			msg.BodyText = bodyText.String
		}
		if bodyHTML.Valid {
			msg.BodyHTML = bodyHTML.String
		}
	} else if err != sql.ErrNoRows {
		return nil, fmt.Errorf("get message body: %w", err)
	}

	// If body is empty, try to extract from raw MIME
	if msg.BodyText == "" && msg.BodyHTML == "" {
		if body, err := extractBodyFromRawShared(ctx, db, tablePrefix, msg.ID); err == nil && body != "" {
			msg.BodyText = body
		}
	}

	// Fetch participants
	if err := fetchParticipantsShared(ctx, db, tablePrefix, &msg); err != nil {
		return nil, fmt.Errorf("fetch participants: %w", err)
	}

	// Fetch labels
	if err := fetchMessageLabelsDetail(ctx, db, tablePrefix, &msg); err != nil {
		return nil, fmt.Errorf("fetch labels: %w", err)
	}

	// Fetch attachments
	if err := fetchAttachmentsShared(ctx, db, tablePrefix, &msg); err != nil {
		return nil, fmt.Errorf("fetch attachments: %w", err)
	}

	return &msg, nil
}

// collectGmailIDs scans rows for source_message_id strings.
func collectGmailIDs(rows *sql.Rows) ([]string, error) {
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan gmail id: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate gmail ids: %w", err)
	}
	return ids, nil
}
