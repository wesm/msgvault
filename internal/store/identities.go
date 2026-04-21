package store

import (
	"fmt"
	"strings"
)

// IdentityCandidate is a single "likely me" email address discovered
// in the archive.
type IdentityCandidate struct {
	Email        string
	MessageCount int64
	Signals      IdentitySignal
	SourceIDs    []int64
}

// IdentitySignal is a bitmask describing which evidence types
// support an address being "me".
type IdentitySignal uint8

const (
	SignalFromMe       IdentitySignal = 1 << iota
	SignalSentLabel    IdentitySignal = 1 << iota
	SignalAccountMatch IdentitySignal = 1 << iota
)

// String renders a short human-readable label for an IdentitySignal bitmask.
func (s IdentitySignal) String() string {
	if s == 0 {
		return ""
	}
	parts := make([]string, 0, 3)
	if s&SignalFromMe != 0 {
		parts = append(parts, "is_from_me")
	}
	if s&SignalSentLabel != 0 {
		parts = append(parts, "sent-label")
	}
	if s&SignalAccountMatch != 0 {
		parts = append(parts, "account-match")
	}
	return strings.Join(parts, ",")
}

// ListLikelyIdentities returns every From: address that the archive
// considers a candidate "me" identity, ranked by total sent count.
//
// Three independent signals contribute:
//  1. messages.is_from_me = 1
//  2. Message carries a SENT label
//  3. Address equals a source identifier within scope
func (s *Store) ListLikelyIdentities(
	sourceIDs ...int64,
) ([]IdentityCandidate, error) {
	scopeClause := ""
	var scopeArgs []any
	if len(sourceIDs) > 0 {
		placeholders := make([]string, len(sourceIDs))
		for i, id := range sourceIDs {
			placeholders[i] = "?"
			scopeArgs = append(scopeArgs, id)
		}
		scopeClause = " AND m.source_id IN (" +
			strings.Join(placeholders, ",") + ")"
	}

	query := `
		WITH sent_messages AS (
			SELECT
				m.id,
				m.source_id,
				m.is_from_me,
				EXISTS (
					SELECT 1
					FROM message_labels ml
					JOIN labels l ON l.id = ml.label_id
					WHERE ml.message_id = m.id
					  AND (l.source_label_id = 'SENT'
					       OR UPPER(l.name) = 'SENT')
				) AS has_sent_label,
				LOWER(p_from.email_address) AS email,
				LOWER(src.identifier) AS src_identifier
			FROM messages m
			JOIN message_recipients mr_from
				ON mr_from.message_id = m.id
				AND mr_from.recipient_type = 'from'
			JOIN participants p_from
				ON p_from.id = mr_from.participant_id
			JOIN sources src ON src.id = m.source_id
			WHERE m.deleted_at IS NULL
			  AND p_from.email_address IS NOT NULL
			  AND p_from.email_address != ''` +
		scopeClause + `
		)
		SELECT
			email,
			COUNT(*) AS sent_count,
			MAX(CASE WHEN is_from_me = 1 THEN 1 ELSE 0 END) AS sig_from_me,
			MAX(CASE WHEN has_sent_label THEN 1 ELSE 0 END) AS sig_sent_label,
			MAX(CASE WHEN email = src_identifier THEN 1 ELSE 0 END) AS sig_account_match,
			GROUP_CONCAT(DISTINCT source_id) AS source_ids
		FROM sent_messages
		WHERE (is_from_me = 1
		       OR has_sent_label
		       OR email = src_identifier)
		GROUP BY email
		ORDER BY sent_count DESC, email ASC
	`

	rows, err := s.db.Query(query, scopeArgs...)
	if err != nil {
		return nil, fmt.Errorf("list likely identities: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []IdentityCandidate
	for rows.Next() {
		var (
			email         string
			sentCount     int64
			sigFromMe     int
			sigSent       int
			sigAccount    int
			sourceIDsList string
		)
		if err := rows.Scan(
			&email, &sentCount,
			&sigFromMe, &sigSent, &sigAccount,
			&sourceIDsList,
		); err != nil {
			return nil, err
		}

		var sigs IdentitySignal
		if sigFromMe == 1 {
			sigs |= SignalFromMe
		}
		if sigSent == 1 {
			sigs |= SignalSentLabel
		}
		if sigAccount == 1 {
			sigs |= SignalAccountMatch
		}

		out = append(out, IdentityCandidate{
			Email:        email,
			MessageCount: sentCount,
			Signals:      sigs,
			SourceIDs:    parseInt64CSV(sourceIDsList),
		})
	}
	return out, rows.Err()
}

func parseInt64CSV(s string) []int64 {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]int64, 0, len(parts))
	for _, p := range parts {
		var id int64
		if _, err := fmt.Sscanf(
			strings.TrimSpace(p), "%d", &id,
		); err == nil {
			out = append(out, id)
		}
	}
	return out
}
