package store

import (
	"database/sql"
	"fmt"
)

// GetSourcesByIdentifier returns all sources matching an identifier,
// regardless of source_type. Use this when the identifier may be
// shared across source types (e.g., gmail + mbox import).
func (s *Store) GetSourcesByIdentifier(
	identifier string,
) ([]*Source, error) {
	rows, err := s.db.Query(`
		SELECT id, source_type, identifier, display_name,
		       google_user_id, last_sync_at, sync_cursor,
		       created_at, updated_at
		FROM sources
		WHERE identifier = ?
		ORDER BY source_type
	`, identifier)
	if err != nil {
		return nil, fmt.Errorf("query sources: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var sources []*Source
	for rows.Next() {
		src, err := scanSource(rows)
		if err != nil {
			return nil, fmt.Errorf("scan source: %w", err)
		}
		sources = append(sources, src)
	}
	return sources, rows.Err()
}

// RemoveSource deletes a source and all its associated data.
// FTS5 rows are cleaned up explicitly (no FK cascade for virtual tables).
// CASCADE handles conversations, messages, labels, attachments, sync state.
// Orphaned participants are left for a future `gc` command.
func (s *Store) RemoveSource(sourceID int64) error {
	return s.withTx(func(tx *sql.Tx) error {
		if s.fts5Available {
			_, err := tx.Exec(`
				DELETE FROM messages_fts
				WHERE message_id IN (
					SELECT id FROM messages WHERE source_id = ?
				)
			`, sourceID)
			if err != nil {
				return fmt.Errorf("delete FTS rows: %w", err)
			}
		}

		res, err := tx.Exec(
			`DELETE FROM sources WHERE id = ?`, sourceID,
		)
		if err != nil {
			return fmt.Errorf("delete source: %w", err)
		}

		rows, err := res.RowsAffected()
		if err != nil {
			return fmt.Errorf("check rows affected: %w", err)
		}
		if rows == 0 {
			return fmt.Errorf("source %d not found", sourceID)
		}

		return nil
	})
}
