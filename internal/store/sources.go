package store

import (
	"database/sql"
	"fmt"
)

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
