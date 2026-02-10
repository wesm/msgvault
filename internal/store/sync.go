package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

const (
	SyncStatusRunning   = "running"
	SyncStatusCompleted = "completed"
	SyncStatusFailed    = "failed"
)

// dbTimeLayouts lists formats used by SQLite/go-sqlite3 for timestamp storage.
// This matches the full set from SQLiteTimestampFormats in mattn/go-sqlite3,
// plus RFC3339/RFC3339Nano as fallbacks for maximum compatibility.
// The order matters: more specific formats (with fractional seconds/timezones) come first.
var dbTimeLayouts = []string{
	// Formats from mattn/go-sqlite3 SQLiteTimestampFormats
	"2006-01-02 15:04:05.999999999-07:00", // space-separated with fractional seconds and TZ
	"2006-01-02T15:04:05.999999999-07:00", // T-separated with fractional seconds and TZ
	"2006-01-02 15:04:05.999999999",       // space-separated with fractional seconds
	"2006-01-02T15:04:05.999999999",       // T-separated with fractional seconds
	"2006-01-02 15:04:05",                 // SQLite datetime('now') format
	"2006-01-02T15:04:05",                 // T-separated basic
	"2006-01-02 15:04",                    // space-separated without seconds
	"2006-01-02T15:04",                    // T-separated without seconds
	"2006-01-02",                          // date only
	// Additional fallback formats
	time.RFC3339,     // go-sqlite3 DATETIME column format (e.g., "2006-01-02T15:04:05Z")
	time.RFC3339Nano, // RFC3339 with nanoseconds (e.g., "2006-01-02T15:04:05.999999999Z07:00")
}

// scanner is satisfied by both *sql.Row and *sql.Rows.
type scanner interface {
	Scan(dest ...interface{}) error
}

// parseDBTime attempts to parse a timestamp string using known SQLite/go-sqlite3 formats.
func parseDBTime(s string) (time.Time, error) {
	for _, layout := range dbTimeLayouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("unrecognized timestamp format %q", s)
}

func parseNullTime(ns sql.NullString) (sql.NullTime, error) {
	if !ns.Valid {
		return sql.NullTime{}, nil
	}
	t, err := parseDBTime(ns.String)
	if err != nil {
		return sql.NullTime{}, err
	}
	return sql.NullTime{Time: t, Valid: true}, nil
}

// parseRequiredTime parses a timestamp that must not be NULL.
// Use this for required fields like created_at/updated_at.
func parseRequiredTime(ns sql.NullString, field string) (time.Time, error) {
	if !ns.Valid {
		return time.Time{}, fmt.Errorf("%s: required timestamp is NULL", field)
	}
	t, err := parseDBTime(ns.String)
	if err != nil {
		return time.Time{}, fmt.Errorf("%s: %w", field, err)
	}
	return t, nil
}

func scanSource(sc scanner) (*Source, error) {
	var source Source
	var lastSyncAt, createdAt, updatedAt sql.NullString

	err := sc.Scan(
		&source.ID, &source.SourceType, &source.Identifier, &source.DisplayName,
		&source.GoogleUserID, &lastSyncAt, &source.SyncCursor, &createdAt, &updatedAt,
	)
	if err != nil {
		return nil, err
	}

	source.LastSyncAt, err = parseNullTime(lastSyncAt)
	if err != nil {
		return nil, fmt.Errorf("source %d: last_sync_at: %w", source.ID, err)
	}
	source.CreatedAt, err = parseRequiredTime(createdAt, "created_at")
	if err != nil {
		return nil, fmt.Errorf("source %d: %w", source.ID, err)
	}
	source.UpdatedAt, err = parseRequiredTime(updatedAt, "updated_at")
	if err != nil {
		return nil, fmt.Errorf("source %d: %w", source.ID, err)
	}

	return &source, nil
}

func scanSyncRun(sc scanner) (*SyncRun, error) {
	var run SyncRun
	var startedAt string
	var completedAt sql.NullString

	err := sc.Scan(
		&run.ID, &run.SourceID, &startedAt, &completedAt, &run.Status,
		&run.MessagesProcessed, &run.MessagesAdded, &run.MessagesUpdated, &run.ErrorsCount,
		&run.ErrorMessage, &run.CursorBefore, &run.CursorAfter,
	)
	if err != nil {
		return nil, err
	}

	run.StartedAt, err = parseDBTime(startedAt)
	if err != nil {
		return nil, fmt.Errorf("sync_run %d: parse started_at %q: %w", run.ID, startedAt, err)
	}
	run.CompletedAt, err = parseNullTime(completedAt)
	if err != nil {
		return nil, fmt.Errorf("sync_run %d: completed_at: %w", run.ID, err)
	}

	return &run, nil
}

// SyncRun represents a sync operation in progress or completed.
type SyncRun struct {
	ID                int64
	SourceID          int64
	StartedAt         time.Time
	CompletedAt       sql.NullTime
	Status            string // SyncStatusRunning, SyncStatusCompleted, SyncStatusFailed
	MessagesProcessed int64
	MessagesAdded     int64
	MessagesUpdated   int64
	ErrorsCount       int64
	ErrorMessage      sql.NullString
	CursorBefore      sql.NullString // Page token for resumption
	CursorAfter       sql.NullString // Final history ID
}

// Checkpoint represents sync progress for resumption.
type Checkpoint struct {
	PageToken         string
	MessagesProcessed int64
	MessagesAdded     int64
	MessagesUpdated   int64
	ErrorsCount       int64
}

// StartSync creates a new sync run record and returns its ID.
func (s *Store) StartSync(sourceID int64, syncType string) (int64, error) {
	// Mark any existing running syncs as failed
	_, err := s.db.Exec(`
		UPDATE sync_runs
		SET status = 'failed',
		    error_message = 'superseded by new sync',
		    completed_at = datetime('now')
		WHERE source_id = ? AND status = 'running'
	`, sourceID)
	if err != nil {
		return 0, fmt.Errorf("mark old syncs failed: %w", err)
	}

	// Create new sync run
	result, err := s.db.Exec(`
		INSERT INTO sync_runs (source_id, started_at, status, messages_processed, messages_added, messages_updated, errors_count)
		VALUES (?, datetime('now'), 'running', 0, 0, 0, 0)
	`, sourceID)
	if err != nil {
		return 0, fmt.Errorf("insert sync_run: %w", err)
	}

	return result.LastInsertId()
}

// UpdateSyncCheckpoint saves progress for resumption.
func (s *Store) UpdateSyncCheckpoint(syncID int64, cp *Checkpoint) error {
	_, err := s.db.Exec(`
		UPDATE sync_runs
		SET cursor_before = ?,
		    messages_processed = ?,
		    messages_added = ?,
		    messages_updated = ?,
		    errors_count = ?
		WHERE id = ?
	`, cp.PageToken, cp.MessagesProcessed, cp.MessagesAdded, cp.MessagesUpdated, cp.ErrorsCount, syncID)
	return err
}

// CompleteSync marks a sync as successfully completed.
func (s *Store) CompleteSync(syncID int64, finalHistoryID string) error {
	_, err := s.db.Exec(`
		UPDATE sync_runs
		SET status = 'completed',
		    completed_at = datetime('now'),
		    cursor_after = ?
		WHERE id = ?
	`, finalHistoryID, syncID)
	return err
}

// FailSync marks a sync as failed with an error message.
func (s *Store) FailSync(syncID int64, errMsg string) error {
	_, err := s.db.Exec(`
		UPDATE sync_runs
		SET status = 'failed',
		    completed_at = datetime('now'),
		    error_message = ?
		WHERE id = ?
	`, errMsg, syncID)
	return err
}

// GetActiveSync returns the most recent running sync for a source, if any.
func (s *Store) GetActiveSync(sourceID int64) (*SyncRun, error) {
	row := s.db.QueryRow(`
		SELECT id, source_id, started_at, completed_at, status,
		       messages_processed, messages_added, messages_updated, errors_count,
		       error_message, cursor_before, cursor_after
		FROM sync_runs
		WHERE source_id = ? AND status = 'running'
		ORDER BY started_at DESC
		LIMIT 1
	`, sourceID)

	run, err := scanSyncRun(row)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return run, err
}

// GetLastSuccessfulSync returns the most recent successful sync for a source.
func (s *Store) GetLastSuccessfulSync(sourceID int64) (*SyncRun, error) {
	row := s.db.QueryRow(`
		SELECT id, source_id, started_at, completed_at, status,
		       messages_processed, messages_added, messages_updated, errors_count,
		       error_message, cursor_before, cursor_after
		FROM sync_runs
		WHERE source_id = ? AND status = 'completed'
		ORDER BY completed_at DESC
		LIMIT 1
	`, sourceID)

	run, err := scanSyncRun(row)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return run, err
}

// Source represents a Gmail account or other message source.
type Source struct {
	ID           int64
	SourceType   string // "gmail"
	Identifier   string // email address
	DisplayName  sql.NullString
	GoogleUserID sql.NullString
	LastSyncAt   sql.NullTime
	SyncCursor   sql.NullString // historyId for Gmail
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// GetOrCreateSource gets or creates a source by type and identifier.
func (s *Store) GetOrCreateSource(sourceType, identifier string) (*Source, error) {
	// Try to get existing
	row := s.db.QueryRow(`
		SELECT id, source_type, identifier, display_name, google_user_id,
		       last_sync_at, sync_cursor, created_at, updated_at
		FROM sources
		WHERE source_type = ? AND identifier = ?
	`, sourceType, identifier)

	source, err := scanSource(row)
	if err == nil {
		return source, nil
	}
	if err != sql.ErrNoRows {
		return nil, err
	}

	// Create new
	result, err := s.db.Exec(`
		INSERT INTO sources (source_type, identifier, created_at, updated_at)
		VALUES (?, ?, datetime('now'), datetime('now'))
	`, sourceType, identifier)
	if err != nil {
		return nil, fmt.Errorf("insert source: %w", err)
	}

	newSource := &Source{
		SourceType: sourceType,
		Identifier: identifier,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}
	newSource.ID, _ = result.LastInsertId()

	return newSource, nil
}

// UpdateSourceSyncCursor updates the sync cursor (historyId) for a source.
func (s *Store) UpdateSourceSyncCursor(sourceID int64, cursor string) error {
	_, err := s.db.Exec(`
		UPDATE sources
		SET sync_cursor = ?, last_sync_at = datetime('now'), updated_at = datetime('now')
		WHERE id = ?
	`, cursor, sourceID)
	return err
}

// ListSources returns all sources, optionally filtered by source type.
// Pass an empty string to return all sources.
func (s *Store) ListSources(sourceType string) ([]*Source, error) {
	var rows *sql.Rows
	var err error

	if sourceType != "" {
		rows, err = s.db.Query(`
			SELECT id, source_type, identifier, display_name, google_user_id,
			       last_sync_at, sync_cursor, created_at, updated_at
			FROM sources
			WHERE source_type = ?
			ORDER BY identifier
		`, sourceType)
	} else {
		rows, err = s.db.Query(`
			SELECT id, source_type, identifier, display_name, google_user_id,
			       last_sync_at, sync_cursor, created_at, updated_at
			FROM sources
			ORDER BY identifier
		`)
	}
	if err != nil {
		return nil, fmt.Errorf("list sources: %w", err)
	}
	defer rows.Close()

	var sources []*Source
	for rows.Next() {
		src, err := scanSource(rows)
		if err != nil {
			return nil, fmt.Errorf("scan source: %w", err)
		}
		sources = append(sources, src)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate sources: %w", err)
	}

	return sources, nil
}

// UpdateSourceDisplayName updates the display name for a source.
func (s *Store) UpdateSourceDisplayName(sourceID int64, displayName string) error {
	_, err := s.db.Exec(`
		UPDATE sources
		SET display_name = ?, updated_at = datetime('now')
		WHERE id = ?
	`, displayName, sourceID)
	return err
}

// GetSourceByIdentifier returns a source by its identifier (email address).
func (s *Store) GetSourceByIdentifier(identifier string) (*Source, error) {
	row := s.db.QueryRow(`
		SELECT id, source_type, identifier, display_name, google_user_id,
		       last_sync_at, sync_cursor, created_at, updated_at
		FROM sources
		WHERE identifier = ?
	`, identifier)

	source, err := scanSource(row)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	return source, nil
}

// ResetProgress reports progress during a reset operation.
type ResetProgress struct {
	Phase           string // "counting", "message_bodies", "message_raw", etc.
	TotalMessages   int64  // Total messages to delete
	DeletedMessages int64  // Messages deleted so far
	CurrentTable    string // Table currently being processed
	RowsInBatch     int64  // Rows deleted in this batch
}

// ResetProgressFunc is called periodically during reset to report progress.
type ResetProgressFunc func(p ResetProgress)

// ResetSourceData deletes all synced data for a source while keeping the source
// entry itself. This allows a clean re-sync from Gmail without losing the account
// configuration. Returns the number of messages deleted.
func (s *Store) ResetSourceData(sourceID int64) (int64, error) {
	return s.ResetSourceDataWithProgress(sourceID, nil)
}

// ResetSourceDataWithProgress is like ResetSourceData but reports progress via callback.
// Uses batched deletes with FK checks disabled for much better performance.
func (s *Store) ResetSourceDataWithProgress(sourceID int64, progress ResetProgressFunc) (int64, error) {
	if progress == nil {
		progress = func(ResetProgress) {} // no-op
	}

	const batchSize = 5000
	ctx := context.Background()

	// Count messages first
	var totalMessages int64
	if err := s.db.QueryRow("SELECT COUNT(*) FROM messages WHERE source_id = ?", sourceID).Scan(&totalMessages); err != nil {
		return 0, fmt.Errorf("count messages: %w", err)
	}

	progress(ResetProgress{Phase: "counting", TotalMessages: totalMessages})

	// Use a dedicated connection to ensure PRAGMA applies to all our operations.
	// This is critical because *sql.DB is a connection pool - without this,
	// PRAGMA foreign_keys = OFF might run on a different connection than deletes.
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return 0, fmt.Errorf("acquire connection: %w", err)
	}
	defer conn.Close()

	// Disable foreign keys for bulk delete performance on this connection
	if _, err := conn.ExecContext(ctx, "PRAGMA foreign_keys = OFF"); err != nil {
		return 0, fmt.Errorf("disable foreign keys: %w", err)
	}

	// Helper to delete from a child table in batches.
	// Uses rowid-based deletion to ensure each batch finds actual rows to delete.
	deleteChildBatched := func(table, fkColumn string, onProgress func()) error {
		// Query selects child table rowids by joining to messages filtered by source.
		// This ensures each iteration finds actual existing child rows.
		query := fmt.Sprintf(`
			DELETE FROM %s WHERE rowid IN (
				SELECT %s.rowid FROM %s
				JOIN messages ON messages.id = %s.%s
				WHERE messages.source_id = ?
				LIMIT ?
			)
		`, table, table, table, table, fkColumn)

		for {
			result, err := conn.ExecContext(ctx, query, sourceID, batchSize)
			if err != nil {
				return fmt.Errorf("delete from %s: %w", table, err)
			}
			rows, _ := result.RowsAffected()
			if rows == 0 {
				break
			}
			onProgress()
		}
		return nil
	}

	var deletedMessages int64

	// Delete child tables explicitly (avoiding CASCADE overhead)
	// Order: children before parents

	// Child tables of messages
	childTables := []struct {
		table    string
		fkColumn string
	}{
		{"message_bodies", "message_id"},
		{"message_raw", "message_id"},
		{"message_recipients", "message_id"},
		{"message_labels", "message_id"},
		{"attachments", "message_id"},
		{"reactions", "message_id"},
	}

	for _, ct := range childTables {
		tableName := ct.table
		if err := deleteChildBatched(ct.table, ct.fkColumn, func() {
			progress(ResetProgress{
				Phase:           "deleting",
				TotalMessages:   totalMessages,
				DeletedMessages: deletedMessages,
				CurrentTable:    tableName,
			})
		}); err != nil {
			return 0, err
		}
	}

	// Delete messages in batches (parent table)
	for {
		result, err := conn.ExecContext(ctx, `
			DELETE FROM messages WHERE id IN (
				SELECT id FROM messages WHERE source_id = ? LIMIT ?
			)
		`, sourceID, batchSize)
		if err != nil {
			return deletedMessages, fmt.Errorf("delete messages batch: %w", err)
		}

		rows, _ := result.RowsAffected()
		if rows == 0 {
			break
		}
		deletedMessages += rows

		progress(ResetProgress{
			Phase:           "deleting",
			TotalMessages:   totalMessages,
			DeletedMessages: deletedMessages,
			CurrentTable:    "messages",
			RowsInBatch:     rows,
		})
	}

	// Delete conversation_participants (child of conversations)
	if _, err := conn.ExecContext(ctx, `
		DELETE FROM conversation_participants WHERE conversation_id IN (
			SELECT id FROM conversations WHERE source_id = ?
		)
	`, sourceID); err != nil {
		return deletedMessages, fmt.Errorf("delete conversation_participants: %w", err)
	}

	// Delete conversations
	if _, err := conn.ExecContext(ctx, "DELETE FROM conversations WHERE source_id = ?", sourceID); err != nil {
		return deletedMessages, fmt.Errorf("delete conversations: %w", err)
	}

	progress(ResetProgress{
		Phase:           "deleting",
		TotalMessages:   totalMessages,
		DeletedMessages: deletedMessages,
		CurrentTable:    "conversations",
	})

	// Delete labels
	if _, err := conn.ExecContext(ctx, "DELETE FROM labels WHERE source_id = ?", sourceID); err != nil {
		return deletedMessages, fmt.Errorf("delete labels: %w", err)
	}

	// Delete sync history
	if _, err := conn.ExecContext(ctx, "DELETE FROM sync_runs WHERE source_id = ?", sourceID); err != nil {
		return deletedMessages, fmt.Errorf("delete sync_runs: %w", err)
	}
	if _, err := conn.ExecContext(ctx, "DELETE FROM sync_checkpoints WHERE source_id = ?", sourceID); err != nil {
		return deletedMessages, fmt.Errorf("delete sync_checkpoints: %w", err)
	}

	// Reset the source's sync cursor
	if _, err := conn.ExecContext(ctx, `
		UPDATE sources
		SET sync_cursor = NULL, last_sync_at = NULL, updated_at = datetime('now')
		WHERE id = ?
	`, sourceID); err != nil {
		return deletedMessages, fmt.Errorf("reset source: %w", err)
	}

	// Re-enable foreign keys on this connection before returning it to pool
	if _, err := conn.ExecContext(ctx, "PRAGMA foreign_keys = ON"); err != nil {
		return deletedMessages, fmt.Errorf("re-enable foreign keys: %w", err)
	}

	progress(ResetProgress{
		Phase:           "complete",
		TotalMessages:   totalMessages,
		DeletedMessages: deletedMessages,
	})

	return deletedMessages, nil
}
