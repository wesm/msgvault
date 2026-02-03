package store

import (
	"database/sql"
	"fmt"
	"time"
)

// SyncRun represents a sync operation in progress or completed.
type SyncRun struct {
	ID                int64
	SourceID          int64
	StartedAt         time.Time
	CompletedAt       sql.NullTime
	Status            string // "running", "completed", "failed"
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

	var run SyncRun
	var startedAt string
	var completedAt sql.NullString

	err := row.Scan(
		&run.ID, &run.SourceID, &startedAt, &completedAt, &run.Status,
		&run.MessagesProcessed, &run.MessagesAdded, &run.MessagesUpdated, &run.ErrorsCount,
		&run.ErrorMessage, &run.CursorBefore, &run.CursorAfter,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	run.StartedAt, _ = time.Parse("2006-01-02 15:04:05", startedAt)
	if completedAt.Valid {
		t, _ := time.Parse("2006-01-02 15:04:05", completedAt.String)
		run.CompletedAt = sql.NullTime{Time: t, Valid: true}
	}

	return &run, nil
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

	var run SyncRun
	var startedAt string
	var completedAt sql.NullString

	err := row.Scan(
		&run.ID, &run.SourceID, &startedAt, &completedAt, &run.Status,
		&run.MessagesProcessed, &run.MessagesAdded, &run.MessagesUpdated, &run.ErrorsCount,
		&run.ErrorMessage, &run.CursorBefore, &run.CursorAfter,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	run.StartedAt, _ = time.Parse("2006-01-02 15:04:05", startedAt)
	if completedAt.Valid {
		t, _ := time.Parse("2006-01-02 15:04:05", completedAt.String)
		run.CompletedAt = sql.NullTime{Time: t, Valid: true}
	}

	return &run, nil
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

	var source Source
	var lastSyncAt, createdAt, updatedAt sql.NullString

	err := row.Scan(
		&source.ID, &source.SourceType, &source.Identifier, &source.DisplayName,
		&source.GoogleUserID, &lastSyncAt, &source.SyncCursor, &createdAt, &updatedAt,
	)
	if err == nil {
		// Parse dates
		if lastSyncAt.Valid {
			t, _ := time.Parse("2006-01-02 15:04:05", lastSyncAt.String)
			source.LastSyncAt = sql.NullTime{Time: t, Valid: true}
		}
		if createdAt.Valid {
			source.CreatedAt, _ = time.Parse("2006-01-02 15:04:05", createdAt.String)
		}
		if updatedAt.Valid {
			source.UpdatedAt, _ = time.Parse("2006-01-02 15:04:05", updatedAt.String)
		}
		return &source, nil
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

	source.ID, _ = result.LastInsertId()
	source.SourceType = sourceType
	source.Identifier = identifier
	source.CreatedAt = time.Now()
	source.UpdatedAt = time.Now()

	return &source, nil
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
		var source Source
		var lastSyncAt, createdAt, updatedAt sql.NullString

		err := rows.Scan(
			&source.ID, &source.SourceType, &source.Identifier, &source.DisplayName,
			&source.GoogleUserID, &lastSyncAt, &source.SyncCursor, &createdAt, &updatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("scan source: %w", err)
		}

		if lastSyncAt.Valid {
			t, _ := time.Parse("2006-01-02 15:04:05", lastSyncAt.String)
			source.LastSyncAt = sql.NullTime{Time: t, Valid: true}
		}
		if createdAt.Valid {
			source.CreatedAt, _ = time.Parse("2006-01-02 15:04:05", createdAt.String)
		}
		if updatedAt.Valid {
			source.UpdatedAt, _ = time.Parse("2006-01-02 15:04:05", updatedAt.String)
		}

		sources = append(sources, &source)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate sources: %w", err)
	}

	return sources, nil
}

// GetSourceByIdentifier returns a source by its identifier (email address).
func (s *Store) GetSourceByIdentifier(identifier string) (*Source, error) {
	row := s.db.QueryRow(`
		SELECT id, source_type, identifier, display_name, google_user_id,
		       last_sync_at, sync_cursor, created_at, updated_at
		FROM sources
		WHERE identifier = ?
	`, identifier)

	var source Source
	var lastSyncAt, createdAt, updatedAt sql.NullString

	err := row.Scan(
		&source.ID, &source.SourceType, &source.Identifier, &source.DisplayName,
		&source.GoogleUserID, &lastSyncAt, &source.SyncCursor, &createdAt, &updatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	// Parse dates
	if lastSyncAt.Valid {
		t, _ := time.Parse("2006-01-02 15:04:05", lastSyncAt.String)
		source.LastSyncAt = sql.NullTime{Time: t, Valid: true}
	}
	if createdAt.Valid {
		source.CreatedAt, _ = time.Parse("2006-01-02 15:04:05", createdAt.String)
	}
	if updatedAt.Valid {
		source.UpdatedAt, _ = time.Parse("2006-01-02 15:04:05", updatedAt.String)
	}

	return &source, nil
}
