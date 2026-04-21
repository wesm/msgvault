// Package store provides database access for msgvault.
package store

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/mattn/go-sqlite3"
)

//go:embed schema.sql schema_sqlite.sql
var schemaFS embed.FS

// Store provides database operations for msgvault.
//
// The db field wraps a *sql.DB with a thin logging adapter that
// emits slog records for every Query / Exec / QueryRow call.
// Because loggedDB embeds *sql.DB and overrides the instrumented
// methods, existing store code that does s.db.Query(...) compiles
// unchanged and automatically routes through the logger.
type Store struct {
	db            *loggedDB
	dbPath        string
	dialect       Dialect
	readOnly      bool // Opened via OpenReadOnly; skips WAL checkpoint on close
	fts5Available bool // Whether FTS5 is available for full-text search
}

const defaultSQLiteParams = "?_journal_mode=WAL&_busy_timeout=30000&_synchronous=NORMAL&_foreign_keys=ON"

// isSQLiteError checks if err is a sqlite3.Error with a message containing substr.
// This is more robust than strings.Contains on err.Error() because it first
// type-asserts to the specific driver error type using errors.As.
// Handles both value (sqlite3.Error) and pointer (*sqlite3.Error) forms.
//
// SQLiteDialect's error predicates are thin wrappers around this helper; it also
// services subset.go (which has not been migrated to Dialect).
func isSQLiteError(err error, substr string) bool {
	var sqliteErr sqlite3.Error
	if errors.As(err, &sqliteErr) {
		return strings.Contains(sqliteErr.Error(), substr)
	}
	var sqliteErrPtr *sqlite3.Error
	if errors.As(err, &sqliteErrPtr) && sqliteErrPtr != nil {
		return strings.Contains(sqliteErrPtr.Error(), substr)
	}
	return false
}

// Open opens or creates the database at the given path.
// Currently only SQLite is supported. PostgreSQL URLs will return an error.
func Open(dbPath string) (*Store, error) {
	// Check for unsupported database URLs
	if strings.HasPrefix(dbPath, "postgresql://") || strings.HasPrefix(dbPath, "postgres://") {
		return nil, fmt.Errorf("PostgreSQL is not yet supported in the Go implementation; use SQLite path instead")
	}

	// Ensure directory exists (skip for in-memory databases)
	if dbPath != ":memory:" && !strings.Contains(dbPath, ":memory:") {
		dir := filepath.Dir(dbPath)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("create db directory: %w", err)
		}
	}

	dsn := dbPath + defaultSQLiteParams
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}

	// Test connection
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}

	// SQLite with WAL supports one writer + multiple readers.
	// Allow enough connections for concurrent reads (TUI async
	// queries, FTS backfill) while SQLite handles write serialization.
	// Exception: :memory: databases are per-connection, so multiple
	// connections would create separate databases.
	if dbPath == ":memory:" || strings.Contains(dbPath, ":memory:") {
		db.SetMaxOpenConns(1)
	} else {
		db.SetMaxOpenConns(4)
	}

	dialect := &SQLiteDialect{}
	if err := dialect.InitConn(db); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("init connection: %w", err)
	}

	return &Store{
		db:      newLoggedDB(db, dialect.Rebind),
		dbPath:  dbPath,
		dialect: dialect,
	}, nil
}

// OpenReadOnly opens an existing database in read-only mode. Suitable for
// query-only workloads (MCP server) where multiple processes access the
// same database concurrently. Does not create the database, run migrations,
// or checkpoint WAL on close.
func OpenReadOnly(dbPath string) (*Store, error) {
	if _, err := os.Stat(dbPath); err != nil {
		return nil, fmt.Errorf(
			"database not found: %s "+
				"(run 'msgvault init-db' first)", dbPath,
		)
	}

	// Use _query_only instead of mode=ro. WAL-mode databases may need
	// to create or update -wal/-shm sidecar files on open, which fails
	// under SQLITE_OPEN_READONLY. _query_only opens normally (so SQLite
	// can manage sidecars) but rejects all write SQL at the query layer.
	dsn := dbPath + "?_query_only=true&_busy_timeout=5000"
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("open database (read-only): %w", err)
	}

	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}

	db.SetMaxOpenConns(4)

	dialect := &SQLiteDialect{}
	if err := dialect.InitConn(db); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("init connection: %w", err)
	}

	s := &Store{
		db:       newLoggedDB(db, dialect.Rebind),
		dbPath:   dbPath,
		dialect:  dialect,
		readOnly: true,
	}

	s.fts5Available = dialect.FTSAvailable(db)

	return s, nil
}

// Close checkpoints the WAL (unless read-only) and closes the database.
func (s *Store) Close() error {
	if !s.readOnly {
		// Checkpoint WAL before closing to fold it back into the main
		// database. This prevents WAL accumulation across sessions and
		// reduces the risk of corruption from stale WAL entries.
		_ = s.CheckpointWAL()
	}
	return s.db.Close()
}

// CheckpointWAL forces a WAL checkpoint, folding the WAL back into the main
// database file. Uses TRUNCATE mode which also resets the WAL file to zero
// bytes. Returns nil on success; callers may log but should not fail on error.
// No-op for non-SQLite backends.
func (s *Store) CheckpointWAL() error {
	return s.dialect.CheckpointWAL(s.db.DB)
}

// DB returns the underlying *sql.DB for consumers that need to
// pass the raw handle elsewhere (e.g. the DuckDB engine's
// sqlite_scan wrapper). The wrapper's structured-logging
// behaviour is bypassed for those consumers — they're operating
// at a different abstraction layer.
func (s *Store) DB() *sql.DB {
	return s.db.DB
}

// WithExclusiveLock executes fn while holding an exclusive write lock on the
// database. In WAL mode this blocks concurrent writers (e.g. StartSync) while
// allowing reads (e.g. IsAttachmentPathReferenced) to proceed. Use this to
// serialize destructive file operations against concurrent sync attachment
// ingestion. The context controls both lock acquisition and the lifetime of
// the underlying connection; cancelling it aborts a pending BEGIN EXCLUSIVE
// and rolls back any held transaction.
func (s *Store) WithExclusiveLock(ctx context.Context, fn func() error) error {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire connection: %w", err)
	}
	defer func() { _ = conn.Close() }()

	if _, err := conn.ExecContext(ctx, "BEGIN EXCLUSIVE"); err != nil {
		return fmt.Errorf("begin exclusive: %w", err)
	}

	committed := false
	defer func() {
		if !committed {
			_, _ = conn.ExecContext(ctx, "ROLLBACK")
		}
	}()

	if err := fn(); err != nil {
		return err
	}

	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		return fmt.Errorf("commit exclusive: %w", err)
	}
	committed = true
	return nil
}

// withTx executes fn within a database transaction. If fn returns an error,
// the transaction is rolled back; otherwise it is committed. The callback
// receives *loggedTx so every statement inside the transaction goes through
// the dialect's Rebind automatically.
func (s *Store) withTx(fn func(tx *loggedTx) error) error {
	start := time.Now()
	slog.Debug("sql tx begin")
	tx, err := s.db.Begin()
	if err != nil {
		slog.Warn("sql tx begin failed", "error", err.Error())
		return fmt.Errorf("begin tx: %w", err)
	}
	if err := fn(tx); err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			slog.Warn("sql tx rollback failed",
				"error", rbErr.Error(),
				"fn_error", err.Error(),
				"duration_ms", time.Since(start).Milliseconds())
		} else {
			slog.Info("sql tx rollback",
				"reason", err.Error(),
				"duration_ms", time.Since(start).Milliseconds())
		}
		return err
	}
	if err := tx.Commit(); err != nil {
		slog.Warn("sql tx commit failed",
			"error", err.Error(),
			"duration_ms", time.Since(start).Milliseconds())
		return err
	}
	ms := time.Since(start).Milliseconds()
	if slowMs := sqlLogSlowMs.Load(); slowMs > 0 && ms >= slowMs {
		slog.Warn("sql tx slow", "duration_ms", ms)
	} else {
		slog.Debug("sql tx commit", "duration_ms", ms)
	}
	return nil
}

// queryInChunks executes a parameterized IN-query in chunks to stay within
// SQLite's parameter limit. queryTemplate must contain a single %s placeholder
// for the comma-separated "?" list. The prefix args are prepended before each
// chunk's args (e.g., a source_id filter).
// chunkQuerier abstracts the subset of *sql.DB that queryInChunks
// and execInChunks actually use, so the helpers accept either a
// raw *sql.DB (tests) or the logging wrapper (production path).
type chunkQuerier interface {
	Query(query string, args ...any) (*sql.Rows, error)
	Exec(query string, args ...any) (sql.Result, error)
}

func queryInChunks[T any](db chunkQuerier, ids []T, prefixArgs []interface{}, queryTemplate string, fn func(*sql.Rows) error) error {
	const chunkSize = 500
	for i := 0; i < len(ids); i += chunkSize {
		end := i + chunkSize
		if end > len(ids) {
			end = len(ids)
		}
		chunk := ids[i:end]

		placeholders := make([]string, len(chunk))
		args := make([]interface{}, 0, len(prefixArgs)+len(chunk))
		args = append(args, prefixArgs...)
		for j, id := range chunk {
			placeholders[j] = "?"
			args = append(args, id)
		}

		query := fmt.Sprintf(queryTemplate, strings.Join(placeholders, ","))
		rows, err := db.Query(query, args...)
		if err != nil {
			return err
		}

		for rows.Next() {
			if err := fn(rows); err != nil {
				_ = rows.Close()
				return err
			}
		}
		_ = rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
	}
	return nil
}

// chunkInsert describes a multi-row INSERT for insertInChunks.
// Prefix is everything up to "VALUES ", suffix is anything after the values
// (e.g. " ON CONFLICT DO NOTHING" for PostgreSQL). ValuesPerRow counts the
// parameters in one row's tuple (used to stay under the driver's parameter
// limit).
type chunkInsert struct {
	totalRows    int
	valuesPerRow int
	prefix       string
	suffix       string
}

// insertInChunks executes a multi-value INSERT in chunks to stay within SQLite's
// parameter limit (999). valueBuilder generates the VALUES placeholders and
// args for each chunk of row indices. Rebinding to the dialect's placeholder
// form happens inside tx.Exec (loggedTx wraps the dialect's Rebind).
func insertInChunks(tx *loggedTx, c chunkInsert, valueBuilder func(start, end int) ([]string, []interface{})) error {
	// SQLite default SQLITE_MAX_VARIABLE_NUMBER is 999
	// Leave some margin for safety
	const maxParams = 900
	chunkSize := maxParams / c.valuesPerRow
	if chunkSize < 1 {
		chunkSize = 1
	}

	for i := 0; i < c.totalRows; i += chunkSize {
		end := i + chunkSize
		if end > c.totalRows {
			end = c.totalRows
		}

		values, args := valueBuilder(i, end)
		query := c.prefix + strings.Join(values, ",") + c.suffix
		if _, err := tx.Exec(query, args...); err != nil {
			return err
		}
	}
	return nil
}

// execInChunks executes a parameterized DELETE/UPDATE with an IN-clause in chunks
// to stay within SQLite's parameter limit. queryTemplate must contain a single %s
// placeholder for the comma-separated "?" list. The prefix args are prepended before
// each chunk's args (e.g., a message_id filter).
func execInChunks[T any](db chunkQuerier, ids []T, prefixArgs []interface{}, queryTemplate string) error {
	const chunkSize = 500
	for i := 0; i < len(ids); i += chunkSize {
		end := i + chunkSize
		if end > len(ids) {
			end = len(ids)
		}
		chunk := ids[i:end]

		placeholders := make([]string, len(chunk))
		args := make([]interface{}, 0, len(prefixArgs)+len(chunk))
		args = append(args, prefixArgs...)
		for j, id := range chunk {
			placeholders[j] = "?"
			args = append(args, id)
		}

		query := fmt.Sprintf(queryTemplate, strings.Join(placeholders, ","))
		if _, err := db.Exec(query, args...); err != nil {
			return err
		}
	}
	return nil
}

// Rebind converts a query with ? placeholders to the appropriate format
// for the current database driver. No-op for SQLite; converts to $1, $2, ...
// for PostgreSQL.
func (s *Store) Rebind(query string) string {
	return s.dialect.Rebind(query)
}

// FTS5Available returns whether FTS5 full-text search is available.
func (s *Store) FTS5Available() bool {
	return s.fts5Available
}

// SchemaStale checks whether the database schema is missing columns
// added by recent migrations. Returns (stale, column, err). Only
// reports stale when the query succeeds and the column is absent;
// query errors are returned separately so callers don't misdiagnose
// corruption or permission problems as outdated schema.
func (s *Store) SchemaStale() (bool, string, error) {
	var count int
	err := s.db.QueryRow(s.dialect.SchemaStaleCheck()).Scan(&count)
	if err != nil {
		return false, "", fmt.Errorf("check schema version: %w", err)
	}
	if count == 0 {
		return true, "conversations.conversation_type", nil
	}
	return false, "", nil
}

// InitSchema initializes the database schema.
// This creates all tables if they don't exist.
func (s *Store) InitSchema() error {
	// Load and execute schema files provided by the dialect.
	for _, filename := range s.dialect.SchemaFiles() {
		schema, err := schemaFS.ReadFile(filename)
		if err != nil {
			return fmt.Errorf("read %s: %w", filename, err)
		}
		if _, err := s.db.Exec(string(schema)); err != nil {
			return fmt.Errorf("execute %s: %w", filename, err)
		}
	}

	// Migrations: add columns for databases created before these features.
	// The dialect determines whether a "duplicate column" error is benign.
	for _, m := range []struct {
		sql  string
		desc string
	}{
		{`ALTER TABLE sources ADD COLUMN sync_config JSON`, "sync_config"},
		{`ALTER TABLE messages ADD COLUMN rfc822_message_id TEXT`, "rfc822_message_id"},
		{`ALTER TABLE sources ADD COLUMN oauth_app TEXT`, "oauth_app"},
		{`ALTER TABLE participants ADD COLUMN phone_number TEXT`, "phone_number"},
		{`ALTER TABLE participants ADD COLUMN canonical_id TEXT`, "canonical_id"},
		{`ALTER TABLE messages ADD COLUMN sender_id INTEGER REFERENCES participants(id)`, "sender_id"},
		{`ALTER TABLE messages ADD COLUMN message_type TEXT NOT NULL DEFAULT 'email'`, "message_type"},
		{`ALTER TABLE messages ADD COLUMN attachment_count INTEGER DEFAULT 0`, "attachment_count"},
		{`ALTER TABLE messages ADD COLUMN deleted_from_source_at DATETIME`, "deleted_from_source_at"},
		{`ALTER TABLE messages ADD COLUMN delete_batch_id TEXT`, "delete_batch_id"},
		{`ALTER TABLE conversations ADD COLUMN title TEXT`, "title"},
		{`ALTER TABLE conversations ADD COLUMN conversation_type TEXT NOT NULL DEFAULT 'email_thread'`, "conversation_type"},
	} {
		if _, err := s.db.Exec(m.sql); err != nil {
			if !s.dialect.IsDuplicateColumnError(err) {
				return fmt.Errorf("migrate schema (%s): %w", m.desc, err)
			}
		}
	}

	// Load the optional FTS schema, if the dialect keeps one separate.
	// PostgreSQL returns "" here because its tsvector lives in the main schema.
	if ftsFile := s.dialect.SchemaFTS(); ftsFile != "" {
		ftsSchema, err := schemaFS.ReadFile(ftsFile)
		if err != nil {
			return fmt.Errorf("read %s: %w", ftsFile, err)
		}
		if _, err := s.db.Exec(string(ftsSchema)); err != nil {
			if !s.dialect.IsNoSuchModuleError(err) {
				return fmt.Errorf("init FTS schema: %w", err)
			}
			// Module not compiled in; availability stays false.
			return nil
		}
	}

	// Probe availability through the dialect so it works uniformly for
	// backends that carry FTS inside their main schema.
	s.fts5Available = s.dialect.FTSAvailable(s.db.DB)
	return nil
}

// NeedsFTSBackfill reports whether the FTS index needs to be populated.
func (s *Store) NeedsFTSBackfill() bool {
	if !s.fts5Available {
		return false
	}
	return s.dialect.FTSNeedsBackfill(s.db.DB)
}

// Stats holds database statistics.
type Stats struct {
	MessageCount    int64
	ThreadCount     int64
	AttachmentCount int64
	LabelCount      int64
	SourceCount     int64
	DatabaseSize    int64
}

// GetStats returns statistics about the database.
func (s *Store) GetStats() (*Stats, error) {
	stats := &Stats{}

	queries := []struct {
		query string
		dest  *int64
	}{
		{"SELECT COUNT(*) FROM messages", &stats.MessageCount},
		{"SELECT COUNT(*) FROM conversations", &stats.ThreadCount},
		{"SELECT COUNT(*) FROM attachments", &stats.AttachmentCount},
		{"SELECT COUNT(*) FROM labels", &stats.LabelCount},
		{"SELECT COUNT(*) FROM sources", &stats.SourceCount},
	}

	for _, q := range queries {
		if err := s.db.QueryRow(q.query).Scan(q.dest); err != nil {
			if s.dialect.IsNoSuchTableError(err) {
				continue
			}
			return nil, fmt.Errorf("get stats %q: %w", q.query, err)
		}
	}

	// Get database file size
	if info, err := os.Stat(s.dbPath); err == nil {
		stats.DatabaseSize = info.Size()
	}

	return stats, nil
}
