// Package store provides database access for msgvault.
package store

import (
	"database/sql"
	"embed"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

//go:embed schema.sql schema_sqlite.sql
var schemaFS embed.FS

// Store provides database operations for msgvault.
type Store struct {
	db            *sql.DB
	dbPath        string
	fts5Available bool // Whether FTS5 is available for full-text search
}

// Open opens or creates the database at the given path.
// Currently only SQLite is supported. PostgreSQL URLs will return an error.
func Open(dbPath string) (*Store, error) {
	// Check for unsupported database URLs
	if strings.HasPrefix(dbPath, "postgresql://") || strings.HasPrefix(dbPath, "postgres://") {
		return nil, fmt.Errorf("PostgreSQL is not yet supported in the Go implementation; use SQLite path instead")
	}

	// Ensure directory exists
	dir := filepath.Dir(dbPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create db directory: %w", err)
	}

	// Open with WAL mode and busy timeout for better concurrency
	dsn := dbPath + "?_journal_mode=WAL&_busy_timeout=5000&_foreign_keys=ON"
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}

	// Test connection
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}

	return &Store{
		db:     db,
		dbPath: dbPath,
	}, nil
}

// Close closes the database connection.
func (s *Store) Close() error {
	return s.db.Close()
}

// DB returns the underlying database connection for advanced queries.
func (s *Store) DB() *sql.DB {
	return s.db
}

// withTx executes fn within a database transaction. If fn returns an error,
// the transaction is rolled back; otherwise it is committed.
func (s *Store) withTx(fn func(tx *sql.Tx) error) error {
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

// queryInChunks executes a parameterized IN-query in chunks to stay within
// SQLite's parameter limit. queryTemplate must contain a single %s placeholder
// for the comma-separated "?" list. The prefix args are prepended before each
// chunk's args (e.g., a source_id filter).
func queryInChunks[T any](db *sql.DB, ids []T, prefixArgs []interface{}, queryTemplate string, fn func(*sql.Rows) error) error {
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
				rows.Close()
				return err
			}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
	}
	return nil
}

// Rebind converts a query with ? placeholders to the appropriate format
// for the current database driver. Currently SQLite-only (no conversion needed).
// When PostgreSQL support is added, this will convert ? to $1, $2, etc.
func (s *Store) Rebind(query string) string {
	// SQLite uses ? placeholders, no conversion needed
	// TODO: When adding PostgreSQL support, convert ? to $1, $2, etc.
	return query
}

// InitSchema initializes the database schema.
// This creates all tables if they don't exist.
func (s *Store) InitSchema() error {
	// Load and execute main schema
	schema, err := schemaFS.ReadFile("schema.sql")
	if err != nil {
		return fmt.Errorf("read schema.sql: %w", err)
	}

	if _, err := s.db.Exec(string(schema)); err != nil {
		return fmt.Errorf("execute schema.sql: %w", err)
	}

	// Try to load and execute SQLite-specific schema (FTS5)
	// This is optional - FTS5 may not be available in all builds
	sqliteSchema, err := schemaFS.ReadFile("schema_sqlite.sql")
	if err != nil {
		return fmt.Errorf("read schema_sqlite.sql: %w", err)
	}

	if _, err := s.db.Exec(string(sqliteSchema)); err != nil {
		// FTS5 not available - this is OK, search will be degraded
		s.fts5Available = false
	} else {
		s.fts5Available = true
	}

	return nil
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
			// Table might not exist yet
			if err != sql.ErrNoRows {
				continue
			}
		}
	}

	// Get database file size
	if info, err := os.Stat(s.dbPath); err == nil {
		stats.DatabaseSize = info.Size()
	}

	return stats, nil
}
