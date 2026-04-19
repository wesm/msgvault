//go:build sqlite_vec

package sqlitevec

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

// openMainDBWithOneMessage creates an in-memory *sql.DB that looks enough
// like msgvault's main database for this test: a messages table with
// one non-deleted row (id=1).
func openMainDBWithOneMessage(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open main: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(`CREATE TABLE messages (
		id INTEGER PRIMARY KEY,
		deleted_from_source_at DATETIME
	)`); err != nil {
		t.Fatalf("create messages: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO messages (id) VALUES (1)`); err != nil {
		t.Fatalf("insert message: %v", err)
	}
	return db
}

// openBackendWithOneDeletedMessage is a variant where the only message
// is soft-deleted (deleted_from_source_at is set) — the seed query
// must skip it.
func openBackendWithOneDeletedMessage(t *testing.T) *Backend {
	t.Helper()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open main: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(`CREATE TABLE messages (
		id INTEGER PRIMARY KEY,
		deleted_from_source_at DATETIME
	)`); err != nil {
		t.Fatalf("create messages: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO messages (id, deleted_from_source_at) VALUES (1, CURRENT_TIMESTAMP)`); err != nil {
		t.Fatalf("insert deleted message: %v", err)
	}

	ctx := context.Background()
	b, err := Open(ctx, Options{
		Path:      filepath.Join(t.TempDir(), "vectors.db"),
		Dimension: 768,
		MainDB:    db,
	})
	if err != nil {
		t.Fatalf("Open backend: %v", err)
	}
	return b
}
