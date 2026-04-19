//go:build sqlite_vec

package sqlitevec

import (
	"context"
	"database/sql"
	"path/filepath"
	"sort"
	"testing"

	_ "github.com/mattn/go-sqlite3"

	"github.com/wesm/msgvault/internal/vector"
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

// unitVec returns a unit vector of the given dimension with 1.0 at
// position axis and 0.0 elsewhere.
func unitVec(dim, axis int) []float32 {
	v := make([]float32, dim)
	v[axis] = 1
	return v
}

// openFusedMainDB creates a main DB with the minimum schema FusedSearch
// needs: messages columns, messages_fts virtual table, and message_labels.
// It populates 3 non-deleted messages with searchable FTS content and
// returns the DB plus its temp file path (needed for ATTACH).
func openFusedMainDB(t *testing.T) (*sql.DB, string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "main.db")
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("open main: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	schema := `
CREATE TABLE messages (
    id INTEGER PRIMARY KEY,
    source_id INTEGER,
    sender_id INTEGER,
    has_attachments INTEGER DEFAULT 0,
    sent_at INTEGER,
    deleted_from_source_at DATETIME
);
CREATE VIRTUAL TABLE messages_fts USING fts5(subject, body, content='', contentless_delete=1);
CREATE TABLE message_labels (
    message_id INTEGER NOT NULL,
    label_id INTEGER NOT NULL,
    PRIMARY KEY (message_id, label_id)
);`
	if _, err := db.Exec(schema); err != nil {
		t.Fatalf("schema: %v", err)
	}

	rows := []struct {
		id      int64
		subject string
		body    string
	}{
		{1, "lunch plans", "want to grab lunch tomorrow"},
		{2, "meeting notes", "quarterly meeting agenda"},
		{3, "travel itinerary", "flight confirmation"},
	}
	for _, r := range rows {
		if _, err := db.Exec(`INSERT INTO messages (id) VALUES (?)`, r.id); err != nil {
			t.Fatalf("insert msg: %v", err)
		}
		if _, err := db.Exec(
			`INSERT INTO messages_fts (rowid, subject, body) VALUES (?, ?, ?)`,
			r.id, r.subject, r.body); err != nil {
			t.Fatalf("insert fts: %v", err)
		}
	}
	return db, path
}

// newFusedBackendForTest opens a backend pointing at a main DB seeded
// with FTS content and the minimum schema FusedSearch needs.
func newFusedBackendForTest(t *testing.T) (*Backend, context.Context, func()) {
	t.Helper()
	ctx := context.Background()
	main, mainPath := openFusedMainDB(t)
	vecPath := filepath.Join(t.TempDir(), "vectors.db")
	b, err := Open(ctx, Options{
		Path:      vecPath,
		MainPath:  mainPath,
		Dimension: 768,
		MainDB:    main,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	cleanup := func() { _ = b.Close() }
	t.Cleanup(cleanup)
	return b, ctx, cleanup
}

// seedAndEmbed inserts any missing message rows into the main DB,
// creates a fresh generation sized to the first vector, and upserts all
// supplied vectors as chunks. Returns the generation ID.
func seedAndEmbed(t *testing.T, b *Backend, vecs map[int64][]float32) vector.GenerationID {
	t.Helper()
	if len(vecs) == 0 {
		t.Fatal("seedAndEmbed: no vectors supplied")
	}
	ctx := context.Background()

	ids := make([]int64, 0, len(vecs))
	for id := range vecs {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	expectedDim := len(vecs[ids[0]])
	for _, id := range ids {
		if v := vecs[id]; len(v) != expectedDim {
			t.Fatalf("seedAndEmbed: vector for msg %d has %d dims, want %d", id, len(v), expectedDim)
		}
	}

	for _, id := range ids {
		if _, err := b.mainDB.ExecContext(ctx,
			`INSERT OR IGNORE INTO messages (id) VALUES (?)`, id); err != nil {
			t.Fatalf("seed message %d: %v", id, err)
		}
	}

	gid, err := b.CreateGeneration(ctx, "m", expectedDim)
	if err != nil {
		t.Fatalf("CreateGeneration: %v", err)
	}

	chunks := make([]vector.Chunk, 0, len(ids))
	for _, id := range ids {
		chunks = append(chunks, vector.Chunk{MessageID: id, Vector: vecs[id]})
	}
	if err := b.Upsert(ctx, gid, chunks); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	return gid
}
