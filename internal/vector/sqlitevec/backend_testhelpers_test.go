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
	var dim int
	for id, v := range vecs {
		ids = append(ids, id)
		if dim == 0 {
			dim = len(v)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	for _, id := range ids {
		if _, err := b.mainDB.ExecContext(ctx,
			`INSERT OR IGNORE INTO messages (id) VALUES (?)`, id); err != nil {
			t.Fatalf("seed message %d: %v", id, err)
		}
	}

	gid, err := b.CreateGeneration(ctx, "m", dim)
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
