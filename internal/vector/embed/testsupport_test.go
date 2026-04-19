//go:build sqlite_vec

package embed

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/wesm/msgvault/internal/vector"
	"github.com/wesm/msgvault/internal/vector/sqlitevec"
)

// openVectorsDBWithPending opens a fresh vectors.db with one generation
// (id=1) and n pending rows for that generation. The database is closed
// automatically on test cleanup.
func openVectorsDBWithPending(t *testing.T, n int) *sql.DB {
	t.Helper()
	ctx := context.Background()
	if err := sqlitevec.RegisterExtension(); err != nil {
		t.Fatalf("RegisterExtension: %v", err)
	}
	path := filepath.Join(t.TempDir(), "vectors.db")
	db, err := sql.Open(sqlitevec.DriverName(), path)
	if err != nil {
		t.Fatalf("open vectors.db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := sqlitevec.Migrate(ctx, db, 768); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	if _, err := db.ExecContext(ctx, `
        INSERT INTO index_generations (id, model, dimension, fingerprint, started_at, state)
        VALUES (1, 'm', 768, 'm:768', 0, 'building')`); err != nil {
		t.Fatalf("insert generation: %v", err)
	}
	for i := 1; i <= n; i++ {
		if _, err := db.ExecContext(ctx,
			`INSERT INTO pending_embeddings (generation_id, message_id, enqueued_at) VALUES (1, ?, 0)`,
			i); err != nil {
			t.Fatalf("insert pending: %v", err)
		}
	}
	return db
}

// countAvailable returns the number of rows for gen whose claimed_at
// IS NULL (i.e. available to be claimed).
func countAvailable(t *testing.T, db *sql.DB, gen int64) int {
	t.Helper()
	var n int
	if err := db.QueryRow(
		`SELECT COUNT(*) FROM pending_embeddings WHERE generation_id = ? AND claimed_at IS NULL`,
		gen).Scan(&n); err != nil {
		t.Fatalf("countAvailable: %v", err)
	}
	return n
}

// workerFixture bundles everything needed for an end-to-end worker test.
type workerFixture struct {
	MainDB      *sql.DB
	VectorsDB   *sql.DB
	Backend     vector.Backend
	BuildingGen vector.GenerationID
	FakeClient  *fakeEmbeddingClient
}

// newWorkerFixture creates a main DB with n messages (subject="msg N",
// body="body N"), opens a real sqlitevec backend, creates a building
// generation (seeds pending_embeddings from the main DB), and installs a
// fakeEmbeddingClient that returns a deterministic vector per input.
func newWorkerFixture(t *testing.T, n int) *workerFixture {
	t.Helper()
	ctx := context.Background()

	dir := t.TempDir()
	mainPath := filepath.Join(dir, "main.db")
	if err := sqlitevec.RegisterExtension(); err != nil {
		t.Fatalf("RegisterExtension: %v", err)
	}
	mainDB, err := sql.Open(sqlitevec.DriverName(), mainPath)
	if err != nil {
		t.Fatalf("open main: %v", err)
	}
	t.Cleanup(func() { _ = mainDB.Close() })

	schema := `
CREATE TABLE messages (
    id INTEGER PRIMARY KEY,
    subject TEXT,
    deleted_from_source_at DATETIME
);
CREATE TABLE message_bodies (
    message_id INTEGER PRIMARY KEY,
    body_text TEXT
);`
	if _, err := mainDB.Exec(schema); err != nil {
		t.Fatalf("schema: %v", err)
	}
	for i := 1; i <= n; i++ {
		if _, err := mainDB.Exec(
			`INSERT INTO messages (id, subject) VALUES (?, ?)`, i, fmt.Sprintf("msg %d", i)); err != nil {
			t.Fatalf("insert message: %v", err)
		}
		if _, err := mainDB.Exec(
			`INSERT INTO message_bodies (message_id, body_text) VALUES (?, ?)`, i, fmt.Sprintf("body %d", i)); err != nil {
			t.Fatalf("insert body: %v", err)
		}
	}

	vecPath := filepath.Join(dir, "vectors.db")
	b, err := sqlitevec.Open(ctx, sqlitevec.Options{
		Path:      vecPath,
		MainPath:  mainPath,
		Dimension: 4,
		MainDB:    mainDB,
	})
	if err != nil {
		t.Fatalf("sqlitevec.Open: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })

	gid, err := b.CreateGeneration(ctx, "fake", 4)
	if err != nil {
		t.Fatalf("CreateGeneration: %v", err)
	}

	// The worker needs a *sql.DB for its VectorsDB field. Open a second
	// handle to vectors.db (SQLite handles concurrent file opens).
	vecDB, err := sql.Open(sqlitevec.DriverName(), vecPath)
	if err != nil {
		t.Fatalf("open vectors.db handle: %v", err)
	}
	t.Cleanup(func() { _ = vecDB.Close() })

	fc := &fakeEmbeddingClient{dim: 4}
	return &workerFixture{
		MainDB:      mainDB,
		VectorsDB:   vecDB,
		Backend:     b,
		BuildingGen: gid,
		FakeClient:  fc,
	}
}

// fakeEmbeddingClient returns a deterministic vector per input; tests
// may force failures with FailNext(n).
type fakeEmbeddingClient struct {
	dim   int
	failN int
	calls int
}

// FailNext forces the next n Embed calls to return an error.
func (c *fakeEmbeddingClient) FailNext(n int) { c.failN = n }

// Embed returns one deterministic, non-zero vector per input.
func (c *fakeEmbeddingClient) Embed(_ context.Context, inputs []string) ([][]float32, error) {
	c.calls++
	if c.failN > 0 {
		c.failN--
		return nil, fmt.Errorf("simulated embed failure (call %d)", c.calls)
	}
	out := make([][]float32, len(inputs))
	for i := range inputs {
		v := make([]float32, c.dim)
		// First component encodes input length mod dim — deterministic, non-zero.
		v[0] = float32(len(inputs[i])%c.dim + 1)
		out[i] = v
	}
	return out, nil
}
