//go:build sqlite_vec

package embed

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

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
