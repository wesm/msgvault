//go:build sqlite_vec

package sqlitevec

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
)

func TestMigrate_FreshAndIdempotent(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	path := filepath.Join(dir, "vectors.db")

	db := openTestDB(t, path)
	t.Cleanup(func() { _ = db.Close() })

	if err := Migrate(ctx, db, 768); err != nil {
		t.Fatalf("first migrate: %v", err)
	}

	for _, tbl := range []string{
		"index_generations", "embeddings", "embed_runs",
		"pending_embeddings", "vectors_vec_d768", "schema_version",
	} {
		var name string
		err := db.QueryRow(`SELECT name FROM sqlite_master WHERE name = ?`, tbl).Scan(&name)
		if err != nil {
			t.Errorf("table %s missing: %v", tbl, err)
		}
	}

	// Idempotent: running again must not error.
	if err := Migrate(ctx, db, 768); err != nil {
		t.Fatalf("second migrate: %v", err)
	}
}

func TestMigrate_CreatesDimensionSpecificVecTable(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t, filepath.Join(t.TempDir(), "v.db"))
	t.Cleanup(func() { _ = db.Close() })

	if err := Migrate(ctx, db, 768); err != nil {
		t.Fatalf("migrate 768: %v", err)
	}
	if err := EnsureVectorTable(ctx, db, 1024); err != nil {
		t.Fatalf("ensure 1024: %v", err)
	}
	var name string
	if err := db.QueryRow(
		`SELECT name FROM sqlite_master WHERE name = 'vectors_vec_d1024'`).Scan(&name); err != nil {
		t.Errorf("vectors_vec_d1024 not created: %v", err)
	}
}

func openTestDB(t *testing.T, path string) *sql.DB {
	t.Helper()
	if err := RegisterExtension(); err != nil {
		t.Fatalf("register: %v", err)
	}
	db, err := sql.Open(DriverName(), path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return db
}

// TestForeignKeys_PerConnection verifies that `PRAGMA foreign_keys = ON`
// applies to every pooled connection, not just the one that ran Migrate.
// SQLite's foreign_keys PRAGMA is per-connection, so a single ExecContext
// against *sql.DB only enables enforcement on whatever physical conn the
// pool happened to hand back. The ConnectHook in RegisterExtension is
// what makes enforcement pool-wide.
//
// We force the pool to allocate N distinct physical connections by
// holding N *sql.Conn handles open simultaneously — sequential
// db.Conn() calls or db.ExecContext() calls can all be served by the
// same pooled conn, which would let a buggy hook hide undetected.
func TestForeignKeys_PerConnection(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	path := filepath.Join(dir, "vectors.db")

	db := openTestDB(t, path)
	t.Cleanup(func() { _ = db.Close() })
	if err := Migrate(ctx, db, 768); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	const conns = 4
	db.SetMaxOpenConns(conns)

	// Acquire all N conns up front and keep them open. Each db.Conn()
	// call must allocate a fresh physical connection because earlier
	// ones haven't been released. This is what guarantees the hook is
	// being tested against distinct conns rather than a single reused
	// one.
	held := make([]*sql.Conn, conns)
	for i := 0; i < conns; i++ {
		c, err := db.Conn(ctx)
		if err != nil {
			t.Fatalf("conn %d: %v", i, err)
		}
		held[i] = c
	}
	t.Cleanup(func() {
		for _, c := range held {
			_ = c.Close()
		}
	})

	// Verify each held conn directly: PRAGMA foreign_keys must read
	// back as 1, and an FK-violating insert must fail on every conn.
	for i, c := range held {
		var fk int
		if err := c.QueryRowContext(ctx, `PRAGMA foreign_keys`).Scan(&fk); err != nil {
			t.Fatalf("conn %d pragma read: %v", i, err)
		}
		if fk != 1 {
			t.Errorf("conn %d: foreign_keys = %d, want 1 (ConnectHook missed this conn)", i, fk)
		}
		_, err := c.ExecContext(ctx,
			`INSERT INTO pending_embeddings (generation_id, message_id, enqueued_at)
			 VALUES (?, ?, ?)`, 9999999, int64(i), int64(i))
		if err == nil {
			t.Errorf("conn %d: FK-violating insert succeeded; foreign_keys not enforced", i)
			continue
		}
		msg := err.Error()
		if !strings.Contains(msg, "FOREIGN KEY") && !strings.Contains(msg, "foreign key") {
			t.Errorf("conn %d: error = %v; want FOREIGN KEY violation", i, err)
		}
	}
}
