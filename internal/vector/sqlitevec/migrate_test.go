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
func TestForeignKeys_PerConnection(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	path := filepath.Join(dir, "vectors.db")

	db := openTestDB(t, path)
	t.Cleanup(func() { _ = db.Close() })
	if err := Migrate(ctx, db, 768); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	// Force the pool to have multiple physical connections, then probe
	// each for FK enforcement. If the hook fails on any of them, one
	// will report foreign_keys = 0.
	db.SetMaxOpenConns(4)

	// Probe by inserting a pending_embeddings row with a non-existent
	// generation_id on several connections. The FK to index_generations
	// must fail every time.
	const probes = 6
	for i := 0; i < probes; i++ {
		_, err := db.ExecContext(ctx,
			`INSERT INTO pending_embeddings (generation_id, message_id, enqueued_at)
			 VALUES (?, ?, ?)`, 9999999, int64(i), int64(i))
		if err == nil {
			t.Errorf("probe %d: insert succeeded; foreign_keys enforcement missing on this connection", i)
			continue
		}
		msg := err.Error()
		if !strings.Contains(msg, "FOREIGN KEY") && !strings.Contains(msg, "foreign key") {
			t.Errorf("probe %d: error = %v; want FOREIGN KEY violation", i, err)
		}
	}

	// Also verify directly: each probe opens a fresh conn and reads
	// the PRAGMA back.
	for i := 0; i < probes; i++ {
		conn, err := db.Conn(ctx)
		if err != nil {
			t.Fatalf("conn %d: %v", i, err)
		}
		var fk int
		if err := conn.QueryRowContext(ctx, `PRAGMA foreign_keys`).Scan(&fk); err != nil {
			_ = conn.Close()
			t.Fatalf("conn %d pragma read: %v", i, err)
		}
		_ = conn.Close()
		if fk != 1 {
			t.Errorf("conn %d: foreign_keys = %d, want 1", i, fk)
		}
	}
}
