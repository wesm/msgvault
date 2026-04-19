//go:build sqlite_vec

package sqlitevec

import (
	"context"
	"database/sql"
	"path/filepath"
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
