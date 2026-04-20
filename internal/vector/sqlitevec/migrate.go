//go:build sqlite_vec

package sqlitevec

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"
	"strings"
)

//go:embed schema.sql
var schemaSQL string

// Migrate runs the baseline schema and ensures a vec0 virtual table
// for `defaultDim` exists. Safe to run on every startup.
//
// PRAGMA foreign_keys is applied per-connection by the ConnectHook
// registered in RegisterExtension; it is intentionally not run here.
func Migrate(ctx context.Context, db *sql.DB, defaultDim int) error {
	if _, err := db.ExecContext(ctx, schemaSQL); err != nil {
		return fmt.Errorf("apply schema: %w", err)
	}
	// Idempotent column additions for databases initialized before
	// these columns existed. SQLite returns "duplicate column name"
	// when re-applied; we treat that as success.
	for _, m := range []struct {
		sql  string
		desc string
	}{
		{`ALTER TABLE index_generations ADD COLUMN seeded_at INTEGER`, "seeded_at"},
	} {
		if _, err := db.ExecContext(ctx, m.sql); err != nil &&
			!isDuplicateColumnErr(err) {
			return fmt.Errorf("migrate vectors.db (%s): %w", m.desc, err)
		}
	}
	if _, err := db.ExecContext(ctx, `PRAGMA journal_mode = WAL`); err != nil {
		return fmt.Errorf("enable WAL: %w", err)
	}
	return EnsureVectorTable(ctx, db, defaultDim)
}

// isDuplicateColumnErr matches SQLite's "duplicate column name" error
// from ALTER TABLE … ADD COLUMN re-runs. Same approach as
// internal/store/db_logger.go: substring match keeps the migration
// path driver-agnostic.
func isDuplicateColumnErr(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "duplicate column name")
}

// EnsureVectorTable creates vectors_vec_dN for the given dimension if
// it does not already exist. Idempotent.
func EnsureVectorTable(ctx context.Context, db *sql.DB, dim int) error {
	if dim <= 0 {
		return fmt.Errorf("invalid dimension %d", dim)
	}
	q := fmt.Sprintf(`CREATE VIRTUAL TABLE IF NOT EXISTS vectors_vec_d%d USING vec0(
		generation_id INTEGER PARTITION KEY,
		message_id    INTEGER PRIMARY KEY,
		embedding     FLOAT[%d]
	)`, dim, dim)
	if _, err := db.ExecContext(ctx, q); err != nil {
		return fmt.Errorf("create vectors_vec_d%d: %w", dim, err)
	}
	return nil
}

// VectorTableName returns the dimension-specific vec0 table name.
func VectorTableName(dim int) string {
	return fmt.Sprintf("vectors_vec_d%d", dim)
}
