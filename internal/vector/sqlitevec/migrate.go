//go:build sqlite_vec

package sqlitevec

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"
)

//go:embed schema.sql
var schemaSQL string

// Migrate runs the baseline schema and ensures a vec0 virtual table
// for `defaultDim` exists. Safe to run on every startup.
func Migrate(ctx context.Context, db *sql.DB, defaultDim int) error {
	if _, err := db.ExecContext(ctx, schemaSQL); err != nil {
		return fmt.Errorf("apply schema: %w", err)
	}
	if _, err := db.ExecContext(ctx, `PRAGMA journal_mode = WAL`); err != nil {
		return fmt.Errorf("enable WAL: %w", err)
	}
	if _, err := db.ExecContext(ctx, `PRAGMA foreign_keys = ON`); err != nil {
		return fmt.Errorf("enable foreign_keys: %w", err)
	}
	return EnsureVectorTable(ctx, db, defaultDim)
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
