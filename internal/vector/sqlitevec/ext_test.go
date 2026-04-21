//go:build sqlite_vec

package sqlitevec

import (
	"database/sql"
	"testing"
)

func TestSQLiteVecExtensionLoads(t *testing.T) {
	if err := RegisterExtension(); err != nil {
		t.Fatalf("RegisterExtension: %v", err)
	}
	db, err := sql.Open(DriverName(), ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	_, err = db.Exec(`CREATE VIRTUAL TABLE t USING vec0(
		generation_id INTEGER PARTITION KEY,
		message_id INTEGER PRIMARY KEY,
		embedding FLOAT[4]
	)`)
	if err != nil {
		t.Fatalf("create virtual table: %v", err)
	}

	// Sanity: insert and query a vector.
	// Little-endian float32 blob for [1.0, 0.0, 0.0, 0.0].
	_, err = db.Exec(`INSERT INTO t (generation_id, message_id, embedding) VALUES (?, ?, ?)`,
		1, 42, []byte{0, 0, 0x80, 0x3f, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})
	if err != nil {
		t.Fatalf("insert vector: %v", err)
	}
}
