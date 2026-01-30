package query

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/marcboeker/go-duckdb"
)

// parquetTable defines a table to be written as a Parquet file.
type parquetTable struct {
	name    string // e.g. "messages", "sources"
	subdir  string // subdirectory path relative to tmpDir, e.g. "messages/year=2024"
	file    string // filename, e.g. "data.parquet"
	columns string // column definition for the VALUES AS clause
	values  string // SQL VALUES rows (without the outer VALUES keyword)
	empty   bool   // if true, write schema-only empty file using WHERE false
}

// parquetBuilder creates a temp directory with Parquet test data files.
type parquetBuilder struct {
	t      *testing.T
	tables []parquetTable
}

// newParquetBuilder creates a new builder for Parquet test fixtures.
func newParquetBuilder(t *testing.T) *parquetBuilder {
	t.Helper()
	return &parquetBuilder{t: t}
}

// addTable adds a table definition to be written as Parquet.
func (b *parquetBuilder) addTable(name, subdir, file, columns, values string) *parquetBuilder {
	b.tables = append(b.tables, parquetTable{
		name:    name,
		subdir:  subdir,
		file:    file,
		columns: columns,
		values:  values,
	})
	return b
}

// addEmptyTable adds an empty table (schema only, no rows) to be written as Parquet.
func (b *parquetBuilder) addEmptyTable(name, subdir, file, columns, dummyValues string) *parquetBuilder {
	return b.addTable(name, subdir, file, columns, dummyValues).withEmpty()
}

// withEmpty marks the last added table as empty (schema-only).
func (b *parquetBuilder) withEmpty() *parquetBuilder {
	if len(b.tables) == 0 {
		b.t.Fatal("withEmpty called on builder with no tables")
	}
	b.tables[len(b.tables)-1].empty = true
	return b
}

// build creates the temp directory, writes all Parquet files, and returns the
// analytics directory path and a cleanup function.
func (b *parquetBuilder) build() (string, func()) {
	b.t.Helper()

	tmpDir, err := os.MkdirTemp("", "msgvault-test-parquet-*")
	if err != nil {
		b.t.Fatalf("create temp dir: %v", err)
	}

	// Create all required directories
	for _, tbl := range b.tables {
		dir := filepath.Join(tmpDir, tbl.subdir)
		if err := os.MkdirAll(dir, 0755); err != nil {
			os.RemoveAll(tmpDir)
			b.t.Fatalf("create dir %s: %v", dir, err)
		}
	}

	// Open DuckDB for data generation
	db, err := sql.Open("duckdb", "")
	if err != nil {
		os.RemoveAll(tmpDir)
		b.t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()

	// Write each table
	for _, tbl := range b.tables {
		path := escapePath(filepath.Join(tmpDir, tbl.subdir, tbl.file))
		writeTableParquet(b.t, db, path, tbl.columns, tbl.values, tbl.empty)
	}

	cleanup := func() {
		os.RemoveAll(tmpDir)
	}
	return tmpDir, cleanup
}

// escapePath normalizes a file path for use in DuckDB SQL strings.
func escapePath(p string) string {
	return strings.ReplaceAll(filepath.ToSlash(p), "'", "''")
}

// writeTableParquet writes a single table's data to a Parquet file using DuckDB.
func writeTableParquet(t *testing.T, db *sql.DB, path, columns, values string, empty bool) {
	t.Helper()

	whereClause := ""
	if empty {
		whereClause = "\n\t\t\t\tWHERE false"
	}
	query := fmt.Sprintf(`
			COPY (
				SELECT * FROM (VALUES %s) AS t(%s)%s
			) TO '%s' (FORMAT PARQUET)
		`, values, columns, whereClause, path)

	if _, err := db.Exec(query); err != nil {
		t.Fatalf("create parquet %s: %v", path, err)
	}
}

// Common column definitions for Parquet tables.
const (
	messagesCols          = "id, source_id, source_message_id, conversation_id, subject, snippet, sent_at, size_estimate, has_attachments, deleted_from_source_at, year, month"
	sourcesCols           = "id, account_email"
	participantsCols      = "id, email_address, domain, display_name"
	messageRecipientsCols = "message_id, participant_id, recipient_type, display_name"
	labelsCols            = "id, name"
	messageLabelsCols     = "message_id, label_id"
	attachmentsCols       = "message_id, size, filename"
)

// assertAggregateCounts checks multiple key/count pairs against aggregate results.
func assertAggregateCounts(t *testing.T, results []AggregateRow, expected map[string]int64) {
	t.Helper()
	byKey := make(map[string]int64, len(results))
	for _, r := range results {
		byKey[r.Key] = r.Count
	}
	for key, wantCount := range expected {
		gotCount, ok := byKey[key]
		if !ok {
			t.Errorf("key %q not found in results", key)
		} else if gotCount != wantCount {
			t.Errorf("%s: expected count %d, got %d", key, wantCount, gotCount)
		}
	}
}
