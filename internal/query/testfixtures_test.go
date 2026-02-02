package query

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

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
	b.tables = append(b.tables, parquetTable{
		name:    name,
		subdir:  subdir,
		file:    file,
		columns: columns,
		values:  dummyValues,
		empty:   true,
	})
	return b
}

// build creates the temp directory, writes all Parquet files, and returns the
// analytics directory path and a cleanup function.
func (b *parquetBuilder) build() (string, func()) {
	b.t.Helper()

	tmpDir := b.createTempDirs()

	db, err := sql.Open("duckdb", "")
	if err != nil {
		os.RemoveAll(tmpDir)
		b.t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()

	b.writeParquetFiles(db, tmpDir)

	return tmpDir, func() { os.RemoveAll(tmpDir) }
}

// createTempDirs creates the temp directory and all required subdirectories.
func (b *parquetBuilder) createTempDirs() string {
	b.t.Helper()

	tmpDir, err := os.MkdirTemp("", "msgvault-test-parquet-*")
	if err != nil {
		b.t.Fatalf("create temp dir: %v", err)
	}

	for _, tbl := range b.tables {
		dir := filepath.Join(tmpDir, tbl.subdir)
		if err := os.MkdirAll(dir, 0755); err != nil {
			os.RemoveAll(tmpDir)
			b.t.Fatalf("create dir %s: %v", dir, err)
		}
	}

	return tmpDir
}

// writeParquetFiles writes all table data to Parquet files.
func (b *parquetBuilder) writeParquetFiles(db *sql.DB, tmpDir string) {
	b.t.Helper()

	for _, tbl := range b.tables {
		path := escapePath(filepath.Join(tmpDir, tbl.subdir, tbl.file))
		writeTableParquet(b.t, db, path, tbl.columns, tbl.values, tbl.empty)
	}
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

// createEngineFromBuilder builds Parquet files from the builder and returns a
// DuckDBEngine. Cleanup is registered via t.Cleanup.
func createEngineFromBuilder(t *testing.T, pb *parquetBuilder) *DuckDBEngine {
	t.Helper()
	analyticsDir, cleanup := pb.build()
	t.Cleanup(cleanup)
	engine, err := NewDuckDBEngine(analyticsDir, "", nil)
	if err != nil {
		t.Fatalf("NewDuckDBEngine: %v", err)
	}
	t.Cleanup(func() { engine.Close() })
	return engine
}

// assertAggregateCounts verifies that every key in want exists in got with the
// expected count, and that there are no extra rows.
func assertAggregateCounts(t *testing.T, got []AggregateRow, want map[string]int64) {
	t.Helper()
	gotMap := make(map[string]int64, len(got))
	for _, r := range got {
		gotMap[r.Key] = r.Count
	}
	for key, wantCount := range want {
		if gotCount, ok := gotMap[key]; !ok {
			t.Errorf("missing expected key %q", key)
		} else if gotCount != wantCount {
			t.Errorf("key %q: got count %d, want %d", key, gotCount, wantCount)
		}
	}
	for _, r := range got {
		if _, ok := want[r.Key]; !ok {
			t.Errorf("unexpected key %q (count=%d)", r.Key, r.Count)
		}
	}
}

// makeDate creates a time.Time for the given year, month, day in UTC with zero time.
func makeDate(year, month, day int) time.Time {
	return time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
}
