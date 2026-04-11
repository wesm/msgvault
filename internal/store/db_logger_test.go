package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// captureSlog installs a JSON handler over buf as the default
// slog logger for the duration of a test. Returns a cleanup
// closure that restores the previous default.
func captureSlog(t *testing.T, level slog.Level) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(
		&buf, &slog.HandlerOptions{Level: level},
	)))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return &buf
}

// openLoggedMem opens an in-memory sqlite DB wrapped by loggedDB.
func openLoggedMem(t *testing.T) *loggedDB {
	t.Helper()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open mem db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(
		"CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)",
	); err != nil {
		t.Fatalf("create table: %v", err)
	}
	return newLoggedDB(db)
}

func TestLoggedDB_ExecLogsStatement(t *testing.T) {
	// Force full trace so every exec shows up at INFO.
	ConfigureSQLLogging(SQLLogOptions{FullTrace: true})
	t.Cleanup(func() { ConfigureSQLLogging(SQLLogOptions{}) })

	buf := captureSlog(t, slog.LevelDebug)
	db := openLoggedMem(t)

	res, err := db.Exec(
		"INSERT INTO t (val) VALUES (?)", "hello",
	)
	if err != nil {
		t.Fatalf("exec: %v", err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		t.Errorf("rows_affected = %d, want 1", n)
	}

	// Find the sql line in the captured output.
	rec := findLogLine(t, buf, "sql")
	if rec["kind"] != "exec" {
		t.Errorf("kind = %v, want exec", rec["kind"])
	}
	if !strings.Contains(
		rec["stmt"].(string), "INSERT INTO t",
	) {
		t.Errorf("stmt missing: %v", rec["stmt"])
	}
	if rec["rows_affected"].(float64) != 1 {
		t.Errorf("rows_affected = %v, want 1", rec["rows_affected"])
	}
	if rec["nargs"].(float64) != 1 {
		t.Errorf("nargs = %v, want 1", rec["nargs"])
	}
}

func TestLogStmt_SlowQueryPromotedToWarn(t *testing.T) {
	// Drive the emitter directly with a synthetic elapsed time
	// to avoid flakiness from "actually make a query slow".
	ConfigureSQLLogging(SQLLogOptions{SlowMs: 50})
	t.Cleanup(func() { ConfigureSQLLogging(SQLLogOptions{}) })

	buf := captureSlog(t, slog.LevelDebug)
	logStmtWith(
		"exec", "INSERT INTO t VALUES (?)", 1,
		nil, 100*time.Millisecond,
	)

	rec := findLogLineByMsg(t, buf, "sql slow")
	if rec == nil {
		t.Fatalf("no sql slow line found; buf=%s",
			buf.String())
	}
	if rec["level"] != "WARN" {
		t.Errorf("level = %v, want WARN", rec["level"])
	}
	if rec["duration_ms"].(float64) != 100 {
		t.Errorf("duration_ms = %v, want 100",
			rec["duration_ms"])
	}
}

func TestLoggedDB_ErrorAlwaysLogged(t *testing.T) {
	ConfigureSQLLogging(SQLLogOptions{})
	buf := captureSlog(t, slog.LevelDebug)
	db := openLoggedMem(t)

	_, err := db.ExecContext(
		context.Background(), "INSERT INTO no_such_table VALUES (1)",
	)
	if err == nil {
		t.Fatal("expected exec error")
	}
	if !errors.Is(err, err) {
		t.Errorf("bad error shape")
	}

	rec := findLogLineByMsg(t, buf, "sql error")
	if rec == nil {
		t.Fatalf("no sql error line; buf=%s", buf.String())
	}
	if rec["level"] != "WARN" {
		t.Errorf("level = %v, want WARN", rec["level"])
	}
	if _, ok := rec["error"]; !ok {
		t.Errorf("error attr missing: %v", rec)
	}
}

func TestLoggedDB_QueryRowLogsButNoError(t *testing.T) {
	ConfigureSQLLogging(SQLLogOptions{FullTrace: true})
	t.Cleanup(func() { ConfigureSQLLogging(SQLLogOptions{}) })

	buf := captureSlog(t, slog.LevelDebug)
	db := openLoggedMem(t)

	if _, err := db.Exec(
		"INSERT INTO t (val) VALUES ('row')",
	); err != nil {
		t.Fatalf("seed: %v", err)
	}
	var got string
	if err := db.QueryRow(
		"SELECT val FROM t WHERE id = ?", 1,
	).Scan(&got); err != nil {
		t.Fatalf("queryrow: %v", err)
	}
	if got != "row" {
		t.Errorf("got = %q, want row", got)
	}

	// Expect to see both an exec line and a queryrow line.
	seen := map[string]bool{}
	for _, rec := range decodeAll(t, buf) {
		if kind, ok := rec["kind"].(string); ok {
			seen[kind] = true
		}
	}
	if !seen["exec"] || !seen["queryrow"] {
		t.Errorf("missing kinds; seen=%v", seen)
	}
}

func TestNormalizeStmt_CollapsesWhitespace(t *testing.T) {
	in := "SELECT\n  *\nFROM\n\tt WHERE id = ?"
	got := normalizeStmt(in, 0)
	want := "SELECT * FROM t WHERE id = ?"
	if got != want {
		t.Errorf("got %q want %q", got, want)
	}
}

func TestNormalizeStmt_TruncatesLong(t *testing.T) {
	in := strings.Repeat("a", 500)
	got := normalizeStmt(in, 100)
	if len(got) != 103 || !strings.HasSuffix(got, "...") {
		t.Errorf("bad truncation: len=%d tail=%q",
			len(got), got[len(got)-3:])
	}
}

// ---- test helpers ----

// findLogLine returns the first record whose msg matches exactly.
func findLogLine(
	t *testing.T, buf *bytes.Buffer, msg string,
) map[string]any {
	t.Helper()
	for _, rec := range decodeAll(t, buf) {
		if rec["msg"] == msg {
			return rec
		}
	}
	t.Fatalf("no log line with msg=%q; buf=%s", msg, buf.String())
	return nil
}

// findLogLineByMsg is like findLogLine but returns nil rather
// than failing so callers can assert absence.
func findLogLineByMsg(
	t *testing.T, buf *bytes.Buffer, msg string,
) map[string]any {
	t.Helper()
	for _, rec := range decodeAll(t, buf) {
		if rec["msg"] == msg {
			return rec
		}
	}
	return nil
}

func decodeAll(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	dec := json.NewDecoder(bytes.NewReader(buf.Bytes()))
	for {
		var rec map[string]any
		if err := dec.Decode(&rec); err != nil {
			break
		}
		out = append(out, rec)
	}
	return out
}
