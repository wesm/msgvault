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
	"unicode/utf8"

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
	return newLoggedDB(db, nil)
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
		"exec", "INSERT INTO t VALUES (?)", []any{"v"},
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

// TestLoggedRows_LogsAtClose verifies that the timing log line
// for a streaming Query is emitted on Close, not at QueryContext
// return. This is the behaviour change that gives streaming queries
// honest duration_ms numbers.
func TestLoggedRows_LogsAtClose(t *testing.T) {
	ConfigureSQLLogging(SQLLogOptions{FullTrace: true})
	t.Cleanup(func() { ConfigureSQLLogging(SQLLogOptions{}) })

	buf := captureSlog(t, slog.LevelDebug)
	db := openLoggedMem(t)
	if _, err := db.Exec(
		"INSERT INTO t (val) VALUES ('a'), ('b'), ('c')",
	); err != nil {
		t.Fatalf("seed: %v", err)
	}
	// Reset buffer so we only see the post-Query log line(s).
	buf.Reset()

	rows, err := db.Query("SELECT val FROM t ORDER BY id")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if findLogLineByMsg(t, buf, "sql") != nil {
		t.Fatalf("query log emitted before Close; want only at Close. buf=%s",
			buf.String())
	}

	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			t.Fatalf("scan: %v", err)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	rec := findLogLine(t, buf, "sql")
	if rec["kind"] != "query" {
		t.Errorf("kind = %v, want query", rec["kind"])
	}
}

// TestLoggedRows_CloseIdempotent verifies that double-Close
// (e.g. an early-return defer plus an explicit close) does not
// emit two log lines.
func TestLoggedRows_CloseIdempotent(t *testing.T) {
	ConfigureSQLLogging(SQLLogOptions{FullTrace: true})
	t.Cleanup(func() { ConfigureSQLLogging(SQLLogOptions{}) })

	buf := captureSlog(t, slog.LevelDebug)
	db := openLoggedMem(t)
	rows, err := db.Query("SELECT 1")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	for rows.Next() {
		var n int
		_ = rows.Scan(&n)
	}
	_ = rows.Close()
	_ = rows.Close()

	count := 0
	for _, rec := range decodeAll(t, buf) {
		if rec["msg"] == "sql" && rec["kind"] == "query" {
			count++
		}
	}
	if count != 1 {
		t.Errorf("got %d query log lines, want 1", count)
	}
}

// TestLoggedRows_QueryErrorLogsImmediately verifies that an
// error returned from db.Query (e.g. bad SQL, no such table)
// is logged at the QueryContext call site, not deferred to a
// Close call that would never happen because no rows handle
// is returned.
func TestLoggedRows_QueryErrorLogsImmediately(t *testing.T) {
	ConfigureSQLLogging(SQLLogOptions{})
	buf := captureSlog(t, slog.LevelDebug)
	db := openLoggedMem(t)

	_, err := db.Query("SELECT * FROM no_such_table")
	if err == nil {
		t.Fatal("expected query error")
	}
	rec := findLogLineByMsg(t, buf, "sql error")
	if rec == nil {
		t.Fatalf("no sql error line; buf=%s", buf.String())
	}
}

// TestLoggedRows_FinalizesAtEndOfScan verifies that duration_ms
// is captured when iteration ends (Next returns false), not when
// Close is eventually called. Most callers defer Close, so any
// time spent between the last row and the deferred Close (count
// queries, batchPopulate, unrelated work) would otherwise be
// charged to the streaming query. The end-of-Next finalizer
// keeps the timing honest.
func TestLoggedRows_FinalizesAtEndOfScan(t *testing.T) {
	ConfigureSQLLogging(SQLLogOptions{FullTrace: true})
	t.Cleanup(func() { ConfigureSQLLogging(SQLLogOptions{}) })

	buf := captureSlog(t, slog.LevelDebug)
	db := openLoggedMem(t)
	if _, err := db.Exec(
		"INSERT INTO t (val) VALUES ('a'), ('b'), ('c')",
	); err != nil {
		t.Fatalf("seed: %v", err)
	}
	buf.Reset()

	rows, err := db.Query("SELECT val FROM t ORDER BY id")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			t.Fatalf("scan: %v", err)
		}
	}
	// The log line must be emitted by the time Next returns
	// false, before any deferred Close fires.
	rec := findLogLine(t, buf, "sql")
	if rec["kind"] != "query" {
		t.Errorf("kind = %v, want query", rec["kind"])
	}
	durAtEndOfScan := rec["duration_ms"].(float64)

	// Simulate caller doing unrelated work between end-of-scan
	// and the deferred Close. The log line must not be re-emitted
	// and the duration must already be recorded.
	time.Sleep(50 * time.Millisecond)
	if err := rows.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	count := 0
	var lastDuration float64
	for _, r := range decodeAll(t, buf) {
		if r["msg"] == "sql" && r["kind"] == "query" {
			count++
			lastDuration = r["duration_ms"].(float64)
		}
	}
	if count != 1 {
		t.Errorf("got %d query log lines, want exactly 1", count)
	}
	// Duration recorded at end-of-scan must not include the 50ms
	// of post-iteration work — give a generous ceiling so a slow
	// CI host doesn't flake.
	if lastDuration != durAtEndOfScan {
		t.Errorf("duration_ms changed after Close: %v -> %v",
			durAtEndOfScan, lastDuration)
	}
	if lastDuration >= 40 {
		t.Errorf("duration_ms %v includes post-iteration sleep; "+
			"finalizer should run at end-of-Next", lastDuration)
	}
}

// TestLoggedRows_EarlyExitFinalizesOnClose covers the path where
// the caller breaks out of the Next loop without exhausting rows.
// The finalizer must run from Close on that path so the log line
// is still emitted exactly once.
func TestLoggedRows_EarlyExitFinalizesOnClose(t *testing.T) {
	ConfigureSQLLogging(SQLLogOptions{FullTrace: true})
	t.Cleanup(func() { ConfigureSQLLogging(SQLLogOptions{}) })

	buf := captureSlog(t, slog.LevelDebug)
	db := openLoggedMem(t)
	if _, err := db.Exec(
		"INSERT INTO t (val) VALUES ('a'), ('b'), ('c')",
	); err != nil {
		t.Fatalf("seed: %v", err)
	}
	buf.Reset()

	rows, err := db.Query("SELECT val FROM t ORDER BY id")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	// Read a single row and break — finalizer should not fire yet.
	if !rows.Next() {
		t.Fatalf("expected at least one row")
	}
	var v string
	if err := rows.Scan(&v); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if findLogLineByMsg(t, buf, "sql") != nil {
		t.Fatalf("log line emitted before Close on early-exit path")
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	rec := findLogLine(t, buf, "sql")
	if rec["kind"] != "query" {
		t.Errorf("kind = %v, want query", rec["kind"])
	}
}

// TestLoggedRows_IterationErrorSurfacedOnClose verifies that a
// context cancellation discovered during rows.Next() is logged
// as an error on Close, even when Rows.Close() itself returns
// nil. Without checking Rows.Err(), a cancelled scan would log
// as a successful query.
func TestLoggedRows_IterationErrorSurfacedOnClose(t *testing.T) {
	ConfigureSQLLogging(SQLLogOptions{})
	buf := captureSlog(t, slog.LevelDebug)
	db := openLoggedMem(t)
	if _, err := db.Exec(
		"INSERT INTO t (val) VALUES ('a'), ('b'), ('c')",
	); err != nil {
		t.Fatalf("seed: %v", err)
	}
	buf.Reset()

	ctx, cancel := context.WithCancel(context.Background())
	rows, err := db.QueryContext(ctx, "SELECT val FROM t ORDER BY id")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	// Cancel before iterating; Next() will see the cancellation
	// and stop, leaving the error on Rows.Err(), not Close().
	cancel()
	for rows.Next() {
	}
	_ = rows.Close()

	rec := findLogLineByMsg(t, buf, "sql error")
	if rec == nil {
		t.Fatalf("expected sql error line for cancelled scan; buf=%s", buf.String())
	}
	if errStr, _ := rec["error"].(string); errStr == "" {
		t.Errorf("error attr missing or empty: %v", rec)
	}
}

// TestLogStmt_SlowQueryIncludesArgsShape verifies that a slow
// query attaches an "args_shape" attr describing each bound
// parameter's type and length, but never the raw value. Raw
// values can carry PII (addresses, subjects, tokens) and must
// not be persisted in logs by default.
func TestLogStmt_SlowQueryIncludesArgsShape(t *testing.T) {
	ConfigureSQLLogging(SQLLogOptions{SlowMs: 50})
	t.Cleanup(func() { ConfigureSQLLogging(SQLLogOptions{}) })

	buf := captureSlog(t, slog.LevelDebug)
	logStmtWith(
		"query", "SELECT * FROM t WHERE id = ? AND src = ?",
		[]any{int64(42), "gmail"},
		nil, 100*time.Millisecond,
	)

	rec := findLogLineByMsg(t, buf, "sql slow")
	if rec == nil {
		t.Fatalf("no sql slow line; buf=%s", buf.String())
	}
	gotShape, ok := rec["args_shape"].(string)
	if !ok {
		t.Fatalf("args_shape attr missing or wrong type: %v", rec["args_shape"])
	}
	// Type info present.
	if !strings.Contains(gotShape, "int64") {
		t.Errorf("args_shape missing int64 type: %q", gotShape)
	}
	if !strings.Contains(gotShape, "string(len=5)") {
		t.Errorf("args_shape missing string length: %q", gotShape)
	}
	// Raw values must not appear.
	if strings.Contains(gotShape, "42") {
		t.Errorf("args_shape leaked numeric value: %q", gotShape)
	}
	if strings.Contains(gotShape, "gmail") {
		t.Errorf("args_shape leaked string value: %q", gotShape)
	}
	// Legacy "args" attr must not be present.
	if _, present := rec["args"]; present {
		t.Errorf("legacy args attr should not be set: %v", rec)
	}
}

// TestLogStmt_FullTraceOmitsArgs verifies that --full-trace mode
// does not attach args or args_shape. nargs is enough at
// high-volume Info level.
func TestLogStmt_FullTraceOmitsArgs(t *testing.T) {
	ConfigureSQLLogging(SQLLogOptions{FullTrace: true})
	t.Cleanup(func() { ConfigureSQLLogging(SQLLogOptions{}) })

	buf := captureSlog(t, slog.LevelDebug)
	logStmtWith(
		"query", "SELECT * FROM t WHERE id = ?",
		[]any{int64(42)},
		nil, 1*time.Millisecond,
	)

	rec := findLogLine(t, buf, "sql")
	if _, present := rec["args"]; present {
		t.Errorf("args should not be present on info/full-trace lines: %v", rec)
	}
	if _, present := rec["args_shape"]; present {
		t.Errorf("args_shape should not be present on info/full-trace lines: %v", rec)
	}
	if rec["nargs"].(float64) != 1 {
		t.Errorf("nargs = %v, want 1", rec["nargs"])
	}
}

// TestFormatArgsShape_RedactsValues ensures the shape formatter
// emits type and length only, never raw values, even for long
// strings that could carry sensitive content.
func TestFormatArgsShape_RedactsValues(t *testing.T) {
	long := strings.Repeat("x", 200)
	got := formatArgsShape([]any{long, "secret-token", []byte("hello world"), nil, int64(42)})
	if strings.Contains(got, "x") || strings.Contains(got, "secret-token") {
		t.Errorf("shape leaked raw string: %q", got)
	}
	if strings.Contains(got, "hello world") {
		t.Errorf("shape leaked raw bytes: %q", got)
	}
	if strings.Contains(got, "42") {
		t.Errorf("shape leaked raw numeric: %q", got)
	}
	for _, want := range []string{
		"string(len=200)",
		"string(len=12)",
		"bytes(len=11)",
		"nil",
		"int64",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("shape missing %q: %q", want, got)
		}
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
	// Long uniform input gets a head + " ... " + tail split.
	// The truncation budget includes the separator, so the
	// final string is exactly maxChars long.
	in := strings.Repeat("a", 500)
	got := normalizeStmt(in, 100)
	const sep = " ... "
	if !strings.Contains(got, sep) {
		t.Errorf("missing separator: %q", got)
	}
	if len(got) != 100 {
		t.Errorf("bad length: len=%d want=%d", len(got), 100)
	}
}

// TestNormalizeStmt_KeepsWhereClause is the guard for the bug
// that motivated head+tail truncation: a long SELECT whose only
// distinguishing feature is the WHERE clause must remain
// distinguishable in the logs.
func TestNormalizeStmt_KeepsWhereClause(t *testing.T) {
	in := "SELECT m.id, m.source_id, s.source_type, s.identifier, " +
		"m.source_message_id, COALESCE(m.subject, ''), m.sent_at, " +
		"m.archived_at, (CASE WHEN mr.message_id IS NOT NULL THEN 1 " +
		"ELSE 0 END) AS has_raw, (SELECT COUNT(*) FROM message_labels " +
		"ml WHERE ml.message_id = m.id) AS label_count, " +
		"COALESCE(m.is_from_me, 0) AS is_from_me " +
		"FROM messages m JOIN sources s ON s.id = m.source_id " +
		"WHERE m.rfc822_message_id = ? AND m.deleted_at IS NULL"
	got := normalizeStmt(in, 300)
	if !strings.Contains(got, "WHERE m.rfc822_message_id") {
		t.Errorf("WHERE clause missing from truncated stmt: %q", got)
	}
}

// TestNormalizeStmt_TinyBudgetFallsBackToHead protects the
// edge case where the budget is too small for a meaningful
// head+tail split.
func TestNormalizeStmt_TinyBudgetFallsBackToHead(t *testing.T) {
	in := strings.Repeat("a", 50)
	got := normalizeStmt(in, 8)
	if !strings.HasSuffix(got, "...") {
		t.Errorf("expected trailing ellipsis on tiny budget; got %q", got)
	}
	if strings.Contains(got, " ... ") {
		t.Errorf("did not expect head+tail split on tiny budget; got %q", got)
	}
}

// TestNormalizeStmt_UTF8Safe ensures truncation respects rune
// boundaries — multi-byte characters in SQL literals or comments
// must not be split, which would emit invalid UTF-8 to logs.
func TestNormalizeStmt_UTF8Safe(t *testing.T) {
	// Each "café — 漢" is 13 bytes / 7 runes; repeat to exceed
	// any reasonable budget.
	in := strings.Repeat("café — 漢 ", 30)
	got := normalizeStmt(in, 50)
	if !utf8.ValidString(got) {
		t.Errorf("normalizeStmt returned invalid UTF-8: %q", got)
	}
	// Tiny-budget head-only path.
	got2 := normalizeStmt(in, 8)
	if !utf8.ValidString(got2) {
		t.Errorf("tiny-budget normalizeStmt returned invalid UTF-8: %q", got2)
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
