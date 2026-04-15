package store

import (
	"context"
	"database/sql"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"
)

// SQLLogOptions controls the store-level SQL logging behaviour.
// Configured once per process via ConfigureSQLLogging(); every
// Store subsequently opened picks up the values atomically.
type SQLLogOptions struct {
	// SlowMs is the threshold above which any query is logged
	// at Warn regardless of the normal logging level. Zero
	// means "never flag as slow". Defaults to 100 when the
	// caller passes a zero-value struct to ConfigureSQLLogging.
	SlowMs int64

	// FullTrace makes every query emit an Info-level log line
	// (not just Debug). Use with care — generates one line per
	// query, which is enormous volume for anything non-trivial.
	FullTrace bool

	// MaxStmtChars truncates logged SQL at this many characters.
	// 0 disables truncation. Defaults to 300.
	MaxStmtChars int
}

// Package-level atomic config so every *loggedDB instance in the
// process reads the same settings without passing options through
// every store.Open call. Atomics avoid a mutex on the hot path —
// these values are read for every query.
var (
	sqlLogSlowMs   atomic.Int64
	sqlLogFull     atomic.Bool
	sqlLogMaxChars atomic.Int64
)

func init() {
	sqlLogSlowMs.Store(100)
	sqlLogMaxChars.Store(300)
}

// ConfigureSQLLogging sets the process-wide SQL logging behaviour.
// Call this after slog.SetDefault but before opening a Store.
func ConfigureSQLLogging(opts SQLLogOptions) {
	slow := opts.SlowMs
	if slow == 0 {
		slow = 100
	}
	max := opts.MaxStmtChars
	if max == 0 {
		max = 300
	}
	sqlLogSlowMs.Store(slow)
	sqlLogFull.Store(opts.FullTrace)
	sqlLogMaxChars.Store(int64(max))
}

// loggedDB wraps *sql.DB and emits slog records for every query
// it executes. It embeds *sql.DB so store methods continue to
// compile against the sql.DB method surface — the Query/Exec
// overrides below shadow the embedded ones.
type loggedDB struct {
	*sql.DB
}

func newLoggedDB(db *sql.DB) *loggedDB {
	return &loggedDB{DB: db}
}

// Query logs the statement via logStmt and delegates to the
// embedded sql.DB. Uses a background context to match the
// sql.DB.Query semantics.
func (d *loggedDB) Query(
	query string, args ...any,
) (*sql.Rows, error) {
	return d.QueryContext(context.Background(), query, args...)
}

// QueryContext logs the statement and delegates.
func (d *loggedDB) QueryContext(
	ctx context.Context, query string, args ...any,
) (*sql.Rows, error) {
	start := time.Now()
	rows, err := d.DB.QueryContext(ctx, query, args...)
	logStmt("query", query, len(args), err, time.Since(start))
	return rows, err
}

// QueryRow logs and delegates. sql.Row does not expose its error
// until Scan, so we can only record issue time + duration here.
func (d *loggedDB) QueryRow(
	query string, args ...any,
) *sql.Row {
	return d.QueryRowContext(context.Background(), query, args...)
}

// QueryRowContext logs and delegates.
func (d *loggedDB) QueryRowContext(
	ctx context.Context, query string, args ...any,
) *sql.Row {
	start := time.Now()
	row := d.DB.QueryRowContext(ctx, query, args...)
	logStmt("queryrow", query, len(args), nil, time.Since(start))
	return row
}

// Exec logs and delegates.
func (d *loggedDB) Exec(
	query string, args ...any,
) (sql.Result, error) {
	return d.ExecContext(context.Background(), query, args...)
}

// ExecContext logs and delegates. Records rows affected when
// available so write sizes show up in the log.
func (d *loggedDB) ExecContext(
	ctx context.Context, query string, args ...any,
) (sql.Result, error) {
	start := time.Now()
	res, err := d.DB.ExecContext(ctx, query, args...)
	elapsed := time.Since(start)
	rowsAffected := int64(-1)
	if err == nil && res != nil {
		if n, rerr := res.RowsAffected(); rerr == nil {
			rowsAffected = n
		}
	}
	logStmtWith("exec", query, len(args), err, elapsed,
		slog.Int64("rows_affected", rowsAffected),
	)
	return res, err
}

// Begin logs and delegates. The returned *sql.Tx is NOT wrapped —
// queries issued inside the transaction are not individually
// logged. Transaction lifecycle (begin / commit / rollback) is
// logged from Store.withTx, which is the single entry point for
// transactional work.
func (d *loggedDB) Begin() (*sql.Tx, error) {
	return d.DB.Begin()
}

// BeginTx matches the sql.DB signature.
func (d *loggedDB) BeginTx(
	ctx context.Context, opts *sql.TxOptions,
) (*sql.Tx, error) {
	return d.DB.BeginTx(ctx, opts)
}

// logStmt is the common emitter used by Query / Exec / QueryRow.
func logStmt(
	kind, query string, nargs int,
	err error, elapsed time.Duration,
) {
	logStmtWith(kind, query, nargs, err, elapsed)
}

// logStmtWith is the explicit form that lets callers add extra
// structured attributes (used by Exec to report rows_affected).
func logStmtWith(
	kind, query string, nargs int,
	err error, elapsed time.Duration, extra ...slog.Attr,
) {
	stmt := normalizeStmt(query, int(sqlLogMaxChars.Load()))
	ms := elapsed.Milliseconds()
	slowMs := sqlLogSlowMs.Load()
	fullTrace := sqlLogFull.Load()

	// Base attributes that are on every line.
	attrs := []any{
		"kind", kind,
		"stmt", stmt,
		"nargs", nargs,
		"duration_ms", ms,
	}
	for _, a := range extra {
		attrs = append(attrs, a)
	}

	switch {
	case err != nil:
		attrs = append(attrs, "error", err.Error())
		if isBenignMigrationError(err) {
			// Expected during idempotent migrations; don't
			// spam WARN in the per-run log for every startup.
			slog.Debug("sql benign error", attrs...)
		} else {
			slog.Warn("sql error", attrs...)
		}
	case slowMs > 0 && ms >= slowMs:
		slog.Warn("sql slow", attrs...)
	case fullTrace:
		slog.Info("sql", attrs...)
	default:
		// Debug level: only visible when the handler is at
		// Debug (e.g. --verbose). Produces zero allocations
		// when the handler short-circuits on Enabled().
		slog.Debug("sql", attrs...)
	}
}

// isBenignMigrationError returns true for SQLite errors that the
// store layer intentionally tolerates: idempotent ALTER TABLE
// migrations that re-run on every InitSchema (duplicate column),
// and optional FTS5 module missing in builds without the fts5
// tag. Matching on error substrings is pragmatic because those
// messages are stable across go-sqlite3 versions.
func isBenignMigrationError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "duplicate column name") ||
		strings.Contains(msg, "no such module: fts5")
}

// normalizeStmt collapses whitespace in a SQL statement and
// truncates it to maxChars. Truncation is marked with an
// ellipsis so the log consumer can tell. Intended for human log
// reading — not for reconstructing the exact SQL.
func normalizeStmt(q string, maxChars int) string {
	// Fast path: if there's no whitespace to collapse AND the
	// statement is within budget, skip the allocation.
	if len(q) <= maxChars && !strings.ContainsAny(q, "\n\t") {
		return strings.TrimSpace(q)
	}

	var b strings.Builder
	b.Grow(len(q))
	prevSpace := false
	for _, r := range q {
		switch r {
		case ' ', '\t', '\n', '\r':
			if !prevSpace {
				b.WriteByte(' ')
				prevSpace = true
			}
		default:
			b.WriteRune(r)
			prevSpace = false
		}
	}
	s := strings.TrimSpace(b.String())
	if maxChars > 0 && len(s) > maxChars {
		s = s[:maxChars] + "..."
	}
	return s
}
