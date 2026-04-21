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
//
// loggedDB also owns the dialect's placeholder-rebind step: every
// SQL string passed to Query/Exec/QueryRow is run through rebind
// before reaching the driver. Call sites in the store package can
// emit portable `?` placeholders and get the correct `$N` form on
// PostgreSQL without any per-call wrapping.
type loggedDB struct {
	*sql.DB
	rebind func(string) string
}

func newLoggedDB(db *sql.DB, rebind func(string) string) *loggedDB {
	if rebind == nil {
		rebind = identityRebind
	}
	return &loggedDB{DB: db, rebind: rebind}
}

func identityRebind(q string) string { return q }

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
	query = d.rebind(query)
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
	query = d.rebind(query)
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
	query = d.rebind(query)
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

// Begin returns a *loggedTx that inherits the rebind function, so
// statements issued inside the transaction are also rebound before
// reaching the driver. Wrapping Begin (not just Exec/Query) is what
// keeps the auto-rebind promise intact across transactional code.
func (d *loggedDB) Begin() (*loggedTx, error) {
	tx, err := d.DB.Begin()
	if err != nil {
		return nil, err
	}
	return &loggedTx{Tx: tx, rebind: d.rebind}, nil
}

// BeginTx matches the sql.DB signature but returns *loggedTx.
func (d *loggedDB) BeginTx(
	ctx context.Context, opts *sql.TxOptions,
) (*loggedTx, error) {
	tx, err := d.DB.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &loggedTx{Tx: tx, rebind: d.rebind}, nil
}

// loggedTx mirrors loggedDB for *sql.Tx: it embeds the raw
// transaction and rebinds every query before dispatching. Store
// code that previously took *sql.Tx takes *loggedTx instead.
type loggedTx struct {
	*sql.Tx
	rebind func(string) string
}

// Exec rebinds before delegating. Transaction-scoped queries are
// not individually logged — the per-tx duration from Store.withTx
// gives enough signal.
func (t *loggedTx) Exec(
	query string, args ...any,
) (sql.Result, error) {
	return t.Tx.Exec(t.rebind(query), args...)
}

// ExecContext rebinds before delegating.
func (t *loggedTx) ExecContext(
	ctx context.Context, query string, args ...any,
) (sql.Result, error) {
	return t.Tx.ExecContext(ctx, t.rebind(query), args...)
}

// Query rebinds before delegating.
func (t *loggedTx) Query(
	query string, args ...any,
) (*sql.Rows, error) {
	return t.Tx.Query(t.rebind(query), args...)
}

// QueryContext rebinds before delegating.
func (t *loggedTx) QueryContext(
	ctx context.Context, query string, args ...any,
) (*sql.Rows, error) {
	return t.Tx.QueryContext(ctx, t.rebind(query), args...)
}

// QueryRow rebinds before delegating.
func (t *loggedTx) QueryRow(
	query string, args ...any,
) *sql.Row {
	return t.Tx.QueryRow(t.rebind(query), args...)
}

// QueryRowContext rebinds before delegating.
func (t *loggedTx) QueryRowContext(
	ctx context.Context, query string, args ...any,
) *sql.Row {
	return t.Tx.QueryRowContext(ctx, t.rebind(query), args...)
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
