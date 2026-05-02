# PostgreSQL Backend Status

This document tracks the state of PostgreSQL backend support in msgvault.

## Summary

PR1 (tag `pr1-dialect-extraction`) extracted all SQLite-specific behavior
behind a `Dialect` interface. Zero functional change; SQLite is still the
default and only production-ready backend.

PR2 (tag `pr2-postgresql-dialect`) adds **foundational scaffolding** for
PostgreSQL support:

- `PostgreSQLDialect` implementing the `Dialect` interface
- `pgx` driver wired into `store.Open()` for `postgres://` URLs
- `schema_pg.sql` with tsvector FTS column and GIN index
- `PostgreSQLEngine` scaffold parallel to `SQLiteEngine`
- Dual-backend test harness via `MSGVAULT_TEST_DB`
- Unit tests for dialect string methods

**PostgreSQL is NOT functionally usable yet.** The work below must complete
before a PostgreSQL connection can successfully insert a single row.

## What Works

- `PostgreSQLDialect.Rebind()` correctly converts `?` → `$1, $2, ...`
  (including quoted-string safety)
- `PostgreSQLDialect.Now()`, `InsertOrIgnore()` (complete + prefix),
  `InsertOrIgnoreSuffix()`, `FTSSearchClause()`, `UpdateOrIgnore()`
- `PostgreSQLDialect` error-code classification (23505, 42701, 42P01)
- `Open("postgres://...")` establishes a connection with pool settings
- `OpenReadOnly` for PostgreSQL enforces `default_transaction_read_only=on`
  via pgx `RuntimeParams` (set on every pooled connection at startup)
- Unit tests for dialect string methods pass without a live Postgres
- SQLite regression: all existing tests pass unmodified

## Follow-Up Work (Required for PostgreSQL to Actually Work)

### Blockers (schema will not load, no row can be inserted)

1. **Schema type translation**: `schema.sql` uses SQLite-specific types
   (`DATETIME`, `BLOB`) and `INTEGER PRIMARY KEY` which is not
   auto-incrementing in PostgreSQL. Options:
   - Create a dedicated `schema_pg.sql` with PostgreSQL-native DDL
     (`TIMESTAMPTZ`, `BYTEA`, `BIGINT GENERATED ALWAYS AS IDENTITY`)
   - Parameterize the shared schema via dialect type mappings
   - Translate at load time

2. **Thread `Rebind()` through all queries**: Most `s.db.Exec` / `QueryRow`
   calls in the store layer still pass raw `?` placeholders. pgx rejects
   these. Affected files: `messages.go`, `sync.go`, `sources.go`,
   `sources_oauthapp.go`, `api.go`. (`inspect.go` already uses `Rebind()`.)

3. **`queryInChunks` / `insertInChunks` bypass the dialect**: These
   helpers in `store.go` hardcode `?` placeholders. They need to accept
   (or be wrapped by) a rebinder.

4. **`LastInsertId()` is not supported by pgx**: Call sites in
   `messages.go` (EnsureConversation, EnsureParticipant, etc.) and
   `sync.go` (StartSync, GetOrCreateSource) must be rewritten to use
   `RETURNING id` (the pattern already exists in `upsertMessageWith`).

5. **Mixed placeholder styles in search**: `api.go:SearchMessages` now
   builds queries with `$1` from `FTSSearchClause` but still appends
   `LIMIT ? OFFSET ?`. Must pick one style consistently.

### Issues (correctness/behavior differences)

6. **`FTSBackfillBatchSQL` INNER vs LEFT JOIN**: PostgreSQL version uses
   inner join on `message_bodies`; SQLite uses LEFT JOIN. Messages with
   no body row are not indexed on PostgreSQL.

7. **`SET statement_timeout` runs on only one pool connection**: Move to
   pgx connection string or `AfterConnect` hook.

8. **(Resolved)** `openPostgresReadOnly` now sets
   `default_transaction_read_only=on` via pgx `RuntimeParams`, so the
   parameter is applied during the startup packet of every pooled
   connection rather than once via `db.Exec("SET …")`. The same pattern
   should be used for `statement_timeout` (item #7).

9. **`GetStats` calls `os.Stat(s.dbPath)`**: For PostgreSQL, `dbPath` is
   a URL, not a file. `DatabaseSize` silently reports 0. Either skip
   or query `pg_database_size(current_database())`.

10. **`PostgreSQLEngine` returns `ErrNotImplemented` for most methods**:
    TUI/MCP/HTTP API will not work against PG. Aggregate, Search,
    SearchFast, GetGmailIDsByFilter, ListMessages all need parameterized
    query builders (currently SQLite-specific: `strftime`, FTS5 MATCH).

11. **FTS weight differences**: PostgreSQL applies `setweight('A')` to
    subject and `'B'` to sender. SQLite FTS5 has no weighting. Ranking
    results will differ between backends.

12. **`PostgreSQLEngine` is not constructed anywhere**: TUI/API/MCP
    still build `SQLiteEngine` unconditionally.

## Running Tests Against PostgreSQL

Once blockers above are resolved:

```bash
# Start a PostgreSQL instance, then:
export MSGVAULT_TEST_DB=postgres://user:pass@localhost:5432/msgvault_test
make test
```

Each test creates and drops its own schema (`msgvault_test_<hex>`) for
isolation. The `testutil.NewTestStore()` helper detects the env var and
routes accordingly. If `MSGVAULT_TEST_DB` is unset, SQLite is used (default).

## Why Ship Scaffolding?

The `Dialect` abstraction + scaffolded PostgreSQL implementation lets the
remaining work proceed incrementally without further disrupting the
SQLite path. The interface design has been validated end-to-end
(SQLiteDialect produces identical SQL; unit tests confirm PostgreSQLDialect
generates valid PostgreSQL SQL). Future PRs can tackle the follow-up work
file-by-file.
