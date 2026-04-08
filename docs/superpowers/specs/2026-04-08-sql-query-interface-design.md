# SQL Analytical Query Interface

## Problem

External consumers (Claude Code skills, web UIs, scripts) need to query
msgvault's email archive analytics. The current PR approach shells out to
the `duckdb` CLI binary against raw Parquet files, which:

- Duplicates DuckDB infrastructure msgvault already embeds
- Exposes internal Parquet layout (hive partitioning, file paths, table
  relationships) to consumers
- Creates SQL injection surface area in bash wrapper scripts
- Only serves Claude Code skills, not other consumers

## Design

Expose a SQL query interface over DuckDB views that encapsulate the
Parquet file layout. V1 is CLI-only (`msgvault query`). The view layer
is also reusable internally by `DuckDBEngine`.

### View Layer

DuckDB views registered on a single connection, replacing the per-query
CTE pattern currently in `internal/query/duckdb.go`.

**Base views** (stable names over Parquet files):

| View | Source | Notes |
|------|--------|-------|
| `messages` | `analytics/messages/year=*/*.parquet` | Hive-partitioned, `union_by_name=true` |
| `participants` | `analytics/participants.parquet` | Email addresses, domains, phone numbers |
| `message_recipients` | `analytics/message_recipients.parquet` | from/to/cc/bcc mappings |
| `labels` | `analytics/labels.parquet` | Gmail label definitions |
| `message_labels` | `analytics/message_labels.parquet` | Message-label associations |
| `attachments` | `analytics/attachments.parquet` | Attachment metadata |
| `conversations` | `analytics/conversations.parquet` | Thread grouping |
| `sources` | `analytics/sources.parquet` | Account metadata |

Base views handle column type casting and optional column fallbacks
(the `hasCol` / `COALESCE` pattern currently in `baseCTEs()`). This
means consumers always see a stable schema regardless of cache age.

**Convenience views** (pre-joined for common queries):

| View | Definition |
|------|-----------|
| `v_messages` | Messages with sender resolved via the existing dual-path logic: `message_recipients` (email sources) OR `messages.sender_id` (chat sources), with `phone_number` included. Includes label list as `list()` aggregate. |
| `v_senders` | Per-sender aggregates: message count, total size, attachment count, first/last message date. Uses the same sender resolution as `v_messages`. |
| `v_domains` | Per-domain aggregates: message count, total size, sender count. |
| `v_threads` | Messages grouped by conversation with participant email list. |
| `v_labels` | Label name + message count + total size. |

The sender resolution in `v_messages` and `v_senders` must replicate
the logic in `DuckDBEngine` (`duckdb.go:852`): check both
`message_recipients` (where `recipient_type = 'from'`) and
`messages.sender_id` joined to `participants`, including
`participants.phone_number` for chat/WhatsApp data.

Convenience views are defined as SQL over the base views, not over raw
Parquet paths. If a base view changes, convenience views adapt
automatically.

### Connection Model

DuckDB requires session state (SET, CREATE VIEW, ATTACH) to be pinned
to a single connection. The existing engine enforces this via
`db.SetMaxOpenConns(1)` (`duckdb.go:93`).

`RegisterViews(db *sql.DB, analyticsDir string) error` is a standalone
function that:

1. Probes Parquet schemas for optional columns (reusing the existing
   `probeParquetColumns` logic)
2. Creates base views with appropriate type casts and column fallbacks
3. Creates convenience views over the base views

Callers must ensure the `*sql.DB` is constrained to a single connection
before calling `RegisterViews`. Both the CLI command and `DuckDBEngine`
satisfy this requirement.

### CLI Command

```
msgvault query "SELECT * FROM v_senders ORDER BY message_count DESC LIMIT 10"
msgvault query --format csv "SELECT * FROM v_messages WHERE from_domain = 'example.com'"
msgvault query --format table "SELECT * FROM v_labels ORDER BY message_count DESC"
```

**Flags:**
- `--format json|csv|table` (default: `json`)

**Behavior:**
- Opens a single-connection DuckDB instance, calls `RegisterViews`
- Executes the user's SQL
- Outputs results to stdout in the requested format
- Exits with non-zero status on SQL errors
- Requires analytics cache to exist (errors with guidance to run
  `build-cache` if missing)

**Implementation:** New file `cmd/msgvault/cmd/query.go`. Opens a DuckDB
`*sql.DB` with `SetMaxOpenConns(1)`, calls `RegisterViews`, executes the
SQL via `db.Query`, serializes results. The CLI is a local-only tool
running in the user's process with no network exposure.

### Claude Code Skill

Thin skill that teaches Claude to use `msgvault query`:

- View schema reference (view names, column names and types, what each
  view represents)
- Example queries for common analytical tasks (top senders, domain
  breakdown, thread analysis, label distribution, time series)
- Pointers to existing CLI commands for non-analytical operations (sync,
  search, deletion, export)

No bash wrapper scripts. No Parquet path knowledge. The skill file and
a reference doc with view schemas and example queries.

### What This Does NOT Change

- **MCP tools** stay as-is (structured interface, separate from raw SQL)
- **Existing API endpoints** unchanged (aggregates, search, stats)
- **TUI** continues using the existing `Engine` interface
- **Internal query engine** (`DuckDBEngine`) keeps its structured methods;
  the view layer is an addition, not a replacement. Over time the
  internal CTE-based queries can migrate to use the same views, but
  that's a separate concern.

### Future: HTTP Endpoint

A remote SQL endpoint (`POST /api/v1/query`) is not in V1 scope.
Exposing arbitrary DuckDB SQL over HTTP requires:

- **Statement validation**: DuckDB supports INSTALL, LOAD, ATTACH,
  CREATE, and other DDL that is not constrained by Parquet read-only
  semantics. A network-facing endpoint needs an allowlist of permitted
  statement types (SELECT only) or DuckDB's access mode restrictions.
- **Resource limits**: query timeout, result size cap, concurrent query
  limits.
- **Fallback behavior**: `serve` falls back to SQLite when the Parquet
  cache is stale/incomplete (`serve.go:89`). The query endpoint needs to
  either return 503 or define a SQLite-backed fallback for that state.

These constraints are worth getting right rather than shipping with
"read-only by construction" hand-waving.

## Implementation Scope

### New files
- `internal/query/views.go` -- view DDL definitions + `RegisterViews`
- `cmd/msgvault/cmd/query.go` -- CLI command
- `skills/claude-code/SKILL.md` -- skill definition
- `skills/claude-code/references/views.md` -- view schema reference

### Modified files
- `internal/query/duckdb.go` -- call `RegisterViews` at startup,
  optionally migrate internal queries to use views
- `cmd/msgvault/cmd/root.go` -- register query subcommand

### Tests
- `internal/query/views_test.go` -- verify views create successfully
  and return expected columns against test Parquet fixtures
- `cmd/msgvault/cmd/query_test.go` -- CLI integration test
