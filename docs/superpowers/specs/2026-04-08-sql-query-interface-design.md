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

Expose a read-only SQL query interface over DuckDB views that encapsulate
the Parquet file layout. Two access methods: a CLI subcommand for local
use and an HTTP endpoint for remote/daemon deployments.

### View Layer

DuckDB views registered at engine startup, replacing the per-query CTE
pattern currently in `internal/query/duckdb.go`.

**Base views** (stable names over Parquet files):

| View | Source | Notes |
|------|--------|-------|
| `messages` | `analytics/messages/year=*/*.parquet` | Hive-partitioned, `union_by_name=true` |
| `participants` | `analytics/participants.parquet` | Email addresses + domains |
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
| `v_messages` | Messages with sender email, name, domain joined from `message_recipients` + `participants`. Includes label list as array. |
| `v_senders` | Per-sender aggregates: message count, total size, attachment count, first/last message date. |
| `v_domains` | Per-domain aggregates: message count, total size, sender count. |
| `v_threads` | Messages grouped by conversation with participant email list. |
| `v_labels` | Label name + message count + total size. |

Convenience views are defined as SQL over the base views, not over raw
Parquet paths. If a base view changes, convenience views adapt
automatically.

### CLI Command

```
msgvault query "SELECT * FROM v_senders ORDER BY message_count DESC LIMIT 10"
msgvault query --format csv "SELECT * FROM v_messages WHERE from_domain = 'example.com'"
msgvault query --format table "SELECT * FROM v_labels ORDER BY message_count DESC"
```

**Flags:**
- `--format json|csv|table` (default: `json`)

**Behavior:**
- Opens DuckDB in-process with the view layer registered
- Executes the SQL query read-only (Parquet files are inherently read-only)
- Outputs results to stdout
- Exits with non-zero status on SQL errors
- Requires analytics cache to exist (errors with guidance to run
  `build-cache` if missing)

**Implementation:** New file `cmd/msgvault/cmd/query.go`. Opens a DuckDB
connection, calls `RegisterViews(db, analyticsDir)` to create the view
layer, then executes the user's SQL via `database/sql.Query`. This is
independent of `DuckDBEngine` -- it only needs the view registration
function and a raw `*sql.DB`.

### HTTP Endpoint

```
POST /api/v1/query
Content-Type: application/json
Authorization: Bearer <api-key>

{"sql": "SELECT * FROM v_senders ORDER BY message_count DESC LIMIT 10"}
```

**Response:**
```json
{
  "columns": ["from_email", "message_count", "total_size", ...],
  "rows": [
    ["alice@example.com", 1234, 5678901, ...],
    ...
  ],
  "row_count": 10
}
```

**Behavior:**
- Read-only by construction (DuckDB over Parquet)
- Uses the same view layer as the CLI command
- Returns columnar JSON (column names + row arrays) for efficiency
- SQL errors return 400 with the DuckDB error message
- Requires authentication (existing API key mechanism)

**Implementation:** New handler `handleQuery` in
`internal/api/handlers.go`, registered on the existing chi router in
`internal/api/server.go`.

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

## Implementation Scope

### New files
- `internal/query/views.go` -- view DDL definitions + registration
- `cmd/msgvault/cmd/query.go` -- CLI command
- `skills/claude-code/SKILL.md` -- skill definition
- `skills/claude-code/references/views.md` -- view schema reference

### Modified files
- `internal/query/duckdb.go` -- call view registration at startup
- `internal/api/server.go` -- register query endpoint
- `internal/api/handlers.go` -- add `handleQuery`
- `cmd/msgvault/cmd/root.go` -- register query subcommand

### Tests
- `internal/query/views_test.go` -- verify views create successfully
  and return expected columns against test Parquet fixtures
- `cmd/msgvault/cmd/query_test.go` -- CLI integration test
- `internal/api/handlers_test.go` -- HTTP query endpoint test
