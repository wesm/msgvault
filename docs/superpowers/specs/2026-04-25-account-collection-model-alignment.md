# Account, Collection, and Dedup Model Alignment

**Status:** Supersedes [`2026-04-20-deduplicate-command.md`](./2026-04-20-deduplicate-command.md) for the data-model, scope, and live-message-filtering portions. The earlier spec's product framing (why msgvault needs identities, collections, and dedup) still stands; this document replaces its data-model details and adds the cross-cutting work that PR #286 review surfaced.

**Driving review:** [PR #286 review comment](https://github.com/wesm/msgvault/pull/286#issuecomment-4320039075) from the maintainer.

## Context

PR #286 (`jesse/dedup-v2`) implemented identities, collections, and deduplication, but the maintainer's review identified that the implementation encoded a different data model than msgvault should have. The terminology was inconsistent across the spec, package comments, and CLI; one flag (`--account`) silently resolved to either an account or a collection; soft-delete filtering was applied inconsistently across query surfaces (vector and hybrid search still leaked pruned duplicates); identity was implemented as a global config rather than the per-account model the spec described; and collections lived outside the canonical schema.

Rather than patch each concern in isolation, this spec settles the underlying model and the cross-cutting plumbing that has to follow from it.

## Decisions

Each open question from the review and the brainstorming session, with the chosen answer.

| # | Question | Decision |
|---|---|---|
| 1 | What is an account? | **One ingest source.** One Gmail sync, one mbox import, one IMAP backup, one emlx import — each is its own account. The same mailbox imported twice produces two accounts. |
| 2 | What is a collection? | **A many-to-many grouping of accounts/sources.** Default `All` collection contains every account. |
| 3 | Should `--account` resolve to either an account or a collection? | **No.** Mutually exclusive `--account` and `--collection` flags. No silent reroute. |
| 4 | Where is identity scoped? | **Per-account, stored in the database.** Collection identity is the union of member-account identities. Global `[identity]` config is removed (with a one-time migration). |
| 5 | How is soft-delete filtering applied across query paths? | **Centralized live-message predicate**, applied in SQLite, DuckDB-over-Parquet, FTS5, vector backends, fused search, MCP, HTTP API, TUI, and stats — covering both `deleted_at` (local hide) and `deleted_from_source_at` (remote-deletion record). |
| 6 | Cache/index invalidation policy? | **Filter + auto-rebuild.** Every read path filters; `deduplicate --apply` also rebuilds affected derived surfaces (Parquet years, FTS, vector) best-effort. |
| 7 | Are collections first-class query scopes in this PR? | **Yes**, as a side effect of decision 5 — once the predicate and `SourceIDs` propagate everywhere the reviewer named, scope-by-collection works on every read path. |
| 8 | Where does the collections schema live? | **Canonical schema** (`internal/store/schema.sql` and the dialect-aware migration path). `ensureCollectionSchema` is removed. |
| 9 | What does `--undo` actually restore? | **Hidden rows and pending remote-deletion manifests.** It does **not** revert the survivor's label union, raw-MIME backfill, or already-executed remote deletions. CLI help and success messages say so explicitly. |
| 10 | How is remote deletion handled under collection scopes? | The existing per-source safety rule stands: only stage remote deletion when loser and survivor share `source_id`. Manifests stay source-scoped even when invoked under `--collection`. |
| 11 | What is in this PR vs future work? | This spec separates **shipped now** from **future**; the existing PR description follows the same split. |
| 12 | PR shape? | Stack on `jesse/dedup-v2` (PR #286). Keep the maintainer's review thread intact. |

## Design

### 1. Vocabulary and documentation

- **Account = ingest source.** Every reference in the codebase, CLI help, error messages, and config keys uses this consistently.
- **Collection = many-to-many grouping of accounts.**
- The spec, package comments (notably `internal/dedup/dedup.go`'s "single logical account" line), and any user-facing copy that conflated the two are corrected in the same PR.

### 2. Internal `Scope` type

A new package `internal/scope` introduces an explicit scope type used across all scoped operations:

```go
package scope

type Kind int
const (
    Account Kind = iota
    Collection
    All
)

type Scope struct {
    Kind        Kind
    SourceIDs   []int64
    DisplayName string
}

// Resolve takes the CLI's --account and --collection flag values and the store,
// and returns a Scope. Errors if both flags are non-empty; errors if an --account
// value matches a collection name (with a hint to use --collection); errors if a
// --collection value matches an account identifier.
func Resolve(s *store.Store, accountFlag, collectionFlag string) (Scope, error)
```

`ResolveAccount` (which currently checks collections first, then sources) is removed. Every command that takes a scope (dedup, search, stats, list-identities, future ones) takes `Scope` as its boundary.

### 3. CLI surface

| Command | Flags |
|---|---|
| `msgvault deduplicate --account <id>` | One source. |
| `msgvault deduplicate --collection <name>` | All sources in the collection. |
| `msgvault deduplicate` | Each account independently (catches re-imports). |
| `msgvault search --account <id>` / `--collection <name>` | Same shape. Replaces today's overloaded `--account`. |
| `msgvault stats --account <id>` / `--collection <name>` | Same shape. |
| `msgvault list-identities --account <id>` / `--collection <name>` | Same shape. |
| `msgvault collections create <name> --accounts <id>,<id>,...` | Rejects collection names; only source identifiers/IDs accepted. No nested collections. |

- `--account` and `--collection` are mutually exclusive. Passing both is an error.
- Passing `--account <name>` where `<name>` matches a collection emits: `"<name>" is a collection, not an account; did you mean --collection <name>?`
- TUI account selector surfaces accounts and collections as separate categories rather than blending them.

### 4. Centralized live-message predicate

A new package `internal/livemsg` exposes a single function:

```go
package livemsg

type Dialect int
const (
    SQLite Dialect = iota
    DuckDB
)

// Predicate returns the SQL fragment that selects only "live" messages —
// rows where neither deleted_at (local dedup hide) nor deleted_from_source_at
// (remote deletion recorded) is set. Joined into WHERE clauses across read paths.
func Predicate(dialect Dialect, alias string) string
```

For SQLite with alias `m`: `m.deleted_at IS NULL AND m.deleted_from_source_at IS NULL`. DuckDB-over-Parquet returns the equivalent over the Parquet schema's representation of those columns.

#### Audit and patch

Every read path is audited and updated to use `livemsg.Predicate`. Each path gets a regression test that inserts a soft-deleted row and asserts it is hidden.

- `internal/store` SQLite query paths
- `internal/query` DuckDB-over-Parquet paths
- FTS5 search (predicate joined against the parent `messages` table)
- `internal/vector/sqlitevec/backend.go` live-hit filtering
- `internal/vector/sqlitevec/backend.go` `filteredMessageIDs`
- `internal/vector/sqlitevec/fused.go` fused CTE
- MCP server query paths
- HTTP API query paths
- TUI list and detail paths
- Stats overall and sub-aggregates (label counts, account counts, etc.)

### 5. `SourceIDs` everywhere

Single-`SourceID` paths are replaced with `SourceIDs` throughout user-facing surfaces:

- `local search` and `vector/hybrid search` resolve to `Scope.SourceIDs`, not a single source.
- Stats sub-aggregates that today special-case `SourceID` switch to `SourceIDs`.
- TUI, MCP, and HTTP API paths take `Scope` (or its `SourceIDs`) rather than a single source.

A `--account` invocation produces a one-element `SourceIDs`; `--collection` produces many.

### 6. Per-account identity

#### Schema

```sql
CREATE TABLE account_identities (
    source_id INTEGER NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    address   TEXT NOT NULL,
    kind      TEXT NOT NULL,            -- 'email' | 'phone'
    signal    TEXT,                     -- 'from-header' | 'oauth' | 'config' | 'user'
    confirmed_at TIMESTAMP,
    PRIMARY KEY (source_id, address)
);
```

#### Behavior

- `list-identities` writes candidates to this table with a `signal` value.
- The user confirms candidates (`--confirm` flag, or by editing).
- Dedup reads the union of identity addresses for the scope's `SourceIDs` (one account: that account's identities; collection: union across member accounts).
- `list-identities --collection <name>` displays a per-account breakdown grouped by source, not a flattened union — the user is confirming identities one account at a time.
- The global `[identity] addresses = [...]` config is removed.

#### Migration on first startup after upgrade

If a global `[identity]` block exists in `config.toml`, on first startup msgvault writes its addresses as confirmed identities for **every existing account**, logs a warning naming the migration, and prints a one-time CLI notice asking the user to review per-account identities (`msgvault list-identities`). Global behavior is preserved for the upgrade; the global config block is then no longer read.

### 7. Collections in canonical schema

- `ensureCollectionSchema` is removed.
- Collection tables move into `internal/store/schema.sql` and the dialect-aware migration path (so PostgreSQL parity follows automatically).
- Default `All` collection is seeded by migration.
- `EnsureDefaultCollection` is deleted (or becomes a no-op for older deployments and is removed in a follow-up).
- `collections create --accounts ...` rejects any value that resolves to a collection. Only source identifiers or numeric source IDs are accepted. No nested collections.

### 8. Cache/index invalidation: filter + auto-rebuild

- **Filter (always-on):** every read path uses `livemsg.Predicate`. Soft-deleted rows never surface even if a derived index still has them.
- **Auto-rebuild (best-effort, after `deduplicate --apply`):**
  - **Parquet cache:** incremental rebuild for years where rows were hidden.
  - **FTS5 index:** delete-by-id for hidden rows.
  - **Vector index:** delete-by-id for hidden rows where the backend supports it; otherwise rely on the filter and log that the underlying index still contains the row.
- Rebuild failures are logged, surface in the dedup CLI summary, and do not block the dedup commit. The filter keeps results correct regardless. The user can re-run `build-cache` manually if the auto-rebuild fails.

### 9. Undo language

CLI help, success messages, and the spec all use this exact framing:

> `--undo <batch-id>` restores rows hidden by that dedup batch and cancels the batch's pending remote-deletion manifest where possible. It does **not** revert the survivor's label union or raw-MIME backfill, and it cannot recall a remote deletion that has already been executed against a source server.

### 10. Remote deletion under collection scopes

- The existing safety rule stands: stage remote deletion only when loser and survivor share `source_id`. Cross-source duplicate groups produce no remote-deletion entries even when the scope is a collection.
- Manifests stay source-scoped. Manifest filenames and `Account` labels in reporting reflect the source, never the collection name, even when invoked under `--collection`.
- New tests cover:
  - Collection-scoped dedup with `--delete-dups-from-source-server` produces per-source manifests, never collection-named ones.
  - Cross-source duplicate groups under `--collection` produce no remote-deletion entries.
  - Mixed scope (some same-source pairs, some cross-source pairs) handles each correctly in one run.

### 11. Spec rewrite

The spec narrative is rewritten so:

- "Account" means ingest source consistently throughout.
- "Collection" means a grouping of accounts consistently throughout.
- A clearly-marked **Future work** section contains: `--into` import-time dedup, automatic dedup on collection create/add, collection-to-account export, identity-derived inbound/outbound classification across the corpus.
- The undo language above is canonical.
- The cache/index policy is documented.

### 12. PR shape

- Continue on `jesse/dedup-v2` / PR #286. The maintainer's review thread stays intact.
- Updated PR description leads with what changed in response to review and the boundary between shipped and future.
- Commits are organized to make the model alignment reviewable in isolation from the live-message-predicate plumbing and the per-account identity work. Suggested order:
  1. Vocabulary, `Scope` type, CLI flag split, collections-into-canonical-schema, undo language, remote-deletion tests under collections — the model alignment, isolated.
  2. `internal/livemsg` predicate, audit/patch every read path, regression tests for each path, `SourceIDs` propagation through user-facing surfaces — the cross-cutting plumbing.
  3. Per-account identity schema, `list-identities` writing to it, dedup reading the union, global config migration — the identity rework.

## Out of Scope (Future Work)

These appeared in the original spec but are not implemented in this PR. They are explicitly future work:

- **Import-time dedup (`--into`).** Hash-against-existing during import so duplicates never enter the database.
- **Automatic dedup on collection operations.** `collections create` and `collections add` triggering dedup automatically.
- **Collection export to a clean account.** `collections export <name> --as <new-account>` producing a deduplicated, identity-resolved standalone account.
- **Identity-derived inbound/outbound classification across the corpus.** Backfilling `is_from_me` and sent/received classification for old imports based on confirmed per-account identities.
- **PostgreSQL parity for collections and identities.** The canonical-schema move makes this mechanical, but the actual PostgreSQL test/CI sweep is a follow-up.

## References

- [PR #286](https://github.com/wesm/msgvault/pull/286) — Identities, Collections, and Deduplication
- [PR #286 review comment](https://github.com/wesm/msgvault/pull/286#issuecomment-4320039075) — driving review
- [`2026-04-20-deduplicate-command.md`](./2026-04-20-deduplicate-command.md) — original spec (product framing still applies; data model and scope details superseded by this document)
