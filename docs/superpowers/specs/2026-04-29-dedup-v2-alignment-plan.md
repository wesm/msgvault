# Dedup v2 Alignment Plan

**Companion to:** [`2026-04-29-pr-286-design-alignment.md`](./2026-04-29-pr-286-design-alignment.md) (the design) and the issue #278 body draft (the public proposal).

This plan turns the design alignment spec into concrete work for the
`jesse/dedup-v2` branch. It is organized as the implementation slices
named in the design spec, ordered by safety and dependency.

The slices are independently shippable, but slice 1 should land first
because it closes a present-day safety hole.

## What changed since the spec was written

The spec's "Mapping To The Current Branch" section claims `--account`
and `--collection` flags are already on dedup. That is **not accurate**
for the code as it stands:

- `deduplicate` has **only** `--account` — there is no `--collection` flag.
- `cmd/msgvault/cmd/account_scope.go:60` resolves `--account` against
  collections **first**, falling through to sources only on miss. So
  `msgvault deduplicate --account work` silently dedups across every
  source in a collection named `work`, with no warning.
- `cmd/msgvault/cmd/deduplicate.go:105` then feeds those expanded
  source IDs into `dedup.Config.AccountSourceIDs` as one pool, which is
  the cross-source dedup the design says must require explicit opt-in.

This is a semantic regression against the target model, not just a
missing feature. Slice 1 fixes it. The mapping section of the design
spec should be updated to match reality once slice 1 lands.

## Slice 1 — CLI scope correctness

**Goal:** the CLI cannot silently cross source boundaries. Ship as one PR.

- Add `--collection <name>` to `deduplicate`. Mutually exclusive with
  `--account`.
- Make `--account` reject collection names. If the input matches a
  collection, error with: `"work" is a collection, not an account; use --collection work`.
- Make `--collection` reject account identifiers, with the symmetric hint.
- Replace `ResolveAccount` (collection-first ambiguous resolver) with
  two narrow resolvers: `ResolveAccountFlag` and `ResolveCollectionFlag`.
  Keep the `Scope` type internal as an output of either resolver.
- Move `collections` and `collection_sources` from
  `internal/store/collections.go` lazy `CREATE TABLE IF NOT EXISTS`
  into the canonical `internal/store/schema.sql` migration path. Drop
  `ensureCollectionSchema()` once it's owned by the schema.
- Update `list-identities` to take `--account` and `--collection`
  symmetrically with the same rejection rules.
- Tests:
  - `--account <collection-name>` returns the rejection error.
  - `--collection <account-id>` returns the rejection error.
  - `--account` and `--collection` together is rejected.
  - Per-source unscoped dedup unchanged (regression coverage).

**Out of scope for slice 1:** changing dedup engine behavior, adding
`--collection` to other commands, schema for batches/manifests beyond
collections.

## Slice 2 — Live-message contract on retrieval

**Goal:** dedup-hidden rows never appear on a normal read surface.

- Define one named live-message predicate. Two pieces:
  - SQL fragment (`messages.deleted_at IS NULL`-shaped) used by
    `internal/query/sqlite.go` and `internal/query/duckdb.go`.
  - A Go-side filter for in-memory paths (vector hits, MCP responses).
- Audit every read surface and apply it. The known gap from the audit
  is vector/hybrid retrieval (`internal/vector/`), which currently
  does not filter `deleted_at`. Fix that first; it is the one Wes
  named explicitly.
- Audit list (in priority order): vector/hybrid search, MCP responses
  (if a server exists), exports, FTS search, stats command, TUI
  drill-downs, API responses if `serve` exposes any.
- For each surface, add a test that inserts a hidden message and
  asserts it does not appear.
- Document the predicate's location in code with a comment that points
  at the design spec section.

**Out of scope:** rebuilding caches/indexes on dedup. The contract is
filtering, not invalidation.

## Slice 3 — Collections as first-class query scopes

**Goal:** `--collection` works wherever `--account` works.

- Add `--collection` to: `search`, `stats`, `tui` (where it accepts
  `--account` today), and any other scoping flag mentioned in
  `cmd/msgvault/cmd/`.
- Use the slice 1 resolvers — no flag should re-implement the
  collection-vs-account rejection logic.
- Output should make the scope visible: `"Searching collection 'work'
  (3 accounts: ...)"` rather than just listing results.
- Tests: each command, with `--collection All` and a named collection,
  returns results that match the union of member-source results.

## Slice 4 — Identity migration and per-account model

**Goal:** identity is account-scoped at runtime; legacy global config
migrates cleanly.

- On first startup after upgrade, if `config.toml` contains an
  `[identity]` block, copy each address into per-account confirmed
  identity records for every existing account, log a warning naming the
  migration, and print a one-time CLI notice pointing the user at
  `msgvault list-identities`.
- After migration, the `[identity]` block is no longer read.
- Mark the migration as done with a sentinel (e.g., a row in a
  `migrations` table or a `config_meta` flag) so it doesn't repeat.
- Derive collection identity at read time as the union of confirmed
  identities from member accounts. Use it wherever sent-message
  eligibility runs against a collection scope.
- Tests:
  - Upgrade path: existing `[identity]` config + existing accounts
    produces per-account confirmed identities.
  - Migration runs once: subsequent startups don't re-migrate.
  - Collection identity = union of member-account confirmed
    identities.

## Slice 5 — Local hard-delete rung

**Goal:** the third rung of the safety ladder exists as a separate
command.

- Add `msgvault dedup-purge --batch <batch-id>` (name to be
  bikeshedded). Permanently removes rows that a named dedup batch
  hid. Refuses to operate on rows it didn't hide.
- `--all-hidden` variant requires interactive confirmation and an
  explicit listing of how many rows from how many batches will be
  purged.
- Purge is irreversible: undo no longer works for purged rows. The
  command output and the `--undo` help text both say so.
- Purge does not touch remote deletion manifests. Pending manifests
  for purged rows remain valid (they reference Gmail IDs, not local
  rows).
- Tests:
  - Purge a batch, then `--undo <batch-id>` reports rows already
    purged and restores nothing.
  - Purge with no batch flag and no `--all-hidden` errors out.

## Slice 6 — Schema ownership and dedup audit tables

**Goal:** every durable concept in the model lives in canonical schema.

This may merge into slice 1 or 5 depending on size. Listed
separately so it isn't lost.

- Promote dedup batch metadata, hidden-row metadata, and
  remote-deletion manifest references into
  `internal/store/schema.sql` migrations. Today they live across
  ad-hoc columns and lazy creates.
- Add a proper `dedup_batches` audit table with batch id, scope kind
  (account / collection), scope id, started/completed timestamps,
  message counts, and undo state.

## Things deferred to future product work

These are in the design spec's "Future Product Work" section and
should not creep into v2 alignment:

- Import-time dedup with `--into`.
- Automatic dedup when creating or modifying collections.
- Exporting a deduplicated collection into a clean account.
- Identity confirmation UX beyond `list-identities`.
- Policy controls for survivor scoring.

## Suggested PR shape

One PR per slice. Slice 1 is the largest and most user-visible, but
small enough to review as one unit. Slices 2–5 are independent of each
other and can land in parallel reviews.

PR titles should describe the user-visible change, per the global
conventions. Examples:

- `dedup: add --collection flag and reject collections via --account`
- `query: filter dedup-hidden rows from vector/hybrid search`
- `cli: add --collection scope to search and stats`
- `identity: migrate global [identity] config to per-account on upgrade`
- `dedup: add purge command for hidden rows`

## Verification before declaring v2 aligned

Run the design spec's "Scope Review Checklist" against the merged
state. Every question in that checklist should answer cleanly without
qualifications.
