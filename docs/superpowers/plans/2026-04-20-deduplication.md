# Identities, Collections, and Deduplication -- Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the three concepts from the proposal (wesm/msgvault#278): identities tied to accounts, collections of accounts with a default "All" collection, and deduplication at account and collection level.

**Architecture:** Four layers, bottom up:
1. **Store** -- SQL queries for dedup, identities, and collections (SQLite via mattn/go-sqlite3)
2. **Engine** -- Dedup scan/execute/undo logic, survivor selection, content-hash fallback
3. **Query** -- SourceIDs multi-filter and soft-delete exclusion across all query paths, source_filter helper
4. **CLI + TUI** -- deduplicate command, list-identities, collections CRUD, TUI account selector with collections

**Tech Stack:** Go, SQLite, cobra CLI, Bubble Tea TUI, existing deletion staging (`internal/deletion/`)

**Spec:** `docs/superpowers/specs/2026-04-20-deduplicate-command.md`

**Prior art on existing branches:**

| File | Branch | Lines | What it does |
|------|--------|-------|-------------|
| `internal/store/dedup.go` | jesse/dedupe-integration | 521 | FindDuplicatesByRFC822ID, GetDuplicateGroupMessages, MergeDuplicates, UndoDedup, BackfillRFC822IDs, StreamMessageRaw |
| `internal/store/dedup_test.go` | jesse/dedupe-integration | ~150 | Store dedup tests |
| `internal/dedup/dedup.go` | jesse/dedupe-integration | 1154 | Engine: Scan, Execute, Undo, survivor selection, content-hash, deletion staging, FormatReport, FormatMethodology |
| `internal/dedup/dedup_test.go` | jesse/dedupe-integration | 439 | Engine tests |
| `internal/dedup/dedup_identity_test.go` | jesse/dedupe-integration | 238 | Identity address match tests |
| `cmd/msgvault/cmd/deduplicate.go` | jesse/dedupe-integration | 462 | CLI with safety defaults (--delete-dups-from-source-server) |
| `cmd/msgvault/cmd/deduplicate_test.go` | add-deduplicate-command | 499 | CLI tests |
| `cmd/msgvault/cmd/account_scope.go` | jesse/dedupe-integration | 127 | ResolveAccount helper |
| `internal/store/identities.go` | jesse/dedupe-integration | 215 | ListLikelyIdentities with signal bitmask |
| `internal/store/identities_test.go` | jesse/dedupe-integration | 246 | Identity discovery tests |
| `cmd/msgvault/cmd/list_identities.go` | jesse/dedupe-integration | 303 | list-identities command with --toml, --json, --match |
| `internal/store/merged_accounts.go` | jesse/dedupe-integration | 327 | CRUD for merged accounts + sources |
| `internal/store/merged_accounts_test.go` | jesse/dedupe-integration | 217 | Merged account tests |
| `cmd/msgvault/cmd/merge_accounts.go` | jesse/dedupe-integration | 389 | merge-accounts CLI (create/list/show/add/remove/delete) |
| `internal/query/source_filter.go` | jesse/dedupe-integration | 39 | appendSourceFilter helper for SourceIDs |
| `internal/query/models.go` | jesse/dedupe-integration | diff | SourceIDs on MessageFilter, AggregateOptions, StatsOptions, MergedAccountInfo |
| Soft-delete exclusion | jesse/dedupe-integration (bf49689) | diff | deleted_at IS NULL in all query paths |
| TUI merged accounts | jesse/dedupe-integration (5ac2a58) | diff | Account selector, SourceIDs propagation |

**What's already on main:** Schema has `deleted_at`, `delete_batch_id`, `rfc822_message_id`, `is_from_me` on messages table. No dedup/identity/collection code exists. No query paths filter on `deleted_at`.

---

## Phase 1: Deduplication

### Task 1: Store Layer -- Duplicate Finding and Merging

Port `internal/store/dedup.go` and tests from `jesse/dedupe-integration`.

**Files:**
- Create: `internal/store/dedup.go`
- Create: `internal/store/dedup_test.go`
- Reference: `git show jesse/dedupe-integration:internal/store/dedup.go`
- Reference: `git show jesse/dedupe-integration:internal/store/dedup_test.go`

- [ ] **Step 1: Write failing tests for FindDuplicatesByRFC822ID**

Two messages same rfc822_message_id = one group. After MergeDuplicates, group disappears.

Run: `go test ./internal/store/ -run TestStore_FindDuplicatesByRFC822ID -v`

- [ ] **Step 2: Write failing tests for GetDuplicateGroupMessages**

Sent-label detection, is_from_me, from_email extraction, source scoping.

- [ ] **Step 3: Write failing tests for MergeDuplicates**

Label union, raw MIME backfill, soft-delete with batch ID.

- [ ] **Step 4: Implement store/dedup.go**

Types: DuplicateGroupKey, DuplicateMessageRow, MergeResult, ContentHashCandidate. Methods: FindDuplicatesByRFC822ID, GetDuplicateGroupMessages, MergeDuplicates, UndoDedup, CountActiveMessages, CountMessagesWithoutRFC822ID, BackfillRFC822IDs, StreamMessageRaw, GetAllRawMIMECandidates.

Run: `go test ./internal/store/ -run "TestStore_(FindDuplicates|GetDuplicateGroup|MergeDuplicates)" -v`

- [ ] **Step 5: Write and run tests for UndoDedup and CountActiveMessages**

- [ ] **Step 6: Format, vet, commit**

---

### Task 2: Dedup Engine -- Scan, Execute, Undo

Port core engine from `jesse/dedupe-integration:internal/dedup/dedup.go`.

**Files:**
- Create: `internal/dedup/dedup.go`
- Create: `internal/dedup/dedup_test.go`
- Reference: `git show jesse/dedupe-integration:internal/dedup/dedup.go`
- Reference: `git show jesse/dedupe-integration:internal/dedup/dedup_test.go`

- [ ] **Step 1: Write failing tests for Scan (RFC822 grouping) and survivor selection**

Gmail preferred over mbox. Raw MIME tiebreaker. Sent-copy constraint. Three-signal IsSentCopy.

- [ ] **Step 2: Implement Config, types, NewEngine, Scan, selectSurvivor, isBetter**

Include three-signal IsSentCopy (SENT label, is_from_me, identity address match). Skip content-hash for now.

- [ ] **Step 3: Write failing tests for Execute and Undo**

Execute soft-deletes, transfers labels, backfills MIME. Undo restores.

- [ ] **Step 4: Implement Execute, Undo, deletion staging (same-source_id guard)**

- [ ] **Step 5: Implement FormatReport and FormatMethodology**

- [ ] **Step 6: Format, vet, commit**

---

### Task 3: Dedup Engine -- Content-Hash Fallback

Port normalized content-hash scanning for header-rewritten duplicates.

**Files:**
- Modify: `internal/dedup/dedup.go`
- Modify: `internal/dedup/dedup_test.go`

- [ ] **Step 1: Write failing test for content-hash detection**

Two messages different rfc822_message_id, identical normalized body. ContentHashFallback=true finds them.

- [ ] **Step 2: Implement scanNormalizedHashGroups, normalizeRawMIME, worker pool**

- [ ] **Step 3: Format, vet, commit**

---

### Task 4: Soft-Delete Exclusion Across All Query Paths

Add `deleted_at IS NULL` to every query path. Port from commit `bf49689`.

**Files:**
- Modify: `internal/query/sqlite.go` -- optsToFilterConditions, buildFilterJoinsAndConditions, buildSearchQueryParts, GetTotalStats, GetGmailIDsByFilter
- Modify: `internal/query/duckdb.go` -- Search fallback
- Modify: `cmd/msgvault/cmd/build_cache.go` -- messages export query

- [ ] **Step 1: Add `deleted_at IS NULL` to all SQLite query builders**

One-line addition in each: `conditions = append(conditions, prefix+"deleted_at IS NULL")`

- [ ] **Step 2: Add to DuckDB search fallback**

- [ ] **Step 3: Add `AND deleted_at IS NULL` to Parquet export query**

- [ ] **Step 4: Run full test suite** -- `go test ./... -count=1`

- [ ] **Step 5: Format, vet, commit**

---

### Task 5: Account Resolution Helper and Deduplicate Command

Port CLI from `jesse/dedupe-integration`.

**Files:**
- Create: `cmd/msgvault/cmd/account_scope.go`
- Create: `cmd/msgvault/cmd/deduplicate.go`
- Create: `cmd/msgvault/cmd/deduplicate_test.go`
- Reference: `git show jesse/dedupe-integration:cmd/msgvault/cmd/account_scope.go`
- Reference: `git show jesse/dedupe-integration:cmd/msgvault/cmd/deduplicate.go`
- Reference: `git show add-deduplicate-command:cmd/msgvault/cmd/deduplicate_test.go`

- [ ] **Step 1: Implement AccountScope and ResolveAccount**

Skip merged-account lookup for now (stub returns ErrMergedAccountNotFound). Structure so collection lookup slots in later.

- [ ] **Step 2: Implement deduplicate command**

Flags: --account, --dry-run, --prefer, --content-hash, --delete-dups-from-source-server, --no-backup, --undo, --yes. Two modes: per-source (default) and scoped (--account).

- [ ] **Step 3: Port CLI tests, adapt flag names**

- [ ] **Step 4: Build and smoke test** -- `make build && ./msgvault deduplicate --help`

- [ ] **Step 5: Format, vet, commit**

---

## Phase 2: Identities

### Task 6: Identity Discovery Store Layer

Port `internal/store/identities.go` from `jesse/dedupe-integration`.

**Files:**
- Create: `internal/store/identities.go`
- Create: `internal/store/identities_test.go`
- Reference: `git show jesse/dedupe-integration:internal/store/identities.go`
- Reference: `git show jesse/dedupe-integration:internal/store/identities_test.go`

- [ ] **Step 1: Write failing tests for ListLikelyIdentities**

Test each signal in isolation (is_from_me alone, SENT label without is_from_me, account-identifier match). Negative case: stranger address excluded. Count ordering. Source scoping. Soft-delete exclusion.

- [ ] **Step 2: Implement IdentityCandidate, IdentitySignal, ListLikelyIdentities**

- [ ] **Step 3: Format, vet, commit**

---

### Task 7: list-identities Command

Port `cmd/msgvault/cmd/list_identities.go` from `jesse/dedupe-integration`.

**Files:**
- Create: `cmd/msgvault/cmd/list_identities.go`
- Reference: `git show jesse/dedupe-integration:cmd/msgvault/cmd/list_identities.go`

- [ ] **Step 1: Implement list-identities command**

Flags: --account, --min-count, --match (regex), --json, --toml. Human table output: ADDRESS, MESSAGES, SOURCES, SIGNALS. --toml output produces paste-ready `[identity]` config block.

- [ ] **Step 2: Build and smoke test** -- `make build && ./msgvault list-identities --help`

- [ ] **Step 3: Format, vet, commit**

---

### Task 8: Identity Config Integration

Port `[identity].addresses` config from `jesse/dedupe-integration`.

**Files:**
- Modify: `internal/config/config.go` -- Add IdentityConfig struct, IdentityAddressSet() method
- Modify: `cmd/msgvault/cmd/deduplicate.go` -- Load identity addresses, pass to engine
- Reference: `git show jesse/dedupe-integration:internal/config/config.go`

- [ ] **Step 1: Add IdentityConfig to config.go**

New `[identity]` section with `addresses []string`. `IdentityAddressSet()` returns normalized `map[string]bool`.

- [ ] **Step 2: Wire into deduplicate command**

Load `cfg.IdentityAddressSet()`, pass to `dedup.Config.IdentityAddresses`. Log count when loaded.

- [ ] **Step 3: Port identity-match dedup tests**

Reference: `git show jesse/dedupe-integration:internal/dedup/dedup_identity_test.go`

- [ ] **Step 4: Format, vet, commit**

---

## Phase 3: Collections

### Task 9: Collections Store Layer

Port and rename from `jesse/dedupe-integration:internal/store/merged_accounts.go`. Rename tables from `merged_accounts` to `collections` and `merged_account_sources` to `collection_sources`. Add `collection_identities` table.

**Files:**
- Create: `internal/store/collections.go`
- Create: `internal/store/collections_test.go`
- Reference: `git show jesse/dedupe-integration:internal/store/merged_accounts.go`
- Reference: `git show jesse/dedupe-integration:internal/store/merged_accounts_test.go`

- [ ] **Step 1: Write failing tests for CRUD**

Create, list, show (with source IDs + message count), add sources, remove sources, delete. Rejection of missing sources and duplicate names. Idempotent add/remove. Deletion leaves sources and messages intact.

- [ ] **Step 2: Implement collections store**

Schema: `collections` (id, name, description, created_at), `collection_sources` (collection_id, source_id), `collection_identities` (collection_id, address). Methods: CreateCollection, ListCollections, GetCollectionByName, AddSourcesToCollection, RemoveSourcesFromCollection, DeleteCollection, GetCollectionIdentities, SetCollectionIdentities.

- [ ] **Step 3: Implement default "All" collection**

`EnsureDefaultCollection()` creates "All" if it doesn't exist and adds all sources. Called during InitSchema. New sources auto-join "All" on creation (hook in source creation path).

- [ ] **Step 4: Format, vet, commit**

---

### Task 10: Query Layer -- SourceIDs Multi-Filter

Port source_filter helper and SourceIDs field from `jesse/dedupe-integration`.

**Files:**
- Create: `internal/query/source_filter.go`
- Modify: `internal/query/models.go` -- Add SourceIDs to MessageFilter, AggregateOptions, StatsOptions. Add CollectionInfo type (renamed from MergedAccountInfo).
- Modify: `internal/query/sqlite.go` -- Use appendSourceFilter everywhere SourceID is checked
- Modify: `internal/query/duckdb.go` -- Same
- Reference: `git show jesse/dedupe-integration:internal/query/source_filter.go`
- Reference: `git diff jesse/active-work..jesse/dedupe-integration -- internal/query/models.go`

- [ ] **Step 1: Create source_filter.go with appendSourceFilter helper**

- [ ] **Step 2: Add SourceIDs to MessageFilter, AggregateOptions, StatsOptions**

Add CollectionInfo type. Update MessageFilter.Clone() to deep-copy SourceIDs.

- [ ] **Step 3: Update SQLite and DuckDB engines to use appendSourceFilter**

Replace every manual SourceID check with the shared helper.

- [ ] **Step 4: Add ListCollections to Engine interface**

Returns collection rows. DuckDB delegates to embedded SQLite. Remote returns empty.

- [ ] **Step 5: Run full test suite**

- [ ] **Step 6: Format, vet, commit**

---

### Task 11: Collections CLI

Adapt from `jesse/dedupe-integration:cmd/msgvault/cmd/merge_accounts.go`, renamed to `collections`.

**Files:**
- Create: `cmd/msgvault/cmd/collections.go`
- Modify: `cmd/msgvault/cmd/account_scope.go` -- Add collection lookup to ResolveAccount
- Reference: `git show jesse/dedupe-integration:cmd/msgvault/cmd/merge_accounts.go`

- [ ] **Step 1: Implement collections command**

Subcommands: create, list, show, add, remove, delete. `create` runs identity discovery + confirmation + dedup. `add` triggers dedup of new sources against existing set.

- [ ] **Step 2: Update ResolveAccount to check collections**

Collections take precedence over source identifiers (same pattern as merged accounts). Update error messages to point at `collections list`.

- [ ] **Step 3: Update deduplicate --account to resolve collections**

`--account` now resolves to collections too. Add `--collection` as an explicit alias.

- [ ] **Step 4: Build and smoke test**

- [ ] **Step 5: Format, vet, commit**

---

### Task 12: Collections Export to Account

**Files:**
- Modify: `cmd/msgvault/cmd/collections.go` -- Add `export` subcommand
- Modify: `internal/store/collections.go` -- Add ExportCollectionToAccount method

- [ ] **Step 1: Implement ExportCollectionToAccount in store**

Create a new source from the deduplicated, identity-resolved contents of a collection. Copy survivor messages with unified labels. New source gets source_type "collection_export".

- [ ] **Step 2: Add `collections export <name> --as <account-name>` subcommand**

- [ ] **Step 3: Format, vet, commit**

---

### Task 13: TUI -- Collections in Account Selector

Port TUI integration from `jesse/dedupe-integration` commit `5ac2a58`, renamed from merged accounts to collections.

**Files:**
- Modify: `internal/tui/model.go` -- Add collectionAccounts field, loadAccounts fetches collections, account selector shows collections, SourceIDs propagation
- Modify: `internal/tui/keys.go` -- Collection selection in account modal
- Modify: `internal/tui/view.go` -- Render collections section in selector, header bar shows collection name
- Create: `internal/tui/nav_modal_test.go` -- Test collection selection and SourceIDs propagation
- Reference: `git diff jesse/active-work..jesse/dedupe-integration -- internal/tui/`

- [ ] **Step 1: Add collections to model state**

`collections []query.CollectionInfo`, `accountFilterSourceIDs []int64`, `accountFilterLabel string`. `accountFilterSourceIDsCopy()` for defensive copies.

- [ ] **Step 2: Update loadAccounts to fetch collections**

Best-effort: failure to load collections downgrades silently.

- [ ] **Step 3: Update account selector modal**

Second section below individual accounts. Cursor math, selection, round-trip restoration.

- [ ] **Step 4: Propagate SourceIDs through all query builders**

loadData, loadStats, buildMessageFilter all pass SourceIDs from model.

- [ ] **Step 5: Port tests**

- [ ] **Step 6: Format, vet, commit**

---

## Phase 4: Verification

### Task 14: Full Build and Integration

- [ ] **Step 1: Run full test suite** -- `go test ./... -count=1`

- [ ] **Step 2: Run linter** -- `make lint`

- [ ] **Step 3: Build** -- `make build`

- [ ] **Step 4: Smoke test CLI**

```
./msgvault deduplicate --help
./msgvault deduplicate --dry-run
./msgvault list-identities --help
./msgvault collections --help
./msgvault collections list
```

- [ ] **Step 5: Final commit if needed**
