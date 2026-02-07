# devdata: Dataset Management Tool — PRD

## Overview

`devdata` is a standalone Go CLI tool that manages multiple msgvault data directories, allowing developers to switch between datasets by symlinking `~/.msgvault` to named directories (`~/.msgvault-<name>`). It also creates expendable subsets of production data for development and testing, copying a configurable number of messages with all referentially-linked data into a new dataset.

## Goals

- **Switch datasets**: Point `~/.msgvault` at any named dataset directory via symlink.
- **Initialize dev mode**: Safely move a real `~/.msgvault` directory to `~/.msgvault-gold` and replace it with a symlink, so the original data is preserved and msgvault continues to work transparently.
- **Exit dev mode**: Reverse the init process, restoring the original `~/.msgvault` directory.
- **Create dev subsets**: Copy N messages (and all related rows) from a source dataset into a new dataset, producing a small, self-contained database for development.

## Non-Goals

- Modifying msgvault's own code or config resolution. The tool operates entirely through filesystem symlinks.
- Syncing data between datasets. Each dataset is independent after creation.
- Managing OAuth tokens or credentials. `new-data` copies config.toml but not tokens/ (the dev dataset won't authenticate to Gmail, which is the desired behavior for expendable copies).
- Attachment file copying. Only the SQLite database and config are copied; attachment blobs in `attachments/` are not duplicated. The `attachments` table rows are copied with their `storage_path` values intact, meaning code that tries to read attachment files will get file-not-found errors. This is acceptable for dev datasets — the metadata is present for query/display testing, but actual file access is not expected.

## Technical Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Symlink API | Go `os.Symlink()` / `os.Readlink()` / `os.Lstat()` | Cross-platform. Works on macOS/Linux natively. On Windows requires Developer Mode or admin, which is acceptable for a dev tool. |
| CLI framework | `spf13/cobra` | Matches msgvault's existing CLI. |
| Database copying | SQLite `ATTACH DATABASE` + `INSERT INTO ... SELECT` | Copies data in a single transaction with referential integrity. No need for row-by-row iteration. |
| SQLite driver | `mattn/go-sqlite3` (CGO) | Matches msgvault. Required for FTS5 tag. |
| Build location | `tools/devdata/` | Separate binary, not a msgvault subcommand. Dev tooling lives in `tools/`. (Note: this establishes a new `tools/` convention for Go-based dev tools; the existing `scripts/mimeshootout/` uses `scripts/` for shell-based tooling.) |
| Module | Same `github.com/wesm/msgvault` module | Shares `go.mod` with msgvault. Can import `internal/store` for schema embedding. |
| Row selection | Most recent N messages by `sent_at DESC` | Most useful for development — recent data reflects current schema and patterns. |

## Design and Operation

### Directory Layout

```
~/.msgvault                  # Symlink (when in dev mode) or real directory
~/.msgvault-gold             # Original data (created by init-dev-data)
~/.msgvault-<name>           # Named datasets (created by new-data or manually)
```

### Commands

#### `devdata mount-data --dataset <name>`

Points `~/.msgvault` at `~/.msgvault-<name>`.

**Preconditions:**
- `~/.msgvault` must be a symlink (not a real directory). If it's a real directory, the user must run `init-dev-data` first.
- `~/.msgvault-<name>` must exist.

**Steps:**
1. Verify `~/.msgvault` is a symlink.
2. Verify `~/.msgvault-<name>` exists and contains `msgvault.db`.
3. Remove the existing symlink.
4. Create symlink: `~/.msgvault` → `~/.msgvault-<name>`.
5. Print confirmation with the dataset now active.

**Error handling:**
- If `~/.msgvault` is a real directory: error with suggestion to run `init-dev-data`.
- If target dataset doesn't exist: error listing available datasets.
- If target has no `msgvault.db`: warning (may be an empty/new dataset).

#### `devdata init-dev-data`

Transitions from a single `~/.msgvault` directory to the symlink-based system.

**Preconditions:**
- `~/.msgvault` must be a real directory (not already a symlink).
- `~/.msgvault-gold` must not already exist.

**Steps:**
1. Verify `~/.msgvault` is a real directory.
2. Verify `~/.msgvault-gold` does not exist.
3. Rename `~/.msgvault` → `~/.msgvault-gold`.
4. Create symlink: `~/.msgvault` → `~/.msgvault-gold`.
5. Print confirmation.

**Error handling:**
- If `~/.msgvault` is already a symlink: print current target and exit cleanly (already initialized).
- If `~/.msgvault-gold` exists: error — user must resolve manually.
- If `~/.msgvault` doesn't exist: error.

#### `devdata exit-dev-data`

Undoes `init-dev-data`, restoring the original directory layout.

**Preconditions:**
- `~/.msgvault` must be a symlink.
- `~/.msgvault-gold` must exist.

**Steps:**
1. Verify `~/.msgvault` is a symlink.
2. Verify `~/.msgvault-gold` exists.
3. Remove the symlink `~/.msgvault`.
4. Rename `~/.msgvault-gold` → `~/.msgvault`.
5. Print confirmation.

**Note:** This command always restores `~/.msgvault-gold` regardless of what the symlink currently points to. If the user has mounted a different dataset (e.g., `~/.msgvault-dev`), that dataset is not deleted — it simply becomes unmounted. The user should run `mount-data --dataset gold` first if they want to verify the current mount before exiting dev mode.

**Error handling:**
- If `~/.msgvault` is a real directory: print message that dev mode is not active.
- If `~/.msgvault-gold` doesn't exist: error — cannot restore without gold copy.

#### `devdata new-data --dst <name> [--src <name>] --rows <count>`

Creates a new dataset by copying N messages from a source dataset.

**Arguments:**
- `--dst <name>` (required): Name for the new dataset. Creates `~/.msgvault-<name>`.
- `--src <name>` (optional): Source dataset name. If omitted, reads from whatever `~/.msgvault` currently points to (the active dataset).
- `--rows <count>` (required): Number of messages to copy.

**Steps:**
1. Resolve source path: `~/.msgvault-<src>` if `--src` provided, otherwise resolve `~/.msgvault` (following symlinks).
2. Verify source `msgvault.db` exists.
3. Verify `~/.msgvault-<dst>` does not exist.
4. Create `~/.msgvault-<dst>/`.
5. Copy `config.toml` from source if it exists.
6. Open source database (read-only).
7. Create destination database, initialize schema.
8. Attach source database to destination connection.
9. Copy data in dependency order (single transaction):
   a. `sources` — all rows (small table)
   b. `participants` — all referenced by selected messages
   c. `participant_identifiers` — for copied participants
   d. `conversations` — referenced by selected messages
   e. `conversation_participants` — for copied conversations
   f. `messages` — top N by `sent_at DESC`
   g. `message_bodies` — for copied messages
   h. `message_raw` — for copied messages
   i. `message_recipients` — for copied messages
   j. `reactions` — for copied messages
   k. `attachments` — for copied messages
   l. `labels` — all for copied sources
   m. `message_labels` — for copied messages, matching copied labels
   n. `sync_runs` — skip (not needed for dev)
   o. `sync_checkpoints` — skip (not needed for dev)
10. Update denormalized counts on conversations.
11. Rebuild FTS5 index if available (using the same query structure as `store.backfillFTSBatch`: joins `messages` with `message_bodies` for body text, and resolves `from_addr`/`to_addr`/`cc_addr` via correlated subqueries on `message_recipients` + `participants`).
12. Print summary: messages copied, database size.

**Note:** The `analytics/` Parquet cache directory is intentionally not copied. The TUI auto-builds this cache on launch when new messages are detected, so running `./msgvault tui` against a new dev dataset will trigger a cache build automatically.

**Data selection strategy:**
- Select the N most recent messages by `sent_at DESC`.
- Copy all referentially required rows (participants, conversations, labels, etc.) — only those actually referenced, not the full tables.
- `sources` is always fully copied (it's tiny and needed for foreign keys).

**Error handling:**
- If destination already exists: error.
- If source database is missing: error.
- If row count exceeds source: copy all rows with a warning.
- Transaction rollback on any failure — destination directory is cleaned up.

### Cross-Platform Symlinks

| Platform | Mechanism | Notes |
|---|---|---|
| macOS/Linux | `os.Symlink()` | Works out of the box. |
| Windows | `os.Symlink()` | Requires Developer Mode enabled or admin. Tool prints a clear error if symlink creation fails with a permissions hint. |

All symlink operations use Go's `os` package — no shell-outs to `ln` or `mklink`.

### MSGVAULT_HOME Interaction

msgvault supports a `MSGVAULT_HOME` environment variable that overrides the default `~/.msgvault` path. If `MSGVAULT_HOME` is set, msgvault ignores the `~/.msgvault` symlink entirely — it reads directly from the directory specified by the env var.

`devdata` should check for `MSGVAULT_HOME` on startup:
- If `MSGVAULT_HOME` is set, print a warning: "MSGVAULT_HOME is set to `<path>`; symlink operations on ~/.msgvault will not affect msgvault's data directory."
- The `--home` flag overrides this detection (the user is explicitly choosing a base directory).

This is a warning only — the tool still operates on `~/.msgvault` symlinks. The user may have `MSGVAULT_HOME` set for a separate purpose, and the symlink-based workflow still functions if `MSGVAULT_HOME` is unset at runtime.

### Schema Version Compatibility

`new-data` creates the destination database using the current embedded schema (from `internal/store`), then copies rows from the source using `INSERT INTO ... SELECT`. If the source database was created by an older version of msgvault with a different schema (missing columns, different column order), the copy may fail.

This is acceptable because:
1. The tool is version-coupled to msgvault — it shares the same Go module and is built from the same source tree.
2. If the schema has changed, the user should run `new-data` with a source database that matches the current schema.
3. The `SELECT *` queries will produce a clear SQL error on column count mismatch rather than silently corrupting data.

## Implementation Stages

### Stage 1: Project skeleton and symlink commands
Set up the tool structure with Cobra, implement `init-dev-data`, `exit-dev-data`, and `mount-data`. These are purely filesystem operations with no database access.

**Deliverable:** A working `devdata` binary that can transition `~/.msgvault` to/from symlink mode and switch between datasets.

### Stage 2: Database subset copying (`new-data`)
Implement the `new-data` command with SQLite ATTACH-based data copying. This is the most complex command.

**Deliverable:** `devdata new-data --dst dev --rows 100` creates a working dev dataset with 100 messages and all supporting data.

### Stage 3: Build integration and polish
Add Makefile targets, improve output formatting, add `list` command to show available datasets with their status.

**Deliverable:** `make build-devdata`, `devdata list` showing all datasets with sizes and active indicator.
