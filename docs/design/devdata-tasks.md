# devdata: Implementation Task List

## Stage 1: Project Skeleton and Symlink Commands

### 1.1 Create tool directory structure and entry point

**Files to create:**
- `tools/devdata/main.go` — entry point, calls `cmd.Execute()`
- `tools/devdata/cmd/root.go` — Cobra root command with `--home` flag to override home directory (defaults to `os.UserHomeDir()`)

**Code:**
- `main.go`: minimal — `func main() { cmd.Execute() }`
- `root.go`:
  - `var rootCmd = &cobra.Command{Use: "devdata", Short: "Manage msgvault datasets"}`
  - Persistent flag `--home` (string) to override the base directory (default: user home dir). All dataset paths derive from this.
  - Helper `homeDir() string` returns the resolved home directory.
  - Helper `msgvaultPath() string` returns `<home>/.msgvault`.
  - Helper `datasetPath(name string) string` returns `<home>/.msgvault-<name>`.
  - `func Execute()` calls `rootCmd.Execute()` with `os.Exit(1)` on error.

**Dependencies:**
- `github.com/spf13/cobra` (already in go.mod)

### 1.2 Create symlink helper package

**Files to create:**
- `tools/devdata/dataset/dataset.go` — symlink and filesystem operations

**Code — functions:**
- `IsSymlink(path string) (bool, error)` — `os.Lstat` + check `ModeSymlink`
- `ReadTarget(path string) (string, error)` — `os.Readlink` wrapper
- `Exists(path string) bool` — `os.Stat` check
- `HasDatabase(path string) bool` — checks `<path>/msgvault.db` exists
- `ReplaceSymlink(linkPath, target string) error` — remove old link, create new one
- `ListDatasets(homeDir string) ([]DatasetInfo, error)` — glob `<home>/.msgvault-*`, return name/path/size/hasDB for each
- `DatasetInfo` struct: `Name string`, `Path string`, `HasDB bool`, `Active bool` (matches current symlink target)

### 1.3 Implement `init-dev-data` command

**Files to create:**
- `tools/devdata/cmd/initdevdata.go`

**Code:**
- `var initDevDataCmd = &cobra.Command{...}`
- `func init()` registers with `rootCmd`
- Logic:
  1. `path := msgvaultPath()` (`~/.msgvault`)
  2. `goldPath := datasetPath("gold")` (`~/.msgvault-gold`)
  3. If `IsSymlink(path)`: print "Already in dev mode, linked to <target>" and return nil
  4. If `!Exists(path)`: return error "~/.msgvault does not exist"
  5. If `Exists(goldPath)`: return error "~/.msgvault-gold already exists; resolve manually"
  6. `os.Rename(path, goldPath)`
  7. `os.Symlink(goldPath, path)`
  8. Print "Initialized dev mode: ~/.msgvault → ~/.msgvault-gold"

### 1.4 Implement `exit-dev-data` command

**Files to create:**
- `tools/devdata/cmd/exitdevdata.go`

**Code:**
- `var exitDevDataCmd = &cobra.Command{...}`
- Logic:
  1. `path := msgvaultPath()`
  2. `goldPath := datasetPath("gold")`
  3. If `!IsSymlink(path)`: print "Not in dev mode (no symlink at ~/.msgvault)" and return nil
  4. If `!Exists(goldPath)`: return error "~/.msgvault-gold not found; cannot restore"
  5. `os.Remove(path)` (removes symlink only)
  6. `os.Rename(goldPath, path)`
  7. Print "Exited dev mode: ~/.msgvault restored"

### 1.5 Implement `mount-data` command

**Files to create:**
- `tools/devdata/cmd/mountdata.go`

**Code:**
- `var mountDataCmd = &cobra.Command{...}`
- Flag: `--dataset` (string, required)
- Logic:
  1. `path := msgvaultPath()`
  2. `target := datasetPath(datasetFlag)`
  3. If `!IsSymlink(path)`: return error "~/.msgvault is not a symlink; run init-dev-data first"
  4. If `!Exists(target)`: return error listing available datasets via `ListDatasets()`
  5. `currentTarget := ReadTarget(path)`
  6. If `currentTarget == target`: print "Already mounted" and return nil
  7. `ReplaceSymlink(path, target)`
  8. Print "Mounted dataset '<name>': ~/.msgvault → <target>"
  9. If `!HasDatabase(target)`: print warning "Note: no msgvault.db found in dataset"

### 1.6 Add Makefile target for building devdata

**Files to modify:**
- `Makefile` — add `build-devdata` target

**Code:**
```makefile
build-devdata:
	CGO_ENABLED=1 go build -tags fts5 -o devdata ./tools/devdata
	@chmod +x devdata
```

### 1.7 Verify Stage 1

**Manual testing checklist:**
- `make build-devdata` compiles successfully
- `./devdata init-dev-data` moves `~/.msgvault` and creates symlink
- `./devdata mount-data --dataset gold` confirms already mounted
- `./devdata exit-dev-data` restores original directory
- Running `init-dev-data` twice is idempotent (prints "already in dev mode")
- Running `exit-dev-data` when not in dev mode prints informational message
- Running `mount-data` on non-symlink prints clear error

---

## Stage 2: Database Subset Copying (`new-data`)

### 2.1 Implement `new-data` command scaffold

**Files to create:**
- `tools/devdata/cmd/newdata.go`

**Code:**
- `var newDataCmd = &cobra.Command{...}`
- Flags: `--src` (string, optional), `--dst` (string, required), `--rows` (int, required)
- Logic (scaffold):
  1. Resolve source path: if `--src` set, use `datasetPath(src)`; otherwise resolve `msgvaultPath()` following symlinks via `filepath.EvalSymlinks`
  2. Resolve destination path: `datasetPath(dst)`
  3. Validate: source has `msgvault.db`, destination does not exist
  4. Call `copyDataset(srcDB, dstDir, rows)` (implemented in 2.2)

### 2.2 Implement database subset copy logic

**Files to create:**
- `tools/devdata/dataset/copy.go`

**Code — function `CopySubset(srcDBPath, dstDir string, rowCount int) error`:**

1. Create `dstDir` with `os.MkdirAll(dstDir, 0700)`
2. Create destination DB at `<dstDir>/msgvault.db`
3. Open destination DB with `mattn/go-sqlite3`:
   - `_journal_mode=WAL&_busy_timeout=5000&_foreign_keys=OFF` (FK off during bulk copy)
4. Initialize schema by executing embedded `schema.sql` and `schema_sqlite.sql` (import from `internal/store`)
5. Attach source: `ATTACH DATABASE '<srcDBPath>' AS src`
6. Begin transaction
7. Copy data in dependency order — each step is a single `INSERT INTO ... SELECT`:

```
-- a. Sources (all rows — tiny table)
INSERT INTO sources SELECT * FROM src.sources;

-- b. Select message IDs (the N most recent)
-- Use a temp table to drive all subsequent copies
CREATE TEMP TABLE selected_messages AS
  SELECT id FROM src.messages ORDER BY sent_at DESC LIMIT ?;

-- c. Conversations referenced by selected messages
INSERT INTO conversations SELECT * FROM src.conversations
  WHERE id IN (SELECT DISTINCT conversation_id FROM src.messages
               WHERE id IN (SELECT id FROM selected_messages));

-- d. Participants referenced by selected messages (senders + recipients)
INSERT INTO participants SELECT * FROM src.participants
  WHERE id IN (
    SELECT sender_id FROM src.messages WHERE id IN (SELECT id FROM selected_messages)
    UNION
    SELECT participant_id FROM src.message_recipients WHERE message_id IN (SELECT id FROM selected_messages)
  );

-- e. Participant identifiers for copied participants
INSERT INTO participant_identifiers SELECT * FROM src.participant_identifiers
  WHERE participant_id IN (SELECT id FROM participants);

-- f. Conversation participants for copied conversations + participants
INSERT INTO conversation_participants SELECT * FROM src.conversation_participants
  WHERE conversation_id IN (SELECT id FROM conversations)
    AND participant_id IN (SELECT id FROM participants);

-- g. Messages
INSERT INTO messages SELECT * FROM src.messages
  WHERE id IN (SELECT id FROM selected_messages);

-- h. Message bodies
INSERT INTO message_bodies SELECT * FROM src.message_bodies
  WHERE message_id IN (SELECT id FROM selected_messages);

-- i. Message raw
INSERT INTO message_raw SELECT * FROM src.message_raw
  WHERE message_id IN (SELECT id FROM selected_messages);

-- j. Message recipients
INSERT INTO message_recipients SELECT * FROM src.message_recipients
  WHERE message_id IN (SELECT id FROM selected_messages);

-- k. Reactions
INSERT INTO reactions SELECT * FROM src.reactions
  WHERE message_id IN (SELECT id FROM selected_messages);

-- l. Attachments
INSERT INTO attachments SELECT * FROM src.attachments
  WHERE message_id IN (SELECT id FROM selected_messages);

-- m. Labels (all for copied sources)
INSERT INTO labels SELECT * FROM src.labels
  WHERE source_id IN (SELECT id FROM sources);

-- n. Message labels (intersection of copied messages and copied labels)
INSERT INTO message_labels SELECT * FROM src.message_labels
  WHERE message_id IN (SELECT id FROM selected_messages)
    AND label_id IN (SELECT id FROM labels);
```

8. Commit transaction
9. Re-enable foreign keys: `PRAGMA foreign_keys = ON`
10. Run `PRAGMA foreign_key_check` to verify integrity
11. Update denormalized conversation counts:
```sql
UPDATE conversations SET
  message_count = (SELECT COUNT(*) FROM messages WHERE conversation_id = conversations.id),
  participant_count = (SELECT COUNT(*) FROM conversation_participants WHERE conversation_id = conversations.id),
  last_message_at = (SELECT MAX(sent_at) FROM messages WHERE conversation_id = conversations.id);
```
12. Populate FTS5 index (if available):
```sql
INSERT INTO messages_fts(message_id, subject, body, from_addr, to_addr, cc_addr)
  SELECT m.id, m.subject, mb.body_text, ...
  FROM messages m LEFT JOIN message_bodies mb ON mb.message_id = m.id ...;
```
13. Detach source: `DETACH DATABASE src`
14. Print summary: messages copied, conversations, participants, DB file size

**Error handling:**
- On any error after creating `dstDir`, clean up by removing the directory (`os.RemoveAll`)
- If rowCount exceeds available messages, copy all and print a warning

### 2.3 Copy config.toml to new dataset

**Files to modify:**
- `tools/devdata/cmd/newdata.go` — after `CopySubset`, copy `config.toml` if present

**Code:**
- `copyFileIfExists(filepath.Join(srcDir, "config.toml"), filepath.Join(dstDir, "config.toml"))`
- Simple `io.Copy` from src to dst, skip if source file doesn't exist
- Place this helper in `tools/devdata/dataset/copy.go`

### 2.4 Import schema from internal/store

**Files to modify:**
- `tools/devdata/dataset/copy.go` — import `github.com/wesm/msgvault/internal/store` to access embedded schema

**Code:**
- The `store` package embeds `schema.sql` and `schema_sqlite.sql` via `//go:embed`. Access these through the store package's exported schema initialization, or directly embed them in the tool.
- Preferred approach: use `store.Open()` + `store.InitSchema()` to create the destination database, then close the store and re-open with raw `database/sql` for the ATTACH + bulk copy. This reuses existing schema logic and ensures the destination DB has the exact same schema as msgvault expects.

### 2.5 Verify Stage 2

**Manual testing checklist:**
- `./devdata init-dev-data` (if not already in dev mode)
- `./devdata new-data --dst dev --rows 100` creates `~/.msgvault-dev` with 100 messages
- `./devdata mount-data --dataset dev` switches to dev dataset
- `./msgvault stats` shows ~100 messages in the dev dataset
- `./msgvault tui` works with the dev dataset
- `./devdata mount-data --dataset gold` switches back to gold
- `./msgvault stats` shows full message count
- `PRAGMA foreign_key_check` returns no violations on the dev database
- Running `new-data` with `--dst dev` again fails (already exists)
- Running `new-data` with `--rows 999999` copies all messages with a warning

---

## Stage 3: Build Integration and Polish

### 3.1 Add `list` command to show available datasets

**Files to create:**
- `tools/devdata/cmd/list.go`

**Code:**
- `var listCmd = &cobra.Command{Use: "list", Short: "List available datasets"}`
- Uses `dataset.ListDatasets(homeDir())` to enumerate `~/.msgvault-*` directories
- Output table columns: `NAME`, `PATH`, `DB SIZE`, `ACTIVE` (asterisk if current symlink target)
- Also shows whether `~/.msgvault` is a symlink or real directory
- Uses `text/tabwriter` for aligned output (matches msgvault's `list-accounts` style)

### 3.2 Add `clean` target and update Makefile

**Files to modify:**
- `Makefile`

**Changes:**
- Add `devdata` to `clean` target: `rm -f msgvault mimeshootout devdata`
- Add `build-devdata` to `.PHONY` list
- Add `build-devdata` entry to `help` target output

### 3.3 Add `--dry-run` flag to `new-data`

**Files to modify:**
- `tools/devdata/cmd/newdata.go`

**Code:**
- Add `--dry-run` boolean flag
- When set, resolve paths, validate preconditions, query source for message count, print what would be copied, then exit without writing anything
- Output: source path, destination path, messages available, messages to copy, estimated tables affected

### 3.4 Improve output formatting across all commands

**Files to modify:**
- All command files in `tools/devdata/cmd/`

**Changes:**
- Consistent prefix style: `devdata:` for informational messages
- Use `fmt.Fprintf(os.Stderr, ...)` for progress/status messages
- Use `fmt.Fprintf(os.Stdout, ...)` for data output (list, stats)
- Print elapsed time for `new-data` (database copy can take seconds on large datasets)

### 3.5 Verify Stage 3

**Manual testing checklist:**
- `make build-devdata` builds successfully
- `make clean` removes `devdata` binary
- `./devdata list` shows all datasets with sizes and active marker
- `./devdata new-data --dst test --rows 50 --dry-run` shows preview without creating anything
- All commands print consistent, well-formatted output
- `./devdata --help` shows all subcommands with descriptions
- `./devdata <cmd> --help` shows flags and usage for each command
