# Yahoo Mail IMAP Support — Implementation Tasks

## Stage 1: IMAP Client and Authentication

**Deliverable:** `msgvault add-account you@yahoo.com --provider yahoo` prompts for the App Password interactively, connects to Yahoo IMAP, authenticates, lists folders, and stores the credential securely.

### 1.1 Add go-imap dependency

- **Dependencies:** Add `github.com/emersion/go-imap/v2` to `go.mod`
- **Command:** `go get github.com/emersion/go-imap/v2`

### 1.2 Create IMAP client package

- **File:** `internal/imap/client.go` (new)
- **Types:**
  ```go
  type Config struct {
      Host     string // default "imap.mail.yahoo.com"
      Port     int    // default 993
      Email    string
      Password string
  }

  type Client struct {
      config Config
      conn   *imapclient.Client
      logger *slog.Logger
  }

  type Folder struct {
      Name       string   // IMAP folder name (e.g., "INBOX")
      Attributes []string // IMAP attributes (\Noselect, etc.)
      Messages   uint32   // Total message count
  }
  ```
- **Functions:**
  - `New(cfg Config, opts ...Option) *Client` — create client (does not connect)
  - `Connect(ctx context.Context) error` — dial TLS + authenticate
  - `Close() error` — logout + close connection
  - `ListFolders(ctx context.Context) ([]Folder, error)` — LIST "" "*"
  - `WithLogger(logger *slog.Logger) Option` — option pattern matching gmail client
- **Behavior:**
  - TLS connection to `host:port` with `tls.Config{ServerName: host}`
  - LOGIN authentication with email + app password
  - Retry on connection failure: 3 attempts, backoff 5s/15s/45s
  - Context cancellation support throughout
- **Error types:**
  - `AuthError` — wraps authentication failures with user-friendly message
  - `ConnectionError` — wraps dial/TLS failures

### 1.3 Create IMAP client tests

- **File:** `internal/imap/client_test.go` (new)
- **Tests:**
  - `TestNewClient` — verify config defaults (host, port)
  - `TestConnectAuthError` — verify AuthError on bad credentials (use a mock or skip with build tag)
  - `TestListFolders` — verify folder parsing from IMAP LIST response
- **Approach:** Use `internal/imap/testutil_test.go` with a minimal in-process IMAP server mock, or use build-tag-gated integration tests against a real server

### 1.4 Extend config for Yahoo settings

- **File:** `internal/config/config.go` (modify)
- **Changes:**
  - Add `YahooConfig` struct:
    ```go
    type YahooConfig struct {
        IMAPHost string `toml:"imap_host"`
        IMAPPort int    `toml:"imap_port"`
    }
    ```
  - Add `Yahoo YahooConfig` field to `Config` struct
  - Add defaults in `Load()`: host = `"imap.mail.yahoo.com"`, port = `993`
- **Security:** The App Password is **never stored in config.toml**. Only IMAP host/port settings belong here. Credentials are stored in `~/.msgvault/tokens/` with 0600 permissions.

### 1.5 Add config tests for Yahoo section

- **File:** `internal/config/config_test.go` (modify)
- **Tests:**
  - `TestLoadYahooConfig` — verify TOML parsing of `[yahoo]` section (host/port only, no credentials)
  - `TestYahooDefaults` — verify default host/port when section is empty

### 1.6 Extend credential storage for App Passwords

- **File:** `internal/oauth/oauth.go` (modify)
- **Changes:**
  - Add `Provider` field to stored token JSON: `"gmail"` or `"yahoo"`
  - Add `AppPassword` field to stored token JSON (for Yahoo)
  - Add `SaveAppPassword(email, password string) error` method to `Manager`
  - Add `LoadAppPassword(email string) (string, error)` method to `Manager`
  - Reuse existing `tokenPath()`, atomic write, and 0600 permissions
- **Token file format (Yahoo):**
  ```json
  {
    "provider": "yahoo",
    "app_password": "abcd efgh ijkl mnop",
    "scopes": []
  }
  ```

### 1.7 Add `--provider` flag and interactive password prompt to `add-account`

- **File:** `cmd/msgvault/cmd/addaccount.go` (modify)
- **Dependencies:** `golang.org/x/term` (for `term.ReadPassword()` to suppress echo)
- **Changes:**
  - Add `--provider` flag (string, default `"gmail"`, choices: `"gmail"`, `"yahoo"`)
  - **No `--app-password` CLI flag** — CLI flags are visible in shell history and process listings, exposing credentials to other users. This is a deliberate omission.
  - When `provider == "yahoo"`:
    1. Check for `MSGVAULT_YAHOO_APP_PASSWORD` environment variable. If set, use it but **print a security warning**: `"Warning: Using app password from environment variable. For interactive use, omit the variable and enter the password at the prompt instead."`
    2. If no env var, prompt interactively: `"Enter Yahoo App Password: "` using `term.ReadPassword(int(os.Stdin.Fd()))` to suppress echo
    3. Create IMAP client, call `Connect()` to verify credentials
    4. Call `ListFolders()` to verify access
    5. Save credentials via `SaveAppPassword()` to `~/.msgvault/tokens/{email}.json` with 0600 permissions
    6. Call `store.GetOrCreateSource("yahoo", email)` to register account
    7. Print success message with folder count
  - Existing Gmail flow unchanged when `provider == "gmail"` (default)

### 1.8 Test add-account Yahoo flow

- **File:** `cmd/msgvault/cmd/addaccount_test.go` (new or modify existing)
- **Tests:**
  - `TestAddAccountYahooMissingPassword` — verify error when no password in env and stdin is not a terminal
  - `TestAddAccountYahooProviderFlag` — verify `--provider yahoo` sets source_type correctly
  - `TestAddAccountYahooEnvVarWarning` — verify security warning is printed when env var is used

---

## Stage 2: Message Fetching and Storage

**Deliverable:** `msgvault sync-full you@yahoo.com` fetches messages from all Yahoo IMAP folders and stores them in SQLite, viewable via `show-message`.

### 2.1 Add IMAP message fetching to client

- **File:** `internal/imap/client.go` (modify)
- **Types:**
  ```go
  type RawMessage struct {
      UID          uint32
      Folder       string
      Flags        []string
      InternalDate time.Time
      Size         uint32
      Raw          []byte    // Full RFC 5322 message
  }

  type FetchOptions struct {
      Folder    string
      UIDs      []uint32   // specific UIDs, or empty for all
      Since     time.Time  // IMAP SINCE filter (zero = no filter)
      Before    time.Time  // IMAP BEFORE filter (zero = no filter)
      BatchSize int        // messages per FETCH command (default 50)
  }
  ```
- **Functions:**
  - `SelectFolder(ctx context.Context, folder string) (*FolderStatus, error)` — SELECT folder, return message count + UIDVALIDITY
  - `SearchUIDs(ctx context.Context, criteria *SearchCriteria) ([]uint32, error)` — SEARCH command, returns UIDs
  - `FetchMessages(ctx context.Context, opts FetchOptions) ([]*RawMessage, error)` — FETCH BODY.PEEK[] + INTERNALDATE + FLAGS + RFC822.SIZE for UIDs in batches
- **Behavior:**
  - `FetchMessages` batches UIDs into groups of `BatchSize` (default 50)
  - Uses `BODY.PEEK[]` (not `BODY[]`) to avoid setting \Seen flag
  - Streams results via callback or collects into slice
  - Handles partial fetch failures (log + skip individual messages)

### 2.2 Add IMAP fetch tests

- **File:** `internal/imap/client_test.go` (modify)
- **Tests:**
  - `TestSelectFolder` — verify folder status parsing
  - `TestSearchUIDs` — verify UID search with date criteria
  - `TestFetchMessages` — verify raw message extraction
  - `TestFetchBatching` — verify batching of large UID sets

### 2.3 Create Yahoo sync orchestrator

- **File:** `internal/sync/yahoo.go` (new)
- **Types:**
  ```go
  type YahooSyncer struct {
      client   *imap.Client
      store    *store.Store
      logger   *slog.Logger
      progress gmail.SyncProgress  // reuse existing interface
      opts     *Options
  }
  ```
- **Functions:**
  - `NewYahooSyncer(client *imap.Client, store *store.Store, opts *Options) *YahooSyncer`
  - `Full(ctx context.Context, email string) (*gmail.SyncSummary, error)` — full sync workflow
- **Full sync workflow:**
  1. `GetOrCreateSource("yahoo", email)`
  2. `StartSync(sourceID, "full")`
  3. `Connect()` IMAP client
  4. `ListFolders()` → sync to labels table (reuse existing label storage)
  5. For each folder:
     a. `SelectFolder(folder)` → get UID count
     b. `SearchUIDs(criteria)` → filter by date if `--after`/`--before`
     c. `MessageExistsBatch(sourceID, uids)` → skip existing
     d. `FetchMessages(opts)` → get raw MIME for new messages
     e. For each message:
        - `mime.Parse(raw)` → extract fields
        - Build conversation from `References`/`In-Reply-To` headers
        - `persistMessage()` (reuse from sync.go or extract shared helper)
     f. `UpdateSyncCheckpoint()` after each batch
  6. `UpdateSourceSyncCursor(sourceID, timestamp)`
  7. `CompleteSync(syncID)`
  8. `Close()` IMAP connection

### 2.4 Extract shared message persistence logic

- **File:** `internal/sync/persist.go` (new, extracted from `sync.go`)
- **Purpose:** Move `persistMessage()` and `parseToModel()` helpers out of `sync.go` into a shared file so both `Syncer` (Gmail) and `YahooSyncer` can use them.
- **Changes to `internal/sync/sync.go`:** Remove `persistMessage()` and `parseToModel()` method bodies, replace with calls to shared functions. These become package-level functions or methods on a shared base type.
- **Conversation threading for Yahoo:**
  - Add `buildConversationID(parsed *mime.Message) string` function
  - Logic: Use first `References` header value, fall back to `In-Reply-To`, fall back to `Message-ID`
  - Gmail's `threadId` path remains unchanged

### 2.5 Add source_message_id format for Yahoo

- **File:** `internal/sync/yahoo.go`
- **Convention:** `source_message_id = "{folder}:{uid}"` (e.g., `"INBOX:12345"`)
- **Helper:** `func yahooMessageID(folder string, uid uint32) string`
- **Parser:** `func parseYahooMessageID(id string) (folder string, uid uint32, err error)`

### 2.6 Handle cross-folder deduplication

- **File:** `internal/sync/yahoo.go`
- **Problem:** Same message can appear in multiple IMAP folders (e.g., a message in both Inbox and a custom folder).
- **Solution:**
  - On encountering a message whose `Message-ID` header matches an existing `source_message_id` in a different folder: add the new folder as an additional label (via `message_labels`) but don't create a duplicate message record.
  - Lookup by `Message-ID` header in the messages table or by maintaining an in-memory map during sync.
  - Store the primary `source_message_id` as the first folder:uid encountered; subsequent folders are label-only associations.

### 2.7 Add Yahoo sync tests

- **File:** `internal/sync/yahoo_test.go` (new)
- **Tests:**
  - `TestBuildConversationID` — verify threading from References/In-Reply-To/Message-ID
  - `TestYahooMessageID` — verify format/parse of `"{folder}:{uid}"`
  - `TestCrossFolderDedup` — verify same Message-ID in two folders creates one message + two labels
  - `TestFullSyncWorkflow` — integration test with mock IMAP client (if feasible)

---

## Stage 3: CLI Integration and Progress

**Deliverable:** `sync-full` and `sync` commands detect Yahoo accounts automatically, show progress, support `--after`/`--before`/`--folders` flags, and checkpoint for resume.

### 3.1 Add provider detection to sync commands

- **File:** `cmd/msgvault/cmd/syncfull.go` (modify)
- **Changes:**
  - After resolving the account email, query `store.GetSourceByIdentifier(email)` to get `source_type`
  - If `source_type == "yahoo"`:
    1. Load App Password via `oauth.LoadAppPassword(email)` (fall back to `MSGVAULT_YAHOO_APP_PASSWORD` env var; never from config file)
    2. Create `imap.Client` with config
    3. Create `sync.YahooSyncer`
    4. Call `syncer.Full(ctx, email)` instead of Gmail sync
  - If `source_type == "gmail"`: existing behavior unchanged
  - The decision is transparent to the user — they just run `sync-full email`
- **New flag:** `--folders` (string, comma-separated) — only sync specified IMAP folders. Ignored for Gmail accounts. Default: all folders.

### 3.2 Add provider detection to incremental sync

- **File:** `cmd/msgvault/cmd/sync.go` (modify, previously `syncincremental.go`)
- **Changes:**
  - For Yahoo accounts in v1: print message "Incremental sync not yet supported for Yahoo. Use sync-full instead." and exit
  - Future: call `syncer.Incremental()` when implemented

### 3.3 Implement progress reporting for Yahoo sync

- **File:** `internal/sync/yahoo.go` (modify)
- **Changes:**
  - Call `progress.OnStart(totalMessages)` after listing all folders
  - Call `progress.OnProgress(processed, added, skipped)` after each message batch
  - Call `progress.OnComplete(summary)` at end
  - Implement `SyncProgressWithDate` — call `OnLatestDate()` with the `InternalDate` of the most recently processed message
- **Behavior:** Progress output matches Gmail format exactly — same counters, same rate display

### 3.4 Implement checkpointing for Yahoo sync

- **File:** `internal/sync/yahoo.go` (modify)
- **Changes:**
  - After each batch, call `store.UpdateSyncCheckpoint()` with:
    - `checkpoint_type = "yahoo_folder_uid"`
    - `checkpoint_value = "{folder}:{last_uid_processed}"`
  - On sync start, check for active checkpoint via `store.GetActiveSync()`
  - If checkpoint exists and `--noresume` not set:
    - Parse checkpoint to determine which folder/UID to resume from
    - Skip already-processed folders entirely
    - Within the checkpoint folder, skip UIDs ≤ last processed UID
  - Checkpoint cleared on sync completion

### 3.5 Wire `--after` / `--before` into IMAP SEARCH

- **File:** `internal/sync/yahoo.go` (modify)
- **Changes:**
  - Convert `Options.Query` date filters to IMAP SEARCH criteria:
    - `--after 2024-01-01` → IMAP `SINCE 01-Jan-2024`
    - `--before 2024-12-31` → IMAP `BEFORE 31-Dec-2024`
  - Note: IMAP SINCE/BEFORE operate on INTERNALDATE, which is date-only (no time component). This matches Gmail's `after:`/`before:` behavior.

### 3.6 Update `list-accounts` to show provider

- **File:** `cmd/msgvault/cmd/listaccounts.go` (modify)
- **Changes:**
  - Include `source_type` in output (e.g., `"you@yahoo.com (yahoo)"` vs `"you@gmail.com (gmail)"`)
  - Backward compatible — Gmail accounts still display correctly

### 3.7 Add CLI integration tests

- **File:** `cmd/msgvault/cmd/syncfull_test.go` (new or modify)
- **Tests:**
  - `TestSyncFullProviderDetection` — verify yahoo source_type triggers Yahoo syncer
  - `TestSyncFullFoldersFlag` — verify `--folders` flag parsing
  - `TestSyncIncrementalYahooUnsupported` — verify graceful message for Yahoo

---

## Stage 4: TUI, Analytics, and Deletion

**Deliverable:** Yahoo messages appear in TUI with folder labels. Parquet cache includes Yahoo data. Staged deletions execute via IMAP.

### 4.1 Verify TUI works with Yahoo data (no changes expected)

- **File:** `internal/tui/model.go`, `internal/tui/view.go` (verify, likely no changes)
- **Verification:**
  - Yahoo messages appear in all TUI views (Senders, Domains, Labels, Time, etc.)
  - Yahoo IMAP folders appear as labels in the Labels view
  - Account filter (`a` key) shows Yahoo accounts
  - Search works on Yahoo messages (FTS5 if available, fallback to subject/snippet)
- **Possible change:** If account display shows source_type, ensure "yahoo" renders cleanly

### 4.2 Verify Parquet cache includes Yahoo data (no changes expected)

- **File:** `cmd/msgvault/cmd/build_cache.go`, `internal/query/engine.go` (verify)
- **Verification:**
  - `build-cache` includes Yahoo messages in Parquet export
  - DuckDB queries return Yahoo + Gmail data together
  - No source_type filtering is accidentally applied
- **Possible change:** If the Parquet builder has Gmail-specific JOINs or filters, generalize them

### 4.3 Implement Yahoo IMAP deletion executor

- **File:** `internal/imap/client.go` (modify)
- **Functions:**
  - `DeleteMessages(ctx context.Context, folder string, uids []uint32) error`
    1. `SELECT folder`
    2. `STORE uids +FLAGS (\Deleted)`
    3. `EXPUNGE`
  - `MoveToTrash(ctx context.Context, folder string, uids []uint32) error`
    1. If server supports MOVE: `MOVE uids "Trash"`
    2. Else: `COPY uids "Trash"` then delete from source folder

### 4.4 Wire deletion executor into deletion command

- **File:** `cmd/msgvault/cmd/deletions.go` (modify, or `internal/deletion/manifest.go`)
- **Changes:**
  - When executing a deletion manifest for a Yahoo source:
    1. Group staged messages by folder (parse `source_message_id` → folder + uid)
    2. Connect IMAP client
    3. For each folder group: call `DeleteMessages()` or `MoveToTrash()` based on user choice
    4. Mark messages as `deleted_from_source_at` in database
  - Reuse existing deletion confirmation flow and safety checks
- **New flag or config:** `--trash` vs `--permanent` for Yahoo deletion (IMAP supports both)

### 4.5 Add deletion tests

- **File:** `internal/imap/client_test.go` (modify)
- **Tests:**
  - `TestDeleteMessages` — verify STORE + EXPUNGE sequence
  - `TestMoveToTrash` — verify MOVE or COPY+DELETE fallback

---

## Stage 5: Testing and Polish

**Deliverable:** Production-ready feature with integration tests, documentation, and error handling hardening.

### 5.1 Add integration test suite

- **File:** `internal/imap/integration_test.go` (new)
- **Approach:** Build-tag-gated integration tests (`//go:build integration`) that connect to a real Yahoo IMAP server.
- **Environment variables for test:**
  - `MSGVAULT_TEST_YAHOO_EMAIL` — test account email
  - `MSGVAULT_TEST_YAHOO_APP_PASSWORD` — test account app password
- **Tests:**
  - `TestIntegrationConnect` — verify connection + auth
  - `TestIntegrationListFolders` — verify folder listing
  - `TestIntegrationFetchMessages` — verify message fetch from Inbox
  - `TestIntegrationFullSync` — end-to-end: sync → verify in DB → verify in TUI query

### 5.2 Add unit tests with mock IMAP server

- **File:** `internal/imap/mock_test.go` (new)
- **Approach:** Use `net.Pipe()` or an in-memory IMAP server to test client behavior without a real server.
- **Tests:**
  - Connection retry on failure
  - Handling of IMAP server errors (NO, BAD responses)
  - Batch fetching with partial failures
  - Timeout handling

### 5.3 Error handling hardening

- **File:** `internal/imap/client.go`, `internal/sync/yahoo.go` (modify)
- **Changes:**
  - Wrap all errors with context: `fmt.Errorf("yahoo: select folder %q: %w", folder, err)`
  - Add specific error messages for common Yahoo failure modes:
    - "App Password expired or revoked" (auth failure after previously working)
    - "Too many connections" (Yahoo IMAP connection limit)
    - "Folder not found" (deleted between list and select)
  - Add connection health check: `client.Noop()` before operations to detect stale connections
  - Reconnect logic: if connection drops mid-sync, reconnect and resume from checkpoint
  - **Log sanitization:** Ensure the App Password and `MSGVAULT_YAHOO_APP_PASSWORD` environment variable value are never included in log output, error messages, or panic traces. Specifically:
    - Never log the `Config.Password` field
    - Redact environment variable values in any debug/error dumps
    - Use `[REDACTED]` placeholder if credential context must appear in error messages

### 5.4 Update CLAUDE.md with Yahoo commands

- **File:** `CLAUDE.md` (modify)
- **Changes:**
  - Add Yahoo commands to Quick Commands section
  - Add Yahoo config to Configuration section
  - Add Yahoo environment variables
  - Update Architecture section to mention IMAP package

### 5.5 Update README (if exists) or help text

- **File:** `README.md` or CLI help text (modify)
- **Changes:**
  - Document Yahoo Mail setup process (App Password generation link + steps)
  - Document Yahoo-specific flags (`--provider`, `--folders`)
  - Document environment variable `MSGVAULT_YAHOO_APP_PASSWORD`
  - Note limitations: no incremental sync in v1, IMAP date filtering is date-only

### 5.6 Update Makefile if needed

- **File:** `Makefile` (modify if needed)
- **Changes:**
  - Add `make test-integration` target for integration tests: `go test -tags integration ./internal/imap/...`
  - Verify `make build` still works with new dependency (CGO not required for go-imap)
  - Verify `make lint` passes with new code

---

## Environment Variables Summary

| Variable | Purpose | Default | Security |
|----------|---------|---------|----------|
| `MSGVAULT_YAHOO_APP_PASSWORD` | Yahoo App Password for IMAP auth (automation/CI only) | (none) | Prints warning; prefer interactive prompt. Must never be logged. |
| `MSGVAULT_TEST_YAHOO_EMAIL` | Test account for integration tests | (none) | — |
| `MSGVAULT_TEST_YAHOO_APP_PASSWORD` | Test account password for integration tests | (none) | — |

## CLI Changes Summary

| Command | Change |
|---------|--------|
| `add-account` | New flag: `--provider yahoo`. Interactive password prompt (no `--app-password` flag — see Security). |
| `sync-full` | New flag: `--folders`. Auto-detects Yahoo via source_type |
| `sync` | Prints "not yet supported" for Yahoo accounts |
| `list-accounts` | Shows provider type (gmail/yahoo) |
| `deletions execute` | Supports Yahoo via IMAP deletion |

## New Files Summary

| File | Purpose |
|------|---------|
| `internal/imap/client.go` | IMAP client (connect, auth, list, fetch, delete) |
| `internal/imap/client_test.go` | Unit tests for IMAP client |
| `internal/imap/mock_test.go` | Mock IMAP server for testing |
| `internal/imap/integration_test.go` | Integration tests (build-tag gated) |
| `internal/sync/yahoo.go` | Yahoo sync orchestrator |
| `internal/sync/yahoo_test.go` | Yahoo sync tests |
| `internal/sync/persist.go` | Shared message persistence (extracted from sync.go) |
| `docs/design/yahoo-mail-prd.md` | This PRD |
| `docs/design/yahoo-mail-tasks.md` | This task list |

## Modified Files Summary

| File | Change |
|------|--------|
| `go.mod` | Add go-imap dependency |
| `internal/config/config.go` | Add `YahooConfig` struct and fields |
| `internal/config/config_test.go` | Add Yahoo config tests |
| `internal/oauth/oauth.go` | Add App Password storage methods |
| `cmd/msgvault/cmd/addaccount.go` | Add --provider flag, interactive password prompt |
| `cmd/msgvault/cmd/syncfull.go` | Add provider detection, --folders flag |
| `cmd/msgvault/cmd/sync.go` | Add Yahoo "not supported" message |
| `cmd/msgvault/cmd/listaccounts.go` | Show provider in output |
| `internal/sync/sync.go` | Extract shared persistence to persist.go |
| `cmd/msgvault/cmd/deletions.go` | Add Yahoo IMAP deletion path |
| `CLAUDE.md` | Document Yahoo commands and config |
| `Makefile` | Add integration test target |

## Database Changes Summary

**No schema migrations required.** The existing schema supports Yahoo without modifications:
- `sources.source_type = "yahoo"` (column already supports arbitrary strings)
- `labels` table stores IMAP folders (same as Gmail labels)
- `messages.source_message_id` stores `"{folder}:{uid}"` format
- All other tables (participants, conversations, message_bodies, message_raw, attachments, sync_runs, sync_checkpoints) work unchanged

## Dependency Changes

| Package | Version | Purpose |
|---------|---------|---------|
| `github.com/emersion/go-imap/v2` | latest | IMAP client protocol |
