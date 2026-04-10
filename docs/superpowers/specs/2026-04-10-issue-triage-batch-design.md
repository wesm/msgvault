# Issue Triage Batch — 8 Low-Hanging Fixes

Batch of small, self-contained fixes for open issues. Each is independent
and can be implemented, tested, and committed separately.

## 1. IMAP: classify standard folders as system labels (#198)

**File:** `internal/imap/client.go` — `ListLabels()` function

**Current behavior:** Only `INBOX` gets `label_type = "system"`. All other
mailboxes are `"user"`.

**Change:** Expand system label detection using two strategies:

1. Check `item.Attrs` for RFC 6154 special-use attributes: `\Sent`,
   `\Drafts`, `\Trash`, `\Junk`, `\All`, `\Archive`, `\Flagged`.
2. Fall back to case-insensitive folder name matching for servers that
   don't advertise special-use attributes.

System folder name map (case-insensitive):

| Names | Type |
|-------|------|
| `INBOX` | system |
| `Sent`, `Sent Items`, `Sent Messages` | system |
| `Drafts`, `Draft` | system |
| `Trash`, `Deleted Items`, `Deleted Messages` | system |
| `Junk`, `Bulk Mail`, `Spam` | system |
| `Archive`, `All Mail`, `[Gmail]/All Mail` | system |

**Testing:** Unit test with mock folder list containing both attributed and
non-attributed folders. Verify system/user classification.

## 2. Stop enforcing .mbox extension on import files (#178)

**File:** `internal/importer/mboxzip/mbox_zip.go`

**Current behavior:** Files without `.mbox` or `.mbx` extension are rejected.
Extension checks exist at 7 locations:

1. `ResolveMboxExport` switch (~line 59): rejects non-.mbox/.mbx/.zip
2. `ResolveMboxExport` error (~line 104): error message lists extensions
3. `zipMboxCacheKey` (~line 136): skips non-.mbox/.mbx zip entries
4. `ExtractMboxFromZipWithLimits` (~line 234): skips zip entries
5. `validateExtractedMboxCache` (~line 433): rejects unexpected files
6. `expectedMboxFilesFromZip` (~line 527): skips zip entries
7. `findExtractedMboxFiles` (~line 694): skips non-.mbox/.mbx files

**Change for bare files (locations 1-2):** The `default` case in the
switch treats any non-.zip file as mbox. Remove the extension switch
entirely — if it's a regular file and not a zip, attempt mbox import.

**Change for zip entries (locations 3-6):** Do NOT blindly process all
zip entries (READMEs, metadata files would break the import). Use a
two-tier filter:

1. Always accept entries with `.mbox` or `.mbx` extension (existing).
2. For extensionless entries, use `mbox.Validate()` (reads first few KB
   looking for `From ` separators) to confirm content before extracting.
3. Skip entries with any other extension (`.txt`, `.json`, `.md`, etc.).

Extract an `isMboxCandidate(name string) bool` helper for the extension
check (returns true for `.mbox`, `.mbx`, or no extension). The content
sniff via `mbox.Validate()` applies only during extraction, not during
cache key computation (location 3) where we don't want to read content.
For cache keying, use extension-only filtering (accept `.mbox`, `.mbx`,
and extensionless).

**Change for cache discovery (location 7):** Use `isMboxCandidate()` —
accept files with known mbox extensions or no extension.

**Testing:** Existing tests plus:
- Bare file with no extension
- Zip containing extensionless mbox alongside a README.txt

## 3. import-mbox: support multiple --label flags (#189)

**Files:**
- `cmd/msgvault/cmd/import_mbox.go` — flag definition
- `internal/importer/mbox_import.go` — label application

**Current behavior:** Single `--label` flag via `StringVar`.

**Change:**
1. Change `importMboxLabel string` to `importMboxLabels []string`
2. Use `StringSliceVar` for the flag (supports `--label a --label b` and
   `--label a,b`)
3. Update `MboxImportOptions.Label string` to `Labels []string`
4. In `importMbox()`, loop through all labels calling `EnsureLabel` for
   each, collecting into `labelIDs` slice
5. Deduplicate labels to avoid PK violations on `message_labels`

**Testing:** Test with multiple labels, duplicate labels, empty labels.

## 4. import-mbox: add labels on duplicate re-import (#190)

**File:** `internal/importer/mbox_import.go`

**Current behavior:** Duplicate messages are skipped entirely — no label
updates.

**Change:** When a duplicate is detected and `labelIDs` is non-empty:
1. Look up the existing message ID (from the existence check)
2. Call `st.AddMessageLabels(msgID, labelIDs)`
3. Track with `summary.LabelsUpdated` counter (counts messages where
   label add was attempted — `AddMessageLabels` uses INSERT OR IGNORE
   and does not return rows-affected, so we count attempts not inserts)
4. Log at debug level

This matches the existing pattern in the emlx importer
(`internal/importer/emlx_import.go` ~line 306-310).

**Dependency:** Implement after #189 (multiple labels) since the label
slice is already available.

**Testing:** Import mbox with label A, re-import same mbox with label B,
verify message has both labels.

## 5. CLI completion (#113)

**File:** New `cmd/msgvault/cmd/completion.go`

**Change:** Add a `completion` subcommand using Cobra's built-in
completion generation. Supports bash, zsh, fish, and powershell.

Usage:
```
msgvault completion bash > /etc/bash_completion.d/msgvault
msgvault completion zsh > "${fpath[1]}/_msgvault"
msgvault completion fish > ~/.config/fish/completions/msgvault.fish
```

**Implementation:** Standard Cobra pattern — `cobra.Command` with
subcommands for each shell, calling `rootCmd.GenBashCompletionV2()`,
`rootCmd.GenZshCompletion()`, etc.

**Root pre-run skip:** Add `"completion"` to the config-loading skip
list in `root.go:38` (`cmd.Name() == "completion"`). Without this,
completion generation fails on machines without a valid msgvault config.

**Testing:** Verify command runs and produces non-empty output for each
shell.

## 6. Web server search: support query syntax (#174)

**Files:**
- `internal/api/handlers.go` — `handleSearch()`
- `internal/api/server.go` — `Server` struct (has `engine query.Engine`)
- `internal/query/sqlite.go` — `SQLiteEngine.Search()`
- `internal/search/parser.go` — `Parse()`

**Current behavior:** `handleSearch()` calls `s.store.SearchMessages(rawQuery, offset, pageSize)`
which does raw FTS5/LIKE search with hardcoded `deleted_from_source_at IS NULL`.
The CLI path uses `search.Parse()` → `query.SQLiteEngine.Search()` which
supports `from:`, `to:`, `label:`, `subject:`, `has:attachment`,
`before:`, `after:` operators via `buildSearchQueryParts()`.

**Change:** Switch `handleSearch()` to use `s.engine` (the `Server`
struct already has an `engine` field) with the existing
`SearchFast`/`SearchFastCount` methods:

1. Parse query with `search.Parse(queryStr)`
2. Set `q.HideDeleted = true` to preserve current API behavior
3. Call `s.engine.SearchFastCount(ctx, q, filter)` for total
4. Call `s.engine.SearchFast(ctx, q, filter, pageSize, offset)` for rows
5. Convert `[]query.MessageSummary` to API response format

No new interface methods needed — `SearchFastCount` already exists on
`query.Engine` and is implemented by `SQLiteEngine`, `DuckDBEngine`,
and `MockEngine`.

**Recipients:** `toMessageSummaryFromQuery()` hardcodes `To: []string{}`
because `query.MessageSummary` does not include recipients. The current
store-based path batch-populates To/Cc/Bcc via `store.BatchLoadRecipients`.
To preserve this, after getting search results from the engine, call
`s.store.BatchLoadRecipients(messageIDs)` and merge into the response.

**Fallback:** If `s.engine` is nil (server started without query engine),
fall back to existing `s.store.SearchMessages()` for basic FTS-only
search.

**Testing:** Test API endpoint with `from:user@example.com`,
`label:SENT`, and plain text queries.

## 7. Dockerfile: fix GLIBC issue (#173)

**File:** `Dockerfile`

**Current behavior:** Build stage uses `golang:1.25-bookworm` and
runtime uses `debian:bookworm-slim`. Both are nominally bookworm, but
the golang image may ship a newer glibc than the slim runtime (different
package versions, or CGO deps like DuckDB may link against newer
symbols). Issue reporter confirms the binary requires GLIBC_2.38 at
runtime.

**Change:** Switch runtime base to `chainguard/wolfi-base:latest`
(rolling release with current glibc, smaller image):
- Replace `apt-get` with `apk` for package installation
- Change `libstdc++6` to `libstdc++`
- Update user creation: `adduser -D -h /home/msgvault -u 1000 -s /bin/sh msgvault`
- Keep build stage unchanged (`golang:1.25-bookworm`)

**Verification:** After building, run
`docker run --rm --entrypoint ldd msgvault /usr/local/bin/msgvault`
to confirm all shared libraries resolve (image has
`ENTRYPOINT ["msgvault"]` so `--entrypoint` override is required).
Verify `msgvault serve` starts and `/health` responds.

**Testing:** Build image on CI. Verify health endpoint.

## 8. IMAP: support password via environment variable (#197)

**File:** `cmd/msgvault/cmd/addimap.go`

**Current behavior:** Password read from interactive TTY prompt or piped
stdin. No env var support, blocking CI/Docker use.

**Change:**
1. Before the existing password strategy selection, check
   `MSGVAULT_IMAP_PASSWORD` env var
2. If set, use it directly and print a security warning:
   `"Using password from MSGVAULT_IMAP_PASSWORD environment variable"`
3. No CLI flag (passwords in flags are visible in `ps` and shell history)

**Testing:** Set env var, verify password is used. Unset env var, verify
interactive prompt still works.

## Implementation Order

Issues are independent, but this order minimizes conflicts:

1. #198 — IMAP system labels (isolated file)
2. #197 — IMAP password env var (isolated file)
3. #178 — Remove mbox extension check
4. #189 — Multiple --label flags
5. #190 — Add labels on duplicate re-import (depends on #189)
6. #113 — CLI completion (new file, no conflicts)
7. #174 — Web server search (needs investigation of store layer)
8. #173 — Dockerfile (isolated file)

## Out of Scope

- #200 (list-accounts source type) — already implemented
- #88 (export-eml flags) — already implemented
- Any new import formats (.eml, .pst, Maildir)
- Incremental sync changes
- TUI changes
