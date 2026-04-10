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

**Change:** Remove extension enforcement at all three locations:
- Initial file classification (~line 59): treat non-zip files as mbox
  regardless of extension
- Zip entry filtering (~line 234): process all entries in zip files, not
  just those with `.mbox`/`.mbx` extensions
- Error message (~line 104): update to reflect new behavior

Mbox format has a clear `From ` line marker — invalid files fail at parse
time with an actionable error message. No need for extension gatekeeping.

**Testing:** Existing tests should continue to pass. Add a test with a
file that has no extension.

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
3. Track with `summary.LabelsAdded` counter
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

**Testing:** Verify command runs and produces non-empty output for each
shell.

## 6. Web server search: support query syntax (#174)

**Files:**
- `internal/api/handlers.go` — `handleSearch()`
- `internal/search/parser.go` — `Parse()`

**Current behavior:** HTTP API passes the raw query string directly to
`store.SearchMessages()`. CLI uses `search.Parse()` to support operators
like `from:`, `to:`, `label:`, `subject:`, etc.

**Change:** In `handleSearch()`, pass the query through `search.Parse()`
and use the structured query to build the search, same as the CLI path.

Need to trace how `search.Parse()` result flows into the store layer and
ensure the API can use the same code path. May need to add a
store method that accepts a parsed query, or convert the parsed query
into SQL the same way the CLI search does.

**Testing:** Test API endpoint with `from:`, `label:`, and plain text
queries. Verify results match CLI search.

## 7. Dockerfile: fix GLIBC issue (#173)

**File:** `Dockerfile`

**Current behavior:** Runtime base is `debian:bookworm-slim` which has
GLIBC 2.36. The binary requires GLIBC 2.38+ (from the build stage's
newer bookworm).

**Change:** Switch runtime base to `chainguard/wolfi-base:latest`:
- Replace `apt-get` with `apk` for package installation
- Change `libstdc++6` to `libstdc++`
- Update `adduser` syntax for wolfi (BusyBox)
- Keep build stage unchanged (golang:1.25-bookworm)

**Testing:** Build image, verify `msgvault serve` starts and `/health`
responds.

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
