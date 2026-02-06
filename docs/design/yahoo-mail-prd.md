# Yahoo Mail IMAP Support — Product Requirements Document

## Overview

Add Yahoo Mail archival support to msgvault via IMAP, enabling users to sync, search, and manage Yahoo Mail alongside their existing Gmail archives. Yahoo Mail does not offer a public bulk-export API like Gmail, so IMAP is the standard protocol for third-party access. The feature reuses msgvault's existing storage schema (sources, messages, labels, participants), MIME parsing pipeline, and TUI — adding an IMAP client layer and Yahoo-specific authentication.

## Goals

- **Sync Yahoo Mail messages** into the same SQLite database used by Gmail, with full MIME parsing (subject, body, recipients, attachments).
- **Map IMAP folders to labels** so Yahoo folders (Inbox, Sent, Drafts, Trash, custom) appear alongside Gmail labels in the TUI and analytics.
- **App Password authentication (v1)** — users generate a Yahoo App Password and enter it via interactive terminal prompt during account setup. No Yahoo Developer registration required.
- **OAuth2 authentication (future)** — design the auth layer so Yahoo OAuth2 can be added later without restructuring.
- **Deletion staging** — allow users to stage Yahoo messages for deletion via IMAP, matching the existing Gmail deletion workflow.
- **CLI parity** — provide `add-account`, `sync-full`, and `sync` (incremental) commands that work identically for Yahoo accounts.
- **Parquet/TUI integration** — Yahoo messages appear in the TUI, analytics, search, and export alongside Gmail messages with no special handling.

## Non-Goals

- **Yahoo OAuth2 in v1** — deferred to a follow-up. The auth interface will support it, but implementation is App Password only initially.
- **Incremental sync in v1** — full sync only. IMAP UID tracking for incremental sync is a follow-up.
- **Yahoo-specific TUI features** — no folder tree view or Yahoo-specific UI. Folders are labels.
- **Contact sync** — only message participants are extracted, no Yahoo Contacts integration.
- **Calendar or other Yahoo services** — email only.

## Technical Decisions

### 1. IMAP Library: `github.com/emersion/go-imap/v2` + `github.com/emersion/go-message`

**Rationale:** go-imap v2 is the de facto standard Go IMAP library. It handles connection management, TLS, authentication, mailbox operations, and message fetching. go-message handles RFC 5322 parsing. However, since msgvault already uses `jhillyerd/enmime` for MIME parsing, we fetch raw bytes via IMAP and feed them to the existing `mime.Parse()` pipeline — no new MIME parser needed.

**Alternative considered:** Direct `net` + `textproto` IMAP. Rejected — reimplementing IMAP is error-prone and unnecessary.

### 2. Authentication: App Password via Interactive Prompt

**How it works:**
1. User generates an App Password in Yahoo Account Settings → Security → App Passwords.
2. User provides the App Password to msgvault via **interactive terminal prompt** during `add-account`. The prompt uses `term.ReadPassword()` (from `golang.org/x/term`) to suppress echo, so the password is never visible on screen or in shell history.
3. The password is stored in `~/.msgvault/tokens/{email}.json` with enforced 0600 permissions, matching the Gmail token storage pattern.

**Credential storage:** Store Yahoo credentials in the same `tokens/` directory as Gmail OAuth tokens, using the same JSON format but with different fields. The `tokenFile` struct gains an optional `AppPassword` field and a `Provider` field (`"gmail"` or `"yahoo"`).

**Security considerations:**
- App Passwords are equivalent to OAuth tokens in risk. Same file permissions (0600), same atomic write pattern.
- The password is **never stored in `config.toml`** — only IMAP host/port settings go there.
- The password is **never accepted via CLI flag** — CLI flags are visible in shell history (`~/.bash_history`, `~/.zsh_history`) and in process listings (`ps aux`), exposing credentials to other users on the same machine.
- For automation/CI scenarios, the `MSGVAULT_YAHOO_APP_PASSWORD` environment variable is supported as a fallback, but `add-account` prints a security warning when this path is used, recommending the interactive prompt for human users.

**⚠️ Open issue — Credential storage at rest:** The App Password is currently stored as plaintext in the token file (protected only by file permissions). This is the same security posture as Gmail OAuth refresh tokens, but it is not ideal. Before implementation, the following should be evaluated:
- **System keyring integration** (macOS Keychain, GNOME Keyring / libsecret, Windows Credential Manager) — best security, but adds platform-specific complexity and a dependency on `github.com/zalando/go-keyring` or similar.
- **Encrypted token file** — encrypt the token file with a user-supplied passphrase or a machine-derived key. Adds UX friction (passphrase prompt on every sync) unless a key agent is used.
- **Status quo** (0600 file permissions) — acceptable for single-user machines, insufficient for shared systems.

This is a pre-existing concern that also applies to Gmail OAuth refresh tokens, but adding a second credential type makes it more pressing to resolve.

### 3. Source Type: `"yahoo"` in `sources` Table

The existing `sources` table has `source_type TEXT` (currently always `"gmail"`). Yahoo accounts use `source_type = "yahoo"`. No schema change needed — the column already supports arbitrary strings.

### 4. IMAP Folders → Labels Mapping

IMAP folders map to the existing `labels` table:
- `source_label_id` = IMAP folder name (e.g., `"INBOX"`, `"Sent"`, `"Draft"`)
- `name` = Human-readable name (e.g., `"Inbox"`, `"Sent"`, `"Drafts"`)
- `label_type` = `"system"` for standard folders, `"user"` for custom folders

Standard Yahoo IMAP folders:
| IMAP Name | Label Name | Type |
|-----------|-----------|------|
| `INBOX` | Inbox | system |
| `Sent` | Sent | system |
| `Draft` | Drafts | system |
| `Trash` | Trash | system |
| `Bulk Mail` | Spam | system |
| `Archive` | Archive | system |
| (others) | (folder name) | user |

### 5. Conversation Threading

Gmail provides explicit `threadId` for threading. IMAP does not. Instead, use RFC 5322 headers:
- `Message-ID` — unique message identifier
- `In-Reply-To` — parent message ID
- `References` — chain of ancestor message IDs

**Strategy:** Build a `source_conversation_id` by:
1. If `References` header exists, use the first (root) Message-ID as the thread ID.
2. If only `In-Reply-To` exists, use that as the thread ID.
3. If neither exists, use the message's own `Message-ID` (standalone message).

This matches how most email clients thread messages and leverages the existing `conversations` table.

### 6. Sync Cursor (for future incremental sync)

The `sources.sync_cursor` field will store a JSON blob for Yahoo:
```json
{
  "folders": {
    "INBOX": {"uidvalidity": 12345, "last_uid": 67890},
    "Sent": {"uidvalidity": 12346, "last_uid": 45678}
  }
}
```

For v1 (full sync only), the cursor stores the timestamp of the last successful sync. Incremental sync will use UIDVALIDITY/UID tracking in a follow-up.

### 7. Rate Limiting / Connection Management

Yahoo IMAP has no documented rate limit, but aggressive connections can trigger temporary blocks. Strategy:
- Single IMAP connection per sync (Yahoo limits concurrent connections to ~15).
- Configurable fetch batch size (default: 50 messages per FETCH command).
- Respect IMAP server capabilities (IDLE, COMPRESS, etc.) but don't require them.
- Connection timeout: 30 seconds. Idle timeout: 5 minutes.

### 8. IMAP Server Configuration

Default Yahoo IMAP settings:
- **Host:** `imap.mail.yahoo.com`
- **Port:** `993`
- **TLS:** Required (IMAPS)

Allow override in config for edge cases (Yahoo Japan, AT&T Yahoo, etc.):
```toml
[yahoo]
imap_host = "imap.mail.yahoo.com"
imap_port = 993
```

### 9. Deletion via IMAP

Yahoo deletion uses IMAP `STORE +FLAGS (\Deleted)` + `EXPUNGE`:
1. Stage messages for deletion in the existing `deletion` system.
2. On deletion execution, connect via IMAP, find messages by source_message_id (IMAP UID), and delete.
3. IMAP deletion is per-folder, so the deletion executor must know which folder contains the message.

**Message ID storage:** `messages.source_message_id` stores `"{folder}:{uid}"` (e.g., `"INBOX:12345"`), providing both folder context and unique identification.

### 10. No Database Schema Changes Required

The existing schema handles Yahoo without modifications:
- `sources.source_type = "yahoo"` — already supports arbitrary strings
- `labels` — IMAP folders map directly
- `messages` — all fields applicable (subject, sender, recipients, dates, etc.)
- `message_bodies` / `message_raw` — same storage pattern
- `attachments` — same content-addressed storage
- `sync_runs` / `sync_checkpoints` — same sync tracking

The only addition is a new `source_type` value. No migrations needed.

## Design and Operation

### User Workflow

**Adding a Yahoo account:**
```bash
# Step 1: Generate App Password at https://login.yahoo.com/account/security/app-passwords
# Step 2: Add account to msgvault (interactive prompt for password)
msgvault add-account you@yahoo.com --provider yahoo
# Enter Yahoo App Password: ▊  (input hidden, not echoed)

# For automation/CI only (prints security warning):
MSGVAULT_YAHOO_APP_PASSWORD="abcd efgh ijkl mnop" msgvault add-account you@yahoo.com --provider yahoo
```

**Syncing:**
```bash
# Full sync (all folders)
msgvault sync-full you@yahoo.com

# Full sync with filters
msgvault sync-full you@yahoo.com --after 2024-01-01 --before 2024-12-31

# Full sync specific folders only
msgvault sync-full you@yahoo.com --folders "INBOX,Sent"

# Incremental sync (future, v2)
msgvault sync you@yahoo.com
```

**TUI, search, export — unchanged:**
```bash
msgvault tui                          # Shows Yahoo + Gmail together
msgvault tui --account you@yahoo.com  # Filter to Yahoo only
msgvault search "invoice" --account you@yahoo.com
```

### System Workflow: Full Sync

```
sync-full you@yahoo.com
  │
  ├─ Resolve provider: source_type="yahoo" (from --provider or stored source)
  ├─ Load credentials: App Password from token file / env var (never config file)
  ├─ Connect IMAP: imap.mail.yahoo.com:993 (TLS)
  ├─ Authenticate: LOGIN user password
  ├─ LIST folders → Sync to labels table
  │
  ├─ For each folder (INBOX, Sent, Draft, ...):
  │   ├─ SELECT folder → get message count, UIDVALIDITY
  │   ├─ SEARCH for UIDs (optionally filtered by date)
  │   ├─ Check which UIDs already exist in DB
  │   ├─ For each batch of new UIDs:
  │   │   ├─ FETCH uid:uid (BODY.PEEK[], INTERNALDATE, FLAGS, RFC822.SIZE)
  │   │   ├─ Parse MIME via existing mime.Parse()
  │   │   ├─ Build conversation from References/In-Reply-To
  │   │   ├─ Persist: message, body, raw, recipients, labels, attachments
  │   │   └─ Update progress
  │   └─ Save checkpoint (folder + last UID processed)
  │
  ├─ Update sync cursor
  ├─ Complete sync run
  └─ Disconnect IMAP
```

### Error Handling

| Failure Mode | Handling |
|-------------|----------|
| Wrong App Password | Clear error: "Authentication failed. Verify your Yahoo App Password." |
| Connection timeout | Retry up to 3 times with backoff (5s, 15s, 45s) |
| IMAP server error | Log, skip message, continue sync |
| MIME parse failure | Store raw MIME, use placeholder body (same as Gmail) |
| Folder disappeared mid-sync | Log warning, skip folder, continue |
| Connection dropped | Reconnect and resume from checkpoint |
| TLS certificate error | Fail with clear error, suggest checking IMAP host config |
| Yahoo account locked | Fail with instructions to unlock via Yahoo web |

### Edge Cases

- **Large mailboxes (100k+ messages):** Batch FETCH in groups of 50. Checkpoint after each folder. Resume from checkpoint on interrupt.
- **Duplicate messages across folders:** Same message in Inbox and a custom folder gets two label associations but only one message record. Dedup by Message-ID header.
- **Messages without Message-ID:** Use `"{folder}:{uid}"` as fallback source_message_id. Generate a synthetic conversation ID.
- **Non-UTF-8 content:** Handled by existing `textutil.EnsureUTF8()` and charset detection pipeline.
- **Yahoo Japan / regional variants:** Configurable IMAP host/port in config.

## Implementation Stages

### Stage 1: IMAP Client and Authentication
Build the core IMAP client with App Password authentication, connection management, and folder listing. Deliverable: `msgvault add-account you@yahoo.com --provider yahoo` connects and lists folders.

### Stage 2: Message Fetching and Storage
Fetch messages from IMAP folders, parse via existing MIME pipeline, store in database. Deliverable: `msgvault sync-full you@yahoo.com` archives messages viewable in SQLite.

### Stage 3: CLI Integration and Progress
Wire Yahoo sync into existing CLI commands with progress reporting, filtering (date range, folders), and checkpointing. Deliverable: Full CLI parity with Gmail sync commands.

### Stage 4: TUI, Analytics, and Deletion
Ensure Yahoo messages appear in TUI/Parquet analytics. Implement IMAP deletion for staged messages. Deliverable: End-to-end workflow from sync to TUI browsing to deletion.

### Stage 5: Testing and Polish
Integration tests, error handling hardening, documentation. Deliverable: Production-ready feature with tests and docs.
