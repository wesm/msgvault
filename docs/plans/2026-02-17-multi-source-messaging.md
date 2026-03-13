# Multi-Source Messaging Support

**Issue:** [wesm/msgvault#136](https://github.com/wesm/msgvault/issues/136)
**Author:** Ed Dowding
**Date:** 2026-02-17
**Status:** Draft for review

## Goal

Make msgvault a universal message archive — not just Gmail. Starting with WhatsApp, but ensuring the design works for iMessage, Telegram, SMS, and other chat platforms.

## Good News: The Schema Is Already Ready

The existing schema was designed for this. Key fields already in place:

| Table | Multi-source fields |
|-------|-------------------|
| `sources` | `source_type` ('gmail', 'whatsapp', 'apple_messages', 'google_messages'), `identifier` (email or phone), `sync_cursor` (platform-agnostic) |
| `messages` | `message_type` ('email', 'imessage', 'sms', 'mms', 'rcs', 'whatsapp'), `is_edited`, `is_forwarded`, `delivered_at`, `read_at` |
| `conversations` | `conversation_type` ('email_thread', 'group_chat', 'direct_chat', 'channel') |
| `participants` | `phone_number` (E.164), `canonical_id` (cross-platform dedup) |
| `participant_identifiers` | `identifier_type` ('email', 'phone', 'apple_id', 'whatsapp') |
| `attachments` | `media_type` ('image', 'video', 'audio', 'sticker', 'gif', 'voice_note') |
| `reactions` | `reaction_type` ('tapback', 'emoji', 'like') |
| `message_raw` | `raw_format` ('mime', 'imessage_archive', 'whatsapp_json', 'rcs_json') |

**No schema migrations needed.** The store layer (`UpsertMessage`, `GetOrCreateSource`, etc.) is already generic — it accepts any `source_type` and `message_type`. The tight coupling to Gmail is only in the sync pipeline and CLI commands.

## CLI Design

Per Wes's feedback, use `--type` not `--whatsapp`:

```bash
# Add accounts
msgvault add-account user@gmail.com                       # default: --type gmail
msgvault add-account --type whatsapp "+447700900000"      # WhatsApp via phone
msgvault add-account --type imessage                      # no identifier needed (local DB)

# Sync
msgvault sync-full                                        # all sources
msgvault sync-full user@gmail.com                         # specific account
msgvault sync-full "+447700900000"                        # auto-detects type from sources table
msgvault sync-full --type whatsapp                        # all WhatsApp accounts
msgvault sync-incremental                                 # incremental for all sources
```

**Account identifiers** use E.164 phone numbers for phone-based sources (`+447700900000`), email addresses for email-based sources. The existing `UNIQUE(source_type, identifier)` constraint means the same phone number can be both a WhatsApp and iMessage account.

## How Each Platform Syncs

The fundamental difference: Gmail is pull-based (fetch any message anytime), most chat platforms are push-based (stream messages in real time). Each platform gets its own package under `internal/` that knows how to sync into the shared store.

| Platform | Sync model | History access | Auth | Identifier |
|----------|-----------|---------------|------|------------|
| **Gmail** | Pull via API | Full random access | OAuth2 (browser or device flow) | Email address |
| **WhatsApp** | Connect + stream | One-time dump at pairing, then forward-only | QR code or phone pairing code | E.164 phone |
| **iMessage** | Read local SQLite | Full (reads `~/Library/Messages/chat.db`) | macOS Full Disk Access | None (local) |
| **Telegram** | Pull via TDLib | Full history via API | Phone + code | E.164 phone |
| **SMS/Android** | Read local SQLite | Full (reads `mmssms.db` from backup) | File access | E.164 phone |

No abstract `Provider` interface up front — just build each platform's sync as a standalone package, and extract common patterns once we have two working. YAGNI.

## WhatsApp Specifics (Phase 1)

### Library: whatsmeow

[whatsmeow](https://github.com/tulir/whatsmeow) is a pure Go implementation of the WhatsApp Web multi-device protocol. Production-grade — it powers the [mautrix-whatsapp](https://github.com/mautrix/whatsapp) Matrix bridge (2,200+ stars). Actively maintained (last commit: Feb 2026).

### Auth Flow

1. User runs `msgvault add-account --type whatsapp "+447700900000"`
2. Terminal displays QR code (or pairing code with `--headless`)
3. User scans with WhatsApp on their phone
4. Session credentials stored in SQLite (alongside msgvault's main DB)
5. Session persists across restarts — no re-scanning needed

Session expires if the primary phone doesn't connect to internet for 14 days, or after ~30 days of inactivity.

### Sync Model

**Critical constraint:** WhatsApp history is a one-time dump, not an on-demand API.

```
First sync:
  connect → receive history dump (HistorySync event) → stream until caught up → disconnect

Subsequent syncs:
  connect → stream new messages since last cursor → disconnect
```

On-demand historical backfill exists (`BuildHistorySyncRequest`) but is documented as unreliable, especially for groups. Design accordingly: treat initial history as best-effort, then reliably capture everything going forward.

### Media Must Be Downloaded Immediately

WhatsApp media URLs expire after ~30 days. Unlike Gmail where you can fetch any attachment anytime, WhatsApp media must be downloaded and stored locally at sync time. The existing content-addressed attachment storage (SHA-256 dedup) works perfectly for this.

### Message Type Mapping

| WhatsApp | msgvault field | Value |
|----------|---------------|-------|
| Text message | `messages.message_type` | `'whatsapp'` |
| Image/Video/Audio | `attachments.media_type` | `'image'`, `'video'`, `'audio'` |
| Voice note | `attachments.media_type` | `'voice_note'` |
| Sticker | `attachments.media_type` | `'sticker'` |
| Document | `attachments.media_type` | `'document'` |
| Reaction (emoji) | `reactions.reaction_type` | `'emoji'` |
| Reply/Quote | `messages.reply_to_message_id` | FK to parent message |
| Forwarded | `messages.is_forwarded` | `true` |
| Edited | `messages.is_edited` | `true` |
| Read receipt | `messages.read_at` | Timestamp |
| Delivery receipt | `messages.delivered_at` | Timestamp |
| Group chat | `conversations.conversation_type` | `'group_chat'` |
| 1:1 chat | `conversations.conversation_type` | `'direct_chat'` |
| Sender JID | `participant_identifiers.identifier_type` | `'whatsapp'`, value = `447700900000@s.whatsapp.net` |
| Sender phone | `participants.phone_number` | `+447700900000` (E.164) |
| Raw protobuf | `message_raw.raw_format` | `'whatsapp_protobuf'` |

### What Changes in Existing Code

**New package:** `internal/whatsapp/` — self-contained, no changes to existing Gmail code.

**Small changes needed:**
- `cmd/msgvault/cmd/addaccount.go`: Add `--type` flag, dispatch to WhatsApp auth when type is `"whatsapp"`
- `cmd/msgvault/cmd/syncfull.go`: Currently hardcodes `ListSources("gmail")` — change to `ListSources("")` (all types) with a type-based dispatcher
- `internal/store/`: Add `EnsureParticipantByPhone()` method (currently only handles email-based participants)
- `internal/store/`: Add `'member'` as a valid `recipient_type` for group chat participants

**No changes to:** schema, query engine, TUI, MCP server, HTTP API, or any consumer. Messages from WhatsApp will appear in search, aggregation, and all views automatically because consumers operate on the generic `messages` table.

## Risks

| Risk | Severity | Mitigation |
|------|----------|------------|
| **Account ban/warning** | High | WhatsApp TOS prohibits unofficial clients. Read-only archival is lower risk than bots, but not zero. Document prominently. Recommend a dedicated/secondary number for testing. |
| **History dump is incomplete** | Medium | WhatsApp server decides how much history to send at pairing. Design as "best effort snapshot + reliable stream forward." |
| **whatsmeow protocol breakage** | Medium | WhatsApp changes their protocol regularly. Pin whatsmeow version, expect occasional breakage, track upstream releases. |
| **Media URL expiration** | Low | Download everything at sync time. Already mitigated by design. |
| **Phone must be online every 14 days** | Low | Document requirement. Could add a warning in `sync` output if session is stale. |

## How Other Platforms Would Plug In Later

Each gets its own `internal/<platform>/` package that syncs into the store. Brief notes on feasibility:

**iMessage** (macOS only): Read `~/Library/Messages/chat.db` directly. Full history available. Timestamps use Apple epoch (nanoseconds since 2001-01-01). Tapbacks stored as separate messages referencing parent via `associated_message_guid` — would map to `reactions` table. Requires Full Disk Access permission. No network needed.

**Telegram**: TDLib (official C++ library with Go bindings) or import from Desktop export JSON. Full history available via API. Unique features: channels, supergroups, forums, scheduled messages, silent messages. User IDs are numeric (not phone-based) but phone is the auth method.

**SMS/Android**: Import from `mmssms.db` backup. Simple data model (phone, timestamp, body). MMS attachments in `part` table. No reactions, no threading, no edits.

**Signal**: Hardest. Desktop DB is SQLCipher-encrypted. Schema changes frequently (215+ migration versions). No official export API. Feasible but fragile.

## Implementation Phases

**Phase 1 — CLI + dispatcher (no new platforms):**
Add `--type` flag. Change sync dispatch from Gmail-only to type-based. All existing behavior unchanged.

**Phase 2 — WhatsApp sync:**
`internal/whatsapp/` package. QR pairing. History dump. Forward streaming. Media download. Phone participant handling.

**Phase 3 — WhatsApp features:**
Reactions, replies, groups with metadata, voice notes, stickers, read receipts.

**Phase 4 — Next platform (iMessage or Telegram):**
By this point we'll have two implementations and can extract common patterns if they emerge naturally. Not before.
