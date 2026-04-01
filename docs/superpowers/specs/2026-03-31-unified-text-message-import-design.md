# Unified Text Message Import

Merge three independent text message import implementations (WhatsApp
#160, iMessage #224, Google Voice #225) into a coherent system with a
shared schema, unified participant model, and dedicated TUI experience.

## Guiding Principles

1. **Phone number is the unification key.** If you communicate with
   someone through multiple channels (iMessage, WhatsApp, Google Voice),
   all messages appear under one contact.
2. **Texts are not emails.** The TUI has a separate Texts mode with
   conversation-centric navigation, not the sender-aggregate model used
   for email.
3. **Consistent UX across modes.** Same keybindings, sort/filter
   patterns, and visual language in both Email and Texts modes. Only the
   available views and drill-down behavior differ.

## Schema & Persistence

All text message importers converge on the same storage pattern.

### Participant Model

- `participants.phone_number` stores E.164 normalized phone numbers.
  No synthetic email addresses (`@phone.imessage`, `@phone.gvoice`).
- `EnsureParticipantByPhone` deduplicates across sources: the same
  phone number from WhatsApp, iMessage, and Google Voice resolves to
  one `participants` row.
- `participant_identifiers` tracks which platforms a contact is known
  on (`identifier_type = 'whatsapp'`, `'imessage'`, `'google_voice'`).
- A shared `NormalizePhone()` utility ensures consistent E.164
  normalization across all importers.

### Message Storage

| Column | Value |
|---|---|
| `messages.message_type` | `'whatsapp'`, `'imessage'`, `'sms'`, `'google_voice'` (see note below) |
| `messages.sender_id` | FK to `participants.id` (direct link, not via `message_recipients`) |
| `messages.subject` | NULL for text messages |
| `conversations.conversation_type` | `'group_chat'` or `'direct_chat'` |
| `conversations.title` | Group name, or contact name for 1:1 chats |
| `sources.source_type` | `'whatsapp'`, `'apple_messages'`, `'google_voice'` |
| `message_bodies.body_text` | Message text stored directly |
| `message_raw.raw_format` | `'whatsapp_json'`, `'imessage_json'`, `'gvoice_html'` |

No synthetic MIME wrapping for text messages. Body text goes directly
into `message_bodies`. Raw source data is stored in its native format.

### Message Type Values

- iMessage sets `'imessage'` or `'sms'` based on the service field in
  `chat.db` (Apple distinguishes these natively).
- Google Voice uses `'google_voice'` for all record types (texts, calls,
  voicemails). Call records and voicemails are differentiated via labels
  (`sms`, `mms`, `call_received`, `call_placed`, `call_missed`,
  `voicemail`) rather than separate `message_type` values. Call records
  have `conversation_type = 'direct_chat'` and are grouped into
  `calls:<phone>` threads.

### `conversation_participants`

All three importers populate this table to track who is in each
conversation, with roles where applicable (e.g., WhatsApp group admins).

## Importer Architecture

### Per-Source Packages

Each importer is its own package with source-specific parsing:

- `internal/whatsapp/` — reads decrypted WhatsApp `msgstore.db`
- `internal/imessage/` — reads macOS `chat.db`
- `internal/gvoice/` — parses Google Takeout HTML/VCF files

No shared interface is forced — each source is too different. But all
converge on the same store methods for persistence:
`EnsureParticipantByPhone`, `EnsureConversationWithType`, message
insertion with proper `message_type`/`sender_id`/`conversation_type`.

### Shared Utilities (`internal/textimport/`)

- `NormalizePhone(raw string) string` — E.164 normalization
- Progress reporting (callback-based, like WhatsApp's `ImportCLIProgress`)

### iMessage Refactoring

Drop `gmail.API` interface implementation and synthetic MIME generation.
Instead:
- Read from `chat.db` directly (parsing stays the same)
- Call store methods for persistence with proper phone-based participants
- Set `message_type = 'imessage'` or `'sms'` (based on iMessage service field)
- Set `conversation_type` based on chat type (group vs 1:1)
- Populate `conversations.title` from `chat.display_name`

### Google Voice Refactoring

Drop `gmail.API` interface implementation and synthetic MIME generation.
Instead:
- Parse HTML/VCF files (parsing stays the same)
- Call store methods for persistence with proper phone-based participants
- Set `message_type = 'google_voice'`
- Set `conversation_type` based on participant count
- Store body text directly, raw HTML in `message_raw`

### WhatsApp

Mostly fine as-is — already follows the target pattern. Minor cleanup:
- Use shared `NormalizePhone()` instead of internal normalization
- Ensure consistent `raw_format` naming

### CLI Commands

Renamed for consistency (each stays separate since inputs differ):

```
msgvault import-whatsapp <msgstore.db> --phone +1... [--media-dir] [--contacts]
msgvault import-imessage [--me +1...]
msgvault import-gvoice <takeout-dir>
```

The `source_type` is `'whatsapp'` regardless of import method (backup
now, web sync API later). `raw_format` in `message_raw` can distinguish
import methods if needed.

## TUI Texts Mode

### Mode Switching

A new key (`m`) toggles between Email mode and Texts mode. The status
bar shows the current mode. All existing email TUI behavior is
unchanged in Email mode.

### Conversations View (Primary)

The default view when entering Texts mode. Each row shows:

| Name | Source | Messages | Participants | Last Message |
|------|--------|----------|-------------|--------------|
| Jane Smith | iMessage | 1,247 | 2 | 2026-03-28 |
| Family Group | WhatsApp | 8,432 | 6 | 2026-03-30 |

- Default sort: last message date (newest first)
- Drill into a conversation: chronological message timeline
- Messages display in compact chat style (timestamp, sender, body snippet)

### Aggregate Views (Tab to Cycle)

- **Contacts** — aggregate by participant phone number/name, total
  messages across all sources and conversations
- **Contact Names** — aggregate by display name
- **Sources** — aggregate by source type (WhatsApp / iMessage / GVoice)
- **Labels** — source-specific labels (GVoice: sms/voicemail/call)
- **Time** — message volume over time (year/month/day granularity)

### Drill-Down

- From Conversations: chronological message timeline
- From Contacts: all conversations with that person (across all
  sources), then drill into a specific conversation
- From Time: conversations active in that period

### Filters and Interaction

All existing patterns carry over:
- Account filter (`a`) — doubles as source-type filter
- Date range, attachment filter
- Search (`/`) — queries FTS, results filtered to text messages
- Selection (`Space`/`A`), deletion staging (`d`/`D`)
- Sort cycling (`s`), reverse (`r`)

## Parquet Analytics

### Separate Cache for Texts

```
~/.msgvault/analytics/
  messages/year=*/        # Email (existing)
  texts/year=*/           # Text messages (new)
  _last_sync.json
```

### Text Parquet Schema (Denormalized)

- `message_id`, `source_id`, `conversation_id`
- `phone_number`, `display_name` (sender)
- `message_type` (whatsapp/imessage/sms/google_voice)
- `source_type` (whatsapp/apple_messages/google_voice)
- `conversation_title`, `conversation_type`
- `sent_at`, `year` (partition key)
- `body_length`, `has_attachments`, `attachment_count`
- `to_phones[]` (recipient phone numbers)
- `labels[]`

### Query Engine

DuckDB query engine gets parallel methods for texts — same
aggregate/filter patterns as email but keyed on phone numbers and
conversations instead of email addresses and domains.

## Search

Text messages are indexed in `messages_fts` alongside emails. Search
in Texts mode filters results to text message types; search in Email
mode filters to email. The FTS table and indexing pipeline are shared.

## Scope

### In Scope

- Refactor iMessage and Google Voice to phone-based persistence
- Shared `NormalizePhone()` utility
- Participant deduplication by phone number across all sources
- CLI command renaming
- TUI Texts mode (Conversations + aggregate views)
- Text message Parquet cache and DuckDB query methods
- FTS indexing of text messages
- `build-cache` builds both email and text Parquet files

### Deferred

- WhatsApp web sync API (future import method)
- MMS/iMessage attachment extraction
- Contact name resolution from macOS address book
- Cross-mode unified search (emails + texts together)
- Rich message detail view for texts (headers, raw data display)
