# Unified Text Message Import

Merge three independent text message import implementations (WhatsApp
#160, iMessage #224, Google Voice #225) into a coherent system with a
shared schema, unified participant model, and dedicated TUI experience.

## Guiding Principles

1. **Phone number is the primary unification key.** If you communicate
   with someone through multiple channels (iMessage, WhatsApp, Google
   Voice) using the same phone number, all messages appear under one
   contact. Cross-channel unification where the only shared identifier
   is an address book entry (e.g., alice@icloud.com in iMessage and
   +1... in WhatsApp) requires address book resolution, which is
   deferred. Phone-based dedup handles the common case; gaps are
   acknowledged, not hidden.
2. **Texts are not emails.** The TUI has a separate Texts mode with
   conversation-centric navigation, not the sender-aggregate model used
   for email.
3. **Consistent UX across modes.** Same keybindings, sort/filter
   patterns, and visual language in both Email and Texts modes. Only the
   available views and drill-down behavior differ.
4. **Texts mode is read-only.** Imported text archives have no live
   delete API (iMessage reads a local DB, WhatsApp reads a backup,
   GVoice reads a Takeout export). Deletion staging (`d`/`D`) is
   disabled in Texts mode. Selection keybindings (`Space`/`A`) are
   reserved for future use (e.g., export) but do not stage deletions.

## Schema & Persistence

All text message importers converge on the same storage pattern.

### Participant Model

Phone number is the preferred unification key, but not all participants
have one. iMessage handles can be email addresses, and some senders are
short codes or system identifiers.

**Resolution order:**
1. If the handle normalizes to a valid E.164 phone number, use
   `EnsureParticipantByPhone` — this deduplicates across sources so the
   same phone from WhatsApp, iMessage, and Google Voice resolves to one
   `participants` row.
2. If the handle is an email address (common in iMessage), use the
   existing `EnsureParticipant` by email — the participant gets an
   `email_address` but no `phone_number`.
3. If the handle is neither (short codes, system senders), create a
   participant with the raw handle stored in `participant_identifiers`
   and no canonical phone or email.

No synthetic email addresses (`@phone.imessage`, `@phone.gvoice`).

**Platform identifier tracking:** `EnsureParticipantByPhone` (and the
email path) accept an `identifierType` parameter (`'whatsapp'`,
`'imessage'`, `'google_voice'`) so each importer registers its own
platform-specific identifier in `participant_identifiers`. The current
WhatsApp-hardcoded behavior is generalized.

A shared `NormalizePhone()` utility ensures consistent E.164
normalization across all importers. It returns an error for inputs that
cannot be normalized (email handles, short codes), signaling the caller
to fall through to path 2 or 3 above.

**Cross-channel limitations:** Participants matched by phone number are
unified automatically. Participants only known by email (e.g., an
iMessage contact using their iCloud address) remain separate from the
same person's phone-based participant until address book resolution is
implemented. The Contacts aggregate view in Texts mode will show these
as separate entries.

### Message Storage

| Column | Value |
|---|---|
| `messages.message_type` | `'whatsapp'`, `'imessage'`, `'sms'`, `'google_voice_text'`, `'google_voice_call'`, `'google_voice_voicemail'` |
| `messages.sender_id` | FK to `participants.id` (direct link, not via `message_recipients`) |
| `messages.subject` | NULL for text messages |
| `conversations.conversation_type` | `'group_chat'` or `'direct_chat'` |
| `conversations.title` | Group name, or resolved contact name for 1:1 (see fallback below) |
| `sources.source_type` | `'whatsapp'`, `'apple_messages'`, `'google_voice'` |
| `message_bodies.body_text` | Message text stored directly |
| `message_raw.raw_format` | `'whatsapp_json'`, `'imessage_json'`, `'gvoice_html'` |

No synthetic MIME wrapping for text messages. Body text goes directly
into `message_bodies`. Raw source data is stored in its native format.

### Conversation Title Fallback

Group chats use the group name from the source (WhatsApp subject,
iMessage `display_name`). For 1:1 chats, title is resolved with this
fallback chain:
1. `chat.display_name` (if set by the source)
2. Other participant's `display_name` from `participants`
3. Other participant's phone number or email handle

The TUI Conversations view uses this title for display. If the title
is still empty at display time (e.g., participant not yet resolved),
the raw handle is shown.

### Message Type Values

- iMessage sets `'imessage'` or `'sms'` based on the service field in
  `chat.db` (Apple distinguishes these natively).
- Google Voice uses distinct `message_type` values per record kind:
  `'google_voice_text'` for SMS/MMS, `'google_voice_call'` for call
  records, and `'google_voice_voicemail'` for voicemails. Labels
  (`sms`, `mms`, `call_received`, `call_placed`, `call_missed`,
  `voicemail`) provide finer-grained classification within each type.
  Call records have `conversation_type = 'direct_chat'` and are
  grouped into `calls:<phone>` threads.

### Texts Mode Message Type Filtering

Texts mode displays messages where `message_type` is one of:
`'whatsapp'`, `'imessage'`, `'sms'`, `'google_voice_text'`. Call
records (`'google_voice_call'`) and voicemails
(`'google_voice_voicemail'`) are excluded from the default Texts view.
They are accessible via the Labels aggregate view when filtered to the
relevant label.

### Conversation Stats Maintenance

The `conversations` table has denormalized stats columns:
`message_count`, `participant_count`, `last_message_at`,
`last_message_preview`. These are required for the Conversations
primary view.

**Store-level maintenance:** The store layer maintains these stats as
part of message insertion — not left to each importer. When a message
is inserted for a text source (identified by `message_type`), the
store updates the parent conversation's stats atomically:
- `message_count` incremented
- `last_message_at` updated if the new message is newer
- `last_message_preview` set to the message snippet
- `participant_count` updated when new `conversation_participants`
  rows are added

This replaces the WhatsApp importer's current approach of bulk-
updating stats in a post-processing step. All three importers get
correct stats automatically.

### Label Persistence

All importers that produce labels must create `labels` rows and link
them via `message_labels`. This is part of the shared persistence
contract:
- **WhatsApp:** source-specific labels as needed
- **iMessage:** `'iMessage'`, `'SMS'` (from service field)
- **Google Voice:** `'sms'`, `'mms'`, `'call_received'`,
  `'call_placed'`, `'call_missed'`, `'voicemail'`

The store provides `EnsureLabel(name, sourceID)` and
`LinkMessageLabel(messageID, labelID)`. Google Voice call/voicemail
records depend on labels for discoverability in the Labels aggregate
view.

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
`EnsureParticipantByPhone(phone, identifierType)`,
`EnsureParticipant(email, identifierType)` (for email-based handles),
`EnsureConversationWithType`, `EnsureLabel`, `LinkMessageLabel`, and
message insertion with proper
`message_type`/`sender_id`/`conversation_type`. The store handles
conversation stats maintenance automatically on insert.

### Shared Utilities (`internal/textimport/`)

- `NormalizePhone(raw string) (string, error)` — E.164 normalization;
  returns error for non-phone inputs
- Progress reporting (callback-based, like WhatsApp's
  `ImportCLIProgress`)

### iMessage Refactoring

Drop `gmail.API` interface implementation and synthetic MIME generation.
Instead:
- Read from `chat.db` directly (parsing stays the same)
- Resolve participants via phone or email (iMessage handles can be
  either); use `NormalizePhone` first, fall back to email path
- Set `message_type = 'imessage'` or `'sms'` (based on iMessage
  service field)
- Set `conversation_type` based on chat type (group vs 1:1)
- Populate `conversations.title` using the fallback chain (see
  Conversation Title Fallback section)
- Create labels (`'iMessage'`, `'SMS'`) and link to messages

### Google Voice Refactoring

Drop `gmail.API` interface implementation and synthetic MIME generation.
Instead:
- Parse HTML/VCF files (parsing stays the same)
- Call store methods for persistence with proper phone-based participants
- Set `message_type` per record kind: `'google_voice_text'`,
  `'google_voice_call'`, or `'google_voice_voicemail'`
- Set `conversation_type` based on participant count
- Store body text directly, raw HTML in `message_raw`
- Create labels (`'sms'`, `'mms'`, `'call_received'`, etc.) and link
  to messages

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
- Messages display in compact chat style (timestamp, sender, body
  snippet)
- Conversation stats (Messages, Participants, Last Message) come from
  denormalized columns maintained by the store layer

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
- Sort cycling (`s`), reverse (`r`)

**Read-only:** Deletion staging (`d`/`D`) and selection (`Space`/`A`)
are disabled in Texts mode. Imported text archives have no live delete
API — iMessage reads a local DB snapshot, WhatsApp reads a decrypted
backup, GVoice reads a Takeout export. There is no server to delete
from.

## Parquet Analytics

### Unified Cache with Mode Filtering

Text messages are stored in the same Parquet cache as emails, with
additional columns to support mode-specific queries. This avoids
duplicating the entire cache/query/staleness infrastructure.

```
~/.msgvault/analytics/
  messages/year=*/        # All messages (email + text)
  _last_sync.json
```

### Additional Parquet Columns

The existing denormalized Parquet schema is extended with:
- `phone_number` (sender, from `participants.phone_number`)
- `message_type` (whatsapp/imessage/sms/google_voice_*/email)
- `source_type` (whatsapp/apple_messages/google_voice/gmail)
- `conversation_title` (from `conversations.title`)
- `conversation_type` (group_chat/direct_chat/email_thread)
- `sender_id` (from `messages.sender_id`)

Email mode queries filter `WHERE message_type = 'email'` (or
`source_type = 'gmail'`). Texts mode queries filter on the text
message types. The DuckDB query engine branches on mode for aggregate
key columns (email uses `from_email`/`from_domain`; texts use
`phone_number`/`conversation_title`).

### Query Engine

The DuckDB query engine gains mode-aware aggregate methods. Same
function signatures as email aggregates, but the mode determines:
- Which `message_type` values are included
- Which columns are used for grouping (phone vs email, conversation
  vs domain)
- Which views are available (Conversations is texts-only; Domains is
  email-only)

Existing email queries are unchanged — they gain an implicit
`message_type = 'email'` filter.

## Search

Text messages are indexed in `messages_fts` alongside emails. The
FTS backfill pipeline is updated to populate the `from_addr` field
from `participants.phone_number` (via `messages.sender_id`) for text
messages, rather than only reading from `message_recipients` email
fields. Search in Texts mode filters results to text message types;
search in Email mode filters to email.

## Scope

### In Scope

- Refactor iMessage and Google Voice to phone-based persistence
- Shared `NormalizePhone()` utility
- Participant deduplication by phone number across all sources
- Store-level conversation stats maintenance
- Label persistence contract for all importers
- CLI command renaming
- TUI Texts mode (Conversations + aggregate views), read-only
- Unified Parquet cache with mode-aware columns and queries
- FTS indexing of text messages (including phone-based sender lookup)
- `build-cache` exports text messages alongside emails

### Deferred

- WhatsApp web sync API (future import method)
- MMS/iMessage attachment extraction
- Contact name resolution from macOS address book (needed for full
  cross-channel unification of email-only iMessage handles with
  phone-based contacts)
- Cross-mode unified search (emails + texts together)
- Rich message detail view for texts (headers, raw data display)
- Deletion support for text sources with live APIs
