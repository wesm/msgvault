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

### Conversation Stats

The `conversations` table has denormalized stats columns:
`message_count`, `participant_count`, `last_message_at`,
`last_message_preview`. These are required for the Conversations
primary view.

**Stats are not maintained during message insertion.** Message
insertion is idempotent (`INSERT ... ON CONFLICT DO UPDATE`) and
imports are expected to be re-runnable. The store does not attempt to
detect insert-vs-update, and does not increment counters on upsert.

Instead, each importer calls `RecomputeConversationStats(sourceID)`
as a post-import step (like WhatsApp already does today). This runs
aggregate queries against `messages` and `conversation_participants`
to set all stats columns for conversations belonging to that source.
The operation is idempotent — running it twice produces the same
result.

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
`EnsureConversationWithType`, `EnsureLabel`, `LinkMessageLabel`,
`RecomputeConversationStats`, and message insertion with proper
`message_type`/`sender_id`/`conversation_type`.

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
- Call `RecomputeConversationStats` after import completes

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
- Call `RecomputeConversationStats` after import completes

### WhatsApp

Mostly fine as-is — already follows the target pattern. Minor cleanup:
- Use shared `NormalizePhone()` instead of internal normalization
- Migrate bulk stats update to shared `RecomputeConversationStats`
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

### New Navigation Model

Texts mode requires a different navigation shape than Email mode. The
current TUI is built around a single-key aggregate model: `ViewType`
selects a grouping dimension (sender, domain, label, time),
`AggregateRow` holds one key plus counts/sizes, and drill-down goes
from aggregate → message list → message detail. This structure does
not accommodate conversation-first navigation.

**Texts mode introduces a parallel navigation tree:**

```
Texts Mode
├── Conversations view (primary)
│   └── Drill: conversation → message timeline
├── Contacts view (aggregate)
│   └── Drill: contact → conversations with that contact → timeline
├── Contact Names view (aggregate)
│   └── Drill: name → conversations → timeline
├── Sources view (aggregate)
│   └── Drill: source → conversations from that source → timeline
├── Labels view (aggregate)
│   └── Drill: label → messages with that label
└── Time view (aggregate)
    └── Drill: period → conversations active in that period → timeline
```

**Implementation approach:** This is a new set of view types, query
methods, and TUI states — not a parameterization of the existing email
views.

- New `TextViewType` enum: `TextViewConversations`,
  `TextViewContacts`, `TextViewContactNames`, `TextViewSources`,
  `TextViewLabels`, `TextViewTime`.
- New `ConversationRow` struct for the Conversations view: `Title`,
  `SourceType`, `MessageCount`, `ParticipantCount`, `LastMessageAt`,
  `ConversationID`. This is not an `AggregateRow` — it has different
  fields and different drill-down semantics.
- New query engine methods: `ListConversations(filter)`,
  `TextAggregate(viewType, opts)`, `ListConversationMessages(convID,
  filter)`. These are separate from the email `Aggregate`/
  `ListMessages` methods.
- New TUI state machine entries for Texts mode navigation. The mode
  key (`m`) switches between the two state machines. Keybindings that
  overlap (Tab, Enter, Esc, `s`, `r`, `a`, `/`, `?`, `q`) behave
  the same way within each mode's navigation tree.

The email TUI code is untouched. Texts mode is additive — new files,
new types, new methods.

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
- Conversation stats come from denormalized columns recomputed
  post-import

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

### Message Detail

Pressing Enter on a message in the timeline does not open the email-
style detail view. The current detail model is email-shaped:
participants are `Address{Email, Name}` only, participant loading
reads `message_recipients`, and fallback body extraction assumes MIME
raw format. None of this works for text messages.

In Texts mode, Enter on a message in the timeline is a no-op (or
scrolls to show the full message body inline if truncated). A proper
text message detail view is deferred.

### Keybindings

Texts mode reuses the same key assignments as Email mode where the
action applies. Keys that map to email-only actions are disabled.

| Key | Email mode | Texts mode |
|-----|-----------|------------|
| `Tab` | Cycle aggregate views | Cycle text views (Conversations → Contacts → ...) |
| `Enter` | Drill down | Drill down (conversation → timeline; no message detail) |
| `Esc`/`Backspace` | Go back | Go back |
| `j`/`k`/`↑`/`↓` | Navigate rows | Navigate rows |
| `s` | Cycle sort field | Cycle sort field |
| `r` | Reverse sort | Reverse sort |
| `t` | Jump to Time view | Jump to Time view |
| `A` | Account selector | Source selector (lists text source accounts) |
| `a` | Jump to all messages | Jump to all conversations (reset filters) |
| `f` | Filter by attachments | Filter by attachments |
| `/` | Search (email FTS) | Search (text FTS, plain text only) |
| `?` | Help | Help |
| `q` | Quit | Quit |
| `m` | Switch to Texts mode | Switch to Email mode |
| `Space` | Toggle selection | Disabled (no deletion staging) |
| `d`/`D` | Stage deletion | Disabled (read-only) |
| `x` | Clear selection | Disabled |

The `A` key opens the same account selector UI but filtered to text
sources. This is a per-account filter (same `SourceID *int64`
plumbing), not a source-type bucket.

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
`source_type IN ('gmail', 'imap')`). Texts mode queries filter on the
text message types. The DuckDB query engine branches on mode for
aggregate key columns (email uses `from_email`/`from_domain`; texts
use `phone_number`/`conversation_title`).

### Query Engine

The DuckDB query engine gains new methods for Texts mode — these are
separate functions, not parameterizations of the existing email
methods:

- `ListConversations(filter TextFilter) ([]ConversationRow, error)` —
  queries denormalized conversation stats from Parquet, filtered and
  sorted.
- `TextAggregate(viewType TextViewType, opts TextAggregateOptions)
  ([]AggregateRow, error)` — aggregates text messages by contact,
  source, label, or time. Reuses `AggregateRow` since the shape
  (key + count + size) fits these views.
- `ListConversationMessages(convID int64, filter TextFilter)
  ([]MessageSummary, error)` — messages within a single conversation,
  chronological.

Existing email query methods are unchanged — they gain an implicit
`message_type = 'email'` filter to exclude text messages from email
views.

## Search

### FTS Indexing

Text messages are indexed in `messages_fts` alongside emails. The
FTS backfill pipeline is updated to populate the `from_addr` field
from `participants.phone_number` (via `messages.sender_id`) for text
messages, rather than only reading from `message_recipients` email
fields.

### Search Semantics by Mode

**Email mode** retains the current Gmail-style search operators:
`from:`, `to:`, `cc:`, `bcc:`, `subject:`, `account:`, etc. These
resolve against `message_recipients` and email-specific fields. No
changes.

**Texts mode uses plain full-text search only.** The `/` key opens
the same search input, but the query is treated as a plain text match
against `messages_fts` (body + sender phone/name), filtered to text
message types. The Gmail-style operators (`from:`, `subject:`, etc.)
are not supported in Texts mode — they map to email-specific fields
(`message_recipients`, `subject`) that don't apply to text messages.

If structured text search is needed later (e.g., `from:+1555...`,
`in:groupname`), it would be a new parser for text-specific
operators. For now, plain FTS is sufficient.

## Scope

### In Scope

- Refactor iMessage and Google Voice to phone-based persistence
- Shared `NormalizePhone()` utility
- Participant deduplication by phone number across all sources
- `RecomputeConversationStats` shared store method
- Label persistence contract for all importers
- CLI command renaming
- TUI Texts mode with new navigation model (Conversations +
  aggregates + message timeline), read-only, detail view disabled
- New query engine methods for text conversations and aggregates
- Unified Parquet cache with mode-aware columns and queries
- FTS indexing of text messages (including phone-based sender lookup)
- `build-cache` exports text messages alongside emails
- Source filter in Texts mode (per-account, same plumbing as email)

### Deferred

- WhatsApp web sync API (future import method)
- MMS/iMessage attachment extraction
- Contact name resolution from macOS address book (needed for full
  cross-channel unification of email-only iMessage handles with
  phone-based contacts)
- Cross-mode unified search (emails + texts together)
- Rich message detail view for texts
- Deletion support for text sources with live APIs
- Source-type bucket filter (filter by "all WhatsApp" vs per-account)
