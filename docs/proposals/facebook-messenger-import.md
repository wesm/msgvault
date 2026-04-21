# Facebook Messenger Import

Your Facebook Messenger history is trapped. Facebook gives you a "Download Your Information" export, which is a zip file full of JSON or HTML files organized by thread. The data is all there: timestamps, participants, reactions, photos, videos, call logs. It is yours. You cannot search it. You cannot cross-reference it with your email. It sits in a folder on your desktop until you forget about it.

msgvault already ingests Gmail, IMAP, Apple Mail, mbox, WhatsApp, iMessage, Google Voice. Messenger is the obvious gap. Especially for anyone archiving 20 years of communications, the Facebook era is a big chunk of that history.

## What this adds

`msgvault import-messenger` ingests a Facebook DYI export directory and stores every conversation in the same schema as email, WhatsApp, and iMessage. Messages become searchable in the TUI, the MCP server, and the HTTP API. Attachments land in content-addressed storage. Reactions are preserved.

```shell
# Basic import
msgvault import-messenger ~/Downloads/facebook-your_activity --me jesse

# Explicit format override (auto-detected by default)
msgvault import-messenger --format json ~/facebook-dyi --me jesse

# Limit for testing
msgvault import-messenger --limit 100 ~/facebook-dyi --me jesse
```

After import, Messenger conversations show up everywhere other message types do. Search them. Browse them in the TUI. Stage old threads for deletion. Build a collection that spans email and Messenger and deduplicate across both.

## DYI export formats

Facebook ships two DYI layouts, and both have changed over the years.

**JSON format** is the good one. Millisecond timestamps, structured participant lists, typed message categories (Generic, Share, Call, Unsubscribe), reaction metadata. The catch: Facebook encodes all strings as Latin-1 bytes stuffed into a JSON UTF-8 document. Every non-ASCII character is mojibake. The parser decodes this transparently.

**HTML format** is what most people have. Less metadata, no reaction structure, timestamps that vary by locale and DYI version. The parser handles four known timestamp layouts and falls back to best-effort extraction. When a thread has both JSON and HTML files, JSON wins.

**E2EE encrypted threads** use a flat-file format that differs from both. One message per line, colon-delimited, no JSON structure. A separate parser handles these.

All three formats are auto-detected per thread. A single export directory can contain a mix of all three, and often does.

## How it works

**Discovery.** The importer walks the export directory looking for the standard DYI structure: `messages/inbox/`, `messages/archived_threads/`, `messages/filtered_threads/`, etc. It handles both the old `messages/` layout and the newer `your_activity_across_facebook/messages/` layout. Each thread directory becomes one conversation.

**Parsing.** Each thread is parsed by the appropriate format parser. Participants are extracted from the thread metadata (JSON) or inferred from message senders (HTML). Messages are sorted chronologically. Attachments are resolved to absolute paths for ingestion.

**Identity.** The `--me` flag identifies the importing user's display name. This sets `is_from_me` on outbound messages so the TUI sender/recipient views work correctly. Participant addresses are synthesized as `<slug>@facebook.messenger` since Messenger does not expose real email addresses in DYI exports.

**Ingestion.** Messages are stored with `message_type='fbmessenger'` and `source_type='fbmessenger'`. The raw source bytes are preserved in `message_raw` for round-tripping. Attachments (photos, videos, stickers, audio) are ingested into content-addressed storage. Reactions are stored both as relational rows and appended to the message body so they are FTS-searchable.

**Resumability.** Import checkpoints every 50 threads (configurable). If interrupted, `--no-resume` starts fresh; otherwise it picks up where it left off.

## What it does not do

- No Facebook API integration. This is offline-only, from an export you already downloaded.
- No encrypted backup decryption. If Facebook ever ships encrypted local backups (like WhatsApp's msgstore.db), that would be a separate effort.
- No message-type filtering in the TUI aggregate views. The query layer hard-coded `message_type = 'email'` filters, which excluded Messenger results from aggregate views. A companion change removes those filters so all message types participate in search and aggregation. This is the right default: if you archived it, you want to find it.

## Scope

~4,400 lines across 40 files. The core importer (`internal/fbmessenger/`) is ~2,100 lines of implementation and ~1,600 lines of tests. The CLI command is 150 lines. Test coverage includes JSON parsing, HTML parsing, E2EE parsing, mojibake decoding, multi-file thread assembly, attachment resolution, FTS indexing, and Parquet cache integration.

The implementation follows the same patterns as the WhatsApp, iMessage, and Google Voice importers: a `discover → parse → ingest` pipeline with resumable checkpoints, using the standard `Store` API for all database writes.

## Related issues

- #136 (WhatsApp support) established the non-email message schema that this builds on.
- #192 (allow other formats for import) is partially addressed. Messenger is the most-requested non-email format after WhatsApp.
- #278 (dedup/collections) pairs with this. Once you have Messenger conversations alongside email, collections let you view them together and dedup catches any overlap.

## Branch

`jesse/fbmessenger` — 6 commits, cherry-picked cleanly from prior work onto current main. Tests pass, build clean.
