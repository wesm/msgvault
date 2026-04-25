# Identities, Collections, and Deduplication

> **Note:** The data-model, scope, and live-message-filtering portions of this spec have been superseded by [`2026-04-25-account-collection-model-alignment.md`](./2026-04-25-account-collection-model-alignment.md), which incorporates the [PR #286 review feedback](https://github.com/wesm/msgvault/pull/286#issuecomment-4320039075). The product framing below still stands.

Our communications are scattered across decades of email accounts, chat apps, phone backups, and meeting recordings. Getting it all into one place is the first step. msgvault handles that -- it accepts Gmail API syncs, old mbox exports, Apple Mail folders from a retired laptop, IMAP backups, Facebook Messenger dumps, SMS exports, WhatsApp histories, meeting recordings, call logs. Throw everything at it.

The mess comes after. The same email appears three times because it arrived via Gmail sync, an old mbox export, and an Apple Mail import. Message counts are wrong. Search returns duplicates. There's no way to tell what was sent vs received in an old mbox dump that lost its metadata. And every import sits in its own silo -- there's no unified view of "all my communications."

msgvault already does the hard part: ingesting data from anywhere. What it lacks are the tools to properly organize that pile so it can be used and reused.

## Proposal

### I'm proposing three concepts that turn a pile of imports into an authoritative archive:

1. **Identities** (tied to accounts). "Who am I in this data?" A set of email addresses and phone numbers attached to an account. Identity determines what was sent vs received, powers sender exchange analysis, and recovers metadata that old imports lost.
2. **Collections** (of accounts). A named grouping of accounts -- "personal," "work," "everything." Collections are how the user sees their communications as one unified archive across multiple accounts.
3. **Deduplication** (of accounts, and of collections). Collapse redundant copies so counts and search results reflect reality. No data destroyed, ever. Runs at import time, at collection creation, or on demand.

None of these require a specific order. Dedup a single account today. Import more data next month. Create a collection once you've figured out what you have. Dedup again. The system works in whatever sequence the user needs.

## Identities

An identity is a set of addresses and phone numbers that represent "me" for a given account. Identity answers the most basic question about any message: did I send this or receive it?

Old imports lose this signal. A Gmail sync knows which messages are sent (SENT label, is_from_me flag). An mbox dump from 2012 does not. Without identity, there's no way to recover it.

Identities are tied to accounts. An account's identity is the set of addresses the user sent from through that account. The same address can appear in multiple accounts' identities -- jesse@gmail.com might be an identity on both a Gmail sync account and an old mbox import of the same mailbox. This is expected and correct.

**Discovery:** `msgvault list-identities` scans an account for likely "me" addresses, ranked by message count with evidence signals (is_from_me, sent-label, account-identifier match). The user reviews, prunes, and confirms.

Identity drives:

- **Sent-copy detection** for dedup survivor selection
- **Inbound/outbound classification** for every message in the account
- **Sender exchange analysis** in the TUI

## Collections

A collection is a named grouping of accounts. It is the unit of "all my communications" in msgvault -- email, messages, transcripts, across multiple accounts, all in one view.

A collection's identity is the union of its accounts' identities.

#### Why "Collection"

In my prototype I called this a "Merged account" and then realized that only mattered when I was creating it, not once I was using it. The user doesn't think "I'm merging accounts." They think "this is all my work email" or "this is everything."

I propose **Collection** as the right name because:

- It describes what the user has, not how they assembled it. "My personal collection." "My work collection."
- It includes accounts without being limited to them. A collection might span a Gmail account, an old mbox import, and a Messenger export. Calling that an "account" is a stretch. Calling it a collection is accurate.
- It's natural at every scale. "All" is a collection. "Work" is a collection.
- Accounts are the individual archives. Collections are the user's organization of them.
- Accounts can be created from exported collections

### The Default Collection

I propose that by default we create an "All" collection exists from the start and automatically includes every account. When a user imports an account, it joins "All" without any extra steps.

For users who want more granularity, additional collections can be created:

```shell
msgvault collections create work --accounts alice@company.com,old-imap-backup
```

1. Groups the named accounts
2. Inherits identity from each account (union of all identity addresses)
3. Runs dedup across the set

Accounts can belong to multiple collections (e.g., an account in both "work" and "All"). Adding accounts later (`collections add work --accounts old-backup`) triggers dedup of the new account against the existing set.

### Exporting a Collection to an Account

A collection can be exported to a single, clean account. This produces a deduplicated, identity-resolved view of the collection as a standalone account -- no duplicates, correct sent/received classification, and unified labels.

```shell
msgvault collections export personal --as personal-archive
```

This is the path from "I assembled a pile of imports" to "I have one authoritative account I trust." The original accounts and collection remain intact.

## Deduplication

### Account-Level Dedup

- `msgvault deduplicate --account alice@gmail.com` -- dedup within a single account
- Runs automatically when importing into an existing account (`--into`)
- Can be run at any time afterward

### Collection-Level Dedup

- `msgvault deduplicate --collection personal` -- dedup across all accounts in the collection
- Runs automatically when creating a collection or adding accounts to one
- Can be run at any time afterward

### Unscoped Dedup

- `msgvault deduplicate` (no flags) -- scans each account independently. Catches re-imports of the same file.

Dedup only operates within the boundary the user specifies. It never implicitly reaches across accounts or collections. Creating a collection that combines multiple accounts is how cross-account dedup works -- the user declares "these are all mine, deduplicate them together."

### Detection

Primary grouping is by RFC822 Message-ID. The engine backfills this header from stored raw MIME for messages ingested before the field was captured. A secondary content-hash pass catches duplicates where re-export rewrote headers (normalized body hash, parallelized).

### Survivor Selection

One message survives per duplicate group, chosen by:

1. Source type preference: gmail > imap > mbox > emlx > hey (configurable)
2. Has raw MIME
3. More labels
4. Earlier archived timestamp
5. Lower row ID

The survivor inherits all labels and raw MIME from pruned copies. Pruned copies are soft-deleted and hidden from all query paths. Every run gets a batch ID; `--undo` reverses it.

### Sent-Message Safety

When any message in a duplicate group looks like a sent copy, only sent copies are eligible survivors. This preserves the "I sent this" signal. Detection uses three signals, OR-combined: Gmail SENT label, is_from_me flag, and identity address match.

### Importing with Dedup

Importing into an existing account or collection deduplicates at ingest time. Messages are hashed against the existing corpus; duplicates are skipped before they enter the database.

```shell
# Import into an existing account (dedup against that account's messages)
msgvault import-mbox --into alice@gmail.com /path/to/old-backup.mbox

# Import into a collection (dedup against all accounts in the collection)
msgvault import-mbox --into personal /path/to/another-backup.mbox
```

Only new messages land. No separate dedup step needed. When `--into` is not specified, import creates a new account as it does today. The user can add it to a collection later.

## Safety

Dedup is a progression from identification to hiding to deletion. Each step is deliberate:

1. **Scan.** Find duplicates, report what would change. No data touched.
2. **Soft-delete.** Pruned copies are hidden from all queries. The data stays in the archive. `--undo` restores everything.
3. **Delete.** Eventually, the user can permanently remove redundant data from the local archive. This is a separate, explicit action -- never automatic, never triggered by dedup itself.
4. **Remote deletion.** Deleting duplicates from source servers (Gmail, IMAP) is yet another separate decision. Defaults to trash (~30-day recovery). Permanent deletion requires explicit opt-in and interactive confirmation.

The user controls every step. The system never escalates from one level to the next without being told to.

Attachment dedup is already handled -- attachments use content-hash addressed storage, so identical files are stored once regardless of how many messages reference them.

## Out of Scope

- Implicit cross-collection dedup (the user must explicitly create a collection to combine accounts)
- Automatic identity inference without user confirmation
