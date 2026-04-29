# Design Alignment: Accounts, Collections, Identities, and Deduplication

**Status:** Scope-first design alignment. This document intentionally ignores the
current PR #286 implementation shape and defines the model we want before
deciding how to modify the branch.

**Supersedes:**

- [`2026-04-25-account-collection-model-alignment.md`](./2026-04-25-account-collection-model-alignment.md) — replaces the implementation-detail-driven model alignment.
- [`2026-04-20-deduplicate-command.md`](./2026-04-20-deduplicate-command.md) — the original product framing. Its safety progression and product motivation are folded into this document; consult the original only for historical context.

**Discussion sources:**

- [Issue #278: Enhancement Proposal: Identities, Collections, and Deduplication](https://github.com/wesm/msgvault/issues/278)
- [PR #286: Identities, Collections, and Deduplication](https://github.com/wesm/msgvault/pull/286)
- [Wes's design review comment](https://github.com/wesm/msgvault/pull/286#issuecomment-4320039075)

## Purpose

msgvault can already ingest communications from many places. The design problem
is what happens after ingestion: users need to organize sources, identify which
addresses represent them, remove redundant local copies from normal search and
analysis, and preserve clear safety boundaries when data came from independent
archives.

The central alignment point from the discussion is this:

> Account/source is the atomic ingest unit. Collection is the explicit grouping
> boundary. Cross-account behavior happens only through collections.

This spec defines that model independent of the current implementation. PR #286
can then be reshaped to match the model, split into smaller PRs, or narrowed to a
foundation subset. The implementation path is secondary; the scope and semantics
come first.

## Feature Introduction Draft

msgvault is good at getting communications into one archive. Over time, that
archive can become a pile of overlapping sources: a current Gmail sync, an old
mbox export, Apple Mail from a retired laptop, IMAP backups, chat exports, SMS
history, and meeting transcripts. Each source is valuable, but the combined
archive can be hard to trust when the same message appears several times, old
imports no longer know which messages were sent by the user, and searches or
counts are dominated by duplicate copies.

## Accounts, Identities, and Collections

This feature introduces three related concepts that make the archive usable
without erasing provenance.

### Accounts

An **account** is one imported message source/archive. A Gmail sync is an
account. An mbox import is an account. An Apple Mail import is an account.

Even if two accounts represent the same real-world mailbox, msgvault keeps
them separate until the user groups them. That preserves the boundary between
independent archives and avoids guessing that two sources should be merged
just because their data overlaps.

### Identities

An **identity** identifies the owner of an account: the set of addresses,
phone numbers, or provider identifiers that mean "me" inside that account.
An account has one identity, which may contain many identifiers.

Identities are stored per account because the meaning of an address depends
on the source. A collection's identity is derived from its member accounts.
This lets msgvault recover sent/received meaning in old imports and make
safer deduplication choices without applying one global identity list to
every archive.

### Collections

A **collection** is the user's explicit grouping of accounts. `All` is the
default collection containing every account, and users can create
collections such as `work`, `personal`, or `old laptop imports`.

Collections are how msgvault offers a unified view across sources while
keeping source provenance intact. Any operation that crosses account
boundaries does so because the user selected a collection.

The name "collection" matters. An earlier prototype used "merged
account," which only described the moment of assembly, not the thing the
user actually has afterward. Users don't think "I merged accounts." They
think "this is all my work email" or "this is everything." A collection
describes what the user has, not how they assembled it. It works at every
scale — `All` is a collection, `work` is a collection — and it doesn't
overload the word "account," which already means one ingest source.

A future revision may introduce other identity or collection types (for
example, device identities or saved-query collections). If that happens,
renaming these objects to `account_identity` and `account_collection` would
disambiguate. For now, `identity` and `collection` are unambiguous.

![Accounts and collections concept diagram](./assets/account-collection-concept.png)

## Deduplication

**Deduplication** then works inside those boundaries. `deduplicate --account`
cleans up repeated rows within one source. `deduplicate --collection` compares
messages across the accounts the user deliberately grouped. `deduplicate` with no
scope processes each account independently. Applying dedup hides redundant local
copies from normal search, browse, stats, API, MCP, and vector/hybrid retrieval
paths. It does not destroy data: hidden copies stay on disk and `--undo`
restores them. Hard-deleting locally and deleting from a source server are
separate, explicit steps the user must take on top of dedup; the system
never escalates from one to the next on its own.

The result is a model that supports both safety and usefulness: users can search
and analyze a unified communication archive, but msgvault still knows which source
each message came from, which identities apply to which account, and when an
operation is crossing account boundaries.

![Deduplication concept diagram](./assets/deduplication-concept.png)



## Design Decisions From The Discussion


| Topic                  | Discussion Evidence                                                                                                                                                                                                                                                                                   | Resolution                                                                                                                                         |
| ------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| Product need           | Issue #278: "The same email appears three times because it arrived via Gmail sync, an old mbox export, and an Apple Mail import." ([source](https://github.com/wesm/msgvault/issues/278))                                                                                                             | msgvault needs explicit organization and dedup tools for archives assembled from many independent imports.                                         |
| Account meaning        | Wes: "**Account**: one ingest source/archive." ([source](https://github.com/wesm/msgvault/pull/286#issuecomment-4320039075))                                                                                                                                                                          | An account is one source/archive created by one ingest path. It is not a logical mailbox spanning sources.                                         |
| Collection meaning     | Issue #278: "A collection is a named grouping of accounts." ([source](https://github.com/wesm/msgvault/issues/278)) Wes: "**Collection**: a named grouping of accounts/sources." ([source](https://github.com/wesm/msgvault/pull/286#issuecomment-4320039075))                                        | A collection is a named grouping of account/source IDs. It is the only supported cross-account grouping primitive.                                 |
| Default collection     | Issue #278: "create an 'All' collection ... automatically includes every account." ([source](https://github.com/wesm/msgvault/issues/278)) Wes: "**All**: the default collection containing every account/source." ([source](https://github.com/wesm/msgvault/pull/286#issuecomment-4320039075))      | `All` exists by default and includes every account/source. It is a collection, not an account.                                                     |
| Dedup safety boundary  | Issue #278: "Dedup only operates within the boundary the user specifies." ([source](https://github.com/wesm/msgvault/issues/278)) Wes: "Dedup across accounts should require an explicit collection boundary." ([source](https://github.com/wesm/msgvault/pull/286#issuecomment-4320039075))          | Dedup within one account never crosses source boundaries. Cross-account dedup requires an explicit collection scope.                               |
| Unscoped dedup         | Issue #278: "`msgvault deduplicate` (no flags) -- scans each account independently." ([source](https://github.com/wesm/msgvault/issues/278)) Wes: "`msgvault deduplicate`: each account/source independently." ([source](https://github.com/wesm/msgvault/pull/286#issuecomment-4320039075))          | Unscoped dedup is per-account iteration. It is not shorthand for "dedup everything together."                                                      |
| CLI scope clarity      | Wes: "keep `--account` restricted to one account/source, and add an explicit `--collection` flag." ([source](https://github.com/wesm/msgvault/pull/286#issuecomment-4320039075))                                                                                                                      | User-facing flags must encode the boundary: `--account` means one source; `--collection` means a named group.                                      |
| Name shadowing         | Wes: "`--account work` may target a collection named `work`, not an account/source." ([source](https://github.com/wesm/msgvault/pull/286#issuecomment-4320039075))                                                                                                                                    | A collection name must never be accepted through `--account`. Ambiguous names produce explicit errors with the correct flag hint.                   |
| Nested collections     | Wes: collection creation can "effectively allow nested collection references." ([source](https://github.com/wesm/msgvault/pull/286#issuecomment-4320039075))                                                                                                                                          | Collections contain accounts/sources only. Nested collections are not part of this model.                                                          |
| Identity scope         | Issue #278: "Identities are tied to accounts." ([source](https://github.com/wesm/msgvault/issues/278)) Wes: global `[identity]` config is "a different model." ([source](https://github.com/wesm/msgvault/pull/286#issuecomment-4320039075))                                                          | Identities are stored as per-account records. A global identity list is legacy input at most, not an active scope model.                           |
| Collection identity    | Issue #278: "A collection's identity is the union of its accounts' identities." ([source](https://github.com/wesm/msgvault/issues/278))                                                                                                                                                               | A collection identity is derived from member accounts. It is not separately configured as its own identity set.                                    |
| Query hiding contract  | Issue #278: "Pruned copies are soft-deleted and hidden from all query paths." ([source](https://github.com/wesm/msgvault/issues/278)) Wes: vector/hybrid paths can still "surface pruned duplicates." ([source](https://github.com/wesm/msgvault/pull/286#issuecomment-4320039075))                   | If dedup hides a row, every normal read surface must treat it as hidden. This is a product contract, not just a query optimization.                |
| Live-message predicate | Wes: "Centralize live-message filtering" across storage and retrieval surfaces. ([source](https://github.com/wesm/msgvault/pull/286#issuecomment-4320039075))                                                                                                                                         | Define one live-message rule and apply it consistently across SQLite, DuckDB, FTS, vector, API, MCP, TUI, and stats.                               |
| Collection query scope | Wes: decide whether collections are "only a dedup/admin concept" or "first-class query scopes." ([source](https://github.com/wesm/msgvault/pull/286#issuecomment-4320039075))                                                                                                                         | Collections should be first-class user scopes for search, browse, stats, and dedup. A collection that only works for administration is incomplete. |
| Cache/index policy     | Wes: "Cache/index invalidation needs a clearer policy." ([source](https://github.com/wesm/msgvault/pull/286#issuecomment-4320039075))                                                                                                                                                                 | Correctness comes from live-message filtering. Rebuilds improve storage/performance hygiene but must not be required to hide pruned duplicates.    |
| Schema ownership       | Wes: permanent collections "probably belong in the canonical schema/migration path." ([source](https://github.com/wesm/msgvault/pull/286#issuecomment-4320039075))                                                                                                                                    | Accounts, collections, identities, and dedup metadata are core data model concepts and belong in canonical schema/migrations.                      |
| Undo semantics         | Wes: "Undo is not a full rollback." ([source](https://github.com/wesm/msgvault/pull/286#issuecomment-4320039075))                                                                                                                                                                                     | Undo restores local visibility and pending deletion intent where possible. It is not a guarantee of exact pre-run database state.                  |
| Remote deletion scope  | Wes: manifest naming and reporting need to remain "source/account-specific rather than collection-specific." ([source](https://github.com/wesm/msgvault/pull/286#issuecomment-4320039075))                                                                                                            | Remote deletion remains source-scoped even when duplicate detection used a collection boundary.                                                    |
| Spec scope             | Wes: distinguish "what is implemented now from the longer-term proposal." ([source](https://github.com/wesm/msgvault/pull/286#issuecomment-4320039075)) Jesse: "Let me take another pass at clarifying the spec first." ([source](https://github.com/wesm/msgvault/pull/286#issuecomment-4320385497)) | The design should define the target model first, then identify an implementation slice.                                                            |

## Core Model

### Account

An account is one ingest source/archive. It is the smallest durable provenance
unit in msgvault.

Examples:

- one Gmail sync source
- one IMAP source
- one mbox import
- one Apple Mail import
- one iMessage import
- one SMS import
- one Facebook Messenger import
- one meeting transcript import source

The same real-world mailbox imported through Gmail sync and later through an old
mbox export creates two accounts. They may represent the same human mailbox, but
they are distinct archives with distinct provenance and source-specific deletion
semantics.

This keeps the data model honest: msgvault does not infer that two imports belong
together just because an email address, display name, or message content overlaps.

### Collection

A collection is a named grouping of accounts. It is the user's explicit statement
that multiple sources should be viewed or operated on together.

Examples:

- `All`
- `work`
- `personal`
- `old laptop imports`
- `gmail plus exports`
- `family messages`

Collections are many-to-many:

- An account can belong to multiple collections.
- A collection can contain multiple accounts.
- A collection contains account/source IDs, not other collections.

Collections are the boundary for cross-account features. If two independent
archives should be searched, counted, deduplicated, or exported together, the user
expresses that by putting them in a collection.

### All

`All` is the default collection containing every account/source.

`All` gives users a natural unified view without collapsing account provenance.
It is still a collection. Operations against `All` are collection-scoped
operations and should be displayed that way.

## Scope Semantics

The user-facing scope vocabulary is deliberately small:


| Scope            | Meaning                                                 |
| ------------------ | --------------------------------------------------------- |
| Account scope    | One source/archive.                                     |
| Collection scope | All member accounts of one collection.                  |
| All scope        | The default collection containing every account/source. |

CLI flags should expose those boundaries directly:


| Command shape               | Meaning                                                                                                  |
| ----------------------------- | ---------------------------------------------------------------------------------------------------------- |
| `--account <account>`       | Resolve exactly one account/source.                                                                      |
| `--collection <collection>` | Resolve exactly one collection.                                                                          |
| no flag where supported     | Use the command's documented default, such as per-account iteration for dedup or`All` for search/browse. |

`--account` and `--collection` are mutually exclusive. A generic internal
`Scope` type is useful, but generic user-facing flags are not. Users should be
able to tell when they are crossing source boundaries by reading the command.

Name conflicts should fail clearly:

- If `work` is a collection, `--account work` fails and suggests
  `--collection work`.
- If `alice@example.com` is an account, `--collection alice@example.com` fails
  and suggests `--account alice@example.com`.
- If the same string exists as both an account display name and a collection
  name, the user must choose the correct flag and may need a more specific
  account identifier.

## Identity Model

Identity answers: "Who am I in this source?"

An identity belongs to an account/source. It can include email addresses, phone
numbers, or other protocol-specific identifiers. A confirmed identity means that
messages from that address or identifier can be treated as "from me" within that
account's context.

Identity is account-scoped for two reasons:

- The same address may appear in multiple imports, and that is expected.
- An address that is safe to treat as "me" in one account may be misleading in
  another account or shared archive.

A collection's identity is derived from its member accounts. It is the union of
confirmed identities from those accounts, used only within the collection's
scope.

Identity discovery should be evidence-based and reviewable. Candidate signals
include:

- `is_from_me` metadata from ingest
- sent-folder or sent-label evidence
- account/source identifier match
- OAuth or provider account metadata
- user confirmation

Global identity configuration is not part of the target model.

If a `[identity]` block exists in `config.toml` from an older msgvault
version, the first startup after upgrade migrates its addresses into
per-account confirmed identities for every existing account, logs a
warning naming the migration, and prints a one-time CLI notice asking
the user to review per-account identities (`msgvault list-identities`).
The global config block is then no longer read. This preserves the
old behavior across the upgrade while moving every account onto the
per-account model.

After migration, identity is stored per account. The global config block
is legacy input only.

## Collection Behavior

Collections are a primary user concept, not just a dedup helper.

Required behavior:

- `All` is created and maintained automatically.
- Users can create named collections from accounts.
- Users can add and remove accounts from collections.
- Collection membership accepts only accounts/sources.
- Collection views preserve account provenance.

Out of scope for the core model:

- Nested collections.
- Implicit collection creation based on matching email addresses.
- Treating a collection as an account.

Collection names and account identifiers can share human-friendly names, so the
CLI and UI must preserve the distinction visually and behaviorally.

## Deduplication Model

Deduplication removes redundant local copies from normal user-facing results
without destroying the underlying archive by default.

### Valid Dedup Scopes


| Invocation                              | Boundary                                                    |
| ----------------------------------------- | ------------------------------------------------------------- |
| `deduplicate --account <account>`       | Compare messages only within that account/source.           |
| `deduplicate --collection <collection>` | Compare messages across member accounts in that collection. |
| `deduplicate`                           | Process each account independently.                         |

The unscoped form is a convenience for per-account cleanup. It must not compare
all messages across all accounts as one global set.

The unscoped default is per-account iteration rather than `--collection All`
because cross-account dedup is the higher-risk operation: it can collapse
duplicates between independent archives whose provenance the user may want
to preserve. Cross-account dedup should require explicit opt-in through
`--collection`. A user who genuinely wants to dedup across every account can
still write `--collection All`.

### Detection

Duplicate detection can use multiple signals:

- RFC822 `Message-ID`
- normalized raw MIME or body content hash
- provider/source message IDs where appropriate
- attachment content hashes where relevant

Detection signals should be merged into duplicate groups carefully. A content-hash
match can connect messages that do not share a `Message-ID`, and a `Message-ID`
match can connect messages with slightly different stored bodies. The grouping
model should allow transitive duplicate sets rather than treating each signal as
an isolated pass.

### Survivor Selection

Survivor selection should be deterministic and explainable. The policy
prefers the copy that is most useful as the durable representative,
evaluated in this priority order:

1. source preference when configured
2. has raw MIME or complete original payload
3. source metadata quality
4. richer label or folder metadata
5. earlier archived timestamp when meaningful
6. stable row ID as the final tie-breaker

Earlier rules win outright; later rules only apply when all earlier ones
tie. The exact policy should be documented and visible in dry-run output,
so a user can read why one copy survived and another was hidden.

#### Sent-Message Safety Rule

Sent-copy safety is an **eligibility filter**, not a tie-breaker. When any
message in a duplicate group looks like a sent copy, only sent copies are
eligible to survive. Received-copy candidates are removed from the group
before the priority list above runs. Losing the sent signal silently
changes user interpretation of the archive — "I sent this" is harder to
recover than "I received this."

A message looks like a sent copy when any of these signals fires (OR):

- a Gmail `SENT` label on the message
- an `is_from_me` flag on the message from ingest metadata
- the `From` address matches a confirmed identity for the message's
  account

### Effects

Applying dedup should:

- choose one survivor per duplicate group
- hide redundant local rows from normal query paths
- preserve enough metadata to explain what happened
- write a batch ID for audit and undo
- avoid remote deletion unless explicitly requested

Dedup should not silently escalate from local hiding to local hard deletion or
remote deletion.

## Safety Progression

Dedup is a ladder, not a single switch. Each rung is a separate, explicit
user action. The system never escalates from one rung to the next on its
own.

1. **Scan.** Detect duplicates and report what would change. No data is
   touched. Dry-run is the default.
2. **Hide.** Apply dedup. Pruned copies are soft-deleted: hidden from
   normal reads but kept on disk. `--undo <batch-id>` restores visibility.
3. **Local hard delete.** A separate, opt-in action that permanently
   removes hidden rows from the local archive. Dedup itself never does
   this; the user runs it explicitly after a hide step they're confident
   in.
4. **Remote delete.** Deleting from the source server (Gmail, IMAP,
   another service) is a further separate decision. The default is
   trash-with-recovery (Gmail's ~30-day trash). Permanent remote deletion
   requires explicit opt-in and interactive confirmation.

The user controls every rung. "Apply dedup" never implies hard delete.
"Hard delete locally" never implies remote delete. "Remote delete" never
implies permanent remote delete.

Attachment dedup is independent of message dedup: attachments are stored
in a content-addressed pool, so identical files are stored once
regardless of how many messages reference them. Hiding or hard-deleting a
duplicate message does not delete the underlying attachment blob unless
no remaining message references it.

## Live-Message Contract

A **live message** is a message that has not been locally hidden by dedup
and has not been recorded as deleted from the source server. The term is
internal vocabulary for this contract and shows up in implementation slices
and code.

Normal user-facing reads should return live messages only.

This contract applies to:

- message search
- vector and hybrid search
- TUI browsing
- stats and aggregates
- API responses
- MCP responses
- exports that claim to represent the visible archive

Indexes and caches may lag behind SQLite state, but normal retrieval must still
filter hidden rows. Rebuilding derived surfaces is valuable for size and
performance; it should not be the only thing preventing hidden duplicates from
appearing.

## Query Scope

Collections should be first-class query scopes.

If users can create `work` or `personal`, they should be able to search, browse,
count, and inspect those collections without learning which source IDs are inside.
That applies across local search, vector/hybrid search, TUI, API, MCP, and stats.

The scope model should produce the same result set regardless of retrieval
backend:

- account scope maps to one source ID
- collection scope maps to many source IDs
- `All` maps to every source ID

Backend differences are acceptable for ranking or performance, but not for scope
membership or live-message visibility.

## Cache And Index Policy

The product contract is:

- Dedup changes the canonical archive state.
- Normal reads hide rows that are no longer live.
- Derived indexes may be rebuilt, updated, or marked stale as an operational
  concern.

Recommended policy:

- Filtering is mandatory for correctness.
- Best-effort derived index cleanup is allowed.
- Manual rebuild commands remain available.
- Any known stale derived surface should be visible in command output or logs.

This avoids coupling dedup correctness to every cache and index implementation.

## Undo Model

Undo is not full time travel.

Undo should restore local visibility for rows hidden by a dedup batch and cancel
pending remote deletion manifests when they have not executed. It should not
promise to reverse every side effect of dedup, such as survivor label unioning,
raw-MIME enrichment, index cleanup, or remote deletion already performed against a
source service.

Canonical user-facing language:

> `--undo <batch-id>` restores rows hidden by that dedup batch and cancels the
> batch's pending remote-deletion manifest where possible. It does not restore an
> exact pre-run database state.

## Remote Deletion Model

Remote deletion is a separate operation from local dedup.

Even when duplicate detection runs across a collection, remote deletion
decisions remain source-specific. It is only valid to stage remote
deletion when the survivor and loser belong to the **same source** and
that source supports the requested remote-deletion behavior.

Rules:

- **Same-source constraint.** A remote-deletion entry is only staged when
  the loser and the survivor share a `source_id`. Cross-source duplicate
  groups produce no remote-deletion entries even when the dedup scope is
  a collection that spans those sources.
- **Source-scoped manifests.** Remote-deletion manifests, manifest
  filenames, and reporting labels reflect the source, never the
  collection name, even when dedup was invoked under `--collection`.
- **Trash by default.** Where the source supports a trash or recoverable
  state (e.g. Gmail's ~30-day trash), the default remote-deletion
  behavior moves messages there rather than removing them outright.
- **Permanent deletion is opt-in.** Permanent remote deletion requires an
  explicit flag and interactive confirmation. It is never the default,
  never inferred from dedup, and never applied in batch without the user
  acknowledging the source and scope at the moment of the action.

This preserves the distinction between "hide this redundant local row,"
"hard-delete it from the local archive," and "delete something from
Gmail / IMAP / another source service."

## Schema And Persistence

Accounts, collections, identities, dedup batches, and deletion manifests are core
domain concepts. Their durable state belongs in canonical schema and migrations,
with dialect-aware ownership where msgvault supports multiple database engines.

The target model needs durable storage for:

- collection definitions
- collection membership
- account-scoped identity records
- dedup batches
- hidden duplicate row metadata
- remote deletion manifests or manifest references

Ad hoc lazy table creation is acceptable only as a development bridge, not as the
settled architecture for these concepts.

## Product Scope

### Core Scope

These concepts belong together and should be designed as one coherent model:

- Account/source as one ingest unit.
- Collection as explicit grouping.
- Default `All` collection.
- Account-scoped identities, with one-time migration from any legacy
  global identity config.
- Collection identity as derived union.
- Account-scoped dedup.
- Collection-scoped dedup.
- Sent-message safety as a survivor eligibility filter, not a tie-breaker.
- Live-message filtering across normal reads.
- Safety progression of scan → hide → local hard delete → remote delete,
  with no automatic escalation between rungs.
- Undo as local visibility restore, not full rollback.
- Remote deletion as explicit source-scoped follow-up, same-source-only,
  trash-by-default with permanent deletion behind interactive confirmation.

### Implementation Slices

The implementation does not have to land all at once. Reasonable slices are:

- **Model and CLI scope:** vocabulary, `--account`/`--collection`, `All`, no nested collections.
- **Read scope and visibility:** live-message predicate, collection query scope, backend consistency.
- **Dedup application:** account and collection dedup, survivor policy, batch audit, undo language.
- **Identity persistence:** per-account identity records, discovery,
  confirmation, and collection union.
- **Remote deletion:** source-scoped manifests and collection-scope safety tests.

These slices should preserve the model even if delivered separately. A partial
slice should not introduce user-facing semantics that contradict the target
design.

### Future Product Work

These are valuable but do not need to define the first aligned implementation:

- Import-time dedup with `--into`.
- Automatic dedup when creating or adding to collections.
- Exporting a deduplicated collection into a clean account. Once a
  collection has been deduplicated, its survivors form a coherent unified
  view across the member sources. A future operation should be able to
  export those survivors into a single new account that becomes the
  canonical archive going forward, while the original member accounts
  remain intact for provenance. This gives users a path from "many
  overlapping imports" to "one clean source of truth" without forcing them
  to throw away the originals.
- Identity-derived inbound/outbound classification across historical imports.
- Rich identity review UI.
- Policy controls for source preference and survivor scoring.

## Mapping To The Current Branch

This spec is forward-looking in framing but most of the model is already
implemented on the PR #286 branch. Recording the mapping here so the gap
between the target model and the shipped code is visible:

- **Already on the branch:** account-as-source vocabulary, `--account` and
  `--collection` flags on dedup, default `All` collection bootstrap,
  per-account dedup, collection-scope dedup, sent-message safety in
  survivor selection, undo as local-visibility restore, source-scoped
  remote-deletion manifests with same-source-only staging, and
  per-account identity records via `list-identities`.
- **Partial:** live-message filtering (applied to SQLite and DuckDB read
  paths; coverage across vector/hybrid retrieval and MCP responses still
  needs an audit), name-collision errors between accounts and collections
  (basic guards in place; full ambiguity-suggestion UX is not), and
  collections as first-class query scopes outside dedup.
- **Not yet on the branch:** identity discovery beyond ingest metadata,
  identity confirmation UX, derived collection identity used at read time,
  policy controls for survivor scoring, the explicit local hard-delete
  rung of the safety progression, and the legacy `[identity]` config
  migration on first startup after upgrade.

The implementation slices in the next section can be applied to the
existing branch incrementally rather than as a single reshape.

## Scope Review Checklist

Use this checklist before translating the design back into implementation tasks:

- Does "account" always mean one ingest source/archive?
- Is every cross-account operation expressed through a collection?
- Can users tell from the command or UI when they are crossing account/source
  boundaries?
- Are identities account-scoped rather than global, with a defined
  migration from any legacy global config?
- Is `All` modeled as a collection?
- Are collections first-class query scopes?
- Are hidden duplicates excluded from every normal read path by contract?
- Does dedup honor sent-message eligibility before falling back to the
  survivor priority list?
- Does dedup keep scan / hide / local hard delete / remote delete as four
  separate user actions, with no automatic escalation between them?
- Does remote deletion stay same-source-only, trash-by-default, and
  require explicit confirmation for permanent removal?
- Does undo avoid promising exact rollback?
- Are implementation slices allowed only when they preserve these semantics?
