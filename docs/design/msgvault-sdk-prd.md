# msgvault_sdk - Python SDK for msgvault

## Overview

A Python package that exposes an object model over a msgvault SQLite database, enabling programmatic email analysis and manipulation. Users write Python scripts or Jupyter notebooks that import the package to browse, filter, group, sort, and mutate their archived email. All mutations (deletes, label changes) are recorded in a change log table that can later drive replay against the Gmail API.

## Goals

- Expose a Pythonic object model: `Vault` (root) -> `Message` (leaf), with `Participant`, `Label`, `Conversation`, and `Attachment` as related objects.
- Support lazy, chainable queries: `vault.messages.filter(sender="...").filter(before="2020-01-01").sort_by("date")`.
- Support grouping: `vault.messages.group_by("sender")`, `group_by("domain")`, `group_by("year")`, `group_by("label")`, `group_by("recipient")`.
- Record all mutations in a `change_log` table with enough detail to undo locally and replay against Gmail later.
- Integrate with pandas for notebook workflows via `.to_dataframe()`.
- Distribute as a proper Python package in `scripts/msgvault_sdk/`, runnable via `uv`.

## Non-Goals

- No Gmail API calls. The package operates only on the local SQLite database.
- No web UI or server component.
- No migration or schema management for the core msgvault tables (those are owned by the Go binary).
- No write access to `message_raw` or `message_bodies` content (read-only for bodies, no mutations).
- No real-time sync or watch functionality.

## Technical Decisions

### Package name: `msgvault_sdk`

Concise, conventional for a programmatic interface. Importable as `from msgvault_sdk import Vault`.

### Python version: 3.11+

Uses `datetime.fromisoformat()` improvements, `tomllib` (stdlib), and modern type hints. Compatible with current uv.

### Database access: `sqlite3` (stdlib)

The Go codebase uses `mattn/go-sqlite3`. Python's built-in `sqlite3` module reads the same database file with no additional dependencies. The connection opens in WAL mode for concurrent read access alongside the running Go binary.

### No ORM

Lightweight data classes backed by explicit SQL queries. This matches the Go codebase's approach (explicit SQL in store methods, no ORM) and keeps the dependency footprint minimal.

### Lazy query execution

`MessageQuery` objects build SQL incrementally. Queries execute only when results are consumed (iteration, `.count()`, `.first()`, `.to_dataframe()`). This enables chaining without unnecessary DB round-trips.

### Change log table

A new table `change_log` created by the Python package on first mutation. The Go binary does not need to know about it until replay is implemented.

```sql
CREATE TABLE IF NOT EXISTS change_log (
    id INTEGER PRIMARY KEY,
    operation TEXT NOT NULL,       -- 'delete', 'undelete', 'label_add', 'label_remove'
    created_at TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    message_ids TEXT NOT NULL,     -- JSON array of message IDs
    message_count INTEGER NOT NULL,
    details TEXT,                  -- JSON: {"label": "Archive"} or {"label": "INBOX"}
    is_undone INTEGER DEFAULT 0,
    undone_at TEXT,
    undo_data TEXT                 -- JSON: state needed to reverse operation
);
```

### Pandas as optional dependency

Installed via `pip install msgvault_sdk[pandas]` or declared in script metadata. The core package has zero non-stdlib dependencies.

## Design and Operation

### User perspective

```python
from msgvault_sdk import Vault

# Connect to the vault
vault = Vault()                                # ~/.msgvault/msgvault.db
vault = Vault("/path/to/msgvault.db")          # explicit path
vault = Vault.from_config()                    # reads ~/.msgvault/config.toml

# Browse accounts
for acct in vault.accounts:
    print(acct.identifier, acct.source_type)

# Query messages
recent = vault.messages.filter(after="2024-01-01")
noreply = vault.messages.filter(sender_like="%noreply%")
big = vault.messages.filter(min_size=1_000_000)

# Chain filters
targets = (vault.messages
    .filter(sender_like="%noreply%")
    .filter(before="2020-01-01")
    .filter(label="INBOX"))

# Group and inspect
for group in vault.messages.group_by("sender"):
    print(f"{group.key}: {group.count} emails, {group.total_size:,} bytes")

for group in vault.messages.group_by("year"):
    print(f"{group.key}: {group.count} emails")

# Sort
vault.messages.sort_by("date", desc=True)
vault.messages.sort_by("size")
vault.messages.sort_by("sender")

# Access message properties
msg = vault.messages.first()
msg.sender.email        # "john@example.com"
msg.sender.domain       # "example.com"
msg.date                # datetime.datetime
msg.subject             # str
msg.body                # str (lazy-loaded from message_bodies)
msg.labels              # [Label("INBOX"), Label("IMPORTANT")]
msg.recipients          # [Participant(...), ...]
msg.to                  # [Participant(...)] (only 'to' type)
msg.cc                  # [Participant(...)]
msg.attachments         # [Attachment(...)]
msg.size                # int (bytes)

# Mutations (recorded in change log)
targets.delete()                # soft-delete: sets deleted_at
targets.add_label("Archive")   # adds label to matching messages
targets.remove_label("INBOX")  # removes label from matching messages

msg.delete()
msg.add_label("Important")

# Change log
for entry in vault.changelog:
    print(entry.operation, entry.message_count, entry.created_at)

vault.changelog.undo_last()     # reverses the most recent operation

# Pandas integration
df = vault.messages.to_dataframe()
df = vault.messages.filter(after="2024-01-01").to_dataframe()
```

### System perspective

**Query execution flow:**

1. `vault.messages` returns a `MessageQuery` bound to the vault's DB connection.
2. `.filter()`, `.sort_by()` return new `MessageQuery` instances (immutable chaining).
3. Iteration triggers SQL generation and execution.
4. Each `Message` object holds scalar fields from the `messages` table. Related data (body, recipients, labels, attachments) is lazy-loaded on first access via descriptors.

**Mutation flow:**

1. `.delete()` on a query or message starts a transaction.
2. The change log entry is inserted first with the affected message IDs and undo data (e.g., previous `deleted_at` values).
3. The mutation SQL executes (`UPDATE messages SET deleted_at = ...`).
4. Transaction commits atomically.

**Body access:**

Following the Go codebase's separation pattern, `msg.body` and `msg.html_body` are loaded from `message_bodies` only on access, never in bulk queries.

### Error handling

- `VaultNotFoundError` - database file does not exist at the given path.
- `VaultReadOnlyError` - mutation attempted but vault opened read-only.
- `QueryError` - SQL execution failed (wraps sqlite3.Error with context).
- `ChangeLogError` - undo failed (e.g., nothing to undo, entry already undone).

### Edge cases

- **Empty vault** - `vault.messages` returns an empty query; `.first()` returns `None`; `.count()` returns `0`.
- **Concurrent access** - WAL mode allows reads while Go binary writes. Mutations acquire write locks briefly; if the Go binary holds one, sqlite3 raises `OperationalError` which is wrapped with a retry hint.
- **Large result sets** - Iteration uses a cursor (fetches rows incrementally). `.to_dataframe()` loads all matching rows into memory; the user controls scope with filters.
- **Missing FTS5** - The SDK does not depend on FTS5. Full-text search is out of scope (use the Go CLI).
- **Messages with NULL sender** - possible for system messages; `msg.sender` returns `None`.

## Implementation Stages

### Stage 1: Package Foundation and Read-Only Models

**Deliverable:** A `uv`-runnable package that can open a vault and iterate messages with their properties.

- Package scaffold with `pyproject.toml`, source layout, and test infrastructure.
- `Vault` class with DB connection management.
- `Message`, `Participant`, `Label`, `Conversation`, `Attachment` data classes.
- Basic iteration: `for msg in vault.messages` loads messages with eager scalar fields and lazy related data.

### Stage 2: Query System

**Deliverable:** Chainable filter/sort/group queries that generate efficient SQL.

- `MessageQuery` class with immutable chaining (`.filter()`, `.sort_by()`).
- `GroupedQuery` with `.group_by()` returning `Group` objects (key, count, total_size, messages).
- Filter predicates: sender, sender_like, recipient, domain, label, account, before, after, min_size, max_size, has_attachments, subject_like.
- Sort fields: date, sender, subject, size.
- Aggregation methods: `.count()`, `.first()`, `.exists()`.

### Stage 3: Mutations and Change Log

**Deliverable:** Write operations with transactional change logging and undo support.

- `change_log` table creation on first mutation.
- Query-level mutations: `.delete()`, `.add_label()`, `.remove_label()`.
- Message-level mutations: `msg.delete()`, `msg.add_label()`, `msg.remove_label()`.
- `ChangeLog` class with iteration, `.undo_last()`, and `.export_json()`.
- Read-only mode (default) vs. writable mode.

### Stage 4: Data Export, Examples, and Documentation

**Deliverable:** Pandas integration, example scripts, and user documentation.

- `.to_dataframe()` on `MessageQuery` and `GroupedQuery`.
- Example scripts in `scripts/msgvault_sdk/examples/`.
- README with installation, quickstart, and API reference.
- Jupyter notebook example.
