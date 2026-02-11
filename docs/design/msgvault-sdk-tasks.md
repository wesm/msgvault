# msgvault_sdk - Implementation Task List

## Stage 1: Package Foundation and Read-Only Models

**Deliverable:** A `uv`-runnable package that can open a vault and iterate messages with their properties.

### 1.1 Create package scaffold

**Files to create:**

- `scripts/msgvault_sdk/pyproject.toml` — Package metadata, Python >=3.11, zero required dependencies, optional `[pandas]` extra. Entry point: none (library only). Build system: hatchling.
- `scripts/msgvault_sdk/src/msgvault_sdk/__init__.py` — Public API exports: `Vault`, `Message`, `Participant`, `Label`, `Conversation`, `Attachment`.
- `scripts/msgvault_sdk/src/msgvault_sdk/py.typed` — PEP 561 marker for type checking.
- `scripts/msgvault_sdk/tests/__init__.py` — Empty.
- `scripts/msgvault_sdk/tests/conftest.py` — Shared pytest fixtures: `tmp_vault` fixture that creates a temporary SQLite database with the msgvault schema and seed data (a handful of messages, participants, labels, conversations).

**Config in `pyproject.toml`:**

```toml
[project]
name = "msgvault-sdk"
version = "0.1.0"
requires-python = ">=3.11"
dependencies = []

[project.optional-dependencies]
pandas = ["pandas>=2.0"]
dev = ["pytest>=8.0", "pytest-cov"]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["src/msgvault_sdk"]

[tool.pytest.ini_options]
testpaths = ["tests"]
```

**Tests:** Verify the package imports correctly (`from msgvault_sdk import Vault`).

### 1.2 Implement database connection (`db.py`)

**Files to create:**

- `scripts/msgvault_sdk/src/msgvault_sdk/db.py`

**Code:**

- `connect(db_path: str | Path) -> sqlite3.Connection` — Opens SQLite with WAL mode, foreign keys enabled, returns connection. Sets `row_factory = sqlite3.Row` for dict-like access.
- `find_db_path(explicit: str | Path | None = None) -> Path` — Resolution order: (1) explicit argument, (2) `MSGVAULT_HOME` env var + `/msgvault.db`, (3) `~/.msgvault/config.toml` → `data.data_dir`, (4) `~/.msgvault/msgvault.db`. Raises `VaultNotFoundError` if none exist.
- Config parsing: read `config.toml` with `tomllib` (stdlib in 3.11+) to extract `data_dir` and `database_url`.

**Tests** (`tests/test_db.py`):

- `test_connect_opens_wal_mode` — Verifies PRAGMA journal_mode returns 'wal'.
- `test_connect_enables_foreign_keys` — Verifies PRAGMA foreign_keys returns 1.
- `test_find_db_path_explicit` — Explicit path is returned as-is.
- `test_find_db_path_env_var` — `MSGVAULT_HOME` is respected.
- `test_find_db_path_default` — Falls back to `~/.msgvault/msgvault.db`.
- `test_find_db_path_missing` — Raises `VaultNotFoundError`.

### 1.3 Implement error types (`errors.py`)

**Files to create:**

- `scripts/msgvault_sdk/src/msgvault_sdk/errors.py`

**Code:**

- `MsgvaultError(Exception)` — Base class.
- `VaultNotFoundError(MsgvaultError)` — Database file not found. Stores the searched path.
- `VaultReadOnlyError(MsgvaultError)` — Mutation attempted in read-only mode.
- `QueryError(MsgvaultError)` — SQL execution failed. Wraps `sqlite3.Error` with the query and params.
- `ChangeLogError(MsgvaultError)` — Undo or changelog operation failed.

**Tests** (`tests/test_errors.py`):

- Verify each error type is a subclass of `MsgvaultError`.
- Verify `VaultNotFoundError` stores path attribute.
- Verify `QueryError` stores query and original exception.

### 1.4 Implement core data models (`models.py`)

**Files to create:**

- `scripts/msgvault_sdk/src/msgvault_sdk/models.py`

**Code — all are `@dataclass(frozen=True, slots=True)`:**

- `Participant` — Fields: `id: int`, `email: str | None`, `phone: str | None`, `display_name: str | None`, `domain: str | None`. Class method `from_row(row: sqlite3.Row) -> Participant`.

- `Label` — Fields: `id: int`, `name: str`, `label_type: str | None`, `source_id: int | None`. Class method `from_row(row)`.

- `Attachment` — Fields: `id: int`, `message_id: int`, `filename: str | None`, `mime_type: str | None`, `size: int | None`, `content_hash: str | None`, `media_type: str | None`. Class method `from_row(row)`.

- `Conversation` — Fields: `id: int`, `title: str | None`, `conversation_type: str`, `message_count: int`, `last_message_at: datetime | None`. Class method `from_row(row)`.

- `Account` — Fields: `id: int`, `source_type: str`, `identifier: str`, `display_name: str | None`, `last_sync_at: datetime | None`. Class method `from_row(row)`.

- `Message` — Fields: `id: int`, `conversation_id: int`, `source_id: int`, `message_type: str`, `sent_at: datetime | None`, `subject: str | None`, `snippet: str | None`, `is_read: bool`, `is_from_me: bool`, `has_attachments: bool`, `size_estimate: int | None`, `deleted_at: datetime | None`, `sender_id: int | None`. Private `_conn: sqlite3.Connection` field (not frozen — use `__init__` override or a separate mutable wrapper).

  Lazy properties via `@property` with internal caching:
  - `sender -> Participant | None` — Queries `participants` by `sender_id`.
  - `recipients -> list[Participant]` — Queries `message_recipients JOIN participants` for all types.
  - `to -> list[Participant]` — Filters recipients by `recipient_type = 'to'`.
  - `cc -> list[Participant]` — Filters by `'cc'`.
  - `bcc -> list[Participant]` — Filters by `'bcc'`.
  - `body -> str | None` — Queries `message_bodies.body_text` by PK.
  - `html_body -> str | None` — Queries `message_bodies.body_html` by PK.
  - `labels -> list[Label]` — Queries `message_labels JOIN labels`.
  - `attachments -> list[Attachment]` — Queries `attachments` by message_id.
  - `conversation -> Conversation` — Queries `conversations` by conversation_id.
  - `date -> datetime | None` — Alias for `sent_at`.

  Class method `from_row(row: sqlite3.Row, conn: sqlite3.Connection) -> Message`.

**Tests** (`tests/test_models.py`):

- `test_participant_from_row` — Round-trip through a mock Row.
- `test_label_from_row` — Same.
- `test_message_from_row` — Verify scalar fields populate correctly.
- `test_message_lazy_sender` — Insert a participant, create a message with that sender_id, verify `msg.sender` loads it.
- `test_message_lazy_body` — Insert into `message_bodies`, verify `msg.body` returns the text.
- `test_message_lazy_labels` — Insert labels and message_labels, verify `msg.labels` returns them.
- `test_message_lazy_recipients` — Insert message_recipients, verify `msg.to`, `msg.cc`, `msg.recipients`.
- `test_message_sender_none` — Message with NULL sender_id returns `None`.

### 1.5 Implement `Vault` class (`vault.py`)

**Files to create:**

- `scripts/msgvault_sdk/src/msgvault_sdk/vault.py`

**Code:**

- `Vault(db_path: str | Path | None = None, writable: bool = False)` — Constructor. Calls `find_db_path()`, `connect()`. Stores `writable` flag. Opens read-only by default (via `?mode=ro` URI or `PRAGMA query_only = ON`).
- `Vault.from_config(config_path: str | Path | None = None) -> Vault` — Class method. Reads config.toml, extracts db path.
- `vault.accounts -> list[Account]` — Queries `sources` table.
- `vault.messages -> MessageQuery` — Returns a new `MessageQuery` bound to this vault's connection. (MessageQuery is a forward reference, implemented in Stage 2. For Stage 1, return a simple iterator that yields `Message` objects.)
- `vault.close()` — Closes the DB connection.
- Context manager: `__enter__` / `__exit__` for `with Vault() as v:`.

**Tests** (`tests/test_vault.py`):

- `test_vault_opens_db` — `Vault(tmp_db_path)` succeeds, connection is live.
- `test_vault_context_manager` — Connection is closed after `with` block.
- `test_vault_accounts` — Returns seeded accounts.
- `test_vault_messages_iteration` — Iterating `vault.messages` yields `Message` objects.
- `test_vault_not_found` — `Vault("/nonexistent")` raises `VaultNotFoundError`.
- `test_vault_readonly_default` — Mutations raise `VaultReadOnlyError` in default mode.

### 1.6 Update `__init__.py` exports

**Files to modify:**

- `scripts/msgvault_sdk/src/msgvault_sdk/__init__.py`

**Code:**

```python
from msgvault_sdk.vault import Vault
from msgvault_sdk.models import Message, Participant, Label, Conversation, Attachment, Account
from msgvault_sdk.errors import MsgvaultError, VaultNotFoundError, VaultReadOnlyError, QueryError, ChangeLogError

__all__ = [
    "Vault", "Message", "Participant", "Label", "Conversation",
    "Attachment", "Account", "MsgvaultError", "VaultNotFoundError",
    "VaultReadOnlyError", "QueryError", "ChangeLogError",
]
```

### 1.7 Create test fixtures with seed data

**Files to modify:**

- `scripts/msgvault_sdk/tests/conftest.py`

**Code:**

- `schema_sql() -> str` — Reads `internal/store/schema.sql` from the project root (relative path from the test directory).
- `@pytest.fixture` `tmp_vault(tmp_path)` — Creates a temporary SQLite database, applies the schema, inserts seed data:
  - 2 sources (accounts): `test@gmail.com` and `other@gmail.com`.
  - 5 participants: `alice@example.com`, `bob@example.com`, `noreply@service.com`, `admin@example.com`, `test@gmail.com`.
  - 3 labels: `INBOX`, `SENT`, `IMPORTANT`.
  - 2 conversations.
  - 10 messages spanning 2023-2024, with varying senders, sizes, subjects.
  - Message bodies for all 10 messages.
  - Message recipients (to/cc) for all messages.
  - Message-label associations.
  - 2 attachments on 1 message.
  - Returns a `Vault` instance connected to this DB.

---

## Stage 2: Query System

**Deliverable:** Chainable filter/sort/group queries that generate efficient SQL.

### 2.1 Implement `MessageQuery` (`query.py`)

**Files to create:**

- `scripts/msgvault_sdk/src/msgvault_sdk/query.py`

**Code:**

- `MessageQuery` class — Immutable query builder. Internal state: `_conn`, `_filters: tuple[Filter, ...]`, `_sort: tuple[str, bool] | None`, `_limit: int | None`, `_offset: int | None`.
- `MessageQuery(conn, filters=(), sort=None, limit=None, offset=None)` — Constructor.
- `filter(**kwargs) -> MessageQuery` — Returns a new MessageQuery with additional filters. Supported kwargs:
  - `sender: str` — Exact match on `participants.email_address` for sender.
  - `sender_like: str` — LIKE match on sender email.
  - `recipient: str` — Exact match on any recipient email.
  - `recipient_like: str` — LIKE match on recipient.
  - `domain: str` — Exact match on `participants.domain` for sender.
  - `label: str` — Exact match on `labels.name`.
  - `account: str` — Exact match on `sources.identifier`.
  - `before: str | datetime` — `sent_at < value`.
  - `after: str | datetime` — `sent_at >= value`.
  - `min_size: int` — `size_estimate >= value`.
  - `max_size: int` — `size_estimate < value`.
  - `has_attachments: bool` — `has_attachments = value`.
  - `subject_like: str` — LIKE match on `subject`.
  - `is_deleted: bool` — `deleted_at IS NOT NULL` or `IS NULL` (default: False, only non-deleted).
- `sort_by(field: str, desc: bool = False) -> MessageQuery` — Returns new query with sort. Fields: `"date"` (sent_at), `"sender"` (sender email), `"subject"`, `"size"` (size_estimate).
- `limit(n: int) -> MessageQuery` — Returns new query with limit.
- `offset(n: int) -> MessageQuery` — Returns new query with offset.
- `count() -> int` — Executes `SELECT COUNT(*)` with current filters.
- `first() -> Message | None` — Executes query with LIMIT 1.
- `exists() -> bool` — Executes `SELECT EXISTS(...)`.
- `message_ids() -> list[int]` — Returns just the IDs (used internally by mutations).
- `__iter__() -> Iterator[Message]` — Executes the full query, yields `Message` objects.
- `__len__() -> int` — Alias for `count()`.
- `__bool__() -> bool` — Alias for `exists()`.

**SQL generation (private method `_build_sql()`):**

Uses EXISTS subqueries per CLAUDE.md guidelines (no SELECT DISTINCT with JOINs):

```sql
SELECT m.id, m.conversation_id, m.source_id, m.message_type,
       m.sent_at, m.subject, m.snippet, m.is_read, m.is_from_me,
       m.has_attachments, m.size_estimate, m.deleted_at, m.sender_id
FROM messages m
WHERE m.deleted_at IS NULL
  AND EXISTS (
      SELECT 1 FROM participants p
      WHERE p.id = m.sender_id AND p.email_address LIKE ?
  )
  AND EXISTS (
      SELECT 1 FROM message_labels ml
      JOIN labels l ON l.id = ml.label_id
      WHERE ml.message_id = m.id AND l.name = ?
  )
ORDER BY m.sent_at DESC
```

**Tests** (`tests/test_query.py`):

- `test_query_all_messages` — No filters returns all non-deleted messages.
- `test_query_filter_sender` — Exact sender match.
- `test_query_filter_sender_like` — Wildcard sender match.
- `test_query_filter_domain` — Domain filter.
- `test_query_filter_label` — Label filter.
- `test_query_filter_account` — Account filter.
- `test_query_filter_before_after` — Date range filters.
- `test_query_filter_size` — Size range filters.
- `test_query_filter_has_attachments` — Boolean filter.
- `test_query_filter_chained` — Multiple filters combined.
- `test_query_sort_by_date` — Ascending and descending.
- `test_query_sort_by_sender` — Alphabetical by sender email.
- `test_query_sort_by_size` — By size_estimate.
- `test_query_count` — Returns correct count.
- `test_query_first` — Returns first message or None.
- `test_query_exists` — Returns True/False.
- `test_query_limit_offset` — Pagination.
- `test_query_immutable` — `.filter()` returns a new object, original unchanged.
- `test_query_empty_result` — Empty result set handled gracefully.
- `test_query_is_deleted` — Can query deleted messages.

### 2.2 Implement `GroupedQuery` (in `query.py`)

**Code (add to `query.py`):**

- `MessageQuery.group_by(field: str) -> GroupedQuery` — Fields: `"sender"`, `"sender_name"`, `"recipient"`, `"domain"`, `"label"`, `"year"`, `"month"`, `"account"`.
- `GroupedQuery` class — Iterable of `Group` objects.
- `Group` — `@dataclass`: `key: str`, `count: int`, `total_size: int`, `messages: MessageQuery` (lazy — a pre-filtered query for this group's messages).
- `GroupedQuery.__iter__()` — Executes a `GROUP BY` query, yields `Group` objects. Default sort: count descending.
- `GroupedQuery.sort_by(field: str, desc: bool = True)` — Sort groups by `"key"`, `"count"`, or `"total_size"`.

**SQL generation for groups:**

```sql
SELECT p.email_address AS key,
       COUNT(*) AS count,
       COALESCE(SUM(m.size_estimate), 0) AS total_size
FROM messages m
JOIN participants p ON p.id = m.sender_id
WHERE m.deleted_at IS NULL
GROUP BY p.email_address
ORDER BY count DESC
```

(Adjusted per group field — domain uses `p.domain`, year uses `strftime('%Y', m.sent_at)`, label uses a subquery, etc.)

**Tests** (`tests/test_query.py`, additional):

- `test_group_by_sender` — Groups by sender, returns correct keys and counts.
- `test_group_by_domain` — Groups by domain.
- `test_group_by_year` — Groups by year extracted from sent_at.
- `test_group_by_label` — Groups by label name.
- `test_group_messages_lazy` — `group.messages` is a `MessageQuery` that yields the group's messages.
- `test_group_sort_by_count` — Default sort is count descending.
- `test_group_sort_by_key` — Alphabetical sort.
- `test_group_with_filters` — Grouping respects existing query filters.

### 2.3 Wire `Vault.messages` to `MessageQuery`

**Files to modify:**

- `scripts/msgvault_sdk/src/msgvault_sdk/vault.py`

**Code:**

- Replace the Stage 1 simple iterator with `MessageQuery(self._conn)`.
- Add `vault.messages_including_deleted -> MessageQuery` property that sets the `is_deleted=None` default (shows all).

**Tests** (`tests/test_vault.py`, additional):

- `test_vault_messages_returns_query` — `vault.messages` is a `MessageQuery` instance.
- `test_vault_messages_chainable` — `vault.messages.filter(...).sort_by(...)` works.

---

## Stage 3: Mutations and Change Log

**Deliverable:** Write operations with transactional change logging and undo support.

### 3.1 Implement change log schema and manager (`changelog.py`)

**Files to create:**

- `scripts/msgvault_sdk/src/msgvault_sdk/changelog.py`

**Code:**

- `CHANGE_LOG_SCHEMA: str` — The CREATE TABLE statement for `change_log`.
- `ensure_changelog_table(conn: sqlite3.Connection) -> None` — Executes CREATE TABLE IF NOT EXISTS.
- `ChangeEntry` dataclass — `id: int`, `operation: str`, `created_at: datetime`, `message_ids: list[int]`, `message_count: int`, `details: dict | None`, `is_undone: bool`, `undone_at: datetime | None`, `undo_data: dict | None`. Class method `from_row(row)`.
- `ChangeLog` class — Bound to a connection.
  - `__iter__() -> Iterator[ChangeEntry]` — Yields all entries, newest first.
  - `__len__() -> int` — Count of entries.
  - `last() -> ChangeEntry | None` — Most recent entry.
  - `undo_last() -> None` — Undoes the most recent non-undone entry. Raises `ChangeLogError` if nothing to undo. Executes the reverse operation in a transaction.
  - `export_json() -> str` — Exports all entries as JSON (for Go CLI consumption).
  - `_record(operation, message_ids, details, undo_data) -> int` — Internal: inserts a change_log row. Returns the entry ID.

**Undo logic per operation type:**

- `delete` → Sets `deleted_at = NULL` for affected message IDs.
- `undelete` → Sets `deleted_at` to value stored in `undo_data`.
- `label_add` → Removes the label from affected messages (`DELETE FROM message_labels`).
- `label_remove` → Re-adds the label to affected messages (`INSERT INTO message_labels`).

**Tests** (`tests/test_changelog.py`):

- `test_ensure_table_creates` — Table is created on first call.
- `test_ensure_table_idempotent` — Second call does not error.
- `test_record_entry` — Inserts a change log entry with correct fields.
- `test_changelog_iteration` — Multiple entries iterated in reverse chronological order.
- `test_changelog_last` — Returns most recent entry.
- `test_changelog_undo_delete` — Undo restores `deleted_at` to NULL.
- `test_changelog_undo_label_add` — Undo removes the label.
- `test_changelog_undo_label_remove` — Undo re-adds the label.
- `test_changelog_undo_already_undone` — Raises `ChangeLogError`.
- `test_changelog_undo_nothing` — Raises `ChangeLogError` when log is empty.
- `test_changelog_export_json` — Returns valid JSON.

### 3.2 Add mutation methods to `MessageQuery`

**Files to modify:**

- `scripts/msgvault_sdk/src/msgvault_sdk/query.py`

**Code (add to `MessageQuery`):**

- `delete() -> int` — Soft-deletes all matching messages. Records in change log. Returns count of affected messages. Raises `VaultReadOnlyError` if vault is read-only.
- `add_label(name: str) -> int` — Adds a label to all matching messages. Creates the label in `labels` table if it doesn't exist (with `label_type='user'`). Records in change log. Returns count.
- `remove_label(name: str) -> int` — Removes a label from matching messages. Records in change log. Returns count.

**Implementation detail:** Each mutation:
1. Checks `writable` flag on vault.
2. Calls `ensure_changelog_table()`.
3. Collects `message_ids()` for the current query.
4. Begins a transaction.
5. Records the change log entry (with undo data).
6. Executes the mutation SQL.
7. Commits.

**Tests** (`tests/test_mutations.py`):

- `test_delete_messages` — Marks messages as deleted, records in change log.
- `test_delete_already_deleted` — No-op for already-deleted messages (count = 0).
- `test_add_label` — Adds label to messages, creates label if needed.
- `test_add_label_existing` — Uses existing label, doesn't duplicate.
- `test_remove_label` — Removes label from messages.
- `test_remove_label_not_present` — No-op, count = 0.
- `test_mutation_readonly_raises` — Raises `VaultReadOnlyError`.
- `test_mutation_changelog_recorded` — Change log entry exists after mutation.
- `test_mutation_undo_delete` — Delete then undo restores messages.
- `test_mutation_undo_add_label` — Add label then undo removes it.
- `test_mutation_undo_remove_label` — Remove label then undo re-adds it.
- `test_mutation_transaction_atomicity` — If mutation SQL fails, change log entry is also rolled back.

### 3.3 Add mutation methods to `Message`

**Files to modify:**

- `scripts/msgvault_sdk/src/msgvault_sdk/models.py`

**Code (add to `Message`):**

- `delete() -> None` — Soft-deletes this single message. Delegates to a single-message query mutation.
- `add_label(name: str) -> None` — Adds label to this message.
- `remove_label(name: str) -> None` — Removes label from this message.

These are convenience wrappers that create a single-message query and call the corresponding `MessageQuery` method.

**Tests** (`tests/test_mutations.py`, additional):

- `test_message_delete` — Single message soft-deleted.
- `test_message_add_label` — Label added to single message.
- `test_message_remove_label` — Label removed from single message.

### 3.4 Wire `Vault.changelog`

**Files to modify:**

- `scripts/msgvault_sdk/src/msgvault_sdk/vault.py`

**Code:**

- `vault.changelog -> ChangeLog` — Property that returns a `ChangeLog` bound to the vault's connection.
- Pass `writable` and `changelog` references through to `MessageQuery` and `Message` objects.

**Tests** (`tests/test_vault.py`, additional):

- `test_vault_changelog` — Returns `ChangeLog` instance.
- `test_vault_writable_mutations` — `Vault(path, writable=True)` allows mutations.

---

## Stage 4: Data Export, Examples, and Documentation

**Deliverable:** Pandas integration, example scripts, and user documentation.

### 4.1 Implement `.to_dataframe()` (`dataframe.py`)

**Files to create:**

- `scripts/msgvault_sdk/src/msgvault_sdk/dataframe.py`

**Code:**

- `query_to_dataframe(query: MessageQuery) -> pd.DataFrame` — Executes the query, builds a DataFrame with columns: `id`, `date`, `sender`, `sender_domain`, `subject`, `snippet`, `size`, `has_attachments`, `is_read`, `labels` (comma-separated string), `to` (comma-separated), `account`.
- `groups_to_dataframe(grouped: GroupedQuery) -> pd.DataFrame` — Builds a DataFrame with columns: `key`, `count`, `total_size`.
- Raises `ImportError` with a helpful message if pandas is not installed.

**Files to modify:**

- `scripts/msgvault_sdk/src/msgvault_sdk/query.py` — Add `to_dataframe()` method to `MessageQuery` and `GroupedQuery` that delegates to `dataframe.py`.

**Tests** (`tests/test_dataframe.py`):

- `test_query_to_dataframe` — Returns a DataFrame with expected columns and row count.
- `test_query_to_dataframe_with_filters` — Filtered query produces filtered DataFrame.
- `test_groups_to_dataframe` — Grouped query produces summary DataFrame.
- `test_to_dataframe_no_pandas` — Raises clear `ImportError` when pandas missing.

### 4.2 Add `__repr__` and display helpers

**Files to modify:**

- `scripts/msgvault_sdk/src/msgvault_sdk/models.py` — Add `__repr__` to `Message`, `Participant`, `Label`, `Conversation`, `Attachment` that show key identifying fields.
- `scripts/msgvault_sdk/src/msgvault_sdk/query.py` — Add `__repr__` to `MessageQuery` showing filter summary and count. Add `_repr_html_()` to `MessageQuery` for Jupyter rendering (shows an HTML table of first 10 results).

### 4.3 Create example scripts

**Files to create:**

- `scripts/msgvault_sdk/examples/list_top_senders.py` — Lists top 20 senders by message count. Demonstrates `group_by("sender")`, sorting, iteration.

- `scripts/msgvault_sdk/examples/cleanup_noreply.py` — Finds and optionally deletes all messages from noreply senders. Demonstrates `filter(sender_like=...)`, `count()`, `delete()`, change log.

- `scripts/msgvault_sdk/examples/analyze_volume.py` — Shows message volume by year and month. Demonstrates `group_by("year")`, `group_by("month")`, and pandas DataFrame export.

- `scripts/msgvault_sdk/examples/find_large_messages.py` — Finds messages over a given size threshold. Demonstrates `filter(min_size=...)`, `sort_by("size", desc=True)`, `limit()`.

Each example script includes PEP 723 inline metadata:

```python
# /// script
# requires-python = ">=3.11"
# dependencies = ["msgvault-sdk"]
# ///
```

Runnable via: `uv run examples/list_top_senders.py`

### 4.4 Write README

**Files to create:**

- `scripts/msgvault_sdk/README.md`

**Sections:**

- **Installation** — `uv` setup, `pip install -e .`, dev setup with `uv sync --extra dev`.
- **Quickstart** — 10-line example showing open vault, filter, iterate, print.
- **API Reference** — Brief docs for `Vault`, `MessageQuery`, `GroupedQuery`, `Message`, mutations, change log.
- **Examples** — Links to example scripts with one-line descriptions.
- **Running scripts with uv** — How to use PEP 723 inline metadata.
- **Development** — How to run tests (`uv run pytest`).
