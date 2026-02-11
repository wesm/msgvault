"""Query builder for msgvault_sdk."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterator

from msgvault_sdk.changelog import ChangeLog
from msgvault_sdk.errors import VaultReadOnlyError
from msgvault_sdk.models import Message

# Column list used in all message SELECT queries
_MSG_COLUMNS = (
    "m.id, m.conversation_id, m.source_id, m.message_type, m.sent_at, "
    "m.subject, m.snippet, m.is_read, m.is_from_me, m.has_attachments, "
    "m.size_estimate, m.deleted_at, m.sender_id"
)

# Valid sort fields mapped to SQL expressions
_SORT_FIELDS = {
    "date": "m.sent_at",
    "sender": (
        "(SELECT p.email_address FROM participants p WHERE p.id = m.sender_id)"
    ),
    "subject": "m.subject",
    "size": "m.size_estimate",
}


@dataclass(frozen=True, slots=True)
class _Filter:
    """A single filter predicate."""

    clause: str
    params: tuple[Any, ...]


def _to_iso(value: str | datetime) -> str:
    """Convert a date value to an ISO string for SQLite comparison."""
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%dT%H:%M:%SZ")
    return value


class MessageQuery:
    """Immutable, chainable query builder over the messages table.

    Filters are combined with AND. Each method that modifies the query
    returns a new MessageQuery instance.
    """

    __slots__ = (
        "_conn", "_filters", "_sort", "_limit_val", "_offset_val",
        "_include_deleted", "_changelog", "_writable",
    )

    def __init__(
        self,
        conn: sqlite3.Connection,
        filters: tuple[_Filter, ...] = (),
        sort: tuple[str, bool] | None = None,
        limit: int | None = None,
        offset: int | None = None,
        include_deleted: bool = False,
        changelog: ChangeLog | None = None,
        writable: bool = False,
    ) -> None:
        self._conn = conn
        self._filters = filters
        self._sort = sort
        self._limit_val = limit
        self._offset_val = offset
        self._include_deleted = include_deleted
        self._changelog = changelog
        self._writable = writable

    def _clone(self, **overrides: Any) -> MessageQuery:
        """Return a copy with selected fields overridden."""
        return MessageQuery(
            conn=overrides.get("conn", self._conn),
            filters=overrides.get("filters", self._filters),
            sort=overrides.get("sort", self._sort),
            limit=overrides.get("limit", self._limit_val),
            offset=overrides.get("offset", self._offset_val),
            include_deleted=overrides.get("include_deleted", self._include_deleted),
            changelog=overrides.get("changelog", self._changelog),
            writable=overrides.get("writable", self._writable),
        )

    # ------------------------------------------------------------------
    # Filter methods
    # ------------------------------------------------------------------

    def filter(self, **kwargs: Any) -> MessageQuery:
        """Return a new query with additional filters.

        Supported keyword arguments:
            sender: str          - exact sender email
            sender_like: str     - LIKE pattern on sender email
            recipient: str       - exact recipient email (any type)
            recipient_like: str  - LIKE pattern on recipient email
            domain: str          - exact sender domain
            label: str           - exact label name
            account: str         - exact source identifier
            before: str|datetime - sent_at < value
            after: str|datetime  - sent_at >= value
            min_size: int        - size_estimate >= value
            max_size: int        - size_estimate < value
            has_attachments: bool
            subject_like: str    - LIKE pattern on subject
            is_deleted: bool|None - None=no filter, True=only deleted,
                                    False=only non-deleted (default)
        """
        new_filters: list[_Filter] = []

        if "sender" in kwargs:
            new_filters.append(_Filter(
                "EXISTS (SELECT 1 FROM participants p "
                "WHERE p.id = m.sender_id AND p.email_address = ?)",
                (kwargs["sender"],),
            ))

        if "sender_like" in kwargs:
            new_filters.append(_Filter(
                "EXISTS (SELECT 1 FROM participants p "
                "WHERE p.id = m.sender_id AND p.email_address LIKE ?)",
                (kwargs["sender_like"],),
            ))

        if "recipient" in kwargs:
            new_filters.append(_Filter(
                "EXISTS (SELECT 1 FROM message_recipients mr "
                "JOIN participants p ON p.id = mr.participant_id "
                "WHERE mr.message_id = m.id AND p.email_address = ?)",
                (kwargs["recipient"],),
            ))

        if "recipient_like" in kwargs:
            new_filters.append(_Filter(
                "EXISTS (SELECT 1 FROM message_recipients mr "
                "JOIN participants p ON p.id = mr.participant_id "
                "WHERE mr.message_id = m.id AND p.email_address LIKE ?)",
                (kwargs["recipient_like"],),
            ))

        if "domain" in kwargs:
            new_filters.append(_Filter(
                "EXISTS (SELECT 1 FROM participants p "
                "WHERE p.id = m.sender_id AND p.domain = ?)",
                (kwargs["domain"],),
            ))

        if "label" in kwargs:
            new_filters.append(_Filter(
                "EXISTS (SELECT 1 FROM message_labels ml "
                "JOIN labels l ON l.id = ml.label_id "
                "WHERE ml.message_id = m.id AND l.name = ?)",
                (kwargs["label"],),
            ))

        if "account" in kwargs:
            new_filters.append(_Filter(
                "EXISTS (SELECT 1 FROM sources s "
                "WHERE s.id = m.source_id AND s.identifier = ?)",
                (kwargs["account"],),
            ))

        if "before" in kwargs:
            new_filters.append(_Filter(
                "m.sent_at < ?",
                (_to_iso(kwargs["before"]),),
            ))

        if "after" in kwargs:
            new_filters.append(_Filter(
                "m.sent_at >= ?",
                (_to_iso(kwargs["after"]),),
            ))

        if "min_size" in kwargs:
            new_filters.append(_Filter(
                "m.size_estimate >= ?",
                (kwargs["min_size"],),
            ))

        if "max_size" in kwargs:
            new_filters.append(_Filter(
                "m.size_estimate < ?",
                (kwargs["max_size"],),
            ))

        if "has_attachments" in kwargs:
            new_filters.append(_Filter(
                "m.has_attachments = ?",
                (int(kwargs["has_attachments"]),),
            ))

        if "subject_like" in kwargs:
            new_filters.append(_Filter(
                "m.subject LIKE ?",
                (kwargs["subject_like"],),
            ))

        include_deleted = self._include_deleted
        if "is_deleted" in kwargs:
            val = kwargs["is_deleted"]
            if val is True:
                new_filters.append(_Filter("m.deleted_at IS NOT NULL", ()))
                include_deleted = True
            elif val is False:
                new_filters.append(_Filter("m.deleted_at IS NULL", ()))
            elif val is None:
                include_deleted = True

        return self._clone(
            filters=self._filters + tuple(new_filters),
            include_deleted=include_deleted,
        )

    def sort_by(self, field: str, desc: bool = False) -> MessageQuery:
        """Return a new query sorted by the given field."""
        if field not in _SORT_FIELDS:
            raise ValueError(
                f"Unknown sort field {field!r}. "
                f"Valid fields: {', '.join(_SORT_FIELDS)}"
            )
        return self._clone(sort=(field, desc))

    def limit(self, n: int) -> MessageQuery:
        """Return a new query with a row limit."""
        return self._clone(limit=n)

    def offset(self, n: int) -> MessageQuery:
        """Return a new query with a row offset."""
        return self._clone(offset=n)

    # ------------------------------------------------------------------
    # SQL generation
    # ------------------------------------------------------------------

    def _base_where(self) -> tuple[list[str], list[Any]]:
        """Build the WHERE clauses and params.

        By default, deleted messages are excluded unless include_deleted
        is set (via is_deleted=True or is_deleted=None).
        """
        clauses: list[str] = []
        params: list[Any] = []

        if not self._include_deleted:
            clauses.append("m.deleted_at IS NULL")

        for f in self._filters:
            clauses.append(f.clause)
            params.extend(f.params)

        return clauses, params

    def _build_select(
        self, columns: str = _MSG_COLUMNS
    ) -> tuple[str, list[Any]]:
        """Build a full SELECT query."""
        clauses, params = self._base_where()

        sql = f"SELECT {columns} FROM messages m"
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)

        if self._sort:
            field, desc = self._sort
            direction = "DESC" if desc else "ASC"
            sql += f" ORDER BY {_SORT_FIELDS[field]} {direction}"
        else:
            sql += " ORDER BY m.sent_at DESC"

        if self._limit_val is not None:
            sql += f" LIMIT {self._limit_val}"
        elif self._offset_val is not None:
            # SQLite requires LIMIT before OFFSET; use -1 for unlimited
            sql += " LIMIT -1"
        if self._offset_val is not None:
            sql += f" OFFSET {self._offset_val}"

        return sql, params

    # ------------------------------------------------------------------
    # Execution methods
    # ------------------------------------------------------------------

    def __iter__(self) -> Iterator[Message]:
        sql, params = self._build_select()
        cursor = self._conn.execute(sql, params)
        for row in cursor:
            yield Message.from_row(
                row, self._conn,
                changelog=self._changelog,
                writable=self._writable,
            )

    def count(self) -> int:
        """Return the count of matching messages."""
        clauses, params = self._base_where()
        sql = "SELECT COUNT(*) FROM messages m"
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        row = self._conn.execute(sql, params).fetchone()
        return row[0]

    def first(self) -> Message | None:
        """Return the first matching message, or None."""
        q = self.limit(1)
        sql, params = q._build_select()
        row = self._conn.execute(sql, params).fetchone()
        if row is None:
            return None
        return Message.from_row(
            row, self._conn,
            changelog=self._changelog,
            writable=self._writable,
        )

    def exists(self) -> bool:
        """Return True if at least one message matches."""
        clauses, params = self._base_where()
        inner = "SELECT 1 FROM messages m"
        if clauses:
            inner += " WHERE " + " AND ".join(clauses)
        inner += " LIMIT 1"
        sql = f"SELECT EXISTS ({inner})"
        row = self._conn.execute(sql, params).fetchone()
        return bool(row[0])

    def message_ids(self) -> list[int]:
        """Return the IDs of all matching messages."""
        sql, params = self._build_select(columns="m.id")
        rows = self._conn.execute(sql, params).fetchall()
        return [r[0] for r in rows]

    def __len__(self) -> int:
        return self.count()

    def __bool__(self) -> bool:
        return self.exists()

    def __repr__(self) -> str:
        n_filters = len(self._filters)
        parts = [f"MessageQuery(filters={n_filters}"]
        if self._sort:
            field, desc = self._sort
            parts.append(f"sort={field}{'_desc' if desc else ''}")
        if self._limit_val is not None:
            parts.append(f"limit={self._limit_val}")
        parts_str = ", ".join(parts)
        return f"{parts_str})"

    # ------------------------------------------------------------------
    # Mutations (require writable vault)
    # ------------------------------------------------------------------

    def _check_writable(self) -> None:
        if not self._writable:
            raise VaultReadOnlyError()

    def delete(self) -> int:
        """Soft-delete all matching messages. Returns count affected."""
        self._check_writable()
        ids = self.message_ids()
        if not ids:
            return 0

        self._changelog._record(
            "delete", ids,
            details=None,
            undo_data=None,
        )

        placeholders = ",".join("?" for _ in ids)
        self._conn.execute(
            f"UPDATE messages SET deleted_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') "
            f"WHERE id IN ({placeholders}) AND deleted_at IS NULL",
            ids,
        )
        self._conn.commit()
        return len(ids)

    def add_label(self, name: str) -> int:
        """Add a label to all matching messages. Returns count affected."""
        self._check_writable()
        ids = self.message_ids()
        if not ids:
            return 0

        # Get or create the label
        row = self._conn.execute(
            "SELECT id FROM labels WHERE name = ?", (name,)
        ).fetchone()
        if row:
            label_id = row[0]
        else:
            cursor = self._conn.execute(
                "INSERT INTO labels (name, label_type) VALUES (?, 'user')",
                (name,),
            )
            label_id = cursor.lastrowid

        self._changelog._record(
            "label_add", ids,
            details={"label": name, "label_id": label_id},
            undo_data=None,
        )

        self._conn.executemany(
            "INSERT OR IGNORE INTO message_labels (message_id, label_id) "
            "VALUES (?, ?)",
            [(mid, label_id) for mid in ids],
        )
        self._conn.commit()
        return len(ids)

    def remove_label(self, name: str) -> int:
        """Remove a label from all matching messages. Returns count affected."""
        self._check_writable()
        ids = self.message_ids()
        if not ids:
            return 0

        row = self._conn.execute(
            "SELECT id FROM labels WHERE name = ?", (name,)
        ).fetchone()
        if row is None:
            return 0
        label_id = row[0]

        # Find which messages actually have this label
        placeholders = ",".join("?" for _ in ids)
        affected_rows = self._conn.execute(
            f"SELECT message_id FROM message_labels "
            f"WHERE label_id = ? AND message_id IN ({placeholders})",
            [label_id, *ids],
        ).fetchall()
        affected_ids = [r[0] for r in affected_rows]
        if not affected_ids:
            return 0

        self._changelog._record(
            "label_remove", affected_ids,
            details={"label": name},
            undo_data={"label_id": label_id},
        )

        placeholders = ",".join("?" for _ in affected_ids)
        self._conn.execute(
            f"DELETE FROM message_labels "
            f"WHERE label_id = ? AND message_id IN ({placeholders})",
            [label_id, *affected_ids],
        )
        self._conn.commit()
        return len(affected_ids)

    # ------------------------------------------------------------------
    # Data export
    # ------------------------------------------------------------------

    def to_dataframe(self):
        """Convert query results to a pandas DataFrame.

        Requires pandas: pip install 'msgvault-sdk[pandas]'
        """
        from msgvault_sdk.dataframe import query_to_dataframe

        return query_to_dataframe(self)

    # ------------------------------------------------------------------
    # Grouping (delegates to GroupedQuery)
    # ------------------------------------------------------------------

    def group_by(self, field: str) -> GroupedQuery:
        """Group matching messages by the given field.

        Supported fields: sender, sender_name, recipient, domain,
        label, year, month, account.
        """
        return GroupedQuery(self, field)


# ======================================================================
# GroupedQuery
# ======================================================================

# Maps group field names to (key_expr, from_extra, key_column_alias)
# key_expr: SQL expression for the group key
# from_extra: extra JOIN/FROM clause needed (or empty string)
# filter_builder: function(key) -> _Filter for drill-down
_GROUP_CONFIGS: dict[str, tuple[str, str]] = {
    "sender": (
        "p.email_address",
        " JOIN participants p ON p.id = m.sender_id",
    ),
    "sender_name": (
        "COALESCE(p.display_name, p.email_address)",
        " JOIN participants p ON p.id = m.sender_id",
    ),
    "domain": (
        "p.domain",
        " JOIN participants p ON p.id = m.sender_id",
    ),
    "year": (
        "strftime('%Y', m.sent_at)",
        "",
    ),
    "month": (
        "strftime('%Y-%m', m.sent_at)",
        "",
    ),
    "account": (
        "s.identifier",
        " JOIN sources s ON s.id = m.source_id",
    ),
    "label": (
        "l.name",
        " JOIN message_labels ml ON ml.message_id = m.id"
        " JOIN labels l ON l.id = ml.label_id",
    ),
    "recipient": (
        "rp.email_address",
        " JOIN message_recipients mr ON mr.message_id = m.id"
        " JOIN participants rp ON rp.id = mr.participant_id",
    ),
}

_GROUP_SORT_FIELDS = {"key", "count", "total_size"}


@dataclass(slots=True)
class Group:
    """A single group from a GroupedQuery."""

    key: str
    count: int
    total_size: int
    _query: MessageQuery
    _field: str

    @property
    def messages(self) -> MessageQuery:
        """Return a MessageQuery filtered to this group's messages."""
        return _drill_down(self._query, self._field, self.key)

    def __repr__(self) -> str:
        return f"Group(key={self.key!r}, count={self.count}, total_size={self.total_size})"


def _drill_down(base: MessageQuery, field: str, key: str) -> MessageQuery:
    """Create a filtered query that matches a specific group key."""
    if field == "sender":
        return base.filter(sender=key)
    elif field == "sender_name":
        return base.filter(sender_like=key)
    elif field == "domain":
        return base.filter(domain=key)
    elif field == "year":
        return base.filter(
            after=f"{key}-01-01", before=f"{int(key) + 1}-01-01"
        )
    elif field == "month":
        # key is "YYYY-MM"
        year, month = key.split("-")
        y, m = int(year), int(month)
        if m == 12:
            next_month = f"{y + 1}-01-01"
        else:
            next_month = f"{y}-{m + 1:02d}-01"
        return base.filter(after=f"{key}-01", before=next_month)
    elif field == "account":
        return base.filter(account=key)
    elif field == "label":
        return base.filter(label=key)
    elif field == "recipient":
        return base.filter(recipient=key)
    else:
        raise ValueError(f"Unknown group field: {field!r}")


class GroupedQuery:
    """Result of grouping a MessageQuery by a field."""

    __slots__ = ("_base", "_field", "_sort_field", "_sort_desc")

    def __init__(self, base: MessageQuery, field: str) -> None:
        if field not in _GROUP_CONFIGS:
            raise ValueError(
                f"Unknown group field {field!r}. "
                f"Valid fields: {', '.join(_GROUP_CONFIGS)}"
            )
        self._base = base
        self._field = field
        self._sort_field = "count"
        self._sort_desc = True

    def sort_by(self, field: str, desc: bool = True) -> GroupedQuery:
        """Return a new GroupedQuery with a different sort order."""
        if field not in _GROUP_SORT_FIELDS:
            raise ValueError(
                f"Unknown group sort field {field!r}. "
                f"Valid fields: {', '.join(_GROUP_SORT_FIELDS)}"
            )
        gq = GroupedQuery(self._base, self._field)
        gq._sort_field = field
        gq._sort_desc = desc
        return gq

    def __iter__(self) -> Iterator[Group]:
        key_expr, from_extra = _GROUP_CONFIGS[self._field]

        clauses, params = self._base._base_where()

        sql = (
            f"SELECT {key_expr} AS grp_key, "
            f"COUNT(*) AS grp_count, "
            f"COALESCE(SUM(m.size_estimate), 0) AS grp_total_size "
            f"FROM messages m{from_extra}"
        )
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)

        sql += f" GROUP BY grp_key"

        # Sort
        sort_col = {
            "key": "grp_key",
            "count": "grp_count",
            "total_size": "grp_total_size",
        }[self._sort_field]
        direction = "DESC" if self._sort_desc else "ASC"
        sql += f" ORDER BY {sort_col} {direction}"

        cursor = self._base._conn.execute(sql, params)
        for row in cursor:
            key = str(row["grp_key"]) if row["grp_key"] is not None else ""
            yield Group(
                key=key,
                count=row["grp_count"],
                total_size=row["grp_total_size"],
                _query=self._base,
                _field=self._field,
            )

    def to_dataframe(self):
        """Convert grouped results to a pandas DataFrame.

        Requires pandas: pip install 'msgvault-sdk[pandas]'
        """
        from msgvault_sdk.dataframe import groups_to_dataframe

        return groups_to_dataframe(self)

    def __repr__(self) -> str:
        return f"GroupedQuery(field={self._field!r}, sort={self._sort_field})"
