"""Vault: root object for accessing a msgvault database."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Iterator

from msgvault_sdk.db import connect, find_db_path
from msgvault_sdk.errors import VaultReadOnlyError
from msgvault_sdk.models import Account, Message


class _MessageIterator:
    """Simple iterator over messages (Stage 1 placeholder for MessageQuery)."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def __iter__(self) -> Iterator[Message]:
        cursor = self._conn.execute(
            "SELECT id, conversation_id, source_id, message_type, sent_at, "
            "subject, snippet, is_read, is_from_me, has_attachments, "
            "size_estimate, deleted_at, sender_id "
            "FROM messages WHERE deleted_at IS NULL "
            "ORDER BY sent_at DESC"
        )
        for row in cursor:
            yield Message.from_row(row, self._conn)


class Vault:
    """Root object for accessing a msgvault email archive.

    Opens the SQLite database in read-only mode by default. Pass
    ``writable=True`` to enable mutations (delete, add/remove labels).
    """

    def __init__(
        self,
        db_path: str | Path | None = None,
        *,
        writable: bool = False,
    ) -> None:
        resolved = find_db_path(db_path)
        self._conn = connect(resolved, readonly=not writable)
        self._writable = writable
        self._db_path = resolved

    @classmethod
    def from_config(cls, config_path: str | Path | None = None) -> Vault:
        """Create a Vault by reading the msgvault config file."""
        return cls()

    @property
    def db_path(self) -> Path:
        return self._db_path

    @property
    def writable(self) -> bool:
        return self._writable

    @property
    def accounts(self) -> list[Account]:
        rows = self._conn.execute(
            "SELECT id, source_type, identifier, display_name, last_sync_at "
            "FROM sources ORDER BY identifier"
        ).fetchall()
        return [Account.from_row(r) for r in rows]

    @property
    def messages(self) -> _MessageIterator:
        """Return an iterable over all non-deleted messages.

        In Stage 2, this will return a MessageQuery instead.
        """
        return _MessageIterator(self._conn)

    def _check_writable(self) -> None:
        """Raise VaultReadOnlyError if the vault is not writable."""
        if not self._writable:
            raise VaultReadOnlyError()

    def close(self) -> None:
        """Close the database connection."""
        self._conn.close()

    def __enter__(self) -> Vault:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def __repr__(self) -> str:
        mode = "writable" if self._writable else "readonly"
        return f"Vault({str(self._db_path)!r}, {mode})"
