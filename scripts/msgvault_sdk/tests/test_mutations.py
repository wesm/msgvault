"""Tests for mutation methods on MessageQuery and Message."""

from __future__ import annotations

import pytest

from msgvault_sdk.changelog import ChangeLog
from msgvault_sdk.errors import VaultReadOnlyError
from msgvault_sdk.query import MessageQuery


@pytest.fixture()
def writable_mq(db_conn) -> MessageQuery:
    """A writable MessageQuery bound to the seeded test database."""
    cl = ChangeLog(db_conn)
    return MessageQuery(db_conn, changelog=cl, writable=True)


@pytest.fixture()
def readonly_mq(db_conn) -> MessageQuery:
    """A read-only MessageQuery."""
    return MessageQuery(db_conn, writable=False)


# ------------------------------------------------------------------
# Delete
# ------------------------------------------------------------------


class TestDeleteMessages:
    def test_delete_messages(self, writable_mq, db_conn) -> None:
        q = writable_mq.filter(sender="alice@example.com")
        count = q.delete()
        assert count > 0

        # Verify messages are soft-deleted
        rows = db_conn.execute(
            "SELECT deleted_at FROM messages WHERE sender_id = 1 AND deleted_at IS NOT NULL"
        ).fetchall()
        assert len(rows) == count

    def test_delete_already_deleted(self, writable_mq) -> None:
        # Message 10 is already deleted; filtering for deleted messages
        # won't match with default (non-deleted) filter
        q = writable_mq.filter(sender="nobody@nowhere.com")
        count = q.delete()
        assert count == 0

    def test_delete_changelog_recorded(self, writable_mq) -> None:
        q = writable_mq.filter(sender="alice@example.com")
        q.delete()

        cl = ChangeLog(writable_mq._conn)
        last = cl.last()
        assert last is not None
        assert last.operation == "delete"
        assert last.message_count > 0

    def test_delete_readonly_raises(self, readonly_mq) -> None:
        with pytest.raises(VaultReadOnlyError):
            readonly_mq.delete()


# ------------------------------------------------------------------
# Add label
# ------------------------------------------------------------------


class TestAddLabel:
    def test_add_label(self, writable_mq, db_conn) -> None:
        q = writable_mq.filter(sender="bob@example.com")
        msg_ids = q.message_ids()
        count = q.add_label("Archive")
        assert count == len(msg_ids)

        # Verify label was created
        row = db_conn.execute(
            "SELECT id FROM labels WHERE name = 'Archive'"
        ).fetchone()
        assert row is not None
        label_id = row["id"]

        # Verify associations
        for mid in msg_ids:
            row = db_conn.execute(
                "SELECT COUNT(*) FROM message_labels "
                "WHERE message_id = ? AND label_id = ?",
                (mid, label_id),
            ).fetchone()
            assert row[0] == 1

    def test_add_label_existing(self, writable_mq, db_conn) -> None:
        # INBOX label already exists
        q = writable_mq.filter(sender="bob@example.com")
        q.add_label("INBOX")

        # Should not create a duplicate label
        rows = db_conn.execute(
            "SELECT COUNT(*) FROM labels WHERE name = 'INBOX'"
        ).fetchone()
        assert rows[0] == 1

    def test_add_label_empty_query(self, writable_mq) -> None:
        q = writable_mq.filter(sender="nobody@nowhere.com")
        count = q.add_label("Archive")
        assert count == 0

    def test_add_label_readonly_raises(self, readonly_mq) -> None:
        with pytest.raises(VaultReadOnlyError):
            readonly_mq.add_label("Archive")

    def test_add_label_changelog(self, writable_mq) -> None:
        q = writable_mq.filter(sender="bob@example.com")
        q.add_label("TestLabel")

        cl = ChangeLog(writable_mq._conn)
        last = cl.last()
        assert last.operation == "label_add"
        assert last.details["label"] == "TestLabel"
        assert "label_id" in last.details


# ------------------------------------------------------------------
# Remove label
# ------------------------------------------------------------------


class TestRemoveLabel:
    def test_remove_label(self, writable_mq, db_conn) -> None:
        # Messages 1, 2, 3, 4, 5, 7, 9 have INBOX label
        q = writable_mq.filter(label="INBOX")
        initial_count = q.count()
        removed = q.remove_label("INBOX")
        assert removed == initial_count

        # Verify associations are gone
        row = db_conn.execute(
            "SELECT COUNT(*) FROM message_labels ml "
            "JOIN labels l ON l.id = ml.label_id WHERE l.name = 'INBOX'"
        ).fetchone()
        assert row[0] == 0

    def test_remove_label_not_present(self, writable_mq) -> None:
        q = writable_mq.filter(sender="alice@example.com")
        count = q.remove_label("NonexistentLabel")
        assert count == 0

    def test_remove_label_readonly_raises(self, readonly_mq) -> None:
        with pytest.raises(VaultReadOnlyError):
            readonly_mq.remove_label("INBOX")

    def test_remove_label_changelog(self, writable_mq) -> None:
        writable_mq.filter(label="IMPORTANT").remove_label("IMPORTANT")

        cl = ChangeLog(writable_mq._conn)
        last = cl.last()
        assert last.operation == "label_remove"
        assert last.details["label"] == "IMPORTANT"
        assert last.undo_data["label_id"] is not None


# ------------------------------------------------------------------
# Undo round-trips
# ------------------------------------------------------------------


class TestMutationUndo:
    def test_undo_delete(self, writable_mq, db_conn) -> None:
        q = writable_mq.filter(sender="alice@example.com")
        original_ids = q.message_ids()
        q.delete()

        # Messages should be deleted
        for mid in original_ids:
            row = db_conn.execute(
                "SELECT deleted_at FROM messages WHERE id = ?", (mid,)
            ).fetchone()
            assert row["deleted_at"] is not None

        # Undo
        cl = ChangeLog(db_conn)
        cl.undo_last()

        # Messages should be restored
        for mid in original_ids:
            row = db_conn.execute(
                "SELECT deleted_at FROM messages WHERE id = ?", (mid,)
            ).fetchone()
            assert row["deleted_at"] is None

    def test_undo_add_label(self, writable_mq, db_conn) -> None:
        q = writable_mq.filter(sender="bob@example.com")
        msg_ids = q.message_ids()
        q.add_label("UndoTest")

        label_row = db_conn.execute(
            "SELECT id FROM labels WHERE name = 'UndoTest'"
        ).fetchone()
        label_id = label_row["id"]

        # Undo
        cl = ChangeLog(db_conn)
        cl.undo_last()

        # Label associations should be gone
        for mid in msg_ids:
            row = db_conn.execute(
                "SELECT COUNT(*) FROM message_labels "
                "WHERE message_id = ? AND label_id = ?",
                (mid, label_id),
            ).fetchone()
            assert row[0] == 0

    def test_undo_remove_label(self, writable_mq, db_conn) -> None:
        # Find messages with IMPORTANT label
        q = writable_mq.filter(label="IMPORTANT")
        msg_ids = q.message_ids()
        assert len(msg_ids) > 0

        q.remove_label("IMPORTANT")

        # Verify removed
        for mid in msg_ids:
            row = db_conn.execute(
                "SELECT COUNT(*) FROM message_labels ml "
                "JOIN labels l ON l.id = ml.label_id "
                "WHERE ml.message_id = ? AND l.name = 'IMPORTANT'",
                (mid,),
            ).fetchone()
            assert row[0] == 0

        # Undo
        cl = ChangeLog(db_conn)
        cl.undo_last()

        # Verify restored
        for mid in msg_ids:
            row = db_conn.execute(
                "SELECT COUNT(*) FROM message_labels ml "
                "JOIN labels l ON l.id = ml.label_id "
                "WHERE ml.message_id = ? AND l.name = 'IMPORTANT'",
                (mid,),
            ).fetchone()
            assert row[0] == 1
