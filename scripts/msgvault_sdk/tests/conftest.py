"""Shared pytest fixtures for msgvault_sdk tests."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

# Path to the schema file in the Go project
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
_SCHEMA_SQL = _PROJECT_ROOT / "internal" / "store" / "schema.sql"


def _read_schema() -> str:
    """Read the msgvault schema SQL from the Go project."""
    return _SCHEMA_SQL.read_text()


def _seed_data(conn: sqlite3.Connection) -> None:
    """Insert seed data for testing."""
    # Accounts
    conn.execute(
        "INSERT INTO sources (id, source_type, identifier, display_name) "
        "VALUES (1, 'gmail', 'test@gmail.com', 'Test User')"
    )
    conn.execute(
        "INSERT INTO sources (id, source_type, identifier, display_name) "
        "VALUES (2, 'gmail', 'other@gmail.com', 'Other User')"
    )

    # Participants
    participants = [
        (1, "alice@example.com", None, "Alice Smith", "example.com"),
        (2, "bob@example.com", None, "Bob Jones", "example.com"),
        (3, "noreply@service.com", None, None, "service.com"),
        (4, "admin@example.com", None, "Admin", "example.com"),
        (5, "test@gmail.com", None, "Test User", "gmail.com"),
    ]
    conn.executemany(
        "INSERT INTO participants (id, email_address, phone_number, display_name, domain) "
        "VALUES (?, ?, ?, ?, ?)",
        participants,
    )

    # Labels
    conn.execute(
        "INSERT INTO labels (id, source_id, name, label_type) "
        "VALUES (1, 1, 'INBOX', 'system')"
    )
    conn.execute(
        "INSERT INTO labels (id, source_id, name, label_type) "
        "VALUES (2, 1, 'SENT', 'system')"
    )
    conn.execute(
        "INSERT INTO labels (id, source_id, name, label_type) "
        "VALUES (3, 1, 'IMPORTANT', 'system')"
    )

    # Conversations
    conn.execute(
        "INSERT INTO conversations (id, source_id, source_conversation_id, "
        "conversation_type, title, message_count, last_message_at) "
        "VALUES (1, 1, 'thread-1', 'email_thread', 'Project Discussion', 6, "
        "'2024-06-15T10:00:00Z')"
    )
    conn.execute(
        "INSERT INTO conversations (id, source_id, source_conversation_id, "
        "conversation_type, title, message_count, last_message_at) "
        "VALUES (2, 1, 'thread-2', 'email_thread', 'Weekly Report', 4, "
        "'2024-03-20T14:00:00Z')"
    )

    # Messages (10 messages spanning 2023-2024)
    messages = [
        (1, 1, 1, "msg-1", "email", "2023-01-15T09:00:00Z", "Hello from Alice",
         "Quick hello", 1, True, False, False, 1500, None),
        (2, 1, 1, "msg-2", "email", "2023-03-20T14:30:00Z", "Project Update",
         "Here is the update", 2, True, False, False, 3200, None),
        (3, 1, 1, "msg-3", "email", "2023-06-10T08:00:00Z", "Notification",
         "You have a notification", 3, True, False, False, 800, None),
        (4, 1, 1, "msg-4", "email", "2023-09-01T16:00:00Z", "Q3 Report",
         "Quarterly report attached", 1, True, False, True, 52000, None),
        (5, 2, 2, "msg-5", "email", "2023-12-25T00:00:00Z", "Holiday Greetings",
         "Happy holidays", 2, True, False, False, 1200, None),
        (6, 1, 1, "msg-6", "email", "2024-01-10T10:00:00Z", "New Year Plans",
         "Plans for the year", 1, True, False, False, 2100, None),
        (7, 1, 1, "msg-7", "email", "2024-03-15T11:00:00Z", "Weekly Summary",
         "This week's summary", 4, True, False, False, 1800, None),
        (8, 2, 2, "msg-8", "email", "2024-06-01T09:30:00Z", "Meeting Notes",
         "Notes from meeting", 5, True, True, False, 4500, None),
        (9, 1, 1, "msg-9", "email", "2024-06-15T10:00:00Z", "Re: Project Discussion",
         "Follow up on project", 1, True, False, False, 2800, None),
        (10, 1, 1, "msg-10", "email", "2024-09-01T08:00:00Z", "Deleted message",
         "This was deleted", 3, True, False, False, 900, "2024-09-02T08:00:00Z"),
    ]
    conn.executemany(
        "INSERT INTO messages (id, conversation_id, source_id, source_message_id, "
        "message_type, sent_at, subject, snippet, sender_id, is_read, is_from_me, "
        "has_attachments, size_estimate, deleted_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        messages,
    )

    # Message bodies
    bodies = [
        (1, "Hello Alice here, just saying hi!", "<p>Hello Alice here</p>"),
        (2, "Here is the project update for Q1.", "<p>Project update Q1</p>"),
        (3, "You have a new notification from the system.", None),
        (4, "Please find the Q3 report attached.", "<p>Q3 report</p>"),
        (5, "Happy holidays to everyone!", "<p>Happy holidays!</p>"),
        (6, "Let's discuss plans for the new year.", None),
        (7, "Summary of this week's activities.", "<p>Weekly summary</p>"),
        (8, "Notes from our meeting on Friday.", "<p>Meeting notes</p>"),
        (9, "Following up on the project discussion.", None),
        (10, "This message was later deleted.", None),
    ]
    conn.executemany(
        "INSERT INTO message_bodies (message_id, body_text, body_html) VALUES (?, ?, ?)",
        bodies,
    )

    # Message recipients
    recipients = [
        (1, 5, "to"),    # msg 1: alice -> test@gmail.com
        (2, 1, "to"),    # msg 2: bob -> alice
        (2, 4, "cc"),    # msg 2: bob -> admin (cc)
        (3, 5, "to"),    # msg 3: noreply -> test@gmail.com
        (4, 5, "to"),    # msg 4: alice -> test@gmail.com
        (4, 2, "cc"),    # msg 4: alice -> bob (cc)
        (5, 1, "to"),    # msg 5: bob -> alice
        (6, 2, "to"),    # msg 6: alice -> bob
        (7, 5, "to"),    # msg 7: admin -> test@gmail.com
        (8, 1, "to"),    # msg 8: test@gmail.com -> alice
        (9, 2, "to"),    # msg 9: alice -> bob
        (10, 5, "to"),   # msg 10: noreply -> test@gmail.com
    ]
    conn.executemany(
        "INSERT INTO message_recipients (message_id, participant_id, recipient_type) "
        "VALUES (?, ?, ?)",
        recipients,
    )

    # Message labels
    message_labels = [
        (1, 1),   # msg 1: INBOX
        (2, 1),   # msg 2: INBOX
        (2, 3),   # msg 2: IMPORTANT
        (3, 1),   # msg 3: INBOX
        (4, 1),   # msg 4: INBOX
        (4, 3),   # msg 4: IMPORTANT
        (5, 1),   # msg 5: INBOX
        (6, 2),   # msg 6: SENT
        (7, 1),   # msg 7: INBOX
        (8, 2),   # msg 8: SENT
        (9, 1),   # msg 9: INBOX
    ]
    conn.executemany(
        "INSERT INTO message_labels (message_id, label_id) VALUES (?, ?)",
        message_labels,
    )

    # Attachments (on message 4: Q3 Report)
    conn.execute(
        "INSERT INTO attachments (id, message_id, filename, mime_type, size, "
        "content_hash, media_type, storage_path) "
        "VALUES (1, 4, 'q3-report.pdf', 'application/pdf', 48000, "
        "'abc123def456', 'document', 'ab/abc123def456')"
    )
    conn.execute(
        "INSERT INTO attachments (id, message_id, filename, mime_type, size, "
        "content_hash, media_type, storage_path) "
        "VALUES (2, 4, 'chart.png', 'image/png', 3200, "
        "'789ghi012jkl', 'image', '78/789ghi012jkl')"
    )

    conn.commit()


@pytest.fixture()
def tmp_db(tmp_path):
    """Create a temporary msgvault database with schema and seed data."""
    db_path = tmp_path / "msgvault.db"
    conn = sqlite3.connect(str(db_path))
    conn.executescript(_read_schema())
    _seed_data(conn)
    conn.close()
    return db_path


@pytest.fixture()
def db_conn(tmp_db):
    """Provide an open connection to the seeded test database."""
    conn = sqlite3.connect(str(tmp_db))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    yield conn
    conn.close()
