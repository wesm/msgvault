"""Tests for MessageQuery and GroupedQuery."""

from __future__ import annotations

from datetime import datetime

import pytest

from msgvault_sdk.models import Message
from msgvault_sdk.query import GroupedQuery, MessageQuery


@pytest.fixture()
def mq(db_conn) -> MessageQuery:
    """A MessageQuery bound to the seeded test database."""
    return MessageQuery(db_conn)


# ------------------------------------------------------------------
# MessageQuery basics
# ------------------------------------------------------------------


class TestMessageQueryBasics:
    def test_all_messages(self, mq) -> None:
        msgs = list(mq)
        # 10 messages total, 1 deleted -> 9 non-deleted
        assert len(msgs) == 9
        assert all(isinstance(m, Message) for m in msgs)

    def test_count(self, mq) -> None:
        assert mq.count() == 9

    def test_len(self, mq) -> None:
        assert len(mq) == 9

    def test_exists_true(self, mq) -> None:
        assert mq.exists() is True

    def test_bool_true(self, mq) -> None:
        assert bool(mq) is True

    def test_first(self, mq) -> None:
        msg = mq.first()
        assert msg is not None
        assert isinstance(msg, Message)

    def test_first_returns_most_recent(self, mq) -> None:
        msg = mq.first()
        # Default sort is sent_at DESC, so first should be most recent
        assert msg.id == 9  # 2024-06-15

    def test_message_ids(self, mq) -> None:
        ids = mq.message_ids()
        assert len(ids) == 9
        assert 10 not in ids  # deleted

    def test_immutable(self, mq) -> None:
        filtered = mq.filter(sender="alice@example.com")
        assert mq.count() == 9
        assert filtered.count() < 9
        assert mq is not filtered

    def test_empty_result(self, mq) -> None:
        empty = mq.filter(sender="nobody@nowhere.com")
        assert empty.count() == 0
        assert empty.first() is None
        assert empty.exists() is False
        assert list(empty) == []

    def test_repr(self, mq) -> None:
        r = repr(mq)
        assert "MessageQuery" in r


# ------------------------------------------------------------------
# Filters
# ------------------------------------------------------------------


class TestMessageQueryFilters:
    def test_filter_sender(self, mq) -> None:
        msgs = list(mq.filter(sender="alice@example.com"))
        assert len(msgs) > 0
        for msg in msgs:
            assert msg.sender.email == "alice@example.com"

    def test_filter_sender_like(self, mq) -> None:
        msgs = list(mq.filter(sender_like="%@example.com"))
        assert len(msgs) > 0
        for msg in msgs:
            assert msg.sender.domain == "example.com"

    def test_filter_recipient(self, mq) -> None:
        msgs = list(mq.filter(recipient="alice@example.com"))
        assert len(msgs) > 0

    def test_filter_recipient_like(self, mq) -> None:
        msgs = list(mq.filter(recipient_like="%@example.com"))
        assert len(msgs) > 0

    def test_filter_domain(self, mq) -> None:
        msgs = list(mq.filter(domain="service.com"))
        # Only noreply@service.com is sender from service.com
        assert len(msgs) > 0
        for msg in msgs:
            assert msg.sender.domain == "service.com"

    def test_filter_label(self, mq) -> None:
        msgs = list(mq.filter(label="IMPORTANT"))
        assert len(msgs) == 2  # messages 2 and 4
        ids = {m.id for m in msgs}
        assert ids == {2, 4}

    def test_filter_account(self, mq) -> None:
        msgs = list(mq.filter(account="other@gmail.com"))
        # Messages 5 and 8 are from source_id=2 (other@gmail.com)
        assert len(msgs) == 2
        for msg in msgs:
            assert msg.source_id == 2

    def test_filter_before(self, mq) -> None:
        msgs = list(mq.filter(before="2024-01-01"))
        assert len(msgs) > 0
        for msg in msgs:
            assert msg.sent_at < datetime(2024, 1, 1)

    def test_filter_after(self, mq) -> None:
        msgs = list(mq.filter(after="2024-01-01"))
        assert len(msgs) > 0
        for msg in msgs:
            assert msg.sent_at >= datetime(2024, 1, 1)

    def test_filter_before_after_combined(self, mq) -> None:
        msgs = list(mq.filter(after="2023-06-01", before="2024-01-01"))
        for msg in msgs:
            assert datetime(2023, 6, 1) <= msg.sent_at < datetime(2024, 1, 1)

    def test_filter_min_size(self, mq) -> None:
        msgs = list(mq.filter(min_size=10000))
        assert len(msgs) > 0
        for msg in msgs:
            assert msg.size_estimate >= 10000

    def test_filter_max_size(self, mq) -> None:
        msgs = list(mq.filter(max_size=1000))
        assert len(msgs) > 0
        for msg in msgs:
            assert msg.size_estimate < 1000

    def test_filter_size_range(self, mq) -> None:
        msgs = list(mq.filter(min_size=1000, max_size=3000))
        for msg in msgs:
            assert 1000 <= msg.size_estimate < 3000

    def test_filter_has_attachments(self, mq) -> None:
        msgs = list(mq.filter(has_attachments=True))
        assert len(msgs) == 1  # only message 4
        assert msgs[0].id == 4

    def test_filter_subject_like(self, mq) -> None:
        msgs = list(mq.filter(subject_like="%Report%"))
        assert len(msgs) > 0
        for msg in msgs:
            assert "Report" in msg.subject

    def test_filter_chained(self, mq) -> None:
        msgs = list(
            mq.filter(domain="example.com")
            .filter(after="2023-06-01")
            .filter(label="INBOX")
        )
        assert len(msgs) > 0
        for msg in msgs:
            assert msg.sender.domain == "example.com"
            assert msg.sent_at >= datetime(2023, 6, 1)

    def test_filter_is_deleted_true(self, mq) -> None:
        msgs = list(mq.filter(is_deleted=True))
        assert len(msgs) == 1
        assert msgs[0].id == 10

    def test_filter_is_deleted_false(self, mq) -> None:
        msgs = list(mq.filter(is_deleted=False))
        assert len(msgs) == 9

    def test_filter_is_deleted_none(self, mq) -> None:
        # None means show all (no deleted_at filter)
        msgs = list(mq.filter(is_deleted=None))
        assert len(msgs) == 10

    def test_filter_with_datetime_object(self, mq) -> None:
        dt = datetime(2024, 1, 1)
        msgs = list(mq.filter(before=dt))
        assert len(msgs) > 0
        for msg in msgs:
            assert msg.sent_at < dt


# ------------------------------------------------------------------
# Sorting
# ------------------------------------------------------------------


class TestMessageQuerySort:
    def test_sort_by_date_asc(self, mq) -> None:
        msgs = list(mq.sort_by("date"))
        dates = [m.sent_at for m in msgs]
        assert dates == sorted(dates)

    def test_sort_by_date_desc(self, mq) -> None:
        msgs = list(mq.sort_by("date", desc=True))
        dates = [m.sent_at for m in msgs]
        assert dates == sorted(dates, reverse=True)

    def test_sort_by_size(self, mq) -> None:
        msgs = list(mq.sort_by("size"))
        sizes = [m.size_estimate for m in msgs]
        assert sizes == sorted(sizes)

    def test_sort_by_size_desc(self, mq) -> None:
        msgs = list(mq.sort_by("size", desc=True))
        sizes = [m.size_estimate for m in msgs]
        assert sizes == sorted(sizes, reverse=True)

    def test_sort_by_sender(self, mq) -> None:
        msgs = list(mq.sort_by("sender"))
        emails = [m.sender.email for m in msgs if m.sender]
        assert emails == sorted(emails)

    def test_sort_by_subject(self, mq) -> None:
        msgs = list(mq.sort_by("subject"))
        subjects = [m.subject for m in msgs]
        assert subjects == sorted(subjects)

    def test_sort_invalid_field(self, mq) -> None:
        with pytest.raises(ValueError, match="Unknown sort field"):
            mq.sort_by("invalid")


# ------------------------------------------------------------------
# Limit / Offset
# ------------------------------------------------------------------


class TestMessageQueryPagination:
    def test_limit(self, mq) -> None:
        msgs = list(mq.limit(3))
        assert len(msgs) == 3

    def test_offset(self, mq) -> None:
        all_ids = [m.id for m in mq]
        offset_ids = [m.id for m in mq.offset(2)]
        assert offset_ids == all_ids[2:]

    def test_limit_offset(self, mq) -> None:
        all_ids = [m.id for m in mq]
        page_ids = [m.id for m in mq.limit(3).offset(2)]
        assert len(page_ids) == 3
        assert page_ids == all_ids[2:5]


# ------------------------------------------------------------------
# GroupedQuery
# ------------------------------------------------------------------


class TestGroupedQuery:
    def test_group_by_sender(self, mq) -> None:
        groups = list(mq.group_by("sender"))
        assert len(groups) > 0
        keys = {g.key for g in groups}
        assert "alice@example.com" in keys
        total_count = sum(g.count for g in groups)
        assert total_count == 9  # all non-deleted

    def test_group_by_domain(self, mq) -> None:
        groups = list(mq.group_by("domain"))
        keys = {g.key for g in groups}
        assert "example.com" in keys
        assert "service.com" in keys

    def test_group_by_year(self, mq) -> None:
        groups = list(mq.group_by("year"))
        keys = {g.key for g in groups}
        assert "2023" in keys
        assert "2024" in keys

    def test_group_by_month(self, mq) -> None:
        groups = list(mq.group_by("month"))
        assert len(groups) > 0
        for g in groups:
            assert len(g.key) == 7  # "YYYY-MM"

    def test_group_by_label(self, mq) -> None:
        groups = list(mq.group_by("label"))
        keys = {g.key for g in groups}
        assert "INBOX" in keys

    def test_group_by_account(self, mq) -> None:
        groups = list(mq.group_by("account"))
        keys = {g.key for g in groups}
        assert "test@gmail.com" in keys
        assert "other@gmail.com" in keys

    def test_group_by_recipient(self, mq) -> None:
        groups = list(mq.group_by("recipient"))
        keys = {g.key for g in groups}
        assert "alice@example.com" in keys

    def test_group_messages_lazy(self, mq) -> None:
        groups = list(mq.group_by("sender"))
        alice_group = next(g for g in groups if g.key == "alice@example.com")
        msgs = list(alice_group.messages)
        assert len(msgs) == alice_group.count
        for m in msgs:
            assert m.sender.email == "alice@example.com"

    def test_group_sort_by_count(self, mq) -> None:
        groups = list(mq.group_by("sender"))
        counts = [g.count for g in groups]
        assert counts == sorted(counts, reverse=True)

    def test_group_sort_by_key(self, mq) -> None:
        groups = list(mq.group_by("sender").sort_by("key", desc=False))
        keys = [g.key for g in groups]
        assert keys == sorted(keys)

    def test_group_sort_by_total_size(self, mq) -> None:
        groups = list(mq.group_by("sender").sort_by("total_size", desc=True))
        sizes = [g.total_size for g in groups]
        assert sizes == sorted(sizes, reverse=True)

    def test_group_with_filters(self, mq) -> None:
        groups = list(mq.filter(after="2024-01-01").group_by("sender"))
        total = sum(g.count for g in groups)
        expected = mq.filter(after="2024-01-01").count()
        assert total == expected

    def test_group_invalid_field(self, mq) -> None:
        with pytest.raises(ValueError, match="Unknown group field"):
            mq.group_by("invalid")

    def test_group_sort_invalid_field(self, mq) -> None:
        with pytest.raises(ValueError, match="Unknown group sort field"):
            mq.group_by("sender").sort_by("invalid")

    def test_group_repr(self, mq) -> None:
        gq = mq.group_by("sender")
        assert "sender" in repr(gq)

    def test_group_item_repr(self, mq) -> None:
        groups = list(mq.group_by("sender"))
        r = repr(groups[0])
        assert "Group" in r
        assert "count=" in r
