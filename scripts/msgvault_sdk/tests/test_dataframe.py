"""Tests for DataFrame export."""

from __future__ import annotations

import pytest

pd = pytest.importorskip("pandas")

from msgvault_sdk.changelog import ChangeLog
from msgvault_sdk.query import MessageQuery


@pytest.fixture()
def mq(db_conn) -> MessageQuery:
    cl = ChangeLog(db_conn)
    return MessageQuery(db_conn, changelog=cl)


class TestQueryToDataframe:
    def test_returns_dataframe(self, mq) -> None:
        df = mq.to_dataframe()
        assert isinstance(df, pd.DataFrame)

    def test_expected_columns(self, mq) -> None:
        df = mq.to_dataframe()
        expected = {
            "id", "date", "sender", "sender_domain", "subject", "snippet",
            "size", "has_attachments", "is_read", "labels", "to", "account",
        }
        assert set(df.columns) == expected

    def test_row_count_matches(self, mq) -> None:
        df = mq.to_dataframe()
        assert len(df) == mq.count()

    def test_with_filters(self, mq) -> None:
        filtered = mq.filter(sender="alice@example.com")
        df = filtered.to_dataframe()
        assert len(df) > 0
        assert all(df["sender"] == "alice@example.com")

    def test_sender_populated(self, mq) -> None:
        df = mq.to_dataframe()
        # All messages have senders in seed data
        assert df["sender"].notna().all()

    def test_account_populated(self, mq) -> None:
        df = mq.to_dataframe()
        assert df["account"].notna().all()


class TestGroupsToDataframe:
    def test_returns_dataframe(self, mq) -> None:
        df = mq.group_by("sender").to_dataframe()
        assert isinstance(df, pd.DataFrame)

    def test_expected_columns(self, mq) -> None:
        df = mq.group_by("sender").to_dataframe()
        assert set(df.columns) == {"key", "count", "total_size"}

    def test_row_count_matches(self, mq) -> None:
        groups = list(mq.group_by("sender"))
        df = mq.group_by("sender").to_dataframe()
        assert len(df) == len(groups)

    def test_count_sum_matches_total(self, mq) -> None:
        df = mq.group_by("sender").to_dataframe()
        assert df["count"].sum() == mq.count()
