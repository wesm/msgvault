"""Tests for the Vault class."""

from __future__ import annotations

import pytest

from msgvault_sdk.errors import VaultNotFoundError, VaultReadOnlyError
from msgvault_sdk.models import Account, Message
from msgvault_sdk.vault import Vault


class TestVaultBasics:
    def test_opens_db(self, tmp_db) -> None:
        v = Vault(tmp_db)
        assert v.db_path == tmp_db.resolve()
        v.close()

    def test_context_manager(self, tmp_db) -> None:
        with Vault(tmp_db) as v:
            assert v.db_path == tmp_db.resolve()
        # Connection should be closed after exiting context

    def test_not_found(self) -> None:
        with pytest.raises(VaultNotFoundError):
            Vault("/nonexistent/path.db")

    def test_readonly_default(self, tmp_db) -> None:
        v = Vault(tmp_db)
        assert v.writable is False
        v.close()

    def test_writable_mode(self, tmp_db) -> None:
        v = Vault(tmp_db, writable=True)
        assert v.writable is True
        v.close()

    def test_check_writable_raises(self, tmp_db) -> None:
        v = Vault(tmp_db)
        with pytest.raises(VaultReadOnlyError):
            v._check_writable()
        v.close()

    def test_check_writable_passes(self, tmp_db) -> None:
        v = Vault(tmp_db, writable=True)
        v._check_writable()  # should not raise
        v.close()

    def test_repr(self, tmp_db) -> None:
        v = Vault(tmp_db)
        r = repr(v)
        assert "readonly" in r
        v.close()

        v2 = Vault(tmp_db, writable=True)
        r2 = repr(v2)
        assert "writable" in r2
        v2.close()


class TestVaultAccounts:
    def test_returns_accounts(self, tmp_db) -> None:
        with Vault(tmp_db) as v:
            accts = v.accounts
            assert len(accts) == 2
            assert all(isinstance(a, Account) for a in accts)

    def test_account_identifiers(self, tmp_db) -> None:
        with Vault(tmp_db) as v:
            identifiers = {a.identifier for a in v.accounts}
            assert identifiers == {"test@gmail.com", "other@gmail.com"}


class TestVaultMessages:
    def test_iteration_yields_messages(self, tmp_db) -> None:
        with Vault(tmp_db) as v:
            msgs = list(v.messages)
            assert all(isinstance(m, Message) for m in msgs)

    def test_excludes_deleted(self, tmp_db) -> None:
        with Vault(tmp_db) as v:
            msgs = list(v.messages)
            # Message 10 is deleted, so we should get 9
            assert len(msgs) == 9
            ids = {m.id for m in msgs}
            assert 10 not in ids

    def test_ordered_by_date_desc(self, tmp_db) -> None:
        with Vault(tmp_db) as v:
            msgs = list(v.messages)
            dates = [m.sent_at for m in msgs if m.sent_at]
            assert dates == sorted(dates, reverse=True)

    def test_message_properties_accessible(self, tmp_db) -> None:
        with Vault(tmp_db) as v:
            msgs = list(v.messages)
            msg = next(m for m in msgs if m.id == 1)
            assert msg.subject == "Hello from Alice"
            assert msg.sender is not None
            assert msg.sender.email == "alice@example.com"
