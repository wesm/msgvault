"""Tests for msgvault_sdk error types."""

from msgvault_sdk.errors import (
    ChangeLogError,
    MsgvaultError,
    QueryError,
    VaultNotFoundError,
    VaultReadOnlyError,
)


def test_all_errors_are_msgvault_errors() -> None:
    assert issubclass(VaultNotFoundError, MsgvaultError)
    assert issubclass(VaultReadOnlyError, MsgvaultError)
    assert issubclass(QueryError, MsgvaultError)
    assert issubclass(ChangeLogError, MsgvaultError)


def test_vault_not_found_stores_path() -> None:
    err = VaultNotFoundError("/some/path")
    assert err.path == "/some/path"
    assert "/some/path" in str(err)


def test_vault_readonly_message() -> None:
    err = VaultReadOnlyError()
    assert "read-only" in str(err)


def test_query_error_stores_query_and_original() -> None:
    original = ValueError("bad value")
    err = QueryError("query failed", query="SELECT 1", original=original)
    assert err.query == "SELECT 1"
    assert err.original is original
    assert "query failed" in str(err)


def test_query_error_optional_fields() -> None:
    err = QueryError("simple failure")
    assert err.query is None
    assert err.original is None
