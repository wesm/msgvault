"""Tests for msgvault_sdk database connection and path resolution."""

import os
import sqlite3

import pytest

from msgvault_sdk.db import connect, find_db_path
from msgvault_sdk.errors import VaultNotFoundError


@pytest.fixture()
def tmp_db(tmp_path):
    """Create a minimal SQLite database for testing."""
    db_path = tmp_path / "msgvault.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY)")
    conn.close()
    return db_path


def test_connect_opens_wal_mode(tmp_db) -> None:
    conn = connect(tmp_db, readonly=False)
    mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
    assert mode == "wal"
    conn.close()


def test_connect_enables_foreign_keys(tmp_db) -> None:
    conn = connect(tmp_db, readonly=False)
    fk = conn.execute("PRAGMA foreign_keys").fetchone()[0]
    assert fk == 1
    conn.close()


def test_connect_row_factory(tmp_db) -> None:
    conn = connect(tmp_db, readonly=False)
    assert conn.row_factory is sqlite3.Row
    conn.close()


def test_connect_missing_file() -> None:
    with pytest.raises(VaultNotFoundError):
        connect("/nonexistent/path/msgvault.db")


def test_connect_readonly(tmp_db) -> None:
    conn = connect(tmp_db, readonly=True)
    qo = conn.execute("PRAGMA query_only").fetchone()[0]
    assert qo == 1
    with pytest.raises(sqlite3.OperationalError):
        conn.execute("CREATE TABLE should_fail (id INTEGER)")
    conn.close()


def test_connect_writable(tmp_db) -> None:
    conn = connect(tmp_db, readonly=False)
    qo = conn.execute("PRAGMA query_only").fetchone()[0]
    assert qo == 0
    conn.execute("CREATE TABLE new_table (id INTEGER)")
    conn.close()


def test_find_db_path_explicit(tmp_db) -> None:
    result = find_db_path(tmp_db)
    assert result == tmp_db.resolve()


def test_find_db_path_explicit_missing() -> None:
    with pytest.raises(VaultNotFoundError):
        find_db_path("/nonexistent/msgvault.db")


def test_find_db_path_env_var(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "msgvault.db"
    sqlite3.connect(str(db_path)).close()
    monkeypatch.setenv("MSGVAULT_HOME", str(tmp_path))
    result = find_db_path()
    assert result == db_path


def test_find_db_path_config_toml(tmp_path, monkeypatch) -> None:
    # Set up a config.toml pointing to a data_dir
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    db_path = data_dir / "msgvault.db"
    sqlite3.connect(str(db_path)).close()

    config_dir = tmp_path / ".msgvault"
    config_dir.mkdir()
    config_file = config_dir / "config.toml"
    config_file.write_text(f'[data]\ndata_dir = "{data_dir}"\n')

    monkeypatch.delenv("MSGVAULT_HOME", raising=False)
    monkeypatch.setattr("pathlib.Path.home", lambda: tmp_path)
    result = find_db_path()
    assert result == db_path


def test_find_db_path_default(tmp_path, monkeypatch) -> None:
    config_dir = tmp_path / ".msgvault"
    config_dir.mkdir()
    db_path = config_dir / "msgvault.db"
    sqlite3.connect(str(db_path)).close()

    monkeypatch.delenv("MSGVAULT_HOME", raising=False)
    monkeypatch.setattr("pathlib.Path.home", lambda: tmp_path)
    result = find_db_path()
    assert result == db_path


def test_find_db_path_nothing_found(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("MSGVAULT_HOME", raising=False)
    monkeypatch.setattr("pathlib.Path.home", lambda: tmp_path)
    with pytest.raises(VaultNotFoundError):
        find_db_path()
