"""Database connection and path resolution for msgvault_sdk."""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path

from msgvault_sdk.errors import VaultNotFoundError


def connect(db_path: str | Path, readonly: bool = True) -> sqlite3.Connection:
    """Open a SQLite connection to the msgvault database.

    Enables WAL mode (when writable) and foreign keys to match the Go binary's
    behavior. Read-only connections use PRAGMA query_only instead of URI mode
    so that WAL can still be read.
    """
    path = Path(db_path).expanduser().resolve()
    if not path.exists():
        raise VaultNotFoundError(str(path))

    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA foreign_keys = ON")
    if readonly:
        conn.execute("PRAGMA query_only = ON")
    return conn


def find_db_path(explicit: str | Path | None = None) -> Path:
    """Resolve the msgvault database path.

    Resolution order:
    1. Explicit argument
    2. MSGVAULT_HOME env var + /msgvault.db
    3. ~/.msgvault/config.toml -> data.data_dir
    4. ~/.msgvault/msgvault.db
    """
    if explicit is not None:
        path = Path(explicit).expanduser().resolve()
        if not path.exists():
            raise VaultNotFoundError(str(path))
        return path

    home_env = os.environ.get("MSGVAULT_HOME")
    if home_env:
        path = Path(home_env).expanduser().resolve() / "msgvault.db"
        if path.exists():
            return path

    msgvault_dir = Path.home() / ".msgvault"

    config_path = msgvault_dir / "config.toml"
    if config_path.exists():
        path = _db_path_from_config(config_path)
        if path is not None and path.exists():
            return path

    default = msgvault_dir / "msgvault.db"
    if default.exists():
        return default

    raise VaultNotFoundError(str(default))


def _db_path_from_config(config_path: Path) -> Path | None:
    """Extract database path from config.toml."""
    import tomllib

    with open(config_path, "rb") as f:
        config = tomllib.load(f)

    data = config.get("data", {})

    db_url = data.get("database_url")
    if db_url:
        return Path(db_url).expanduser().resolve()

    data_dir = data.get("data_dir")
    if data_dir:
        return Path(data_dir).expanduser().resolve() / "msgvault.db"

    return None
