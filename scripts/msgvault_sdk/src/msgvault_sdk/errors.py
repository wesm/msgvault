"""Error types for msgvault_sdk."""

from __future__ import annotations


class MsgvaultError(Exception):
    """Base exception for all msgvault_sdk errors."""


class VaultNotFoundError(MsgvaultError):
    """Database file not found at the expected path."""

    def __init__(self, path: str) -> None:
        self.path = path
        super().__init__(f"msgvault database not found: {path}")


class VaultReadOnlyError(MsgvaultError):
    """Mutation attempted on a read-only vault."""

    def __init__(self) -> None:
        super().__init__(
            "vault is opened read-only; pass writable=True to enable mutations"
        )


class QueryError(MsgvaultError):
    """SQL query execution failed."""

    def __init__(
        self, message: str, query: str | None = None, original: Exception | None = None
    ) -> None:
        self.query = query
        self.original = original
        super().__init__(message)


class ChangeLogError(MsgvaultError):
    """Change log operation failed (e.g., nothing to undo)."""
