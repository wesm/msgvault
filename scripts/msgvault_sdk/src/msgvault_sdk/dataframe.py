"""Pandas DataFrame conversion for msgvault_sdk queries."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd

    from msgvault_sdk.query import GroupedQuery, MessageQuery


def _import_pandas():
    """Import pandas, raising a clear error if not installed."""
    try:
        import pandas as pd
    except ImportError:
        raise ImportError(
            "pandas is required for DataFrame export. "
            "Install it with: pip install 'msgvault-sdk[pandas]'"
        ) from None
    return pd


def query_to_dataframe(query: MessageQuery) -> pd.DataFrame:
    """Convert a MessageQuery to a pandas DataFrame.

    Columns: id, date, sender, sender_domain, subject, snippet, size,
    has_attachments, is_read, labels, to, account.
    """
    pd = _import_pandas()

    rows = []
    for msg in query:
        sender = msg.sender
        labels = ", ".join(l.name for l in msg.labels)
        to = ", ".join(p.email for p in msg.to if p.email)
        # Look up account identifier from source_id
        acct_row = msg._conn.execute(
            "SELECT identifier FROM sources WHERE id = ?",
            (msg.source_id,),
        ).fetchone()
        account = acct_row["identifier"] if acct_row else None

        rows.append({
            "id": msg.id,
            "date": msg.sent_at,
            "sender": sender.email if sender else None,
            "sender_domain": sender.domain if sender else None,
            "subject": msg.subject,
            "snippet": msg.snippet,
            "size": msg.size_estimate,
            "has_attachments": msg.has_attachments,
            "is_read": msg.is_read,
            "labels": labels,
            "to": to,
            "account": account,
        })

    return pd.DataFrame(rows)


def groups_to_dataframe(grouped: GroupedQuery) -> pd.DataFrame:
    """Convert a GroupedQuery to a pandas DataFrame.

    Columns: key, count, total_size.
    """
    pd = _import_pandas()

    rows = [
        {"key": g.key, "count": g.count, "total_size": g.total_size}
        for g in grouped
    ]
    return pd.DataFrame(rows)
