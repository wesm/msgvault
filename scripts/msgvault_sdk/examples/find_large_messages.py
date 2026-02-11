# /// script
# requires-python = ">=3.11"
# dependencies = ["msgvault-sdk"]
# ///
"""Find messages over a given size threshold.

Usage:
    uv run examples/find_large_messages.py
    uv run examples/find_large_messages.py --min-size 10485760   # 10 MB
    uv run examples/find_large_messages.py --limit 50
"""

from __future__ import annotations

import argparse

from msgvault_sdk import Vault


def main() -> None:
    parser = argparse.ArgumentParser(description="Find large messages")
    parser.add_argument("--db", help="Path to msgvault database")
    parser.add_argument(
        "--min-size", type=int, default=5 * 1024 * 1024,
        help="Minimum size in bytes (default: 5 MB)",
    )
    parser.add_argument("--limit", type=int, default=20, help="Max messages to show")
    args = parser.parse_args()

    with Vault(args.db) as v:
        query = (
            v.messages
            .filter(min_size=args.min_size)
            .sort_by("size", desc=True)
            .limit(args.limit)
        )

        threshold_mb = args.min_size / (1024 * 1024)
        total = v.messages.filter(min_size=args.min_size).count()
        print(f"Messages >= {threshold_mb:.1f} MB: {total:,} total\n")

        print(f"{'ID':>8} {'Size':>10} {'From':<30} {'Subject'}")
        print("-" * 80)

        for msg in query:
            size_mb = msg.size_estimate / (1024 * 1024) if msg.size_estimate else 0
            sender = msg.sender.email if msg.sender else "?"
            if len(sender) > 28:
                sender = sender[:25] + "..."
            subj = msg.subject or "(no subject)"
            if len(subj) > 40:
                subj = subj[:37] + "..."
            print(f"{msg.id:>8} {size_mb:>9.1f}M {sender:<30} {subj}")


if __name__ == "__main__":
    main()
