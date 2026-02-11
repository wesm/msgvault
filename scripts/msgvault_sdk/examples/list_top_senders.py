# /// script
# requires-python = ">=3.11"
# dependencies = ["msgvault-sdk"]
# ///
"""List top senders by message count.

Usage:
    uv run examples/list_top_senders.py
    uv run examples/list_top_senders.py --limit 10
    uv run examples/list_top_senders.py --db ~/custom/path.db
"""

from __future__ import annotations

import argparse

from msgvault_sdk import Vault


def main() -> None:
    parser = argparse.ArgumentParser(description="List top senders by message count")
    parser.add_argument("--db", help="Path to msgvault database")
    parser.add_argument("--limit", type=int, default=20, help="Number of senders to show")
    args = parser.parse_args()

    with Vault(args.db) as v:
        groups = v.messages.group_by("sender").sort_by("count", desc=True)

        print(f"{'Sender':<40} {'Count':>8} {'Total Size':>12}")
        print("-" * 62)

        for i, g in enumerate(groups):
            if i >= args.limit:
                break
            size_mb = g.total_size / (1024 * 1024) if g.total_size else 0
            print(f"{g.key:<40} {g.count:>8,} {size_mb:>11.1f}M")


if __name__ == "__main__":
    main()
