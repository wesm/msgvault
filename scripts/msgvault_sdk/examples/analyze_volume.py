# /// script
# requires-python = ">=3.11"
# dependencies = ["msgvault-sdk"]
# ///
"""Show message volume by year and month.

Usage:
    uv run examples/analyze_volume.py
    uv run examples/analyze_volume.py --by month
    uv run examples/analyze_volume.py --db ~/custom/path.db
"""

from __future__ import annotations

import argparse

from msgvault_sdk import Vault


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze message volume over time")
    parser.add_argument("--db", help="Path to msgvault database")
    parser.add_argument(
        "--by", choices=["year", "month"], default="year",
        help="Group by year or month (default: year)",
    )
    args = parser.parse_args()

    with Vault(args.db) as v:
        groups = v.messages.group_by(args.by).sort_by("key")

        print(f"{'Period':<12} {'Messages':>10} {'Total Size':>12}")
        print("-" * 36)

        for g in groups:
            size_mb = g.total_size / (1024 * 1024) if g.total_size else 0
            print(f"{g.key:<12} {g.count:>10,} {size_mb:>11.1f}M")

        total = v.messages.count()
        print("-" * 36)
        print(f"{'Total':<12} {total:>10,}")


if __name__ == "__main__":
    main()
