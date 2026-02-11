# /// script
# requires-python = ">=3.11"
# dependencies = ["msgvault-sdk"]
# ///
"""Find and optionally delete messages from noreply senders.

Usage:
    uv run examples/cleanup_noreply.py                    # dry run
    uv run examples/cleanup_noreply.py --delete           # actually delete
    uv run examples/cleanup_noreply.py --pattern "%noreply%"
"""

from __future__ import annotations

import argparse

from msgvault_sdk import Vault


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Find and delete messages from noreply senders"
    )
    parser.add_argument("--db", help="Path to msgvault database")
    parser.add_argument(
        "--pattern", default="%noreply%",
        help="LIKE pattern for sender email (default: %%noreply%%)",
    )
    parser.add_argument(
        "--delete", action="store_true",
        help="Actually delete the messages (default is dry run)",
    )
    args = parser.parse_args()

    writable = args.delete
    with Vault(args.db, writable=writable) as v:
        query = v.messages.filter(sender_like=args.pattern)
        count = query.count()

        if count == 0:
            print(f"No messages matching sender LIKE '{args.pattern}'")
            return

        print(f"Found {count:,} messages matching sender LIKE '{args.pattern}'")

        # Show a sample
        print("\nSample messages:")
        for msg in query.limit(5):
            sender = msg.sender.email if msg.sender else "?"
            print(f"  [{msg.id}] {sender} - {msg.subject or '(no subject)'}")

        if args.delete:
            deleted = query.delete()
            print(f"\nDeleted {deleted:,} messages.")
            print("(Use vault.changelog.undo_last() to reverse this.)")
        else:
            print(f"\nDry run: pass --delete to actually delete {count:,} messages.")


if __name__ == "__main__":
    main()
