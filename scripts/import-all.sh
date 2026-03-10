#!/bin/bash
set -e

usage() {
  echo "Usage: $0 <account-email> <mail-dir>"
  echo
  echo "Import all Apple Mail account directories from a V10 (or similar) mail directory."
  echo
  echo "Arguments:"
  echo "  account-email   Email address to associate with imported messages"
  echo "  mail-dir        Path to Apple Mail directory containing account GUIDs"
  echo "                  (e.g. ~/Library/Mail/V10)"
  echo
  echo "Example:"
  echo "  $0 me@gmail.com ~/Library/Mail/V10"
  exit 1
}

if [ $# -ne 2 ]; then
  usage
fi

ACCOUNT="$1"
MAIL_DIR="$2"
MSGVAULT="${MSGVAULT:-./msgvault}"

if [ ! -d "$MAIL_DIR" ]; then
  echo "Error: mail directory not found: $MAIL_DIR" >&2
  exit 1
fi

if [ ! -x "$MSGVAULT" ]; then
  echo "Error: msgvault binary not found at $MSGVAULT" >&2
  echo "Set MSGVAULT env var or run from the project root after 'make build'" >&2
  exit 1
fi

imported=0
failed=0

for dir in "$MAIL_DIR"/*/; do
  # Skip if not a directory
  [ -d "$dir" ] || continue

  name=$(basename "$dir")

  # Skip known non-account directories
  case "$name" in
    Signatures|MailData|Bundles|"RSS") continue ;;
  esac

  echo "=== Importing $name ==="
  if "$MSGVAULT" import-emlx "$ACCOUNT" "$dir"; then
    imported=$((imported + 1))
  else
    echo "  (failed or empty, continuing)"
    failed=$((failed + 1))
  fi
  echo
done

echo "Done. Imported: $imported, Failed/empty: $failed"
