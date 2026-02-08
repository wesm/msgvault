#!/bin/bash
# update-nix-flake.sh — Update flake.nix vendorHash (and optionally version),
# then open a PR. Designed to be run after dependency changes or releases.
#
# Usage:
#   ./scripts/update-nix-flake.sh              # Update vendorHash only
#   ./scripts/update-nix-flake.sh v0.2.0       # Update vendorHash + version
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

VERSION_TAG="${1:-}"

# Require nix and gh
for cmd in nix gh sed; do
    if ! command -v "$cmd" &>/dev/null; then
        echo "Error: $cmd is required but not found" >&2
        exit 1
    fi
done

# Must be on a clean main branch
if ! git diff-index --quiet HEAD --; then
    echo "Error: uncommitted changes. Commit or stash first." >&2
    exit 1
fi

BRANCH="update-nix-flake"
if [ -n "$VERSION_TAG" ]; then
    BRANCH="update-nix-flake-${VERSION_TAG}"
fi

echo "==> Creating branch $BRANCH from origin/main..."
git fetch origin
git checkout -b "$BRANCH" origin/main

# Step 1: Update version in flake.nix if a version tag was provided
if [ -n "$VERSION_TAG" ]; then
    # Strip leading 'v' if present
    VERSION="${VERSION_TAG#v}"
    echo "==> Updating version to $VERSION..."
    sed -i.bak -E "s/version = \"[^\"]+\"/version = \"$VERSION\"/" flake.nix
    rm -f flake.nix.bak
fi

# Step 2: Compute the correct vendorHash
echo "==> Computing vendorHash (this runs nix build with a fake hash)..."

# Set a fake hash to force nix to report the correct one
sed -i.bak -E 's|vendorHash = "sha256-[^"]+"|vendorHash = "sha256-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="|' flake.nix
rm -f flake.nix.bak

# Run nix build — it will fail and print the correct hash
CORRECT_HASH=""
BUILD_OUTPUT=$(nix build 2>&1 || true)
CORRECT_HASH=$(echo "$BUILD_OUTPUT" | sed -n 's/.*got:[[:space:]]*\(sha256-[A-Za-z0-9+/=]*\).*/\1/p' | head -1)

if [ -z "$CORRECT_HASH" ]; then
    echo "Error: Could not extract vendorHash from nix build output:" >&2
    echo "$BUILD_OUTPUT" >&2
    git checkout -- flake.nix
    git checkout -
    git branch -D "$BRANCH"
    exit 1
fi

echo "==> Got vendorHash: $CORRECT_HASH"
sed -i.bak -E "s|vendorHash = \"sha256-[^\"]+\"|vendorHash = \"$CORRECT_HASH\"|" flake.nix
rm -f flake.nix.bak

# Step 3: Verify it actually builds
echo "==> Verifying nix build succeeds..."
if ! nix build; then
    echo "Error: nix build failed even with updated hash" >&2
    git checkout -- flake.nix
    git checkout -
    git branch -D "$BRANCH"
    exit 1
fi

# Step 4: Commit and open PR
echo "==> Committing and opening PR..."
git add flake.nix

COMMIT_MSG="Update nix flake vendorHash"
PR_TITLE="Update nix flake vendorHash"
if [ -n "$VERSION_TAG" ]; then
    COMMIT_MSG="Update nix flake for $VERSION_TAG"
    PR_TITLE="Update nix flake for $VERSION_TAG"
fi

git commit -m "$COMMIT_MSG"
git push -u origin "$BRANCH"

gh pr create \
    --title "$PR_TITLE" \
    --body "$(cat <<EOF
## Summary
- Updated \`vendorHash\` to \`$CORRECT_HASH\`$([ -n "$VERSION_TAG" ] && echo "
- Updated version to \`${VERSION_TAG#v}\`")

Automated update via \`scripts/update-nix-flake.sh\`.
EOF
)"

echo ""
echo "Done! PR created."
