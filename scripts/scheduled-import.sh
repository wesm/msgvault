#!/bin/bash
# scheduled-import.sh — called by com.msgvault.import-emlx launchd agent
# Waits for network + remote host, rsyncs Mail directory, then imports.

set -euo pipefail

REMOTE_HOST="blackbook.tail659d03.ts.net"
REMOTE_PATH="Library/Mail/"
LOCAL_MAIL="$HOME/mnt/mail"
MSGVAULT="$HOME/Projects/msgvault/msgvault"
IDENTIFIER="chris@4dolan.com"
MAX_WAIT=300  # 5 minutes

LOG_FILE="$HOME/Projects/msgvault/.flox/log/import-emlx.log"
LOG_MAX_LINES=1000

log() { echo "$(date '+%Y-%m-%d %H:%M:%S') [scheduled-import] $*"; }

# Trim log file to last N lines
if [ -f "$LOG_FILE" ] && [ "$(wc -l < "$LOG_FILE")" -gt "$LOG_MAX_LINES" ]; then
    tail -n "$LOG_MAX_LINES" "$LOG_FILE" > "${LOG_FILE}.tmp" && mv "${LOG_FILE}.tmp" "$LOG_FILE"
fi

# Wait for Tailscale network and remote host
log "waiting for ${REMOTE_HOST}..."
elapsed=0
while ! ssh -o ConnectTimeout=2 -o BatchMode=yes "cdolan@${REMOTE_HOST}" true 2>/dev/null; do
    elapsed=$((elapsed + 5))
    if [ "$elapsed" -ge "$MAX_WAIT" ]; then
        log "ERROR: ${REMOTE_HOST} not reachable via SSH after ${MAX_WAIT}s, aborting"
        exit 1
    fi
    sleep 5
done
log "${REMOTE_HOST} is up (waited ${elapsed}s)"

# Rsync Mail directory
mkdir -p "$LOCAL_MAIL"
log "syncing mail from ${REMOTE_HOST}..."
rsync -a --delete \
    "cdolan@${REMOTE_HOST}:${REMOTE_PATH}" \
    "$LOCAL_MAIL/"
log "sync complete"

# Verify content
if [ ! -d "$LOCAL_MAIL/V10" ]; then
    log "ERROR: V10 directory not found after sync, aborting"
    exit 1
fi

# Run import
log "starting import..."
"$MSGVAULT" import-emlx "$LOCAL_MAIL" --identifier "$IDENTIFIER" --no-resume
log "import complete"

# Purge trashed messages
log "purging trash..."
"$MSGVAULT" empty-trash
log "purge complete"

# Rebuild cache and bounce serve daemon so UI picks up changes
log "rebuilding cache..."
"$MSGVAULT" build-cache --full-rebuild
log "cache rebuilt"

log "bouncing serve daemon..."
launchctl kickstart -k "gui/$(id -u)/com.msgvault.serve"
log "done"
