#!/usr/bin/env bash
# msgvault DuckDB query helper
# Wraps common analytical queries against the Parquet cache
# Usage: query.sh <command> [args]
#
# Requires: duckdb on PATH
# Respects: MSGVAULT_HOME env var (default: ~/.msgvault)

set -euo pipefail

DATA="${MSGVAULT_HOME:-$HOME/.msgvault}/analytics"

# Verify analytics cache exists
if [ ! -d "$DATA/messages" ]; then
  echo "Error: Analytics cache not found at $DATA" >&2
  echo "Run 'msgvault build-cache' first." >&2
  exit 1
fi

MSG="read_parquet('$DATA/messages/*/data_0.parquet', hive_partitioning=true)"
RECIP="read_parquet('$DATA/message_recipients/data.parquet')"
PARTS="read_parquet('$DATA/participants/participants.parquet')"
LABELS="read_parquet('$DATA/labels/labels.parquet')"
MLABELS="read_parquet('$DATA/message_labels/data.parquet')"
ATTACH="read_parquet('$DATA/attachments/data.parquet')"

cmd="${1:-help}"
shift || true

case "$cmd" in
  # Full sender graph: query.sh senders [limit] [--after YYYY-MM-DD] [--before YYYY-MM-DD]
  senders)
    limit="${1:-100}"
    where=""
    shift || true
    while [[ $# -gt 0 ]]; do
      case "$1" in
        --after)  where="$where AND m.sent_at >= '$2'"; shift 2 ;;
        --before) where="$where AND m.sent_at < '$2'"; shift 2 ;;
        *) shift ;;
      esac
    done
    duckdb -c "
    SELECT p.email_address, p.domain, p.display_name, COUNT(*) as emails,
           MIN(m.sent_at) as first_seen, MAX(m.sent_at) as last_seen
    FROM $MSG m
    JOIN $RECIP r ON r.message_id = m.id AND r.recipient_type = 'from'
    JOIN $PARTS p ON p.id = r.participant_id
    WHERE 1=1 $where
    GROUP BY p.email_address, p.domain, p.display_name
    ORDER BY emails DESC LIMIT $limit;
    "
    ;;

  # Senders from specific domains: query.sh by-domain gmail.com,hotmail.com [limit]
  by-domain)
    domains="$1"
    limit="${2:-100}"
    in_list=$(echo "$domains" | sed "s/,/','/g")
    duckdb -c "
    SELECT p.email_address, p.display_name, p.domain, COUNT(*) as emails,
           MIN(m.sent_at) as first_seen, MAX(m.sent_at) as last_seen
    FROM $MSG m
    JOIN $RECIP r ON r.message_id = m.id AND r.recipient_type = 'from'
    JOIN $PARTS p ON p.id = r.participant_id
    WHERE p.domain IN ('$in_list')
    GROUP BY p.email_address, p.display_name, p.domain
    ORDER BY emails DESC LIMIT $limit;
    "
    ;;

  # Domain breakdown: query.sh domains [limit]
  domains)
    limit="${1:-100}"
    duckdb -c "
    SELECT p.domain, COUNT(*) as emails, COUNT(DISTINCT p.email_address) as unique_senders,
           SUM(m.size_estimate) as total_bytes
    FROM $MSG m
    JOIN $RECIP r ON r.message_id = m.id AND r.recipient_type = 'from'
    JOIN $PARTS p ON p.id = r.participant_id
    GROUP BY p.domain ORDER BY emails DESC LIMIT $limit;
    "
    ;;

  # Count emails per domain list: query.sh classify domain1,domain2,...
  classify)
    domains="$1"
    in_list=$(echo "$domains" | sed "s/,/','/g")
    duckdb -c "
    SELECT p.domain, COUNT(*) as emails, COUNT(DISTINCT p.email_address) as senders
    FROM $MSG m
    JOIN $RECIP r ON r.message_id = m.id AND r.recipient_type = 'from'
    JOIN $PARTS p ON p.id = r.participant_id
    WHERE p.domain IN ('$in_list')
    GROUP BY p.domain ORDER BY emails DESC;
    "
    ;;

  # Thread co-participants: query.sh threads <email>
  threads)
    email="$1"
    duckdb -c "
    WITH target_threads AS (
      SELECT DISTINCT m.conversation_id
      FROM $MSG m
      JOIN $RECIP r ON r.message_id = m.id
      JOIN $PARTS p ON p.id = r.participant_id
      WHERE p.email_address = '$email'
    )
    SELECT p.email_address, p.domain, COUNT(DISTINCT m.conversation_id) as shared_threads
    FROM target_threads tt
    JOIN $MSG m ON m.conversation_id = tt.conversation_id
    JOIN $RECIP r ON r.message_id = m.id
    JOIN $PARTS p ON p.id = r.participant_id
    WHERE p.email_address != '$email'
    GROUP BY p.email_address, p.domain
    ORDER BY shared_threads DESC LIMIT 20;
    "
    ;;

  # Label counts: query.sh labels
  labels)
    duckdb -c "
    SELECT l.name, COUNT(*) as emails
    FROM $MLABELS ml
    JOIN $LABELS l ON l.id = ml.label_id
    GROUP BY l.name ORDER BY emails DESC;
    "
    ;;

  # Messages with a specific label: query.sh label-messages <label-name> [limit]
  label-messages)
    label="$1"
    limit="${2:-50}"
    duckdb -c "
    SELECT m.id, m.subject, m.sent_at, p.email_address as sender
    FROM $MSG m
    JOIN $RECIP r ON r.message_id = m.id AND r.recipient_type = 'from'
    JOIN $PARTS p ON p.id = r.participant_id
    JOIN $MLABELS ml ON ml.message_id = m.id
    JOIN $LABELS l ON l.id = ml.label_id
    WHERE l.name = '$label'
    ORDER BY m.sent_at DESC LIMIT $limit;
    "
    ;;

  # Unclassified domains: query.sh unclassified domain1,domain2,...
  unclassified)
    domains="$1"
    in_list=$(echo "$domains" | sed "s/,/','/g")
    duckdb -c "
    SELECT p.domain, COUNT(*) as emails, COUNT(DISTINCT p.email_address) as senders
    FROM $MSG m
    JOIN $RECIP r ON r.message_id = m.id AND r.recipient_type = 'from'
    JOIN $PARTS p ON p.id = r.participant_id
    WHERE p.domain NOT IN ('$in_list')
    GROUP BY p.domain ORDER BY emails DESC LIMIT 50;
    "
    ;;

  # Raw SQL: query.sh sql "SELECT ..."
  sql)
    duckdb -c "$1"
    ;;

  help|*)
    cat <<'EOF'
msgvault DuckDB query helper

Queries the Parquet analytics cache directly for operations the CLI
search can't handle (boolean logic, multi-domain, aggregations, JOINs).

Requires: duckdb on PATH, analytics cache built (msgvault build-cache)
Respects: MSGVAULT_HOME env var (default: ~/.msgvault)

Commands:
  senders [limit] [--after DATE] [--before DATE]   Full sender graph
  by-domain <domains> [limit]                       Senders from comma-separated domains
  domains [limit]                                   Domain breakdown with sender counts
  classify <domains>                                Count emails per domain (classification)
  threads <email>                                   Co-participants in threads with sender
  labels                                            All labels with counts
  label-messages <label> [limit]                    Messages with a specific label
  unclassified <known-domains>                      Domains NOT in the provided list
  sql "<query>"                                     Raw DuckDB SQL

Examples:
  query.sh senders 50 --after 2020-01-01
  query.sh by-domain gmail.com,hotmail.com
  query.sh classify example.com,supplier.co
  query.sh threads alice@example.com
  query.sh labels
  query.sh label-messages Personal 20
  query.sh unclassified mycompany.com,asana.com,gmail.com
EOF
    ;;
esac
