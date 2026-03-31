---
name: msgvault-ops
description: "Local email archive operations with msgvault — search, analyze, export, and manage Gmail archives stored in SQLite/Parquet. Use when: querying email history, analyzing senders/domains, exporting messages or attachments, managing Gmail deletions, building sender graphs, running email analytics, importing mbox/emlx, or any task involving msgvault CLI. Triggers on: msgvault, email archive, email search, gmail archive, email export, sender analysis, sender graph, email classification, attachment export, email deletion, list senders, list domains, email analytics, mbox import."
---

# msgvault-ops

Operate the msgvault email archive CLI. All data is local (SQLite + Parquet). Queries run in milliseconds against DuckDB-powered indexes. Gmail API is only used for sync and deletion.

## Environment

```
Binary:  msgvault (or full path if not on PATH)
Data:    ~/.msgvault/ (override with MSGVAULT_HOME)
Config:  ~/.msgvault/config.toml
```

Ensure `msgvault` is on PATH or use the full binary path.

## Quick Reference

| Task | Command |
|------|---------|
| Archive status | `msgvault stats` |
| Search | `msgvault search "<query>" --json` |
| Top senders | `msgvault list-senders -n 100 --json` |
| Top domains | `msgvault list-domains -n 100 --json` |
| All labels | `msgvault list-labels --json` |
| Read message | `msgvault show-message <id> --json` |
| Export .eml | `msgvault export-eml <id> -o file.eml` |
| Export attachments | `msgvault export-attachments <id> -o ./dir/` |
| Incremental sync | `msgvault sync` |
| Full sync | `msgvault sync-full <email>` (resumable) |
| Build analytics cache | `msgvault build-cache` (required for DuckDB) |
| TUI | `msgvault tui` (interactive, not for agents) |

**Always use `--json` for programmatic access.** Parse with `jq`.

## Search

### Operators

Single-operator queries only. `from:` requires an **exact** email address — no fuzzy matching.

| Operator | Example | Notes |
|----------|---------|-------|
| `from:` | `from:alice@example.com` | Exact sender address |
| `from:@domain` | `from:@gmail.com` | All senders from domain |
| `to:` | `to:team@company.com` | Recipient |
| `cc:` / `bcc:` | `cc:manager@co.com` | CC/BCC fields |
| `subject:` | `subject:meeting` | Subject text |
| `label:` / `l:` | `label:INBOX` | Gmail label |
| `has:attachment` | `has:attachment` | Has attachments |
| `before:` / `after:` | `after:2024-01-01` | Date (YYYY-MM-DD) |
| `older_than:` / `newer_than:` | `newer_than:7d` | Relative (d/w/m/y) |
| `larger:` / `smaller:` | `larger:5M` | Size filter (K/M) |
| bare words | `project update` | Full-text search |
| `"quoted"` | `"exact phrase"` | Exact phrase match |

**Known limitations:** OR, NOT (-), wildcards (*), and parentheses do NOT work. For boolean/multi-domain queries, use DuckDB (see below).

### Search Strategy

The CLI search is single-operator and requires exact email addresses for `from:`. Work around this by layering tools.

**Resolve sender first, then search:**
```bash
# Don't know the email? Find it via the sender index
msgvault list-senders -n 200 --json | jq -r '.[] | .key' | grep -i lastname
# Or use the query helper for domain-scoped lookup
bash scripts/query.sh by-domain gmail.com 20
# Then search with the resolved address
msgvault search 'from:jdoe@example.com subject:proposal' -n 10 --json
```

**Narrow progressively:** Start broad (full-text), add operators (from:, subject:, date range) to filter down. Use `--json | jq` to post-filter results the CLI can't handle.

**Escape to DuckDB when CLI can't do it:** Multi-domain, boolean logic, aggregations, thread analysis — drop to `query.sh` or raw DuckDB. Don't fight the CLI's limitations.

**Stop after 5 attempts.** If targeted queries with plausible sender + keywords don't find it, more searching rarely helps. Check `msgvault list-accounts` (right account?), `msgvault stats` (sync fresh?), or suggest the user check a different account.

### Pagination

Default limit is 50. Use `--limit` and `--offset`:

```bash
msgvault search "from:@gmail.com" --limit 100 --offset 0 --json
msgvault search "from:@gmail.com" --limit 100 --offset 100 --json
```

## Common Workflows

For complete command reference with all flags, see [references/cli-reference.md](references/cli-reference.md).

For complex multi-step workflows, see [references/workflows.md](references/workflows.md).

### Sender Graph Analysis

```bash
# Top 500 senders with counts
msgvault list-senders -n 500 --json | jq -r '.[] | "\(.count)\t\(.key)"' | sort -rn

# Senders in a date range
msgvault list-senders -n 500 --after 2017-01-01 --before 2020-01-01 --json

# Domain breakdown
msgvault list-domains -n 200 --json | jq -r '.[] | "\(.count)\t\(.key)"'
```

### Message Drill-Down

```bash
# Search → get ID → read full message
msgvault search "from:alice@example.com subject:contract" --json | jq '.[0].id'
msgvault show-message <id> --json

# Extract just the body (avoids context bloat)
msgvault show-message <id> --json | jq '.body_text'

# Extract just attachments list
msgvault show-message <id> --json | jq '.attachments'
```

### Attachment Operations

```bash
# Find messages with large attachments
msgvault search "has:attachment larger:5M" --limit 100 --json

# Export all attachments from a message
msgvault export-attachments <id> -o ./exports/

# Export single attachment by SHA-256 hash (from show-message .attachments[].content_hash)
msgvault export-attachment <hash> -o file.pdf

# Batch export
msgvault search "has:attachment label:Personal" --limit 100 --json | \
  jq -r '.[].id' | while read id; do msgvault export-attachments "$id" -o ./exports/; done
```

### Deletion (Staged, Safe)

**WARNING:** `delete-staged` without `--trash` is PERMANENT and IRREVERSIBLE. Always `--dry-run` first.

Two-step process — stage in TUI, execute via CLI:

1. `msgvault tui` → navigate → select with `Space` → press `d` to stage
2. Review and execute:

```bash
msgvault list-deletions                   # review pending batches
msgvault delete-staged --dry-run          # preview what would be deleted
msgvault delete-staged --trash            # move to Gmail trash (recoverable 30 days)
msgvault delete-staged --yes              # permanent delete (IRREVERSIBLE)
msgvault cancel-deletion <batch-id>       # cancel a batch
msgvault cancel-deletion --all            # cancel all
```

Always confirm with the user before executing. Suggest `--dry-run` first.

## JSON Output Shapes (verified)

### search --json

```json
[{
  "id": 12345,
  "source_message_id": "18f0abc123",
  "conversation_id": 67890,
  "source_conversation_id": "thread-abc",
  "subject": "...",
  "from_email": "alice@example.com",
  "from_name": "Alice Smith",
  "sent_at": "2024-01-15T10:30:00Z",
  "snippet": "...",
  "labels": ["INBOX", "IMPORTANT"],
  "has_attachments": true,
  "attachment_count": 2,
  "size_estimate": 45678
}]
```

Notes:
- search returns `from_email` and `from_name` (not `from`). No `to`/`cc`/`bcc` — use `show-message` for recipients.
- **Empty results return non-JSON error text.** Always check exit code or wrap: `msgvault search "..." --json 2>/dev/null || echo '[]'`

### list-senders / list-domains / list-labels --json

```json
[{"key": "alice@example.com", "count": 142, "total_size": 5678900, "attachment_size": 1234567}]
```

### show-message --json

```json
{
  "id": 12345,
  "source_message_id": "18f0abc",
  "conversation_id": 67890,
  "source_conversation_id": "thread-abc",
  "subject": "...",
  "from": "Alice Smith <alice@example.com>",
  "to": [{"email": "bob@example.com", "name": "Bob Jones"}],
  "cc": [],
  "bcc": [],
  "sent_at": "2024-01-15T10:30:00Z",
  "labels": ["INBOX"],
  "snippet": "...",
  "has_attachments": true,
  "size_estimate": 45678,
  "body_text": "...",
  "body_html": "...",
  "attachments": [{"id": 123, "filename": "doc.pdf", "mime_type": "application/pdf", "size": 12345, "content_hash": "abc123..."}]
}
```

Notes:
- `to`/`cc`/`bcc` are **arrays of objects**: `[{"email": "...", "name": "..."}]` — extract emails with `.to[].email`
- `attachments[].content_hash` is the SHA-256 hash used by `export-attachment`
- `show-message` can return ~11k tokens for long threads. Always pipe through `jq` to extract only what you need: `.body_text`, `.attachments`, `.to[].email`, etc.

## DuckDB Queries (Advanced)

The CLI `search` is single-operator only. For boolean logic, multi-domain queries, aggregations, or cross-table joins, use DuckDB against the Parquet cache.

### Query Helper Script

`scripts/query.sh` wraps common DuckDB patterns — no raw SQL needed:

```bash
bash scripts/query.sh senders 50                                  # Top 50 senders
bash scripts/query.sh senders 50 --after 2020-01-01               # Time-scoped
bash scripts/query.sh by-domain gmail.com,hotmail.com,yahoo.com   # Senders from specific domains
bash scripts/query.sh classify example.com,supplier.co,partner.org # Count by domain list
bash scripts/query.sh threads alice@example.com                   # Thread co-participants
bash scripts/query.sh labels                                      # All labels with counts
bash scripts/query.sh label-messages Personal 20                  # Messages with label
bash scripts/query.sh unclassified mycompany.com,asana.com       # Domains NOT in list
bash scripts/query.sh sql "SELECT ..."                            # Raw SQL escape hatch
```

### Raw DuckDB (when the script doesn't cover it)

See [references/duckdb-queries.md](references/duckdb-queries.md) for full schema and query patterns.

```bash
duckdb -c "
SELECT p.domain, COUNT(*) as emails, COUNT(DISTINCT p.email_address) as senders
FROM read_parquet('~/.msgvault/analytics/messages/*/data_0.parquet', hive_partitioning=true) m
JOIN read_parquet('~/.msgvault/analytics/message_recipients/data.parquet') r
  ON r.message_id = m.id AND r.recipient_type = 'from'
JOIN read_parquet('~/.msgvault/analytics/participants/participants.parquet') p
  ON p.id = r.participant_id
WHERE p.domain IN ('example.com', 'supplier.co', 'partner.org')
GROUP BY p.domain ORDER BY emails DESC;
"
```

### Key tables (Parquet in `~/.msgvault/analytics/`)

| Table | Path | Key Columns |
|-------|------|-------------|
| messages | `messages/*/data_0.parquet` (hive by year) | id, subject, snippet, sent_at, has_attachments, year, month |
| message_recipients | `message_recipients/data.parquet` | message_id, participant_id, recipient_type (from/to/cc/bcc) |
| participants | `participants/participants.parquet` | id, email_address, domain, display_name |
| message_labels | `message_labels/data.parquet` | message_id, label_id |
| labels | `labels/labels.parquet` | id, name |
| attachments | `attachments/data.parquet` | message_id, size, filename |

**Use DuckDB when:** multi-domain IN(), boolean AND/OR/NOT, GROUP BY, JOINs, regex, window functions, CSV/JSON export, thread co-participant analysis.

**Use CLI `search` when:** simple single-field lookup, quick message retrieval by ID, full-text search on body content.

**Prerequisite:** DuckDB queries require the analytics cache. Run `msgvault build-cache` if the `analytics/` directory is missing or stale.

**Security:** The `sql` subcommand blocks write operations but can still read local files. Never pass unsanitised user input to any subcommand. Prefer validated subcommands (senders, by-domain, etc.) over raw SQL.

## Safety Rules

1. **Never delete without dry-run first** — `delete-staged --dry-run` before `--yes`
2. **Sync is read-only** — sync/sync-full never modify Gmail
3. **Deletion is two-step** — must stage in TUI first, then execute via CLI
4. **Cancel before execute** — use `cancel-deletion` if unsure
5. **Verify after sync** — `msgvault verify <email>` checks integrity
6. **Control output size** — always use `jq` with `show-message` to avoid context bloat
