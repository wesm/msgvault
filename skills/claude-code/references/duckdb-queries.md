# msgvault DuckDB Query Reference

The CLI `search` command is limited to single-operator queries. For anything complex, query the Parquet analytics cache directly with DuckDB.

**DuckDB CLI must be installed** (`which duckdb` to verify).

## Data Layout

```
~/.msgvault/analytics/
├── messages/year=YYYY/data_0.parquet   # Hive-partitioned by year
├── message_recipients/data.parquet      # from/to/cc/bcc links
├── participants/participants.parquet    # email addresses + domains
├── message_labels/data.parquet          # message ↔ label links
├── labels/labels.parquet                # label names
├── attachments/data.parquet             # attachment metadata
├── conversations/conversations.parquet  # thread grouping
└── sources/sources.parquet              # account info
```

## Table Aliases

Use these in all queries for readability:

```sql
-- Standard table references
read_parquet('~/.msgvault/analytics/messages/*/data_0.parquet', hive_partitioning=true) AS m
read_parquet('~/.msgvault/analytics/message_recipients/data.parquet') AS r
read_parquet('~/.msgvault/analytics/participants/participants.parquet') AS p
read_parquet('~/.msgvault/analytics/message_labels/data.parquet') AS ml
read_parquet('~/.msgvault/analytics/labels/labels.parquet') AS l
read_parquet('~/.msgvault/analytics/attachments/data.parquet') AS a
read_parquet('~/.msgvault/analytics/conversations/conversations.parquet') AS c
```

## Schema

### messages (partitioned by year)
| Column | Type | Notes |
|--------|------|-------|
| id | BIGINT | Primary key |
| source_id | BIGINT | FK → sources |
| source_message_id | VARCHAR | Gmail message ID |
| conversation_id | BIGINT | FK → conversations (thread) |
| subject | VARCHAR | |
| snippet | VARCHAR | Preview text |
| sent_at | TIMESTAMP | |
| size_estimate | BIGINT | Bytes |
| has_attachments | BOOLEAN | |
| deleted_from_source_at | TIMESTAMP | NULL if not deleted |
| month | INTEGER | 1-12 |
| year | BIGINT | Hive partition key |

### message_recipients
| Column | Type | Notes |
|--------|------|-------|
| message_id | BIGINT | FK → messages |
| participant_id | BIGINT | FK → participants |
| recipient_type | VARCHAR | `from`, `to`, `cc`, `bcc` |
| display_name | VARCHAR | As shown in email |

### participants
| Column | Type | Notes |
|--------|------|-------|
| id | BIGINT | Primary key |
| email_address | VARCHAR | Full address |
| domain | VARCHAR | Extracted domain |
| display_name | VARCHAR | |

### message_labels
| Column | Type | Notes |
|--------|------|-------|
| message_id | BIGINT | FK → messages |
| label_id | BIGINT | FK → labels |

### labels
| Column | Type | Notes |
|--------|------|-------|
| id | BIGINT | Primary key |
| name | VARCHAR | Gmail label name |

### attachments
| Column | Type | Notes |
|--------|------|-------|
| message_id | BIGINT | FK → messages |
| size | BIGINT | Bytes |
| filename | VARCHAR | |

## Common Joins

### Message with sender
```sql
SELECT m.id, m.subject, m.sent_at, p.email_address, p.domain
FROM read_parquet('~/.msgvault/analytics/messages/*/data_0.parquet', hive_partitioning=true) m
JOIN read_parquet('~/.msgvault/analytics/message_recipients/data.parquet') r
  ON r.message_id = m.id AND r.recipient_type = 'from'
JOIN read_parquet('~/.msgvault/analytics/participants/participants.parquet') p
  ON p.id = r.participant_id
```

### Message with labels
```sql
SELECT m.id, m.subject, l.name as label
FROM read_parquet('~/.msgvault/analytics/messages/*/data_0.parquet', hive_partitioning=true) m
JOIN read_parquet('~/.msgvault/analytics/message_labels/data.parquet') ml
  ON ml.message_id = m.id
JOIN read_parquet('~/.msgvault/analytics/labels/labels.parquet') l
  ON l.id = ml.label_id
```

## Sender Analysis Queries

### Full sender graph (top N by volume)
```sql
SELECT p.email_address, p.domain, p.display_name,
       COUNT(*) as emails,
       MIN(m.sent_at) as first_seen,
       MAX(m.sent_at) as last_seen
FROM read_parquet('~/.msgvault/analytics/messages/*/data_0.parquet', hive_partitioning=true) m
JOIN read_parquet('~/.msgvault/analytics/message_recipients/data.parquet') r
  ON r.message_id = m.id AND r.recipient_type = 'from'
JOIN read_parquet('~/.msgvault/analytics/participants/participants.parquet') p
  ON p.id = r.participant_id
GROUP BY p.email_address, p.domain, p.display_name
ORDER BY emails DESC
LIMIT 500;
```

### Multi-domain search (impossible via CLI)
```sql
SELECT p.domain, COUNT(*) as emails, COUNT(DISTINCT p.email_address) as unique_senders
FROM read_parquet('~/.msgvault/analytics/messages/*/data_0.parquet', hive_partitioning=true) m
JOIN read_parquet('~/.msgvault/analytics/message_recipients/data.parquet') r
  ON r.message_id = m.id AND r.recipient_type = 'from'
JOIN read_parquet('~/.msgvault/analytics/participants/participants.parquet') p
  ON p.id = r.participant_id
WHERE p.domain IN ('example.com', 'supplier.co', 'partner.org', 'ledger.com')
GROUP BY p.domain
ORDER BY emails DESC;
```

### Emails to/from known personal contacts
```sql
SELECT p.email_address, r.recipient_type, COUNT(*) as emails
FROM read_parquet('~/.msgvault/analytics/messages/*/data_0.parquet', hive_partitioning=true) m
JOIN read_parquet('~/.msgvault/analytics/message_recipients/data.parquet') r
  ON r.message_id = m.id
JOIN read_parquet('~/.msgvault/analytics/participants/participants.parquet') p
  ON p.id = r.participant_id
WHERE p.email_address IN ('alice@gmail.com', 'bob@example.com', 'carol@example.org')
GROUP BY p.email_address, r.recipient_type
ORDER BY emails DESC;
```

### All gmail.com senders (excluding known work contacts)
```sql
SELECT p.email_address, p.display_name, COUNT(*) as emails,
       MIN(m.sent_at) as first_seen, MAX(m.sent_at) as last_seen
FROM read_parquet('~/.msgvault/analytics/messages/*/data_0.parquet', hive_partitioning=true) m
JOIN read_parquet('~/.msgvault/analytics/message_recipients/data.parquet') r
  ON r.message_id = m.id AND r.recipient_type = 'from'
JOIN read_parquet('~/.msgvault/analytics/participants/participants.parquet') p
  ON p.id = r.participant_id
WHERE p.domain = 'gmail.com'
  AND p.email_address NOT IN ('adrian.halliday@gmail.com') -- known work
GROUP BY p.email_address, p.display_name
ORDER BY emails DESC;
```

### Senders in a time period
```sql
SELECT p.email_address, p.domain, COUNT(*) as emails
FROM read_parquet('~/.msgvault/analytics/messages/*/data_0.parquet', hive_partitioning=true) m
JOIN read_parquet('~/.msgvault/analytics/message_recipients/data.parquet') r
  ON r.message_id = m.id AND r.recipient_type = 'from'
JOIN read_parquet('~/.msgvault/analytics/participants/participants.parquet') p
  ON p.id = r.participant_id
WHERE m.year BETWEEN 2017 AND 2019
GROUP BY p.email_address, p.domain
ORDER BY emails DESC
LIMIT 100;
```

## Classification Queries

### Classify all messages by domain list
```sql
WITH sensitive_domains AS (
  SELECT unnest(['example.com','supplier.co','partner.org','anz.com.au','medibank.com.au']) as domain
),
sender_info AS (
  SELECT m.id, p.email_address, p.domain
  FROM read_parquet('~/.msgvault/analytics/messages/*/data_0.parquet', hive_partitioning=true) m
  JOIN read_parquet('~/.msgvault/analytics/message_recipients/data.parquet') r
    ON r.message_id = m.id AND r.recipient_type = 'from'
  JOIN read_parquet('~/.msgvault/analytics/participants/participants.parquet') p
    ON p.id = r.participant_id
)
SELECT s.domain, COUNT(*) as emails
FROM sender_info s
JOIN sensitive_domains sd ON s.domain = sd.domain
GROUP BY s.domain
ORDER BY emails DESC;
```

### Emails with specific labels
```sql
SELECT l.name as label, COUNT(*) as emails
FROM read_parquet('~/.msgvault/analytics/message_labels/data.parquet') ml
JOIN read_parquet('~/.msgvault/analytics/labels/labels.parquet') l
  ON l.id = ml.label_id
WHERE l.name IN ('Personal', '00_Private', 'Travel', 'Fusioneer')
GROUP BY l.name
ORDER BY emails DESC;
```

### Messages with label AND from domain
```sql
SELECT m.id, m.subject, m.sent_at, p.email_address, l.name as label
FROM read_parquet('~/.msgvault/analytics/messages/*/data_0.parquet', hive_partitioning=true) m
JOIN read_parquet('~/.msgvault/analytics/message_recipients/data.parquet') r
  ON r.message_id = m.id AND r.recipient_type = 'from'
JOIN read_parquet('~/.msgvault/analytics/participants/participants.parquet') p
  ON p.id = r.participant_id
JOIN read_parquet('~/.msgvault/analytics/message_labels/data.parquet') ml
  ON ml.message_id = m.id
JOIN read_parquet('~/.msgvault/analytics/labels/labels.parquet') l
  ON l.id = ml.label_id
WHERE l.name = 'Personal' AND p.domain = 'gmail.com'
LIMIT 50;
```

### Unclassified domains (not in any known list)
```sql
WITH known_domains AS (
  SELECT unnest([
    -- work
    'mycompany.com','mycompany.io','asana.com','slack.com','github.com',
    -- sensitive
    'example.com','supplier.co','anz.com.au','medibank.com.au',
    -- personal
    'gmail.com','hotmail.com','yahoo.com'
    -- add more...
  ]) as domain
)
SELECT p.domain, COUNT(*) as emails, COUNT(DISTINCT p.email_address) as senders
FROM read_parquet('~/.msgvault/analytics/messages/*/data_0.parquet', hive_partitioning=true) m
JOIN read_parquet('~/.msgvault/analytics/message_recipients/data.parquet') r
  ON r.message_id = m.id AND r.recipient_type = 'from'
JOIN read_parquet('~/.msgvault/analytics/participants/participants.parquet') p
  ON p.id = r.participant_id
WHERE p.domain NOT IN (SELECT domain FROM known_domains)
GROUP BY p.domain
ORDER BY emails DESC
LIMIT 100;
```

## Thread Analysis

### Co-participants in threads with a sender
```sql
WITH target_threads AS (
  SELECT DISTINCT m.conversation_id
  FROM read_parquet('~/.msgvault/analytics/messages/*/data_0.parquet', hive_partitioning=true) m
  JOIN read_parquet('~/.msgvault/analytics/message_recipients/data.parquet') r
    ON r.message_id = m.id
  JOIN read_parquet('~/.msgvault/analytics/participants/participants.parquet') p
    ON p.id = r.participant_id
  WHERE p.email_address = 'person@example.com'
)
SELECT p.email_address, p.domain, COUNT(DISTINCT m.conversation_id) as shared_threads
FROM target_threads tt
JOIN read_parquet('~/.msgvault/analytics/messages/*/data_0.parquet', hive_partitioning=true) m
  ON m.conversation_id = tt.conversation_id
JOIN read_parquet('~/.msgvault/analytics/message_recipients/data.parquet') r
  ON r.message_id = m.id
JOIN read_parquet('~/.msgvault/analytics/participants/participants.parquet') p
  ON p.id = r.participant_id
WHERE p.email_address != 'person@example.com'
GROUP BY p.email_address, p.domain
ORDER BY shared_threads DESC
LIMIT 20;
```

## Export Patterns

### Export query to CSV
```sql
COPY (
  SELECT p.email_address, p.domain, COUNT(*) as emails
  FROM read_parquet('~/.msgvault/analytics/messages/*/data_0.parquet', hive_partitioning=true) m
  JOIN read_parquet('~/.msgvault/analytics/message_recipients/data.parquet') r
    ON r.message_id = m.id AND r.recipient_type = 'from'
  JOIN read_parquet('~/.msgvault/analytics/participants/participants.parquet') p
    ON p.id = r.participant_id
  GROUP BY p.email_address, p.domain
  ORDER BY emails DESC
) TO 'senders.csv' (HEADER, DELIMITER ',');
```

### Export to JSON
```sql
COPY (
  SELECT ...
) TO 'output.json' (FORMAT JSON);
```

## Performance Tips

- Messages are **Hive-partitioned by year** — add `WHERE m.year = 2024` to limit scan scope
- Use `LIMIT` to preview before running full queries
- `COUNT(DISTINCT ...)` is expensive on large sets — use approximations if speed matters
- For repeated queries, consider creating a DuckDB view file
```
