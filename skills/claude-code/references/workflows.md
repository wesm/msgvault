# msgvault Workflows

Complex multi-step patterns for email analysis, classification, and export.

## Sender Graph Analysis

Build a complete picture of who emails you and how often.

### Full sender graph
```bash
# All senders ranked by volume
msgvault list-senders -n 1000 --json | jq -r '.[] | "\(.count)\t\(.key)"' | sort -rn

# Domain breakdown
msgvault list-domains -n 500 --json | jq -r '.[] | "\(.count)\t\(.key)"' | sort -rn

# Senders from a specific domain (e.g. all gmail.com senders)
msgvault search "from:@gmail.com" --limit 500 --json | \
  jq -r '[.[].from_email] | group_by(.) | map({sender: .[0], count: length}) | sort_by(-.count) | .[] | "\(.count)\t\(.sender)"'
```

### Time-scoped sender analysis
```bash
# Who emailed during the crypto era (2017-2019)?
msgvault list-senders -n 200 --after 2017-01-01 --before 2019-12-31 --json

# Recent senders only
msgvault list-senders -n 100 --after 2025-01-01 --json

# Compare sender volume across periods
for year in 2020 2021 2022 2023 2024 2025; do
  echo "=== $year ==="
  msgvault list-domains -n 10 --after $year-01-01 --before $year-12-31 --json | \
    jq -r '.[] | "\(.count)\t\(.key)"'
done
```

### Unique sender extraction for classification
```bash
# Extract unique senders with counts, suitable for review spreadsheet
msgvault list-senders -n 5000 --json | \
  jq -r '.[] | [.key, .count, (.total_size / 1024 | floor | tostring) + "K"] | @csv' \
  > senders.csv

# Extract unique domains
msgvault list-domains -n 1000 --json | \
  jq -r '.[] | [.key, .count] | @csv' > domains.csv
```

## Email Classification Pipeline

### Step 1: Domain-based classification
```bash
# Check which domains from a list appear in the archive
for domain in example.com supplier.co partner.org; do
  count=$(msgvault search "from:@$domain" --limit 1 --json | jq 'length')
  echo "$count\t$domain"
done

# Count emails per sensitive domain
for domain in $(cat sensitive-domains.txt); do
  count=$(msgvault search "from:@$domain" --limit 1 --json 2>/dev/null | jq 'length // 0')
  [ "$count" -gt 0 ] && echo "$count\t$domain"
done
```

### Step 2: Sender-based classification
```bash
# Find all emails to/from a known personal contact
msgvault search "from:person@gmail.com" --limit 500 --json
msgvault search "to:person@gmail.com" --limit 500 --json

# Batch check known personal senders
while IFS= read -r sender; do
  count=$(msgvault search "from:$sender" --limit 1 --json | jq 'length')
  [ "$count" -gt 0 ] && echo "$count\t$sender"
done < known-personal-senders.txt
```

### Step 3: Label-based classification
```bash
# See all labels and their counts
msgvault list-labels --json | jq -r '.[] | "\(.count)\t\(.key)"' | sort -rn

# Emails with a specific label
msgvault search "label:Personal" --limit 500 --json
msgvault search "label:Travel" --limit 500 --json
```

## Attachment Mining

### Find valuable attachments
```bash
# All PDF attachments
msgvault search "has:attachment" --limit 500 --json | \
  jq '[.[] | select(.has_attachments)] | length'

# Large attachments (likely documents, not inline images)
msgvault search "has:attachment larger:1M" --limit 100 --json

# Attachments from a specific sender
msgvault search "has:attachment from:accountant@firm.com" --json
```

### Batch export
```bash
# Export all attachments from matching messages
mkdir -p exports
msgvault search "has:attachment label:Personal" --limit 200 --json | \
  jq -r '.[].id' | while read id; do
    msgvault export-attachments "$id" -o ./exports/ 2>/dev/null
  done

# Export a single message as .eml for forensics
msgvault export-eml 12345 -o message.eml
```

## Thread Analysis

### Find conversation threads
```bash
# All emails in a thread with a specific person
msgvault search "from:alice@example.com" --limit 100 --json | \
  jq -r '.[].subject' | sort -u

# Cross-reference: who else is on threads with a sender
msgvault search "from:alice@example.com" --limit 50 --json | \
  jq -r '.[].to, .[].cc // empty' | tr ',' '\n' | sort -u
```

## Pagination for Large Queries

```bash
# Paginate through all results (50 at a time)
offset=0
while true; do
  results=$(msgvault search "from:@gmail.com" --limit 50 --offset $offset --json)
  count=$(echo "$results" | jq 'length')
  [ "$count" -eq 0 ] && break
  echo "$results" >> all_gmail_results.json
  offset=$((offset + 50))
done

# Simpler: fixed page count
for offset in $(seq 0 50 500); do
  msgvault search "from:@gmail.com" --limit 50 --offset $offset --json
done
```

## Reporting

### Archive overview
```bash
# Full stats
msgvault stats

# Top 20 senders
msgvault list-senders -n 20

# Top 20 domains
msgvault list-domains -n 20

# All labels
msgvault list-labels
```

### Export to CSV for spreadsheet review
```bash
# Senders CSV
msgvault list-senders -n 5000 --json | \
  jq -r '["sender","count","size_kb","attachment_kb"], (.[] | [.key, .count, (.total_size/1024|floor), (.attachment_size/1024|floor)]) | @csv' \
  > senders-report.csv

# Domains CSV
msgvault list-domains -n 1000 --json | \
  jq -r '["domain","count","size_kb"], (.[] | [.key, .count, (.total_size/1024|floor)]) | @csv' \
  > domains-report.csv
```
