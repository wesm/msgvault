# msgvault HTTP API

The msgvault HTTP API provides remote access to your email archive when running in daemon mode.

## Starting the API Server

```bash
msgvault serve
```

The server listens on `127.0.0.1:8080` by default (loopback only). Configure in `config.toml`:

```toml
[server]
api_port = 8080
bind_addr = "127.0.0.1"    # Default: loopback only
api_key = "your-secret-key" # Required for non-loopback bind addresses
```

## Security

### Bind Address

The server binds to `127.0.0.1` (loopback) by default. To expose the API on a network interface:

1. Set `bind_addr` to the desired address (e.g., `"0.0.0.0"` for all interfaces)
2. You **must** also set `api_key` — the server refuses to start on a non-loopback address without authentication

To explicitly opt out of this safety check (not recommended for production):

```toml
[server]
bind_addr = "0.0.0.0"
allow_insecure = true   # Allows unauthenticated non-loopback access
```

### Authentication

All API endpoints (except `/health`) require authentication when `api_key` is configured.

**Header Options:**
- `Authorization: Bearer <api_key>`
- `Authorization: <api_key>`
- `X-API-Key: <api_key>`

**Example:**
```bash
curl -H "Authorization: Bearer your-secret-key" http://localhost:8080/api/v1/stats
```

### CORS

CORS is disabled by default (no origins allowed). To enable CORS for browser-based clients, configure allowed origins explicitly:

```toml
[server]
cors_origins = ["http://localhost:3000", "https://myapp.example.com"]
cors_credentials = false   # Whether to allow credentials
cors_max_age = 86400       # Preflight cache duration (seconds)
```

**Allowed methods:** `GET, POST, PUT, DELETE, OPTIONS`
**Allowed headers:** `Accept, Authorization, Content-Type, X-API-Key`

## Rate Limiting

The API is rate limited to 10 requests/second per IP address with a burst allowance of 20.

Rate limited responses return HTTP 429 with a `Retry-After` header.

## Endpoints

### Health Check

```
GET /health
```

Returns server health status. No authentication required.

**Response:**
```json
{
  "status": "ok"
}
```

---

### Get Statistics

```
GET /api/v1/stats
```

Returns archive statistics. When vector search is configured on the
server, the response also includes a `vector_search` sub-object
describing the state of the index.

**Response (vector search disabled):**
```json
{
  "total_messages": 125000,
  "total_threads": 45000,
  "total_accounts": 2,
  "total_labels": 35,
  "total_attachments": 8500,
  "database_size_bytes": 52428800
}
```

**Response (vector search enabled):**
```json
{
  "total_messages": 125000,
  "total_threads": 45000,
  "total_accounts": 2,
  "total_labels": 35,
  "total_attachments": 8500,
  "database_size_bytes": 52428800,
  "vector_search": {
    "enabled": true,
    "active_generation": {
      "id": 3,
      "model": "nomic-embed-text-v1.5",
      "dimension": 768,
      "fingerprint": "nomic-embed-text-v1.5:768",
      "state": "active",
      "activated_at": "2026-04-18T15:12:33Z",
      "message_count": 124980
    },
    "building_generation": {
      "id": 4,
      "model": "nomic-embed-text-v2",
      "dimension": 768,
      "started_at": "2026-04-19T09:02:10Z",
      "progress": { "done": 8200, "total": 125000 }
    },
    "pending_embeddings_total": 116820
  }
}
```

`active_generation` is always present in the object (null until the
first build completes). `building_generation` is omitted when no
rebuild is in flight. `pending_embeddings_total` is the sum of rows
still pending across the active and building generations.

---

### List Messages

```
GET /api/v1/messages
```

Returns a paginated list of messages, sorted by date (newest first).

**Query Parameters:**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `page` | int | 1 | Page number (1-indexed) |
| `page_size` | int | 20 | Items per page (max 100) |

**Response:**
```json
{
  "total": 125000,
  "page": 1,
  "page_size": 20,
  "messages": [
    {
      "id": 12345,
      "subject": "Meeting Tomorrow",
      "from": "sender@example.com",
      "to": ["recipient@example.com"],
      "sent_at": "2024-01-15T10:30:00Z",
      "snippet": "Hi, just wanted to confirm our meeting...",
      "labels": ["INBOX", "IMPORTANT"],
      "has_attachments": false,
      "size_bytes": 2048
    }
  ]
}
```

---

### Get Message

```
GET /api/v1/messages/{id}
```

Returns a single message with full body and attachments.

**Response:**
```json
{
  "id": 12345,
  "subject": "Meeting Tomorrow",
  "from": "sender@example.com",
  "to": ["recipient@example.com"],
  "sent_at": "2024-01-15T10:30:00Z",
  "snippet": "Hi, just wanted to confirm our meeting...",
  "labels": ["INBOX", "IMPORTANT"],
  "has_attachments": true,
  "size_bytes": 15360,
  "body": "Hi,\n\nJust wanted to confirm our meeting tomorrow at 2pm...",
  "attachments": [
    {
      "filename": "agenda.pdf",
      "mime_type": "application/pdf",
      "size_bytes": 12288
    }
  ]
}
```

**Error Responses:**
- `400 Bad Request` - Invalid message ID
- `404 Not Found` - Message not found

---

### Search Messages

```
GET /api/v1/search?q={query}
```

Search messages. The default mode is full-text search (FTS5 with LIKE
fallback). When the server is configured for vector search, `mode=vector`
runs semantic-only search and `mode=hybrid` fuses BM25 and vector
ranking via Reciprocal Rank Fusion.

`mode=vector` and `mode=hybrid` both require at least one free-text term
in `q` — the free text is what gets embedded as the query vector.
Operator-only queries such as `q=from:alice` have nothing to embed and
return `400 missing_free_text`; route filter-only requests to `mode=fts`
instead.

**Query Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `q` | string | Yes | Search query |
| `mode` | enum | No | `fts` (default), `vector`, or `hybrid` |
| `page` | int | No | Page number (default: 1; FTS only — vector/hybrid reject `page>1`) |
| `page_size` | int | No | Items per page (default: 20; max 100 for FTS, max [vector].search.max_page_size_hybrid for vector/hybrid) |
| `explain` | 0/1 | No | When `1` and `mode=vector|hybrid`, include per-signal scores |

**Response (mode=fts, default):**
```json
{
  "query": "meeting tomorrow",
  "total": 42,
  "page": 1,
  "page_size": 20,
  "messages": [
    {
      "id": 12345,
      "subject": "Meeting Tomorrow",
      "from": "sender@example.com",
      "to": ["recipient@example.com"],
      "sent_at": "2024-01-15T10:30:00Z",
      "snippet": "Hi, just wanted to confirm our meeting...",
      "labels": ["INBOX"],
      "has_attachments": false,
      "size_bytes": 2048
    }
  ]
}
```

**Response (mode=vector or mode=hybrid):**
```json
{
  "query": "when is the planning offsite",
  "mode": "hybrid",
  "returned": 12,
  "pool_saturated": false,
  "generation": {
    "id": 3,
    "model": "nomic-embed-text-v1.5",
    "dimension": 768,
    "fingerprint": "nomic-embed-text-v1.5:768",
    "state": "active"
  },
  "took_ms": 84,
  "results": [
    {
      "id": 12345,
      "subject": "Q2 planning offsite agenda",
      "from": "alice@example.com",
      "to": ["team@example.com"],
      "sent_at": "2024-01-15T10:30:00Z",
      "snippet": "Proposed agenda for the offsite on...",
      "labels": ["INBOX"],
      "has_attachments": false,
      "size_bytes": 2048
    }
  ]
}
```

Vector and hybrid responses expose `returned` instead of `total`
(ANN search does not have a meaningful total count), add a
`generation` sub-object naming the index generation that answered the
query, and include `took_ms`. The top-level `results` array replaces
`messages`. `pool_saturated` is true when the number of results
equals the per-signal candidate pool, hinting that some relevant
hits may have been cut off.

When `explain=1`, each element of `results` carries an extra `score`
object exposing the fused-score components:

```json
{
  "id": 12345,
  "subject": "...",
  "score": {
    "rrf": 0.032,
    "bm25": 7.4,
    "vector": 0.82,
    "subject_boosted": true
  }
}
```

`bm25` and `vector` are omitted when the message did not appear in
that signal (i.e. BM25 missed it or ANN pool did not include it).
`subject_boosted` is true when the subject-line boost was applied.

---

### List Accounts

```
GET /api/v1/accounts
```

Returns all configured email accounts with sync status.

**Response:**
```json
{
  "accounts": [
    {
      "email": "user@gmail.com",
      "schedule": "0 2 * * *",
      "enabled": true,
      "last_sync_at": "2024-01-15T02:00:00Z",
      "next_sync_at": "2024-01-16T02:00:00Z"
    }
  ]
}
```

---

### Trigger Sync

```
POST /api/v1/sync/{account}
```

Manually trigger an incremental sync for an account.

**Path Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `account` | string | Email address of the account |

**Response (202 Accepted):**
```json
{
  "status": "accepted",
  "message": "Sync started for user@gmail.com"
}
```

**Error Responses:**
- `404 Not Found` - Account is not scheduled
- `409 Conflict` - Sync already running for this account

---

### Scheduler Status

```
GET /api/v1/scheduler/status
```

Returns the current scheduler status and all scheduled accounts.

**Response:**
```json
{
  "running": true,
  "accounts": [
    {
      "email": "user@gmail.com",
      "running": false,
      "last_run": "2024-01-15T02:00:00Z",
      "next_run": "2024-01-16T02:00:00Z",
      "schedule": "0 2 * * *",
      "last_error": ""
    }
  ]
}
```

The `running` field at the top level reflects the actual scheduler lifecycle state (true after `Start()`, false after `Stop()`). Per-account `running` indicates whether a sync is currently in progress for that account.

---

## TUI Support Endpoints

These endpoints support the remote TUI feature, allowing `msgvault tui` to work against a remote server.

### Get Aggregates

```
GET /api/v1/aggregates
```

Returns aggregate data grouped by a specified view type (senders, domains, labels, etc.).

**Query Parameters:**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `view_type` | string | required | `senders`, `sender_names`, `recipients`, `recipient_names`, `domains`, `labels`, `time` |
| `sort` | string | `count` | `count`, `size`, `attachment_size`, `name` |
| `direction` | string | `desc` | `asc`, `desc` |
| `limit` | int | 100 | Maximum rows to return |
| `time_granularity` | string | `month` | `year`, `month`, `day` (for time view) |
| `source_id` | int | - | Filter by account |
| `attachments_only` | bool | false | Only messages with attachments |
| `hide_deleted` | bool | false | Exclude deleted messages |
| `search_query` | string | - | Filter by search query |

**Response:**
```json
{
  "view_type": "senders",
  "rows": [
    {
      "key": "alice@example.com",
      "count": 150,
      "total_size": 2048000,
      "attachment_size": 512000,
      "attachment_count": 25,
      "total_unique": 1
    }
  ]
}
```

---

### Get Sub-Aggregates

```
GET /api/v1/aggregates/sub
```

Returns aggregates for a filtered subset of messages (drill-down navigation).

**Query Parameters:**
All parameters from `/aggregates`, plus filter parameters:
| Parameter | Type | Description |
|-----------|------|-------------|
| `sender` | string | Filter by sender email |
| `sender_name` | string | Filter by sender name |
| `recipient` | string | Filter by recipient email |
| `recipient_name` | string | Filter by recipient name |
| `domain` | string | Filter by domain |
| `label` | string | Filter by label |
| `time_period` | string | Filter by time period |

---

### Get Filtered Messages

```
GET /api/v1/messages/filter
```

Returns a filtered list of messages with pagination.

**Query Parameters:**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `sender` | string | - | Filter by sender |
| `domain` | string | - | Filter by domain |
| `label` | string | - | Filter by label |
| `conversation_id` | int | - | Filter by thread (for thread view) |
| `offset` | int | 0 | Pagination offset |
| `limit` | int | 500 | Pagination limit (max 500; uncapped for `conversation_id` thread fetches) |
| `sort` | string | `date` | `date`, `size`, `subject` |
| `direction` | string | `desc` | `asc`, `desc` |

**Response:**
```json
{
  "count": 150,
  "has_more": false,
  "offset": 0,
  "limit": 500,
  "messages": [
    {
      "id": 12345,
      "subject": "Meeting Tomorrow",
      "from": "sender@example.com",
      "to": ["recipient@example.com"],
      "sent_at": "2024-01-15T10:30:00Z",
      "snippet": "Hi, just wanted to confirm...",
      "labels": ["INBOX"],
      "has_attachments": false,
      "size_bytes": 2048
    }
  ]
}
```

---

### Get Total Stats

```
GET /api/v1/stats/total
```

Returns detailed statistics with optional filters.

**Query Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `source_id` | int | Filter by account |
| `attachments_only` | bool | Only messages with attachments |
| `hide_deleted` | bool | Exclude deleted messages |
| `search_query` | string | Filter by search query |

**Response:**
```json
{
  "message_count": 125000,
  "total_size": 5242880000,
  "attachment_count": 8500,
  "attachment_size": 1048576000,
  "label_count": 35,
  "account_count": 2
}
```

---

### Fast Search

```
GET /api/v1/search/fast
```

Fast metadata search (subject, sender, recipient). Does not search message body.

**Query Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `q` | string | Yes | Search query |
| `offset` | int | No | Pagination offset |
| `limit` | int | No | Pagination limit (default 100) |

Plus all filter parameters from `/messages/filter`.

**Response:**
```json
{
  "query": "invoice",
  "messages": [...],
  "total_count": 42,
  "stats": {
    "message_count": 42,
    "total_size": 1048576,
    "attachment_count": 5,
    "attachment_size": 524288,
    "label_count": 3,
    "account_count": 1
  }
}
```

---

### Deep Search

```
GET /api/v1/search/deep
```

Full-text search including message body (uses FTS5).

**Query Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `q` | string | Yes | Search query |
| `offset` | int | No | Pagination offset |
| `limit` | int | No | Pagination limit (default 100) |

**Response:**
```json
{
  "query": "project proposal",
  "messages": [...],
  "count": 15,
  "has_more": false,
  "offset": 0,
  "limit": 100
}
```

---

## Error Responses

All errors return a consistent JSON format:

```json
{
  "error": "error_code",
  "message": "Human-readable description"
}
```

**Error Codes:**
| Code | HTTP Status | Description |
|------|-------------|-------------|
| `unauthorized` | 401 | Missing or invalid API key |
| `rate_limit_exceeded` | 429 | Too many requests |
| `invalid_id` | 400 | Invalid message/resource ID |
| `invalid_mode` | 400 | `mode=` must be `fts`, `vector`, or `hybrid` |
| `missing_query` | 400 | Required query parameter missing |
| `missing_free_text` | 400 | `mode=vector` and `mode=hybrid` require free-text terms (not just filter operators); use `mode=fts` for filter-only queries |
| `missing_account` | 400 | Account email is required |
| `pagination_unsupported` | 400 | `mode=vector|hybrid` only supports `page=1` |
| `not_found` | 404 | Resource not found |
| `sync_error` | 409 | Sync conflict (already running) |
| `internal_error` | 500 | Server error |
| `store_unavailable` | 503 | Database not available |
| `scheduler_unavailable` | 503 | Scheduler not available |
| `vector_not_enabled` | 503 | Server has no vector backend configured |
| `index_stale` | 503 | Active generation does not match the configured model; run `msgvault embed --full-rebuild` |
| `index_building` | 503 | Initial vector index is still being built |

All timestamps in responses use RFC 3339 format in UTC (e.g., `"2024-01-15T10:30:00Z"`).

## Examples

### cURL

```bash
# Get stats
curl -H "X-API-Key: your-key" http://localhost:8080/api/v1/stats

# Search messages (full-text)
curl -H "X-API-Key: your-key" "http://localhost:8080/api/v1/search?q=invoice"

# Hybrid search (requires server-side vector config)
curl -H "X-API-Key: your-key" \
  "http://localhost:8080/api/v1/search?q=planning+offsite&mode=hybrid&explain=1"

# Trigger sync
curl -X POST -H "X-API-Key: your-key" http://localhost:8080/api/v1/sync/user@gmail.com
```

### Python

```python
import requests

API_URL = "http://localhost:8080/api/v1"
headers = {"X-API-Key": "your-key"}

# Get stats
stats = requests.get(f"{API_URL}/stats", headers=headers).json()
print(f"Total messages: {stats['total_messages']}")

# Search
results = requests.get(f"{API_URL}/search", params={"q": "invoice"}, headers=headers).json()
for msg in results["messages"]:
    print(f"- {msg['subject']}")
```

### JavaScript

```javascript
const API_URL = 'http://localhost:8080/api/v1';
const headers = { 'X-API-Key': 'your-key' };

// Get stats
const stats = await fetch(`${API_URL}/stats`, { headers }).then(r => r.json());
console.log(`Total messages: ${stats.total_messages}`);

// Search
const params = new URLSearchParams({ q: 'invoice' });
const results = await fetch(`${API_URL}/search?${params}`, { headers }).then(r => r.json());
results.messages.forEach(msg => console.log(`- ${msg.subject}`));
```

## Deferred Enhancements

These are tracked as follow-ups and not required for the initial merge:

- Unifying API on top of `internal/query.Engine` as the only query path
- Advanced auth models (tokens/scopes/users) beyond API key
- Multi-node daemon coordination and distributed rate limiting
- Full observability package (metrics/tracing dashboards)
- IMAP import support for Yahoo, Outlook, and other email systems
