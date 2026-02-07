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
cors_max_age = 86400       # Preflight cache duration (seconds, default: 86400)
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

Returns archive statistics.

**Response:**
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

Search messages using full-text search (FTS5 with LIKE fallback).

**Query Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `q` | string | Yes | Search query |
| `page` | int | No | Page number (default: 1) |
| `page_size` | int | No | Items per page (default: 20, max: 100) |

**Response:**
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
| `missing_query` | 400 | Required query parameter missing |
| `missing_account` | 400 | Account email is required |
| `not_found` | 404 | Resource not found |
| `sync_error` | 409 | Sync conflict (already running) |
| `internal_error` | 500 | Server error |
| `store_unavailable` | 503 | Database not available |
| `scheduler_unavailable` | 503 | Scheduler not available |

All timestamps in responses use RFC 3339 format in UTC (e.g., `"2024-01-15T10:30:00Z"`).

## Examples

### cURL

```bash
# Get stats
curl -H "X-API-Key: your-key" http://localhost:8080/api/v1/stats

# Search messages
curl -H "X-API-Key: your-key" "http://localhost:8080/api/v1/search?q=invoice"

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
