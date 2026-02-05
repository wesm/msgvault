# msgvault HTTP API

The msgvault HTTP API provides remote access to your email archive when running in daemon mode.

## Starting the API Server

```bash
msgvault serve
```

The server listens on port 8080 by default. Configure in `config.toml`:

```toml
[server]
api_port = 8080
api_key = "your-secret-key"
```

## Authentication

All API endpoints (except `/health`) require authentication when `api_key` is configured.

**Header Options:**
- `Authorization: Bearer <api_key>`
- `Authorization: <api_key>`
- `X-API-Key: <api_key>`

**Example:**
```bash
curl -H "Authorization: Bearer your-secret-key" http://localhost:8080/api/v1/stats
```

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

Search messages using full-text search.

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

**Error Response (409 Conflict):**
```json
{
  "error": "sync_error",
  "message": "sync already running for user@gmail.com"
}
```

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

## Error Responses

All errors return a consistent JSON format:

```json
{
  "error": "error_code",
  "message": "Human-readable description"
}
```

**Common Error Codes:**
| Code | HTTP Status | Description |
|------|-------------|-------------|
| `unauthorized` | 401 | Missing or invalid API key |
| `rate_limit_exceeded` | 429 | Too many requests |
| `invalid_id` | 400 | Invalid message/resource ID |
| `missing_query` | 400 | Required query parameter missing |
| `not_found` | 404 | Resource not found |
| `internal_error` | 500 | Server error |
| `store_unavailable` | 503 | Database not available |

## CORS

The API supports Cross-Origin Resource Sharing (CORS) for browser-based clients:

- **Allowed Origins:** `*` (configurable)
- **Allowed Methods:** `GET, POST, PUT, DELETE, OPTIONS`
- **Allowed Headers:** `Accept, Authorization, Content-Type, X-API-Key`
- **Preflight Cache:** 24 hours

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
