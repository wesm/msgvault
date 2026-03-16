# Web UI

The web UI provides a browser-based interface for browsing, searching, and managing your msgvault email archive. It runs as part of the HTTP API server and requires no separate process.

## Starting the Web UI

```bash
# Start the server (web UI + API)
msgvault serve

# Custom port and bind address
msgvault serve --port 9090 --bind 0.0.0.0
```

The web UI is available at `http://127.0.0.1:8080/` by default.

**Prerequisites:** The Parquet analytics cache must be built before using the web UI:

```bash
msgvault build-cache
```

## Pages

### Dashboard (`/`)

Overview of your archive with message counts, total size, date range, and per-account statistics.

### Browse (`/browse`)

Aggregate view of messages grouped by sender, sender name, recipient, domain, label, or time period. Click any row to drill down into sub-groups, or click the message count to view individual messages.

**Controls:**
- View tabs: Senders, Names, Recipients, Domains, Labels, Time
- Sort: Name, Count, Size, Attachment Size
- Filters: Account selector, Attachments Only, Hide Deleted
- Time granularity: Year, Month, Day (in Time view)

### Search (`/search`)

Full-text search with Gmail-like query syntax.

**Search modes:**
- **Fast** (default): Searches subject and sender metadata. Returns results quickly with aggregate stats.
- **Deep**: Searches full message body text via FTS5. Slower but more thorough.

**Query syntax examples:**
- `from:user@example.com` — messages from a specific sender
- `subject:invoice` — subject line contains "invoice"
- `has:attachment` — messages with attachments
- `before:2024/01/01` — messages before a date
- `after:2023/06/01 from:boss@company.com` — combined filters

Search results support sorting by date, subject, or size.

### Message Detail (`/messages/{id}`)

Full message view showing headers (From, To, Cc, Bcc), labels, attachments with download links, and message body.

**Features:**
- Attachment download (click filename to download)
- Thread view link (View thread)
- Stage for deletion button
- Prev/next navigation between messages (when navigating from a list)

### Deletions (`/deletions`)

Manage deletion batches. Messages staged for deletion from the web UI appear here as pending batches. Execute deletions from the CLI:

```bash
msgvault delete-staged              # Execute all pending
msgvault delete-staged <batch-id>   # Execute specific batch
msgvault delete-staged --dry-run    # Preview without deleting
msgvault delete-staged --trash      # Move to trash (recoverable)
```

## Keyboard Shortcuts

The web UI supports vim-style keyboard navigation:

| Key | Action |
|-----|--------|
| `j` / `k` | Navigate rows up / down |
| `Enter` | Drill into selected row |
| `o` | Open messages for row |
| `g` / `G` | Jump to first / last row |
| `n` / `p` | Next / previous page |
| `←` / `→` | Previous / next message (detail view) |
| `/` | Focus search input (or go to search) |
| `Esc` | Blur search / close help / exit delete mode |
| `Backspace` | Go back (breadcrumb) |
| `H` | Go to Dashboard |
| `B` | Go to Browse |
| `?` | Toggle keyboard shortcuts help |

### Delete Mode

Press `d` to enter delete mode, which reveals checkboxes on message lists:

| Key | Action |
|-----|--------|
| `d` | Enter delete mode |
| `Space` | Toggle selection on current row |
| `A` | Select all messages on page |
| `x` | Clear selection |
| `Esc` | Exit delete mode |

## Theme

The UI uses the Solarized color scheme with light and dark modes. Click the theme toggle button (top right) to cycle between auto (follows OS preference), dark, and light modes. The preference is saved in localStorage.

## Architecture

The web UI is built with:

- **[Templ](https://templ.guide/)** — Type-safe Go HTML templates (compiled to Go)
- **[chi](https://github.com/go-chi/chi)** — HTTP router
- **Vanilla JS** — Keyboard shortcuts and interactions (no framework)
- **go:embed** — Static assets (CSS, JS) embedded in the binary

All pages are server-rendered. The query engine (DuckDB over Parquet) provides fast aggregate queries for the browse and search views.

### File Structure

```
internal/web/
├── server.go              # Handler struct, route registration
├── handlers.go            # Page handlers (dashboard, browse, search, etc.)
├── handlers_deletions.go  # Deletion staging handlers
├── params.go              # Query parameter parsing
├── static/
│   ├── style.css          # Solarized theme, all styles
│   └── keys.js            # Keyboard shortcuts, delete mode, theme toggle
└── templates/
    ├── layout.templ        # Base layout, nav, help overlay
    ├── dashboard.templ     # Dashboard page
    ├── aggregates.templ    # Browse/drill-down views
    ├── messages.templ      # Message list with sort controls
    ├── message_detail.templ # Single message view
    ├── search.templ        # Search page with sort controls
    ├── deletions.templ     # Deletion batch management
    ├── stats.templ         # Stats bar component
    └── helpers.go          # Template helper functions
```

### Security

- API key authentication (shared with REST API, configured in `config.toml`)
- Attachment downloads validate content hashes (SHA-256, 64 hex chars) to prevent path traversal
- Deletion manifest IDs are validated against a strict regex pattern
- Back-link URLs are restricted to same-origin paths
- Filenames in Content-Disposition headers are sanitized
