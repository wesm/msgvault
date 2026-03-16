# msgvault Web UI Plan

## Stack

- **Templ** — type-safe Go HTML templating (compiles to Go, no runtime)
- **HTMX** — HTML-over-the-wire interactivity (single JS file, ~14KB gzipped)
- **Go embed** — bundle everything into the single binary
- **chi router** — already in use for the JSON API
- **DuckDB query engine** — same `query.Engine` the TUI uses
- **Flox** — add `templ` package to manifest

No npm. No node. No JS build step. `templ generate` produces `.go` files that compile with everything else.

---

## Architecture

```
internal/
├── api/                    # Existing JSON API (unchanged)
│   ├── server.go
│   ├── handlers.go
│   └── middleware.go
│
└── web/                    # New — Web UI
    ├── server.go           # Router setup, mount on chi, embed static assets
    ├── handlers.go         # HTTP handlers (call query.Engine, render Templ)
    ├── helpers.go          # Template helpers (format bytes, dates, etc.)
    ├── static/             # Static assets (embedded via go:embed)
    │   ├── htmx.min.js     # HTMX library (vendored, ~45KB)
    │   └── style.css       # Single stylesheet
    └── templates/
        ├── layout.templ        # Base HTML shell, nav, stats bar
        ├── dashboard.templ     # Landing page — account overview + stats
        ├── aggregates.templ    # Aggregate table (senders, domains, labels, time)
        ├── messages.templ      # Message list table
        ├── message.templ       # Message detail view
        ├── thread.templ        # Thread/conversation view
        ├── search.templ        # Search results (fast + deep)
        ├── partials/
        │   ├── table.templ         # Reusable data table rows (HTMX swap target)
        │   ├── stats_bar.templ     # Stats bar fragment (msg count, size, attachments)
        │   ├── breadcrumb.templ    # Navigation breadcrumb fragment
        │   ├── pagination.templ    # Pagination controls
        │   ├── filters.templ       # Filter controls (account, attachments, deleted)
        │   └── sort_header.templ   # Clickable sort column headers
        └── components/
            ├── modal.templ         # Generic modal (delete confirm, filter select)
            └── search_bar.templ    # Search input with mode toggle
```

---

## Routing

All web UI routes mount under `/` on the existing chi router alongside `/api/v1/`.
Auth middleware applies to both — same API key mechanism.

```
GET  /                                          → Dashboard (full page)
GET  /browse                                    → Aggregates view (full page)
GET  /browse?view=senders&sort=count&dir=desc   → Aggregates with params
GET  /browse/drill?view=recipients&sender=x     → Drill-down (full page)
GET  /messages                                  → Message list (full page)
GET  /messages?sender=x&label=y                 → Filtered message list
GET  /messages/{id}                             → Message detail (full page)
GET  /messages/{id}/thread                      → Thread view (full page)
GET  /search?q=term&mode=fast                   → Search results (full page)

# HTMX partial endpoints (return HTML fragments, not full pages)
GET  /htmx/aggregates                           → Table body rows
GET  /htmx/drill                                → Drill-down table body rows
GET  /htmx/messages                             → Message list rows
GET  /htmx/search                               → Search result rows
GET  /htmx/stats                                → Stats bar fragment
GET  /htmx/breadcrumb                           → Breadcrumb fragment
GET  /htmx/filters                              → Filter panel fragment

# Static assets
GET  /static/*                                  → Embedded static files
```

### URL State

All view state lives in URL query parameters — bookmarkable, shareable, back-button works:

```
/browse?view=senders&sort=count&dir=desc&account=2&attachments=1&hide_deleted=1
/browse/drill?sender=foo@gmail.com&view=recipients&sort=count&dir=desc
/messages?sender=foo@gmail.com&label=INBOX&page=1&sort=date&dir=desc
/search?q=invoice&mode=fast&page=1
```

---

## Feature Mapping: TUI → Web

| TUI Feature | Web Implementation |
|---|---|
| **7 aggregate views** (Tab cycle) | Dropdown/tabs: Senders, Sender Names, Recipients, Recipient Names, Domains, Labels, Time |
| **Drill-down** (Enter) | Click row → `/browse/drill?sender=x&view=recipients` (HTMX pushes URL) |
| **Multi-level drill** | Breadcrumb accumulates filters; each click adds a filter param |
| **Message list** | Click aggregate row → `/messages?sender=x` or drill further |
| **Message detail** | Click message row → `/messages/{id}` |
| **Thread view** | Link from message detail → `/messages/{id}/thread` |
| **Prev/Next message** | Arrow links in detail view header |
| **Sort cycling** (s key) | Clickable column headers with sort arrows |
| **Sort direction** (r key) | Click same header toggles direction |
| **Account filter** (A key) | Dropdown in filter bar, updates via HTMX |
| **Attachment filter** (f key) | Checkbox toggle in filter bar |
| **Hide deleted filter** (f key) | Checkbox toggle in filter bar |
| **Time granularity** (t key) | Year/Month/Day toggle buttons in Time view |
| **Fast search** (/) | Search input with debounce (`hx-trigger="keyup changed delay:200ms"`) |
| **Deep search** (Tab in search) | Toggle button switches `mode=fast` ↔ `mode=deep` |
| **Selection + deletion staging** | Checkboxes on rows → "Stage for Deletion" button → confirm modal |
| **Stats bar** | Persistent stats bar updated via HTMX on filter/view changes |
| **Breadcrumb** | Clickable breadcrumb trail showing drill path |
| **Pagination** | Page controls (Prev/Next/page numbers) or infinite scroll via `hx-trigger="revealed"` |
| **Help** (?) | Not needed — controls are visible |

---

## Query Engine Integration

The web handlers call the **same `query.Engine` methods** the TUI uses.
No new query code needed.

| Web Handler | Engine Method | Notes |
|---|---|---|
| `handleDashboard` | `GetTotalStats()`, `ListAccounts()` | Landing page stats |
| `handleAggregates` | `Aggregate(viewType, opts)` | Main browse view |
| `handleDrill` | `SubAggregate(filter, viewType, opts)` | Drill-down |
| `handleMessages` | `ListMessages(filter)` | Filtered message list |
| `handleMessageDetail` | `GetMessage(id)` | Full message view |
| `handleThread` | `ListMessages(filter{ConversationID})` | Thread messages |
| `handleSearch` | `SearchFastWithStats()` or `Search()` | Fast vs deep search |
| `handleStats` | `GetTotalStats(opts)` | Stats bar partial |
| `handleStageDelete` | `GetGmailIDsByFilter(filter)` | Deletion staging |

### Filter → Query Parameter Mapping

URL params map directly to query engine types:

```go
func filterFromRequest(r *http.Request) query.MessageFilter {
    return query.MessageFilter{
        Sender:                r.URL.Query().Get("sender"),
        SenderName:            r.URL.Query().Get("sender_name"),
        Recipient:             r.URL.Query().Get("recipient"),
        RecipientName:         r.URL.Query().Get("recipient_name"),
        Domain:                r.URL.Query().Get("domain"),
        Label:                 r.URL.Query().Get("label"),
        SourceID:              parseOptionalInt64(r, "account"),
        WithAttachmentsOnly:   r.URL.Query().Get("attachments") == "1",
        HideDeletedFromSource: r.URL.Query().Get("hide_deleted") == "1",
        Pagination:            paginationFromRequest(r),
        Sorting:               sortingFromRequest(r),
    }
}
```

---

## Server Integration

The web UI mounts on the **existing** `api.Server` router in `server.go`:

```go
// internal/api/server.go — add to setupRouter()
func (s *Server) setupRouter() {
    // ... existing middleware ...

    // Existing JSON API
    r.Route("/api/v1", func(r chi.Router) { /* ... existing ... */ })

    // New: Web UI
    webHandler := web.NewHandler(s.engine, s.store, s.scheduler)
    r.Mount("/", webHandler.Routes())
}
```

The `web.Handler` receives the same `query.Engine` the TUI uses. This means:
- The `serve` command needs to initialize the query engine (currently only TUI does this)
- The Parquet cache must exist (auto-build on serve start, same as TUI does)

### Changes to `serve.go`

1. Initialize `query.Engine` (DuckDB over Parquet) alongside the store
2. Auto-build Parquet cache if stale (reuse `build_cache.go` logic)
3. Pass engine to `api.Server`
4. Web UI available at `http://localhost:8080/`

---

## Styling Approach

Single CSS file, no framework. Minimal, functional design:

- CSS custom properties for theming (light/dark via `prefers-color-scheme`)
- CSS Grid for page layout (sidebar/main or full-width)
- Native HTML `<table>` for data tables (fast rendering, accessible)
- System font stack (no web fonts to load)
- Responsive: tables scroll horizontally on mobile
- Color palette: monochrome with accent color for interactive elements
- File size target: < 5KB CSS

---

## Flox Changes

Add `templ` to the Flox manifest:

```toml
[install]
go.pkg-path = "go"
go.version = "^1.25.7"
templ.pkg-path = "templ"
```

Add a Makefile target:

```makefile
generate:
	templ generate

build: generate
	go build -o msgvault ./cmd/msgvault
```

---

## Implementation Phases

### Phase 1: Foundation
- [ ] Add `templ` to Flox manifest
- [ ] Create `internal/web/` package structure
- [ ] Vendor `htmx.min.js` into `static/`
- [ ] Write `layout.templ` — HTML shell with head, nav, content slot, stats bar
- [ ] Write `style.css` — base styles, table styles, responsive layout
- [ ] Write `server.go` — chi sub-router, static file serving via `go:embed`
- [ ] Wire into existing `api.Server` and `serve.go` command
- [ ] Initialize query engine in `serve` command (port logic from `tui.go`)
- [ ] `GET /` → dashboard with stats and account list
- [ ] Verify: `make build && ./msgvault serve` → browser shows dashboard

### Phase 2: Browse & Drill-Down
- [ ] `aggregates.templ` — data table with view type selector
- [ ] `GET /browse?view=senders` — aggregate view with all 7 view types
- [ ] Clickable column headers for sort field + direction toggle
- [ ] Account/attachment/deleted filter controls in filter bar
- [ ] `GET /browse/drill?sender=x&view=recipients` — drill-down view
- [ ] Breadcrumb navigation showing drill path
- [ ] Time view with Year/Month/Day granularity toggle
- [ ] HTMX partials: table rows swap on view/sort/filter change
- [ ] Stats bar updates on filter changes

### Phase 3: Messages & Detail
- [ ] `messages.templ` — message list table with pagination
- [ ] Click aggregate row → message list filtered by that key
- [ ] `message.templ` — full message detail (metadata, body, attachments)
- [ ] Prev/Next navigation between messages
- [ ] Thread view — list all messages in conversation
- [ ] Link from message detail to thread view
- [ ] Pagination controls (page numbers or infinite scroll)

### Phase 4: Search
- [ ] `search_bar.templ` — persistent search input in header/nav
- [ ] Fast search with debounced HTMX requests (`delay:200ms`)
- [ ] Search results page with message list
- [ ] Fast/Deep mode toggle
- [ ] Search within aggregate views (filter aggregates by search term)
- [ ] Search result count and pagination

### Phase 5: Deletion Staging
- [ ] Row checkboxes for selection
- [ ] "Select all on page" control
- [ ] "Stage for Deletion" button → confirmation modal
- [ ] Modal shows count, batch ID preview
- [ ] POST endpoint creates deletion manifest
- [ ] Success modal shows batch ID and `delete-staged` command

### Phase 6: Polish
- [ ] Dark/light theme support via `prefers-color-scheme`
- [ ] Loading indicators for HTMX requests (`htmx:beforeRequest` class)
- [ ] Empty states (no messages, no results, no accounts)
- [ ] Error states (query failures, connection issues)
- [ ] Mobile responsive layout
- [ ] Keyboard shortcuts (optional, via small Alpine.js or vanilla JS)
- [ ] Cache-Control headers for static assets

---

## What We're NOT Building

- No user authentication system (API key only, same as existing)
- No real-time sync status (poll or manual refresh is fine)
- No inline email composition or reply
- No inline attachment preview (download only)
- No settings/config UI (edit `config.toml` directly)
- No WebSocket connections

---

## File Count Estimate

```
New files:       ~20
  templates:     ~12 (.templ files)
  Go:            ~4  (server.go, handlers.go, helpers.go, helpers_test.go)
  Static:        ~2  (htmx.min.js, style.css)
  Tests:         ~2  (handlers_test.go, integration_test.go)

Modified files:  ~4
  api/server.go      — mount web routes
  cmd/.../serve.go   — init query engine, pass to server
  Makefile            — add generate target
  .flox/manifest.toml — add templ
```

---

## Key Design Decisions

1. **Templ over html/template**: Type-safe, LSP support, compile-time errors, composable components. Worth the `templ generate` step.

2. **HTMX over SPA**: The TUI's interaction model is request-response (user acts → data loads → view updates). HTMX models this perfectly. No client-side state management needed.

3. **Same query engine, not the JSON API**: The existing JSON API uses the `Store` interface (direct SQLite) which is slower for aggregates. The TUI's `query.Engine` (DuckDB over Parquet) is ~3000x faster for the aggregate views that make up most of the UI. The web handlers call the engine directly, not the JSON API.

4. **URL-driven state**: Every view state is a URL. No client-side routing, no localStorage state. Back button works. Links are shareable. Server always knows the full context.

5. **No Alpine.js initially**: HTMX handles 95% of interactions. If we need client-side behavior (keyboard shortcuts, dropdown menus), we add a small vanilla JS file or Alpine.js later.

6. **Vendor HTMX**: No CDN dependency. Single file embedded in binary. Works fully offline, which fits the "offline archive" philosophy.
