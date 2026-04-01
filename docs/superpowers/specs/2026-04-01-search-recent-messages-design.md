# Search Page: Recent Messages on Load

## Summary

Seed the search page with the 100 most recent messages (across all accounts) when no search query is active. Replaces the current empty state ("Search your archive") with an immediately useful message list.

## Behavior

1. **No query (`q` is empty):** Display the 100 most recent messages sorted by date descending, across all accounts. Filters (hide_deleted, attachments) and sort controls are ignored in this state. A "Recent messages" heading replaces the empty state.

2. **Search submitted (with or without query text):** Execute the search with all active filters (mode, hide_deleted, attachments, account) and display results as today. If the query text is empty and the user clicks Search, treat it the same as navigating to `/search` with no params — show recent messages again.

3. **Reset:** Clicking Search with an empty query field (or navigating to `/search`) returns to the recent messages view.

## Changes

### `internal/web/handlers.go` — `handleSearch`

In the `queryStr == ""` branch (currently a no-op that falls through to the empty state template):

- Call `h.engine.ListMessages(ctx, filter)` with an empty `MessageFilter` using default sort (date desc) and a limit of 100.
- Set `data.Messages` to the result.
- Add a `ShowRecent bool` field to `SearchData` and set it to `true`.

### `internal/web/templates/search.templ`

- Add `ShowRecent bool` to `SearchData`.
- Replace the `data.Query == ""` empty state block: when `ShowRecent` is true and messages are present, render a "Recent messages" heading followed by `searchMessageTable(data)`. No pagination needed (fixed at 100).
- Keep the existing empty state as fallback if the DB has zero messages.

### No other files change.

## What does NOT change

- Search form, filters, mode toggle, pagination — all unchanged.
- `/messages`, `/browse`, and all other routes — untouched.
- No new routes or endpoints.
- No HTMX or client-side fetching.

## Edge cases

- **Empty database:** Falls back to the existing "Search your archive" empty state.
- **Fewer than 100 messages:** Shows all of them, no pagination.
- **Deleted messages in recent list:** Shown (no filters applied). User can toggle hide_deleted then search to filter.
