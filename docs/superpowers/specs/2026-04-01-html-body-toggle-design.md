# HTML Body Toggle on Message Detail

## Summary

Add a toggle on the message detail view to switch between plain text (default) and rendered HTML. Allows viewing email formatting, layout, and images as the sender intended.

## Behavior

- **Default:** Plain text, identical to current behavior (`htmlToPlainText` conversion).
- **Toggle:** A Text/HTML pill pair above the message body (same pattern as Fast/Deep on search page). Only shown when `BodyHTML` is non-empty.
- **HTML mode:** Renders the HTML body inside a sandboxed `<iframe srcdoc="...">`. The iframe sandbox allows images but blocks scripts and form submission.
- **State:** Controlled by `?view=html` query param. Absent or `?view=text` means plain text. The toggle links preserve all other URL state (back URL context).

## Scope

- External images load freely (user has DNS-level pixel blocking).
- `cid:` inline images will appear broken — follow-up feature to add `content_id` column and rewrite URLs.
- No sanitization/allowlisting of HTML tags — the iframe sandbox provides isolation.

## Changes

### `internal/web/templates/message_detail.templ`

- Add `ViewMode string` to `MessageDetailData` (`"text"` or `"html"`).
- Add helper methods for generating toggle URLs (preserve message ID and back URL context).
- In `messageBody`: when `ViewMode == "html"` and `BodyHTML` is non-empty, render an `<iframe>` with `srcdoc` containing the HTML body. Otherwise, render plain text as today.
- Add Text/HTML toggle pills above the body card, only when `BodyHTML != ""`.

### `internal/web/handlers.go` — `handleMessageDetail`

- Read `view` query param, default to `"text"`.
- Pass `ViewMode` through `MessageDetailData`.

### `internal/web/static/style.css`

- Add iframe styling: `width: 100%`, `border: none`, min-height, and auto-resize if feasible via a small inline script in the srcdoc.

## What does NOT change

- No new routes or endpoints.
- No schema changes.
- No JavaScript additions to the parent page.
- Plain text rendering path is unchanged.
- Attachment download, deletion staging, thread view — all untouched.

## Edge cases

- **No HTML body:** Toggle not shown. Plain text or "(No message content)" as today.
- **No text body, has HTML body:** Plain text mode shows `htmlToPlainText(BodyHTML)` as today. HTML mode shows the iframe.
- **Very large HTML bodies:** Iframe handles this naturally; no truncation needed.
- **Dark mode:** The iframe content uses the email's own styling (light background typical). This is expected — emails are authored with their own colors.