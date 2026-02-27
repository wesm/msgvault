# msgvault Desktop App Proposal

## Overview

This document proposes the development of a native desktop application for msgvault using **Rust** and the **Iced** GUI framework. The desktop app will provide a modern, cross-platform alternative to the terminal UI (TUI).

## Motivation

While the TUI is powerful, a native desktop app offers:

- **Richer UI** - Better message rendering, inline attachments, syntax highlighting
- **Mouse-friendly** - Click-based navigation alongside keyboard shortcuts
- **Native experience** - Platform-appropriate menus, dialogs, and behaviors
- **Accessibility** - Better screen reader support and high-contrast themes

## Architecture

The desktop app will be an **HTTP API client** that communicates with the existing `msgvault serve` backend:

```
┌─────────────────────────────┐     HTTP/REST     ┌─────────────────────────────┐
│  msgvault-desktop (Rust)    │  <───────────>    │  msgvault server (Go)       │
│  - Iced GUI framework       │                   │  - All existing endpoints   │
│  - reqwest HTTP client      │                   │  - SQLite/Parquet storage   │
└─────────────────────────────┘                   └─────────────────────────────┘
```

This approach:
- Reuses all existing Go backend logic
- Works with both local and remote (NAS) deployments
- Keeps the desktop app focused on UI concerns

## Scope

**Full TUI feature parity plus:**

| Feature | TUI | Desktop |
|---------|-----|---------|
| Aggregate views (7 types) | ✓ | ✓ |
| Drill-down navigation | ✓ | ✓ |
| Fast/deep search | ✓ | ✓ |
| Selection & deletion | ✓ | ✓ |
| Account setup & OAuth | CLI | ✓ GUI |
| Sync management | CLI | ✓ GUI |
| Settings UI | config.toml | ✓ GUI |
| First-run wizard | CLI | ✓ GUI |

## Technology Stack

- **Language:** Rust
- **GUI Framework:** [Iced](https://iced.rs/) - Elm-inspired, cross-platform
- **HTTP Client:** reqwest + serde
- **Platforms:** macOS, Linux, Windows

## Timeline

**Estimated: 8-12 weeks** (10-15 hours/week)

Phased implementation with stacked PRs:
1. Project scaffolding & basic app (Week 1)
2. Stats display & navigation (Week 1-2)
3. Aggregate views & drill-down (Week 2-3)
4. Message list & detail views (Week 3-4)
5. Search functionality (Week 4-5)
6. Selection & deletion staging (Week 5-6)
7. Sync management (Week 6-7)
8. Account setup with OAuth (Week 7-8)
9. Configuration UI (Week 8-9)
10. First-run experience & polish (Week 9-10)

## Repository

Development will happen in a separate repository:
- **Repo:** `EconoBen/msgvault-desktop`
- **PRs:** Stacked PRs against that repo, with periodic updates here

## New Server Endpoints

Some features require new API endpoints (to be added to the Go server):

| Endpoint | Purpose |
|----------|---------|
| `POST /api/v1/deletion/stage` | Stage messages for deletion |
| `GET /api/v1/sync/{account}/progress` | Sync progress polling |
| `POST /api/v1/auth/initiate/{email}` | Start browser OAuth flow |
| `DELETE /api/v1/accounts/{email}` | Remove account |

These will be proposed as separate PRs when needed.

## Questions for Maintainers

1. Any concerns with this approach?
2. Preferences on where new server endpoints should live?
3. Interest in eventually merging the desktop app into this repo?

---

*This is a draft proposal. Feedback welcome!*
