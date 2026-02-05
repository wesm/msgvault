# msgvault

[![Go 1.25+](https://img.shields.io/badge/Go-1.25+-00ADD8?logo=go)](https://go.dev)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Docs](https://img.shields.io/badge/Docs-msgvault.io-blue)](https://msgvault.io)
[![Discord](https://img.shields.io/badge/Discord-Join-5865F2?logo=discord&logoColor=white)](https://discord.gg/fDnmxB8Wkq)

[Documentation](https://msgvault.io) · [Setup Guide](https://msgvault.io/guides/oauth-setup/) · [Interactive TUI](https://msgvault.io/usage/tui/)

> **Alpha software.** APIs, storage format, and CLI flags may change without notice. Back up your data.

Archive a lifetime of email. Analytics and search in milliseconds, entirely offline.

## Why msgvault?

Your messages are yours. Decades of correspondence, attachments, and history shouldn't be locked behind a web interface or an API. msgvault downloads a complete local copy and then everything runs offline. Search, analytics, and the MCP server all work against local data with no network access required.

Currently supports Gmail, with WhatsApp and other messaging platforms planned.

## Features

- **Full Gmail backup**: raw MIME, attachments, labels, and metadata
- **Interactive TUI**: drill-down analytics over your entire message history, powered by DuckDB over Parquet
- **Full-text search**: FTS5 with Gmail-like query syntax (`from:`, `has:attachment`, date ranges)
- **MCP server**: access your full archive at the speed of thought in Claude Desktop and other MCP-capable AI agents
- **DuckDB analytics**: millisecond aggregate queries across hundreds of thousands of messages in the TUI, CLI, and MCP server
- **Incremental sync**: History API picks up only new and changed messages
- **Multi-account**: archive several Gmail accounts in a single database
- **Resumable**: interrupted syncs resume from the last checkpoint
- **Content-addressed attachments**: deduplicated by SHA-256

## Installation

**macOS / Linux:**
```bash
curl -fsSL https://msgvault.io/install.sh | bash
```

**Windows (PowerShell):**
```powershell
powershell -ExecutionPolicy ByPass -c "irm https://msgvault.io/install.ps1 | iex"
```

The installer detects your OS and architecture, downloads the latest release from [GitHub Releases](https://github.com/wesm/msgvault/releases), verifies the SHA-256 checksum, and installs the binary. You can review the script ([bash](https://msgvault.io/install.sh), [PowerShell](https://msgvault.io/install.ps1)) before running, or download a release binary directly from GitHub.

To build from source instead (requires **Go 1.25+** and a C/C++ compiler for CGO and to statically link DuckDB):

```bash
git clone https://github.com/wesm/msgvault.git
cd msgvault
make install
```

## Quick Start

> **Prerequisites:** You need a Google Cloud OAuth credential before adding an account.
> Follow the **[OAuth Setup Guide](https://msgvault.io/guides/oauth-setup/)** to create one (~5 minutes).

```bash
msgvault init-db
msgvault add-account you@gmail.com          # opens browser for OAuth
msgvault sync-full you@gmail.com --limit 100
msgvault tui
```

## Commands

| Command | Description |
|---------|-------------|
| `init-db` | Create the database |
| `add-account EMAIL` | Authorize a Gmail account (use `--headless` for servers) |
| `sync-full EMAIL` | Full sync (`--limit N`, `--after`/`--before` for date ranges) |
| `sync EMAIL` | Sync only new/changed messages |
| `tui` | Launch the interactive TUI (`--account` to filter) |
| `search QUERY` | Search messages (`--json` for machine output) |
| `mcp` | Start the MCP server for AI assistant integration |
| `stats` | Show archive statistics |
| `verify EMAIL` | Verify archive integrity against Gmail |
| `export-eml` | Export a message as `.eml` |
| `build-cache` | Rebuild the Parquet analytics cache |
| `repair-encoding` | Fix UTF-8 encoding issues |
| `list-senders` / `list-domains` / `list-labels` | Explore metadata |

See the [CLI Reference](https://msgvault.io/cli-reference/) for full details.

## Configuration

All data lives in `~/.msgvault/` by default (override with `MSGVAULT_HOME`).

```toml
# ~/.msgvault/config.toml
[oauth]
client_secrets = "/path/to/client_secret.json"

[sync]
rate_limit_qps = 5
```

See the [Configuration Guide](https://msgvault.io/configuration/) for all options.

## MCP Server

msgvault includes an MCP server that lets AI assistants search, analyze, and read your archived messages. Connect it to Claude Desktop or any MCP-capable agent and query your full message history conversationally. See the [MCP documentation](https://msgvault.io/usage/chat/) for setup instructions.

## Documentation

- [Setup Guide](https://msgvault.io/guides/oauth-setup/): OAuth, first sync, headless servers
- [Searching](https://msgvault.io/usage/searching/): query syntax and operators
- [Interactive TUI](https://msgvault.io/usage/tui/): keybindings, views, deletion staging
- [CLI Reference](https://msgvault.io/cli-reference/): all commands and flags
- [Multi-Account](https://msgvault.io/usage/multi-account/): managing multiple Gmail accounts
- [Configuration](https://msgvault.io/configuration/): config file and environment variables
- [Architecture](https://msgvault.io/architecture/storage/): SQLite, Parquet, and attachment storage
- [MCP Server](https://msgvault.io/usage/chat/): AI assistant integration
- [Troubleshooting](https://msgvault.io/troubleshooting/): common issues and fixes
- [Development](https://msgvault.io/development/): contributing, testing, building

## Community

Join the [msgvault Discord](https://discord.gg/fDnmxB8Wkq) to ask questions, share feedback, report issues, and connect with other users.

## Development

```bash
git clone https://github.com/wesm/msgvault.git
cd msgvault
make test           # run tests
make lint           # run linter
make install        # build and install
```

## License

MIT. See [LICENSE](LICENSE) for details.
