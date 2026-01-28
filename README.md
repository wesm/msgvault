# msgvault

msgvault is an offline Gmail archive tool that exports and stores your
email data locally with full-text search capabilities. It's designed
to archive years of Gmail from multiple accounts, make it searchable
offline, and eventually enable safe deletion of emails from Gmail once
archived.

## Features

- **Full Gmail backup**: Downloads complete messages including raw MIME, attachments, labels, and metadata
- **Lightning-fast interactive TUI**: Terminal-based interface for exploring your archive, powered by DuckDB
- **Incremental sync**: Uses Gmail History API for efficient updates after initial sync
- **Full-text search**: SQLite FTS5-powered search with Gmail-like query syntax
- **Multi-account support**: Archive multiple Gmail accounts in a single database
- **Archive verification**: Verify local archive integrity against Gmail
- **Content-addressed storage**: Attachments deduplicated by SHA-256 hash
- **Resumable sync**: Interrupted syncs can resume from where they left off
- **Date filtering**: Sync specific time ranges to test or archive historical data
- **Rate limiting**: Token bucket rate limiting respects Gmail API quotas

## Installation

### Prerequisites

- Go 1.21+
- GCC/Clang (for CGO/SQLite FTS5 support)

### Build from Source

```bash
# Clone the repository
git clone https://github.com/wesm/msgvault.git
cd msgvault

# Build (debug)
make build

# Or build with optimizations (release)
make build-release

# Install to ~/.local/bin or GOPATH
make install
```

## Google OAuth Configuration

msgvault requires OAuth credentials to access the Gmail API. Follow these steps to configure access:

### Step 1: Create a Google Cloud Project

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project or select an existing one
3. Note your project ID

### Step 2: Enable the Gmail API

1. Navigate to **APIs & Services > Library**
2. Search for "Gmail API"
3. Click **Enable**

### Step 3: Configure OAuth Consent Screen

1. Go to **APIs & Services > OAuth consent screen**
2. Choose **External** user type (or Internal if using Google Workspace)
3. Fill in required fields:
   - App name: "msgvault" (or your preferred name)
   - User support email: your email
   - Developer contact email: your email
4. Click **Save and Continue**
5. On the Scopes page, click **Add or Remove Scopes**
6. Add the scope: `https://www.googleapis.com/auth/gmail.modify`
   - Note: `gmail.modify` enables deletion features; sync is read-only
7. Save and continue through the remaining screens
8. **Add Test Users**: Under "Test users", add all Gmail addresses you want to sync (required while app is in testing mode)

### Step 4: Create OAuth Client Credentials

1. Go to **APIs & Services > Credentials**
2. Click **Create Credentials > OAuth client ID**
3. Choose **Desktop application**
4. Name it "msgvault Desktop Client"
5. Click **Create**
6. Download the JSON file (click the download icon)
7. Save it as `client_secret.json` in a secure location

### Step 5: Configure msgvault

Create the config file at `~/.msgvault/config.toml`:

```toml
[oauth]
client_secrets = "/path/to/your/client_secret.json"

[data]
# Optional: customize data directory (default: ~/.msgvault)
# data_dir = "/path/to/msgvault/data"

[sync]
rate_limit_qps = 5
```

### Step 6: Add Your Gmail Account

```bash
# Add your Gmail account (opens browser for OAuth)
msgvault add-account you@gmail.com
```

This will:
1. Open your browser to Google's OAuth consent page
2. Ask you to sign in and grant access to msgvault
3. Store the OAuth tokens locally in `~/.msgvault/tokens/`

### Headless Server Setup

For servers without a browser (e.g., running over SSH or on a headless VPS), use the device authorization flow:

```bash
# Use --headless flag for device authorization flow
msgvault add-account you@gmail.com --headless
```

This will display instructions like:

```
Starting device code flow...

1. Open this URL in any browser:
   https://www.google.com/device

2. Enter this code when prompted:
   ABC-XYZ

Waiting for authorization...
```

Open the URL on any device with a browser (phone, laptop, etc.), sign in to Google, enter the code, and grant access. The server will automatically detect when authorization is complete.

## Usage

### Syncing Email

```bash
# Initial full sync (use --limit for testing)
msgvault sync-full you@gmail.com --limit 100

# Full sync without limit (downloads everything)
msgvault sync-full you@gmail.com

# Sync a specific date range (useful for testing or historical archives)
msgvault sync-full you@gmail.com --after 2024-01-01 --before 2024-02-01

# Incremental sync (only new/changed messages)
msgvault sync-incremental you@gmail.com
```

### Searching Messages

msgvault supports Gmail-like search syntax:

```bash
# Search by sender
msgvault search from:alice@example.com

# Search by subject
msgvault search subject:meeting

# Search with date range
msgvault search "after:2024-01-01 before:2024-06-01"

# Search for attachments
msgvault search has:attachment

# Search by label
msgvault search label:INBOX

# Combine filters
msgvault search "from:boss@company.com has:attachment after:2024-01-01"

# Full-text search
msgvault search "quarterly report"

# Output as JSON
msgvault search from:alice@example.com --json
```

**Supported search operators:**
- `from:`, `to:`, `cc:`, `bcc:` - Address filters
- `subject:` - Subject text search
- `label:` - Label filter (e.g., `label:INBOX`, `label:SENT`)
- `has:attachment` - Messages with attachments
- `before:`, `after:` - Date filters (YYYY-MM-DD format)
- `older_than:`, `newer_than:` - Relative dates (e.g., `7d`, `2w`, `1m`, `1y`)
- `larger:`, `smaller:` - Size filters (e.g., `5M`, `100K`)
- Bare words and `"quoted phrases"` - Full-text search

### Interactive TUI

Launch the interactive terminal interface:

```bash
# Launch the TUI
msgvault tui

# Filter by account
msgvault tui --account you@gmail.com
```

**TUI Keyboard shortcuts:**
- `j/k` or `↑/↓` - Navigate rows
- `Enter` - Drill down into selection
- `Esc` or `Backspace` - Go back
- `g` - Cycle views (Senders → Recipients → Domains → Labels → Time)
- `s` - Cycle sort field (Name → Count → Size)
- `r` - Reverse sort direction
- `t` - Cycle time granularity (in Time view)
- `a` - Filter by account
- `Space` - Toggle selection
- `d` - Stage selected for deletion
- `/` - Search
- `?` - Help
- `q` - Quit

### Exporting Messages

```bash
# Export by internal message ID
msgvault export-eml --message-id 12345 --output message.eml

# Export by Gmail message ID
msgvault export-eml --gmail-id 18abc123def --output message.eml

# Output to stdout
msgvault export-eml --gmail-id 18abc123def
```

### Verifying Archive Integrity

```bash
# Verify archive against Gmail (samples 100 messages by default)
msgvault verify you@gmail.com

# Verify with larger sample
msgvault verify you@gmail.com --sample-size 500
```

This compares:
- Message counts (local vs Gmail)
- Random sample of messages have raw MIME stored
- FTS index entries exist for sampled messages

### Analytics

```bash
# Show archive statistics
msgvault stats

# List top senders
msgvault list-senders --limit 20

# List top domains
msgvault list-domains --limit 20

# List labels
msgvault list-labels
```

## Multi-Account Support

msgvault supports archiving multiple Gmail accounts in a single database:

```bash
# Add multiple accounts
msgvault add-account personal@gmail.com
msgvault add-account work@company.com

# Sync each account
msgvault sync-full personal@gmail.com
msgvault sync-full work@company.com

# Search across all accounts
msgvault search "quarterly report"

# TUI with specific account
msgvault tui --account work@company.com
```

## Configuration Reference

Full configuration options in `~/.msgvault/config.toml`:

```toml
[data]
# Base data directory (default: ~/.msgvault)
data_dir = "/path/to/msgvault/data"

# Database path (default: {data_dir}/msgvault.db)
db_path = "/path/to/msgvault.db"

# Attachments directory (default: {data_dir}/attachments)
attachments_dir = "/path/to/attachments"

# Token storage directory (default: {data_dir}/tokens)
tokens_dir = "/path/to/tokens"

[oauth]
# Path to Google OAuth client secrets JSON (required)
client_secrets = "/path/to/client_secret.json"

[sync]
# Gmail API rate limit (requests per second)
rate_limit_qps = 5
```

### Environment Variables

- `MSGVAULT_HOME`: Base directory for all msgvault data (default: `~/.msgvault`)
- `MSGVAULT_CONFIG`: Path to config file (default: `~/.msgvault/config.toml`)

## Local Development & Testing

### Safety First

**msgvault only reads from Gmail** - it does not modify or delete anything in your Gmail account. The sync operations are completely read-only:

- `sync-full` and `sync-incremental` only use Gmail's `messages.list` and `messages.get` APIs
- No write operations (delete, modify, trash) are performed during sync
- Your Gmail data remains untouched

The `gmail.modify` scope is requested to enable *future* deletion features (for archiving old emails), but the current sync implementation is read-only.

### Quick Start: Test with a Small Date Range

The fastest way to test is to sync just one month of email:

```bash
# 1. Set up OAuth (see Google OAuth Configuration section above)
# Create ~/.msgvault/config.toml with your client_secret path

# 2. Add your Gmail account
msgvault add-account you@gmail.com

# 3. Sync ONE MONTH of email (adjust dates as needed)
msgvault sync-full you@gmail.com \
    --after 2024-01-01 \
    --before 2024-02-01 \
    --verbose

# 4. Verify the sync worked
msgvault search "after:2024-01-01 before:2024-02-01"
```

### Recommended Test Scenarios

**Minimal test (few messages):**
```bash
# Sync just the last week
msgvault sync-full you@gmail.com \
    --after 2024-12-01 \
    --before 2024-12-08
```

**Small historical archive:**
```bash
# Sync one year from a decade ago (likely fewer emails)
msgvault sync-full you@gmail.com \
    --after 2010-01-01 \
    --before 2011-01-01
```

**Test with limit:**
```bash
# Sync at most 50 messages (ignores date filters, just limits count)
msgvault sync-full you@gmail.com --limit 50
```

### Understanding the Data

After syncing, you can explore your data:

```bash
# Show archive statistics
msgvault stats

# Find messages from specific senders
msgvault search from:important@example.com

# Check messages with attachments
msgvault search has:attachment

# Export a specific message as .eml for inspection
msgvault export-eml --message-id 1 --output test.eml

# Launch TUI to explore interactively
msgvault tui
```

### Database Location

By default, all data is stored in `~/.msgvault/`:
- **Database**: `~/.msgvault/msgvault.db`
- **Attachments**: `~/.msgvault/attachments/`
- **OAuth tokens**: `~/.msgvault/tokens/`
- **Config**: `~/.msgvault/config.toml`

To start fresh:
```bash
rm -rf ~/.msgvault
```

### Running the Test Suite

```bash
# Run all tests
make test

# Run with verbose output
make test-v
```

### Development Tools

```bash
# Build (debug)
make build

# Build (release)
make build-release

# Format code
make fmt

# Run linter (requires golangci-lint)
make lint

# Tidy dependencies
make tidy
```

## Data Storage

- **Messages**: Raw MIME stored compressed (zlib) in the database
- **Attachments**: Stored on disk at `{attachments_dir}/{hash[:2]}/{hash}`, deduplicated by SHA-256
- **OAuth tokens**: Stored in `{tokens_dir}/{email}.json`

## Security Notes

- OAuth tokens are stored locally and should be protected
- The `gmail.modify` scope is used to enable future deletion features
- Consider encrypting your data directory for sensitive archives
- Client secrets should never be committed to version control

## Troubleshooting

### OAuth Errors

**"Error 403: access_denied"**: Your email isn't added as a test user. Go to OAuth consent screen > Test users and add your Gmail address.

**"Access blocked: This app's request is invalid"**: The Gmail scope isn't configured. Verify you added `gmail.modify` scope and that Gmail API is enabled.

**"redirect_uri_mismatch"**: Wrong application type. Ensure you selected "Desktop app" when creating OAuth credentials.

**General OAuth issues**:
1. Remove old tokens: `rm ~/.msgvault/tokens/you@gmail.com.json`
2. Re-add account: `msgvault add-account you@gmail.com`
3. Revoke and retry: https://myaccount.google.com/permissions

### Rate Limiting

If you hit Gmail API rate limits during large syncs:
1. Reduce `rate_limit_qps` in config (default is 5)
2. Use `--limit` during initial testing
3. Wait and retry - rate limits reset over time

### Database Errors

If the database is corrupted:
1. Back up your database file
2. Delete and re-sync: `rm ~/.msgvault/msgvault.db`
3. Re-sync: `msgvault sync-full you@gmail.com`

### Resuming Interrupted Syncs

If a sync is interrupted (network error, Ctrl+C, etc.), simply run the same command again:

```bash
# The sync will resume from where it left off
msgvault sync-full you@gmail.com --after 2024-01-01 --before 2024-02-01
```

To force a fresh start instead of resuming:

```bash
msgvault sync-full you@gmail.com --noresume
```

## License

MIT License - See LICENSE file for details.
