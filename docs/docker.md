# Docker Deployment

Deploy msgvault on Docker for NAS devices (Synology, QNAP), Raspberry Pi, or any Docker-capable server.

## Quick Start

```bash
# Pull the image
docker pull ghcr.io/wesm/msgvault:latest

# Create data directory and config
mkdir -p ./data

# Generate API key and create config
API_KEY=$(openssl rand -hex 32)
cat > ./data/config.toml << EOF
[server]
bind_addr = "0.0.0.0"
api_key = "$API_KEY"
EOF
echo "Your API key: $API_KEY"

# Run the daemon
docker run -d \
  --name msgvault \
  -p 8080:8080 \
  -v ./data:/data \
  -e TZ=America/New_York \
  ghcr.io/wesm/msgvault:latest serve
```

> **Note:** The `api_key` is required when binding to `0.0.0.0`. Without a config file, the server binds to `127.0.0.1` (loopback only inside the container), making the port mapping ineffective.

## Image Tags

| Tag | Description |
|-----|-------------|
| `latest` | Latest stable release from main branch |
| `v1.2.3` | Specific version |
| `1.2` | Latest patch of minor version |
| `1` | Latest minor/patch of major version |
| `sha-abc1234` | Specific commit (for debugging) |

## Architectures

The image supports:
- `linux/amd64` - Intel/AMD x86-64 (most NAS devices, standard servers)
- `linux/arm64` - ARM 64-bit (Raspberry Pi 4/5, Apple Silicon via Rosetta, newer NAS)

Docker automatically selects the correct architecture.

---

## OAuth Setup (Headless)

Since Docker containers run without a browser, use the device flow to authenticate Gmail accounts.

### Step 1: Create Google OAuth Credentials

1. Go to [Google Cloud Console](https://console.cloud.google.com/apis/credentials)
2. Create a new project or select existing
3. Enable the **Gmail API**:
   - Go to **APIs & Services** → **Library**
   - Search for "Gmail API" and enable it
4. Create OAuth credentials:
   - Go to **APIs & Services** → **Credentials**
   - Click **Create Credentials** → **OAuth client ID**
   - Application type: **Desktop app**
   - Name: `msgvault`
5. Download the JSON file and save as `client_secret.json`

### Step 2: Configure msgvault

Copy your credentials to the data directory:

```bash
cp client_secret.json ./data/client_secret.json
```

Create `./data/config.toml`:

```toml
[oauth]
client_secrets = "/data/client_secret.json"

[server]
api_port = 8080
bind_addr = "0.0.0.0"
api_key = "your-secret-api-key-here"  # Generate with: openssl rand -hex 32

[[accounts]]
email = "you@gmail.com"
schedule = "0 2 * * *"  # Daily at 2 AM
enabled = true
```

### Step 3: Add Account via Device Flow

Run the add-account command with `--headless`:

```bash
docker exec -it msgvault msgvault add-account you@gmail.com --headless
```

You'll see output like:

```
To authorize this device, visit:
  https://www.google.com/device

And enter code: ABCD-EFGH

Waiting for authorization...
```

**On any device** (phone, laptop, tablet):
1. Open the URL shown
2. Sign in to your Google account
3. Enter the code displayed
4. Grant msgvault access to Gmail

The command will detect authorization and save the token:

```
Authorization successful!
Token saved to /data/tokens/you@gmail.com.json
```

### Step 4: Verify Setup

```bash
# Check token was saved
docker exec msgvault ls -la /data/tokens/

# Test sync (limit to 10 messages)
docker exec msgvault msgvault sync you@gmail.com --limit 10

# Check daemon logs
docker logs msgvault
```

### Alternative: Token Export (Recommended)

If the device flow doesn't work (Google's device flow doesn't support all Gmail API scopes for some OAuth configurations), you can authenticate on your local machine and export the token to your NAS.

**On your local machine** (with a browser):

```bash
# 1. Install msgvault locally or run from source
go install github.com/wesm/msgvault@latest

# 2. Authenticate via browser
msgvault add-account you@gmail.com

# 3. Export token to your NAS
msgvault export-token you@gmail.com \
  --to http://nas-ip:8080 \
  --api-key YOUR_API_KEY
```

The token is uploaded securely via the API and saved to `/data/tokens/` on the NAS.

**Then on your NAS**, add the account to `config.toml`:

```toml
[[accounts]]
email = "you@gmail.com"
schedule = "0 2 * * *"
enabled = true
```

Restart the container or trigger a sync:

```bash
docker-compose restart
# Or:
curl -X POST -H "X-API-Key: YOUR_KEY" http://nas-ip:8080/api/v1/sync/you@gmail.com
```

### Troubleshooting OAuth

| Error | Cause | Solution |
|-------|-------|----------|
| "Authorization timeout" | Didn't complete device flow in time | Re-run `add-account --headless` and complete faster |
| "Invalid grant" | Token expired or revoked | Delete token file, re-authorize: `rm /data/tokens/you@gmail.com.json` |
| "Access blocked: msgvault has not completed the Google verification process" | Using personal OAuth app | Click **Advanced** → **Go to msgvault (unsafe)** |
| "Quota exceeded" | Gmail API rate limits | Wait 24 hours, then retry |
| "Network error" / timeout | Container can't reach Google | Check DNS, proxy settings, firewall |
| "Device flow scope error" | Gmail API scopes not supported | Use **Token Export** workflow instead |

---

## NAS Setup Guide

Complete setup for Synology, QNAP, or any NAS with Docker support.

### docker-compose.yml

```yaml
version: "3.8"
services:
  msgvault:
    image: ghcr.io/wesm/msgvault:latest
    container_name: msgvault
    restart: unless-stopped
    ports:
      - "8080:8080"
    volumes:
      - ./data:/data
    environment:
      - TZ=America/New_York  # Adjust to your timezone
      - MSGVAULT_HOME=/data
    command: ["serve"]
    healthcheck:
      test: ["CMD", "wget", "-q", "--spider", "http://localhost:8080/health"]
      interval: 30s
      timeout: 5s
      retries: 3
      start_period: 10s
```

### Directory Structure

After setup, your data directory will contain:

```
./data/
├── config.toml           # Configuration file
├── client_secret.json    # Google OAuth credentials
├── msgvault.db           # SQLite database
├── tokens/               # OAuth tokens (one per account)
│   └── you@gmail.com.json
├── attachments/          # Content-addressed attachment storage
└── analytics/            # Parquet cache for fast queries
```

### Step-by-Step Setup

**1. Create directory structure**

```bash
mkdir -p ./data
```

**2. Add OAuth credentials**

Copy your `client_secret.json` to `./data/client_secret.json`

**3. Create config file**

Create `./data/config.toml`:

```toml
[oauth]
client_secrets = "/data/client_secret.json"

[server]
api_port = 8080
bind_addr = "0.0.0.0"  # Listen on all interfaces
api_key = "your-secret-api-key-here"  # Required for non-loopback

# Add multiple accounts with different schedules
[[accounts]]
email = "personal@gmail.com"
schedule = "0 2 * * *"   # Daily at 2 AM
enabled = true

[[accounts]]
email = "work@gmail.com"
schedule = "0 */6 * * *" # Every 6 hours
enabled = true
```

**4. Start the container**

```bash
docker-compose up -d
```

**5. Add Gmail accounts**

For each account in your config:

```bash
docker exec -it msgvault msgvault add-account personal@gmail.com --headless
# Complete device flow...

docker exec -it msgvault msgvault add-account work@gmail.com --headless
# Complete device flow...
```

**6. Run initial sync**

```bash
# Full sync (first time)
docker exec msgvault msgvault sync personal@gmail.com
docker exec msgvault msgvault sync work@gmail.com
```

**7. Verify scheduled sync**

Check logs for scheduled sync activity:

```bash
docker logs -f msgvault
```

Look for entries like:
```
level=INFO msg="scheduled sync started" email=personal@gmail.com
level=INFO msg="scheduled sync completed" email=personal@gmail.com messages=150
```

Or query the API:

```bash
curl -H "X-API-Key: your-key" http://localhost:8080/api/v1/scheduler/status
```

### Accessing the API

Once running, access your archive remotely:

```bash
# Get archive statistics
curl -H "X-API-Key: your-key" http://nas-ip:8080/api/v1/stats

# Search messages
curl -H "X-API-Key: your-key" "http://nas-ip:8080/api/v1/search?q=invoice"

# List recent messages
curl -H "X-API-Key: your-key" "http://nas-ip:8080/api/v1/messages?page_size=10"

# Trigger manual sync
curl -X POST -H "X-API-Key: your-key" http://nas-ip:8080/api/v1/sync/you@gmail.com
```

See [API Documentation](api.md) for full endpoint reference.

---

## Security Recommendations

### API Key

Generate a strong, random API key:

```bash
openssl rand -hex 32
```

### HTTPS (Reverse Proxy)

For internet-facing deployments, put msgvault behind a reverse proxy with TLS:

**Caddy** (automatic HTTPS):
```
msgvault.example.com {
    reverse_proxy localhost:8080
}
```

**Nginx**:
```nginx
server {
    listen 443 ssl;
    server_name msgvault.example.com;

    ssl_certificate /path/to/cert.pem;
    ssl_certificate_key /path/to/key.pem;

    location / {
        proxy_pass http://localhost:8080;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```

### Firewall

If not using a reverse proxy, restrict port 8080 to your local network:

```bash
# UFW example
ufw allow from 192.168.1.0/24 to any port 8080
```

### Backups

Regularly backup the `/data` directory:

```bash
# Stop container for consistent backup
docker-compose stop

# Backup
tar -czf msgvault-backup-$(date +%Y%m%d).tar.gz ./data

# Restart
docker-compose start
```

Critical files to backup:
- `msgvault.db` - Email metadata and bodies
- `tokens/` - OAuth tokens (re-auth required if lost)
- `config.toml` - Configuration
- `attachments/` - Email attachments (large, optional if you can re-sync)

---

## Platform-Specific Notes

### Synology DSM

1. Install **Container Manager** (Docker) package from Package Center
2. Create a shared folder for data (e.g., `/volume1/docker/msgvault`)
3. Use Container Manager UI or SSH to run docker-compose

**Important: Synology ACL Permissions**

Synology uses ACLs (Access Control Lists) that can override standard Unix permissions. The default container user (UID 1000) may not have write access even if you set folder permissions.

**Solution:** Add `user: root` to your docker-compose.yml:

```yaml
services:
  msgvault:
    image: ghcr.io/wesm/msgvault:latest
    user: root  # Required for Synology ACLs
    # ... rest of config
```

**Via SSH:**
```bash
cd /volume1/docker/msgvault
docker-compose up -d
```

### QNAP

1. Install **Container Station** from App Center
2. Create a folder for data (e.g., `/share/Container/msgvault`)
3. Use Container Station or SSH

### Raspberry Pi

Works on Pi 4 and Pi 5 with arm64 OS:

```bash
# Verify 64-bit OS
uname -m  # Should show aarch64

# Standard docker-compose setup
docker-compose up -d
```

**Note:** Initial sync of large mailboxes may take longer on Pi hardware.

---

## Cron Schedule Reference

The `schedule` field uses standard cron format (5 fields):

```
┌───────────── minute (0-59)
│ ┌───────────── hour (0-23)
│ │ ┌───────────── day of month (1-31)
│ │ │ ┌───────────── month (1-12)
│ │ │ │ ┌───────────── day of week (0-6, 0=Sunday)
│ │ │ │ │
* * * * *
```

**Examples:**

| Schedule | Description |
|----------|-------------|
| `0 2 * * *` | Daily at 2:00 AM |
| `0 */6 * * *` | Every 6 hours |
| `*/30 * * * *` | Every 30 minutes |
| `0 8,18 * * *` | Twice daily at 8 AM and 6 PM |
| `0 2 * * 0` | Weekly on Sunday at 2 AM |
| `0 2 1 * *` | Monthly on the 1st at 2 AM |

---

## Container Management

```bash
# View logs
docker logs msgvault
docker logs -f msgvault  # Follow

# Execute commands
docker exec msgvault msgvault stats
docker exec -it msgvault msgvault tui  # Interactive TUI

# Restart
docker-compose restart

# Update to latest
docker-compose pull
docker-compose up -d

# Stop
docker-compose down
```

## Health Checks

The container includes a health check that polls `/health` every 30 seconds.

Check container health:

```bash
docker inspect --format='{{.State.Health.Status}}' msgvault
# Returns: healthy, unhealthy, or starting
```

View health check history:

```bash
docker inspect --format='{{json .State.Health}}' msgvault | jq
```

---

## Troubleshooting

### Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| `unable to open database file` | Database doesn't exist | Run `msgvault init-db` first, or the `serve` command auto-creates it |
| `permission denied` on Synology | ACLs override Unix permissions | Add `user: root` to docker-compose.yml |
| `OAuth client secrets not configured` | Missing config.toml | Run `msgvault setup` or create config manually |
| Token export fails | Missing --to or --api-key | Use flags, env vars (`MSGVAULT_REMOTE_URL`), or run `msgvault setup` |
| Search API returns 500 | Bug in older versions | Upgrade to latest image |

### Local Setup Issues

**"OAuth client secrets not configured"**

msgvault needs Google OAuth credentials. Run the setup wizard:

```bash
msgvault setup
```

Or manually create `~/.msgvault/config.toml`:

```toml
[oauth]
client_secrets = "/path/to/client_secret.json"
```

**Token export requires flags every time**

After a successful export, msgvault saves the remote server config. For the first export:

```bash
# First time: provide flags
msgvault export-token you@gmail.com --to http://nas:8080 --api-key KEY

# Subsequent exports: no flags needed
msgvault export-token another@gmail.com
```

Or use environment variables:

```bash
export MSGVAULT_REMOTE_URL=http://nas:8080
export MSGVAULT_REMOTE_API_KEY=your-key
msgvault export-token you@gmail.com
```

### Container Issues

**Container won't start**

Check logs:

```bash
docker logs msgvault
```

Common causes:
- Missing `config.toml` with `bind_addr = "0.0.0.0"` and `api_key`
- Port 8080 already in use
- Volume mount permissions (see Synology section above)

**Scheduled sync not running**

1. Verify accounts are configured in `config.toml`:
   ```toml
   [[accounts]]
   email = "you@gmail.com"
   schedule = "0 2 * * *"
   enabled = true
   ```

2. Verify token exists:
   ```bash
   docker exec msgvault ls -la /data/tokens/
   ```

3. Check scheduler status:
   ```bash
   curl -H "X-API-Key: KEY" http://localhost:8080/api/v1/scheduler/status
   ```

### Sync Issues

**"No source found for email"**

The account hasn't been added to the database. Run:

```bash
docker exec msgvault msgvault add-account you@gmail.com --headless
```

Or if using token export, the token exists but account isn't registered. The `add-account` command will detect the existing token and register the account.

**First sync fails with "incremental sync requires full sync first"**

Run a full sync before scheduled incremental syncs work:

```bash
docker exec msgvault msgvault sync-full you@gmail.com
```

### Getting Help

- GitHub Issues: https://github.com/wesm/msgvault/issues
- Documentation: https://msgvault.io
