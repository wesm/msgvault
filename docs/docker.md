# Docker Deployment

Deploy msgvault on Docker for NAS devices (Synology, QNAP), Raspberry Pi, or any Docker-capable server.

## Quick Start

```bash
# Pull the image
docker pull ghcr.io/wesm/msgvault:latest

# Create data directory
mkdir -p ./data

# Run the daemon
docker run -d \
  --name msgvault \
  -p 8080:8080 \
  -v ./data:/data \
  -e TZ=America/New_York \
  ghcr.io/wesm/msgvault:latest serve
```

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

### Troubleshooting OAuth

| Error | Cause | Solution |
|-------|-------|----------|
| "Authorization timeout" | Didn't complete device flow in time | Re-run `add-account --headless` and complete faster |
| "Invalid grant" | Token expired or revoked | Delete token file, re-authorize: `rm /data/tokens/you@gmail.com.json` |
| "Access blocked: msgvault has not completed the Google verification process" | Using personal OAuth app | Click **Advanced** → **Go to msgvault (unsafe)** |
| "Quota exceeded" | Gmail API rate limits | Wait 24 hours, then retry |
| "Network error" / timeout | Container can't reach Google | Check DNS, proxy settings, firewall |

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
4. Set folder permissions: container runs as UID 1000

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
