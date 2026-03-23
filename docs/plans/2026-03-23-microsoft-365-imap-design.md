# Microsoft 365 OAuth2 IMAP Support

## Problem

Microsoft deprecated basic auth for Exchange Online. IMAP access to Office 365
now requires OAuth2 with the XOAUTH2 SASL mechanism. msgvault's IMAP client
only supports username/password authentication, blocking Microsoft 365 users.

## Design

### Architecture

Approach A: separate `internal/microsoft/` package parallel to `internal/oauth/`
(Gmail). The providers differ enough (endpoints, scopes, token validation APIs,
client config formats) that a shared abstraction adds more complexity than it
saves.

### 1. IMAP Client — XOAUTH2 SASL Support

Add `AuthMethod` field to `imap.Config`:

- `"password"` (default, backward-compatible) — existing `Login()` path
- `"xoauth2"` — fetch access token via callback, authenticate with SASL XOAUTH2

New `Client` option `WithTokenSource(func(ctx) (string, error))` provides fresh
access tokens at connection time. In `connect()`, branch on auth method:
password calls `Login()`, xoauth2 calls `Authenticate()` with
`sasl.NewXoauth2Client()` from `emersion/go-sasl`.

### 2. Microsoft OAuth2 Provider (`internal/microsoft/`)

`Manager` struct with:

- **Azure AD endpoints:** `login.microsoftonline.com/{tenant}/oauth2/v2.0/...`
- **Default tenant:** `common` (personal + org accounts)
- **Scopes:** `IMAP.AccessAsUser.All`, `offline_access`, `openid`, `email`
- **Token validation:** MS Graph `/v1.0/me` to verify email matches expected account
- **Token storage:** `tokens/microsoft_{email}.json` with atomic write, 0600 perms
- **Browser flow:** localhost:8089 callback at `/callback/microsoft`, PKCE (S256)

Config in `config.toml`:

```toml
[microsoft]
client_id = "your-azure-app-client-id"
tenant_id = "common"
```

No client secret needed — Azure public client apps (desktop) don't require one.

### 3. CLI — `add-o365` Command

`msgvault add-o365 user@company.com`

1. Read `[microsoft]` config from `config.toml`
2. Run Microsoft OAuth2 browser flow, validate via MS Graph `/me`
3. Save token to `tokens/microsoft_{email}.json`
4. Auto-configure IMAP: `outlook.office365.com:993`, TLS, `auth_method: "xoauth2"`
5. Create source record (`source_type: "imap"`, config in `sync_config`)
6. Set display name from email

Optional `--tenant` flag for org-specific tenant IDs.

### 4. Sync Routing

`buildAPIClient()` in `syncfull.go` gets a small branch for XOAUTH2 IMAP configs:

- `auth_method == "xoauth2"` → load Microsoft token, create token source, pass
  to IMAP client via `WithTokenSource`
- Otherwise → existing password path (unchanged)

No changes needed to the sync orchestration layer.

### 5. Testing

- Unit: mock IMAP server for XOAUTH2 SASL string format, mock MS Graph for
  token validation, test token storage/load/refresh
- Integration: requires real Outlook.com account + Azure AD app registration

## Gotchas

- XOAUTH2 SASL string: `user=<email>\x01auth=Bearer <token>\x01\x01` — exact
  format is critical
- Azure AD requires PKCE (S256) for public clients
- Scope must be `https://outlook.office365.com/IMAP.AccessAsUser.All` — using
  `.default` requires admin consent
- Some tenants block OAuth for IMAP via Security Defaults
- Microsoft access tokens expire in ~60-90 minutes; refresh tokens last longer
  but can also expire

## Dependencies

- `github.com/emersion/go-sasl` — XOAUTH2 SASL client (likely already
  transitive via go-imap)
- `golang.org/x/oauth2` — already in go.mod
