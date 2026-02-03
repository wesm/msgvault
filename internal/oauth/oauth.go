// Package oauth provides OAuth2 authentication flows for Gmail.
package oauth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

// Scopes for normal msgvault operations (sync, search, read).
var Scopes = []string{
	"https://www.googleapis.com/auth/gmail.readonly",
	"https://www.googleapis.com/auth/gmail.modify",
}

// ScopesDeletion includes full access required for batchDelete API.
// gmail.modify supports trash/untrash but NOT batchDelete.
var ScopesDeletion = []string{
	"https://mail.google.com/",
}

// Manager handles OAuth2 token acquisition and storage.
type Manager struct {
	config    *oauth2.Config
	tokensDir string
	logger    *slog.Logger
}

// NewManager creates an OAuth manager from client secrets.
func NewManager(clientSecretsPath, tokensDir string, logger *slog.Logger) (*Manager, error) {
	return NewManagerWithScopes(clientSecretsPath, tokensDir, logger, Scopes)
}

// TokenSource returns a token source for the given email.
// If a valid token exists, it will be reused and auto-refreshed.
func (m *Manager) TokenSource(ctx context.Context, email string) (oauth2.TokenSource, error) {
	tf, err := m.loadTokenFile(email)
	if err != nil {
		return nil, fmt.Errorf("no valid token for %s: %w", email, err)
	}

	// Create a token source that auto-refreshes
	ts := m.config.TokenSource(ctx, &tf.Token)

	// Save refreshed token if it changed
	newToken, err := ts.Token()
	if err != nil {
		return nil, fmt.Errorf("refresh token: %w", err)
	}

	if newToken.AccessToken != tf.Token.AccessToken {
		// Preserve the original scopes when saving refreshed token
		scopes := tf.Scopes
		if len(scopes) == 0 {
			scopes = m.config.Scopes // fallback for legacy tokens
		}
		if err := m.saveToken(email, newToken, scopes); err != nil {
			m.logger.Warn("failed to save refreshed token", "email", email, "error", err)
		}
	}

	return ts, nil
}

// HasToken checks if a token exists for the given email.
func (m *Manager) HasToken(email string) bool {
	_, err := m.loadToken(email)
	return err == nil
}

// PrintHeadlessInstructions prints setup instructions for headless servers.
// Google's device flow does not support Gmail scopes, so users must authorize
// on a machine with a browser and copy the token file.
func PrintHeadlessInstructions(email string) {
	fmt.Println()
	fmt.Println("=== Headless Server Setup ===")
	fmt.Println()
	fmt.Println("Google's OAuth device flow does not support Gmail scopes, so --headless")
	fmt.Println("cannot directly authorize. Instead, authorize on a machine with a browser")
	fmt.Println("and copy the token to your server.")
	fmt.Println()
	fmt.Println("Step 1: On a machine with a browser, run:")
	fmt.Println()
	fmt.Printf("    msgvault add-account %s\n", email)
	fmt.Println()
	fmt.Println("Step 2: Copy the token file to your headless server:")
	fmt.Println()
	fmt.Printf("    scp ~/.msgvault/tokens/%s.json user@server:~/.msgvault/tokens/\n", email)
	fmt.Println()
	fmt.Println("The token will work on the server and auto-refresh as needed.")
	fmt.Println("All msgvault commands (sync, tui, etc.) will work normally.")
	fmt.Println()
}

// Authorize performs the browser OAuth flow for a new account.
func (m *Manager) Authorize(ctx context.Context, email string) error {
	token, err := m.browserFlow(ctx)
	if err != nil {
		return err
	}

	return m.saveToken(email, token, m.config.Scopes)
}

const (
	redirectPort = "8089"
	callbackPath = "/callback"
)

// newCallbackHandler returns an HTTP handler that processes the OAuth callback.
func (m *Manager) newCallbackHandler(expectedState string, codeChan chan<- string, errChan chan<- error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("state") != expectedState {
			errChan <- fmt.Errorf("state mismatch: possible CSRF attack")
			fmt.Fprintf(w, "Error: state mismatch")
			return
		}
		code := r.URL.Query().Get("code")
		if code == "" {
			errChan <- fmt.Errorf("no code in callback")
			fmt.Fprintf(w, "Error: no authorization code received")
			return
		}
		codeChan <- code
		fmt.Fprintf(w, "Authorization successful! You can close this window.")
	}
}

// browserFlow opens a browser for OAuth authorization.
func (m *Manager) browserFlow(ctx context.Context) (*oauth2.Token, error) {
	// Generate random state for CSRF protection
	stateBytes := make([]byte, 16)
	if _, err := rand.Read(stateBytes); err != nil {
		return nil, fmt.Errorf("generate state: %w", err)
	}
	state := base64.URLEncoding.EncodeToString(stateBytes)

	// Start local server for callback
	codeChan := make(chan string, 1)
	errChan := make(chan error, 1)

	mux := http.NewServeMux()
	mux.Handle(callbackPath, m.newCallbackHandler(state, codeChan, errChan))
	server := &http.Server{Addr: "localhost:" + redirectPort, Handler: mux}

	go func() {
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			errChan <- err
		}
	}()

	defer func() { _ = server.Shutdown(ctx) }()

	// Generate auth URL
	m.config.RedirectURL = "http://localhost:" + redirectPort + callbackPath
	authURL := m.config.AuthCodeURL(state, oauth2.AccessTypeOffline, oauth2.ApprovalForce)

	// Open browser
	fmt.Printf("Opening browser for authorization...\n")
	fmt.Printf("If browser doesn't open, visit:\n%s\n\n", authURL)

	if err := openBrowser(authURL); err != nil {
		m.logger.Warn("failed to open browser", "error", err)
	}

	// Wait for callback
	select {
	case code := <-codeChan:
		return m.config.Exchange(ctx, code)
	case err := <-errChan:
		return nil, err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// tokenFile wraps an OAuth2 token with metadata about the scopes it was
// authorized with. This enables proactive scope checking (e.g., detecting
// that deletion requires re-authorization) without making an API call first.
type tokenFile struct {
	oauth2.Token
	Scopes []string `json:"scopes,omitempty"`
}

// loadToken loads a saved token for the given email.
func (m *Manager) loadToken(email string) (*oauth2.Token, error) {
	path := m.tokenPath(email)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var tf tokenFile
	if err := json.Unmarshal(data, &tf); err != nil {
		return nil, err
	}

	return &tf.Token, nil
}

// loadTokenFile loads the full token file including scope metadata.
func (m *Manager) loadTokenFile(email string) (*tokenFile, error) {
	path := m.tokenPath(email)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var tf tokenFile
	if err := json.Unmarshal(data, &tf); err != nil {
		return nil, err
	}

	return &tf, nil
}

// HasScopeMetadata returns true if the token file for this account has any
// scope metadata stored. Legacy tokens (saved before scope tracking) return false.
func (m *Manager) HasScopeMetadata(email string) bool {
	tf, err := m.loadTokenFile(email)
	if err != nil {
		return false
	}
	return len(tf.Scopes) > 0
}

// HasScope checks if the stored token for the given email was authorized
// with the specified scope. Returns false if the token doesn't exist or
// doesn't have scope metadata (legacy tokens saved before scope tracking).
func (m *Manager) HasScope(email string, scope string) bool {
	tf, err := m.loadTokenFile(email)
	if err != nil {
		return false
	}
	for _, s := range tf.Scopes {
		if s == scope {
			return true
		}
	}
	return false
}

// saveToken saves a token for the given email with the specified scopes.
func (m *Manager) saveToken(email string, token *oauth2.Token, scopes []string) error {
	if err := os.MkdirAll(m.tokensDir, 0700); err != nil {
		return err
	}

	tf := tokenFile{
		Token:  *token,
		Scopes: scopes,
	}

	data, err := json.MarshalIndent(tf, "", "  ")
	if err != nil {
		return err
	}

	path := m.tokenPath(email)
	return os.WriteFile(path, data, 0600)
}

// tokenPath returns the path to the token file for an email.
// The email is sanitized to prevent path traversal attacks.
func (m *Manager) tokenPath(email string) string {
	// Sanitize email to prevent path traversal
	// Replace characters that could be used for path traversal
	safe := strings.ReplaceAll(email, "/", "_")
	safe = strings.ReplaceAll(safe, "\\", "_")
	safe = strings.ReplaceAll(safe, "..", "_")

	// Ensure the final path is within tokensDir
	path := filepath.Join(m.tokensDir, safe+".json")
	cleanPath := filepath.Clean(path)

	// Verify the path is still within tokensDir
	if !strings.HasPrefix(cleanPath, filepath.Clean(m.tokensDir)) {
		// If path escapes tokensDir, use a hash-based fallback
		return filepath.Join(m.tokensDir, fmt.Sprintf("%x.json", sha256.Sum256([]byte(email))))
	}

	return cleanPath
}

// scopesToString joins scopes with spaces.
func scopesToString(scopes []string) string {
	return strings.Join(scopes, " ")
}

// openBrowser opens the default browser to the given URL.
func openBrowser(url string) error {
	var cmd *exec.Cmd

	switch runtime.GOOS {
	case "darwin":
		cmd = exec.Command("open", url)
	case "linux":
		cmd = exec.Command("xdg-open", url)
	case "windows":
		cmd = exec.Command("rundll32", "url.dll,FileProtocolHandler", url)
	default:
		return fmt.Errorf("unsupported platform: %s", runtime.GOOS)
	}

	return cmd.Start()
}

// NewManagerWithScopes creates an OAuth manager with custom scopes.
func NewManagerWithScopes(clientSecretsPath, tokensDir string, logger *slog.Logger, scopes []string) (*Manager, error) {
	data, err := os.ReadFile(clientSecretsPath)
	if err != nil {
		return nil, fmt.Errorf("read client secrets: %w", err)
	}

	config, err := parseClientSecrets(data, scopes)
	if err != nil {
		return nil, fmt.Errorf("parse client secrets: %w", err)
	}

	if logger == nil {
		logger = slog.Default()
	}

	return &Manager{
		config:    config,
		tokensDir: tokensDir,
		logger:    logger,
	}, nil
}

// parseClientSecrets parses Google OAuth client secrets JSON.
// Handles both "Desktop application" (with redirect_uris) and
// "TVs and Limited Input devices" (without redirect_uris) client types.
func parseClientSecrets(data []byte, scopes []string) (*oauth2.Config, error) {
	// Try standard parsing first (works for Desktop apps with redirect URIs)
	config, err := google.ConfigFromJSON(data, scopes...)
	if err == nil {
		return config, nil
	}

	// If that fails, try parsing manually for TV/device clients without redirect URIs
	var secrets struct {
		Installed *struct {
			ClientID     string `json:"client_id"`
			ClientSecret string `json:"client_secret"`
			AuthURI      string `json:"auth_uri"`
			TokenURI     string `json:"token_uri"`
		} `json:"installed"`
		Web *struct {
			ClientID     string `json:"client_id"`
			ClientSecret string `json:"client_secret"`
			AuthURI      string `json:"auth_uri"`
			TokenURI     string `json:"token_uri"`
		} `json:"web"`
	}

	if jsonErr := json.Unmarshal(data, &secrets); jsonErr != nil {
		return nil, fmt.Errorf("invalid client secrets JSON: %w", jsonErr)
	}

	// Check installed (Desktop/TV) clients first, then web
	creds := secrets.Installed
	if creds == nil {
		creds = secrets.Web
	}
	if creds == nil || creds.ClientID == "" || creds.ClientSecret == "" {
		return nil, fmt.Errorf("client secrets missing client_id or client_secret")
	}
	if creds.AuthURI == "" || creds.TokenURI == "" {
		return nil, fmt.Errorf("client secrets missing auth_uri or token_uri")
	}

	// Build config manually - no redirect URI needed for device flow
	return &oauth2.Config{
		ClientID:     creds.ClientID,
		ClientSecret: creds.ClientSecret,
		Endpoint: oauth2.Endpoint{
			AuthURL:  creds.AuthURI,
			TokenURL: creds.TokenURI,
		},
		Scopes: scopes,
	}, nil
}

// DeleteToken removes the token file for the given email.
func (m *Manager) DeleteToken(email string) error {
	path := m.tokenPath(email)
	err := os.Remove(path)
	if os.IsNotExist(err) {
		return nil // Already gone
	}
	return err
}

// TokenPath returns the path to the token file for an email (for external use).
func (m *Manager) TokenPath(email string) string {
	return m.tokenPath(email)
}
