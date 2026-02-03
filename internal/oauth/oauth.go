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
	"time"

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
	token, err := m.loadToken(email)
	if err != nil {
		return nil, fmt.Errorf("no valid token for %s: %w", email, err)
	}

	// Create a token source that auto-refreshes
	ts := m.config.TokenSource(ctx, token)

	// Save refreshed token if it changed
	newToken, err := ts.Token()
	if err != nil {
		return nil, fmt.Errorf("refresh token: %w", err)
	}

	if newToken.AccessToken != token.AccessToken {
		if err := m.saveToken(email, newToken); err != nil {
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

// Authorize performs the OAuth flow for a new account.
// If headless is true, uses device code flow; otherwise opens browser.
func (m *Manager) Authorize(ctx context.Context, email string, headless bool) error {
	var token *oauth2.Token
	var err error

	if headless {
		token, err = m.deviceFlow(ctx)
	} else {
		token, err = m.browserFlow(ctx)
	}

	if err != nil {
		return err
	}

	return m.saveToken(email, token)
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

// deviceFlow uses the device authorization grant for headless environments.
func (m *Manager) deviceFlow(ctx context.Context) (*oauth2.Token, error) {
	// Device flow endpoint
	deviceEndpoint := "https://oauth2.googleapis.com/device/code"

	// Request device code
	resp, err := http.PostForm(deviceEndpoint, map[string][]string{
		"client_id": {m.config.ClientID},
		"scope":     {scopesToString(Scopes)},
	})
	if err != nil {
		return nil, fmt.Errorf("request device code: %w", err)
	}
	defer resp.Body.Close()

	var deviceResp struct {
		DeviceCode      string `json:"device_code"`
		UserCode        string `json:"user_code"`
		VerificationURL string `json:"verification_url"`
		ExpiresIn       int    `json:"expires_in"`
		Interval        int    `json:"interval"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&deviceResp); err != nil {
		return nil, fmt.Errorf("parse device response: %w", err)
	}

	// Display instructions to user
	fmt.Printf("\n")
	fmt.Printf("To authorize msgvault, visit:\n")
	fmt.Printf("  %s\n\n", deviceResp.VerificationURL)
	fmt.Printf("And enter code: %s\n\n", deviceResp.UserCode)
	fmt.Printf("Waiting for authorization...\n")

	// Poll for token
	interval := time.Duration(deviceResp.Interval) * time.Second
	if interval < 5*time.Second {
		interval = 5 * time.Second
	}

	deadline := time.Now().Add(time.Duration(deviceResp.ExpiresIn) * time.Second)

	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(interval):
		}

		token, err := m.pollForToken(ctx, deviceResp.DeviceCode)
		if err == nil {
			fmt.Printf("Authorization successful!\n")
			return token, nil
		}

		// Check if we should continue polling
		errStr := err.Error()
		if errStr == "oauth error: authorization_pending" || errStr == "oauth error: slow_down" {
			continue
		}

		return nil, err
	}

	return nil, fmt.Errorf("authorization timed out")
}

// pollForToken polls the token endpoint during device flow.
func (m *Manager) pollForToken(ctx context.Context, deviceCode string) (*oauth2.Token, error) {
	resp, err := http.PostForm("https://oauth2.googleapis.com/token", map[string][]string{
		"client_id":     {m.config.ClientID},
		"client_secret": {m.config.ClientSecret},
		"device_code":   {deviceCode},
		"grant_type":    {"urn:ietf:params:oauth:grant-type:device_code"},
	})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var tokenResp struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
		ExpiresIn    int    `json:"expires_in"`
		TokenType    string `json:"token_type"`
		Error        string `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&tokenResp); err != nil {
		return nil, err
	}

	if tokenResp.Error != "" {
		return nil, fmt.Errorf("oauth error: %s", tokenResp.Error)
	}

	return &oauth2.Token{
		AccessToken:  tokenResp.AccessToken,
		RefreshToken: tokenResp.RefreshToken,
		TokenType:    tokenResp.TokenType,
		Expiry:       time.Now().Add(time.Duration(tokenResp.ExpiresIn) * time.Second),
	}, nil
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

// saveToken saves a token for the given email, including the scopes from
// the manager's config.
func (m *Manager) saveToken(email string, token *oauth2.Token) error {
	if err := os.MkdirAll(m.tokensDir, 0700); err != nil {
		return err
	}

	tf := tokenFile{
		Token:  *token,
		Scopes: m.config.Scopes,
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

	config, err := google.ConfigFromJSON(data, scopes...)
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
