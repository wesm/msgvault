package oauth

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"golang.org/x/oauth2"
)

func setupTestManager(t *testing.T, scopes []string) *Manager {
	t.Helper()
	dir := t.TempDir()
	tokensDir := filepath.Join(dir, "tokens")
	if err := os.MkdirAll(tokensDir, 0700); err != nil {
		t.Fatal(err)
	}
	return &Manager{
		config:    &oauth2.Config{Scopes: scopes},
		tokensDir: tokensDir,
	}
}

func writeTokenFile(t *testing.T, mgr *Manager, email string, token oauth2.Token, scopes []string) {
	t.Helper()
	tf := tokenFile{
		Token:  token,
		Scopes: scopes,
	}
	data, err := json.Marshal(tf)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(mgr.tokensDir, email+".json"), data, 0600); err != nil {
		t.Fatal(err)
	}
}

func writeLegacyTokenFile(t *testing.T, mgr *Manager, email string, token oauth2.Token) {
	t.Helper()
	data, err := json.Marshal(token)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(mgr.tokensDir, email+".json"), data, 0600); err != nil {
		t.Fatal(err)
	}
}

var testToken = oauth2.Token{AccessToken: "test", TokenType: "Bearer"}

// assertNoSend is a test helper to assert that a channel remains empty.
// Uses a 100ms timeout to balance between flakiness on slow CI and detection
// of late asynchronous sends.
func assertNoSend[T any](t *testing.T, ch <-chan T, chanName string) {
	t.Helper()
	const noSendTimeout = 100 * time.Millisecond
	select {
	case v := <-ch:
		t.Errorf("unexpected value on %s: %v", chanName, v)
	case <-time.After(noSendTimeout):
		// expected: no value arrived
	}
}

func TestScopesToString(t *testing.T) {
	tests := []struct {
		name   string
		scopes []string
		want   string
	}{
		{
			name:   "empty scopes",
			scopes: []string{},
			want:   "",
		},
		{
			name:   "single scope",
			scopes: []string{"https://www.googleapis.com/auth/gmail.readonly"},
			want:   "https://www.googleapis.com/auth/gmail.readonly",
		},
		{
			name:   "multiple scopes",
			scopes: []string{"https://www.googleapis.com/auth/gmail.readonly", "https://www.googleapis.com/auth/gmail.modify"},
			want:   "https://www.googleapis.com/auth/gmail.readonly https://www.googleapis.com/auth/gmail.modify",
		},
		{
			name:   "three scopes",
			scopes: []string{"scope1", "scope2", "scope3"},
			want:   "scope1 scope2 scope3",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := scopesToString(tt.scopes)
			if got != tt.want {
				t.Errorf("scopesToString() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestHasScope(t *testing.T) {
	mgr := setupTestManager(t, Scopes)

	writeTokenFile(t, mgr, "test@gmail.com", testToken, []string{
		"https://www.googleapis.com/auth/gmail.readonly",
		"https://www.googleapis.com/auth/gmail.modify",
	})

	// Has a scope that was saved
	if !mgr.HasScope("test@gmail.com", "https://www.googleapis.com/auth/gmail.readonly") {
		t.Error("expected HasScope to return true for gmail.readonly")
	}

	// Does not have deletion scope
	if mgr.HasScope("test@gmail.com", "https://mail.google.com/") {
		t.Error("expected HasScope to return false for mail.google.com")
	}

	// Non-existent account
	if mgr.HasScope("missing@gmail.com", "https://www.googleapis.com/auth/gmail.readonly") {
		t.Error("expected HasScope to return false for missing account")
	}
}

func TestTokenFileScopesRoundTrip(t *testing.T) {
	mgr := setupTestManager(t, ScopesDeletion)

	token := &oauth2.Token{
		AccessToken:  "access",
		RefreshToken: "refresh",
		TokenType:    "Bearer",
	}

	if err := mgr.saveToken("test@gmail.com", token, ScopesDeletion); err != nil {
		t.Fatal(err)
	}

	// Load and verify scopes were saved
	tf, err := mgr.loadTokenFile("test@gmail.com")
	if err != nil {
		t.Fatal(err)
	}

	if len(tf.Scopes) != 1 || tf.Scopes[0] != "https://mail.google.com/" {
		t.Errorf("expected ScopesDeletion, got %v", tf.Scopes)
	}

	// loadToken should still work (returns just the token)
	loaded, err := mgr.loadToken("test@gmail.com")
	if err != nil {
		t.Fatal(err)
	}
	if loaded.AccessToken != "access" {
		t.Errorf("expected access token 'access', got %q", loaded.AccessToken)
	}
}

func TestSaveToken_OverwriteExisting(t *testing.T) {
	mgr := setupTestManager(t, Scopes)

	token1 := &oauth2.Token{
		AccessToken:  "first",
		RefreshToken: "refresh1",
		TokenType:    "Bearer",
	}
	if err := mgr.saveToken("test@gmail.com", token1, Scopes); err != nil {
		t.Fatal(err)
	}

	// Save again with a different access token — must overwrite (not fail).
	token2 := &oauth2.Token{
		AccessToken:  "second",
		RefreshToken: "refresh2",
		TokenType:    "Bearer",
	}
	if err := mgr.saveToken("test@gmail.com", token2, Scopes); err != nil {
		t.Fatalf("second saveToken should overwrite existing file: %v", err)
	}

	loaded, err := mgr.loadToken("test@gmail.com")
	if err != nil {
		t.Fatal(err)
	}
	if loaded.AccessToken != "second" {
		t.Errorf("expected access token 'second' after overwrite, got %q", loaded.AccessToken)
	}
}

func TestHasScope_LegacyToken(t *testing.T) {
	mgr := setupTestManager(t, Scopes)

	writeLegacyTokenFile(t, mgr, "legacy@gmail.com", testToken)

	if mgr.HasScope("legacy@gmail.com", "https://www.googleapis.com/auth/gmail.readonly") {
		t.Error("expected HasScope to return false for legacy token")
	}
}

func TestHasScopeMetadata(t *testing.T) {
	mgr := setupTestManager(t, Scopes)

	writeTokenFile(t, mgr, "scoped@gmail.com", testToken, []string{
		"https://www.googleapis.com/auth/gmail.readonly",
	})
	writeLegacyTokenFile(t, mgr, "legacy@gmail.com", testToken)
	if err := os.WriteFile(filepath.Join(mgr.tokensDir, "corrupt@gmail.com.json"), []byte("not json"), 0600); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name  string
		email string
		want  bool
	}{
		{"valid scoped token", "scoped@gmail.com", true},
		{"legacy token", "legacy@gmail.com", false},
		{"missing token", "missing@gmail.com", false},
		{"corrupt token file", "corrupt@gmail.com", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mgr.HasScopeMetadata(tt.email)
			if got != tt.want {
				t.Errorf("HasScopeMetadata(%q) = %v, want %v", tt.email, got, tt.want)
			}
		})
	}
}

func TestShellQuote(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"/path/to/file", "'/path/to/file'"},
		{"/path with spaces/file", "'/path with spaces/file'"},
		{"/path/with'quote/file", "'/path/with'\\''quote/file'"},
		{"simple", "'simple'"},
		{"", "''"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := shellQuote(tt.input)
			if got != tt.want {
				t.Errorf("shellQuote(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestSanitizeEmail(t *testing.T) {
	tests := []struct {
		email string
		want  string
	}{
		{"user@gmail.com", "user@gmail.com"},
		{"user/slash@gmail.com", "user_slash@gmail.com"},
		{"user\\backslash@gmail.com", "user_backslash@gmail.com"},
		{"user..dots@gmail.com", "user_dots@gmail.com"},
		{"../../../etc/passwd", "______etc_passwd"},
	}

	for _, tt := range tests {
		t.Run(tt.email, func(t *testing.T) {
			got := sanitizeEmail(tt.email)
			if got != tt.want {
				t.Errorf("sanitizeEmail(%q) = %q, want %q", tt.email, got, tt.want)
			}
		})
	}
}

func TestTokenPath_SymlinkEscape(t *testing.T) {
	// This test verifies that symlinks inside tokensDir cannot be used
	// to write tokens outside the tokens directory.
	//
	// Attack scenario:
	// 1. Attacker creates symlink: tokensDir/evil.json -> /tmp/outside/evil.json
	// 2. saveToken("evil", ...) would follow the symlink and write outside tokensDir
	// 3. The fix should detect this and use a hash-based fallback path

	dir := t.TempDir()
	tokensDir := filepath.Join(dir, "tokens")
	outsideDir := filepath.Join(dir, "outside")

	if err := os.MkdirAll(tokensDir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(outsideDir, 0700); err != nil {
		t.Fatal(err)
	}

	// Create a symlink inside tokensDir that points outside
	symlinkPath := filepath.Join(tokensDir, "evil.json")
	outsideTarget := filepath.Join(outsideDir, "evil.json")
	if err := os.Symlink(outsideTarget, symlinkPath); err != nil {
		t.Skipf("cannot create symlink (may require admin on Windows): %v", err)
	}

	mgr := &Manager{
		config:    &oauth2.Config{Scopes: Scopes},
		tokensDir: tokensDir,
	}

	// Get the token path for "evil" - this should NOT return the symlink path
	// because following it would write outside tokensDir
	gotPath := mgr.tokenPath("evil")

	// The path should NOT be the symlink (which would write outside tokensDir)
	if gotPath == symlinkPath {
		t.Errorf("tokenPath returned symlink path %q, should use hash-based fallback to prevent escape", gotPath)
	}

	// Verify the returned path is exactly the expected hash-based fallback
	expectedPath := filepath.Join(tokensDir, fmt.Sprintf("%x.json", sha256.Sum256([]byte("evil"))))
	if gotPath != expectedPath {
		t.Errorf("tokenPath = %q, want hash-based fallback %q", gotPath, expectedPath)
	}
}

func TestHasPathPrefix(t *testing.T) {
	tests := []struct {
		name string
		path string
		dir  string
		want bool
	}{
		{"child path", "/a/b/c", "/a/b", true},
		{"exact match", "/a/b", "/a/b", true},
		{"prefix attack", "/a/b-evil/c", "/a/b", false},
		{"sibling", "/a/c", "/a/b", false},
		{"parent escape", "/a", "/a/b", false},
		{"root dir child", "/foo", "/", true},
		{"root dir exact", "/", "/", true},
		{"unrelated", "/x/y", "/a/b", false},
		{"dotdot prefix child", "/a/b/..backup", "/a/b", true},
	}

	// Add Windows drive-root cases when running on Windows.
	if runtime.GOOS == "windows" {
		vol := filepath.VolumeName(os.TempDir())
		root := vol + string(filepath.Separator)
		tests = append(tests,
			struct {
				name string
				path string
				dir  string
				want bool
			}{"windows drive root exact", root, root, true},
			struct {
				name string
				path string
				dir  string
				want bool
			}{"windows drive root child", root + "Users", root, true},
		)
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := hasPathPrefix(tt.path, tt.dir)
			if got != tt.want {
				t.Errorf("hasPathPrefix(%q, %q) = %v, want %v", tt.path, tt.dir, got, tt.want)
			}
		})
	}
}

func TestParseClientSecrets(t *testing.T) {
	// Valid Desktop application credentials
	validDesktop := `{
		"installed": {
			"client_id": "123.apps.googleusercontent.com",
			"client_secret": "secret",
			"auth_uri": "https://accounts.google.com/o/oauth2/auth",
			"token_uri": "https://oauth2.googleapis.com/token",
			"redirect_uris": ["http://localhost"]
		}
	}`

	// Valid Web application credentials
	validWeb := `{
		"web": {
			"client_id": "123.apps.googleusercontent.com",
			"client_secret": "secret",
			"auth_uri": "https://accounts.google.com/o/oauth2/auth",
			"token_uri": "https://oauth2.googleapis.com/token",
			"redirect_uris": ["http://localhost:8080/callback"]
		}
	}`

	// TV/device client (no redirect_uris in installed)
	tvClient := `{
		"installed": {
			"client_id": "123.apps.googleusercontent.com",
			"client_secret": "secret",
			"auth_uri": "https://accounts.google.com/o/oauth2/auth",
			"token_uri": "https://oauth2.googleapis.com/token"
		}
	}`

	// Web client missing redirect_uris
	webNoRedirects := `{
		"web": {
			"client_id": "123.apps.googleusercontent.com",
			"client_secret": "secret",
			"auth_uri": "https://accounts.google.com/o/oauth2/auth",
			"token_uri": "https://oauth2.googleapis.com/token"
		}
	}`

	// Malformed JSON
	malformedJSON := `{not valid json`

	tests := []struct {
		name    string
		data    string
		wantErr string
	}{
		{
			name:    "valid desktop client",
			data:    validDesktop,
			wantErr: "",
		},
		{
			name:    "valid web client",
			data:    validWeb,
			wantErr: "",
		},
		{
			name:    "TV/device client rejected",
			data:    tvClient,
			wantErr: "missing redirect_uris",
		},
		{
			name:    "web client without redirect_uris rejected",
			data:    webNoRedirects,
			wantErr: "missing redirect_uris",
		},
		{
			name:    "malformed JSON",
			data:    malformedJSON,
			wantErr: "invalid character",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parseClientSecrets([]byte(tt.data), Scopes)
			if tt.wantErr == "" {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			} else {
				if err == nil {
					t.Error("expected error, got nil")
				} else if !strings.Contains(err.Error(), tt.wantErr) {
					t.Errorf("error = %q, want to contain %q", err.Error(), tt.wantErr)
				}
			}
		})
	}
}

func TestNewCallbackHandler(t *testing.T) {
	mgr := setupTestManager(t, Scopes)

	tests := []struct {
		name             string
		queryState       string
		expectedState    string
		queryCode        string
		wantStatusCode   int
		wantBodyContains string
		wantCode         string
		wantErr          string
	}{
		{
			name:             "success",
			queryState:       "valid-state",
			expectedState:    "valid-state",
			queryCode:        "auth-code-123",
			wantStatusCode:   http.StatusOK,
			wantBodyContains: "Authorization successful",
			wantCode:         "auth-code-123",
		},
		{
			name:             "state mismatch",
			queryState:       "wrong-state",
			expectedState:    "expected-state",
			queryCode:        "auth-code-123",
			wantStatusCode:   http.StatusOK,
			wantBodyContains: "state mismatch",
			wantErr:          "state mismatch: possible CSRF attack",
		},
		{
			name:             "missing code",
			queryState:       "valid-state",
			expectedState:    "valid-state",
			queryCode:        "",
			wantStatusCode:   http.StatusOK,
			wantBodyContains: "no authorization code",
			wantErr:          "no code in callback",
		},
		{
			name:             "empty state",
			queryState:       "",
			expectedState:    "expected-state",
			queryCode:        "auth-code-123",
			wantStatusCode:   http.StatusOK,
			wantBodyContains: "state mismatch",
			wantErr:          "state mismatch: possible CSRF attack",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			codeChan := make(chan string, 1)
			errChan := make(chan error, 1)

			handler := mgr.newCallbackHandler(tt.expectedState, codeChan, errChan)

			url := "/callback?state=" + tt.queryState
			if tt.queryCode != "" {
				url += "&code=" + tt.queryCode
			}
			req := httptest.NewRequest(http.MethodGet, url, nil)
			rec := httptest.NewRecorder()

			handler(rec, req)

			if rec.Code != tt.wantStatusCode {
				t.Errorf("status code = %d, want %d", rec.Code, tt.wantStatusCode)
			}

			body := rec.Body.String()
			if tt.wantBodyContains != "" && !strings.Contains(body, tt.wantBodyContains) {
				t.Errorf("body = %q, want to contain %q", body, tt.wantBodyContains)
			}

			// Check for expected code on success
			if tt.wantCode != "" {
				select {
				case code := <-codeChan:
					if code != tt.wantCode {
						t.Errorf("code = %q, want %q", code, tt.wantCode)
					}
				default:
					t.Error("expected code on codeChan, got nothing")
				}
			} else {
				assertNoSend(t, codeChan, "codeChan")
			}

			// Check for expected error
			if tt.wantErr != "" {
				select {
				case err := <-errChan:
					if err.Error() != tt.wantErr {
						t.Errorf("error = %q, want %q", err.Error(), tt.wantErr)
					}
				default:
					t.Error("expected error on errChan, got nothing")
				}
			} else {
				assertNoSend(t, errChan, "errChan")
			}
		})
	}
}

func TestValidateBrowserURL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		url     string
		wantErr string
	}{
		{"http allowed", "http://localhost:8080/callback", ""},
		{"https allowed", "https://accounts.google.com/o/oauth2/auth", ""},
		{"HTTP uppercase allowed", "HTTP://example.com", ""},
		{"Https mixed case allowed", "Https://example.com", ""},
		{"HTTPS all caps allowed", "HTTPS://example.com", ""},
		{"file scheme rejected", "file:///etc/passwd", "only http and https are allowed"},
		{"javascript scheme rejected", "javascript:alert(1)", "only http and https are allowed"},
		{"custom scheme rejected", "myapp://callback", "only http and https are allowed"},
		{"ftp scheme rejected", "ftp://example.com/file", "only http and https are allowed"},
		{"empty scheme rejected", "://no-scheme", "invalid URL"},
		{"no scheme rejected", "example.com", "only http and https are allowed"},
		{"malformed URL", "://", "invalid URL"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := validateBrowserURL(tt.url)
			if tt.wantErr == "" {
				if err != nil {
					t.Errorf("validateBrowserURL(%q) = %v, want nil", tt.url, err)
				}
			} else {
				if err == nil {
					t.Errorf("validateBrowserURL(%q) = nil, want error containing %q", tt.url, tt.wantErr)
				} else if !strings.Contains(err.Error(), tt.wantErr) {
					t.Errorf("validateBrowserURL(%q) error = %q, want to contain %q", tt.url, err.Error(), tt.wantErr)
				}
			}
		})
	}
}
