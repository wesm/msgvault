package oauth

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

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

	if err := mgr.saveToken("test@gmail.com", token); err != nil {
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
