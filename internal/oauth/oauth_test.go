package oauth

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/oauth2"
)

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
	dir := t.TempDir()
	tokensDir := filepath.Join(dir, "tokens")
	if err := os.MkdirAll(tokensDir, 0700); err != nil {
		t.Fatal(err)
	}

	mgr := &Manager{
		config:    &oauth2.Config{Scopes: Scopes},
		tokensDir: tokensDir,
	}

	// Write a token file with scopes
	tf := tokenFile{
		Token:  oauth2.Token{AccessToken: "test", TokenType: "Bearer"},
		Scopes: []string{"https://www.googleapis.com/auth/gmail.readonly", "https://www.googleapis.com/auth/gmail.modify"},
	}
	data, _ := json.Marshal(tf)
	if err := os.WriteFile(filepath.Join(tokensDir, "test@gmail.com.json"), data, 0600); err != nil {
		t.Fatal(err)
	}

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
	dir := t.TempDir()
	tokensDir := filepath.Join(dir, "tokens")

	mgr := &Manager{
		config:    &oauth2.Config{Scopes: ScopesDeletion},
		tokensDir: tokensDir,
	}

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
	dir := t.TempDir()
	tokensDir := filepath.Join(dir, "tokens")
	if err := os.MkdirAll(tokensDir, 0700); err != nil {
		t.Fatal(err)
	}

	mgr := &Manager{
		config:    &oauth2.Config{Scopes: Scopes},
		tokensDir: tokensDir,
	}

	// Write a legacy token (no scopes field)
	token := oauth2.Token{AccessToken: "test", TokenType: "Bearer"}
	data, _ := json.Marshal(token)
	if err := os.WriteFile(filepath.Join(tokensDir, "legacy@gmail.com.json"), data, 0600); err != nil {
		t.Fatal(err)
	}

	// Legacy token has no scopes — HasScope returns false
	if mgr.HasScope("legacy@gmail.com", "https://www.googleapis.com/auth/gmail.readonly") {
		t.Error("expected HasScope to return false for legacy token")
	}
}
