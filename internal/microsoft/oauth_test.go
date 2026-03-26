package microsoft

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"testing"

	"golang.org/x/oauth2"
)

func TestTokenPath(t *testing.T) {
	m := &Manager{tokensDir: "/tmp/tokens"}
	path := m.TokenPath("user@example.com")
	want := "/tmp/tokens/microsoft_user@example.com.json"
	if path != want {
		t.Errorf("TokenPath = %q, want %q", path, want)
	}
}

func TestSaveAndLoadToken(t *testing.T) {
	dir := t.TempDir()
	m := &Manager{tokensDir: dir}
	token := &oauth2.Token{
		AccessToken:  "access-123",
		RefreshToken: "refresh-456",
		TokenType:    "Bearer",
	}
	scopes := []string{"IMAP.AccessAsUser.All", "offline_access"}

	if err := m.saveToken("user@example.com", token, scopes); err != nil {
		t.Fatal(err)
	}

	loaded, err := m.loadTokenFile("user@example.com")
	if err != nil {
		t.Fatal(err)
	}
	if loaded.AccessToken != "access-123" {
		t.Errorf("AccessToken = %q, want %q", loaded.AccessToken, "access-123")
	}
	if loaded.RefreshToken != "refresh-456" {
		t.Errorf("RefreshToken = %q, want %q", loaded.RefreshToken, "refresh-456")
	}
	if len(loaded.Scopes) != 2 {
		t.Errorf("Scopes len = %d, want 2", len(loaded.Scopes))
	}

	// Verify file permissions
	path := m.TokenPath("user@example.com")
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0600 {
		t.Errorf("permissions = %o, want 0600", info.Mode().Perm())
	}
}

func TestHasToken(t *testing.T) {
	dir := t.TempDir()
	m := &Manager{tokensDir: dir}

	if m.HasToken("nobody@example.com") {
		t.Error("HasToken should be false for non-existent token")
	}

	token := &oauth2.Token{AccessToken: "test"}
	if err := m.saveToken("user@example.com", token, nil); err != nil {
		t.Fatal(err)
	}
	if !m.HasToken("user@example.com") {
		t.Error("HasToken should be true after save")
	}
}

func TestDeleteToken(t *testing.T) {
	dir := t.TempDir()
	m := &Manager{tokensDir: dir}

	token := &oauth2.Token{AccessToken: "test"}
	if err := m.saveToken("user@example.com", token, nil); err != nil {
		t.Fatal(err)
	}
	if err := m.DeleteToken("user@example.com"); err != nil {
		t.Fatal(err)
	}
	if m.HasToken("user@example.com") {
		t.Error("HasToken should be false after delete")
	}
	// Delete non-existent should not error
	if err := m.DeleteToken("nobody@example.com"); err != nil {
		t.Errorf("DeleteToken non-existent: %v", err)
	}
}

func TestSanitizeEmail(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"user@example.com", "user@example.com"},
		{"../evil", "_.._evil"},
		{"a/b", "a_b"},
		{"a\\b", "a_b"},
	}
	for _, tt := range tests {
		got := sanitizeEmail(tt.input)
		if got != tt.want {
			t.Errorf("sanitizeEmail(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

// makeIDToken builds a minimal unsigned JWT with the given claims.
// Only used in tests — the signature is not verified at runtime.
func makeIDToken(claims map[string]any) string {
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"RS256","typ":"JWT"}`))
	payload, _ := json.Marshal(claims)
	body := base64.RawURLEncoding.EncodeToString(payload)
	return header + "." + body + ".fake-sig"
}

func TestResolveTokenEmail_Match(t *testing.T) {
	m := &Manager{clientID: "test-client", tenantID: "common", tokensDir: t.TempDir()}
	idToken := makeIDToken(map[string]any{"email": "user@example.com"})
	token := (&oauth2.Token{AccessToken: "test-token", TokenType: "Bearer"}).
		WithExtra(map[string]any{"id_token": idToken})

	actual, err := m.resolveTokenEmail(t.Context(), "user@example.com", token)
	if err != nil {
		t.Fatal(err)
	}
	if actual != "user@example.com" {
		t.Errorf("actual = %q, want %q", actual, "user@example.com")
	}
}

func TestResolveTokenEmail_Mismatch(t *testing.T) {
	m := &Manager{clientID: "test-client", tenantID: "common", tokensDir: t.TempDir()}
	idToken := makeIDToken(map[string]any{"email": "other@example.com"})
	token := (&oauth2.Token{AccessToken: "test-token", TokenType: "Bearer"}).
		WithExtra(map[string]any{"id_token": idToken})

	_, err := m.resolveTokenEmail(t.Context(), "user@example.com", token)
	if err == nil {
		t.Fatal("expected error for mismatch")
	}
	if _, ok := err.(*TokenMismatchError); !ok {
		t.Errorf("expected *TokenMismatchError, got %T: %v", err, err)
	}
}

func TestResolveTokenEmail_FallbackToUPN(t *testing.T) {
	// Some accounts omit "email" and only have "preferred_username".
	m := &Manager{clientID: "test-client", tenantID: "common", tokensDir: t.TempDir()}
	idToken := makeIDToken(map[string]any{"preferred_username": "user@example.com"})
	token := (&oauth2.Token{AccessToken: "test-token", TokenType: "Bearer"}).
		WithExtra(map[string]any{"id_token": idToken})

	actual, err := m.resolveTokenEmail(t.Context(), "user@example.com", token)
	if err != nil {
		t.Fatal(err)
	}
	if actual != "user@example.com" {
		t.Errorf("actual = %q, want %q", actual, "user@example.com")
	}
}
