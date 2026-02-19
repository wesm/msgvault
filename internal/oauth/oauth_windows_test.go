//go:build windows

package oauth

import (
	"log/slog"
	"os"
	"strings"
	"testing"

	"golang.org/x/oauth2"
)

// TestSaveToken_Windows_Overwrite verifies that saveToken correctly overwrites
// existing token files on Windows, where os.Rename doesn't overwrite by default.
func TestSaveToken_Windows_Overwrite(t *testing.T) {
	tmpDir := t.TempDir()

	mgr := &Manager{
		config: &oauth2.Config{
			Scopes: []string{"scope1"},
		},
		tokensDir: tmpDir,
		logger:    slog.Default(),
	}

	email := "test@example.com"

	// Save initial token
	token1 := &oauth2.Token{AccessToken: "token1"}
	if err := mgr.saveToken(email, token1, []string{"scope1"}); err != nil {
		t.Fatalf("saveToken (first): %v", err)
	}

	// Verify initial token was saved
	loaded1, err := mgr.loadToken(email)
	if err != nil {
		t.Fatalf("loadToken (first): %v", err)
	}
	if loaded1.AccessToken != "token1" {
		t.Errorf("first token = %q, want %q", loaded1.AccessToken, "token1")
	}

	// Save second token (should overwrite)
	token2 := &oauth2.Token{AccessToken: "token2"}
	if err := mgr.saveToken(email, token2, []string{"scope1", "scope2"}); err != nil {
		t.Fatalf("saveToken (second): %v", err)
	}

	// Verify second token replaced the first
	loaded2, err := mgr.loadToken(email)
	if err != nil {
		t.Fatalf("loadToken (second): %v", err)
	}
	if loaded2.AccessToken != "token2" {
		t.Errorf("second token = %q, want %q", loaded2.AccessToken, "token2")
	}

	// Verify scopes were updated
	tf, err := mgr.loadTokenFile(email)
	if err != nil {
		t.Fatalf("loadTokenFile: %v", err)
	}
	if len(tf.Scopes) != 2 || tf.Scopes[0] != "scope1" || tf.Scopes[1] != "scope2" {
		t.Errorf("scopes = %v, want [scope1 scope2]", tf.Scopes)
	}
}

// TestSaveToken_Windows_ErrorHandling verifies that saveToken properly handles
// errors when removing existing files on Windows, such as permission denied.
func TestSaveToken_Windows_ErrorHandling(t *testing.T) {
	tmpDir := t.TempDir()

	mgr := &Manager{
		config: &oauth2.Config{
			Scopes: []string{"scope1"},
		},
		tokensDir: tmpDir,
		logger:    slog.Default(),
	}

	email := "test@example.com"
	tokenPath := mgr.tokenPath(email)

	// Create a directory at the token path (can't be removed by os.Remove(file))
	if err := os.Mkdir(tokenPath, 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	// Attempt to save token should fail when trying to remove the directory
	token := &oauth2.Token{AccessToken: "token"}
	err := mgr.saveToken(email, token, []string{"scope1"})
	if err == nil {
		t.Fatal("saveToken should fail when token path is a directory")
	}

	// Error should mention "remove existing token file" (our custom error wrapping)
	if !strings.Contains(err.Error(), "remove existing token file") {
		t.Errorf("error = %q, should mention 'remove existing token file'", err)
	}
}

// TestSaveToken_Windows_InitialSave verifies that saveToken works correctly
// when no existing token file exists (the os.Remove should succeed with NotExist).
func TestSaveToken_Windows_InitialSave(t *testing.T) {
	tmpDir := t.TempDir()

	mgr := &Manager{
		config: &oauth2.Config{
			Scopes: []string{"scope1"},
		},
		tokensDir: tmpDir,
		logger:    slog.Default(),
	}

	email := "new-user@example.com"

	// Save token when no file exists (initial save)
	token := &oauth2.Token{AccessToken: "initial-token"}
	if err := mgr.saveToken(email, token, []string{"scope1"}); err != nil {
		t.Fatalf("saveToken (initial): %v", err)
	}

	// Verify token was saved
	loaded, err := mgr.loadToken(email)
	if err != nil {
		t.Fatalf("loadToken: %v", err)
	}
	if loaded.AccessToken != "initial-token" {
		t.Errorf("token = %q, want %q", loaded.AccessToken, "initial-token")
	}
}
