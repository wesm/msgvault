package cmd

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/wesm/msgvault/internal/config"
	"github.com/wesm/msgvault/internal/oauth"
	"github.com/wesm/msgvault/internal/store"
)

func TestRemoveAccountCmd_RequiresEmail(t *testing.T) {
	root := newTestRootCmd()
	root.AddCommand(newRemoveAccountCmd())
	root.SetArgs([]string{"remove-account"})

	err := root.Execute()
	if err == nil {
		t.Fatal("expected error for missing email arg")
	}
}

func TestRemoveAccountCmd_NotFound(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := tmpDir + "/msgvault.db"

	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if err := s.InitSchema(); err != nil {
		t.Fatalf("init schema: %v", err)
	}
	_ = s.Close()

	savedCfg := cfg
	defer func() { cfg = savedCfg }()

	cfg = &config.Config{
		HomeDir: tmpDir,
		Data:    config.DataConfig{DataDir: tmpDir},
	}

	root := newTestRootCmd()
	root.AddCommand(newRemoveAccountCmd())
	root.SetArgs([]string{
		"remove-account", "nobody@example.com", "--yes",
	})

	err = root.Execute()
	if err == nil {
		t.Fatal("expected error for unknown email")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("error = %q, want 'not found'", err.Error())
	}
}

func TestRemoveAccountCmd_WithYesFlag(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := tmpDir + "/msgvault.db"

	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if err := s.InitSchema(); err != nil {
		t.Fatalf("init schema: %v", err)
	}

	_, err = s.GetOrCreateSource("gmail", "test@example.com")
	if err != nil {
		t.Fatalf("create source: %v", err)
	}
	_ = s.Close()

	savedCfg := cfg
	defer func() { cfg = savedCfg }()

	cfg = &config.Config{
		HomeDir: tmpDir,
		Data:    config.DataConfig{DataDir: tmpDir},
	}

	root := newTestRootCmd()
	root.AddCommand(newRemoveAccountCmd())
	root.SetArgs([]string{
		"remove-account", "test@example.com", "--yes",
	})

	err = root.Execute()
	if err != nil {
		t.Fatalf("remove-account --yes: %v", err)
	}

	// Verify account is gone
	s, err = store.Open(dbPath)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer func() { _ = s.Close() }()
	if err := s.InitSchema(); err != nil {
		t.Fatalf("reinit schema: %v", err)
	}

	src, err := s.GetSourceByIdentifier("test@example.com")
	if err != nil {
		t.Fatalf("GetSourceByIdentifier: %v", err)
	}
	if src != nil {
		t.Error("account should be removed after --yes")
	}
}

func TestRemoveAccountCmd_DuplicateIdentifierRequiresType(
	t *testing.T,
) {
	tmpDir := t.TempDir()
	dbPath := tmpDir + "/msgvault.db"

	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if err := s.InitSchema(); err != nil {
		t.Fatalf("init schema: %v", err)
	}

	_, err = s.GetOrCreateSource("gmail", "dup@example.com")
	if err != nil {
		t.Fatalf("create gmail source: %v", err)
	}
	_, err = s.GetOrCreateSource("mbox", "dup@example.com")
	if err != nil {
		t.Fatalf("create mbox source: %v", err)
	}
	_ = s.Close()

	savedCfg := cfg
	defer func() { cfg = savedCfg }()

	cfg = &config.Config{
		HomeDir: tmpDir,
		Data:    config.DataConfig{DataDir: tmpDir},
	}

	// Without --type should fail
	root := newTestRootCmd()
	root.AddCommand(newRemoveAccountCmd())
	root.SetArgs([]string{
		"remove-account", "dup@example.com", "--yes",
	})

	err = root.Execute()
	if err == nil {
		t.Fatal("expected error for ambiguous identifier")
	}
	if !strings.Contains(err.Error(), "multiple accounts") {
		t.Errorf("error = %q, want 'multiple accounts'", err.Error())
	}

	// With --type should succeed
	root2 := newTestRootCmd()
	root2.AddCommand(newRemoveAccountCmd())
	root2.SetArgs([]string{
		"remove-account", "dup@example.com",
		"--yes", "--type", "mbox",
	})

	err = root2.Execute()
	if err != nil {
		t.Fatalf("remove-account --type mbox: %v", err)
	}

	// Verify only mbox source was removed
	s, err = store.Open(dbPath)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer func() { _ = s.Close() }()
	if err := s.InitSchema(); err != nil {
		t.Fatalf("reinit schema: %v", err)
	}

	sources, err := s.GetSourcesByIdentifier("dup@example.com")
	if err != nil {
		t.Fatalf("GetSourcesByIdentifier: %v", err)
	}
	if len(sources) != 1 {
		t.Fatalf("got %d sources, want 1", len(sources))
	}
	if sources[0].SourceType != "gmail" {
		t.Errorf(
			"remaining source type = %q, want gmail",
			sources[0].SourceType,
		)
	}
}

func TestRemoveAccountCmd_GmailRemovesToken(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := tmpDir + "/msgvault.db"
	tokensDir := filepath.Join(tmpDir, "tokens")
	if err := os.MkdirAll(tokensDir, 0700); err != nil {
		t.Fatalf("mkdir tokens: %v", err)
	}

	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if err := s.InitSchema(); err != nil {
		t.Fatalf("init schema: %v", err)
	}
	_, err = s.GetOrCreateSource("gmail", "tok@example.com")
	if err != nil {
		t.Fatalf("create source: %v", err)
	}
	_ = s.Close()

	// Create a fake token file
	tokenPath := oauth.TokenFilePath(tokensDir, "tok@example.com")
	if err := os.WriteFile(tokenPath, []byte(`{}`), 0600); err != nil {
		t.Fatalf("write token: %v", err)
	}

	savedCfg := cfg
	defer func() { cfg = savedCfg }()

	cfg = &config.Config{
		HomeDir: tmpDir,
		Data:    config.DataConfig{DataDir: tmpDir},
	}

	root := newTestRootCmd()
	root.AddCommand(newRemoveAccountCmd())
	root.SetArgs([]string{
		"remove-account", "tok@example.com", "--yes",
	})

	err = root.Execute()
	if err != nil {
		t.Fatalf("remove-account: %v", err)
	}

	if _, err := os.Stat(tokenPath); !os.IsNotExist(err) {
		t.Error("token file should be removed for gmail source")
	}
}

func TestRemoveAccountCmd_NonGmailSkipsToken(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := tmpDir + "/msgvault.db"
	tokensDir := filepath.Join(tmpDir, "tokens")
	if err := os.MkdirAll(tokensDir, 0700); err != nil {
		t.Fatalf("mkdir tokens: %v", err)
	}

	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if err := s.InitSchema(); err != nil {
		t.Fatalf("init schema: %v", err)
	}
	_, err = s.GetOrCreateSource("mbox", "imp@example.com")
	if err != nil {
		t.Fatalf("create source: %v", err)
	}
	_ = s.Close()

	// Create a token file that should NOT be removed
	tokenPath := oauth.TokenFilePath(tokensDir, "imp@example.com")
	if err := os.WriteFile(tokenPath, []byte(`{}`), 0600); err != nil {
		t.Fatalf("write token: %v", err)
	}

	savedCfg := cfg
	defer func() { cfg = savedCfg }()

	cfg = &config.Config{
		HomeDir: tmpDir,
		Data:    config.DataConfig{DataDir: tmpDir},
	}

	root := newTestRootCmd()
	root.AddCommand(newRemoveAccountCmd())
	root.SetArgs([]string{
		"remove-account", "imp@example.com", "--yes",
	})

	err = root.Execute()
	if err != nil {
		t.Fatalf("remove-account: %v", err)
	}

	if _, err := os.Stat(tokenPath); os.IsNotExist(err) {
		t.Error("token file should NOT be removed for non-gmail source")
	}
}

func TestRemoveAccountCmd_ClosedStdinReturnsError(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := tmpDir + "/msgvault.db"

	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if err := s.InitSchema(); err != nil {
		t.Fatalf("init schema: %v", err)
	}
	_, err = s.GetOrCreateSource("gmail", "eof@example.com")
	if err != nil {
		t.Fatalf("create source: %v", err)
	}
	_ = s.Close()

	savedCfg := cfg
	defer func() { cfg = savedCfg }()

	cfg = &config.Config{
		HomeDir: tmpDir,
		Data:    config.DataConfig{DataDir: tmpDir},
	}

	// Replace stdin with a closed pipe to simulate EOF
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("create pipe: %v", err)
	}
	_ = w.Close()

	origStdin := os.Stdin
	os.Stdin = r
	defer func() {
		os.Stdin = origStdin
		_ = r.Close()
	}()

	// Run WITHOUT --yes so it tries to read confirmation
	root := newTestRootCmd()
	root.AddCommand(newRemoveAccountCmd())
	root.SetArgs([]string{"remove-account", "eof@example.com"})

	err = root.Execute()
	if err == nil {
		t.Fatal("expected error when stdin is closed")
	}
	if !strings.Contains(err.Error(), "use --yes") {
		t.Errorf("error = %q, want 'use --yes'", err.Error())
	}
}
