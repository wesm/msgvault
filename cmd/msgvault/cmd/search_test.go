package cmd

import (
	"strings"
	"testing"

	"github.com/wesm/msgvault/internal/config"
	"github.com/wesm/msgvault/internal/store"
)

func resetSearchFlags() {
	searchAccount = ""
	searchLimit = 50
	searchOffset = 0
	searchJSON = false
}

func TestSearchCmd_AccountFlagRejectsRemoteMode(t *testing.T) {
	savedCfg := cfg
	defer func() { cfg = savedCfg; resetSearchFlags() }()

	cfg = &config.Config{}
	cfg.Remote.URL = "http://example.com"

	root := newTestRootCmd()
	root.AddCommand(searchCmd)
	root.SetArgs([]string{"search", "--account", "a@b.com", "hello"})

	err := root.Execute()
	if err == nil {
		t.Fatal("expected error when --account used in remote mode")
	}
	if !strings.Contains(err.Error(), "not supported in remote mode") {
		t.Errorf("error = %q, want 'not supported in remote mode'", err)
	}
}

func TestSearchCmd_AccountFlagWithoutQuery(t *testing.T) {
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
		t.Fatalf("add source: %v", err)
	}
	_ = s.Close()

	savedCfg := cfg
	defer func() { cfg = savedCfg; resetSearchFlags() }()

	cfg = &config.Config{
		HomeDir: tmpDir,
		Data:    config.DataConfig{DataDir: tmpDir},
	}

	root := newTestRootCmd()
	root.AddCommand(searchCmd)
	root.SetArgs([]string{"search", "--account", "test@example.com"})

	// Should not error on empty query — account-only is valid.
	// It may return "No messages found." but should not fail
	// with "empty search query".
	err = root.Execute()
	if err != nil && strings.Contains(err.Error(), "empty search query") {
		t.Errorf("account-only search rejected as empty: %v", err)
	}
}

func TestSearchCmd_NoQueryNoAccount(t *testing.T) {
	savedCfg := cfg
	defer func() { cfg = savedCfg; resetSearchFlags() }()

	cfg = &config.Config{}

	root := newTestRootCmd()
	root.AddCommand(searchCmd)
	root.SetArgs([]string{"search"})

	err := root.Execute()
	if err == nil {
		t.Fatal("expected error for search with no query and no --account")
	}
	if !strings.Contains(err.Error(), "provide a search query") {
		t.Errorf("error = %q, want 'provide a search query'", err)
	}
}
