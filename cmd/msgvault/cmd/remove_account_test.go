package cmd

import (
	"strings"
	"testing"

	"github.com/wesm/msgvault/internal/config"
	"github.com/wesm/msgvault/internal/store"
)

func TestRemoveAccountCmd_RequiresEmail(t *testing.T) {
	root := newTestRootCmd()
	root.AddCommand(removeAccountCmd)
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
	cmd := *removeAccountCmd // shallow copy to avoid mutation
	root.AddCommand(&cmd)
	root.SetArgs([]string{"remove-account", "nobody@example.com", "--yes"})

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
	cmd := *removeAccountCmd
	root.AddCommand(&cmd)
	root.SetArgs([]string{"remove-account", "test@example.com", "--yes"})

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
