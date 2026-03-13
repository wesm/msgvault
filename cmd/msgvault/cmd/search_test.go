package cmd

import (
	"database/sql"
	"os"
	"strings"
	"testing"

	"github.com/wesm/msgvault/internal/config"
	"github.com/wesm/msgvault/internal/store"
)

// captureStdout redirects os.Stdout to a pipe and returns a function
// that restores the original stdout and returns captured output.
func captureStdout(t *testing.T) func() string {
	t.Helper()
	origStdout := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("create pipe: %v", err)
	}
	os.Stdout = w

	return func() string {
		_ = w.Close()
		os.Stdout = origStdout
		buf := make([]byte, 64*1024)
		n, _ := r.Read(buf)
		_ = r.Close()
		return string(buf[:n])
	}
}

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

	// Seed two accounts with one message each.
	src1, err := s.GetOrCreateSource("gmail", "alice@example.com")
	if err != nil {
		t.Fatalf("create source 1: %v", err)
	}
	src2, err := s.GetOrCreateSource("gmail", "bob@example.com")
	if err != nil {
		t.Fatalf("create source 2: %v", err)
	}
	conv1, err := s.EnsureConversation(src1.ID, "c1", "")
	if err != nil {
		t.Fatalf("create conv 1: %v", err)
	}
	conv2, err := s.EnsureConversation(src2.ID, "c2", "")
	if err != nil {
		t.Fatalf("create conv 2: %v", err)
	}
	_, err = s.UpsertMessage(&store.Message{
		SourceID: src1.ID, ConversationID: conv1,
		SourceMessageID: "m1", MessageType: "email",
		Subject:      sql.NullString{String: "Alice msg", Valid: true},
		SizeEstimate: 100,
	})
	if err != nil {
		t.Fatalf("insert msg 1: %v", err)
	}
	_, err = s.UpsertMessage(&store.Message{
		SourceID: src2.ID, ConversationID: conv2,
		SourceMessageID: "m2", MessageType: "email",
		Subject:      sql.NullString{String: "Bob msg", Valid: true},
		SizeEstimate: 200,
	})
	if err != nil {
		t.Fatalf("insert msg 2: %v", err)
	}
	_ = s.Close()

	savedCfg := cfg
	defer func() { cfg = savedCfg; resetSearchFlags() }()

	cfg = &config.Config{
		HomeDir: tmpDir,
		Data:    config.DataConfig{DataDir: tmpDir},
	}

	// Search with --account only (no query terms) — must succeed.
	done := captureStdout(t)

	root := newTestRootCmd()
	root.AddCommand(searchCmd)
	root.SetArgs([]string{
		"search", "--account", "alice@example.com", "--json",
	})

	err = root.Execute()
	out := done()
	if err != nil {
		t.Fatalf("account-only search failed: %v", err)
	}

	if !strings.Contains(out, "Alice msg") {
		t.Errorf("expected Alice's message in output, got: %s", out)
	}
	if strings.Contains(out, "Bob msg") {
		t.Errorf("Bob's message should be filtered out, got: %s", out)
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
