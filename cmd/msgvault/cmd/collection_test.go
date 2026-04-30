package cmd

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/wesm/msgvault/internal/config"
	"github.com/wesm/msgvault/internal/store"
)

func TestCollectionShowPrintsReadableSourceNames(t *testing.T) {
	savedCfg := cfg
	defer func() { cfg = savedCfg }()

	tmpDir := t.TempDir()
	cfg = &config.Config{
		HomeDir: tmpDir,
		Data:    config.DataConfig{DataDir: tmpDir},
	}

	dbPath := filepath.Join(tmpDir, "msgvault.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if err := st.InitSchema(); err != nil {
		t.Fatalf("init schema: %v", err)
	}

	alice, err := st.GetOrCreateSource("gmail", "alice@example.com")
	if err != nil {
		t.Fatalf("create alice source: %v", err)
	}
	if err := st.UpdateSourceDisplayName(alice.ID, "Personal"); err != nil {
		t.Fatalf("set display name: %v", err)
	}
	bob, err := st.GetOrCreateSource("imap", "bob@example.com")
	if err != nil {
		t.Fatalf("create bob source: %v", err)
	}
	if _, err := st.CreateCollection("team", "", []int64{alice.ID, bob.ID}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close setup store: %v", err)
	}

	done := captureStdout(t)
	if err := runCollectionShow(&cobra.Command{}, []string{"team"}); err != nil {
		t.Fatalf("runCollectionShow: %v", err)
	}
	out := done()

	if !strings.Contains(out, "Personal (id ") {
		t.Fatalf("missing display name in output:\n%s", out)
	}
	if !strings.Contains(out, "bob@example.com (id ") {
		t.Fatalf("missing identifier in output:\n%s", out)
	}
}

func TestResolveAccountListRejectsMissingNumericID(t *testing.T) {
	tmpDir := t.TempDir()
	st, err := store.Open(filepath.Join(tmpDir, "msgvault.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = st.Close() }()
	if err := st.InitSchema(); err != nil {
		t.Fatalf("init schema: %v", err)
	}

	src, err := st.GetOrCreateSource("gmail", "alice@example.com")
	if err != nil {
		t.Fatalf("create source: %v", err)
	}

	ids, err := resolveAccountList(st, fmt.Sprintf("%d", src.ID))
	if err != nil {
		t.Fatalf("resolveAccountList(existing id): %v", err)
	}
	if len(ids) != 1 || ids[0] != src.ID {
		t.Fatalf("resolveAccountList(existing id) = %v, want [%d]", ids, src.ID)
	}

	if _, err := resolveAccountList(st, "999999"); err == nil {
		t.Fatal("expected error for missing numeric source ID, got nil")
	}
}
