package testutil

import (
	"path/filepath"
	"testing"

	"github.com/wesm/msgvault/internal/store"
)

// NewTestStore creates a temporary database for testing.
// The database is automatically cleaned up when the test completes.
func NewTestStore(t *testing.T) *store.Store {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "test.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}

	// Register close on cleanup
	t.Cleanup(func() {
		st.Close()
	})

	// Initialize schema
	if err := st.InitSchema(); err != nil {
		t.Fatalf("init schema: %v", err)
	}

	return st
}
