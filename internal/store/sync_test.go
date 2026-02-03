package store_test

import (
	"testing"
	"time"

	"github.com/wesm/msgvault/internal/testutil"
	"github.com/wesm/msgvault/internal/testutil/storetest"
)

// TestScanSource_NullLastSyncAt_Valid verifies that a new source with NULL
// last_sync_at is handled correctly (Valid=false).
func TestScanSource_NullLastSyncAt_Valid(t *testing.T) {
	st := testutil.NewTestStore(t)

	// Create a fresh source (should have NULL last_sync_at)
	source, err := st.GetOrCreateSource("gmail", "null-lastsync@example.com")
	testutil.MustNoErr(t, err, "GetOrCreateSource")

	// Retrieve it - should work fine with NULL last_sync_at
	retrieved, err := st.GetSourceByIdentifier("null-lastsync@example.com")
	testutil.MustNoErr(t, err, "GetSourceByIdentifier")

	if retrieved == nil {
		t.Fatal("expected source, got nil")
	}
	if retrieved.ID != source.ID {
		t.Errorf("ID = %d, want %d", retrieved.ID, source.ID)
	}
	if retrieved.LastSyncAt.Valid {
		t.Error("LastSyncAt should not be valid for a new source")
	}
}

// TestScanSyncRun_ZeroTime verifies that the scanner handles timestamps that
// the go-sqlite3 driver normalizes to zero time (from invalid input).
// The driver converts unparseable DATETIME values to "0001-01-01T00:00:00Z".
func TestScanSyncRun_ZeroTime(t *testing.T) {
	f := storetest.New(t)

	syncID := f.StartSync()

	// Corrupt the started_at with an invalid value.
	// go-sqlite3 normalizes this to "0001-01-01T00:00:00Z" for DATETIME columns.
	_, err := f.Store.DB().Exec(`
		UPDATE sync_runs SET started_at = 'invalid-timestamp' WHERE id = ?
	`, syncID)
	testutil.MustNoErr(t, err, "corrupt started_at")

	// GetActiveSync should still work - the driver normalizes to zero time
	run, err := f.Store.GetActiveSync(f.Source.ID)
	testutil.MustNoErr(t, err, "GetActiveSync")

	if run == nil {
		t.Fatal("expected sync run, got nil")
	}

	// The driver normalizes invalid timestamps to zero time
	if !run.StartedAt.IsZero() {
		t.Errorf("StartedAt = %v, expected zero time", run.StartedAt)
	}
}

// TestScanSource_ZeroTime verifies that sources with timestamps that the driver
// normalizes to zero time are handled correctly.
func TestScanSource_ZeroTime(t *testing.T) {
	st := testutil.NewTestStore(t)

	// Create a source
	source, err := st.GetOrCreateSource("gmail", "zerotime@example.com")
	testutil.MustNoErr(t, err, "GetOrCreateSource")

	// Corrupt the created_at with an invalid value.
	// go-sqlite3 normalizes this to "0001-01-01T00:00:00Z" for DATETIME columns.
	_, err = st.DB().Exec(`
		UPDATE sources SET created_at = 'garbage' WHERE id = ?
	`, source.ID)
	testutil.MustNoErr(t, err, "corrupt created_at")

	// Should still work - the driver normalizes to zero time
	retrieved, err := st.GetSourceByIdentifier("zerotime@example.com")
	testutil.MustNoErr(t, err, "GetSourceByIdentifier")

	if retrieved == nil {
		t.Fatal("expected source, got nil")
	}

	// The driver normalizes invalid timestamps to zero time
	if !retrieved.CreatedAt.IsZero() {
		t.Errorf("CreatedAt = %v, expected zero time", retrieved.CreatedAt)
	}
}

// TestParseDBTime_MultipleFormats verifies that the timestamp parser accepts
// both SQLite datetime('now') format and RFC3339 format from go-sqlite3.
func TestParseDBTime_MultipleFormats(t *testing.T) {
	f := storetest.New(t)

	// Start a sync (uses datetime('now') which go-sqlite3 normalizes to RFC3339)
	syncID := f.StartSync()

	// GetActiveSync should parse the RFC3339 timestamp successfully
	run, err := f.Store.GetActiveSync(f.Source.ID)
	testutil.MustNoErr(t, err, "GetActiveSync")

	if run == nil {
		t.Fatal("expected sync run, got nil")
	}
	if run.ID != syncID {
		t.Errorf("ID = %d, want %d", run.ID, syncID)
	}

	// StartedAt should be recent (within last minute)
	age := time.Since(run.StartedAt)
	if age < 0 || age > time.Minute {
		t.Errorf("StartedAt age = %v, expected recent time", age)
	}
}

// TestListSources_ParsesTimestamps verifies that ListSources correctly parses
// timestamps for all returned sources.
func TestListSources_ParsesTimestamps(t *testing.T) {
	st := testutil.NewTestStore(t)

	// Create a few sources
	emails := []string{"user1@example.com", "user2@example.com", "user3@example.com"}
	for _, email := range emails {
		_, err := st.GetOrCreateSource("gmail", email)
		testutil.MustNoErr(t, err, "GetOrCreateSource")
	}

	// ListSources should parse timestamps correctly
	sources, err := st.ListSources("gmail")
	testutil.MustNoErr(t, err, "ListSources")

	if len(sources) != 3 {
		t.Fatalf("len(sources) = %d, want 3", len(sources))
	}

	for _, src := range sources {
		// CreatedAt should be recent
		age := time.Since(src.CreatedAt)
		if age < 0 || age > time.Minute {
			t.Errorf("source %d: CreatedAt age = %v, expected recent time", src.ID, age)
		}
	}
}
