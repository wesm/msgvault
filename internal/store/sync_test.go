package store_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/wesm/msgvault/internal/store"
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

// TestScanSource_UnrecognizedFormat verifies that parseDBTime returns an error
// with helpful context when encountering a truly unrecognized timestamp format.
func TestScanSource_UnrecognizedFormat(t *testing.T) {
	badTimestamp := "not-a-date-at-all"

	// Verify that parseDBTime rejects unrecognized formats
	_, err := store.ParseDBTime(badTimestamp)
	if err == nil {
		t.Fatal("expected error for unrecognized timestamp format, got nil")
	}

	// Error should include the bad value for debugging
	errStr := err.Error()
	if !strings.Contains(errStr, badTimestamp) {
		t.Errorf("error should include the bad value %q, got: %s", badTimestamp, errStr)
	}
}

// TestScanSource_NullRequiredTimestamp verifies that parseRequiredTime returns
// an error when a required timestamp field (created_at/updated_at) is NULL.
func TestScanSource_NullRequiredTimestamp(t *testing.T) {
	st := testutil.NewTestStore(t)

	// Create a source
	source, err := st.GetOrCreateSource("gmail", "nullrequired@example.com")
	testutil.MustNoErr(t, err, "GetOrCreateSource")

	// Corrupt created_at to NULL (violates expected schema invariant)
	_, err = st.DB().Exec(`UPDATE sources SET created_at = NULL WHERE id = ?`, source.ID)
	testutil.MustNoErr(t, err, "set created_at to NULL")

	// Attempting to retrieve should fail with a clear error
	_, err = st.GetSourceByIdentifier("nullrequired@example.com")
	if err == nil {
		t.Fatal("expected error for NULL required timestamp, got nil")
	}

	// Error should mention the field name and that it's NULL
	errStr := err.Error()
	if !strings.Contains(errStr, "created_at") || !strings.Contains(errStr, "NULL") {
		t.Errorf("error should mention field and NULL status, got: %s", errStr)
	}
}

// TestResetSourceData verifies that ResetSourceData clears all data for a source
// while preserving the source entry itself.
func TestResetSourceData(t *testing.T) {
	f := storetest.New(t)

	// Add some messages to the source
	f.CreateMessage("msg1")
	f.CreateMessage("msg2")
	f.CreateMessage("msg3")

	// Verify messages exist
	var count int
	err := f.Store.DB().QueryRow("SELECT COUNT(*) FROM messages WHERE source_id = ?", f.Source.ID).Scan(&count)
	testutil.MustNoErr(t, err, "count messages before reset")
	if count != 3 {
		t.Fatalf("expected 3 messages before reset, got %d", count)
	}

	// Set a sync cursor to verify it gets cleared
	err = f.Store.UpdateSourceSyncCursor(f.Source.ID, "test-cursor-12345")
	testutil.MustNoErr(t, err, "set sync cursor")

	// Reset the source data
	deleted, err := f.Store.ResetSourceData(f.Source.ID)
	testutil.MustNoErr(t, err, "ResetSourceData")

	if deleted != 3 {
		t.Errorf("expected 3 messages deleted, got %d", deleted)
	}

	// Verify messages are gone
	err = f.Store.DB().QueryRow("SELECT COUNT(*) FROM messages WHERE source_id = ?", f.Source.ID).Scan(&count)
	testutil.MustNoErr(t, err, "count messages after reset")
	if count != 0 {
		t.Errorf("expected 0 messages after reset, got %d", count)
	}

	// Verify source still exists but sync cursor is cleared
	source, err := f.Store.GetSourceByIdentifier(f.Source.Identifier)
	testutil.MustNoErr(t, err, "GetSourceByIdentifier after reset")
	if source == nil {
		t.Fatal("source should still exist after reset")
	}
	if source.SyncCursor.Valid {
		t.Errorf("sync cursor should be NULL after reset, got %q", source.SyncCursor.String)
	}
	if source.LastSyncAt.Valid {
		t.Error("last_sync_at should be NULL after reset")
	}
}

// TestResetSourceData_IsolatesAccounts verifies that resetting one account
// does not affect data belonging to other accounts.
func TestResetSourceData_IsolatesAccounts(t *testing.T) {
	st := testutil.NewTestStore(t)

	// Create two accounts
	account1, err := st.GetOrCreateSource("gmail", "account1@example.com")
	testutil.MustNoErr(t, err, "create account1")
	account2, err := st.GetOrCreateSource("gmail", "account2@example.com")
	testutil.MustNoErr(t, err, "create account2")

	// Create conversations for each account
	conv1, err := st.EnsureConversation(account1.ID, "thread-1", "Account 1 Thread")
	testutil.MustNoErr(t, err, "create conversation for account1")
	conv2, err := st.EnsureConversation(account2.ID, "thread-2", "Account 2 Thread")
	testutil.MustNoErr(t, err, "create conversation for account2")

	// Add messages to account 1
	for i := 0; i < 5; i++ {
		_, err := st.UpsertMessage(&store.Message{
			ConversationID:  conv1,
			SourceID:        account1.ID,
			SourceMessageID: fmt.Sprintf("acct1-msg-%d", i),
			MessageType:     "email",
			SizeEstimate:    1000,
		})
		testutil.MustNoErr(t, err, "insert message for account1")
	}

	// Add messages to account 2
	for i := 0; i < 3; i++ {
		_, err := st.UpsertMessage(&store.Message{
			ConversationID:  conv2,
			SourceID:        account2.ID,
			SourceMessageID: fmt.Sprintf("acct2-msg-%d", i),
			MessageType:     "email",
			SizeEstimate:    2000,
		})
		testutil.MustNoErr(t, err, "insert message for account2")
	}

	// Create labels for each account
	_, err = st.EnsureLabel(account1.ID, "INBOX", "Inbox", "system")
	testutil.MustNoErr(t, err, "create label for account1")
	_, err = st.EnsureLabel(account2.ID, "INBOX", "Inbox", "system")
	testutil.MustNoErr(t, err, "create label for account2")

	// Set sync cursors for both
	err = st.UpdateSourceSyncCursor(account1.ID, "cursor-account1")
	testutil.MustNoErr(t, err, "set cursor for account1")
	err = st.UpdateSourceSyncCursor(account2.ID, "cursor-account2")
	testutil.MustNoErr(t, err, "set cursor for account2")

	// Verify initial state
	var count1, count2 int
	err = st.DB().QueryRow("SELECT COUNT(*) FROM messages WHERE source_id = ?", account1.ID).Scan(&count1)
	testutil.MustNoErr(t, err, "count account1 messages before")
	err = st.DB().QueryRow("SELECT COUNT(*) FROM messages WHERE source_id = ?", account2.ID).Scan(&count2)
	testutil.MustNoErr(t, err, "count account2 messages before")

	if count1 != 5 {
		t.Fatalf("expected 5 messages for account1, got %d", count1)
	}
	if count2 != 3 {
		t.Fatalf("expected 3 messages for account2, got %d", count2)
	}

	// Reset account 1 ONLY
	deleted, err := st.ResetSourceData(account1.ID)
	testutil.MustNoErr(t, err, "ResetSourceData for account1")

	if deleted != 5 {
		t.Errorf("expected 5 messages deleted from account1, got %d", deleted)
	}

	// Verify account 1 is cleared
	err = st.DB().QueryRow("SELECT COUNT(*) FROM messages WHERE source_id = ?", account1.ID).Scan(&count1)
	testutil.MustNoErr(t, err, "count account1 messages after")
	if count1 != 0 {
		t.Errorf("expected 0 messages for account1 after reset, got %d", count1)
	}

	// Verify account 1 conversations are cleared
	var convCount1 int
	err = st.DB().QueryRow("SELECT COUNT(*) FROM conversations WHERE source_id = ?", account1.ID).Scan(&convCount1)
	testutil.MustNoErr(t, err, "count account1 conversations after")
	if convCount1 != 0 {
		t.Errorf("expected 0 conversations for account1 after reset, got %d", convCount1)
	}

	// Verify account 1 labels are cleared
	var labelCount1 int
	err = st.DB().QueryRow("SELECT COUNT(*) FROM labels WHERE source_id = ?", account1.ID).Scan(&labelCount1)
	testutil.MustNoErr(t, err, "count account1 labels after")
	if labelCount1 != 0 {
		t.Errorf("expected 0 labels for account1 after reset, got %d", labelCount1)
	}

	// Verify account 1 sync cursor is cleared
	src1, err := st.GetSourceByIdentifier("account1@example.com")
	testutil.MustNoErr(t, err, "get account1 after reset")
	if src1.SyncCursor.Valid {
		t.Errorf("account1 sync cursor should be NULL, got %q", src1.SyncCursor.String)
	}

	// ============================================================
	// CRITICAL: Verify account 2 is COMPLETELY UNTOUCHED
	// ============================================================

	// Account 2 messages should still exist
	err = st.DB().QueryRow("SELECT COUNT(*) FROM messages WHERE source_id = ?", account2.ID).Scan(&count2)
	testutil.MustNoErr(t, err, "count account2 messages after")
	if count2 != 3 {
		t.Errorf("ISOLATION FAILURE: expected 3 messages for account2, got %d", count2)
	}

	// Account 2 conversations should still exist
	var convCount2 int
	err = st.DB().QueryRow("SELECT COUNT(*) FROM conversations WHERE source_id = ?", account2.ID).Scan(&convCount2)
	testutil.MustNoErr(t, err, "count account2 conversations after")
	if convCount2 != 1 {
		t.Errorf("ISOLATION FAILURE: expected 1 conversation for account2, got %d", convCount2)
	}

	// Account 2 labels should still exist
	var labelCount2 int
	err = st.DB().QueryRow("SELECT COUNT(*) FROM labels WHERE source_id = ?", account2.ID).Scan(&labelCount2)
	testutil.MustNoErr(t, err, "count account2 labels after")
	if labelCount2 != 1 {
		t.Errorf("ISOLATION FAILURE: expected 1 label for account2, got %d", labelCount2)
	}

	// Account 2 sync cursor should still be set
	src2, err := st.GetSourceByIdentifier("account2@example.com")
	testutil.MustNoErr(t, err, "get account2 after reset")
	if !src2.SyncCursor.Valid || src2.SyncCursor.String != "cursor-account2" {
		t.Errorf("ISOLATION FAILURE: account2 sync cursor should be 'cursor-account2', got %v", src2.SyncCursor)
	}

	// Verify total message count in database
	var totalMessages int
	err = st.DB().QueryRow("SELECT COUNT(*) FROM messages").Scan(&totalMessages)
	testutil.MustNoErr(t, err, "count total messages")
	if totalMessages != 3 {
		t.Errorf("expected 3 total messages (all from account2), got %d", totalMessages)
	}
}
