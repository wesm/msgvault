package cmd

import (
	"testing"

	"github.com/wesm/msgvault/internal/testutil"
)

// TestRepairOtherStrings_LogsScanErrors verifies that scan errors during
// repairOtherStrings are counted in stats.skippedRows rather than silently
// swallowed. We trigger scan errors by recreating the labels table with a
// TEXT id column and inserting a non-numeric id that can't be scanned into int64.
func TestRepairOtherStrings_LogsScanErrors(t *testing.T) {
	st := testutil.NewTestStore(t)
	db := st.DB()

	// Disable foreign keys so we can recreate the labels table
	if _, err := db.Exec("PRAGMA foreign_keys = OFF"); err != nil {
		t.Fatalf("disable foreign keys: %v", err)
	}

	// Drop labels and recreate with TEXT id (not INTEGER PRIMARY KEY).
	// Scanning a non-numeric TEXT value into int64 triggers a scan error.
	if _, err := db.Exec("DROP TABLE IF EXISTS labels"); err != nil {
		t.Fatalf("drop labels: %v", err)
	}
	if _, err := db.Exec(`CREATE TABLE labels (
		id TEXT, source_id INTEGER, source_label_id TEXT,
		name TEXT NOT NULL, label_type TEXT, color TEXT
	)`); err != nil {
		t.Fatalf("create labels: %v", err)
	}

	// Insert a row with non-numeric id → Scan into int64 will fail
	if _, err := db.Exec(`INSERT INTO labels (id, source_id, source_label_id, name, label_type)
		VALUES ('not-a-number', 1, 'lbl1', 'Test Label', 'user')`); err != nil {
		t.Fatalf("insert bad label: %v", err)
	}

	stats := &repairStats{}
	err := repairOtherStrings(st, stats)
	if err != nil {
		t.Fatalf("repairOtherStrings returned error: %v", err)
	}

	// Before fix: skippedRows == 0 (scan error silently swallowed)
	// After fix: skippedRows == 1 (scan error counted)
	if stats.skippedRows != 1 {
		t.Errorf("skippedRows = %d, want 1", stats.skippedRows)
	}
}

// TestRepairDisplayNames_LogsScanErrors verifies that scan errors during
// repairDisplayNames are counted in stats.skippedRows. We trigger scan errors
// by recreating the participants table with a TEXT id column.
func TestRepairDisplayNames_LogsScanErrors(t *testing.T) {
	st := testutil.NewTestStore(t)
	db := st.DB()

	if _, err := db.Exec("PRAGMA foreign_keys = OFF"); err != nil {
		t.Fatalf("disable foreign keys: %v", err)
	}

	// Drop participants and recreate with TEXT id
	if _, err := db.Exec("DROP TABLE IF EXISTS participants"); err != nil {
		t.Fatalf("drop participants: %v", err)
	}
	if _, err := db.Exec(`CREATE TABLE participants (
		id TEXT, email_address TEXT, phone_number TEXT,
		display_name TEXT, domain TEXT, canonical_id TEXT,
		created_at DATETIME, updated_at DATETIME
	)`); err != nil {
		t.Fatalf("create participants: %v", err)
	}

	// Insert a row with non-numeric id → Scan into int64 will fail
	if _, err := db.Exec(`INSERT INTO participants (id, email_address, display_name, domain)
		VALUES ('bad-id', 'test@example.com', 'Test User', 'example.com')`); err != nil {
		t.Fatalf("insert bad participant: %v", err)
	}

	stats := &repairStats{}
	err := repairDisplayNames(st, stats)
	if err != nil {
		t.Fatalf("repairDisplayNames returned error: %v", err)
	}

	// Before fix: skippedRows == 0 (scan error silently swallowed)
	// After fix: skippedRows == 1 (scan error counted)
	if stats.skippedRows != 1 {
		t.Errorf("skippedRows = %d, want 1", stats.skippedRows)
	}
}

// TestRepairEncoding_NoScanErrors verifies that normal data produces
// zero skipped rows.
func TestRepairEncoding_NoScanErrors(t *testing.T) {
	st := testutil.NewTestStore(t)

	stats := &repairStats{}

	if err := repairMessageFields(st, stats); err != nil {
		t.Fatalf("repairMessageFields: %v", err)
	}
	if err := repairDisplayNames(st, stats); err != nil {
		t.Fatalf("repairDisplayNames: %v", err)
	}
	if err := repairOtherStrings(st, stats); err != nil {
		t.Fatalf("repairOtherStrings: %v", err)
	}

	if stats.skippedRows != 0 {
		t.Errorf("skippedRows = %d, want 0 for valid data", stats.skippedRows)
	}
}
