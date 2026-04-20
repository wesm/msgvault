package hybrid

import (
	"context"
	"database/sql"
	"sort"
	"testing"

	_ "github.com/mattn/go-sqlite3"

	"github.com/wesm/msgvault/internal/search"
)

func newFilterTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	schema := `
CREATE TABLE participants (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    email_address TEXT
);
CREATE TABLE labels (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT
);
`
	if _, err := db.Exec(schema); err != nil {
		t.Fatalf("schema: %v", err)
	}
	participants := []string{
		"alice@example.com",
		"bob@example.com",
		"carol@other.com",
		"dave.work@example.com",
	}
	for _, p := range participants {
		if _, err := db.Exec(`INSERT INTO participants (email_address) VALUES (?)`, p); err != nil {
			t.Fatalf("insert participant: %v", err)
		}
	}
	for _, l := range []string{"INBOX", "Work", "Archive"} {
		if _, err := db.Exec(`INSERT INTO labels (name) VALUES (?)`, l); err != nil {
			t.Fatalf("insert label: %v", err)
		}
	}
	return db
}

func sortedIDs(ids []int64) []int64 {
	out := append([]int64(nil), ids...)
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// TestBuildFilter_AddressesResolveViaSubstring confirms that
// from:/to:/cc:/bcc: use the same substring-LIKE semantic as the
// existing SQLite search path, so vector/hybrid and FTS agree on which
// participants match a token.
func TestBuildFilter_AddressesResolveViaSubstring(t *testing.T) {
	ctx := context.Background()
	db := newFilterTestDB(t)

	q := search.Parse(`from:example.com to:alice cc:other.com`)
	f, err := BuildFilter(ctx, db, q)
	if err != nil {
		t.Fatalf("BuildFilter: %v", err)
	}

	// from:example.com → alice, bob, dave.work (all @example.com)
	if got := len(f.SenderIDs); got != 3 {
		t.Errorf("len(SenderIDs) = %d, want 3 (all @example.com), got ids %v", got, sortedIDs(f.SenderIDs))
	}
	// to:alice → alice only
	if len(f.ToIDs) != 1 {
		t.Errorf("len(ToIDs) = %d, want 1, got %v", len(f.ToIDs), sortedIDs(f.ToIDs))
	}
	// cc:other.com → carol only
	if len(f.CcIDs) != 1 {
		t.Errorf("len(CcIDs) = %d, want 1, got %v", len(f.CcIDs), sortedIDs(f.CcIDs))
	}
}

// TestBuildFilter_SizeAndSubjectAndDate confirms that larger:/smaller:,
// subject:, and date bounds flow through to the Filter struct unchanged.
func TestBuildFilter_SizeAndSubjectAndDate(t *testing.T) {
	ctx := context.Background()
	db := newFilterTestDB(t)

	q := search.Parse(`larger:1M smaller:10M subject:quarterly subject:"offsite plan" after:2025-01-01 before:2025-06-01`)
	f, err := BuildFilter(ctx, db, q)
	if err != nil {
		t.Fatalf("BuildFilter: %v", err)
	}
	if f.LargerThan == nil || *f.LargerThan != 1024*1024 {
		t.Errorf("LargerThan = %v, want 1048576", f.LargerThan)
	}
	if f.SmallerThan == nil || *f.SmallerThan != 10*1024*1024 {
		t.Errorf("SmallerThan = %v, want 10485760", f.SmallerThan)
	}
	if len(f.SubjectSubstrings) != 2 ||
		f.SubjectSubstrings[0] != "quarterly" ||
		f.SubjectSubstrings[1] != "offsite plan" {
		t.Errorf("SubjectSubstrings = %v, want [quarterly offsite plan]", f.SubjectSubstrings)
	}
	if f.After == nil {
		t.Error("After = nil, want parsed")
	}
	if f.Before == nil {
		t.Error("Before = nil, want parsed")
	}
}

// TestBuildFilter_LabelsAndAttachments checks the label: and
// has:attachment operators.
func TestBuildFilter_LabelsAndAttachments(t *testing.T) {
	ctx := context.Background()
	db := newFilterTestDB(t)

	q := search.Parse(`label:Work has:attachment`)
	f, err := BuildFilter(ctx, db, q)
	if err != nil {
		t.Fatalf("BuildFilter: %v", err)
	}
	if len(f.LabelIDs) != 1 {
		t.Errorf("len(LabelIDs) = %d, want 1", len(f.LabelIDs))
	}
	if f.HasAttachment == nil || !*f.HasAttachment {
		t.Errorf("HasAttachment = %v, want true", f.HasAttachment)
	}
}

// TestBuildFilter_EmptyQueryYieldsEmptyFilter covers the
// "no operators" path.
func TestBuildFilter_EmptyQueryYieldsEmptyFilter(t *testing.T) {
	ctx := context.Background()
	db := newFilterTestDB(t)

	q := search.Parse(`lunch plans`)
	f, err := BuildFilter(ctx, db, q)
	if err != nil {
		t.Fatalf("BuildFilter: %v", err)
	}
	if !f.IsEmpty() {
		t.Errorf("filter not empty: %+v", f)
	}
}
