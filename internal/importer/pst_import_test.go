package importer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"mime/multipart"
	"net/textproto"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/wesm/msgvault/internal/store"
)

// mockIngestFunc records IngestRawMessage calls for inspection in tests.
type mockIngestFunc struct {
	calls []mockIngestCall
	err   error
}

type mockIngestCall struct {
	SourceID     int64
	Identifier   string
	SourceMsgID  string
	RawHash      string
	LabelIDs     []int64
	FallbackDate time.Time
	RawLen       int
}

func (m *mockIngestFunc) fn(
	ctx context.Context, st *store.Store,
	sourceID int64, identifier, attachmentsDir string,
	labelIDs []int64, sourceMsgID, rawHash string,
	raw []byte, fallbackDate time.Time,
	log *slog.Logger,
) error {
	m.calls = append(m.calls, mockIngestCall{
		SourceID:     sourceID,
		Identifier:   identifier,
		SourceMsgID:  sourceMsgID,
		RawHash:      rawHash,
		LabelIDs:     append([]int64(nil), labelIDs...),
		FallbackDate: fallbackDate,
		RawLen:       len(raw),
	})
	return m.err
}

func openTestStorePst(t *testing.T) *store.Store {
	t.Helper()
	tmp := t.TempDir()
	dbPath := filepath.Join(tmp, "msgvault.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	if err := st.InitSchema(); err != nil {
		t.Fatalf("init schema: %v", err)
	}
	return st
}

// TestImportPst_MissingFile verifies that ImportPst returns an error for a
// non-existent PST file without panicking or corrupting the database.
func TestImportPst_MissingFile(t *testing.T) {
	st := openTestStorePst(t)
	mock := &mockIngestFunc{}

	_, err := ImportPst(context.Background(), st, "/nonexistent/path.pst", PstImportOptions{
		Identifier: "user@example.com",
		NoResume:   true,
		IngestFunc: mock.fn,
	})
	if err == nil {
		t.Fatal("expected error for non-existent PST file, got nil")
	}
	if len(mock.calls) != 0 {
		t.Errorf("expected 0 ingest calls, got %d", len(mock.calls))
	}
}

// TestImportPst_RequiresIdentifier verifies that ImportPst rejects an empty identifier.
func TestImportPst_RequiresIdentifier(t *testing.T) {
	st := openTestStorePst(t)
	_, err := ImportPst(context.Background(), st, "any.pst", PstImportOptions{
		Identifier: "",
	})
	if err == nil {
		t.Fatal("expected error for empty identifier")
	}
}

// TestPstCheckpoint_RoundTrip verifies that savePstCheckpoint stores a checkpoint
// that can be decoded back to the original values.
func TestPstCheckpoint_RoundTrip(t *testing.T) {
	st := openTestStorePst(t)
	src, err := st.GetOrCreateSource("pst", "user@example.com")
	if err != nil {
		t.Fatalf("get/create source: %v", err)
	}

	syncID, err := st.StartSync(src.ID, "import-pst")
	if err != nil {
		t.Fatalf("start sync: %v", err)
	}

	cp := &store.Checkpoint{
		MessagesProcessed: 42,
		MessagesAdded:     40,
	}
	if err := savePstCheckpoint(st, syncID, "/path/to/file.pst", 3, "Inbox/Archive", 100, cp); err != nil {
		t.Fatalf("savePstCheckpoint: %v", err)
	}

	active, err := st.GetActiveSync(src.ID)
	if err != nil {
		t.Fatalf("get active sync: %v", err)
	}
	if active == nil {
		t.Fatal("expected active sync, got nil")
	}
	if !active.CursorBefore.Valid {
		t.Fatal("expected cursor_before to be set")
	}

	var saved pstCheckpoint
	if err := json.Unmarshal([]byte(active.CursorBefore.String), &saved); err != nil {
		t.Fatalf("unmarshal checkpoint: %v", err)
	}

	if saved.File != "/path/to/file.pst" {
		t.Errorf("File = %q, want %q", saved.File, "/path/to/file.pst")
	}
	if saved.FolderIndex != 3 {
		t.Errorf("FolderIndex = %d, want 3", saved.FolderIndex)
	}
	if saved.FolderPath != "Inbox/Archive" {
		t.Errorf("FolderPath = %q, want %q", saved.FolderPath, "Inbox/Archive")
	}
	if saved.MsgIndex != 100 {
		t.Errorf("MsgIndex = %d, want 100", saved.MsgIndex)
	}
}

// TestPstArchiveFingerprint verifies the helper produces stable, distinct
// identifiers per file. Without this, importing two PST archives with the
// same source identifier would collide on PST EntryIDs (which are unique
// only within a single archive) and falsely skip or update unrelated rows.
func TestPstArchiveFingerprint(t *testing.T) {
	dir := t.TempDir()

	// Two files with different headers — fingerprints must differ.
	headerA := append([]byte("!BDN\x00\x00\x00\x00"), make([]byte, 4096)...)
	headerB := append([]byte("!BDN\xff\xff\xff\xff"), make([]byte, 4096)...)
	pathA := filepath.Join(dir, "a.pst")
	pathB := filepath.Join(dir, "b.pst")
	if err := os.WriteFile(pathA, headerA, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(pathB, headerB, 0o644); err != nil {
		t.Fatal(err)
	}

	fpA, err := pstArchiveFingerprint(pathA)
	if err != nil {
		t.Fatalf("fingerprint A: %v", err)
	}
	fpB, err := pstArchiveFingerprint(pathB)
	if err != nil {
		t.Fatalf("fingerprint B: %v", err)
	}

	if fpA == fpB {
		t.Errorf("expected distinct fingerprints, got %q twice", fpA)
	}
	if len(fpA) != 12 || len(fpB) != 12 {
		t.Errorf("expected 12-hex-char fingerprints, got %q (%d) and %q (%d)",
			fpA, len(fpA), fpB, len(fpB))
	}

	// Same bytes → same fingerprint, regardless of path. This is what
	// makes re-importing the same file idempotent under the new key.
	pathC := filepath.Join(dir, "renamed.pst")
	if err := os.WriteFile(pathC, headerA, 0o644); err != nil {
		t.Fatal(err)
	}
	fpC, err := pstArchiveFingerprint(pathC)
	if err != nil {
		t.Fatalf("fingerprint C: %v", err)
	}
	if fpC != fpA {
		t.Errorf("same bytes should fingerprint the same: %q vs %q", fpA, fpC)
	}
}

// buildAlternativeMIME constructs a multipart/alternative MIME document with
// the supplied boundary, text body, and HTML body. Used by migration tests
// to exercise the case where two raw blobs encode the same content under
// different boundaries — the situation that broke the prior byte-hash check
// and motivated parse-based comparison.
func buildAlternativeMIME(t *testing.T, subject, boundary, text, html string) []byte {
	t.Helper()
	var buf bytes.Buffer
	fmt.Fprintf(&buf,
		"Subject: %s\r\nMIME-Version: 1.0\r\nContent-Type: multipart/alternative; boundary=%q\r\n\r\n",
		subject, boundary,
	)
	mw := multipart.NewWriter(&buf)
	if err := mw.SetBoundary(boundary); err != nil {
		t.Fatalf("set boundary: %v", err)
	}
	th := make(textproto.MIMEHeader)
	th.Set("Content-Type", "text/plain; charset=utf-8")
	pw, err := mw.CreatePart(th)
	if err != nil {
		t.Fatalf("create text part: %v", err)
	}
	if _, err := pw.Write([]byte(text)); err != nil {
		t.Fatalf("write text part: %v", err)
	}
	hh := make(textproto.MIMEHeader)
	hh.Set("Content-Type", "text/html; charset=utf-8")
	pw, err = mw.CreatePart(hh)
	if err != nil {
		t.Fatalf("create html part: %v", err)
	}
	if _, err := pw.Write([]byte(html)); err != nil {
		t.Fatalf("write html part: %v", err)
	}
	if err := mw.Close(); err != nil {
		t.Fatalf("close multipart: %v", err)
	}
	return buf.Bytes()
}

// TestMigratePstKey verifies the dedup migration's parse-based equivalence
// check: rows are migrated when the stored and incoming MIME parse to the
// same Subject/BodyText/BodyHTML even if the raw bytes differ (e.g. random
// vs deterministic multipart boundaries), and refused when the parsed
// content differs.
func TestMigratePstKey(t *testing.T) {
	st := openTestStorePst(t)

	src, err := st.GetOrCreateSource("pst", "user@example.com")
	if err != nil {
		t.Fatalf("get/create source: %v", err)
	}
	convID, err := st.EnsureConversation(src.ID, "thread-1", "Test")
	if err != nil {
		t.Fatalf("ensure conversation: %v", err)
	}

	const (
		legacyID = "pst-12345"
		newID    = "pst-abc123def456-12345"
		subject  = "Same content under different boundaries"
		bodyText = "hello world\r\n"
		bodyHTML = "<p>hello world</p>\r\n"
	)

	// Stored: legacy-style raw with one boundary (mimicking the random
	// boundaries the pre-fingerprint builder produced).
	storedRaw := buildAlternativeMIME(t, subject, "BOUNDARY-LEGACY-aaaa", bodyText, bodyHTML)
	msgID, err := st.PersistMessage(&store.MessagePersistData{
		Message: &store.Message{
			ConversationID:  convID,
			SourceID:        src.ID,
			SourceMessageID: legacyID,
			MessageType:     "email",
		},
		RawMIME: storedRaw,
	})
	if err != nil {
		t.Fatalf("persist message: %v", err)
	}

	// Different content → must not migrate.
	mismatchRaw := buildAlternativeMIME(t, subject, "BOUNDARY-X-1111", "different body", bodyHTML)
	if migratePstKey(st, msgID, mismatchRaw, newID, slog.Default()) {
		t.Fatal("expected migration to fail when body content differs")
	}
	existing, err := st.MessageExistsBatch(src.ID, []string{legacyID})
	if err != nil {
		t.Fatalf("exists check (legacy): %v", err)
	}
	if existing[legacyID] != msgID {
		t.Errorf("legacy key must remain on content mismatch; got %v", existing)
	}

	// Same body/subject under a different boundary → must migrate.
	matchRaw := buildAlternativeMIME(t, subject, "BOUNDARY-CURRENT-bbbb", bodyText, bodyHTML)
	if !migratePstKey(st, msgID, matchRaw, newID, slog.Default()) {
		t.Fatal("expected migration on matching body across different boundaries")
	}
	existing, err = st.MessageExistsBatch(src.ID, []string{newID})
	if err != nil {
		t.Fatalf("exists check (new): %v", err)
	}
	if existing[newID] != msgID {
		t.Errorf("new key should be present after migration; got %v", existing)
	}
	existing, err = st.MessageExistsBatch(src.ID, []string{legacyID})
	if err != nil {
		t.Fatalf("exists check (legacy after): %v", err)
	}
	if len(existing) != 0 {
		t.Errorf("legacy key should be gone after migration; got %v", existing)
	}
}

// TestEntryIDFromPstSourceMessageID covers the parser that buckets existing
// rows by EntryID, including both legacy "pst-<EntryID>" rows and current
// "pst-<fingerprint>-<EntryID>" rows.
func TestEntryIDFromPstSourceMessageID(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"pst-12345", "12345"},
		{"pst-abc123def456-12345", "12345"},
		{"pst-fingerprint-7", "7"},
		{"pst-", ""},
		{"", ""},
		{"gmail-12345", ""},
	}
	for _, tc := range cases {
		if got := entryIDFromPstSourceMessageID(tc.in); got != tc.want {
			t.Errorf("entryIDFromPstSourceMessageID(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestImportPst_ContextCancelledBeforeOpen ensures that context cancellation
// before the PST file is opened is handled gracefully.
func TestImportPst_ContextCancelledBeforeOpen(t *testing.T) {
	st := openTestStorePst(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	// Use a non-existent path so Open fails fast.
	_, err := ImportPst(ctx, st, "/nonexistent.pst", PstImportOptions{
		Identifier: "user@example.com",
		NoResume:   true,
	})
	// Either ctx error or open error is acceptable — we just must not hang.
	if err == nil {
		t.Error("expected error (either ctx or open), got nil")
	}
}
