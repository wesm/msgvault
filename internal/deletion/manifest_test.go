package deletion

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// testManager creates a Manager in a temp directory for testing.
func testManager(t *testing.T) *Manager {
	t.Helper()
	mgr, err := NewManager(filepath.Join(t.TempDir(), "deletions"))
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	return mgr
}

// createTestManifest creates a manifest via the manager with default IDs.
func createTestManifest(t *testing.T, mgr *Manager, desc string) *Manifest {
	t.Helper()
	m, err := mgr.CreateManifest(desc, []string{"a", "b"}, Filters{})
	if err != nil {
		t.Fatalf("CreateManifest(%q) error = %v", desc, err)
	}
	return m
}

func TestSanitizeForFilename(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"simple", "simple"},
		{"with spaces", "with-spaces"},
		{"with.dots", "with-dots"},
		{"with-dash_underscore", "with-dash_underscore"},
		{"Special@#$Characters!", "SpecialCharacters"},
		{"MixedCase123", "MixedCase123"},
		{"", ""},
		{"a b c d e", "a-b-c-d-e"},
	}

	for _, tc := range tests {
		got := sanitizeForFilename(tc.input)
		if got != tc.want {
			t.Errorf("sanitizeForFilename(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

func TestGenerateID(t *testing.T) {
	// Test basic ID generation
	id := generateID("test batch")
	if id == "" {
		t.Error("generateID returned empty string")
	}

	// Should contain timestamp prefix and sanitized description
	parts := strings.SplitN(id, "-", 3)
	if len(parts) < 2 {
		t.Errorf("generateID(%q) = %q, expected timestamp-description format", "test batch", id)
	}

	// Long description should be truncated
	longDesc := "this is a very long description that exceeds twenty characters"
	id = generateID(longDesc)
	// ID format: YYYYMMDD-HHMMSS-description (max 20 chars for description)
	if len(id) > 40 { // timestamp (15) + 2 hyphens + 20 chars
		t.Errorf("generateID with long description too long: %q", id)
	}

	// Empty description should use "batch"
	id = generateID("")
	if !strings.HasSuffix(id, "-batch") {
		t.Errorf("generateID(\"\") = %q, expected to end with -batch", id)
	}
}

func TestNewManifest(t *testing.T) {
	gmailIDs := []string{"msg1", "msg2", "msg3"}
	m := NewManifest("test deletion", gmailIDs)

	if m.Version != 1 {
		t.Errorf("Version = %d, want 1", m.Version)
	}
	if m.ID == "" {
		t.Error("ID is empty")
	}
	if m.Description != "test deletion" {
		t.Errorf("Description = %q, want %q", m.Description, "test deletion")
	}
	if len(m.GmailIDs) != 3 {
		t.Errorf("GmailIDs length = %d, want 3", len(m.GmailIDs))
	}
	if m.Status != StatusPending {
		t.Errorf("Status = %q, want %q", m.Status, StatusPending)
	}
	if m.CreatedBy != "cli" {
		t.Errorf("CreatedBy = %q, want %q", m.CreatedBy, "cli")
	}
	if m.CreatedAt.IsZero() {
		t.Error("CreatedAt is zero")
	}
}

func TestManifest_SaveAndLoad(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test-manifest.json")

	// Create and save manifest
	m := NewManifest("save test", []string{"id1", "id2"})
	m.Filters = Filters{
		Sender: "test@example.com",
		After:  "2024-01-01",
	}
	m.Summary = &Summary{
		MessageCount:   2,
		TotalSizeBytes: 1024,
		DateRange:      [2]string{"2024-01-01", "2024-01-15"},
		TopSenders: []SenderCount{
			{Sender: "test@example.com", Count: 2},
		},
	}

	if err := m.Save(path); err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	// Load and verify
	loaded, err := LoadManifest(path)
	if err != nil {
		t.Fatalf("LoadManifest() error = %v", err)
	}

	if loaded.Description != m.Description {
		t.Errorf("Description = %q, want %q", loaded.Description, m.Description)
	}
	if len(loaded.GmailIDs) != len(m.GmailIDs) {
		t.Errorf("GmailIDs length = %d, want %d", len(loaded.GmailIDs), len(m.GmailIDs))
	}
	if loaded.Filters.Sender != m.Filters.Sender {
		t.Errorf("Filters.Sender = %q, want %q", loaded.Filters.Sender, m.Filters.Sender)
	}
	if loaded.Summary == nil {
		t.Fatal("Summary is nil after load")
	}
	if loaded.Summary.MessageCount != 2 {
		t.Errorf("Summary.MessageCount = %d, want 2", loaded.Summary.MessageCount)
	}
}

func TestManifest_Save_CreatesDirectories(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "subdir", "nested", "manifest.json")

	m := NewManifest("nested test", []string{"id1"})
	if err := m.Save(path); err != nil {
		t.Fatalf("Save() with nested path error = %v", err)
	}

	// Verify file exists
	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Error("File was not created")
	}
}

func TestLoadManifest_NotFound(t *testing.T) {
	_, err := LoadManifest("/nonexistent/path/manifest.json")
	if err == nil {
		t.Error("LoadManifest() should error for nonexistent file")
	}
}

func TestLoadManifest_InvalidJSON(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "invalid.json")
	if err := os.WriteFile(path, []byte("not valid json"), 0644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	_, err := LoadManifest(path)
	if err == nil {
		t.Error("LoadManifest() should error for invalid JSON")
	}
}

func TestManifest_FormatSummary(t *testing.T) {
	m := NewManifest("format test", []string{"id1", "id2", "id3"})
	m.Summary = &Summary{
		MessageCount:   3,
		TotalSizeBytes: 5 * 1024 * 1024, // 5 MB
		DateRange:      [2]string{"2024-01-01", "2024-01-31"},
		TopSenders: []SenderCount{
			{Sender: "alice@example.com", Count: 2},
			{Sender: "bob@example.com", Count: 1},
		},
	}

	summary := m.FormatSummary()

	// Check that key fields are present
	if !strings.Contains(summary, m.ID) {
		t.Error("FormatSummary() missing ID")
	}
	if !strings.Contains(summary, "pending") {
		t.Error("FormatSummary() missing status")
	}
	if !strings.Contains(summary, "Messages: 3") {
		t.Error("FormatSummary() missing message count")
	}
	if !strings.Contains(summary, "5.00 MB") {
		t.Error("FormatSummary() missing size")
	}
	if !strings.Contains(summary, "2024-01-01") {
		t.Error("FormatSummary() missing date range")
	}
	if !strings.Contains(summary, "alice@example.com") {
		t.Error("FormatSummary() missing top sender")
	}
}

func TestManifest_FormatSummary_WithExecution(t *testing.T) {
	m := NewManifest("exec test", []string{"id1"})
	now := time.Now()
	m.Execution = &Execution{
		StartedAt:   now.Add(-time.Hour),
		CompletedAt: &now,
		Method:      MethodTrash,
		Succeeded:   10,
		Failed:      2,
	}

	summary := m.FormatSummary()

	if !strings.Contains(summary, "Execution:") {
		t.Error("FormatSummary() missing Execution section")
	}
	if !strings.Contains(summary, "Method: trash") {
		t.Error("FormatSummary() missing method")
	}
	if !strings.Contains(summary, "Succeeded: 10") {
		t.Error("FormatSummary() missing succeeded count")
	}
	if !strings.Contains(summary, "Failed: 2") {
		t.Error("FormatSummary() missing failed count")
	}
	if !strings.Contains(summary, "Completed:") {
		t.Error("FormatSummary() missing completed timestamp")
	}
}

func TestNewManager(t *testing.T) {
	tmpDir := t.TempDir()
	baseDir := filepath.Join(tmpDir, "deletions")

	mgr, err := NewManager(baseDir)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	// Verify all directories were created
	expectedDirs := []string{"pending", "in_progress", "completed", "failed"}
	for _, d := range expectedDirs {
		path := filepath.Join(baseDir, d)
		if info, err := os.Stat(path); err != nil || !info.IsDir() {
			t.Errorf("Directory %s was not created", d)
		}
	}

	// Verify directory getters
	if mgr.PendingDir() != filepath.Join(baseDir, "pending") {
		t.Errorf("PendingDir() = %q", mgr.PendingDir())
	}
	if mgr.InProgressDir() != filepath.Join(baseDir, "in_progress") {
		t.Errorf("InProgressDir() = %q", mgr.InProgressDir())
	}
	if mgr.CompletedDir() != filepath.Join(baseDir, "completed") {
		t.Errorf("CompletedDir() = %q", mgr.CompletedDir())
	}
	if mgr.FailedDir() != filepath.Join(baseDir, "failed") {
		t.Errorf("FailedDir() = %q", mgr.FailedDir())
	}
}

func TestManager_CreateAndListManifests(t *testing.T) {
	mgr := testManager(t)

	// Create manifests
	m1, err := mgr.CreateManifest("first batch", []string{"a", "b"}, Filters{Sender: "alice@example.com"})
	if err != nil {
		t.Fatalf("CreateManifest() error = %v", err)
	}

	m2 := createTestManifest(t, mgr, "second batch")

	// List pending should return both
	pending, err := mgr.ListPending()
	if err != nil {
		t.Fatalf("ListPending() error = %v", err)
	}
	if len(pending) != 2 {
		t.Fatalf("ListPending() returned %d manifests, want 2", len(pending))
	}

	// Verify both manifests are present
	ids := make(map[string]bool)
	for _, m := range pending {
		ids[m.ID] = true
	}
	if !ids[m1.ID] {
		t.Errorf("ListPending() missing manifest %q", m1.ID)
	}
	if !ids[m2.ID] {
		t.Errorf("ListPending() missing manifest %q", m2.ID)
	}

	// Verify ordering: list should be sorted by CreatedAt (newest first)
	for i := 1; i < len(pending); i++ {
		if pending[i].CreatedAt.After(pending[i-1].CreatedAt) {
			t.Errorf("ListPending() not sorted newest-first: %s (%v) should come before %s (%v)",
				pending[i].ID, pending[i].CreatedAt,
				pending[i-1].ID, pending[i-1].CreatedAt)
		}
	}
}

func TestManager_GetManifest(t *testing.T) {
	mgr := testManager(t)

	// Create a manifest
	m := createTestManifest(t, mgr, "get test")

	// Get it back
	loaded, path, err := mgr.GetManifest(m.ID)
	if err != nil {
		t.Fatalf("GetManifest() error = %v", err)
	}
	if loaded.Description != m.Description {
		t.Errorf("GetManifest() returned Description = %q, want %q", loaded.Description, m.Description)
	}
	if path == "" {
		t.Error("GetManifest() returned empty path")
	}
}

func TestManager_GetManifest_NotFound(t *testing.T) {
	mgr := testManager(t)

	_, _, err := mgr.GetManifest("nonexistent-id")
	if err == nil {
		t.Error("GetManifest() should error for nonexistent manifest")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("GetManifest() error = %v, want 'not found' error", err)
	}
}

func TestManager_MoveManifest(t *testing.T) {
	mgr := testManager(t)

	// Create a pending manifest
	m := createTestManifest(t, mgr, "move test")

	// Move pending -> in_progress
	if err := mgr.MoveManifest(m.ID, StatusPending, StatusInProgress); err != nil {
		t.Fatalf("MoveManifest(pending->in_progress) error = %v", err)
	}

	// Verify it's in in_progress
	inProgress, err := mgr.ListInProgress()
	if err != nil {
		t.Fatalf("ListInProgress() error = %v", err)
	}
	if len(inProgress) != 1 {
		t.Errorf("ListInProgress() returned %d, want 1", len(inProgress))
	}

	// Verify it's not in pending
	pending, err := mgr.ListPending()
	if err != nil {
		t.Fatalf("ListPending() error = %v", err)
	}
	if len(pending) != 0 {
		t.Errorf("ListPending() returned %d, want 0", len(pending))
	}

	// Move in_progress -> completed
	if err := mgr.MoveManifest(m.ID, StatusInProgress, StatusCompleted); err != nil {
		t.Fatalf("MoveManifest(in_progress->completed) error = %v", err)
	}

	completed, err := mgr.ListCompleted()
	if err != nil {
		t.Fatalf("ListCompleted() error = %v", err)
	}
	if len(completed) != 1 {
		t.Errorf("ListCompleted() returned %d, want 1", len(completed))
	}
}

func TestManager_MoveManifest_ToFailed(t *testing.T) {
	mgr := testManager(t)

	m := createTestManifest(t, mgr, "fail test")

	// Move pending -> in_progress -> failed
	if err := mgr.MoveManifest(m.ID, StatusPending, StatusInProgress); err != nil {
		t.Fatalf("MoveManifest(pending->in_progress) error = %v", err)
	}
	if err := mgr.MoveManifest(m.ID, StatusInProgress, StatusFailed); err != nil {
		t.Fatalf("MoveManifest(in_progress->failed) error = %v", err)
	}

	failed, err := mgr.ListFailed()
	if err != nil {
		t.Fatalf("ListFailed() error = %v", err)
	}
	if len(failed) != 1 {
		t.Errorf("ListFailed() returned %d, want 1", len(failed))
	}
}

func TestManager_MoveManifest_InvalidTransitions(t *testing.T) {
	mgr := testManager(t)

	m := createTestManifest(t, mgr, "invalid test")

	// Cannot move from completed
	if err := mgr.MoveManifest(m.ID, StatusCompleted, StatusPending); err == nil {
		t.Error("MoveManifest(completed->pending) should error")
	}

	// Cannot move to pending
	if err := mgr.MoveManifest(m.ID, StatusPending, StatusPending); err == nil {
		t.Error("MoveManifest(pending->pending) should error")
	}
}

func TestManager_CancelManifest(t *testing.T) {
	mgr := testManager(t)

	m := createTestManifest(t, mgr, "cancel test")

	// Cancel it
	if err := mgr.CancelManifest(m.ID); err != nil {
		t.Fatalf("CancelManifest() error = %v", err)
	}

	// Should be gone
	pending, err := mgr.ListPending()
	if err != nil {
		t.Fatalf("ListPending() error = %v", err)
	}
	if len(pending) != 0 {
		t.Errorf("ListPending() after cancel returned %d, want 0", len(pending))
	}
}

func TestManager_SaveManifest(t *testing.T) {
	mgr := testManager(t)

	// Create manifest with each status and verify placement
	statuses := []Status{StatusPending, StatusInProgress, StatusCompleted, StatusFailed}
	for _, status := range statuses {
		m := NewManifest("status-"+string(status), []string{"a"})
		m.Status = status

		if err := mgr.SaveManifest(m); err != nil {
			t.Fatalf("SaveManifest(%s) error = %v", status, err)
		}

		// Verify it can be found
		loaded, _, err := mgr.GetManifest(m.ID)
		if err != nil {
			t.Fatalf("GetManifest(%s) after SaveManifest error = %v", status, err)
		}
		if loaded.Status != status {
			t.Errorf("Status = %q, want %q", loaded.Status, status)
		}
	}
}

func TestManager_ListManifests_SkipsInvalidFiles(t *testing.T) {
	mgr := testManager(t)

	// Create a valid manifest
	createTestManifest(t, mgr, "valid")

	// Add an invalid JSON file
	invalidPath := filepath.Join(mgr.PendingDir(), "invalid.json")
	if err := os.WriteFile(invalidPath, []byte("not json"), 0644); err != nil {
		t.Fatalf("WriteFile(invalid.json) error = %v", err)
	}

	// Add a non-JSON file
	textPath := filepath.Join(mgr.PendingDir(), "readme.txt")
	if err := os.WriteFile(textPath, []byte("readme"), 0644); err != nil {
		t.Fatalf("WriteFile(readme.txt) error = %v", err)
	}

	// Add a directory
	dirPath := filepath.Join(mgr.PendingDir(), "subdir.json")
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		t.Fatalf("MkdirAll(subdir.json) error = %v", err)
	}

	// ListPending should only return the valid manifest
	pending, err := mgr.ListPending()
	if err != nil {
		t.Fatalf("ListPending() error = %v", err)
	}
	if len(pending) != 1 {
		t.Errorf("ListPending() returned %d, want 1 (should skip invalid files)", len(pending))
	}
}

func TestStatus_Values(t *testing.T) {
	// Verify status constants
	if StatusPending != "pending" {
		t.Errorf("StatusPending = %q", StatusPending)
	}
	if StatusInProgress != "in_progress" {
		t.Errorf("StatusInProgress = %q", StatusInProgress)
	}
	if StatusCompleted != "completed" {
		t.Errorf("StatusCompleted = %q", StatusCompleted)
	}
	if StatusFailed != "failed" {
		t.Errorf("StatusFailed = %q", StatusFailed)
	}
	if StatusCancelled != "cancelled" {
		t.Errorf("StatusCancelled = %q", StatusCancelled)
	}
}

func TestMethod_Values(t *testing.T) {
	if MethodTrash != "trash" {
		t.Errorf("MethodTrash = %q", MethodTrash)
	}
	if MethodDelete != "delete" {
		t.Errorf("MethodDelete = %q", MethodDelete)
	}
}

// TestManifest_FormatSummary_EmptyDateRange tests FormatSummary with empty date range.
func TestManifest_FormatSummary_EmptyDateRange(t *testing.T) {
	m := NewManifest("empty date test", []string{"id1"})
	m.Summary = &Summary{
		MessageCount:   1,
		TotalSizeBytes: 1024,
		DateRange:      [2]string{"", ""}, // Empty date range
	}

	summary := m.FormatSummary()

	// Should NOT contain "Date Range:" since dates are empty
	if strings.Contains(summary, "Date Range:") {
		t.Error("FormatSummary() should not include Date Range when dates are empty")
	}
}

// TestManifest_FormatSummary_NoSummary tests FormatSummary with nil Summary.
func TestManifest_FormatSummary_NoSummary(t *testing.T) {
	m := NewManifest("no summary test", []string{"id1", "id2"})
	m.Summary = nil

	summary := m.FormatSummary()

	// Should contain basic info but no summary details
	if !strings.Contains(summary, "Messages: 2") {
		t.Error("FormatSummary() should include message count")
	}
	if strings.Contains(summary, "Total Size:") {
		t.Error("FormatSummary() should not include Total Size when Summary is nil")
	}
}

// TestManifest_FormatSummary_ManyTopSenders tests FormatSummary with >10 top senders.
func TestManifest_FormatSummary_ManyTopSenders(t *testing.T) {
	m := NewManifest("many senders test", []string{"id1"})

	// Create 15 top senders
	topSenders := make([]SenderCount, 15)
	for i := 0; i < 15; i++ {
		topSenders[i] = SenderCount{
			Sender: "sender" + string(rune('a'+i)) + "@example.com",
			Count:  100 - i,
		}
	}

	m.Summary = &Summary{
		MessageCount:   15,
		TotalSizeBytes: 1024,
		DateRange:      [2]string{"2024-01-01", "2024-01-31"},
		TopSenders:     topSenders,
	}

	summary := m.FormatSummary()

	// Should only include first 10 senders
	if !strings.Contains(summary, "sendera@example.com") {
		t.Error("FormatSummary() should include first sender")
	}
	if !strings.Contains(summary, "senderj@example.com") {
		t.Error("FormatSummary() should include 10th sender")
	}
	if strings.Contains(summary, "senderk@example.com") {
		t.Error("FormatSummary() should NOT include 11th sender (limit is 10)")
	}
}

// TestManifest_FormatSummary_ExecutionNoCompletedAt tests execution without completion time.
func TestManifest_FormatSummary_ExecutionNoCompletedAt(t *testing.T) {
	m := NewManifest("no completed test", []string{"id1"})
	m.Execution = &Execution{
		StartedAt:   time.Now(),
		CompletedAt: nil, // Not completed yet
		Method:      MethodDelete,
		Succeeded:   5,
		Failed:      0,
	}

	summary := m.FormatSummary()

	// Should have execution section but no completed timestamp
	if !strings.Contains(summary, "Execution:") {
		t.Error("FormatSummary() should include Execution section")
	}
	if !strings.Contains(summary, "Method: delete") {
		t.Error("FormatSummary() should include method")
	}
	if strings.Contains(summary, "Completed:") {
		t.Error("FormatSummary() should NOT include Completed when CompletedAt is nil")
	}
}

// TestManager_SaveManifest_UnknownStatus tests saving with an unknown status.
func TestManager_SaveManifest_UnknownStatus(t *testing.T) {
	mgr := testManager(t)

	m := NewManifest("unknown status", []string{"a"})
	m.Status = "invalid_status" // Unknown status

	// Should still save (to pending dir by default)
	if err := mgr.SaveManifest(m); err != nil {
		t.Fatalf("SaveManifest() with unknown status error = %v", err)
	}

	// Should be findable (saved to pending by default)
	loaded, _, err := mgr.GetManifest(m.ID)
	if err != nil {
		t.Fatalf("GetManifest() error = %v", err)
	}
	if loaded.Status != "invalid_status" {
		t.Errorf("Status = %q, want %q", loaded.Status, "invalid_status")
	}
}

// TestManager_ListManifests_NonexistentDir tests listing from a nonexistent directory.
func TestManager_ListManifests_NonexistentDir(t *testing.T) {
	mgr := testManager(t)

	// Remove the pending directory
	if err := os.RemoveAll(mgr.PendingDir()); err != nil {
		t.Fatalf("RemoveAll() error = %v", err)
	}

	// ListPending should return empty (not error) for nonexistent dir
	pending, err := mgr.ListPending()
	if err != nil {
		t.Fatalf("ListPending() error = %v", err)
	}
	if len(pending) != 0 {
		t.Errorf("ListPending() for nonexistent dir = %d, want 0", len(pending))
	}
}
