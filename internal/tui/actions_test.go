package tui

import (
	"path/filepath"
	"sort"
	"testing"

	"github.com/wesm/msgvault/internal/deletion"
	"github.com/wesm/msgvault/internal/query"
)

func testActionController(t *testing.T, engine *mockEngine) (*ActionController, string) {
	t.Helper()
	dir := t.TempDir()
	mgr, err := deletion.NewManager(filepath.Join(dir, "deletions"))
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	return NewActionController(engine, dir, mgr), dir
}

func TestStageForDeletion_FromAggregateSelection(t *testing.T) {
	engine := &mockEngine{
		gmailIDs: []string{"gid1", "gid2", "gid3"},
	}
	ctrl, _ := testActionController(t, engine)

	manifest, err := ctrl.StageForDeletion(
		map[string]bool{"alice@example.com": true},
		nil,
		query.ViewSenders,
		nil,
		nil,
		query.ViewSenders,
		"",
		query.TimeYear,
		nil,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(manifest.GmailIDs) != 3 {
		t.Errorf("expected 3 gmail IDs, got %d", len(manifest.GmailIDs))
	}
	if manifest.Filters.Sender != "alice@example.com" {
		t.Errorf("expected sender filter 'alice@example.com', got %q", manifest.Filters.Sender)
	}
	if manifest.CreatedBy != "tui" {
		t.Errorf("expected createdBy 'tui', got %q", manifest.CreatedBy)
	}
}

func TestStageForDeletion_FromMessageSelection(t *testing.T) {
	engine := &mockEngine{}
	ctrl, _ := testActionController(t, engine)

	messages := []query.MessageSummary{
		{ID: 1, SourceMessageID: "gid_a"},
		{ID: 2, SourceMessageID: "gid_b"},
		{ID: 3, SourceMessageID: "gid_c"},
	}
	selection := map[int64]bool{1: true, 3: true}

	manifest, err := ctrl.StageForDeletion(
		nil, selection, query.ViewSenders, nil, nil,
		query.ViewSenders, "", query.TimeYear, messages,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ids := make([]string, len(manifest.GmailIDs))
	copy(ids, manifest.GmailIDs)
	sort.Strings(ids)

	if len(ids) != 2 || ids[0] != "gid_a" || ids[1] != "gid_c" {
		t.Errorf("expected [gid_a gid_c], got %v", ids)
	}
}

func TestStageForDeletion_NoSelection(t *testing.T) {
	engine := &mockEngine{}
	ctrl, _ := testActionController(t, engine)

	_, err := ctrl.StageForDeletion(
		nil, nil, query.ViewSenders, nil, nil,
		query.ViewSenders, "", query.TimeYear, nil,
	)
	if err == nil {
		t.Fatal("expected error for empty selection")
	}
}

func TestStageForDeletion_MultipleAggregates_DeterministicFilter(t *testing.T) {
	engine := &mockEngine{gmailIDs: []string{"gid1"}}
	ctrl, _ := testActionController(t, engine)

	agg := map[string]bool{
		"charlie@example.com": true,
		"alice@example.com":   true,
		"bob@example.com":     true,
	}

	// Run multiple times to verify determinism
	for i := 0; i < 10; i++ {
		manifest, err := ctrl.StageForDeletion(
			agg, nil, query.ViewSenders, nil, nil,
			query.ViewSenders, "", query.TimeYear, nil,
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if manifest.Filters.Sender != "alice@example.com" {
			t.Fatalf("iteration %d: expected sender 'alice@example.com', got %q", i, manifest.Filters.Sender)
		}
	}
}

func TestStageForDeletion_ViewTypes(t *testing.T) {
	tests := []struct {
		name     string
		viewType query.ViewType
		key      string
		check    func(t *testing.T, f deletion.Filters)
	}{
		{"senders", query.ViewSenders, "a@b.com", func(t *testing.T, f deletion.Filters) {
			if f.Sender != "a@b.com" {
				t.Errorf("expected sender a@b.com, got %q", f.Sender)
			}
		}},
		{"recipients", query.ViewRecipients, "c@d.com", func(t *testing.T, f deletion.Filters) {
			if f.Recipient != "c@d.com" {
				t.Errorf("expected recipient c@d.com, got %q", f.Recipient)
			}
		}},
		{"domains", query.ViewDomains, "example.com", func(t *testing.T, f deletion.Filters) {
			if f.SenderDomain != "example.com" {
				t.Errorf("expected domain example.com, got %q", f.SenderDomain)
			}
		}},
		{"labels", query.ViewLabels, "INBOX", func(t *testing.T, f deletion.Filters) {
			if f.Label != "INBOX" {
				t.Errorf("expected label INBOX, got %q", f.Label)
			}
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			engine := &mockEngine{gmailIDs: []string{"gid1"}}
			ctrl, _ := testActionController(t, engine)

			manifest, err := ctrl.StageForDeletion(
				map[string]bool{tt.key: true}, nil, tt.viewType, nil, nil,
				tt.viewType, "", query.TimeYear, nil,
			)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			tt.check(t, manifest.Filters)
		})
	}
}

func TestStageForDeletion_AccountFilter(t *testing.T) {
	engine := &mockEngine{gmailIDs: []string{"gid1"}}
	ctrl, _ := testActionController(t, engine)

	accountID := int64(42)
	accounts := []query.AccountInfo{
		{ID: 42, Identifier: "test@gmail.com"},
	}

	manifest, err := ctrl.StageForDeletion(
		map[string]bool{"sender@x.com": true}, nil, query.ViewSenders,
		&accountID, accounts, query.ViewSenders, "", query.TimeYear, nil,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if manifest.Filters.Account != "test@gmail.com" {
		t.Errorf("expected account 'test@gmail.com', got %q", manifest.Filters.Account)
	}
}

func TestExportAttachments_NilDetail(t *testing.T) {
	ctrl, _ := testActionController(t, &mockEngine{})
	cmd := ctrl.ExportAttachments(nil, nil)
	if cmd != nil {
		t.Error("expected nil cmd for nil detail")
	}
}

func TestExportAttachments_NoSelection(t *testing.T) {
	ctrl, _ := testActionController(t, &mockEngine{})
	detail := &query.MessageDetail{
		Attachments: []query.AttachmentInfo{
			{ID: 1, Filename: "file.pdf", ContentHash: "abc123"},
		},
	}
	cmd := ctrl.ExportAttachments(detail, map[int]bool{})
	if cmd != nil {
		t.Error("expected nil cmd for empty selection")
	}
}
