package tui

import (
	"context"
	"path/filepath"
	"sort"
	"testing"

	"github.com/wesm/msgvault/internal/deletion"
	"github.com/wesm/msgvault/internal/query"
	"github.com/wesm/msgvault/internal/query/querytest"
	"github.com/wesm/msgvault/internal/testutil"
)

// ControllerTestEnv encapsulates common setup for ActionController tests.
type ControllerTestEnv struct {
	Ctrl *ActionController
	Dir  string
	Mgr  *deletion.Manager
}

// NewControllerTestEnv creates a ControllerTestEnv with a temporary directory
// and deletion manager wired to the given engine.
func NewControllerTestEnv(t *testing.T, engine *querytest.MockEngine) *ControllerTestEnv {
	t.Helper()
	dir := t.TempDir()
	mgr, err := deletion.NewManager(filepath.Join(dir, "deletions"))
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	return &ControllerTestEnv{
		Ctrl: NewActionController(engine, dir, mgr),
		Dir:  dir,
		Mgr:  mgr,
	}
}

func newTestController(t *testing.T, gmailIDs ...string) *ActionController {
	t.Helper()
	env := NewControllerTestEnv(t, &querytest.MockEngine{GmailIDs: gmailIDs})
	return env.Ctrl
}

type stageArgs struct {
	aggregates  map[string]bool
	selection   map[int64]bool
	view        query.ViewType
	accountID   *int64
	accounts    []query.AccountInfo
	messages    []query.MessageSummary
	drillFilter *query.MessageFilter
}

func stageForDeletion(t *testing.T, ctrl *ActionController, args stageArgs) *deletion.Manifest {
	t.Helper()
	view := args.view
	manifest, err := ctrl.StageForDeletion(
		args.aggregates, args.selection, view, args.accountID, args.accounts,
		view, "", query.TimeYear, args.messages, args.drillFilter,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	return manifest
}

func msgSummary(id int64, sourceID string) query.MessageSummary {
	return query.MessageSummary{ID: id, SourceMessageID: sourceID}
}

func TestStageForDeletion_FromAggregateSelection(t *testing.T) {
	ctrl := newTestController(t, "gid1", "gid2", "gid3")

	manifest := stageForDeletion(t, ctrl, stageArgs{
		aggregates: testutil.MakeSet("alice@example.com"),
		view:       query.ViewSenders,
	})

	if len(manifest.GmailIDs) != 3 {
		t.Errorf("expected 3 gmail IDs, got %d", len(manifest.GmailIDs))
	}
	testutil.AssertStrings(t, manifest.Filters.Senders, "alice@example.com")
	if manifest.CreatedBy != "tui" {
		t.Errorf("expected createdBy 'tui', got %q", manifest.CreatedBy)
	}
}

func TestStageForDeletion_FromMessageSelection(t *testing.T) {
	ctrl := newTestController(t)

	messages := []query.MessageSummary{
		msgSummary(10, "gid_a"),
		msgSummary(20, "gid_b"),
		msgSummary(30, "gid_c"),
	}

	manifest := stageForDeletion(t, ctrl, stageArgs{
		selection: testutil.MakeSet[int64](10, 30),
		view:      query.ViewSenders,
		messages:  messages,
	})

	ids := make([]string, len(manifest.GmailIDs))
	copy(ids, manifest.GmailIDs)
	sort.Strings(ids)

	if len(ids) != 2 || ids[0] != "gid_a" || ids[1] != "gid_c" {
		t.Errorf("expected [gid_a gid_c], got %v", ids)
	}
}

func TestStageForDeletion_NoSelection(t *testing.T) {
	ctrl := newTestController(t)

	_, err := ctrl.StageForDeletion(
		nil, nil, query.ViewSenders, nil, nil,
		query.ViewSenders, "", query.TimeYear, nil, nil,
	)
	if err == nil {
		t.Fatal("expected error for empty selection")
	}
}

func TestStageForDeletion_MultipleAggregates_DeterministicFilter(t *testing.T) {
	ctrl := newTestController(t, "gid1")

	agg := testutil.MakeSet("charlie@example.com", "alice@example.com", "bob@example.com")

	for i := 0; i < 10; i++ {
		manifest := stageForDeletion(t, ctrl, stageArgs{aggregates: agg, view: query.ViewSenders})
		testutil.AssertStrings(t, manifest.Filters.Senders, "alice@example.com", "bob@example.com", "charlie@example.com")
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
			testutil.AssertStrings(t, f.Senders, "a@b.com")
		}},
		{"recipients", query.ViewRecipients, "c@d.com", func(t *testing.T, f deletion.Filters) {
			testutil.AssertStrings(t, f.Recipients, "c@d.com")
		}},
		{"domains", query.ViewDomains, "example.com", func(t *testing.T, f deletion.Filters) {
			testutil.AssertStrings(t, f.SenderDomains, "example.com")
		}},
		{"labels", query.ViewLabels, "INBOX", func(t *testing.T, f deletion.Filters) {
			testutil.AssertStrings(t, f.Labels, "INBOX")
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := newTestController(t, "gid1")

			manifest := stageForDeletion(t, ctrl, stageArgs{
				aggregates: testutil.MakeSet(tt.key),
				view:       tt.viewType,
			})
			tt.check(t, manifest.Filters)
		})
	}
}

func TestStageForDeletion_AccountFilter(t *testing.T) {
	ctrl := newTestController(t, "gid1")

	accountID := int64(42)
	accounts := []query.AccountInfo{
		{ID: 42, Identifier: "test@gmail.com"},
	}

	manifest := stageForDeletion(t, ctrl, stageArgs{
		aggregates: testutil.MakeSet("sender@x.com"),
		view:       query.ViewSenders,
		accountID:  &accountID,
		accounts:   accounts,
	})
	if manifest.Filters.Account != "test@gmail.com" {
		t.Errorf("expected account 'test@gmail.com', got %q", manifest.Filters.Account)
	}
}

func TestStageForDeletion_DrillFilterApplied(t *testing.T) {
	// Simulate: drill into sender "alice@example.com", switch to time view,
	// select "2024-01". The filter should include both sender AND time period.
	var capturedFilter query.MessageFilter
	engine := &querytest.MockEngine{
		GetGmailIDsByFilterFunc: func(_ context.Context, f query.MessageFilter) ([]string, error) {
			capturedFilter = f
			return []string{"gid1", "gid2"}, nil
		},
	}
	env := NewControllerTestEnv(t, engine)

	drillFilter := &query.MessageFilter{
		Sender: "alice@example.com",
	}

	manifest := stageForDeletion(t, env.Ctrl, stageArgs{
		aggregates:  testutil.MakeSet("2024-01"),
		view:        query.ViewTime,
		drillFilter: drillFilter,
	})

	// Verify the filter passed to the engine includes both drill context and selection
	if capturedFilter.Sender != "alice@example.com" {
		t.Errorf("expected drill filter sender 'alice@example.com', got %q", capturedFilter.Sender)
	}
	if capturedFilter.TimeRange.Period != "2024-01" {
		t.Errorf("expected time period '2024-01', got %q", capturedFilter.TimeRange.Period)
	}
	if len(manifest.GmailIDs) != 2 {
		t.Errorf("expected 2 gmail IDs, got %d", len(manifest.GmailIDs))
	}
}

func TestStageForDeletion_NoDrillFilter(t *testing.T) {
	// Without drill filter, only the aggregate selection filter is applied.
	var capturedFilter query.MessageFilter
	engine := &querytest.MockEngine{
		GetGmailIDsByFilterFunc: func(_ context.Context, f query.MessageFilter) ([]string, error) {
			capturedFilter = f
			return []string{"gid1"}, nil
		},
	}
	env := NewControllerTestEnv(t, engine)

	stageForDeletion(t, env.Ctrl, stageArgs{
		aggregates: testutil.MakeSet("2024-01"),
		view:       query.ViewTime,
	})

	if capturedFilter.Sender != "" {
		t.Errorf("expected no sender filter, got %q", capturedFilter.Sender)
	}
	if capturedFilter.TimeRange.Period != "2024-01" {
		t.Errorf("expected time period '2024-01', got %q", capturedFilter.TimeRange.Period)
	}
}

func TestExportAttachments_NilDetail(t *testing.T) {
	ctrl := newTestController(t)
	cmd := ctrl.ExportAttachments(nil, nil)
	if cmd != nil {
		t.Error("expected nil cmd for nil detail")
	}
}

func TestExportAttachments_NoSelection(t *testing.T) {
	ctrl := newTestController(t)
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
