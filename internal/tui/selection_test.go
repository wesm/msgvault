package tui

import (
	"fmt"
	"strings"
	"testing"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/wesm/msgvault/internal/deletion"
	"github.com/wesm/msgvault/internal/query"
)

func TestSelectionToggle(t *testing.T) {
	model := NewBuilder().WithRows(
		makeRow("alice@example.com", 10),
		makeRow("bob@example.com", 5),
		makeRow("carol@example.com", 3),
	).Build()

	// Toggle selection with space
	model.cursor = 0
	model, _ = sendKey(t, model, key(' '))

	assertSelected(t, model, "alice@example.com")

	// Toggle off
	model, _ = sendKey(t, model, key(' '))

	assertNotSelected(t, model, "alice@example.com")
}


func TestSelectAllVisible(t *testing.T) {
	rows := []query.AggregateRow{
		makeRow("row1", 10), makeRow("row2", 9), makeRow("row3", 8),
		makeRow("row4", 7), makeRow("row5", 6), makeRow("row6", 5),
	}
	model := newTestModelWithRows(rows)
	model.pageSize = 3 // Only 3 rows visible
	model.scrollOffset = 0

	// Select all visible with 'S'
	newModel, _ := model.handleAggregateKeys(key('S'))
	model = newModel.(Model)

	// Should only select first 3 (visible) rows
	assertSelectionCount(t, model, 3)
	assertSelected(t, model, "row1")
	assertSelected(t, model, "row2")
	assertSelected(t, model, "row3")
	assertNotSelected(t, model, "row4")
	assertNotSelected(t, model, "row5")
	assertNotSelected(t, model, "row6")
}


func TestSelectAllVisibleWithScroll(t *testing.T) {
	rows := []query.AggregateRow{
		makeRow("row1", 10), makeRow("row2", 9), makeRow("row3", 8),
		makeRow("row4", 7), makeRow("row5", 6), makeRow("row6", 5),
	}
	model := newTestModelWithRows(rows)
	model.pageSize = 3
	model.scrollOffset = 2 // Scrolled down, showing row3-row5

	// Select all visible with 'S'
	newModel, _ := model.handleAggregateKeys(key('S'))
	model = newModel.(Model)

	// Should only select visible rows (row3, row4, row5)
	assertSelectionCount(t, model, 3)
	assertNotSelected(t, model, "row1")
	assertNotSelected(t, model, "row2")
	assertSelected(t, model, "row3")
	assertSelected(t, model, "row4")
	assertSelected(t, model, "row5")
	assertNotSelected(t, model, "row6")
}


func TestSelectionClearedOnViewSwitch(t *testing.T) {
	model := NewBuilder().
		WithRows(query.AggregateRow{Key: "alice@example.com", Count: 10}).
		WithPageSize(10).WithSize(100, 20).
		Build()

	// Select an item
	model.cursor = 0
	model = applyAggregateKey(t, model, key(' '))

	if len(model.selection.aggregateKeys) != 1 {
		t.Fatal("expected 1 selected")
	}

	// Switch view with Tab
	model = applyAggregateKey(t, model, keyTab())

	// Selection should be cleared
	if len(model.selection.aggregateKeys) != 0 {
		t.Errorf("expected selection cleared on view switch, got %d items", len(model.selection.aggregateKeys))
	}

	// ViewType should match selection's AggregateViewType
	if model.selection.aggregateViewType != model.viewType {
		t.Errorf("expected aggregateViewType %v to match viewType %v", model.selection.aggregateViewType, model.viewType)
	}
}


func TestSelectionClearedOnShiftTab(t *testing.T) {
	model := NewBuilder().
		WithRows(query.AggregateRow{Key: "alice@example.com", Count: 10}).
		WithPageSize(10).WithSize(100, 20).
		Build()

	// Select an item
	model.cursor = 0
	model = applyAggregateKey(t, model, key(' '))

	// Switch view with Shift+Tab
	model = applyAggregateKey(t, model, keyShiftTab())

	// Selection should be cleared
	if len(model.selection.aggregateKeys) != 0 {
		t.Errorf("expected selection cleared on view switch, got %d items", len(model.selection.aggregateKeys))
	}
}


func TestClearSelection(t *testing.T) {
	model := NewBuilder().
		WithRows(query.AggregateRow{Key: "alice@example.com", Count: 10}).
		WithPageSize(10).WithSize(100, 20).
		Build()

	// Select an item
	model.cursor = 0
	model = applyAggregateKey(t, model, key(' '))

	if len(model.selection.aggregateKeys) != 1 {
		t.Fatal("expected 1 selected")
	}

	// Clear with 'x'
	model = applyAggregateKey(t, model, key('x'))

	if len(model.selection.aggregateKeys) != 0 {
		t.Errorf("expected selection cleared, got %d items", len(model.selection.aggregateKeys))
	}
}


func TestStageForDeletionWithAggregateSelection(t *testing.T) {
	model := NewBuilder().
		WithRows(query.AggregateRow{Key: "alice@example.com", Count: 2}).
		WithGmailIDs("msg1", "msg2").
		Build()

	// Select an aggregate
	model.cursor = 0
	model, _ = sendKey(t, model, key(' '))

	// Stage for deletion with 'D'
	model, _ = sendKey(t, model, key('D'))

	// Should show confirmation modal
	assertModal(t, model, modalDeleteConfirm)

	// Should have pending manifest with 2 messages
	if model.pendingManifest == nil {
		t.Fatal("expected pendingManifest to be set")
	}

	if len(model.pendingManifest.GmailIDs) != 2 {
		t.Errorf("expected 2 Gmail IDs, got %d", len(model.pendingManifest.GmailIDs))
	}
}


func TestStageForDeletionWithAccountFilter(t *testing.T) {
	accountID := int64(1)
	model := NewBuilder().
		WithRows(query.AggregateRow{Key: "alice@example.com", Count: 2}).
		WithGmailIDs("msg1", "msg2").
		WithPageSize(10).WithSize(100, 20).
		WithAccounts(
			query.AccountInfo{ID: 1, Identifier: "user1@gmail.com"},
			query.AccountInfo{ID: 2, Identifier: "user2@gmail.com"},
		).
		WithAccountFilter(&accountID).
		Build()

	// Select an aggregate
	model.cursor = 0
	model = applyAggregateKey(t, model, key(' '))

	// Stage for deletion
	newModel, _ := model.stageForDeletion()
	model = newModel.(Model)

	// Should have account set in manifest
	if model.pendingManifest == nil {
		t.Fatal("expected pendingManifest to be set")
	}
	if model.pendingManifest.Filters.Account != "user1@gmail.com" {
		t.Errorf("expected account='user1@gmail.com', got %q", model.pendingManifest.Filters.Account)
	}
}


func TestStageForDeletionWithSingleAccount(t *testing.T) {
	model := NewBuilder().
		WithRows(query.AggregateRow{Key: "alice@example.com", Count: 2}).
		WithGmailIDs("msg1", "msg2").
		WithPageSize(10).WithSize(100, 20).
		WithAccounts(query.AccountInfo{ID: 1, Identifier: "only@gmail.com"}).
		Build()

	// Select an aggregate
	model.cursor = 0
	model = applyAggregateKey(t, model, key(' '))

	// Stage for deletion
	newModel, _ := model.stageForDeletion()
	model = newModel.(Model)

	// Should auto-use the single account
	if model.pendingManifest == nil {
		t.Fatal("expected pendingManifest to be set")
	}
	if model.pendingManifest.Filters.Account != "only@gmail.com" {
		t.Errorf("expected account='only@gmail.com', got %q", model.pendingManifest.Filters.Account)
	}
}


func TestStageForDeletionWithMultipleAccountsNoFilter(t *testing.T) {
	model := NewBuilder().
		WithRows(query.AggregateRow{Key: "alice@example.com", Count: 2}).
		WithGmailIDs("msg1", "msg2").
		WithPageSize(10).WithSize(100, 20).
		WithAccounts(
			query.AccountInfo{ID: 1, Identifier: "user1@gmail.com"},
			query.AccountInfo{ID: 2, Identifier: "user2@gmail.com"},
		).
		Build()

	// Select an aggregate
	model.cursor = 0
	model = applyAggregateKey(t, model, key(' '))

	// Stage for deletion
	newModel, _ := model.stageForDeletion()
	model = newModel.(Model)

	// Should leave account empty (requires --account flag)
	if model.pendingManifest == nil {
		t.Fatal("expected pendingManifest to be set")
	}
	if model.pendingManifest.Filters.Account != "" {
		t.Errorf("expected empty account, got %q", model.pendingManifest.Filters.Account)
	}
}


func TestStageForDeletionWithAccountFilterNotFound(t *testing.T) {
	nonExistentID := int64(999) // ID not in accounts list
	model := NewBuilder().
		WithRows(query.AggregateRow{Key: "alice@example.com", Count: 2}).
		WithGmailIDs("msg1", "msg2").
		WithPageSize(10).WithSize(100, 20).
		WithAccounts(
			query.AccountInfo{ID: 1, Identifier: "user1@gmail.com"},
			query.AccountInfo{ID: 2, Identifier: "user2@gmail.com"},
		).
		WithAccountFilter(&nonExistentID).
		Build()

	// Select an aggregate
	model.cursor = 0
	model = applyAggregateKey(t, model, key(' '))

	// Stage for deletion - should proceed despite filter not found
	newModel, _ := model.stageForDeletion()
	model = newModel.(Model)

	// Should still create manifest with empty account (warning logged)
	if model.pendingManifest == nil {
		t.Fatal("expected pendingManifest to be set (staging should proceed with warning)")
	}
	if model.pendingManifest.Filters.Account != "" {
		t.Errorf("expected empty account when filter not found, got %q", model.pendingManifest.Filters.Account)
	}
	// Modal should be delete confirm, not delete result (error)
	if model.modal != modalDeleteConfirm {
		t.Errorf("expected modalDeleteConfirm, got %v", model.modal)
	}
	// Verify the warning is shown in the modal when account is empty
	view := model.View()
	if !strings.Contains(view, "Account not set") {
		t.Errorf("expected 'Account not set' warning in delete confirm modal, view:\n%s", view)
	}
}


func TestAKeyShowsAllMessages(t *testing.T) {
	model := NewBuilder().
		WithRows(query.AggregateRow{Key: "alice@example.com", Count: 2}).
		Build()

	// Press 'a' - should go to all messages view
	var cmd tea.Cmd
	model, cmd = sendKey(t, model, key('a'))

	// Should navigate to message list with no filter
	assertLevel(t, model, levelMessageList)

	if model.filterKey != "" {
		t.Errorf("expected empty filterKey for all messages, got %q", model.filterKey)
	}

	// Should have a command to load messages
	if cmd == nil {
		t.Error("expected command to load messages")
	}

	// Should have saved breadcrumb
	if len(model.breadcrumbs) != 1 {
		t.Errorf("expected 1 breadcrumb, got %d", len(model.breadcrumbs))
	}
}


func TestModalDismiss(t *testing.T) {
	model := NewBuilder().
		WithModal(modalDeleteResult).
		Build()
	model.modalResult = "Test result"

	// Any key should dismiss result modal
	model, _ = applyModalKey(t, model, key('x'))

	if model.modal != modalNone {
		t.Errorf("expected modalNone after dismissal, got %v", model.modal)
	}

	if model.modalResult != "" {
		t.Error("expected modalResult to be cleared")
	}
}


func TestConfirmModalCancel(t *testing.T) {
	model := NewBuilder().
		WithModal(modalDeleteConfirm).
		Build()
	model.pendingManifest = &deletion.Manifest{}

	// 'n' should cancel
	model, _ = applyModalKey(t, model, key('n'))

	if model.modal != modalNone {
		t.Errorf("expected modalNone after cancel, got %v", model.modal)
	}

	if model.pendingManifest != nil {
		t.Error("expected pendingManifest to be cleared")
	}
}


func TestSelectionCount(t *testing.T) {
	model := Model{
		selection: selectionState{
			aggregateKeys: map[string]bool{"a": true, "b": true},
			messageIDs:    map[int64]bool{1: true, 2: true, 3: true},
		},
	}

	if model.selectionCount() != 5 {
		t.Errorf("expected SelectionCount() = 5, got %d", model.selectionCount())
	}
}


func TestHasSelection(t *testing.T) {
	model := Model{
		selection: selectionState{
			aggregateKeys: make(map[string]bool),
			messageIDs:    make(map[int64]bool),
		},
	}

	if model.hasSelection() {
		t.Error("expected HasSelection() = false for empty selection")
	}

	model.selection.aggregateKeys["test"] = true
	if !model.hasSelection() {
		t.Error("expected HasSelection() = true with aggregate selection")
	}

	model.selection.aggregateKeys = make(map[string]bool)
	model.selection.messageIDs[1] = true
	if !model.hasSelection() {
		t.Error("expected HasSelection() = true with message selection")
	}
}


// TestDKeyAutoSelectsCurrentRow verifies 'd' key selects current row when nothing selected.
func TestDKeyAutoSelectsCurrentRow(t *testing.T) {
	model := NewBuilder().
		WithRows(
			query.AggregateRow{Key: "alice@example.com", Count: 10},
			query.AggregateRow{Key: "bob@example.com", Count: 5},
		).
		WithGmailIDs("msg1", "msg2").
		WithViewType(query.ViewSenders).
		WithPageSize(10).WithSize(100, 20).
		WithAccounts(query.AccountInfo{ID: 1, Identifier: "test@gmail.com"}).
		Build()
	model.cursor = 1 // On bob@example.com

	// Verify nothing is selected
	if model.hasSelection() {
		t.Error("expected no selection initially")
	}

	// Press 'd' without selecting anything first
	m := applyAggregateKey(t, model, key('d'))

	// Should have auto-selected current row
	if !m.selection.aggregateKeys["bob@example.com"] {
		t.Error("expected bob@example.com to be auto-selected")
	}

	// Should show delete confirmation modal
	if m.modal != modalDeleteConfirm {
		t.Errorf("expected modalDeleteConfirm, got %v", m.modal)
	}
}

// TestDKeyWithExistingSelection verifies 'd' key uses existing selection when present.
func TestDKeyWithExistingSelection(t *testing.T) {
	model := NewBuilder().
		WithRows(
			query.AggregateRow{Key: "alice@example.com", Count: 10},
			query.AggregateRow{Key: "bob@example.com", Count: 5},
		).
		WithGmailIDs("msg1", "msg2").
		WithViewType(query.ViewSenders).
		WithPageSize(10).WithSize(100, 20).
		WithAccounts(query.AccountInfo{ID: 1, Identifier: "test@gmail.com"}).
		Build()
	model.cursor = 1 // On bob@example.com

	// Pre-select alice (not the current row)
	model.selection.aggregateKeys["alice@example.com"] = true
	model.selection.aggregateViewType = query.ViewSenders

	// Press 'd' - should use existing selection, not auto-select current row
	m := applyAggregateKey(t, model, key('d'))

	// Should still have alice selected
	if !m.selection.aggregateKeys["alice@example.com"] {
		t.Error("expected alice@example.com to remain selected")
	}

	// Should NOT have auto-selected bob
	if m.selection.aggregateKeys["bob@example.com"] {
		t.Error("expected bob@example.com to NOT be selected")
	}

	// Should show delete confirmation modal
	if m.modal != modalDeleteConfirm {
		t.Errorf("expected modalDeleteConfirm, got %v", m.modal)
	}
}

// TestMessageListDKeyAutoSelectsCurrentMessage verifies 'd' in message list auto-selects.
func TestMessageListDKeyAutoSelectsCurrentMessage(t *testing.T) {
	model := NewBuilder().
		WithMessages(
			query.MessageSummary{ID: 1, SourceMessageID: "msg1", Subject: "Hello"},
			query.MessageSummary{ID: 2, SourceMessageID: "msg2", Subject: "World"},
		).
		WithLevel(levelMessageList).
		WithPageSize(10).WithSize(100, 20).
		WithAccounts(query.AccountInfo{ID: 1, Identifier: "test@gmail.com"}).
		Build()

	// Verify nothing is selected
	if model.hasSelection() {
		t.Error("expected no selection initially")
	}

	// Press 'd' without selecting anything first
	m := applyMessageListKey(t, model, key('d'))

	// Should have auto-selected current message
	if !m.selection.messageIDs[1] {
		t.Error("expected message ID 1 to be auto-selected")
	}

	// Should show delete confirmation modal
	if m.modal != modalDeleteConfirm {
		t.Errorf("expected modalDeleteConfirm, got %v", m.modal)
	}
}


func TestToggleAggregateSelection(t *testing.T) {
	m := NewBuilder().WithRows(
		query.AggregateRow{Key: "alice@example.com"},
		query.AggregateRow{Key: "bob@example.com"},
	).Build()
	m.cursor = 0

	// Toggle on
	m.toggleAggregateSelection()
	if !m.selection.aggregateKeys["alice@example.com"] {
		t.Error("expected alice to be selected")
	}

	// Toggle off
	m.toggleAggregateSelection()
	if m.selection.aggregateKeys["alice@example.com"] {
		t.Error("expected alice to be deselected")
	}
}


func TestSelectVisibleAggregates(t *testing.T) {
	rows := make([]query.AggregateRow, 0, 10)
	for i := 0; i < 10; i++ {
		rows = append(rows, query.AggregateRow{Key: fmt.Sprintf("user%d", i)})
	}
	m := NewBuilder().WithRows(rows...).Build()
	m.pageSize = 3
	m.scrollOffset = 2

	m.selectVisibleAggregates()

	for i := 2; i < 5; i++ {
		key := fmt.Sprintf("user%d", i)
		if !m.selection.aggregateKeys[key] {
			t.Errorf("expected %s to be selected", key)
		}
	}
	// Items outside visible range should not be selected
	if m.selection.aggregateKeys["user0"] {
		t.Error("user0 should not be selected")
	}
}


func TestSelectVisibleAggregates_OffsetBeyondRows(t *testing.T) {
	m := NewBuilder().WithRows(
		query.AggregateRow{Key: "a"},
	).Build()
	m.scrollOffset = 100
	m.pageSize = 5

	m.selectVisibleAggregates()

	if len(m.selection.aggregateKeys) != 0 {
		t.Error("expected no selections when scrollOffset > len(rows)")
	}
}


func TestClearAllSelections(t *testing.T) {
	m := NewBuilder().WithRows(
		query.AggregateRow{Key: "a"},
	).Build()
	m.selection.aggregateKeys["a"] = true
	m.selection.messageIDs[1] = true

	m.clearAllSelections()

	if len(m.selection.aggregateKeys) != 0 || len(m.selection.messageIDs) != 0 {
		t.Error("expected all selections to be cleared")
	}
}

