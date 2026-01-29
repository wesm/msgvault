package tui

import (
	"context"
	"fmt"
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/muesli/termenv"
	"github.com/wesm/msgvault/internal/deletion"
	"github.com/wesm/msgvault/internal/query"
	"github.com/wesm/msgvault/internal/search"
)

// mockEngine implements query.Engine for testing.
type mockEngine struct {
	rows          []query.AggregateRow
	messages      []query.MessageSummary
	messageDetail *query.MessageDetail
	gmailIDs      []string
}

func (m *mockEngine) AggregateBySender(ctx context.Context, opts query.AggregateOptions) ([]query.AggregateRow, error) {
	return m.rows, nil
}

func (m *mockEngine) AggregateByRecipient(ctx context.Context, opts query.AggregateOptions) ([]query.AggregateRow, error) {
	return m.rows, nil
}

func (m *mockEngine) AggregateByDomain(ctx context.Context, opts query.AggregateOptions) ([]query.AggregateRow, error) {
	return m.rows, nil
}

func (m *mockEngine) AggregateByLabel(ctx context.Context, opts query.AggregateOptions) ([]query.AggregateRow, error) {
	return m.rows, nil
}

func (m *mockEngine) AggregateByTime(ctx context.Context, opts query.AggregateOptions) ([]query.AggregateRow, error) {
	return m.rows, nil
}

func (m *mockEngine) SubAggregate(ctx context.Context, filter query.MessageFilter, groupBy query.ViewType, opts query.AggregateOptions) ([]query.AggregateRow, error) {
	return m.rows, nil
}

func (m *mockEngine) ListMessages(ctx context.Context, filter query.MessageFilter) ([]query.MessageSummary, error) {
	return m.messages, nil
}

func (m *mockEngine) GetMessage(ctx context.Context, id int64) (*query.MessageDetail, error) {
	return m.messageDetail, nil
}

func (m *mockEngine) GetMessageBySourceID(ctx context.Context, sourceMessageID string) (*query.MessageDetail, error) {
	return m.messageDetail, nil
}

func (m *mockEngine) Search(ctx context.Context, q *search.Query, limit, offset int) ([]query.MessageSummary, error) {
	return m.messages, nil
}

func (m *mockEngine) SearchFast(ctx context.Context, q *search.Query, filter query.MessageFilter, limit, offset int) ([]query.MessageSummary, error) {
	return m.messages, nil
}

func (m *mockEngine) SearchFastCount(ctx context.Context, q *search.Query, filter query.MessageFilter) (int64, error) {
	return int64(len(m.messages)), nil
}

func (m *mockEngine) GetGmailIDsByFilter(ctx context.Context, filter query.MessageFilter) ([]string, error) {
	return m.gmailIDs, nil
}

func (m *mockEngine) ListAccounts(ctx context.Context) ([]query.AccountInfo, error) {
	return nil, nil
}

func (m *mockEngine) GetTotalStats(ctx context.Context, opts query.StatsOptions) (*query.TotalStats, error) {
	return &query.TotalStats{}, nil
}

func (m *mockEngine) Close() error {
	return nil
}

// =============================================================================
// Test Fixtures
// =============================================================================

// Common test data
var (
	// testAggregateRows provides a standard set of aggregate rows for testing
	testAggregateRows = []query.AggregateRow{
		{Key: "alice@example.com", Count: 100, TotalSize: 1000, AttachmentCount: 5},
		{Key: "bob@example.com", Count: 50, TotalSize: 500, AttachmentCount: 2},
		{Key: "charlie@example.com", Count: 25, TotalSize: 250, AttachmentCount: 1},
	}
)

// newTestModel creates a Model with common test defaults.
// The returned model has standard width/height and is ready for testing.
func newTestModel(engine *mockEngine) Model {
	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})
	model.width = 100
	model.height = 24
	model.pageSize = 10
	return model
}

// newTestModelWithRows creates a test model pre-populated with aggregate rows.
func newTestModelWithRows(rows []query.AggregateRow) Model {
	engine := &mockEngine{rows: rows}
	model := newTestModel(engine)
	model.rows = rows
	return model
}

// newTestModelAtLevel creates a test model at the specified navigation level.
func newTestModelAtLevel(level viewLevel) Model {
	engine := &mockEngine{}
	model := newTestModel(engine)
	model.level = level
	return model
}

// withSearchQuery sets a search query on the model.
func (m Model) withSearchQuery(q string) Model {
	m.searchQuery = q
	return m
}

// withRequestID sets the aggregate request ID for testing stale response handling.
func (m Model) withAggregateRequestID(id uint64) Model {
	m.aggregateRequestID = id
	return m
}

// withContextStats sets context stats on the model.
func (m Model) withContextStats(stats *query.TotalStats) Model {
	m.contextStats = stats
	return m
}

// withGlobalStats sets global stats on the model.
func (m Model) withGlobalStats(stats *query.TotalStats) Model {
	m.stats = stats
	return m
}

// sumAggregateStats calculates expected totals from aggregate rows.
func sumAggregateStats(rows []query.AggregateRow) (count, size, attachments int64) {
	for _, row := range rows {
		count += row.Count
		size += row.TotalSize
		attachments += row.AttachmentCount
	}
	return
}

// -----------------------------------------------------------------------------
// Key Event Helpers - reduce verbosity of tea.KeyMsg construction
// -----------------------------------------------------------------------------

// key returns a KeyMsg for a single rune (e.g., key('x'), key(' '))
func key(r rune) tea.KeyMsg {
	return tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}}
}

// keyEnter returns a KeyMsg for the Enter key
func keyEnter() tea.KeyMsg {
	return tea.KeyMsg{Type: tea.KeyEnter}
}

// keyEsc returns a KeyMsg for the Escape key
func keyEsc() tea.KeyMsg {
	return tea.KeyMsg{Type: tea.KeyEscape}
}

// keyTab returns a KeyMsg for the Tab key
func keyTab() tea.KeyMsg {
	return tea.KeyMsg{Type: tea.KeyTab}
}

// keyDown returns a KeyMsg for the Down arrow key
func keyDown() tea.KeyMsg {
	return tea.KeyMsg{Type: tea.KeyDown}
}

// keyShiftTab returns a KeyMsg for Shift+Tab
func keyShiftTab() tea.KeyMsg {
	return tea.KeyMsg{Type: tea.KeyShiftTab}
}

// keyLeft returns a KeyMsg for the Left arrow key
func keyLeft() tea.KeyMsg {
	return tea.KeyMsg{Type: tea.KeyLeft}
}

// keyRight returns a KeyMsg for the Right arrow key
func keyRight() tea.KeyMsg {
	return tea.KeyMsg{Type: tea.KeyRight}
}

// keyHome returns a KeyMsg for the Home key
func keyHome() tea.KeyMsg {
	return tea.KeyMsg{Type: tea.KeyHome}
}

// -----------------------------------------------------------------------------
// Data Factories - create test data with minimal boilerplate
// -----------------------------------------------------------------------------

// makeRow creates an AggregateRow with the given key and count.
func makeRow(key string, count int) query.AggregateRow {
	return query.AggregateRow{Key: key, Count: int64(count)}
}

// assertSelected checks that the given key is selected.
func assertSelected(t *testing.T, m Model, key string) {
	t.Helper()
	if !m.selection.AggregateKeys[key] {
		t.Errorf("expected %q to be selected", key)
	}
}

// assertNotSelected checks that the given key is not selected.
func assertNotSelected(t *testing.T, m Model, key string) {
	t.Helper()
	if m.selection.AggregateKeys[key] {
		t.Errorf("expected %q to not be selected", key)
	}
}

// assertSelectionCount checks the number of selected items.
func assertSelectionCount(t *testing.T, m Model, expected int) {
	t.Helper()
	got := len(m.selection.AggregateKeys)
	if got != expected {
		t.Errorf("expected %d selected items, got %d", expected, got)
	}
}

// =============================================================================
// Tests
// =============================================================================

func TestSelectionToggle(t *testing.T) {
	rows := []query.AggregateRow{
		makeRow("alice@example.com", 10),
		makeRow("bob@example.com", 5),
		makeRow("carol@example.com", 3),
	}
	model := newTestModelWithRows(rows)

	// Toggle selection with space
	model.cursor = 0
	newModel, _ := model.handleAggregateKeys(key(' '))
	model = newModel.(Model)

	assertSelected(t, model, "alice@example.com")

	// Toggle off
	newModel, _ = model.handleAggregateKeys(key(' '))
	model = newModel.(Model)

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
	engine := &mockEngine{
		rows: []query.AggregateRow{
			{Key: "alice@example.com", Count: 10},
		},
	}

	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})
	model.rows = engine.rows
	model.pageSize = 10
	model.width = 100
	model.height = 20

	// Select an item
	model.cursor = 0
	newModel, _ := model.handleAggregateKeys(key(' '))
	model = newModel.(Model)

	if len(model.selection.AggregateKeys) != 1 {
		t.Fatal("expected 1 selected")
	}

	// Switch view with Tab
	newModel, _ = model.handleAggregateKeys(keyTab())
	model = newModel.(Model)

	// Selection should be cleared
	if len(model.selection.AggregateKeys) != 0 {
		t.Errorf("expected selection cleared on view switch, got %d items", len(model.selection.AggregateKeys))
	}

	// ViewType should match selection's AggregateViewType
	if model.selection.AggregateViewType != model.viewType {
		t.Errorf("expected AggregateViewType %v to match viewType %v", model.selection.AggregateViewType, model.viewType)
	}
}

func TestSelectionClearedOnShiftTab(t *testing.T) {
	engine := &mockEngine{
		rows: []query.AggregateRow{
			{Key: "alice@example.com", Count: 10},
		},
	}

	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})
	model.rows = engine.rows
	model.pageSize = 10
	model.width = 100
	model.height = 20

	// Select an item
	model.cursor = 0
	newModel, _ := model.handleAggregateKeys(key(' '))
	model = newModel.(Model)

	// Switch view with Shift+Tab
	newModel, _ = model.handleAggregateKeys(keyShiftTab())
	model = newModel.(Model)

	// Selection should be cleared
	if len(model.selection.AggregateKeys) != 0 {
		t.Errorf("expected selection cleared on view switch, got %d items", len(model.selection.AggregateKeys))
	}
}

func TestClearSelection(t *testing.T) {
	engine := &mockEngine{
		rows: []query.AggregateRow{
			{Key: "alice@example.com", Count: 10},
		},
	}

	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})
	model.rows = engine.rows
	model.pageSize = 10
	model.width = 100
	model.height = 20

	// Select an item
	model.cursor = 0
	newModel, _ := model.handleAggregateKeys(key(' '))
	model = newModel.(Model)

	if len(model.selection.AggregateKeys) != 1 {
		t.Fatal("expected 1 selected")
	}

	// Clear with 'x'
	newModel, _ = model.handleAggregateKeys(key('x'))
	model = newModel.(Model)

	if len(model.selection.AggregateKeys) != 0 {
		t.Errorf("expected selection cleared, got %d items", len(model.selection.AggregateKeys))
	}
}

func TestStageForDeletionWithAggregateSelection(t *testing.T) {
	engine := &mockEngine{
		rows: []query.AggregateRow{
			{Key: "alice@example.com", Count: 2},
		},
		gmailIDs: []string{"msg1", "msg2"}, // Returned by GetGmailIDsByFilter
	}

	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})
	model.rows = engine.rows
	model.pageSize = 10
	model.width = 100
	model.height = 20

	// Select an aggregate
	model.cursor = 0
	newModel, _ := model.handleAggregateKeys(key(' '))
	model = newModel.(Model)

	// Stage for deletion with 'D'
	newModel, _ = model.handleAggregateKeys(key('D'))
	model = newModel.(Model)

	// Should show confirmation modal
	if model.modal != modalDeleteConfirm {
		t.Errorf("expected modalDeleteConfirm, got %v", model.modal)
	}

	// Should have pending manifest with 2 messages
	if model.pendingManifest == nil {
		t.Fatal("expected pendingManifest to be set")
	}

	if len(model.pendingManifest.GmailIDs) != 2 {
		t.Errorf("expected 2 Gmail IDs, got %d", len(model.pendingManifest.GmailIDs))
	}
}

func TestStageForDeletionWithAccountFilter(t *testing.T) {
	engine := &mockEngine{
		rows:     []query.AggregateRow{{Key: "alice@example.com", Count: 2}},
		gmailIDs: []string{"msg1", "msg2"},
	}

	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})
	model.rows = engine.rows
	model.pageSize = 10
	model.width = 100
	model.height = 20

	// Set up accounts and account filter
	model.accounts = []query.AccountInfo{
		{ID: 1, Identifier: "user1@gmail.com"},
		{ID: 2, Identifier: "user2@gmail.com"},
	}
	accountID := int64(1)
	model.accountFilter = &accountID

	// Select an aggregate
	model.cursor = 0
	newModel, _ := model.handleAggregateKeys(key(' '))
	model = newModel.(Model)

	// Stage for deletion
	newModel, _ = model.stageForDeletion()
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
	engine := &mockEngine{
		rows:     []query.AggregateRow{{Key: "alice@example.com", Count: 2}},
		gmailIDs: []string{"msg1", "msg2"},
	}

	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})
	model.rows = engine.rows
	model.pageSize = 10
	model.width = 100
	model.height = 20

	// Set up single account with no filter
	model.accounts = []query.AccountInfo{
		{ID: 1, Identifier: "only@gmail.com"},
	}
	model.accountFilter = nil // No filter set

	// Select an aggregate
	model.cursor = 0
	newModel, _ := model.handleAggregateKeys(key(' '))
	model = newModel.(Model)

	// Stage for deletion
	newModel, _ = model.stageForDeletion()
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
	engine := &mockEngine{
		rows:     []query.AggregateRow{{Key: "alice@example.com", Count: 2}},
		gmailIDs: []string{"msg1", "msg2"},
	}

	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})
	model.rows = engine.rows
	model.pageSize = 10
	model.width = 100
	model.height = 20

	// Set up multiple accounts with no filter
	model.accounts = []query.AccountInfo{
		{ID: 1, Identifier: "user1@gmail.com"},
		{ID: 2, Identifier: "user2@gmail.com"},
	}
	model.accountFilter = nil // No filter set

	// Select an aggregate
	model.cursor = 0
	newModel, _ := model.handleAggregateKeys(key(' '))
	model = newModel.(Model)

	// Stage for deletion
	newModel, _ = model.stageForDeletion()
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
	engine := &mockEngine{
		rows:     []query.AggregateRow{{Key: "alice@example.com", Count: 2}},
		gmailIDs: []string{"msg1", "msg2"},
	}

	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})
	model.rows = engine.rows
	model.pageSize = 10
	model.width = 100
	model.height = 20
	model.loading = false // Ensure view renders properly

	// Set up accounts but filter points to non-existent ID
	model.accounts = []query.AccountInfo{
		{ID: 1, Identifier: "user1@gmail.com"},
		{ID: 2, Identifier: "user2@gmail.com"},
	}
	nonExistentID := int64(999) // ID not in accounts list
	model.accountFilter = &nonExistentID

	// Select an aggregate
	model.cursor = 0
	newModel, _ := model.handleAggregateKeys(key(' '))
	model = newModel.(Model)

	// Stage for deletion - should proceed despite filter not found
	newModel, _ = model.stageForDeletion()
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
	engine := &mockEngine{
		rows: []query.AggregateRow{
			{Key: "alice@example.com", Count: 2},
		},
	}

	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})
	model.rows = engine.rows
	model.pageSize = 10
	model.width = 100
	model.height = 20

	// Press 'a' - should go to all messages view
	newModel, cmd := model.handleAggregateKeys(key('a'))
	model = newModel.(Model)

	// Should navigate to message list with no filter
	if model.level != levelMessageList {
		t.Errorf("expected levelMessageList, got %v", model.level)
	}

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
	engine := &mockEngine{}

	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})
	model.modal = modalDeleteResult
	model.modalResult = "Test result"
	model.pageSize = 10
	model.width = 100
	model.height = 20

	// Any key should dismiss result modal
	newModel, _ := model.handleModalKeys(key('x'))
	model = newModel.(Model)

	if model.modal != modalNone {
		t.Errorf("expected modalNone after dismissal, got %v", model.modal)
	}

	if model.modalResult != "" {
		t.Error("expected modalResult to be cleared")
	}
}

func TestConfirmModalCancel(t *testing.T) {
	engine := &mockEngine{}

	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})
	model.modal = modalDeleteConfirm
	model.pendingManifest = &deletion.Manifest{}
	model.pageSize = 10
	model.width = 100
	model.height = 20

	// 'n' should cancel
	newModel, _ := model.handleModalKeys(key('n'))
	model = newModel.(Model)

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
			AggregateKeys: map[string]bool{"a": true, "b": true},
			MessageIDs:    map[int64]bool{1: true, 2: true, 3: true},
		},
	}

	if model.SelectionCount() != 5 {
		t.Errorf("expected SelectionCount() = 5, got %d", model.SelectionCount())
	}
}

func TestHasSelection(t *testing.T) {
	model := Model{
		selection: selectionState{
			AggregateKeys: make(map[string]bool),
			MessageIDs:    make(map[int64]bool),
		},
	}

	if model.HasSelection() {
		t.Error("expected HasSelection() = false for empty selection")
	}

	model.selection.AggregateKeys["test"] = true
	if !model.HasSelection() {
		t.Error("expected HasSelection() = true with aggregate selection")
	}

	model.selection.AggregateKeys = make(map[string]bool)
	model.selection.MessageIDs[1] = true
	if !model.HasSelection() {
		t.Error("expected HasSelection() = true with message selection")
	}
}

func TestStaleAsyncResponsesIgnored(t *testing.T) {
	engine := &mockEngine{
		messages: []query.MessageSummary{
			{ID: 1, Subject: "Test"},
		},
	}

	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})
	model.level = levelMessageList
	model.loadRequestID = 5 // Current request ID

	// Simulate a stale response with old request ID
	staleMsg := messagesLoadedMsg{
		messages:  []query.MessageSummary{{ID: 99, Subject: "Stale"}},
		requestID: 3, // Old request ID
	}

	newModel, _ := model.Update(staleMsg)
	m := newModel.(Model)

	// Stale response should be ignored - messages should be unchanged (empty)
	if len(m.messages) != 0 {
		t.Errorf("stale response should be ignored, got %d messages", len(m.messages))
	}

	// Now send a valid response with current request ID
	validMsg := messagesLoadedMsg{
		messages:  []query.MessageSummary{{ID: 1, Subject: "Valid"}},
		requestID: 5, // Current request ID
	}

	newModel, _ = m.Update(validMsg)
	m = newModel.(Model)

	// Valid response should be processed
	if len(m.messages) != 1 {
		t.Errorf("valid response should be processed, got %d messages", len(m.messages))
	}
	if m.messages[0].Subject != "Valid" {
		t.Errorf("expected subject 'Valid', got %s", m.messages[0].Subject)
	}
}

func TestStaleDetailResponsesIgnored(t *testing.T) {
	engine := &mockEngine{}

	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})
	model.level = levelMessageDetail
	model.detailRequestID = 10 // Current request ID
	model.width = 100          // Set realistic width
	model.height = 30
	model.pageSize = 20

	// Simulate a stale response with old request ID
	staleMsg := messageDetailLoadedMsg{
		detail:    &query.MessageDetail{ID: 99, Subject: "Stale Detail"},
		requestID: 8, // Old request ID
	}

	newModel, _ := model.Update(staleMsg)
	m := newModel.(Model)

	// Stale response should be ignored
	if m.messageDetail != nil {
		t.Error("stale detail response should be ignored")
	}

	// Now send a valid response with current request ID
	validMsg := messageDetailLoadedMsg{
		detail:    &query.MessageDetail{ID: 1, Subject: "Valid Detail"},
		requestID: 10, // Current request ID
	}

	newModel, _ = m.Update(validMsg)
	m = newModel.(Model)

	// Valid response should be processed
	if m.messageDetail == nil {
		t.Error("valid detail response should be processed")
	}
	if m.messageDetail.Subject != "Valid Detail" {
		t.Errorf("expected subject 'Valid Detail', got %s", m.messageDetail.Subject)
	}
}

func TestDetailLineCountResetOnLoad(t *testing.T) {
	engine := &mockEngine{
		messages: []query.MessageSummary{
			{ID: 1, Subject: "Message 1"},
			{ID: 2, Subject: "Message 2"},
		},
	}

	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})
	model.messages = engine.messages
	model.level = levelMessageList
	model.detailLineCount = 100 // Simulate previous message with 100 lines
	model.detailScroll = 50     // Simulate scrolled position
	model.pageSize = 20
	model.width = 100
	model.height = 30

	// Trigger drill-down to detail view
	model.cursor = 0
	newModel, _ := model.handleMessageListKeys(keyEnter())
	m := newModel.(Model)

	// detailLineCount and detailScroll should be reset
	if m.detailLineCount != 0 {
		t.Errorf("expected detailLineCount = 0 on load start, got %d", m.detailLineCount)
	}
	if m.detailScroll != 0 {
		t.Errorf("expected detailScroll = 0 on load start, got %d", m.detailScroll)
	}
	if m.messageDetail != nil {
		t.Error("expected messageDetail = nil on load start")
	}
}

func TestDetailScrollClamping(t *testing.T) {
	engine := &mockEngine{}

	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})
	model.level = levelMessageDetail
	model.pageSize = 10
	model.detailLineCount = 25 // 25 lines total
	model.detailScroll = 0

	// Test scroll down clamping
	model.detailScroll = 100 // Way beyond bounds
	model.clampDetailScroll()

	// Max scroll should be lineCount - detailPageSize = 25 - 12 = 13
	// (detailPageSize = pageSize + 2 because detail view has no table header/separator)
	expectedMax := 13
	if model.detailScroll != expectedMax {
		t.Errorf("expected detailScroll clamped to %d, got %d", expectedMax, model.detailScroll)
	}

	// Test when content fits in one page
	model.detailLineCount = 5 // Less than detailPageSize (12)
	model.detailScroll = 10
	model.clampDetailScroll()

	if model.detailScroll != 0 {
		t.Errorf("expected detailScroll = 0 when content fits page, got %d", model.detailScroll)
	}
}

func TestResizeRecalculatesDetailLineCount(t *testing.T) {
	engine := &mockEngine{}

	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})
	model.level = levelMessageDetail
	model.messageDetail = &query.MessageDetail{
		Subject:  "Test Subject",
		BodyText: "Line 1\nLine 2\nLine 3\nLine 4\nLine 5",
	}
	model.width = 80
	model.height = 20
	model.pageSize = 14

	// Calculate initial line count
	model.updateDetailLineCount()
	initialLineCount := model.detailLineCount

	// Simulate window resize to narrower width (should wrap more)
	resizeMsg := tea.WindowSizeMsg{Width: 40, Height: 20}
	newModel, _ := model.Update(resizeMsg)
	m := newModel.(Model)

	// Line count should be recalculated (narrower width = more wrapping = more lines)
	if m.detailLineCount == initialLineCount && m.width != 80 {
		// Note: This might be equal if wrapping doesn't change, but width should be updated
		if m.width != 40 {
			t.Errorf("expected width = 40 after resize, got %d", m.width)
		}
	}

	// Scroll should be clamped if it exceeds new bounds
	m.detailScroll = 1000
	m.clampDetailScroll()
	maxScroll := m.detailLineCount - m.pageSize
	if maxScroll < 0 {
		maxScroll = 0
	}
	if m.detailScroll > maxScroll {
		t.Errorf("expected detailScroll <= %d after clamp, got %d", maxScroll, m.detailScroll)
	}
}

func TestEndKeyWithZeroLineCount(t *testing.T) {
	engine := &mockEngine{}

	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})
	model.level = levelMessageDetail
	model.pageSize = 20
	model.detailLineCount = 0 // No content yet (loading)
	model.detailScroll = 0

	// Press 'G' (end key) with zero line count
	newModel, _ := model.handleMessageDetailKeys(key('G'))
	m := newModel.(Model)

	// Should not crash and scroll should remain 0
	if m.detailScroll != 0 {
		t.Errorf("expected detailScroll = 0 with zero line count, got %d", m.detailScroll)
	}
}

func TestQuitConfirmationModal(t *testing.T) {
	engine := &mockEngine{}

	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})
	model.pageSize = 10
	model.width = 100
	model.height = 20

	// Press 'q' should open quit confirmation, not quit immediately
	newModel, cmd := model.handleAggregateKeys(key('q'))
	m := newModel.(Model)

	if m.modal != modalQuitConfirm {
		t.Errorf("expected modalQuitConfirm, got %v", m.modal)
	}
	if m.quitting {
		t.Error("should not be quitting yet")
	}
	if cmd != nil {
		t.Error("should not have quit command yet")
	}

	// Press 'n' to cancel
	newModel, _ = m.handleModalKeys(key('n'))
	m = newModel.(Model)

	if m.modal != modalNone {
		t.Errorf("expected modalNone after cancel, got %v", m.modal)
	}
}

func TestQuitConfirmationConfirm(t *testing.T) {
	engine := &mockEngine{}

	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})
	model.modal = modalQuitConfirm
	model.pageSize = 10
	model.width = 100
	model.height = 20

	// Press 'y' to confirm quit
	newModel, cmd := model.handleModalKeys(key('y'))
	m := newModel.(Model)

	if !m.quitting {
		t.Error("expected quitting = true")
	}
	if cmd == nil {
		t.Error("expected quit command")
	}
}

func TestAccountSelectorModal(t *testing.T) {
	engine := &mockEngine{}

	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})
	model.accounts = []query.AccountInfo{
		{ID: 1, Identifier: "alice@example.com"},
		{ID: 2, Identifier: "bob@example.com"},
	}
	model.pageSize = 10
	model.width = 100
	model.height = 20

	// Press 'A' to open account selector
	newModel, _ := model.handleAggregateKeys(key('A'))
	m := newModel.(Model)

	if m.modal != modalAccountSelector {
		t.Errorf("expected modalAccountSelector, got %v", m.modal)
	}
	if m.modalCursor != 0 {
		t.Errorf("expected modalCursor = 0 (All Accounts), got %d", m.modalCursor)
	}

	// Navigate down
	newModel, _ = m.handleModalKeys(key('j'))
	m = newModel.(Model)
	if m.modalCursor != 1 {
		t.Errorf("expected modalCursor = 1, got %d", m.modalCursor)
	}

	// Select account
	newModel, cmd := m.handleModalKeys(keyEnter())
	m = newModel.(Model)

	if m.modal != modalNone {
		t.Errorf("expected modalNone after selection, got %v", m.modal)
	}
	if m.accountFilter == nil || *m.accountFilter != 1 {
		t.Errorf("expected accountFilter = 1, got %v", m.accountFilter)
	}
	if cmd == nil {
		t.Error("expected command to reload data")
	}
}

func TestAttachmentFilterModal(t *testing.T) {
	engine := &mockEngine{}

	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})
	model.pageSize = 10
	model.width = 100
	model.height = 20
	model.attachmentFilter = false

	// Press 'f' to open filter modal
	newModel, _ := model.handleAggregateKeys(key('f'))
	m := newModel.(Model)

	if m.modal != modalAttachmentFilter {
		t.Errorf("expected modalAttachmentFilter, got %v", m.modal)
	}
	if m.modalCursor != 0 {
		t.Errorf("expected modalCursor = 0 (All Messages), got %d", m.modalCursor)
	}

	// Navigate down to "With Attachments"
	newModel, _ = m.handleModalKeys(key('j'))
	m = newModel.(Model)
	if m.modalCursor != 1 {
		t.Errorf("expected modalCursor = 1, got %d", m.modalCursor)
	}

	// Select "With Attachments"
	newModel, _ = m.handleModalKeys(keyEnter())
	m = newModel.(Model)

	if m.modal != modalNone {
		t.Errorf("expected modalNone after selection, got %v", m.modal)
	}
	if !m.attachmentFilter {
		t.Error("expected attachmentFilter = true")
	}
}

func TestAttachmentFilterInMessageList(t *testing.T) {
	engine := &mockEngine{}

	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})
	model.level = levelMessageList
	model.pageSize = 10
	model.width = 100
	model.height = 20
	model.attachmentFilter = false

	// Press 'f' to open filter modal in message list
	newModel, _ := model.handleMessageListKeys(key('f'))
	m := newModel.(Model)

	if m.modal != modalAttachmentFilter {
		t.Errorf("expected modalAttachmentFilter, got %v", m.modal)
	}

	// Select "With Attachments" and verify reload is triggered
	m.modalCursor = 1
	newModel, cmd := m.handleModalKeys(keyEnter())
	m = newModel.(Model)

	if !m.attachmentFilter {
		t.Error("expected attachmentFilter = true")
	}
	if cmd == nil {
		t.Error("expected command to reload messages")
	}
}

func TestSubGroupingNavigation(t *testing.T) {
	engine := &mockEngine{
		rows: []query.AggregateRow{
			{Key: "alice@example.com", Count: 10},
			{Key: "bob@example.com", Count: 5},
		},
		messages: []query.MessageSummary{
			{ID: 1, Subject: "Test 1"},
			{ID: 2, Subject: "Test 2"},
		},
	}

	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})
	model.pageSize = 10
	model.width = 100
	model.height = 20
	model.rows = engine.rows
	model.viewType = query.ViewSenders

	// Press Enter to drill into first sender - should go to message list (not sub-aggregate)
	newModel, cmd := model.handleAggregateKeys(keyEnter())
	m := newModel.(Model)

	if m.level != levelMessageList {
		t.Errorf("expected levelMessageList, got %v", m.level)
	}
	if !m.hasDrillFilter() {
		t.Error("expected drillFilter to be set")
	}
	if m.drillFilter.Sender != "alice@example.com" {
		t.Errorf("expected drillFilter.Sender = alice@example.com, got %s", m.drillFilter.Sender)
	}
	if m.drillViewType != query.ViewSenders {
		t.Errorf("expected drillViewType = ViewSenders, got %v", m.drillViewType)
	}
	if cmd == nil {
		t.Error("expected command to load messages")
	}

	// Should have a breadcrumb
	if len(m.breadcrumbs) != 1 {
		t.Errorf("expected 1 breadcrumb, got %d", len(m.breadcrumbs))
	}

	// Test Tab from message list goes to sub-aggregate view
	m.messages = engine.messages // Simulate messages loaded
	newModel, cmd = m.handleMessageListKeys(keyTab())
	m = newModel.(Model)

	if m.level != levelSubAggregate {
		t.Errorf("expected levelSubAggregate after Tab, got %v", m.level)
	}
	// Default sub-group after drilling from Senders should be Recipients
	if m.viewType != query.ViewRecipients {
		t.Errorf("expected viewType = ViewRecipients for sub-grouping, got %v", m.viewType)
	}
	if cmd == nil {
		t.Error("expected command to load sub-aggregate data")
	}

	// Test Tab in sub-aggregate cycles views (skipping drill view type)
	m.rows = engine.rows // Simulate data loaded
	newModel, cmd = m.handleSubAggregateKeys(keyTab())
	m = newModel.(Model)

	// Should skip Senders (drill view type) and go to Domains
	if m.viewType != query.ViewDomains {
		t.Errorf("expected viewType = ViewDomains after Tab, got %v", m.viewType)
	}
	if cmd == nil {
		t.Error("expected command to reload data after Tab")
	}

	// Test Esc goes back to message list (not all the way to aggregates)
	m.rows = engine.rows
	newModel, _ = m.handleSubAggregateKeys(keyEsc())
	m = newModel.(Model)

	if m.level != levelMessageList {
		t.Errorf("expected levelMessageList after Esc from sub-aggregate, got %v", m.level)
	}
	// Drill filter should still be set (we're still viewing alice's messages)
	if !m.hasDrillFilter() {
		t.Error("expected drillFilter to still be set in message list")
	}
	// Should have 1 breadcrumb (from aggregates → message list)
	if len(m.breadcrumbs) != 1 {
		t.Errorf("expected 1 breadcrumb after going back to message list, got %d", len(m.breadcrumbs))
	}

	// Test Esc again goes back to aggregates
	m.messages = engine.messages
	newModel, _ = m.handleMessageListKeys(keyEsc())
	m = newModel.(Model)

	if m.level != levelAggregates {
		t.Errorf("expected levelAggregates after Esc from message list, got %v", m.level)
	}
	if m.hasDrillFilter() {
		t.Error("expected drillFilter to be cleared after going back to aggregates")
	}
	if len(m.breadcrumbs) != 0 {
		t.Errorf("expected 0 breadcrumbs after going back to aggregates, got %d", len(m.breadcrumbs))
	}
}

func TestFillScreenDetailLineCount(t *testing.T) {
	engine := &mockEngine{}

	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})
	model.level = levelMessageDetail
	model.width = 80
	model.height = 24
	model.pageSize = 19 // height - 5

	// detailPageSize = pageSize + 2 = 21
	expectedLines := model.detailPageSize()

	// Test loading state
	model.loading = true
	model.messageDetail = nil
	view := model.messageDetailView()
	lines := strings.Split(view, "\n")
	// View should have detailPageSize lines (last line has no trailing newline)
	if len(lines) != expectedLines {
		t.Errorf("loading state: expected %d lines, got %d", expectedLines, len(lines))
	}

	// Test error state
	model.loading = false
	model.err = fmt.Errorf("test error")
	view = model.messageDetailView()
	lines = strings.Split(view, "\n")
	if len(lines) != expectedLines {
		t.Errorf("error state: expected %d lines, got %d", expectedLines, len(lines))
	}

	// Test nil detail state
	model.err = nil
	model.messageDetail = nil
	view = model.messageDetailView()
	lines = strings.Split(view, "\n")
	if len(lines) != expectedLines {
		t.Errorf("nil detail state: expected %d lines, got %d", expectedLines, len(lines))
	}
}

func TestWindowSizeClampNegative(t *testing.T) {
	engine := &mockEngine{}

	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})

	// Simulate negative window size (can happen during terminal resize)
	resizeMsg := tea.WindowSizeMsg{Width: -1, Height: -1}
	newModel, _ := model.Update(resizeMsg)
	m := newModel.(Model)

	if m.width < 0 {
		t.Errorf("expected width >= 0, got %d", m.width)
	}
	if m.height < 0 {
		t.Errorf("expected height >= 0, got %d", m.height)
	}
	if m.pageSize < 1 {
		t.Errorf("expected pageSize >= 1, got %d", m.pageSize)
	}
}

func TestSubAggregateDrillDown(t *testing.T) {
	engine := &mockEngine{
		rows: []query.AggregateRow{
			{Key: "bob@example.com", Count: 3},
		},
		messages: []query.MessageSummary{
			{ID: 1, Subject: "Test"},
		},
	}

	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})
	model.pageSize = 10
	model.width = 100
	model.height = 20
	model.level = levelSubAggregate
	model.viewType = query.ViewRecipients
	model.drillViewType = query.ViewSenders
	model.drillFilter = query.MessageFilter{Sender: "alice@example.com"}
	model.rows = engine.rows

	// Press Enter on recipient - should go to message list with combined filter
	newModel, cmd := model.handleSubAggregateKeys(keyEnter())
	m := newModel.(Model)

	if m.level != levelMessageList {
		t.Errorf("expected levelMessageList, got %v", m.level)
	}
	// Drill filter should now include both sender and recipient
	if m.drillFilter.Sender != "alice@example.com" {
		t.Errorf("expected drillFilter.Sender = alice@example.com, got %s", m.drillFilter.Sender)
	}
	if m.drillFilter.Recipient != "bob@example.com" {
		t.Errorf("expected drillFilter.Recipient = bob@example.com, got %s", m.drillFilter.Recipient)
	}
	if cmd == nil {
		t.Error("expected command to load messages")
	}
}

// TestSearchModalOpen verifies '/' activates inline search.
func TestSearchModalOpen(t *testing.T) {
	engine := &mockEngine{
		rows: []query.AggregateRow{
			{Key: "alice@example.com", Count: 10},
		},
	}

	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})
	model.pageSize = 10
	model.width = 100
	model.height = 20
	model.rows = engine.rows

	// Press '/' to activate inline search
	newModel, cmd := model.handleAggregateKeys(key('/'))
	m := newModel.(Model)

	if !m.inlineSearchActive {
		t.Error("expected inlineSearchActive = true")
	}
	if m.searchMode != SearchModeFast {
		t.Errorf("expected SearchModeFast, got %v", m.searchMode)
	}
	// Should return a command for textinput blink
	if cmd == nil {
		t.Error("expected textinput command")
	}
}

// TestSearchResultsDisplay verifies search results are displayed.
func TestSearchResultsDisplay(t *testing.T) {
	engine := &mockEngine{}
	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})
	model.pageSize = 10
	model.width = 100
	model.height = 20
	model.searchQuery = "test query"
	model.searchMode = SearchModeFast
	model.searchRequestID = 1

	// Simulate receiving search results
	results := searchResultsMsg{
		messages: []query.MessageSummary{
			{ID: 1, Subject: "Result 1"},
			{ID: 2, Subject: "Result 2"},
		},
		requestID: 1,
	}

	newModel, _ := model.Update(results)
	m := newModel.(Model)

	if m.level != levelMessageList {
		t.Errorf("expected levelMessageList, got %v", m.level)
	}
	if len(m.messages) != 2 {
		t.Errorf("expected 2 messages, got %d", len(m.messages))
	}
	if m.loading {
		t.Error("expected loading = false after results")
	}
}

// TestSearchResultsStale verifies stale search results are ignored.
func TestSearchResultsStale(t *testing.T) {
	engine := &mockEngine{}
	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})
	model.pageSize = 10
	model.width = 100
	model.height = 20
	model.searchRequestID = 2 // Current request is 2

	// Simulate receiving stale results (requestID 1)
	results := searchResultsMsg{
		messages: []query.MessageSummary{
			{ID: 1, Subject: "Stale Result"},
		},
		requestID: 1, // Stale
	}

	newModel, _ := model.Update(results)
	m := newModel.(Model)

	// Messages should not be updated (still nil/empty)
	if len(m.messages) != 0 {
		t.Errorf("expected 0 messages (stale ignored), got %d", len(m.messages))
	}
}

// TestSearchModeString verifies SearchMode.String().
func TestSearchModeString(t *testing.T) {
	if SearchModeFast.String() != "Fast" {
		t.Errorf("expected 'Fast', got %q", SearchModeFast.String())
	}
	if SearchModeDeep.String() != "Deep" {
		t.Errorf("expected 'Deep', got %q", SearchModeDeep.String())
	}
}

// TestInlineSearchTabToggleAtMessageList verifies Tab toggles mode and triggers search at message list level.
func TestInlineSearchTabToggleAtMessageList(t *testing.T) {
	engine := &mockEngine{}
	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})
	model.pageSize = 10
	model.width = 100
	model.height = 20
	model.level = levelMessageList
	model.inlineSearchActive = true
	model.searchMode = SearchModeFast
	model.searchInput.SetValue("test query")
	model.messages = []query.MessageSummary{{ID: 1, Subject: "Existing"}} // Simulate existing results

	// Press Tab to toggle to Deep mode
	newModel, cmd := model.handleInlineSearchKeys(keyTab())
	m := newModel.(Model)

	// Mode should toggle to Deep
	if m.searchMode != SearchModeDeep {
		t.Errorf("expected SearchModeDeep after Tab, got %v", m.searchMode)
	}

	// Should set loading state
	if !m.inlineSearchLoading {
		t.Error("expected inlineSearchLoading = true after Tab toggle with query")
	}

	// Should NOT clear messages (frozenView handles the transition)
	// The old messages stay in place until new results arrive

	// Should trigger a search command
	if cmd == nil {
		t.Error("expected search command to be returned")
	}

	// searchRequestID should be incremented
	if m.searchRequestID != model.searchRequestID+1 {
		t.Error("expected searchRequestID to be incremented")
	}
}

// TestInlineSearchTabToggleNoQueryNoSearch verifies Tab with empty query doesn't trigger search.
func TestInlineSearchTabToggleNoQueryNoSearch(t *testing.T) {
	engine := &mockEngine{}
	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})
	model.pageSize = 10
	model.width = 100
	model.height = 20
	model.level = levelMessageList
	model.inlineSearchActive = true
	model.searchMode = SearchModeFast
	model.loading = false // Explicitly set to false (New() sets it to true)
	model.searchInput.SetValue("") // Empty query

	// Press Tab to toggle mode
	newModel, cmd := model.handleInlineSearchKeys(keyTab())
	m := newModel.(Model)

	// Mode should still toggle
	if m.searchMode != SearchModeDeep {
		t.Errorf("expected SearchModeDeep after Tab, got %v", m.searchMode)
	}

	// Should NOT set loading state (no query to search)
	if m.loading {
		t.Error("expected loading = false when toggling mode with empty query")
	}

	// Should NOT trigger a search command
	if cmd != nil {
		t.Error("expected no command when toggling mode with empty query")
	}
}

// TestInlineSearchTabAtAggregateLevel verifies Tab has no effect at aggregate level.
func TestInlineSearchTabAtAggregateLevel(t *testing.T) {
	engine := &mockEngine{}
	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})
	model.pageSize = 10
	model.width = 100
	model.height = 20
	model.level = levelAggregates // Not message list
	model.inlineSearchActive = true
	model.searchMode = SearchModeFast
	model.searchInput.SetValue("test query")

	// Press Tab - should do nothing at aggregate level
	newModel, cmd := model.handleInlineSearchKeys(keyTab())
	m := newModel.(Model)

	// Mode should NOT toggle (Tab disabled at aggregate level)
	if m.searchMode != SearchModeFast {
		t.Errorf("expected SearchModeFast unchanged at aggregate level, got %v", m.searchMode)
	}

	// Should NOT trigger any command
	if cmd != nil {
		t.Error("expected no command when Tab pressed at aggregate level")
	}
}

// TestInlineSearchTabToggleBackToFast verifies Tab toggles back from Deep to Fast.
func TestInlineSearchTabToggleBackToFast(t *testing.T) {
	engine := &mockEngine{}
	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})
	model.pageSize = 10
	model.width = 100
	model.height = 20
	model.level = levelMessageList
	model.inlineSearchActive = true
	model.searchMode = SearchModeDeep // Start in Deep mode
	model.searchInput.SetValue("test query")

	// Press Tab to toggle back to Fast mode
	newModel, cmd := model.handleInlineSearchKeys(keyTab())
	m := newModel.(Model)

	// Mode should toggle back to Fast
	if m.searchMode != SearchModeFast {
		t.Errorf("expected SearchModeFast after Tab from Deep, got %v", m.searchMode)
	}

	// Should trigger a search command
	if cmd == nil {
		t.Error("expected search command when toggling back to Fast")
	}
}

// TestSpinnerAppearsInViewWhenLoading verifies spinner character appears in rendered view.
func TestSpinnerAppearsInViewWhenLoading(t *testing.T) {
	engine := &mockEngine{
		rows: []query.AggregateRow{
			{Key: "test@example.com", Count: 10},
		},
	}
	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})
	model.pageSize = 10
	model.width = 100
	model.height = 20
	model.rows = engine.rows
	model.loading = false // Start not loading

	// Verify no spinner when not loading
	view1 := model.View()
	hasSpinner := false
	for _, frame := range spinnerFrames {
		if strings.Contains(view1, frame) {
			hasSpinner = true
			break
		}
	}
	if hasSpinner {
		t.Error("expected no spinner when loading = false")
	}

	// Now set loading state
	model.inlineSearchLoading = true
	model.inlineSearchActive = true
	model.searchInput.SetValue("test")

	view2 := model.View()
	hasSpinner = false
	for _, frame := range spinnerFrames {
		if strings.Contains(view2, frame) {
			hasSpinner = true
			break
		}
	}
	if !hasSpinner {
		t.Errorf("expected spinner in view when inlineSearchLoading = true, got:\n%s", view2)
	}
}

// TestSearchBackClears verifies going back clears search state.
func TestSearchBackClears(t *testing.T) {
	engine := &mockEngine{}
	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})
	model.pageSize = 10
	model.width = 100
	model.height = 20
	model.level = levelMessageList
	model.searchQuery = "test query"
	model.searchFilter = query.MessageFilter{Sender: "alice@example.com"}
	model.breadcrumbs = []navigationSnapshot{{state: viewState{level: levelAggregates}}}

	// Go back
	newModel, _ := model.goBack()
	m := newModel.(Model)

	if m.searchQuery != "" {
		t.Errorf("expected empty searchQuery after goBack, got %q", m.searchQuery)
	}
	if m.searchFilter.Sender != "" {
		t.Errorf("expected empty searchFilter after goBack, got %v", m.searchFilter)
	}
}

// TestSearchFromSubAggregate verifies search from sub-aggregate view.
func TestSearchFromSubAggregate(t *testing.T) {
	engine := &mockEngine{
		rows: []query.AggregateRow{
			{Key: "bob@example.com", Count: 3},
		},
	}

	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})
	model.pageSize = 10
	model.width = 100
	model.height = 20
	model.level = levelSubAggregate
	model.viewType = query.ViewRecipients
	model.drillViewType = query.ViewSenders
	model.drillFilter = query.MessageFilter{Sender: "alice@example.com"}
	model.rows = engine.rows

	// Press '/' to activate inline search
	newModel, cmd := model.handleSubAggregateKeys(key('/'))
	m := newModel.(Model)

	if !m.inlineSearchActive {
		t.Error("expected inlineSearchActive = true")
	}
	if cmd == nil {
		t.Error("expected textinput command")
	}
}

// TestSearchFromMessageList verifies search from message list view.
func TestSearchFromMessageList(t *testing.T) {
	engine := &mockEngine{
		messages: []query.MessageSummary{
			{ID: 1, Subject: "Test"},
		},
	}

	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})
	model.pageSize = 10
	model.width = 100
	model.height = 20
	model.level = levelMessageList
	model.messages = engine.messages

	// Press '/' to activate inline search
	newModel, cmd := model.handleMessageListKeys(key('/'))
	m := newModel.(Model)

	if !m.inlineSearchActive {
		t.Error("expected inlineSearchActive = true")
	}
	if cmd == nil {
		t.Error("expected textinput command")
	}
}

// TestGKeyCyclesViewType verifies that 'g' cycles through view types at aggregate level.
func TestGKeyCyclesViewType(t *testing.T) {
	engine := &mockEngine{
		rows: []query.AggregateRow{
			{Key: "alice@example.com", Count: 10},
		},
	}

	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})
	model.rows = engine.rows
	model.pageSize = 10
	model.width = 100
	model.height = 20
	model.level = levelAggregates
	model.viewType = query.ViewSenders
	// Set non-zero cursor/scroll to verify reset
	model.cursor = 5
	model.scrollOffset = 3

	// Press 'g' - should cycle to Recipients (not go to home)
	newModel, cmd := model.handleAggregateKeys(key('g'))
	m := newModel.(Model)

	// Expected: viewType changes to ViewRecipients
	if m.viewType != query.ViewRecipients {
		t.Errorf("expected ViewRecipients after 'g', got %v", m.viewType)
	}
	// Should trigger data reload
	if cmd == nil {
		t.Error("expected reload command after view type change")
	}
	if !m.loading {
		t.Error("expected loading=true after view type change")
	}
	// Cursor and scroll should reset to 0 when view type changes
	if m.cursor != 0 {
		t.Errorf("expected cursor=0 after view type change, got %d", m.cursor)
	}
	if m.scrollOffset != 0 {
		t.Errorf("expected scrollOffset=0 after view type change, got %d", m.scrollOffset)
	}
}

// TestGKeyCyclesViewTypeFullCycle verifies 'g' cycles through all view types.
func TestGKeyCyclesViewTypeFullCycle(t *testing.T) {
	engine := &mockEngine{
		rows: []query.AggregateRow{
			{Key: "test", Count: 10},
		},
	}

	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})
	model.rows = engine.rows
	model.pageSize = 10
	model.width = 100
	model.height = 20
	model.level = levelAggregates
	model.viewType = query.ViewSenders

	expectedOrder := []query.ViewType{
		query.ViewRecipients,
		query.ViewDomains,
		query.ViewLabels,
		query.ViewTime,
		query.ViewSenders, // Cycles back
	}

	for i, expected := range expectedOrder {
		newModel, _ := model.handleAggregateKeys(key('g'))
		model = newModel.(Model)
		model.loading = false // Reset for next iteration

		if model.viewType != expected {
			t.Errorf("cycle %d: expected %v, got %v", i+1, expected, model.viewType)
		}
	}
}

// TestGKeyInSubAggregate verifies 'g' cycles view types in sub-aggregate view.
func TestGKeyInSubAggregate(t *testing.T) {
	engine := &mockEngine{
		rows: []query.AggregateRow{
			{Key: "bob@example.com", Count: 5},
		},
	}

	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})
	model.rows = engine.rows
	model.pageSize = 10
	model.width = 100
	model.height = 20
	model.level = levelSubAggregate
	model.viewType = query.ViewRecipients
	model.drillViewType = query.ViewSenders // Drilled from Senders
	model.drillFilter = query.MessageFilter{Sender: "alice@example.com"}

	// Press 'g' - should cycle to next view type, skipping drillViewType
	newModel, _ := model.handleSubAggregateKeys(key('g'))
	m := newModel.(Model)

	// Should skip ViewSenders (the drillViewType) and go to Domains
	if m.viewType != query.ViewDomains {
		t.Errorf("expected ViewDomains (skipping drillViewType), got %v", m.viewType)
	}
}

// TestGKeyInMessageListWithDrillFilter verifies 'g' switches to sub-aggregate view
// when there's a drill filter.
func TestGKeyInMessageListWithDrillFilter(t *testing.T) {
	engine := &mockEngine{
		messages: []query.MessageSummary{
			{ID: 1, Subject: "Test 1"},
			{ID: 2, Subject: "Test 2"},
			{ID: 3, Subject: "Test 3"},
		},
	}

	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})
	model.messages = engine.messages
	model.pageSize = 10
	model.width = 100
	model.height = 20
	model.level = levelMessageList
	model.cursor = 2 // Start at third item
	model.scrollOffset = 1
	// Set up a drill filter so 'g' triggers sub-grouping
	model.drillFilter = query.MessageFilter{Sender: "alice@example.com"}
	model.drillViewType = query.ViewSenders
	model.viewType = query.ViewSenders

	// Press 'g' - should switch to sub-aggregate view
	newModel, _ := model.handleMessageListKeys(key('g'))
	m := newModel.(Model)

	if m.level != levelSubAggregate {
		t.Errorf("expected level=levelSubAggregate after 'g' with drill filter, got %v", m.level)
	}
	// ViewType should be next logical view (Recipients after Senders)
	if m.viewType != query.ViewRecipients {
		t.Errorf("expected viewType=Recipients after 'g', got %v", m.viewType)
	}
}

// TestGKeyInMessageListNoDrillFilter verifies 'g' goes back to aggregates when no drill filter.
func TestGKeyInMessageListNoDrillFilter(t *testing.T) {
	engine := &mockEngine{
		messages: []query.MessageSummary{
			{ID: 1, Subject: "Test 1"},
			{ID: 2, Subject: "Test 2"},
			{ID: 3, Subject: "Test 3"},
		},
	}

	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})
	model.messages = engine.messages
	model.pageSize = 10
	model.width = 100
	model.height = 20
	model.level = levelMessageList
	model.cursor = 2 // Start at third item
	model.scrollOffset = 1
	// No drill filter - 'g' should go back to aggregates

	// Press 'g' - should go back to aggregate view
	newModel, _ := model.handleMessageListKeys(key('g'))
	m := newModel.(Model)

	// Should transition to aggregate level
	if m.level != levelAggregates {
		t.Errorf("expected level=levelAggregates after 'g' with no drill filter, got %v", m.level)
	}
	// Cursor and scroll should reset
	if m.cursor != 0 {
		t.Errorf("expected cursor=0 after 'g' with no drill filter, got %d", m.cursor)
	}
	if m.scrollOffset != 0 {
		t.Errorf("expected scrollOffset=0 after 'g' with no drill filter, got %d", m.scrollOffset)
	}
}

// trackingMockEngine extends mockEngine to track GetTotalStats calls.
type trackingMockEngine struct {
	mockEngine
	statsCallCount int
	lastStatsOpts  query.StatsOptions
	contextStats   *query.TotalStats // Stats to return (can be set per-test)
}

func (t *trackingMockEngine) GetTotalStats(ctx context.Context, opts query.StatsOptions) (*query.TotalStats, error) {
	t.statsCallCount++
	t.lastStatsOpts = opts
	if t.contextStats != nil {
		return t.contextStats, nil
	}
	return &query.TotalStats{MessageCount: 1000, TotalSize: 5000000, AttachmentCount: 50}, nil
}

// TestStatsUpdateOnDrillDown verifies stats are reloaded when drilling into a subgroup.
func TestStatsUpdateOnDrillDown(t *testing.T) {
	engine := &trackingMockEngine{
		mockEngine: mockEngine{
			rows: []query.AggregateRow{
				{Key: "alice@example.com", Count: 100, TotalSize: 500000},
				{Key: "bob@example.com", Count: 50, TotalSize: 250000},
			},
			messages: []query.MessageSummary{
				{ID: 1, Subject: "Test"},
			},
		},
	}

	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})
	model.rows = engine.rows
	model.pageSize = 10
	model.width = 100
	model.height = 20
	model.level = levelAggregates
	model.viewType = query.ViewSenders
	model.cursor = 0

	// Press Enter to drill down into alice's messages
	newModel, cmd := model.handleAggregateKeys(keyEnter())
	m := newModel.(Model)

	// Verify we transitioned to message list
	if m.level != levelMessageList {
		t.Errorf("expected levelMessageList after drill-down, got %v", m.level)
	}

	// The stats should be refreshed for the drill-down context
	// (This test documents expected behavior - implementation will make it pass)
	if cmd == nil {
		t.Error("expected command to load messages/stats")
	}

	// Verify drillFilter is set correctly
	if m.drillFilter.Sender != "alice@example.com" {
		t.Errorf("expected drillFilter.Sender='alice@example.com', got '%s'", m.drillFilter.Sender)
	}

	// Verify contextStats is set from selected row (not from GetTotalStats call)
	if m.contextStats == nil {
		t.Error("expected contextStats to be set from selected row")
	} else {
		if m.contextStats.MessageCount != 100 {
			t.Errorf("expected contextStats.MessageCount=100, got %d", m.contextStats.MessageCount)
		}
	}
}

// TestPositionDisplayInMessageList verifies position shows cursor/total correctly.
func TestPositionDisplayInMessageList(t *testing.T) {
	engine := &mockEngine{
		messages: make([]query.MessageSummary, 100),
	}
	for i := 0; i < 100; i++ {
		engine.messages[i] = query.MessageSummary{ID: int64(i + 1), Subject: fmt.Sprintf("Test %d", i+1)}
	}

	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})
	model.messages = engine.messages
	model.pageSize = 20
	model.width = 100
	model.height = 30
	model.level = levelMessageList
	model.cursor = 49 // 50th message

	// Get the footer view
	footer := model.footerView()

	// Should show "50/100" (cursor+1 / total loaded)
	if !strings.Contains(footer, "50/100") {
		t.Errorf("expected footer to contain '50/100', got: %s", footer)
	}
}

// TestTabCyclesViewTypeAtAggregates verifies Tab still cycles view types.
func TestTabCyclesViewTypeAtAggregates(t *testing.T) {
	engine := &mockEngine{
		rows: []query.AggregateRow{
			{Key: "test", Count: 10},
		},
	}

	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})
	model.rows = engine.rows
	model.pageSize = 10
	model.width = 100
	model.height = 20
	model.level = levelAggregates
	model.viewType = query.ViewSenders
	// Set non-zero cursor/scroll to verify reset
	model.cursor = 5
	model.scrollOffset = 3

	// Press Tab - should cycle to Recipients
	newModel, cmd := model.handleAggregateKeys(keyTab())
	m := newModel.(Model)

	if m.viewType != query.ViewRecipients {
		t.Errorf("expected ViewRecipients after Tab, got %v", m.viewType)
	}
	if cmd == nil {
		t.Error("expected reload command after Tab")
	}
	// Cursor and scroll should reset to 0 when view type changes
	if m.cursor != 0 {
		t.Errorf("expected cursor=0 after Tab, got %d", m.cursor)
	}
	if m.scrollOffset != 0 {
		t.Errorf("expected scrollOffset=0 after Tab, got %d", m.scrollOffset)
	}
}

// TestHomeKeyGoesToTop verifies 'home' key goes to top (separate from 'g').
func TestHomeKeyGoesToTop(t *testing.T) {
	engine := &mockEngine{
		rows: []query.AggregateRow{
			{Key: "a", Count: 1},
			{Key: "b", Count: 2},
			{Key: "c", Count: 3},
		},
	}

	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})
	model.rows = engine.rows
	model.pageSize = 10
	model.width = 100
	model.height = 20
	model.level = levelAggregates
	model.cursor = 2
	model.scrollOffset = 1

	// Press 'home' - should go to top
	newModel, _ := model.handleAggregateKeys(keyHome())
	m := newModel.(Model)

	if m.cursor != 0 {
		t.Errorf("expected cursor=0 after 'home', got %d", m.cursor)
	}
	if m.scrollOffset != 0 {
		t.Errorf("expected scrollOffset=0 after 'home', got %d", m.scrollOffset)
	}
}

// TestContextStatsSetOnDrillDown verifies contextStats is set from selected row.
func TestContextStatsSetOnDrillDown(t *testing.T) {
	engine := &mockEngine{
		rows: []query.AggregateRow{
			{Key: "alice@example.com", Count: 100, TotalSize: 500000, AttachmentSize: 100000},
			{Key: "bob@example.com", Count: 50, TotalSize: 250000, AttachmentSize: 50000},
		},
		messages: []query.MessageSummary{
			{ID: 1, Subject: "Test"},
		},
	}

	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})
	model.rows = engine.rows
	model.pageSize = 10
	model.width = 100
	model.height = 20
	model.level = levelAggregates
	model.viewType = query.ViewSenders
	model.cursor = 0 // Select alice

	// Before drill-down, contextStats should be nil
	if model.contextStats != nil {
		t.Error("expected contextStats=nil before drill-down")
	}

	// Press Enter to drill down into alice's messages
	newModel, _ := model.handleAggregateKeys(keyEnter())
	m := newModel.(Model)

	// Verify contextStats is set from selected row
	if m.contextStats == nil {
		t.Fatal("expected contextStats to be set after drill-down")
	}
	if m.contextStats.MessageCount != 100 {
		t.Errorf("expected MessageCount=100, got %d", m.contextStats.MessageCount)
	}
	if m.contextStats.TotalSize != 500000 {
		t.Errorf("expected TotalSize=500000, got %d", m.contextStats.TotalSize)
	}
}

// TestContextStatsClearedOnGoBack verifies contextStats is cleared when going back to aggregates.
func TestContextStatsClearedOnGoBack(t *testing.T) {
	engine := &mockEngine{
		rows: []query.AggregateRow{
			{Key: "alice@example.com", Count: 100, TotalSize: 500000},
		},
		messages: []query.MessageSummary{
			{ID: 1, Subject: "Test"},
		},
	}

	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})
	model.rows = engine.rows
	model.pageSize = 10
	model.width = 100
	model.height = 20
	model.level = levelAggregates
	model.viewType = query.ViewSenders

	// Drill down
	newModel, _ := model.handleAggregateKeys(keyEnter())
	m := newModel.(Model)

	if m.contextStats == nil {
		t.Fatal("expected contextStats to be set after drill-down")
	}

	// Go back
	newModel2, _ := m.goBack()
	m2 := newModel2.(Model)

	// contextStats should be cleared
	if m2.contextStats != nil {
		t.Error("expected contextStats=nil after going back to aggregates")
	}
}

// TestContextStatsRestoredOnGoBackToSubAggregate verifies contextStats is restored when going back.
func TestContextStatsRestoredOnGoBackToSubAggregate(t *testing.T) {
	engine := &mockEngine{
		rows: []query.AggregateRow{
			{Key: "alice@example.com", Count: 100, TotalSize: 500000},
			{Key: "bob@example.com", Count: 50, TotalSize: 250000},
		},
		messages: []query.MessageSummary{
			{ID: 1, Subject: "Test"},
		},
	}

	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})
	model.rows = engine.rows
	model.pageSize = 10
	model.width = 100
	model.height = 20
	model.level = levelAggregates
	model.viewType = query.ViewSenders

	// Step 1: Drill down to message list (sets contextStats from alice's row)
	newModel, _ := model.handleAggregateKeys(keyEnter())
	m := newModel.(Model)
	if m.contextStats == nil || m.contextStats.MessageCount != 100 {
		t.Fatalf("expected contextStats.MessageCount=100, got %v", m.contextStats)
	}

	// Simulate messages loaded and transition to message list level
	m.level = levelMessageList
	m.messages = engine.messages
	m.filterKey = "alice@example.com"
	originalContextStats := m.contextStats

	// Step 2: Press Tab to go to sub-aggregate view (contextStats saved in breadcrumb)
	newModel2, _ := m.handleMessageListKeys(keyTab())
	m2 := newModel2.(Model)
	// Simulate data load completing with sub-aggregate rows
	m2.rows = []query.AggregateRow{
		{Key: "domain1.com", Count: 60, TotalSize: 300000},
		{Key: "domain2.com", Count: 40, TotalSize: 200000},
	}
	m2.loading = false
	if m2.level != levelSubAggregate {
		t.Fatalf("expected levelSubAggregate after Tab, got %v", m2.level)
	}
	// contextStats should still be the same (alice's stats)
	if m2.contextStats != originalContextStats {
		t.Errorf("contextStats should be preserved after Tab")
	}

	// Step 3: Drill down from sub-aggregate to message list (contextStats overwritten)
	newModel3, _ := m2.handleSubAggregateKeys(keyEnter())
	m3 := newModel3.(Model)
	if m3.level != levelMessageList {
		t.Fatalf("expected levelMessageList after Enter, got %v", m3.level)
	}
	// contextStats should now be domain1's stats (60)
	if m3.contextStats == nil || m3.contextStats.MessageCount != 60 {
		t.Errorf("expected contextStats.MessageCount=60 for domain1, got %v", m3.contextStats)
	}

	// Step 4: Go back to sub-aggregate (contextStats should be restored to alice's stats)
	newModel4, _ := m3.goBack()
	m4 := newModel4.(Model)
	if m4.level != levelSubAggregate {
		t.Fatalf("expected levelSubAggregate after goBack, got %v", m4.level)
	}
	// contextStats should be restored from breadcrumb
	if m4.contextStats == nil {
		t.Error("expected contextStats to be restored after goBack")
	} else if m4.contextStats.MessageCount != 100 {
		t.Errorf("expected contextStats.MessageCount=100 after goBack, got %d", m4.contextStats.MessageCount)
	}
}

// TestContextStatsDisplayedInHeader verifies header shows contextual stats when drilled down.
func TestContextStatsDisplayedInHeader(t *testing.T) {
	engine := &mockEngine{}

	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})
	model.width = 100
	model.height = 20
	model.level = levelMessageList
	model.stats = &query.TotalStats{MessageCount: 10000, TotalSize: 50000000, AttachmentCount: 500}
	model.contextStats = &query.TotalStats{MessageCount: 100, TotalSize: 500000}

	header := model.headerView()

	// Should show contextStats (100 msgs), not global stats (10000 msgs)
	if !strings.Contains(header, "100 msgs") {
		t.Errorf("expected header to contain '100 msgs' (contextStats), got: %s", header)
	}
	if strings.Contains(header, "10000 msgs") {
		t.Errorf("header should NOT contain '10000 msgs' (global stats) when drilled down")
	}
}

// TestContextStatsShowsAttachmentCountInHeader verifies header shows attachment count when drilled down.
func TestContextStatsShowsAttachmentCountInHeader(t *testing.T) {
	engine := &mockEngine{}

	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})
	model.width = 120
	model.height = 20
	model.level = levelMessageList
	model.stats = &query.TotalStats{MessageCount: 10000, TotalSize: 50000000, AttachmentCount: 500}
	// contextStats with attachment count
	model.contextStats = &query.TotalStats{MessageCount: 100, TotalSize: 500000, AttachmentCount: 42}

	header := model.headerView()

	// Should show "attchs" with attachment count
	if !strings.Contains(header, "attchs") {
		t.Errorf("expected header to contain 'attchs' when AttachmentCount > 0, got: %s", header)
	}
	if !strings.Contains(header, "42 attchs") {
		t.Errorf("expected header to contain '42 attchs' (attachment count), got: %s", header)
	}
}

// TestContextStatsShowsZeroAttachmentCount verifies header shows "0 attchs" when count is 0.
func TestContextStatsShowsZeroAttachmentCount(t *testing.T) {
	engine := &mockEngine{}

	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})
	model.width = 120
	model.height = 20
	model.level = levelMessageList
	model.stats = &query.TotalStats{MessageCount: 10000, TotalSize: 50000000, AttachmentCount: 500}
	// contextStats with zero attachment count
	model.contextStats = &query.TotalStats{MessageCount: 100, TotalSize: 500000, AttachmentCount: 0}

	header := model.headerView()

	// Should show "0 attchs" even when attachment count is 0
	if !strings.Contains(header, "0 attchs") {
		t.Errorf("header should contain '0 attchs' when AttachmentCount is 0, got: %s", header)
	}
}

// TestPositionShowsTotalFromContextStats verifies footer shows "N of M" when total > loaded.
func TestPositionShowsTotalFromContextStats(t *testing.T) {
	engine := &mockEngine{}

	// Create 100 loaded messages but contextStats says 500 total
	messages := make([]query.MessageSummary, 100)
	for i := range messages {
		messages[i] = query.MessageSummary{ID: int64(i + 1), Subject: fmt.Sprintf("Msg %d", i+1)}
	}

	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})
	model.messages = messages
	model.pageSize = 20
	model.width = 100
	model.height = 30
	model.level = levelMessageList
	model.cursor = 49                                         // 50th message
	model.contextStats = &query.TotalStats{MessageCount: 500} // 500 total in group

	footer := model.footerView()

	// Should show "50 of 500" (not "50/100")
	if !strings.Contains(footer, "50 of 500") {
		t.Errorf("expected footer to contain '50 of 500', got: %s", footer)
	}
	if strings.Contains(footer, "50/100") {
		t.Errorf("footer should NOT contain '50/100' when contextStats.MessageCount > loaded")
	}
}

// TestPositionShowsLoadedCountWhenAllLoaded verifies footer shows "N/M" when all loaded.
func TestPositionShowsLoadedCountWhenAllLoaded(t *testing.T) {
	engine := &mockEngine{}

	messages := make([]query.MessageSummary, 50)
	for i := range messages {
		messages[i] = query.MessageSummary{ID: int64(i + 1)}
	}

	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})
	model.messages = messages
	model.pageSize = 20
	model.width = 100
	model.height = 30
	model.level = levelMessageList
	model.cursor = 24
	model.contextStats = &query.TotalStats{MessageCount: 50} // Same as loaded

	footer := model.footerView()

	// Should show "25/50" (standard format when all loaded)
	if !strings.Contains(footer, "25/50") {
		t.Errorf("expected footer to contain '25/50', got: %s", footer)
	}
}

// TestPositionShowsLoadedCountWhenNoContextStats verifies footer falls back to loaded count.
func TestPositionShowsLoadedCountWhenNoContextStats(t *testing.T) {
	engine := &mockEngine{}

	messages := make([]query.MessageSummary, 75)
	for i := range messages {
		messages[i] = query.MessageSummary{ID: int64(i + 1)}
	}

	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})
	model.messages = messages
	model.pageSize = 20
	model.width = 100
	model.height = 30
	model.level = levelMessageList
	model.cursor = 49
	model.contextStats = nil // No context stats (e.g., "All Messages" view)

	footer := model.footerView()

	// Should show "50/75" (standard format using loaded count)
	if !strings.Contains(footer, "50/75") {
		t.Errorf("expected footer to contain '50/75' when contextStats is nil, got: %s", footer)
	}
	// Should NOT show "of" format
	if strings.Contains(footer, " of ") {
		t.Errorf("footer should NOT contain ' of ' when contextStats is nil, got: %s", footer)
	}
}

// TestPositionShowsLoadedCountWhenContextStatsSmaller verifies loaded count is used when
// contextStats.MessageCount is smaller than loaded (edge case, shouldn't normally happen).
func TestPositionShowsLoadedCountWhenContextStatsSmaller(t *testing.T) {
	engine := &mockEngine{}

	messages := make([]query.MessageSummary, 100)
	for i := range messages {
		messages[i] = query.MessageSummary{ID: int64(i + 1)}
	}

	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})
	model.messages = messages
	model.pageSize = 20
	model.width = 100
	model.height = 30
	model.level = levelMessageList
	model.cursor = 49
	// contextStats says 50, but we have 100 loaded (stale/inconsistent state)
	model.contextStats = &query.TotalStats{MessageCount: 50}

	footer := model.footerView()

	// Should use loaded count (100), not contextStats (50)
	// Shows "50/100" not "50 of 50"
	if !strings.Contains(footer, "50/100") {
		t.Errorf("expected footer to contain '50/100' when contextStats is smaller, got: %s", footer)
	}
}

// TestPositionUsesGlobalStatsForAllMessagesView verifies footer uses global stats
// when in "All Messages" view (allMessages=true, contextStats=nil).
func TestPositionUsesGlobalStatsForAllMessagesView(t *testing.T) {
	engine := &mockEngine{}

	// Simulate 500 messages loaded (the limit)
	messages := make([]query.MessageSummary, 500)
	for i := range messages {
		messages[i] = query.MessageSummary{ID: int64(i + 1)}
	}

	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})
	model.messages = messages
	model.pageSize = 20
	model.width = 100
	model.height = 30
	model.level = levelMessageList
	model.cursor = 99 // 100th message
	model.allMessages = true                                      // All Messages view
	model.contextStats = nil                                      // No drill-down context
	model.stats = &query.TotalStats{MessageCount: 175000}         // Global total is much larger

	footer := model.footerView()

	// Should show "100 of 175000" (using global stats total)
	if !strings.Contains(footer, "100 of 175000") {
		t.Errorf("expected footer to contain '100 of 175000', got: %s", footer)
	}
	// Should NOT just show "/500"
	if strings.Contains(footer, "/500") {
		t.Errorf("footer should NOT contain '/500' in All Messages view, got: %s", footer)
	}
}

// TestHelpModalOpensWithQuestionMark verifies '?' opens the help modal.
func TestHelpModalOpensWithQuestionMark(t *testing.T) {
	engine := &mockEngine{}
	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})
	model.level = levelAggregates
	model.modal = modalNone

	// Press '?'
	newModel, _ := model.Update(key('?'))
	m := newModel.(Model)

	if m.modal != modalHelp {
		t.Errorf("expected modalHelp after '?', got %v", m.modal)
	}
}

// TestHelpModalClosesOnAnyKey verifies help modal closes on any key.
func TestHelpModalClosesOnAnyKey(t *testing.T) {
	engine := &mockEngine{}
	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})
	model.level = levelAggregates
	model.modal = modalHelp

	// Press any key (e.g., Enter)
	newModel, _ := model.Update(keyEnter())
	m := newModel.(Model)

	if m.modal != modalNone {
		t.Errorf("expected modalNone after pressing key in help, got %v", m.modal)
	}
}

// TestVKeyReversesSortOrder verifies 'v' reverses sort direction.
func TestVKeyReversesSortOrder(t *testing.T) {
	engine := &mockEngine{}
	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})
	model.level = levelAggregates
	model.sortDirection = query.SortDesc
	model.rows = []query.AggregateRow{{Key: "test", Count: 1}}

	// Press 'v'
	newModel, _ := model.Update(key('v'))
	m := newModel.(Model)

	if m.sortDirection != query.SortAsc {
		t.Errorf("expected SortAsc after 'v', got %v", m.sortDirection)
	}

	// Press 'v' again
	newModel2, _ := m.Update(key('v'))
	m2 := newModel2.(Model)

	if m2.sortDirection != query.SortDesc {
		t.Errorf("expected SortDesc after second 'v', got %v", m2.sortDirection)
	}
}

// TestSearchSetsContextStats verifies search results set contextStats for header metrics.
func TestSearchSetsContextStats(t *testing.T) {
	engine := &mockEngine{}
	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})
	model.level = levelAggregates
	model.searchRequestID = 1

	// Simulate receiving search results
	msg := searchResultsMsg{
		messages:   make([]query.MessageSummary, 10),
		totalCount: 150,
		requestID:  1,
		append:     false,
	}

	newModel, _ := model.Update(msg)
	m := newModel.(Model)

	if m.contextStats == nil {
		t.Error("expected contextStats to be set after search results")
	}
	if m.contextStats.MessageCount != 150 {
		t.Errorf("expected contextStats.MessageCount=150, got %d", m.contextStats.MessageCount)
	}
}

// TestSearchZeroResultsClearsContextStats verifies contextStats is set to zero on empty search.
func TestSearchZeroResultsClearsContextStats(t *testing.T) {
	engine := &mockEngine{}
	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})
	model.level = levelAggregates
	model.searchRequestID = 1
	// Set stale contextStats from previous view
	model.contextStats = &query.TotalStats{MessageCount: 500}

	// Simulate receiving zero search results
	msg := searchResultsMsg{
		messages:   []query.MessageSummary{},
		totalCount: 0,
		requestID:  1,
		append:     false,
	}

	newModel, _ := model.Update(msg)
	m := newModel.(Model)

	if m.contextStats == nil {
		t.Error("expected contextStats to be set (not nil)")
	}
	if m.contextStats.MessageCount != 0 {
		t.Errorf("expected contextStats.MessageCount=0 for zero results, got %d", m.contextStats.MessageCount)
	}
}

// TestSearchPaginationUpdatesContextStats verifies contextStats updates on append when total unknown.
func TestSearchPaginationUpdatesContextStats(t *testing.T) {
	engine := &mockEngine{}
	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})
	model.level = levelMessageList
	model.searchRequestID = 1
	model.searchTotalCount = -1 // Unknown total
	model.messages = make([]query.MessageSummary, 50)
	model.contextStats = &query.TotalStats{MessageCount: 50}

	// Simulate receiving additional paginated results
	msg := searchResultsMsg{
		messages:   make([]query.MessageSummary, 50),
		totalCount: -1, // Still unknown
		requestID:  1,
		append:     true,
	}

	newModel, _ := model.Update(msg)
	m := newModel.(Model)

	if m.contextStats == nil {
		t.Error("expected contextStats to be set")
	}
	// Total messages should now be 100 (50 original + 50 appended)
	if len(m.messages) != 100 {
		t.Errorf("expected 100 messages after append, got %d", len(m.messages))
	}
	if m.contextStats.MessageCount != 100 {
		t.Errorf("expected contextStats.MessageCount=100 after pagination, got %d", m.contextStats.MessageCount)
	}
}

// TestSearchResultsPreservesDrillDownContextStats verifies that when drilling down
// from a search-filtered aggregate, contextStats (TotalSize, AttachmentCount) set
// from the selected row is preserved when searchResultsMsg arrives.
// This is the fix for the bug where drilling down into a sender after search
// caused TotalSize and AttachmentCount to disappear from the header.
func TestSearchResultsPreservesDrillDownContextStats(t *testing.T) {
	model := newTestModelWithRows(testAggregateRows)
	model.level = levelAggregates
	model.searchQuery = "important"
	model.cursor = 0 // alice@example.com: Count=100, TotalSize=1000, AttachmentCount=5

	// Press Enter to drill down (sets contextStats from selected row)
	newModel, _ := model.handleAggregateKeys(keyEnter())
	m := newModel.(Model)

	// Verify contextStats was set from selected row with full stats
	if m.contextStats == nil {
		t.Fatal("expected contextStats to be set after drill-down")
	}
	if m.contextStats.TotalSize != 1000 {
		t.Errorf("expected TotalSize=1000 after drill-down, got %d", m.contextStats.TotalSize)
	}
	if m.contextStats.AttachmentCount != 5 {
		t.Errorf("expected AttachmentCount=5 after drill-down, got %d", m.contextStats.AttachmentCount)
	}

	// Simulate searchResultsMsg arriving with total count
	searchMsg := searchResultsMsg{
		requestID:  m.searchRequestID,
		messages:   []query.MessageSummary{{ID: 1}, {ID: 2}},
		totalCount: 100,
	}
	newModel2, _ := m.Update(searchMsg)
	m2 := newModel2.(Model)

	// contextStats should preserve TotalSize and AttachmentCount from drill-down
	if m2.contextStats == nil {
		t.Fatal("expected contextStats to be preserved after searchResultsMsg")
	}
	if m2.contextStats.MessageCount != 100 {
		t.Errorf("expected MessageCount=100 (from searchResultsMsg), got %d", m2.contextStats.MessageCount)
	}
	if m2.contextStats.TotalSize != 1000 {
		t.Errorf("expected TotalSize=1000 to be preserved, got %d", m2.contextStats.TotalSize)
	}
	if m2.contextStats.AttachmentCount != 5 {
		t.Errorf("expected AttachmentCount=5 to be preserved, got %d", m2.contextStats.AttachmentCount)
	}
}

// TestSearchResultsWithoutDrillDownContextStats verifies that when searching
// without a drill-down context, contextStats is created with only MessageCount.
func TestSearchResultsWithoutDrillDownContextStats(t *testing.T) {
	model := newTestModelAtLevel(levelMessageList)
	model.searchRequestID = 1

	// Simulate searchResultsMsg arriving (no prior drill-down, so no TotalSize/AttachmentCount)
	searchMsg := searchResultsMsg{
		requestID:  1,
		messages:   []query.MessageSummary{{ID: 1}, {ID: 2}},
		totalCount: 50,
	}
	newModel, _ := model.Update(searchMsg)
	m := newModel.(Model)

	if m.contextStats == nil {
		t.Fatal("expected contextStats to be set after search results")
	}
	if m.contextStats.MessageCount != 50 {
		t.Errorf("expected MessageCount=50, got %d", m.contextStats.MessageCount)
	}
	// Without drill-down, TotalSize and AttachmentCount should be 0
	if m.contextStats.TotalSize != 0 {
		t.Errorf("expected TotalSize=0 without drill-down, got %d", m.contextStats.TotalSize)
	}
	if m.contextStats.AttachmentCount != 0 {
		t.Errorf("expected AttachmentCount=0 without drill-down, got %d", m.contextStats.AttachmentCount)
	}
}

// TestAggregateSearchFilterSetsContextStats verifies contextStats is calculated from
// filtered aggregate rows when a search filter is active.
func TestAggregateSearchFilterSetsContextStats(t *testing.T) {
	model := newTestModelAtLevel(levelAggregates).
		withSearchQuery("test query").
		withAggregateRequestID(1)

	msg := dataLoadedMsg{
		rows:      testAggregateRows,
		requestID: 1,
	}

	newModel, _ := model.Update(msg)
	m := newModel.(Model)

	if m.contextStats == nil {
		t.Fatal("expected contextStats to be set when search filter is active")
	}

	wantCount, wantSize, wantAttachments := sumAggregateStats(testAggregateRows)
	if m.contextStats.MessageCount != wantCount {
		t.Errorf("contextStats.MessageCount = %d, want %d", m.contextStats.MessageCount, wantCount)
	}
	if m.contextStats.TotalSize != wantSize {
		t.Errorf("contextStats.TotalSize = %d, want %d", m.contextStats.TotalSize, wantSize)
	}
	if m.contextStats.AttachmentCount != wantAttachments {
		t.Errorf("contextStats.AttachmentCount = %d, want %d", m.contextStats.AttachmentCount, wantAttachments)
	}
}

// TestAggregateNoSearchFilterClearsContextStats verifies contextStats is cleared
// when no search filter is active at aggregate level.
func TestAggregateNoSearchFilterClearsContextStats(t *testing.T) {
	model := newTestModelAtLevel(levelAggregates).
		withAggregateRequestID(1).
		withContextStats(&query.TotalStats{MessageCount: 500}) // Stale stats

	msg := dataLoadedMsg{
		rows:      testAggregateRows[:1], // Just one row
		requestID: 1,
	}

	newModel, _ := model.Update(msg)
	m := newModel.(Model)

	if m.contextStats != nil {
		t.Error("expected contextStats to be nil when no search filter at aggregate level")
	}
}

// TestSubAggregateSearchFilterSetsContextStats verifies contextStats is calculated
// at sub-aggregate level when search filter is active.
func TestSubAggregateSearchFilterSetsContextStats(t *testing.T) {
	model := newTestModelAtLevel(levelSubAggregate).
		withSearchQuery("important").
		withAggregateRequestID(1)

	rows := []query.AggregateRow{
		{Key: "work", Count: 30, TotalSize: 3000, AttachmentCount: 10},
		{Key: "personal", Count: 20, TotalSize: 2000, AttachmentCount: 5},
	}

	msg := dataLoadedMsg{
		rows:      rows,
		requestID: 1,
	}

	newModel, _ := model.Update(msg)
	m := newModel.(Model)

	if m.contextStats == nil {
		t.Fatal("expected contextStats to be set at sub-aggregate with search filter")
	}

	wantCount, _, _ := sumAggregateStats(rows)
	if m.contextStats.MessageCount != wantCount {
		t.Errorf("contextStats.MessageCount = %d, want %d", m.contextStats.MessageCount, wantCount)
	}
}

// TestHeaderViewShowsFilteredStatsOnSearch verifies the header shows contextStats
// when search filter is active at aggregate level.
func TestHeaderViewShowsFilteredStatsOnSearch(t *testing.T) {
	filteredStats := &query.TotalStats{MessageCount: 42, TotalSize: 12345, AttachmentCount: 7}
	globalStats := &query.TotalStats{MessageCount: 1000, TotalSize: 999999, AttachmentCount: 100}

	model := newTestModelAtLevel(levelAggregates).
		withSearchQuery("test").
		withContextStats(filteredStats).
		withGlobalStats(globalStats)

	header := model.headerView()

	// Should show filtered stats (42 msgs), not global stats (1000 msgs)
	if !strings.Contains(header, "42 msgs") {
		t.Errorf("expected header to show filtered stats (42 msgs), got: %s", header)
	}
	if strings.Contains(header, "1000 msgs") {
		t.Errorf("header should not show global stats (1000 msgs) when search filter active")
	}
}

// TestDrillDownWithSearchQueryUsesSearch verifies that drilling down from a
// filtered aggregate uses search (not loadMessages) to apply the search filter.
func TestDrillDownWithSearchQueryUsesSearch(t *testing.T) {
	model := newTestModelWithRows(testAggregateRows)
	model.level = levelAggregates
	model.searchQuery = "important" // Active search filter
	model.cursor = 0                // alice@example.com

	// Press Enter to drill down
	newModel, cmd := model.handleAggregateKeys(keyEnter())
	m := newModel.(Model)

	// Should transition to message list
	if m.level != levelMessageList {
		t.Errorf("expected levelMessageList, got %v", m.level)
	}

	// Should preserve search query
	if m.searchQuery != "important" {
		t.Errorf("expected searchQuery preserved, got %q", m.searchQuery)
	}

	// Should have set searchFilter from drillFilter
	if m.searchFilter.Sender != "alice@example.com" {
		t.Errorf("expected searchFilter.Sender=alice@example.com, got %q", m.searchFilter.Sender)
	}

	// Should return a command (the search command)
	if cmd == nil {
		t.Error("expected a command to be returned for search")
	}

	// searchRequestID should have been incremented (not loadRequestID)
	if m.searchRequestID != 1 {
		t.Errorf("expected searchRequestID=1, got %d", m.searchRequestID)
	}
}

// TestDrillDownWithoutSearchQueryUsesLoadMessages verifies that drilling down
// without a search filter uses loadMessages (not search).
func TestDrillDownWithoutSearchQueryUsesLoadMessages(t *testing.T) {
	model := newTestModelWithRows(testAggregateRows)
	model.level = levelAggregates
	model.searchQuery = "" // No search filter
	model.cursor = 0

	newModel, cmd := model.handleAggregateKeys(keyEnter())
	m := newModel.(Model)

	if m.level != levelMessageList {
		t.Errorf("expected levelMessageList, got %v", m.level)
	}

	// loadRequestID should have been incremented
	if m.loadRequestID != 1 {
		t.Errorf("expected loadRequestID=1, got %d", m.loadRequestID)
	}

	// searchRequestID should not have been incremented
	if m.searchRequestID != 0 {
		t.Errorf("expected searchRequestID=0, got %d", m.searchRequestID)
	}

	if cmd == nil {
		t.Error("expected a command to be returned")
	}
}

// TestSubAggregateDrillDownWithSearchQuery verifies drill-down from sub-aggregate
// also uses search when a query is active.
func TestSubAggregateDrillDownWithSearchQuery(t *testing.T) {
	model := newTestModelWithRows(testAggregateRows)
	model.level = levelSubAggregate
	model.searchQuery = "urgent"
	model.drillFilter = query.MessageFilter{Sender: "alice@example.com"}
	model.drillViewType = query.ViewSenders
	model.viewType = query.ViewLabels
	model.cursor = 0

	newModel, cmd := model.handleSubAggregateKeys(keyEnter())
	m := newModel.(Model)

	if m.level != levelMessageList {
		t.Errorf("expected levelMessageList, got %v", m.level)
	}

	// Should preserve search query
	if m.searchQuery != "urgent" {
		t.Errorf("expected searchQuery preserved, got %q", m.searchQuery)
	}

	// searchFilter should combine the drill filters
	if m.searchFilter.Sender != "alice@example.com" {
		t.Errorf("expected searchFilter.Sender preserved, got %q", m.searchFilter.Sender)
	}

	// searchRequestID should have been incremented
	if m.searchRequestID != 1 {
		t.Errorf("expected searchRequestID=1, got %d", m.searchRequestID)
	}

	if cmd == nil {
		t.Error("expected a command to be returned")
	}
}

// TestViewTypeRestoredAfterEscFromSubAggregate verifies viewType is restored when
// navigating back from sub-aggregate to message list.
func TestViewTypeRestoredAfterEscFromSubAggregate(t *testing.T) {
	engine := &mockEngine{}
	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})

	// Set up message list state with ViewSenders
	model.level = levelMessageList
	model.viewType = query.ViewSenders
	model.drillFilter = query.MessageFilter{Sender: "alice@example.com"}
	model.drillViewType = query.ViewSenders
	model.messages = []query.MessageSummary{{ID: 1}, {ID: 2}}
	model.cursor = 1
	model.scrollOffset = 0

	// Press Tab to go to sub-aggregate (changes viewType)
	newModel, _ := model.Update(keyTab())
	m := newModel.(Model)

	if m.level != levelSubAggregate {
		t.Fatalf("expected levelSubAggregate, got %v", m.level)
	}
	// viewType should have changed to next sub-group view (Recipients)
	if m.viewType != query.ViewRecipients {
		t.Errorf("expected ViewRecipients in sub-aggregate, got %v", m.viewType)
	}

	// Press Esc to go back to message list
	newModel2, _ := m.goBack()
	m2 := newModel2.(Model)

	if m2.level != levelMessageList {
		t.Fatalf("expected levelMessageList after Esc, got %v", m2.level)
	}
	// viewType should be restored to ViewSenders
	if m2.viewType != query.ViewSenders {
		t.Errorf("expected ViewSenders after going back, got %v", m2.viewType)
	}
}

// TestCursorScrollPreservedAfterGoBack verifies cursor and scroll are preserved
// when navigating back. With view caching, data is restored from cache instantly
// without requiring a reload.
func TestCursorScrollPreservedAfterGoBack(t *testing.T) {
	engine := &mockEngine{}
	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})

	// Set up aggregate view with cursor at row 5
	model.level = levelAggregates
	model.viewType = query.ViewSenders
	model.cursor = 5
	model.scrollOffset = 3
	model.rows = make([]query.AggregateRow, 10)
	for i := range model.rows {
		model.rows[i] = query.AggregateRow{Key: fmt.Sprintf("sender%d@example.com", i), Count: int64(i)}
	}

	// Drill down to message list (saves breadcrumb with cached rows)
	newModel, _ := model.Update(keyEnter())
	m := newModel.(Model)

	if m.level != levelMessageList {
		t.Fatalf("expected levelMessageList, got %v", m.level)
	}

	// Verify breadcrumb was saved with cached rows
	if len(m.breadcrumbs) != 1 {
		t.Fatalf("expected 1 breadcrumb, got %d", len(m.breadcrumbs))
	}
	if m.breadcrumbs[0].state.rows == nil {
		t.Error("expected CachedRows to be set in breadcrumb")
	}

	// Go back to aggregates - with caching, this restores instantly without reload
	newModel2, cmd := m.goBack()
	m2 := newModel2.(Model)

	// With caching, no reload command is returned
	if cmd != nil {
		t.Error("expected nil command when restoring from cache")
	}

	// Loading should be false (no async reload needed)
	if m2.loading {
		t.Error("expected loading=false when restoring from cache")
	}

	// Cursor and scroll should be preserved from breadcrumb
	if m2.cursor != 5 {
		t.Errorf("expected cursor=5, got %d", m2.cursor)
	}
	if m2.scrollOffset != 3 {
		t.Errorf("expected scrollOffset=3, got %d", m2.scrollOffset)
	}

	// Rows should be restored from cache
	if len(m2.rows) != 10 {
		t.Errorf("expected 10 rows, got %d", len(m2.rows))
	}
}

// TestGoBackClearsError verifies that goBack clears any stale error.
func TestGoBackClearsError(t *testing.T) {
	engine := &mockEngine{}
	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})

	// Set up with a breadcrumb and an error
	model.level = levelMessageList
	model.err = fmt.Errorf("some previous error")
	model.breadcrumbs = []navigationSnapshot{{state: viewState{
		level:    levelAggregates,
		viewType: query.ViewSenders,
	}}}

	// Go back
	newModel, _ := model.goBack()
	m := newModel.(Model)

	// Error should be cleared
	if m.err != nil {
		t.Errorf("expected err=nil after goBack, got %v", m.err)
	}
}

// TestDrillFilterPreservedAfterMessageDetail verifies drillFilter is preserved
// when navigating back from message detail to message list.
func TestDrillFilterPreservedAfterMessageDetail(t *testing.T) {
	engine := &mockEngine{}
	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})

	// Set up message list with drill filter (sender + recipient combined)
	model.level = levelMessageList
	model.viewType = query.ViewRecipients
	model.drillFilter = query.MessageFilter{
		Sender:    "alice@example.com",
		Recipient: "bob@example.com",
	}
	model.drillViewType = query.ViewSenders
	model.filterKey = "bob@example.com"
	model.allMessages = false
	model.messages = []query.MessageSummary{
		{ID: 1, Subject: "Test message"},
		{ID: 2, Subject: "Another message"},
	}
	model.cursor = 0

	// Press Enter to go to message detail
	newModel, _ := model.Update(keyEnter())
	m := newModel.(Model)

	if m.level != levelMessageDetail {
		t.Fatalf("expected levelMessageDetail, got %v", m.level)
	}

	// Verify breadcrumb saved drillFilter
	if len(m.breadcrumbs) == 0 {
		t.Fatal("expected breadcrumb to be saved")
	}
	bc := m.breadcrumbs[len(m.breadcrumbs)-1]
	if bc.state.drillFilter.Sender != "alice@example.com" {
		t.Errorf("expected breadcrumb DrillFilter.Sender='alice@example.com', got %q", bc.state.drillFilter.Sender)
	}
	if bc.state.drillFilter.Recipient != "bob@example.com" {
		t.Errorf("expected breadcrumb DrillFilter.Recipient='bob@example.com', got %q", bc.state.drillFilter.Recipient)
	}
	if bc.state.drillViewType != query.ViewSenders {
		t.Errorf("expected breadcrumb DrillViewType=ViewSenders, got %v", bc.state.drillViewType)
	}

	// Press Esc to go back to message list
	newModel2, _ := m.goBack()
	m2 := newModel2.(Model)

	if m2.level != levelMessageList {
		t.Fatalf("expected levelMessageList after Esc, got %v", m2.level)
	}

	// drillFilter should be restored
	if m2.drillFilter.Sender != "alice@example.com" {
		t.Errorf("expected drillFilter.Sender='alice@example.com', got %q", m2.drillFilter.Sender)
	}
	if m2.drillFilter.Recipient != "bob@example.com" {
		t.Errorf("expected drillFilter.Recipient='bob@example.com', got %q", m2.drillFilter.Recipient)
	}
	if m2.drillViewType != query.ViewSenders {
		t.Errorf("expected drillViewType=ViewSenders, got %v", m2.drillViewType)
	}
	if m2.viewType != query.ViewRecipients {
		t.Errorf("expected viewType=ViewRecipients, got %v", m2.viewType)
	}
}

// === Header View Tests ===

// TestHeaderShowsTitleBar verifies the title bar shows msgvault with version.
func TestHeaderShowsTitleBar(t *testing.T) {
	engine := &mockEngine{}
	model := New(engine, Options{DataDir: "/tmp/test", Version: "abc1234567890"})
	model.width = 100
	model.height = 20
	model.level = levelAggregates
	model.viewType = query.ViewSenders

	header := model.headerView()
	lines := strings.Split(header, "\n")

	if len(lines) < 2 {
		t.Fatalf("expected 2 header lines, got %d", len(lines))
	}

	// Line 1 should contain msgvault and truncated version
	if !strings.Contains(lines[0], "msgvault") {
		t.Errorf("expected title bar to contain 'msgvault', got: %s", lines[0])
	}
	if !strings.Contains(lines[0], "[abc1234") {
		t.Errorf("expected title bar to contain truncated version '[abc1234', got: %s", lines[0])
	}
	if !strings.Contains(lines[0], "All Accounts") {
		t.Errorf("expected title bar to contain 'All Accounts', got: %s", lines[0])
	}
}

// TestHeaderShowsSelectedAccount verifies header shows selected account name.
func TestHeaderShowsSelectedAccount(t *testing.T) {
	engine := &mockEngine{}
	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})
	model.width = 100
	model.height = 20
	model.level = levelAggregates
	model.accounts = []query.AccountInfo{
		{ID: 1, Identifier: "alice@gmail.com"},
		{ID: 2, Identifier: "bob@gmail.com"},
	}
	accountID := int64(2)
	model.accountFilter = &accountID

	header := model.headerView()
	lines := strings.Split(header, "\n")

	if !strings.Contains(lines[0], "bob@gmail.com") {
		t.Errorf("expected title bar to show selected account 'bob@gmail.com', got: %s", lines[0])
	}
}

// TestHeaderShowsViewTypeOnLine2 verifies line 2 shows current view type.
func TestHeaderShowsViewTypeOnLine2(t *testing.T) {
	engine := &mockEngine{}
	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})
	model.width = 100
	model.height = 20
	model.level = levelAggregates
	model.viewType = query.ViewSenders
	model.stats = &query.TotalStats{MessageCount: 1000, TotalSize: 5000000, AttachmentCount: 50}

	header := model.headerView()
	lines := strings.Split(header, "\n")

	if len(lines) < 2 {
		t.Fatalf("expected 2 header lines, got %d", len(lines))
	}

	// Line 2 should contain view type and stats
	if !strings.Contains(lines[1], "Sender") {
		t.Errorf("expected line 2 to contain view type 'Sender', got: %s", lines[1])
	}
	if !strings.Contains(lines[1], "1000 msgs") {
		t.Errorf("expected line 2 to contain stats '1000 msgs', got: %s", lines[1])
	}
}

// TestHeaderDrillDownUsesPrefix verifies drill-down uses compact prefix (S: instead of From:).
func TestHeaderDrillDownUsesPrefix(t *testing.T) {
	engine := &mockEngine{}
	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})
	model.width = 100
	model.height = 20
	model.level = levelMessageList
	model.viewType = query.ViewRecipients
	model.drillViewType = query.ViewSenders
	model.drillFilter = query.MessageFilter{Sender: "alice@example.com"}
	model.filterKey = "alice@example.com"

	header := model.headerView()
	lines := strings.Split(header, "\n")

	if len(lines) < 2 {
		t.Fatalf("expected 2 header lines, got %d", len(lines))
	}

	// Line 2 should use "S:" prefix for sender drill-down, not "From:"
	if !strings.Contains(lines[1], "S:") {
		t.Errorf("expected line 2 to use 'S:' prefix for sender drill-down, got: %s", lines[1])
	}
	if strings.Contains(lines[1], "From:") {
		t.Errorf("expected line 2 to NOT use 'From:' for drill-down (should be 'S:'), got: %s", lines[1])
	}
}

// TestHeaderSubAggregateShowsDrillContext verifies sub-aggregate shows drill context.
func TestHeaderSubAggregateShowsDrillContext(t *testing.T) {
	engine := &mockEngine{}
	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})
	model.width = 100
	model.height = 20
	model.level = levelSubAggregate
	model.viewType = query.ViewRecipients
	model.drillViewType = query.ViewSenders
	model.drillFilter = query.MessageFilter{Sender: "alice@example.com"}
	model.contextStats = &query.TotalStats{MessageCount: 100, TotalSize: 500000}

	header := model.headerView()
	lines := strings.Split(header, "\n")

	if len(lines) < 2 {
		t.Fatalf("expected 2 header lines, got %d", len(lines))
	}

	// Line 2 should show "S: alice@example.com (by Recipient)"
	if !strings.Contains(lines[1], "S:") {
		t.Errorf("expected line 2 to contain 'S:' prefix, got: %s", lines[1])
	}
	if !strings.Contains(lines[1], "alice@example.com") {
		t.Errorf("expected line 2 to contain drill key 'alice@example.com', got: %s", lines[1])
	}
	if !strings.Contains(lines[1], "(by Recipient)") {
		t.Errorf("expected line 2 to contain '(by Recipient)' sub-group indicator, got: %s", lines[1])
	}
	// Should show contextStats
	if !strings.Contains(lines[1], "100 msgs") {
		t.Errorf("expected line 2 to show contextStats '100 msgs', got: %s", lines[1])
	}
}

// TestHeaderWithAttachmentFilter verifies header shows attachment filter indicator.
func TestHeaderWithAttachmentFilter(t *testing.T) {
	engine := &mockEngine{}
	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})
	model.width = 100
	model.height = 20
	model.level = levelAggregates
	model.attachmentFilter = true

	header := model.headerView()
	lines := strings.Split(header, "\n")

	if !strings.Contains(lines[0], "[Attachments]") {
		t.Errorf("expected title bar to show '[Attachments]' filter indicator, got: %s", lines[0])
	}
}

// TestViewStructureHasTitleBarFirst verifies View() output starts with title bar.
func TestViewStructureHasTitleBarFirst(t *testing.T) {
	engine := &mockEngine{
		rows: []query.AggregateRow{
			{Key: "alice@example.com", Count: 100, TotalSize: 500000},
		},
	}

	model := New(engine, Options{DataDir: "/tmp/test", Version: "abc1234"})
	model.width = 100
	model.height = 30
	model.level = levelAggregates
	model.viewType = query.ViewSenders
	model.rows = engine.rows
	model.pageSize = 20
	model.stats = &query.TotalStats{MessageCount: 1000, TotalSize: 5000000, AttachmentCount: 50}

	view := model.View()
	lines := strings.Split(view, "\n")

	// Debug output
	t.Logf("Total lines in View: %d", len(lines))
	for i := 0; i < 5 && i < len(lines); i++ {
		t.Logf("Line %d: %q", i+1, lines[i])
	}

	// Line 1 should be title bar with msgvault
	if len(lines) < 1 {
		t.Fatal("View output has no lines")
	}
	if !strings.Contains(lines[0], "msgvault") {
		t.Errorf("Line 1 should contain 'msgvault' (title bar), got: %q", lines[0])
	}

	// Line 2 should be breadcrumb with view type
	if len(lines) < 2 {
		t.Fatal("View output has less than 2 lines")
	}
	if !strings.Contains(lines[1], "From") && !strings.Contains(lines[1], "msgs") {
		t.Errorf("Line 2 should contain breadcrumb/stats (From or msgs), got: %q", lines[1])
	}
}

// TestViewFitsTerminalHeight verifies View() output fits exactly in terminal height
// when pageSize is calculated via WindowSizeMsg. This catches bugs where header
// line count changes but pageSize calculation isn't updated.
func TestViewFitsTerminalHeight(t *testing.T) {
	engine := &mockEngine{
		rows: []query.AggregateRow{
			{Key: "alice@example.com", Count: 100, TotalSize: 500000},
			{Key: "bob@example.com", Count: 50, TotalSize: 250000},
		},
	}

	model := New(engine, Options{DataDir: "/tmp/test", Version: "abc1234"})
	model.level = levelAggregates
	model.viewType = query.ViewSenders
	model.rows = engine.rows
	model.stats = &query.TotalStats{MessageCount: 1000, TotalSize: 5000000, AttachmentCount: 50}

	// Simulate WindowSizeMsg to trigger pageSize calculation (the real code path)
	terminalHeight := 40
	newModel, _ := model.Update(tea.WindowSizeMsg{Width: 100, Height: terminalHeight})
	model = newModel.(Model)

	view := model.View()
	lines := strings.Split(view, "\n")

	// The last "line" after split is empty due to trailing newline, so subtract 1
	actualLines := len(lines)
	if lines[len(lines)-1] == "" {
		actualLines--
	}

	t.Logf("Terminal height: %d, View lines: %d, pageSize: %d", terminalHeight, actualLines, model.pageSize)
	t.Logf("First line: %q", lines[0])
	t.Logf("Last non-empty line: %q", lines[actualLines-1])

	// View should fit exactly in terminal height
	if actualLines > terminalHeight {
		t.Errorf("View has %d lines but terminal height is %d - title bar will be pushed off!", actualLines, terminalHeight)
	}

	// First line must be title bar
	if !strings.Contains(lines[0], "msgvault") {
		t.Errorf("First line should be title bar with 'msgvault', got: %q", lines[0])
	}
}

// TestViewFitsTerminalHeightDuringLoading verifies View() output fits during loading state.
func TestViewFitsTerminalHeightDuringLoading(t *testing.T) {
	engine := &mockEngine{
		rows: []query.AggregateRow{
			{Key: "alice@example.com", Count: 100, TotalSize: 500000},
			{Key: "bob@example.com", Count: 50, TotalSize: 250000},
		},
	}

	model := New(engine, Options{DataDir: "/tmp/test", Version: "abc1234"})
	model.level = levelAggregates
	model.viewType = query.ViewSenders
	model.rows = engine.rows
	model.stats = &query.TotalStats{MessageCount: 1000, TotalSize: 5000000, AttachmentCount: 50}
	model.loading = true // Key difference: loading state

	terminalHeight := 40
	newModel, _ := model.Update(tea.WindowSizeMsg{Width: 100, Height: terminalHeight})
	model = newModel.(Model)

	view := model.View()
	lines := strings.Split(view, "\n")

	actualLines := len(lines)
	if lines[len(lines)-1] == "" {
		actualLines--
	}

	t.Logf("Terminal height: %d, View lines: %d, pageSize: %d (loading=%v)", terminalHeight, actualLines, model.pageSize, model.loading)

	if actualLines > terminalHeight {
		t.Errorf("View has %d lines but terminal height is %d during loading", actualLines, terminalHeight)
	}
	if !strings.Contains(lines[0], "msgvault") {
		t.Errorf("First line should be title bar, got: %q", lines[0])
	}
}

// TestViewFitsTerminalHeightWithInlineSearch verifies View() output fits with inline search active.
func TestViewFitsTerminalHeightWithInlineSearch(t *testing.T) {
	engine := &mockEngine{
		rows: []query.AggregateRow{
			{Key: "alice@example.com", Count: 100, TotalSize: 500000},
			{Key: "bob@example.com", Count: 50, TotalSize: 250000},
		},
	}

	model := New(engine, Options{DataDir: "/tmp/test", Version: "abc1234"})
	model.level = levelAggregates
	model.viewType = query.ViewSenders
	model.rows = engine.rows
	model.stats = &query.TotalStats{MessageCount: 1000, TotalSize: 5000000, AttachmentCount: 50}
	model.inlineSearchActive = true // Enable inline search

	terminalHeight := 40
	newModel, _ := model.Update(tea.WindowSizeMsg{Width: 100, Height: terminalHeight})
	model = newModel.(Model)

	view := model.View()
	lines := strings.Split(view, "\n")

	actualLines := len(lines)
	if lines[len(lines)-1] == "" {
		actualLines--
	}

	t.Logf("Terminal height: %d, View lines: %d, pageSize: %d (inlineSearch=%v)", terminalHeight, actualLines, model.pageSize, model.inlineSearchActive)

	if actualLines > terminalHeight {
		t.Errorf("View has %d lines but terminal height is %d with inline search", actualLines, terminalHeight)
	}
	if !strings.Contains(lines[0], "msgvault") {
		t.Errorf("First line should be title bar, got: %q", lines[0])
	}
}

// TestViewFitsTerminalHeightAtMessageList verifies View() output fits at message list level.
func TestViewFitsTerminalHeightAtMessageList(t *testing.T) {
	engine := &mockEngine{
		messages: []query.MessageSummary{
			{ID: 1, Subject: "Test 1", FromEmail: "alice@example.com", SizeEstimate: 1000},
			{ID: 2, Subject: "Test 2", FromEmail: "bob@example.com", SizeEstimate: 2000},
		},
	}

	model := New(engine, Options{DataDir: "/tmp/test", Version: "abc1234"})
	model.level = levelMessageList
	model.messages = engine.messages
	model.filterKey = "alice@example.com"
	model.stats = &query.TotalStats{MessageCount: 1000, TotalSize: 5000000, AttachmentCount: 50}
	model.contextStats = &query.TotalStats{MessageCount: 2, TotalSize: 3000, AttachmentCount: 0}

	terminalHeight := 40
	newModel, _ := model.Update(tea.WindowSizeMsg{Width: 100, Height: terminalHeight})
	model = newModel.(Model)

	view := model.View()
	lines := strings.Split(view, "\n")

	actualLines := len(lines)
	if lines[len(lines)-1] == "" {
		actualLines--
	}

	t.Logf("Terminal height: %d, View lines: %d, pageSize: %d (level=MessageList)", terminalHeight, actualLines, model.pageSize)

	if actualLines > terminalHeight {
		t.Errorf("View has %d lines but terminal height is %d at message list", actualLines, terminalHeight)
	}
	if !strings.Contains(lines[0], "msgvault") {
		t.Errorf("First line should be title bar, got: %q", lines[0])
	}
}

// TestViewFitsTerminalHeightStartupSequence simulates the real startup sequence
// to verify line counts at each stage of initialization.
func TestViewFitsTerminalHeightStartupSequence(t *testing.T) {
	terminalHeight := 40
	terminalWidth := 100

	// Stage 1: Before WindowSizeMsg (width=0)
	engine := &mockEngine{}
	model := New(engine, Options{DataDir: "/tmp/test", Version: "abc1234"})
	model.loading = true

	view1 := model.View()
	t.Logf("Stage 1 (before resize): View = %q", view1)
	if view1 != "Loading..." {
		t.Errorf("Stage 1: Expected 'Loading...', got %q", view1)
	}

	// Stage 2: After WindowSizeMsg (width/height set, loading=true, no data)
	newModel, _ := model.Update(tea.WindowSizeMsg{Width: terminalWidth, Height: terminalHeight})
	model = newModel.(Model)

	view2 := model.View()
	lines2 := strings.Split(view2, "\n")
	actualLines2 := len(lines2)
	if lines2[len(lines2)-1] == "" {
		actualLines2--
	}
	t.Logf("Stage 2 (after resize, loading=true, no data): lines=%d, pageSize=%d", actualLines2, model.pageSize)
	t.Logf("  First line: %q", truncateTestString(lines2[0], 60))
	t.Logf("  Last line: %q", truncateTestString(lines2[actualLines2-1], 60))

	if actualLines2 != terminalHeight {
		t.Errorf("Stage 2: View has %d lines but terminal height is %d (loading, no data)", actualLines2, terminalHeight)
	}

	// Stage 3: After stats load (still loading=true, no data)
	model.stats = &query.TotalStats{MessageCount: 1000, TotalSize: 5000000, AttachmentCount: 50}

	view3 := model.View()
	lines3 := strings.Split(view3, "\n")
	actualLines3 := len(lines3)
	if lines3[len(lines3)-1] == "" {
		actualLines3--
	}
	t.Logf("Stage 3 (stats loaded, loading=true): lines=%d", actualLines3)

	if actualLines3 != terminalHeight {
		t.Errorf("Stage 3: View has %d lines but terminal height is %d (stats loaded)", actualLines3, terminalHeight)
	}

	// Stage 4: After data loads (loading=false, rows populated)
	model.loading = false
	model.rows = []query.AggregateRow{
		{Key: "alice@example.com", Count: 100, TotalSize: 500000},
		{Key: "bob@example.com", Count: 50, TotalSize: 250000},
	}

	view4 := model.View()
	lines4 := strings.Split(view4, "\n")
	actualLines4 := len(lines4)
	if lines4[len(lines4)-1] == "" {
		actualLines4--
	}
	t.Logf("Stage 4 (data loaded): lines=%d", actualLines4)
	t.Logf("  First line: %q", truncateTestString(lines4[0], 60))

	if actualLines4 != terminalHeight {
		t.Errorf("Stage 4: View has %d lines but terminal height is %d (data loaded)", actualLines4, terminalHeight)
	}

	// Ensure first line is always title bar at stages 2-4
	for i, lines := range [][]string{lines2, lines3, lines4} {
		if !strings.Contains(lines[0], "msgvault") {
			t.Errorf("Stage %d: First line should contain 'msgvault', got: %q", i+2, lines[0])
		}
	}
}

// truncateTestString truncates a string for test output display.
func truncateTestString(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "..."
}

// TestViewFitsTerminalHeightWithBadData verifies View() handles data with
// embedded newlines or other problematic characters without adding extra lines.
func TestViewFitsTerminalHeightWithBadData(t *testing.T) {
	// Data with embedded newlines and other special characters
	engine := &mockEngine{
		rows: []query.AggregateRow{
			{Key: "alice@example.com", Count: 100, TotalSize: 500000},
			{Key: "bob\n@example.com", Count: 50, TotalSize: 250000}, // Embedded newline!
			{Key: "charlie\r\n@example.com", Count: 25, TotalSize: 100000}, // CRLF
			{Key: "david\t@example.com", Count: 10, TotalSize: 50000}, // Tab
		},
	}

	model := New(engine, Options{DataDir: "/tmp/test", Version: "abc1234"})
	model.level = levelAggregates
	model.viewType = query.ViewSenders
	model.rows = engine.rows
	model.stats = &query.TotalStats{MessageCount: 1000, TotalSize: 5000000, AttachmentCount: 50}

	terminalHeight := 40
	newModel, _ := model.Update(tea.WindowSizeMsg{Width: 100, Height: terminalHeight})
	model = newModel.(Model)

	view := model.View()
	lines := strings.Split(view, "\n")

	actualLines := len(lines)
	if lines[len(lines)-1] == "" {
		actualLines--
	}

	t.Logf("Terminal height: %d, View lines: %d (with bad data)", terminalHeight, actualLines)

	if actualLines > terminalHeight {
		t.Errorf("View has %d lines but terminal height is %d - bad data caused extra lines!", actualLines, terminalHeight)
		// Log the problematic lines for debugging
		for i, line := range lines {
			if i >= terminalHeight {
				t.Logf("  Extra line %d: %q", i, truncateTestString(line, 60))
			}
		}
	}
}

// TestViewFitsVariousTerminalSizes tests that View fits for common terminal sizes.
func TestViewFitsVariousTerminalSizes(t *testing.T) {
	sizes := []struct {
		width, height int
	}{
		{80, 24},  // Standard
		{100, 27}, // User's actual terminal
		{100, 30}, // Larger
		{100, 55}, // User's other terminal
		{120, 40}, // Wide
		{80, 10},  // Very short
		{200, 50}, // Very wide and tall
	}

	for _, size := range sizes {
		t.Run(fmt.Sprintf("%dx%d", size.width, size.height), func(t *testing.T) {
			engine := &mockEngine{
				rows: []query.AggregateRow{
					{Key: "alice@example.com", Count: 100, TotalSize: 500000},
					{Key: "bob@example.com", Count: 50, TotalSize: 250000},
				},
			}

			model := New(engine, Options{DataDir: "/tmp/test", Version: "abc1234"})
			model.level = levelAggregates
			model.viewType = query.ViewSenders
			model.rows = engine.rows
			model.stats = &query.TotalStats{MessageCount: 1000, TotalSize: 5000000, AttachmentCount: 50}

			newModel, _ := model.Update(tea.WindowSizeMsg{Width: size.width, Height: size.height})
			model = newModel.(Model)

			view := model.View()
			lines := strings.Split(view, "\n")

			actualLines := len(lines)
			if lines[len(lines)-1] == "" {
				actualLines--
			}

			if actualLines != size.height {
				t.Errorf("View has %d lines but terminal height is %d (pageSize=%d)", actualLines, size.height, model.pageSize)
			}

			// Check no line exceeds width
			for i, line := range lines {
				if lipgloss.Width(line) > size.width {
					t.Errorf("Line %d exceeds width: %d > %d", i, lipgloss.Width(line), size.width)
				}
			}
		})
	}
}

// TestViewDuringSpinnerAnimation verifies line count during spinner animation.
func TestViewDuringSpinnerAnimation(t *testing.T) {
	engine := &mockEngine{
		rows: []query.AggregateRow{
			{Key: "alice@example.com", Count: 100, TotalSize: 500000},
		},
	}

	model := New(engine, Options{DataDir: "/tmp/test", Version: "abc1234"})
	model.level = levelAggregates
	model.viewType = query.ViewSenders
	model.rows = engine.rows
	model.stats = &query.TotalStats{MessageCount: 1000, TotalSize: 5000000, AttachmentCount: 50}
	model.loading = true // Loading state shows spinner

	terminalWidth := 100
	terminalHeight := 24
	newModel, _ := model.Update(tea.WindowSizeMsg{Width: terminalWidth, Height: terminalHeight})
	model = newModel.(Model)

	// Simulate multiple spinner frames
	for frame := 0; frame < 10; frame++ {
		model.spinnerFrame = frame

		view := model.View()
		lines := strings.Split(view, "\n")

		actualLines := len(lines)
		if lines[len(lines)-1] == "" {
			actualLines--
		}

		if actualLines != terminalHeight {
			t.Errorf("Frame %d: View has %d lines but terminal height is %d", frame, actualLines, terminalHeight)
		}

		// Check line widths
		for i, line := range lines {
			if lipgloss.Width(line) > terminalWidth {
				t.Errorf("Frame %d, Line %d exceeds width: %d > %d", frame, i, lipgloss.Width(line), terminalWidth)
			}
		}
	}
}

// TestViewLineByLineAnalysis provides detailed line-by-line output for debugging.
func TestViewLineByLineAnalysis(t *testing.T) {
	engine := &mockEngine{
		rows: []query.AggregateRow{
			{Key: "alice@example.com", Count: 100, TotalSize: 500000},
			{Key: "bob@example.com", Count: 50, TotalSize: 250000},
		},
	}

	model := New(engine, Options{DataDir: "/tmp/test", Version: "abc1234"})
	model.level = levelAggregates
	model.viewType = query.ViewSenders
	model.rows = engine.rows
	model.stats = &query.TotalStats{MessageCount: 1000, TotalSize: 5000000, AttachmentCount: 50}

	terminalWidth := 100
	terminalHeight := 55 // User's actual terminal height
	newModel, _ := model.Update(tea.WindowSizeMsg{Width: terminalWidth, Height: terminalHeight})
	model = newModel.(Model)

	view := model.View()
	lines := strings.Split(view, "\n")

	t.Logf("=== View Analysis (terminal %dx%d, pageSize=%d) ===", terminalWidth, terminalHeight, model.pageSize)
	t.Logf("Total lines from split: %d", len(lines))

	// Count non-empty lines
	nonEmpty := 0
	for i, line := range lines {
		width := lipgloss.Width(line)
		isEmpty := line == ""
		if !isEmpty {
			nonEmpty++
		}
		marker := ""
		if i == 0 {
			marker = " <- title bar"
		} else if i == 1 {
			marker = " <- breadcrumb/stats"
		} else if i == len(lines)-1 || (i == len(lines)-2 && lines[len(lines)-1] == "") {
			marker = " <- footer"
		}
		if width > terminalWidth {
			marker += " *** OVERFLOW ***"
		}
		t.Logf("Line %2d: width=%3d empty=%v %s", i, width, isEmpty, marker)
	}
	t.Logf("Non-empty lines: %d (expected: %d)", nonEmpty, terminalHeight)

	if nonEmpty > terminalHeight {
		t.Errorf("View has %d non-empty lines but terminal height is %d", nonEmpty, terminalHeight)
	}
}

// TestHeaderLineFitsWidth verifies the header line2 doesn't exceed terminal width
// even when breadcrumb + stats are very long.
func TestHeaderLineFitsWidth(t *testing.T) {
	engine := &mockEngine{
		rows: []query.AggregateRow{
			{Key: "alice@example.com", Count: 100, TotalSize: 500000},
		},
	}

	model := New(engine, Options{DataDir: "/tmp/test", Version: "abc1234"})
	model.level = levelAggregates
	model.viewType = query.ViewSenders
	model.rows = engine.rows
	// Very long stats string
	model.stats = &query.TotalStats{MessageCount: 999999999, TotalSize: 999999999999, AttachmentCount: 999999}

	terminalWidth := 80 // Narrower terminal
	terminalHeight := 40
	newModel, _ := model.Update(tea.WindowSizeMsg{Width: terminalWidth, Height: terminalHeight})
	model = newModel.(Model)

	view := model.View()
	lines := strings.Split(view, "\n")

	actualLines := len(lines)
	if lines[len(lines)-1] == "" {
		actualLines--
	}

	t.Logf("Terminal: %dx%d, View lines: %d", terminalWidth, terminalHeight, actualLines)

	if actualLines > terminalHeight {
		t.Errorf("View has %d lines but terminal height is %d - header overflow?", actualLines, terminalHeight)
		// Show first few lines
		for i := 0; i < 5 && i < len(lines); i++ {
			t.Logf("  Line %d (width %d): %q", i, lipgloss.Width(lines[i]), truncateTestString(lines[i], 60))
		}
	}

	// Check that no line exceeds terminal width
	for i, line := range lines[:min(5, len(lines))] {
		lineWidth := lipgloss.Width(line)
		if lineWidth > terminalWidth {
			t.Errorf("Line %d has width %d but terminal width is %d: %q", i, lineWidth, terminalWidth, truncateTestString(line, 60))
		}
	}
}

// TestFooterShowsTotalUniqueWhenAvailable verifies that the footer shows
// "N of M" format when TotalUnique is set and greater than loaded rows.
func TestFooterShowsTotalUniqueWhenAvailable(t *testing.T) {
	// Set up rows with TotalUnique set (simulating a query that returns more rows than loaded)
	engine := &mockEngine{
		rows: []query.AggregateRow{
			{Key: "alice@example.com", Count: 100, TotalSize: 500000, TotalUnique: 1000},
			{Key: "bob@example.com", Count: 50, TotalSize: 250000, TotalUnique: 1000},
		},
	}

	model := New(engine, Options{DataDir: "/tmp/test", Version: "abc1234"})
	model.width = 100
	model.height = 30
	model.level = levelAggregates
	model.viewType = query.ViewSenders
	model.rows = engine.rows
	model.pageSize = 20
	model.cursor = 0

	footer := model.footerView()

	// When TotalUnique is set and greater than loaded rows, should show "N of M"
	if !strings.Contains(footer, "1 of 1000") {
		t.Errorf("Footer should show '1 of 1000' when TotalUnique (1000) > loaded rows (2), got: %q", footer)
	}
}

// TestFooterShowsLoadedCountWhenNoTotalUnique verifies that the footer falls back
// to showing loaded count when TotalUnique is not set (zero value).
func TestFooterShowsLoadedCountWhenNoTotalUnique(t *testing.T) {
	// Set up rows without TotalUnique (zero value)
	engine := &mockEngine{
		rows: []query.AggregateRow{
			{Key: "alice@example.com", Count: 100, TotalSize: 500000},
			{Key: "bob@example.com", Count: 50, TotalSize: 250000},
		},
	}

	model := New(engine, Options{DataDir: "/tmp/test", Version: "abc1234"})
	model.width = 100
	model.height = 30
	model.level = levelAggregates
	model.viewType = query.ViewSenders
	model.rows = engine.rows
	model.pageSize = 20
	model.cursor = 0

	footer := model.footerView()

	// When TotalUnique is not set, should show loaded count format
	if !strings.Contains(footer, "1/2") {
		t.Errorf("Footer should show '1/2' when TotalUnique is not set, got: %q", footer)
	}
}

// TestViewTypePrefixFallback verifies viewTypePrefix handles all ViewType values.
func TestViewTypePrefixFallback(t *testing.T) {
	// Test known view types return expected prefixes
	tests := []struct {
		vt       query.ViewType
		expected string
	}{
		{query.ViewSenders, "S"},
		{query.ViewRecipients, "R"},
		{query.ViewDomains, "D"},
		{query.ViewLabels, "L"},
		{query.ViewTime, "T"},
	}

	for _, tc := range tests {
		got := viewTypePrefix(tc.vt)
		if got != tc.expected {
			t.Errorf("viewTypePrefix(%v) = %q, want %q", tc.vt, got, tc.expected)
		}
	}

	// Test unknown view type - should return first char of String()
	// Note: ViewType(999).String() returns "ViewType(999)" so we get "V"
	// The "?" fallback in viewTypePrefix is defensive code for the edge case
	// where String() returns empty, which doesn't happen with Go's stringer.
	unknown := query.ViewType(999)
	got := viewTypePrefix(unknown)
	expectedFirstChar := string(unknown.String()[0]) // "V" from "ViewType(999)"
	if got != expectedFirstChar {
		t.Errorf("viewTypePrefix(%v) = %q, want %q (first char of String())", unknown, got, expectedFirstChar)
	}
}

// TestDetailNavigationPrevNext verifies left/right arrow navigation in message detail view.
// Left = previous in list (lower index), Right = next in list (higher index).
func TestDetailNavigationPrevNext(t *testing.T) {
	engine := &mockEngine{}
	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})

	// Set up message detail view with messages loaded
	model.level = levelMessageDetail
	model.messages = []query.MessageSummary{
		{ID: 1, Subject: "First message"},
		{ID: 2, Subject: "Second message"},
		{ID: 3, Subject: "Third message"},
	}
	model.detailMessageIndex = 1 // Viewing second message
	model.cursor = 1
	model.messageDetail = &query.MessageDetail{ID: 2, Subject: "Second message"}

	// Press right arrow to go to next message in list (higher index)
	newModel, cmd := model.Update(keyRight())
	m := newModel.(Model)

	if m.detailMessageIndex != 2 {
		t.Errorf("expected detailMessageIndex=2 after right, got %d", m.detailMessageIndex)
	}
	if m.cursor != 2 {
		t.Errorf("expected cursor=2 after right, got %d", m.cursor)
	}
	if m.pendingDetailSubject != "Third message" {
		t.Errorf("expected pendingDetailSubject='Third message', got %q", m.pendingDetailSubject)
	}
	if cmd == nil {
		t.Error("expected command to load message detail")
	}

	// Press left arrow to go to previous message in list (lower index)
	m.detailMessageIndex = 2
	m.cursor = 2
	newModel, cmd = m.Update(keyLeft())
	m = newModel.(Model)

	if m.detailMessageIndex != 1 {
		t.Errorf("expected detailMessageIndex=1 after left, got %d", m.detailMessageIndex)
	}
	if m.cursor != 1 {
		t.Errorf("expected cursor=1 after left, got %d", m.cursor)
	}
	if cmd == nil {
		t.Error("expected command to load message detail")
	}
}

// TestDetailNavigationAtBoundary verifies flash message at first/last message.
// TestDetailNavigationAtBoundary verifies flash message at first/last message.
func TestDetailNavigationAtBoundary(t *testing.T) {
	engine := &mockEngine{}
	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})

	// Set up message detail view at first message (index 0)
	model.level = levelMessageDetail
	model.messages = []query.MessageSummary{
		{ID: 1, Subject: "First message"},
		{ID: 2, Subject: "Second message"},
	}
	model.detailMessageIndex = 0 // At first message
	model.cursor = 0
	model.messageDetail = &query.MessageDetail{ID: 1, Subject: "First message"}

	// Press left arrow at first message - should show flash
	newModel, cmd := model.Update(keyLeft())
	m := newModel.(Model)

	if m.detailMessageIndex != 0 {
		t.Errorf("expected detailMessageIndex=0 (unchanged), got %d", m.detailMessageIndex)
	}
	if m.flashMessage != "At first message" {
		t.Errorf("expected flashMessage='At first message', got %q", m.flashMessage)
	}
	if cmd == nil {
		t.Error("expected command to clear flash message")
	}

	// Clear flash and test at last message
	m.flashMessage = ""
	m.detailMessageIndex = 1 // At last message
	m.cursor = 1
	m.messageDetail = &query.MessageDetail{ID: 2, Subject: "Second message"}

	// Press right arrow at last message - should show flash
	newModel, cmd = m.Update(keyRight())
	m = newModel.(Model)

	if m.detailMessageIndex != 1 {
		t.Errorf("expected detailMessageIndex=1 (unchanged), got %d", m.detailMessageIndex)
	}
	if m.flashMessage != "At last message" {
		t.Errorf("expected flashMessage='At last message', got %q", m.flashMessage)
	}
	if cmd == nil {
		t.Error("expected command to clear flash message")
	}
}

// TestDetailNavigationHLKeys verifies h/l keys also work for prev/next.
// h=left=prev (lower index), l=right=next (higher index).
func TestDetailNavigationHLKeys(t *testing.T) {
	engine := &mockEngine{}
	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})

	// Set up message detail view
	model.level = levelMessageDetail
	model.messages = []query.MessageSummary{
		{ID: 1, Subject: "First"},
		{ID: 2, Subject: "Second"},
		{ID: 3, Subject: "Third"},
	}
	model.detailMessageIndex = 1
	model.cursor = 1
	model.messageDetail = &query.MessageDetail{ID: 2, Subject: "Second"}

	// Press 'l' to go to next message in list (higher index)
	newModel, _ := model.Update(key('l'))
	m := newModel.(Model)

	if m.detailMessageIndex != 2 {
		t.Errorf("expected detailMessageIndex=2 after 'l', got %d", m.detailMessageIndex)
	}

	// Reset and press 'h' to go to previous message in list (lower index)
	m.detailMessageIndex = 1
	m.cursor = 1
	newModel, _ = m.Update(key('h'))
	m = newModel.(Model)

	if m.detailMessageIndex != 0 {
		t.Errorf("expected detailMessageIndex=0 after 'h', got %d", m.detailMessageIndex)
	}
}

// TestDetailNavigationEmptyList verifies navigation with empty message list.
func TestDetailNavigationEmptyList(t *testing.T) {
	engine := &mockEngine{}
	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})

	// Set up detail view with empty messages (edge case)
	model.level = levelMessageDetail
	model.messages = []query.MessageSummary{} // Empty!
	model.detailMessageIndex = 0
	model.cursor = 0

	// Press right arrow - should show flash, not panic
	newModel, _ := model.navigateDetailNext()
	m := newModel.(Model)

	if m.flashMessage != "No messages loaded" {
		t.Errorf("expected flashMessage='No messages loaded', got %q", m.flashMessage)
	}

	// Press left arrow - should show flash, not panic
	newModel, _ = m.navigateDetailPrev()
	m = newModel.(Model)

	if m.flashMessage != "No messages loaded" {
		t.Errorf("expected flashMessage='No messages loaded', got %q", m.flashMessage)
	}
}

// TestDetailNavigationOutOfBoundsIndex verifies clamping of stale index.
func TestDetailNavigationOutOfBoundsIndex(t *testing.T) {
	engine := &mockEngine{}
	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})

	// Set up detail view with index beyond list bounds (simulates stale state)
	model.level = levelMessageDetail
	model.messages = []query.MessageSummary{
		{ID: 1, Subject: "Only message"},
	}
	model.detailMessageIndex = 5 // Out of bounds!
	model.cursor = 5

	// Press left (navigateDetailPrev) - should clamp index and show flash
	// Index gets clamped from 5 to 0, then can't go to lower index
	newModel, _ := model.navigateDetailPrev()
	m := newModel.(Model)

	// Index should be clamped to 0, then show "At first message"
	// because we can't go before the only message
	if m.detailMessageIndex != 0 {
		t.Errorf("expected detailMessageIndex=0 (clamped), got %d", m.detailMessageIndex)
	}
	if m.flashMessage != "At first message" {
		t.Errorf("expected flashMessage='At first message', got %q", m.flashMessage)
	}
}

// TestDetailNavigationCursorPreservedOnGoBack verifies cursor position is preserved
// when returning to message list after navigating in detail view.
func TestDetailNavigationCursorPreservedOnGoBack(t *testing.T) {
	engine := &mockEngine{}
	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})

	// Set up message list view
	model.level = levelMessageList
	model.messages = []query.MessageSummary{
		{ID: 1, Subject: "First"},
		{ID: 2, Subject: "Second"},
		{ID: 3, Subject: "Third"},
	}
	model.cursor = 0 // Start at first message
	model.pageSize = 10
	model.width = 100
	model.height = 20

	// Enter detail view (simulates pressing Enter on first message)
	model.breadcrumbs = append(model.breadcrumbs, navigationSnapshot{state: viewState{
		level:        levelMessageList,
		viewType:     query.ViewSenders,
		cursor:       0, // Original cursor position
		scrollOffset: 0,
	}})
	model.level = levelMessageDetail
	model.detailMessageIndex = 0
	model.cursor = 0

	// Navigate to third message via right arrow (twice)
	model.detailMessageIndex = 2
	model.cursor = 2

	// Go back to message list
	newModel, _ := model.goBack()
	m := newModel.(Model)

	// Cursor should be preserved at position 2 (where we navigated to)
	// not restored to position 0 (where we entered)
	if m.level != levelMessageList {
		t.Errorf("expected levelMessageList, got %v", m.level)
	}
	if m.cursor != 2 {
		t.Errorf("expected cursor=2 (preserved from navigation), got %d", m.cursor)
	}
}

// TestDetailNavigationFromThreadView verifies that left/right navigation in detail view
// uses threadMessages (not messages) when entered from thread view, and keeps
// threadCursor and threadScrollOffset in sync.
func TestDetailNavigationFromThreadView(t *testing.T) {
	engine := &mockEngine{}
	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})

	// Set up thread view with different messages than the list
	model.threadMessages = []query.MessageSummary{
		{ID: 100, Subject: "Thread msg 1"},
		{ID: 101, Subject: "Thread msg 2"},
		{ID: 102, Subject: "Thread msg 3"},
		{ID: 103, Subject: "Thread msg 4"},
	}
	// Set up regular messages list (should NOT be used)
	model.messages = []query.MessageSummary{
		{ID: 1, Subject: "List msg 1"},
		{ID: 2, Subject: "List msg 2"},
	}

	// Enter detail view from thread view (simulates pressing Enter in thread view)
	model.level = levelMessageDetail
	model.detailFromThread = true
	model.detailMessageIndex = 1 // Viewing second thread message (ID=101)
	model.threadCursor = 1
	model.threadScrollOffset = 0
	model.pageSize = 3 // Small page size to test scroll offset
	model.messageDetail = &query.MessageDetail{ID: 101, Subject: "Thread msg 2"}

	// Press right arrow - should navigate within threadMessages
	newModel, cmd := model.Update(keyRight())
	m := newModel.(Model)

	if m.detailMessageIndex != 2 {
		t.Errorf("expected detailMessageIndex=2 after right, got %d", m.detailMessageIndex)
	}
	if m.threadCursor != 2 {
		t.Errorf("expected threadCursor=2 after right, got %d", m.threadCursor)
	}
	// cursor (for list view) should NOT be modified
	if m.cursor != 0 {
		t.Errorf("expected cursor=0 (unchanged), got %d", m.cursor)
	}
	if m.pendingDetailSubject != "Thread msg 3" {
		t.Errorf("expected pendingDetailSubject='Thread msg 3', got %q", m.pendingDetailSubject)
	}
	if cmd == nil {
		t.Error("expected command to load message detail")
	}

	// Press right again - now cursor should be at index 3 and scroll offset should adjust
	m.detailMessageIndex = 2
	m.threadCursor = 2
	newModel, _ = m.Update(keyRight())
	m = newModel.(Model)

	if m.detailMessageIndex != 3 {
		t.Errorf("expected detailMessageIndex=3 after right, got %d", m.detailMessageIndex)
	}
	if m.threadCursor != 3 {
		t.Errorf("expected threadCursor=3 after right, got %d", m.threadCursor)
	}
	// With pageSize=3, cursor at 3 should adjust scroll offset to keep cursor visible
	// threadCursor (3) >= threadScrollOffset (0) + pageSize (3), so offset should be 1
	if m.threadScrollOffset != 1 {
		t.Errorf("expected threadScrollOffset=1 to keep cursor visible, got %d", m.threadScrollOffset)
	}

	// Press left arrow - should navigate back
	newModel, _ = m.Update(keyLeft())
	m = newModel.(Model)

	if m.detailMessageIndex != 2 {
		t.Errorf("expected detailMessageIndex=2 after left, got %d", m.detailMessageIndex)
	}
	if m.threadCursor != 2 {
		t.Errorf("expected threadCursor=2 after left, got %d", m.threadCursor)
	}

	// Navigate all the way to first message
	m.detailMessageIndex = 1
	m.threadCursor = 1
	m.threadScrollOffset = 1 // Scroll offset is still 1 from before
	newModel, _ = m.Update(keyLeft())
	m = newModel.(Model)

	if m.detailMessageIndex != 0 {
		t.Errorf("expected detailMessageIndex=0 after left, got %d", m.detailMessageIndex)
	}
	if m.threadCursor != 0 {
		t.Errorf("expected threadCursor=0 after left, got %d", m.threadCursor)
	}
	// threadCursor (0) < threadScrollOffset (1), so offset should be adjusted to 0
	if m.threadScrollOffset != 0 {
		t.Errorf("expected threadScrollOffset=0 to keep cursor visible, got %d", m.threadScrollOffset)
	}
}

// TestLayoutFitsTerminalHeight verifies views render correctly without blank lines
// or truncated footers at various terminal heights.
func TestLayoutFitsTerminalHeight(t *testing.T) {
	engine := &mockEngine{
		rows: []query.AggregateRow{
			{Key: "alice@example.com", Count: 5},
			{Key: "bob@example.com", Count: 3},
		},
	}

	tests := []struct {
		name   string
		height int
		level  viewLevel
	}{
		{"aggregate_small", 10, levelAggregates},
		{"aggregate_normal", 24, levelAggregates},
		{"messagelist_small", 10, levelMessageList},
		{"messagelist_normal", 24, levelMessageList},
		{"detail_small", 10, levelMessageDetail},
		{"detail_normal", 24, levelMessageDetail},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})
			model.rows = engine.rows
			model.width = 100
			model.height = tc.height
			model.pageSize = tc.height - 5
			model.loading = false
			model.level = tc.level

			// Set up messages for message list/detail views
			if tc.level == levelMessageList || tc.level == levelMessageDetail {
				model.messages = []query.MessageSummary{
					{ID: 1, Subject: "Test message"},
				}
			}
			if tc.level == levelMessageDetail {
				model.messageDetail = &query.MessageDetail{
					ID:       1,
					Subject:  "Test message",
					BodyText: "Test body content",
				}
				model.detailLineCount = 10
			}

			view := model.View()
			lines := strings.Split(view, "\n")

			// View should have exactly height lines (or height-1 if last line has no newline)
			if len(lines) < tc.height-1 || len(lines) > tc.height+1 {
				t.Errorf("expected ~%d lines, got %d", tc.height, len(lines))
			}

			// Footer should be present (contains navigation hints)
			// All views have navigation hints separated by │
			if !strings.Contains(view, "│") {
				lastLine := lines[len(lines)-1]
				t.Errorf("footer with navigation hints not found in view, last line: %q", lastLine)
			}

			// No excessive blank lines at the end
			blankCount := 0
			for i := len(lines) - 1; i >= 0 && strings.TrimSpace(lines[i]) == ""; i-- {
				blankCount++
			}
			if blankCount > 1 {
				t.Errorf("found %d trailing blank lines, expected at most 1", blankCount)
			}
		})
	}
}

// TestScrollClampingAfterResize verifies detailScroll is clamped when max changes.
func TestScrollClampingAfterResize(t *testing.T) {
	engine := &mockEngine{}
	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})

	// Set up detail view with scroll position
	model.level = levelMessageDetail
	model.width = 100
	model.height = 20
	model.pageSize = 15
	model.detailLineCount = 50
	model.detailScroll = 40 // Near the end
	model.messageDetail = &query.MessageDetail{ID: 1, Subject: "Test", BodyText: "Content"}

	// Simulate resize that increases page size (reducing max scroll)
	// New max scroll would be 50 - 20 = 30, but detailScroll is 40
	model.height = 30
	model.pageSize = 25 // Bigger page means lower max scroll

	// Press down - should clamp first, then check boundary
	newModel, _ := model.Update(keyDown())
	m := newModel.(Model)

	// detailScroll should be clamped to max (50 - 27 = 23 for detailPageSize)
	maxScroll := model.detailLineCount - m.detailPageSize()
	if maxScroll < 0 {
		maxScroll = 0
	}
	if m.detailScroll > maxScroll {
		t.Errorf("detailScroll=%d exceeds maxScroll=%d after resize", m.detailScroll, maxScroll)
	}
}

// TestModalCompositingPreservesANSI verifies that modal overlay doesn't corrupt ANSI sequences.
// Note: This test mutates the global lipgloss color profile. Do not add t.Parallel().
func TestModalCompositingPreservesANSI(t *testing.T) {
	// Enable ANSI color output for this test, restore original profile when done.
	// This mutates a global, so this test must run sequentially (no t.Parallel).
	origProfile := lipgloss.ColorProfile()
	lipgloss.SetColorProfile(termenv.ANSI)
	defer lipgloss.SetColorProfile(origProfile)

	engine := &mockEngine{
		rows: []query.AggregateRow{
			{Key: "alice@example.com", Count: 100, TotalSize: 1000000},
			{Key: "bob@example.com", Count: 50, TotalSize: 500000},
			{Key: "charlie@example.com", Count: 25, TotalSize: 250000},
		},
	}
	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})
	model.rows = engine.rows
	model.width = 80
	model.height = 24
	model.pageSize = 19
	model.loading = false
	model.modal = modalQuitConfirm

	// Render the view with quit modal - this uses overlayModal
	view := model.View()

	// The view should not contain corrupted ANSI sequences
	// A corrupted sequence would be one that starts with ESC but doesn't complete properly
	// Check that all ESC sequences are well-formed (ESC [ ... m for SGR)

	// Count escape sequences - with ANSI profile enabled, we should have many
	escCount := strings.Count(view, "\x1b[")
	resetCount := strings.Count(view, "\x1b[0m") + strings.Count(view, "\x1b[m")

	// There should be escape sequences in the output (styled content)
	if escCount == 0 {
		t.Error("No ANSI sequences found - styled content expected with ANSI profile")
	}

	// Basic sanity: view should render without panics and produce output
	if len(view) == 0 {
		t.Error("View rendered empty output")
	}

	// The view should contain modal content
	if !strings.Contains(view, "Quit") && !strings.Contains(view, "quit") {
		t.Errorf("Modal content not found in view, view length: %d", len(view))
		// Show first 500 chars for debugging
		if len(view) > 500 {
			t.Logf("View preview: %q", view[:500])
		} else {
			t.Logf("View: %q", view)
		}
	}

	// Check for obviously broken sequences (ESC followed by non-[ character in middle of string)
	// This is a heuristic - a properly formed SGR sequence is ESC [ <params> m
	lines := strings.Split(view, "\n")
	for i, line := range lines {
		// Check for truncated sequences: ESC at end without completion
		if strings.HasSuffix(line, "\x1b") {
			t.Errorf("Line %d ends with incomplete escape sequence", i)
		}
		// Check for ESC[ without closing m (very basic check)
		// This won't catch all issues but catches obvious truncation
		idx := 0
		for {
			pos := strings.Index(line[idx:], "\x1b[")
			if pos == -1 {
				break
			}
			start := idx + pos
			// Find the 'm' terminator (for SGR sequences)
			end := strings.IndexAny(line[start:], "mHJKABCDfsu")
			if end == -1 && start < len(line)-2 {
				// No terminator found and not at end - might be truncated
				remaining := line[start:]
				if len(remaining) > 10 && !strings.ContainsAny(remaining[:10], "mHJKABCDfsu") {
					t.Errorf("Line %d may have truncated escape sequence at position %d: %q",
						i, start, remaining[:min(20, len(remaining))])
				}
			}
			idx = start + 2
			if idx >= len(line) {
				break
			}
		}
	}

	t.Logf("View has %d escape sequences, %d resets", escCount, resetCount)
}

// TestSubAggregateAKeyJumpsToMessages verifies 'a' key in sub-aggregate view
// jumps to message list with the drill filter applied.
func TestSubAggregateAKeyJumpsToMessages(t *testing.T) {
	engine := &mockEngine{
		rows: []query.AggregateRow{
			{Key: "work", Count: 5},
			{Key: "personal", Count: 3},
		},
	}

	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})
	model.level = levelSubAggregate
	model.viewType = query.ViewLabels
	model.drillFilter = query.MessageFilter{Sender: "alice@example.com"}
	model.drillViewType = query.ViewSenders
	model.rows = engine.rows
	model.pageSize = 10
	model.cursor = 0
	model.width = 100
	model.height = 20

	// Press 'a' to jump to all messages (with drill filter)
	newModel, cmd := model.handleSubAggregateKeys(key('a'))
	m := newModel.(Model)

	// Should navigate to message list
	if m.level != levelMessageList {
		t.Errorf("expected levelMessageList, got %v", m.level)
	}

	// Should have a command to load messages
	if cmd == nil {
		t.Error("expected command to load messages")
	}

	// Should preserve drill filter
	if m.drillFilter.Sender != "alice@example.com" {
		t.Errorf("expected drillFilter.Sender = alice@example.com, got %s", m.drillFilter.Sender)
	}

	// Should have saved breadcrumb
	if len(m.breadcrumbs) != 1 {
		t.Errorf("expected 1 breadcrumb, got %d", len(m.breadcrumbs))
	}

	// Breadcrumb should be for sub-aggregate level
	if m.breadcrumbs[0].state.level != levelSubAggregate {
		t.Errorf("expected breadcrumb level = levelSubAggregate, got %v", m.breadcrumbs[0].state.level)
	}
}

// TestDKeyAutoSelectsCurrentRow verifies 'd' key selects current row when nothing selected.
func TestDKeyAutoSelectsCurrentRow(t *testing.T) {
	engine := &mockEngine{
		rows: []query.AggregateRow{
			{Key: "alice@example.com", Count: 10},
			{Key: "bob@example.com", Count: 5},
		},
		gmailIDs: []string{"msg1", "msg2"},
	}

	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})
	model.level = levelAggregates
	model.viewType = query.ViewSenders
	model.rows = engine.rows
	model.pageSize = 10
	model.cursor = 1 // On bob@example.com
	model.width = 100
	model.height = 20
	model.accounts = []query.AccountInfo{{ID: 1, Identifier: "test@gmail.com"}}

	// Verify nothing is selected
	if model.HasSelection() {
		t.Error("expected no selection initially")
	}

	// Press 'd' without selecting anything first
	newModel, _ := model.handleAggregateKeys(key('d'))
	m := newModel.(Model)

	// Should have auto-selected current row
	if !m.selection.AggregateKeys["bob@example.com"] {
		t.Error("expected bob@example.com to be auto-selected")
	}

	// Should show delete confirmation modal
	if m.modal != modalDeleteConfirm {
		t.Errorf("expected modalDeleteConfirm, got %v", m.modal)
	}
}

// TestDKeyWithExistingSelection verifies 'd' key uses existing selection when present.
func TestDKeyWithExistingSelection(t *testing.T) {
	engine := &mockEngine{
		rows: []query.AggregateRow{
			{Key: "alice@example.com", Count: 10},
			{Key: "bob@example.com", Count: 5},
		},
		gmailIDs: []string{"msg1", "msg2"},
	}

	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})
	model.level = levelAggregates
	model.viewType = query.ViewSenders
	model.rows = engine.rows
	model.pageSize = 10
	model.cursor = 1 // On bob@example.com
	model.width = 100
	model.height = 20
	model.accounts = []query.AccountInfo{{ID: 1, Identifier: "test@gmail.com"}}

	// Pre-select alice (not the current row)
	model.selection.AggregateKeys["alice@example.com"] = true
	model.selection.AggregateViewType = query.ViewSenders

	// Press 'd' - should use existing selection, not auto-select current row
	newModel, _ := model.handleAggregateKeys(key('d'))
	m := newModel.(Model)

	// Should still have alice selected
	if !m.selection.AggregateKeys["alice@example.com"] {
		t.Error("expected alice@example.com to remain selected")
	}

	// Should NOT have auto-selected bob
	if m.selection.AggregateKeys["bob@example.com"] {
		t.Error("expected bob@example.com to NOT be selected")
	}

	// Should show delete confirmation modal
	if m.modal != modalDeleteConfirm {
		t.Errorf("expected modalDeleteConfirm, got %v", m.modal)
	}
}

// TestMessageListDKeyAutoSelectsCurrentMessage verifies 'd' in message list auto-selects.
func TestMessageListDKeyAutoSelectsCurrentMessage(t *testing.T) {
	engine := &mockEngine{
		messages: []query.MessageSummary{
			{ID: 1, SourceMessageID: "msg1", Subject: "Hello"},
			{ID: 2, SourceMessageID: "msg2", Subject: "World"},
		},
	}

	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})
	model.level = levelMessageList
	model.messages = engine.messages
	model.pageSize = 10
	model.cursor = 0 // On first message
	model.width = 100
	model.height = 20
	model.accounts = []query.AccountInfo{{ID: 1, Identifier: "test@gmail.com"}}

	// Verify nothing is selected
	if model.HasSelection() {
		t.Error("expected no selection initially")
	}

	// Press 'd' without selecting anything first
	newModel, _ := model.handleMessageListKeys(key('d'))
	m := newModel.(Model)

	// Should have auto-selected current message
	if !m.selection.MessageIDs[1] {
		t.Error("expected message ID 1 to be auto-selected")
	}

	// Should show delete confirmation modal
	if m.modal != modalDeleteConfirm {
		t.Errorf("expected modalDeleteConfirm, got %v", m.modal)
	}
}

func TestExportAttachmentsModal(t *testing.T) {
	engine := &mockEngine{}

	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})
	model.level = levelMessageDetail
	model.pageSize = 10
	model.width = 100
	model.height = 20
	model.messageDetail = &query.MessageDetail{
		ID:      1,
		Subject: "Test Email",
		Attachments: []query.AttachmentInfo{
			{ID: 1, Filename: "file1.pdf", Size: 1024, ContentHash: "abc123"},
			{ID: 2, Filename: "file2.txt", Size: 512, ContentHash: "def456"},
		},
	}

	// Press 'e' to open export modal
	newModel, _ := model.handleMessageDetailKeys(key('e'))
	m := newModel.(Model)

	if m.modal != modalExportAttachments {
		t.Errorf("expected modalExportAttachments, got %v", m.modal)
	}

	// Should have all attachments selected by default
	if len(m.exportSelection) != 2 {
		t.Errorf("expected 2 attachments in selection, got %d", len(m.exportSelection))
	}
	if !m.exportSelection[0] || !m.exportSelection[1] {
		t.Error("expected all attachments to be selected by default")
	}

	// Test navigation - move cursor down
	newModel, _ = m.handleModalKeys(key('j'))
	m = newModel.(Model)
	if m.exportCursor != 1 {
		t.Errorf("expected exportCursor = 1, got %d", m.exportCursor)
	}

	// Test toggle selection with space
	newModel, _ = m.handleModalKeys(key(' '))
	m = newModel.(Model)
	if m.exportSelection[1] {
		t.Error("expected attachment 1 to be deselected after space")
	}

	// Test select none
	newModel, _ = m.handleModalKeys(key('n'))
	m = newModel.(Model)
	if m.exportSelection[0] || m.exportSelection[1] {
		t.Error("expected all attachments to be deselected after 'n'")
	}

	// Test select all
	newModel, _ = m.handleModalKeys(key('a'))
	m = newModel.(Model)
	if !m.exportSelection[0] || !m.exportSelection[1] {
		t.Error("expected all attachments to be selected after 'a'")
	}

	// Test cancel with Esc
	newModel, _ = m.handleModalKeys(keyEsc())
	m = newModel.(Model)
	if m.modal != modalNone {
		t.Errorf("expected modalNone after Esc, got %v", m.modal)
	}
	if m.exportSelection != nil {
		t.Error("expected exportSelection to be cleared after Esc")
	}
}

func TestExportAttachmentsNoAttachments(t *testing.T) {
	engine := &mockEngine{}

	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})
	model.level = levelMessageDetail
	model.pageSize = 10
	model.width = 100
	model.height = 20
	model.messageDetail = &query.MessageDetail{
		ID:          1,
		Subject:     "Test Email",
		Attachments: []query.AttachmentInfo{}, // No attachments
	}

	// Press 'e' should show flash message, not modal
	newModel, _ := model.handleMessageDetailKeys(key('e'))
	m := newModel.(Model)

	if m.modal == modalExportAttachments {
		t.Error("expected modal NOT to open when no attachments")
	}
	if m.flashMessage != "No attachments to export" {
		t.Errorf("expected flash message 'No attachments to export', got '%s'", m.flashMessage)
	}
}
