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

func (m *mockEngine) AggregateBySenderName(ctx context.Context, opts query.AggregateOptions) ([]query.AggregateRow, error) {
	return m.rows, nil
}

func (m *mockEngine) AggregateByRecipient(ctx context.Context, opts query.AggregateOptions) ([]query.AggregateRow, error) {
	return m.rows, nil
}

func (m *mockEngine) AggregateByRecipientName(ctx context.Context, opts query.AggregateOptions) ([]query.AggregateRow, error) {
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

func (m *mockEngine) GetAttachment(ctx context.Context, id int64) (*query.AttachmentInfo, error) {
	return nil, nil
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

// TestModelBuilder helps construct Model instances for testing
type TestModelBuilder struct {
	rows          []query.AggregateRow
	messages      []query.MessageSummary
	messageDetail *query.MessageDetail
	gmailIDs      []string
	accounts      []query.AccountInfo
	width         int
	height        int
	pageSize      int  // explicit override; 0 means auto-calculate from height
	rawPageSize   bool // when true, pageSize is set without clamping
	viewType      query.ViewType
	level         viewLevel
	dataDir       string
	version       string
	loading       *bool // nil = auto (false if data provided), non-nil = explicit
	modal         *modalType
	accountFilter *int64
	stats         *query.TotalStats
	contextStats  *query.TotalStats
}

func NewBuilder() *TestModelBuilder {
	return &TestModelBuilder{
		width:   100,
		height:  24,
		dataDir: "/tmp/test",
		version: "test123",
	}
}

func (b *TestModelBuilder) WithRows(rows ...query.AggregateRow) *TestModelBuilder {
	b.rows = rows
	return b
}

func (b *TestModelBuilder) WithMessages(msgs ...query.MessageSummary) *TestModelBuilder {
	b.messages = msgs
	return b
}

func (b *TestModelBuilder) WithDetail(detail *query.MessageDetail) *TestModelBuilder {
	b.messageDetail = detail
	return b
}

func (b *TestModelBuilder) WithGmailIDs(ids ...string) *TestModelBuilder {
	b.gmailIDs = ids
	return b
}

func (b *TestModelBuilder) WithAccounts(accounts ...query.AccountInfo) *TestModelBuilder {
	b.accounts = accounts
	return b
}

func (b *TestModelBuilder) WithSize(width, height int) *TestModelBuilder {
	b.width = width
	b.height = height
	return b
}

func (b *TestModelBuilder) WithLevel(level viewLevel) *TestModelBuilder {
	b.level = level
	return b
}

func (b *TestModelBuilder) WithViewType(vt query.ViewType) *TestModelBuilder {
	b.viewType = vt
	return b
}

func (b *TestModelBuilder) WithPageSize(size int) *TestModelBuilder {
	b.pageSize = size
	b.rawPageSize = false
	return b
}

// WithPageSizeRaw sets pageSize without clamping, allowing zero/negative values for edge-case testing.
func (b *TestModelBuilder) WithPageSizeRaw(size int) *TestModelBuilder {
	b.pageSize = size
	b.rawPageSize = true
	return b
}

func (b *TestModelBuilder) WithLoading(loading bool) *TestModelBuilder {
	b.loading = &loading
	return b
}

func (b *TestModelBuilder) WithModal(mt modalType) *TestModelBuilder {
	b.modal = &mt
	return b
}

func (b *TestModelBuilder) WithAccountFilter(id *int64) *TestModelBuilder {
	b.accountFilter = id
	return b
}

func (b *TestModelBuilder) WithStats(stats *query.TotalStats) *TestModelBuilder {
	b.stats = stats
	return b
}

func (b *TestModelBuilder) WithContextStats(stats *query.TotalStats) *TestModelBuilder {
	b.contextStats = stats
	return b
}

func (b *TestModelBuilder) Build() Model {
	engine := &mockEngine{
		rows:          b.rows,
		messages:      b.messages,
		messageDetail: b.messageDetail,
		gmailIDs:      b.gmailIDs,
	}

	model := New(engine, Options{DataDir: b.dataDir, Version: b.version})
	model.width = b.width
	model.height = b.height
	if b.rawPageSize {
		model.pageSize = b.pageSize
	} else if b.pageSize > 0 {
		model.pageSize = b.pageSize
	} else {
		model.pageSize = b.height - 5
		if model.pageSize < 1 {
			model.pageSize = 1
		}
	}

	// Pre-populate data if provided
	if len(b.rows) > 0 {
		model.rows = b.rows
	}
	if len(b.messages) > 0 {
		model.messages = b.messages
	}
	if b.messageDetail != nil {
		model.messageDetail = b.messageDetail
	}

	// Loading: explicit if set, otherwise false only when data is provided
	if b.loading != nil {
		model.loading = *b.loading
	} else if len(b.rows) > 0 || len(b.messages) > 0 || b.messageDetail != nil {
		model.loading = false
	}

	if b.level != levelAggregates {
		model.level = b.level
	}

	if b.viewType != 0 {
		model.viewType = b.viewType
	}

	if len(b.accounts) > 0 {
		model.accounts = b.accounts
	}

	if b.modal != nil {
		model.modal = *b.modal
	}

	if b.accountFilter != nil {
		model.accountFilter = b.accountFilter
	}

	if b.stats != nil {
		model.stats = b.stats
	}

	if b.contextStats != nil {
		model.contextStats = b.contextStats
	}

	return model
}

// sendKey sends a key message to the model and returns the updated concrete Model
func sendKey(t *testing.T, m Model, k tea.KeyMsg) (Model, tea.Cmd) {
	t.Helper()
	newM, cmd := m.Update(k)
	return newM.(Model), cmd
}

// assertModal checks that the model is in the expected modal state
func assertModal(t *testing.T, m Model, expected modalType) {
	t.Helper()
	if m.modal != expected {
		t.Errorf("expected modal %v, got %v", expected, m.modal)
	}
}

// assertLevel checks that the model is at the expected view level
func assertLevel(t *testing.T, m Model, expected viewLevel) {
	t.Helper()
	if m.level != expected {
		t.Errorf("expected level %v, got %v", expected, m.level)
	}
}

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
	if !m.selection.aggregateKeys[key] {
		t.Errorf("expected %q to be selected", key)
	}
}

// assertNotSelected checks that the given key is not selected.
func assertNotSelected(t *testing.T, m Model, key string) {
	t.Helper()
	if m.selection.aggregateKeys[key] {
		t.Errorf("expected %q to not be selected", key)
	}
}

// assertSelectionCount checks the number of selected items.
func assertSelectionCount(t *testing.T, m Model, expected int) {
	t.Helper()
	got := len(m.selection.aggregateKeys)
	if got != expected {
		t.Errorf("expected %d selected items, got %d", expected, got)
	}
}

// -----------------------------------------------------------------------------
// Key Application Helpers - remove handleXKeys casting boilerplate
// -----------------------------------------------------------------------------

// applyAggregateKey sends a key through handleAggregateKeys and returns the concrete Model.
func applyAggregateKey(t *testing.T, m Model, k tea.KeyMsg) Model {
	t.Helper()
	newModel, _ := m.handleAggregateKeys(k)
	return newModel.(Model)
}

// applyMessageListKey sends a key through handleMessageListKeys and returns the concrete Model.
func applyMessageListKey(t *testing.T, m Model, k tea.KeyMsg) Model {
	t.Helper()
	newModel, _ := m.handleMessageListKeys(k)
	return newModel.(Model)
}

// applyModalKey sends a key through handleModalKeys and returns the concrete Model and Cmd.
func applyModalKey(t *testing.T, m Model, k tea.KeyMsg) (Model, tea.Cmd) {
	t.Helper()
	newModel, cmd := m.handleModalKeys(k)
	return newModel.(Model), cmd
}

// applyDetailKey sends a key through handleMessageDetailKeys and returns the concrete Model.
func applyDetailKey(t *testing.T, m Model, k tea.KeyMsg) Model {
	t.Helper()
	newModel, _ := m.handleMessageDetailKeys(k)
	return newModel.(Model)
}

// -----------------------------------------------------------------------------
// View Fit Helpers - centralize line counting and resize logic
// -----------------------------------------------------------------------------

// countViewLines returns the number of non-trailing-empty lines in a view string.
func countViewLines(view string) int {
	lines := strings.Split(view, "\n")
	actual := len(lines)
	if actual > 0 && lines[actual-1] == "" {
		actual--
	}
	return actual
}

// assertViewFitsHeight checks that the rendered view fits within the given height.
func assertViewFitsHeight(t *testing.T, view string, height int) {
	t.Helper()
	actual := countViewLines(view)
	if actual > height {
		t.Errorf("View has %d lines but terminal height is %d", actual, height)
	}
}

// resizeModel sends a WindowSizeMsg and returns the updated model.
func resizeModel(t *testing.T, m Model, w, h int) Model {
	t.Helper()
	newModel, _ := m.Update(tea.WindowSizeMsg{Width: w, Height: h})
	return newModel.(Model)
}

// =============================================================================
// Tests
// =============================================================================

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

func TestStaleAsyncResponsesIgnored(t *testing.T) {
	model := NewBuilder().
		WithLevel(levelMessageList).
		Build()
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
	model := NewBuilder().
		WithLevel(levelMessageDetail).
		WithSize(100, 30).
		WithPageSize(20).
		Build()
	model.detailRequestID = 10 // Current request ID

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
	model := NewBuilder().
		WithMessages(
			query.MessageSummary{ID: 1, Subject: "Message 1"},
			query.MessageSummary{ID: 2, Subject: "Message 2"},
		).
		WithLevel(levelMessageList).
		WithSize(100, 30).
		WithPageSize(20).
		Build()
	model.detailLineCount = 100 // Simulate previous message with 100 lines
	model.detailScroll = 50     // Simulate scrolled position

	// Trigger drill-down to detail view
	model.cursor = 0
	m := applyMessageListKey(t, model, keyEnter())

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
	model := NewBuilder().
		WithLevel(levelMessageDetail).
		WithPageSize(10).
		Build()
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
	model := NewBuilder().
		WithLevel(levelMessageDetail).
		WithDetail(&query.MessageDetail{
			Subject:  "Test Subject",
			BodyText: "Line 1\nLine 2\nLine 3\nLine 4\nLine 5",
		}).
		WithSize(80, 20).
		WithPageSize(14).
		Build()

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
	model := NewBuilder().
		WithLevel(levelMessageDetail).
		WithPageSize(20).
		Build()
	model.detailLineCount = 0 // No content yet (loading)
	model.detailScroll = 0

	// Press 'G' (end key) with zero line count
	m := applyDetailKey(t, model, key('G'))

	// Should not crash and scroll should remain 0
	if m.detailScroll != 0 {
		t.Errorf("expected detailScroll = 0 with zero line count, got %d", m.detailScroll)
	}
}

func TestQuitConfirmationModal(t *testing.T) {
	model := NewBuilder().Build()

	// Press 'q' should open quit confirmation, not quit immediately
	var cmd tea.Cmd
	model, cmd = sendKey(t, model, key('q'))

	assertModal(t, model, modalQuitConfirm)

	if model.quitting {
		t.Error("should not be quitting yet")
	}
	if cmd != nil {
		t.Error("should not have quit command yet")
	}

	// Press 'n' to cancel
	model, _ = sendKey(t, model, key('n'))

	assertModal(t, model, modalNone)
}

func TestQuitConfirmationConfirm(t *testing.T) {
	model := NewBuilder().WithModal(modalQuitConfirm).WithPageSize(10).WithSize(100, 20).Build()

	// Press 'y' to confirm quit
	m, cmd := applyModalKey(t, model, key('y'))

	if !m.quitting {
		t.Error("expected quitting = true")
	}
	if cmd == nil {
		t.Error("expected quit command")
	}
}

func TestAccountSelectorModal(t *testing.T) {
	model := NewBuilder().
		WithAccounts(
			query.AccountInfo{ID: 1, Identifier: "alice@example.com"},
			query.AccountInfo{ID: 2, Identifier: "bob@example.com"},
		).
		WithPageSize(10).WithSize(100, 20).
		Build()

	// Press 'A' to open account selector
	m := applyAggregateKey(t, model, key('A'))

	if m.modal != modalAccountSelector {
		t.Errorf("expected modalAccountSelector, got %v", m.modal)
	}
	if m.modalCursor != 0 {
		t.Errorf("expected modalCursor = 0 (All Accounts), got %d", m.modalCursor)
	}

	// Navigate down
	m, _ = applyModalKey(t, m, key('j'))
	if m.modalCursor != 1 {
		t.Errorf("expected modalCursor = 1, got %d", m.modalCursor)
	}

	// Select account
	var cmd tea.Cmd
	m, cmd = applyModalKey(t, m, keyEnter())

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
	model := NewBuilder().WithPageSize(10).WithSize(100, 20).Build()

	// Press 'f' to open filter modal
	m := applyAggregateKey(t, model, key('f'))

	if m.modal != modalAttachmentFilter {
		t.Errorf("expected modalAttachmentFilter, got %v", m.modal)
	}
	if m.modalCursor != 0 {
		t.Errorf("expected modalCursor = 0 (All Messages), got %d", m.modalCursor)
	}

	// Navigate down to "With Attachments"
	m, _ = applyModalKey(t, m, key('j'))
	if m.modalCursor != 1 {
		t.Errorf("expected modalCursor = 1, got %d", m.modalCursor)
	}

	// Select "With Attachments"
	m, _ = applyModalKey(t, m, keyEnter())

	if m.modal != modalNone {
		t.Errorf("expected modalNone after selection, got %v", m.modal)
	}
	if !m.attachmentFilter {
		t.Error("expected attachmentFilter = true")
	}
}

func TestAttachmentFilterInMessageList(t *testing.T) {
	model := NewBuilder().WithLevel(levelMessageList).WithPageSize(10).WithSize(100, 20).Build()

	// Press 'f' to open filter modal in message list
	m := applyMessageListKey(t, model, key('f'))

	if m.modal != modalAttachmentFilter {
		t.Errorf("expected modalAttachmentFilter, got %v", m.modal)
	}

	// Select "With Attachments" and verify reload is triggered
	m.modalCursor = 1
	var cmd tea.Cmd
	m, cmd = applyModalKey(t, m, keyEnter())

	if !m.attachmentFilter {
		t.Error("expected attachmentFilter = true")
	}
	if cmd == nil {
		t.Error("expected command to reload messages")
	}
}

func TestSubGroupingNavigation(t *testing.T) {
	rows := []query.AggregateRow{
		{Key: "alice@example.com", Count: 10},
		{Key: "bob@example.com", Count: 5},
	}
	msgs := []query.MessageSummary{
		{ID: 1, Subject: "Test 1"},
		{ID: 2, Subject: "Test 2"},
	}

	model := NewBuilder().WithRows(rows...).WithMessages(msgs...).
		WithPageSize(10).WithSize(100, 20).WithViewType(query.ViewSenders).Build()

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
	m.messages = msgs // Simulate messages loaded
	newModel, cmd = m.handleMessageListKeys(keyTab())
	m = newModel.(Model)

	if m.level != levelDrillDown {
		t.Errorf("expected levelDrillDown after Tab, got %v", m.level)
	}
	// Default sub-group after drilling from Senders should be Recipients (skips redundant SenderNames)
	if m.viewType != query.ViewRecipients {
		t.Errorf("expected viewType = ViewRecipients for sub-grouping, got %v", m.viewType)
	}
	if cmd == nil {
		t.Error("expected command to load sub-aggregate data")
	}

	// Test Tab in sub-aggregate cycles views (skipping drill view type)
	m.rows = rows // Simulate data loaded
	newModel, cmd = m.handleAggregateKeys(keyTab())
	m = newModel.(Model)

	// From ViewRecipients, Tab cycles to ViewRecipientNames
	if m.viewType != query.ViewRecipientNames {
		t.Errorf("expected viewType = ViewRecipientNames after Tab, got %v", m.viewType)
	}
	if cmd == nil {
		t.Error("expected command to reload data after Tab")
	}

	// Test Esc goes back to message list (not all the way to aggregates)
	m.rows = rows
	m = applyAggregateKey(t, m, keyEsc())

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
	m.messages = msgs
	m = applyMessageListKey(t, m, keyEsc())

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
	model := NewBuilder().WithLevel(levelMessageDetail).WithSize(80, 24).WithPageSize(19).Build()

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
	model := NewBuilder().Build()

	// Simulate negative window size (can happen during terminal resize)
	m := resizeModel(t, model, -1, -1)

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

func TestDefaultLoadingWithNoData(t *testing.T) {
	// Build with no rows/messages and no explicit loading override.
	// The builder should preserve New()'s default loading=true.
	model := NewBuilder().WithPageSize(10).WithSize(100, 20).Build()

	if !model.loading {
		t.Errorf("expected loading=true (New default) when no data provided, got false")
	}
}

func TestPageSizeRawZeroAndNegative(t *testing.T) {
	tests := []struct {
		name     string
		pageSize int
	}{
		{"zero page size", 0},
		{"negative page size", -1},
		{"large negative page size", -100},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Should not panic when building or rendering with raw zero/negative page sizes.
			model := NewBuilder().
				WithPageSizeRaw(tc.pageSize).
				WithRows(testAggregateRows...).
				WithSize(100, 20).
				Build()

			if model.pageSize != tc.pageSize {
				t.Errorf("expected pageSize=%d, got %d", tc.pageSize, model.pageSize)
			}

			// Rendering should not panic even with unusual page sizes.
			_ = model.View()
		})
	}
}

func TestWithPageSizeClearsRawFlag(t *testing.T) {
	// WithPageSizeRaw followed by WithPageSize should clear the raw flag,
	// so the normal clamping logic applies.
	model := NewBuilder().
		WithPageSizeRaw(0).
		WithPageSize(10).
		WithRows(testAggregateRows...).
		WithSize(100, 20).
		Build()

	if model.pageSize != 10 {
		t.Errorf("expected pageSize=10 after WithPageSize cleared raw flag, got %d", model.pageSize)
	}
}

func TestSubAggregateDrillDown(t *testing.T) {
	model := NewBuilder().
		WithRows(query.AggregateRow{Key: "bob@example.com", Count: 3}).
		WithMessages(query.MessageSummary{ID: 1, Subject: "Test"}).
		WithPageSize(10).WithSize(100, 20).
		WithLevel(levelDrillDown).WithViewType(query.ViewRecipients).
		Build()
	model.drillViewType = query.ViewSenders
	model.drillFilter = query.MessageFilter{Sender: "alice@example.com"}

	// Press Enter on recipient - should go to message list with combined filter
	newModel, cmd := model.handleAggregateKeys(keyEnter())
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

func TestSearchModalOpen(t *testing.T) {
	model := NewBuilder().
		WithRows(makeRow("alice@example.com", 10)).
		Build()

	// Press '/' to activate inline search
	var cmd tea.Cmd
	model, cmd = sendKey(t, model, key('/'))

	if !model.inlineSearchActive {
		t.Error("expected inlineSearchActive = true")
	}
	if model.searchMode != searchModeFast {
		t.Errorf("expected searchModeFast, got %v", model.searchMode)
	}
	// Should return a command for textinput blink
	if cmd == nil {
		t.Error("expected textinput command")
	}
}

// TestSearchResultsDisplay verifies search results are displayed.
func TestSearchResultsDisplay(t *testing.T) {
	model := NewBuilder().WithPageSize(10).WithSize(100, 20).Build()
	model.searchQuery = "test query"
	model.searchMode = searchModeFast
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
	model := NewBuilder().WithPageSize(10).WithSize(100, 20).Build()
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

// TestInlineSearchTabToggleAtMessageList verifies Tab toggles mode and triggers search at message list level.
func TestInlineSearchTabToggleAtMessageList(t *testing.T) {
	model := NewBuilder().WithPageSize(10).WithSize(100, 20).
		WithLevel(levelMessageList).
		WithMessages(query.MessageSummary{ID: 1, Subject: "Existing"}).
		Build()
	model.inlineSearchActive = true
	model.searchMode = searchModeFast
	model.searchInput.SetValue("test query")

	// Press Tab to toggle to Deep mode
	newModel, cmd := model.handleInlineSearchKeys(keyTab())
	m := newModel.(Model)

	// Mode should toggle to Deep
	if m.searchMode != searchModeDeep {
		t.Errorf("expected searchModeDeep after Tab, got %v", m.searchMode)
	}

	// Should set loading state
	if !m.inlineSearchLoading {
		t.Error("expected inlineSearchLoading = true after Tab toggle with query")
	}

	// Should NOT clear messages (transitionBuffer handles the transition)
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
	model := NewBuilder().WithPageSize(10).WithSize(100, 20).
		WithLevel(levelMessageList).WithLoading(false).Build()
	model.inlineSearchActive = true
	model.searchMode = searchModeFast
	model.searchInput.SetValue("") // Empty query

	// Press Tab to toggle mode
	newModel, cmd := model.handleInlineSearchKeys(keyTab())
	m := newModel.(Model)

	// Mode should still toggle
	if m.searchMode != searchModeDeep {
		t.Errorf("expected searchModeDeep after Tab, got %v", m.searchMode)
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
	model := NewBuilder().WithPageSize(10).WithSize(100, 20).Build()
	model.inlineSearchActive = true
	model.searchMode = searchModeFast
	model.searchInput.SetValue("test query")

	// Press Tab - should do nothing at aggregate level
	newModel, cmd := model.handleInlineSearchKeys(keyTab())
	m := newModel.(Model)

	// Mode should NOT toggle (Tab disabled at aggregate level)
	if m.searchMode != searchModeFast {
		t.Errorf("expected searchModeFast unchanged at aggregate level, got %v", m.searchMode)
	}

	// Should NOT trigger any command
	if cmd != nil {
		t.Error("expected no command when Tab pressed at aggregate level")
	}
}

// TestInlineSearchTabToggleBackToFast verifies Tab toggles back from Deep to Fast.
func TestInlineSearchTabToggleBackToFast(t *testing.T) {
	model := NewBuilder().WithPageSize(10).WithSize(100, 20).
		WithLevel(levelMessageList).Build()
	model.inlineSearchActive = true
	model.searchMode = searchModeDeep // Start in Deep mode
	model.searchInput.SetValue("test query")

	// Press Tab to toggle back to Fast mode
	newModel, cmd := model.handleInlineSearchKeys(keyTab())
	m := newModel.(Model)

	// Mode should toggle back to Fast
	if m.searchMode != searchModeFast {
		t.Errorf("expected searchModeFast after Tab from Deep, got %v", m.searchMode)
	}

	// Should trigger a search command
	if cmd == nil {
		t.Error("expected search command when toggling back to Fast")
	}
}

// TestSpinnerAppearsInViewWhenLoading verifies spinner character appears in rendered view.
func TestSpinnerAppearsInViewWhenLoading(t *testing.T) {
	model := NewBuilder().
		WithRows(query.AggregateRow{Key: "test@example.com", Count: 10}).
		WithPageSize(10).WithSize(100, 20).Build()

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
	model := NewBuilder().WithPageSize(10).WithSize(100, 20).
		WithLevel(levelMessageList).Build()
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
	model := NewBuilder().
		WithRows(query.AggregateRow{Key: "bob@example.com", Count: 3}).
		WithPageSize(10).WithSize(100, 20).
		WithLevel(levelDrillDown).WithViewType(query.ViewRecipients).
		Build()
	model.drillViewType = query.ViewSenders
	model.drillFilter = query.MessageFilter{Sender: "alice@example.com"}

	// Press '/' to activate inline search
	newModel, cmd := model.handleAggregateKeys(key('/'))
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
	model := NewBuilder().
		WithMessages(query.MessageSummary{ID: 1, Subject: "Test"}).
		WithPageSize(10).WithSize(100, 20).
		WithLevel(levelMessageList).Build()

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
	model := NewBuilder().
		WithRows(query.AggregateRow{Key: "alice@example.com", Count: 10}).
		WithPageSize(10).WithSize(100, 20).
		WithViewType(query.ViewSenders).Build()
	// Set non-zero cursor/scroll to verify reset
	model.cursor = 5
	model.scrollOffset = 3

	// Press 'g' - should cycle to SenderNames (not go to home)
	newModel, cmd := model.handleAggregateKeys(key('g'))
	m := newModel.(Model)

	// Expected: viewType changes to ViewSenderNames
	if m.viewType != query.ViewSenderNames {
		t.Errorf("expected ViewSenderNames after 'g', got %v", m.viewType)
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
	model := NewBuilder().
		WithRows(query.AggregateRow{Key: "test", Count: 10}).
		WithPageSize(10).WithSize(100, 20).
		WithViewType(query.ViewSenders).Build()

	expectedOrder := []query.ViewType{
		query.ViewSenderNames,
		query.ViewRecipients,
		query.ViewRecipientNames,
		query.ViewDomains,
		query.ViewLabels,
		query.ViewTime,
		query.ViewSenders, // Cycles back
	}

	for i, expected := range expectedOrder {
		model = applyAggregateKey(t, model, key('g'))
		model.loading = false // Reset for next iteration

		if model.viewType != expected {
			t.Errorf("cycle %d: expected %v, got %v", i+1, expected, model.viewType)
		}
	}
}

// TestGKeyInSubAggregate verifies 'g' cycles view types in sub-aggregate view.
func TestGKeyInSubAggregate(t *testing.T) {
	model := NewBuilder().
		WithRows(query.AggregateRow{Key: "bob@example.com", Count: 5}).
		WithPageSize(10).WithSize(100, 20).
		WithLevel(levelDrillDown).WithViewType(query.ViewRecipients).
		Build()
	model.drillViewType = query.ViewSenders // Drilled from Senders
	model.drillFilter = query.MessageFilter{Sender: "alice@example.com"}

	// Press 'g' - should cycle to next view type, skipping drillViewType
	m := applyAggregateKey(t, model, key('g'))

	// Should skip ViewSenders (the drillViewType) and go to RecipientNames
	if m.viewType != query.ViewRecipientNames {
		t.Errorf("expected ViewRecipientNames (skipping drillViewType), got %v", m.viewType)
	}
}

// TestGKeyInMessageListWithDrillFilter verifies 'g' switches to sub-aggregate view
// when there's a drill filter.
func TestGKeyInMessageListWithDrillFilter(t *testing.T) {
	model := NewBuilder().
		WithMessages(
			query.MessageSummary{ID: 1, Subject: "Test 1"},
			query.MessageSummary{ID: 2, Subject: "Test 2"},
			query.MessageSummary{ID: 3, Subject: "Test 3"},
		).
		WithPageSize(10).WithSize(100, 20).
		WithLevel(levelMessageList).WithViewType(query.ViewSenders).
		Build()
	model.cursor = 2 // Start at third item
	model.scrollOffset = 1
	// Set up a drill filter so 'g' triggers sub-grouping
	model.drillFilter = query.MessageFilter{Sender: "alice@example.com"}
	model.drillViewType = query.ViewSenders

	// Press 'g' - should switch to sub-aggregate view
	m := applyMessageListKey(t, model, key('g'))

	if m.level != levelDrillDown {
		t.Errorf("expected level=levelDrillDown after 'g' with drill filter, got %v", m.level)
	}
	// ViewType should be next logical view (Recipients after Senders, skipping SenderNames)
	if m.viewType != query.ViewRecipients {
		t.Errorf("expected viewType=Recipients after 'g', got %v", m.viewType)
	}
}

// TestTKeyInMessageListJumpsToTimeSubGroup verifies that pressing 't' in a
// drilled-down message list enters sub-grouping with ViewTime.
func TestTKeyInMessageListJumpsToTimeSubGroup(t *testing.T) {
	model := NewBuilder().
		WithMessages(
			query.MessageSummary{ID: 1, Subject: "Test 1"},
			query.MessageSummary{ID: 2, Subject: "Test 2"},
		).
		WithPageSize(10).WithSize(100, 20).
		WithLevel(levelMessageList).WithViewType(query.ViewSenders).
		Build()
	model.drillFilter = query.MessageFilter{Sender: "alice@example.com"}
	model.drillViewType = query.ViewSenders

	m := applyMessageListKey(t, model, key('t'))

	if m.level != levelDrillDown {
		t.Errorf("expected level=levelDrillDown after 't', got %v", m.level)
	}
	if m.viewType != query.ViewTime {
		t.Errorf("expected viewType=ViewTime after 't', got %v", m.viewType)
	}
}

// TestTKeyInMessageListFromTimeDrillIsNoop verifies that pressing 't' when
// the drill dimension is already Time is a no-op (avoids redundant sub-aggregate).
func TestTKeyInMessageListFromTimeDrillIsNoop(t *testing.T) {
	model := NewBuilder().
		WithMessages(
			query.MessageSummary{ID: 1, Subject: "Test 1"},
		).
		WithPageSize(10).WithSize(100, 20).
		WithLevel(levelMessageList).WithViewType(query.ViewTime).
		Build()
	model.drillFilter = query.MessageFilter{TimePeriod: "2024-01"}
	model.drillViewType = query.ViewTime

	m := applyMessageListKey(t, model, key('t'))

	if m.level != levelMessageList {
		t.Errorf("expected level unchanged at levelMessageList, got %v", m.level)
	}
	if m.loading {
		t.Error("expected loading=false (no-op)")
	}
}

// TestTKeyInMessageListNoDrillFilterIsNoop verifies that 't' does nothing
// in message list without a drill filter.
func TestTKeyInMessageListNoDrillFilterIsNoop(t *testing.T) {
	model := NewBuilder().
		WithMessages(
			query.MessageSummary{ID: 1, Subject: "Test 1"},
		).
		WithPageSize(10).WithSize(100, 20).
		WithLevel(levelMessageList).Build()

	m := applyMessageListKey(t, model, key('t'))

	if m.level != levelMessageList {
		t.Errorf("expected level unchanged at levelMessageList, got %v", m.level)
	}
}

// TestNextSubGroupViewSkipsSenderNames verifies that drilling from Senders
// skips SenderNames (redundant) and goes straight to Recipients.
func TestNextSubGroupViewSkipsSenderNames(t *testing.T) {
	model := NewBuilder().
		WithMessages(
			query.MessageSummary{ID: 1, Subject: "Test 1"},
		).
		WithPageSize(10).WithSize(100, 20).
		WithLevel(levelMessageList).WithViewType(query.ViewSenders).
		Build()
	model.drillFilter = query.MessageFilter{Sender: "alice@example.com"}
	model.drillViewType = query.ViewSenders

	m := applyMessageListKey(t, model, key('g'))

	if m.viewType != query.ViewRecipients {
		t.Errorf("expected sub-group from Senders to be Recipients (skip SenderNames), got %v", m.viewType)
	}
}

// TestNextSubGroupViewSkipsRecipientNames verifies that drilling from Recipients
// skips RecipientNames (redundant) and goes straight to Domains.
func TestNextSubGroupViewSkipsRecipientNames(t *testing.T) {
	model := NewBuilder().
		WithMessages(
			query.MessageSummary{ID: 1, Subject: "Test 1"},
		).
		WithPageSize(10).WithSize(100, 20).
		WithLevel(levelMessageList).WithViewType(query.ViewRecipients).
		Build()
	model.drillFilter = query.MessageFilter{Recipient: "bob@example.com"}
	model.drillViewType = query.ViewRecipients

	m := applyMessageListKey(t, model, key('g'))

	if m.viewType != query.ViewDomains {
		t.Errorf("expected sub-group from Recipients to be Domains (skip RecipientNames), got %v", m.viewType)
	}
}

// TestNextSubGroupViewFromSenderNamesKeepsRecipients verifies that drilling from
// SenderNames goes to Recipients (name→email sub-grouping is useful).
func TestNextSubGroupViewFromSenderNamesKeepsRecipients(t *testing.T) {
	model := NewBuilder().
		WithMessages(
			query.MessageSummary{ID: 1, Subject: "Test 1"},
		).
		WithPageSize(10).WithSize(100, 20).
		WithLevel(levelMessageList).WithViewType(query.ViewSenderNames).
		Build()
	model.drillFilter = query.MessageFilter{SenderName: "Alice"}
	model.drillViewType = query.ViewSenderNames

	m := applyMessageListKey(t, model, key('g'))

	if m.viewType != query.ViewRecipients {
		t.Errorf("expected sub-group from SenderNames to be Recipients, got %v", m.viewType)
	}
}

// TestNextSubGroupViewFromRecipientNamesKeepsDomains verifies that drilling from
// RecipientNames goes to Domains.
func TestNextSubGroupViewFromRecipientNamesKeepsDomains(t *testing.T) {
	model := NewBuilder().
		WithMessages(
			query.MessageSummary{ID: 1, Subject: "Test 1"},
		).
		WithPageSize(10).WithSize(100, 20).
		WithLevel(levelMessageList).WithViewType(query.ViewRecipientNames).
		Build()
	model.drillFilter = query.MessageFilter{RecipientName: "Bob"}
	model.drillViewType = query.ViewRecipientNames

	m := applyMessageListKey(t, model, key('g'))

	if m.viewType != query.ViewDomains {
		t.Errorf("expected sub-group from RecipientNames to be Domains, got %v", m.viewType)
	}
}

// TestNextSubGroupViewFromDomainsGoesToLabels verifies the standard chain continues.
func TestNextSubGroupViewFromDomainsGoesToLabels(t *testing.T) {
	model := NewBuilder().
		WithMessages(
			query.MessageSummary{ID: 1, Subject: "Test 1"},
		).
		WithPageSize(10).WithSize(100, 20).
		WithLevel(levelMessageList).WithViewType(query.ViewDomains).
		Build()
	model.drillFilter = query.MessageFilter{Domain: "example.com"}
	model.drillViewType = query.ViewDomains

	m := applyMessageListKey(t, model, key('g'))

	if m.viewType != query.ViewLabels {
		t.Errorf("expected sub-group from Domains to be Labels, got %v", m.viewType)
	}
}

// TestGKeyInMessageListNoDrillFilter verifies 'g' goes back to aggregates when no drill filter.
func TestGKeyInMessageListNoDrillFilter(t *testing.T) {
	model := NewBuilder().
		WithMessages(
			query.MessageSummary{ID: 1, Subject: "Test 1"},
			query.MessageSummary{ID: 2, Subject: "Test 2"},
			query.MessageSummary{ID: 3, Subject: "Test 3"},
		).
		WithPageSize(10).WithSize(100, 20).
		WithLevel(levelMessageList).Build()
	model.cursor = 2 // Start at third item
	model.scrollOffset = 1
	// No drill filter - 'g' should go back to aggregates

	// Press 'g' - should go back to aggregate view
	m := applyMessageListKey(t, model, key('g'))

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
	msgs := make([]query.MessageSummary, 100)
	for i := 0; i < 100; i++ {
		msgs[i] = query.MessageSummary{ID: int64(i + 1), Subject: fmt.Sprintf("Test %d", i+1)}
	}

	model := NewBuilder().WithMessages(msgs...).
		WithPageSize(20).WithSize(100, 30).
		WithLevel(levelMessageList).Build()
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
	model := NewBuilder().
		WithRows(query.AggregateRow{Key: "test", Count: 10}).
		WithPageSize(10).WithSize(100, 20).
		WithViewType(query.ViewSenders).Build()
	// Set non-zero cursor/scroll to verify reset
	model.cursor = 5
	model.scrollOffset = 3

	// Press Tab - should cycle to SenderNames
	newModel, cmd := model.handleAggregateKeys(keyTab())
	m := newModel.(Model)

	if m.viewType != query.ViewSenderNames {
		t.Errorf("expected ViewSenderNames after Tab, got %v", m.viewType)
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
	model := NewBuilder().
		WithRows(
			query.AggregateRow{Key: "a", Count: 1},
			query.AggregateRow{Key: "b", Count: 2},
			query.AggregateRow{Key: "c", Count: 3},
		).
		WithPageSize(10).WithSize(100, 20).Build()
	model.cursor = 2
	model.scrollOffset = 1

	// Press 'home' - should go to top
	m := applyAggregateKey(t, model, keyHome())

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
	model := NewBuilder().
		WithRows(query.AggregateRow{Key: "alice@example.com", Count: 100, TotalSize: 500000}).
		WithMessages(query.MessageSummary{ID: 1, Subject: "Test"}).
		WithPageSize(10).WithSize(100, 20).
		WithViewType(query.ViewSenders).Build()

	// Drill down
	m := applyAggregateKey(t, model, keyEnter())

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
	msgs := []query.MessageSummary{{ID: 1, Subject: "Test"}}
	model := NewBuilder().
		WithRows(
			query.AggregateRow{Key: "alice@example.com", Count: 100, TotalSize: 500000},
			query.AggregateRow{Key: "bob@example.com", Count: 50, TotalSize: 250000},
		).
		WithMessages(msgs...).
		WithPageSize(10).WithSize(100, 20).
		WithViewType(query.ViewSenders).Build()

	// Step 1: Drill down to message list (sets contextStats from alice's row)
	m := applyAggregateKey(t, model, keyEnter())
	if m.contextStats == nil || m.contextStats.MessageCount != 100 {
		t.Fatalf("expected contextStats.MessageCount=100, got %v", m.contextStats)
	}

	// Simulate messages loaded and transition to message list level
	m.level = levelMessageList
	m.messages = msgs
	m.filterKey = "alice@example.com"
	originalContextStats := m.contextStats

	// Step 2: Press Tab to go to sub-aggregate view (contextStats saved in breadcrumb)
	m2 := applyMessageListKey(t, m, keyTab())
	// Simulate data load completing with sub-aggregate rows
	m2.rows = []query.AggregateRow{
		{Key: "domain1.com", Count: 60, TotalSize: 300000},
		{Key: "domain2.com", Count: 40, TotalSize: 200000},
	}
	m2.loading = false
	if m2.level != levelDrillDown {
		t.Fatalf("expected levelDrillDown after Tab, got %v", m2.level)
	}
	// contextStats should still be the same (alice's stats)
	if m2.contextStats != originalContextStats {
		t.Errorf("contextStats should be preserved after Tab")
	}

	// Step 3: Drill down from sub-aggregate to message list (contextStats overwritten)
	m3 := applyAggregateKey(t, m2, keyEnter())
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
	if m4.level != levelDrillDown {
		t.Fatalf("expected levelDrillDown after goBack, got %v", m4.level)
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
	model := NewBuilder().WithSize(100, 20).WithLevel(levelMessageList).
		WithStats(&query.TotalStats{MessageCount: 10000, TotalSize: 50000000, AttachmentCount: 500}).
		WithContextStats(&query.TotalStats{MessageCount: 100, TotalSize: 500000}).
		Build()

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
	model := NewBuilder().WithSize(120, 20).WithLevel(levelMessageList).
		WithStats(&query.TotalStats{MessageCount: 10000, TotalSize: 50000000, AttachmentCount: 500}).
		WithContextStats(&query.TotalStats{MessageCount: 100, TotalSize: 500000, AttachmentCount: 42}).
		Build()

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
	model := NewBuilder().WithSize(120, 20).WithLevel(levelMessageList).
		WithStats(&query.TotalStats{MessageCount: 10000, TotalSize: 50000000, AttachmentCount: 500}).
		WithContextStats(&query.TotalStats{MessageCount: 100, TotalSize: 500000, AttachmentCount: 0}).
		Build()

	header := model.headerView()

	// Should show "0 attchs" even when attachment count is 0
	if !strings.Contains(header, "0 attchs") {
		t.Errorf("header should contain '0 attchs' when AttachmentCount is 0, got: %s", header)
	}
}

// TestPositionShowsTotalFromContextStats verifies footer shows "N of M" when total > loaded.
func TestPositionShowsTotalFromContextStats(t *testing.T) {
	// Create 100 loaded messages but contextStats says 500 total
	messages := make([]query.MessageSummary, 100)
	for i := range messages {
		messages[i] = query.MessageSummary{ID: int64(i + 1), Subject: fmt.Sprintf("Msg %d", i+1)}
	}

	model := NewBuilder().WithMessages(messages...).
		WithPageSize(20).WithSize(100, 30).
		WithLevel(levelMessageList).
		WithContextStats(&query.TotalStats{MessageCount: 500}).
		Build()
	model.cursor = 49 // 50th message

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
	messages := make([]query.MessageSummary, 50)
	for i := range messages {
		messages[i] = query.MessageSummary{ID: int64(i + 1)}
	}

	model := NewBuilder().WithMessages(messages...).
		WithPageSize(20).WithSize(100, 30).
		WithLevel(levelMessageList).
		WithContextStats(&query.TotalStats{MessageCount: 50}).
		Build()
	model.cursor = 24

	footer := model.footerView()

	// Should show "25/50" (standard format when all loaded)
	if !strings.Contains(footer, "25/50") {
		t.Errorf("expected footer to contain '25/50', got: %s", footer)
	}
}

// TestPositionShowsLoadedCountWhenNoContextStats verifies footer falls back to loaded count.
func TestPositionShowsLoadedCountWhenNoContextStats(t *testing.T) {
	messages := make([]query.MessageSummary, 75)
	for i := range messages {
		messages[i] = query.MessageSummary{ID: int64(i + 1)}
	}

	model := NewBuilder().WithMessages(messages...).
		WithPageSize(20).WithSize(100, 30).
		WithLevel(levelMessageList).Build()
	model.cursor = 49

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
	messages := make([]query.MessageSummary, 100)
	for i := range messages {
		messages[i] = query.MessageSummary{ID: int64(i + 1)}
	}

	model := NewBuilder().WithMessages(messages...).
		WithPageSize(20).WithSize(100, 30).
		WithLevel(levelMessageList).
		WithContextStats(&query.TotalStats{MessageCount: 50}).
		Build()
	model.cursor = 49

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
	// Simulate 500 messages loaded (the limit)
	messages := make([]query.MessageSummary, 500)
	for i := range messages {
		messages[i] = query.MessageSummary{ID: int64(i + 1)}
	}

	model := NewBuilder().WithMessages(messages...).
		WithPageSize(20).WithSize(100, 30).
		WithLevel(levelMessageList).
		WithStats(&query.TotalStats{MessageCount: 175000}).
		Build()
	model.cursor = 99        // 100th message
	model.allMessages = true  // All Messages view

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
	model := NewBuilder().Build()

	// Press '?'
	newModel, _ := model.Update(key('?'))
	m := newModel.(Model)

	if m.modal != modalHelp {
		t.Errorf("expected modalHelp after '?', got %v", m.modal)
	}
}

// TestHelpModalClosesOnAnyKey verifies help modal closes on any key.
func TestHelpModalClosesOnAnyKey(t *testing.T) {
	model := NewBuilder().WithModal(modalHelp).Build()

	// Press any key (e.g., Enter)
	newModel, _ := model.Update(keyEnter())
	m := newModel.(Model)

	if m.modal != modalNone {
		t.Errorf("expected modalNone after pressing key in help, got %v", m.modal)
	}
}

// TestVKeyReversesSortOrder verifies 'v' reverses sort direction.
func TestVKeyReversesSortOrder(t *testing.T) {
	model := NewBuilder().WithRows(query.AggregateRow{Key: "test", Count: 1}).Build()
	model.sortDirection = query.SortDesc

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
	model := NewBuilder().Build()
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
	model := NewBuilder().
		WithContextStats(&query.TotalStats{MessageCount: 500}).
		Build()
	model.searchRequestID = 1

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
	model := NewBuilder().
		WithMessages(make([]query.MessageSummary, 50)...).
		WithLevel(levelMessageList).
		WithContextStats(&query.TotalStats{MessageCount: 50}).
		Build()
	model.searchRequestID = 1
	model.searchTotalCount = -1 // Unknown total

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
	m := applyAggregateKey(t, model, keyEnter())

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

// TestAggregateSearchFilterUsesFilteredStats verifies that contextStats uses
// the filteredStats from the query (distinct message count) rather than summing
// row counts, which would overcount for 1:N views like Recipients and Labels.
func TestAggregateSearchFilterUsesFilteredStats(t *testing.T) {
	model := newTestModelAtLevel(levelAggregates).
		withSearchQuery("test query").
		withAggregateRequestID(1)

	// Simulate recipient view: rows sum to 175 (inflated) but actual distinct is 100
	filteredStats := &query.TotalStats{MessageCount: 100, TotalSize: 5000, AttachmentCount: 10}
	msg := dataLoadedMsg{
		rows: []query.AggregateRow{
			{Key: "alice@example.com", Count: 80, TotalSize: 4000, AttachmentCount: 5},
			{Key: "bob@example.com", Count: 60, TotalSize: 3000, AttachmentCount: 3},
			{Key: "carol@example.com", Count: 35, TotalSize: 1500, AttachmentCount: 2},
		},
		filteredStats: filteredStats,
		requestID:     1,
	}

	newModel, _ := model.Update(msg)
	m := newModel.(Model)

	if m.contextStats == nil {
		t.Fatal("expected contextStats to be set")
	}
	// Should use filteredStats (100), not sum of row counts (175)
	if m.contextStats.MessageCount != 100 {
		t.Errorf("contextStats.MessageCount = %d, want 100 (from filteredStats, not row sum 175)", m.contextStats.MessageCount)
	}
	if m.contextStats.TotalSize != 5000 {
		t.Errorf("contextStats.TotalSize = %d, want 5000", m.contextStats.TotalSize)
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
	model := newTestModelAtLevel(levelDrillDown).
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

// TestDrillDownWithSearchQueryClearsSearch verifies that drilling down from a
// filtered aggregate clears the search query (layered search: each level independent).
func TestDrillDownWithSearchQueryClearsSearch(t *testing.T) {
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

	// Search query should be cleared on drill-down
	if m.searchQuery != "" {
		t.Errorf("expected searchQuery cleared, got %q", m.searchQuery)
	}

	// loadRequestID incremented for loadMessages
	if m.loadRequestID != 1 {
		t.Errorf("expected loadRequestID=1, got %d", m.loadRequestID)
	}
	// searchRequestID incremented to invalidate in-flight search responses
	if m.searchRequestID != 1 {
		t.Errorf("expected searchRequestID=1, got %d", m.searchRequestID)
	}

	if cmd == nil {
		t.Error("expected a command to be returned")
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

	// searchRequestID incremented to invalidate any in-flight search responses
	if m.searchRequestID != 1 {
		t.Errorf("expected searchRequestID=1, got %d", m.searchRequestID)
	}

	if cmd == nil {
		t.Error("expected a command to be returned")
	}
}

// TestSubAggregateDrillDownWithSearchQueryClearsSearch verifies drill-down from
// sub-aggregate also clears the search query (layered search).
func TestSubAggregateDrillDownWithSearchQueryClearsSearch(t *testing.T) {
	model := newTestModelWithRows(testAggregateRows)
	model.level = levelDrillDown
	model.searchQuery = "urgent"
	model.drillFilter = query.MessageFilter{Sender: "alice@example.com"}
	model.drillViewType = query.ViewSenders
	model.viewType = query.ViewLabels
	model.cursor = 0

	newModel, cmd := model.handleAggregateKeys(keyEnter())
	m := newModel.(Model)

	if m.level != levelMessageList {
		t.Errorf("expected levelMessageList, got %v", m.level)
	}

	// Search query should be cleared on drill-down
	if m.searchQuery != "" {
		t.Errorf("expected searchQuery cleared, got %q", m.searchQuery)
	}

	// loadRequestID incremented for loadMessages
	if m.loadRequestID != 1 {
		t.Errorf("expected loadRequestID=1, got %d", m.loadRequestID)
	}
	// searchRequestID incremented to invalidate in-flight search responses
	if m.searchRequestID != 1 {
		t.Errorf("expected searchRequestID=1, got %d", m.searchRequestID)
	}

	if cmd == nil {
		t.Error("expected a command to be returned")
	}
}

// TestDrillDownSearchBreadcrumbRoundTrip verifies that searching at aggregate level,
// drilling down (which clears search), then pressing Esc restores the original search.
func TestDrillDownSearchBreadcrumbRoundTrip(t *testing.T) {
	model := newTestModelWithRows(testAggregateRows)
	model.level = levelAggregates
	model.searchQuery = "important"
	model.cursor = 0

	// Drill down — search should be cleared
	newModel, _ := model.handleAggregateKeys(keyEnter())
	m := newModel.(Model)

	if m.searchQuery != "" {
		t.Errorf("expected searchQuery cleared after drill-down, got %q", m.searchQuery)
	}
	if m.level != levelMessageList {
		t.Errorf("expected levelMessageList, got %v", m.level)
	}

	// Populate messages so Esc handler works
	m.messages = []query.MessageSummary{{ID: 1}}

	// Esc back — should restore outer search from breadcrumb
	newModel2, _ := m.handleMessageListKeys(keyEsc())
	m2 := newModel2.(Model)

	if m2.level != levelAggregates {
		t.Errorf("expected levelAggregates after Esc, got %v", m2.level)
	}
	if m2.searchQuery != "important" {
		t.Errorf("expected searchQuery restored to %q, got %q", "important", m2.searchQuery)
	}
}

// TestDrillDownClearsHighlightTerms verifies that highlightTerms produces no
// highlighting after drill-down (since searchQuery is empty).
func TestDrillDownClearsHighlightTerms(t *testing.T) {
	model := newTestModelWithRows(testAggregateRows)
	model.level = levelAggregates
	model.searchQuery = "alice"
	model.cursor = 0

	newModel, _ := model.handleAggregateKeys(keyEnter())
	m := newModel.(Model)

	// highlightTerms with empty searchQuery should return text unchanged
	text := "alice@example.com"
	result := highlightTerms(text, m.searchQuery)
	if result != text {
		t.Errorf("expected no highlighting after drill-down, got %q", result)
	}
}

// TestSubAggregateDrillDownSearchBreadcrumbRoundTrip verifies the breadcrumb
// round-trip through a sub-aggregate drill-down with active search.
func TestSubAggregateDrillDownSearchBreadcrumbRoundTrip(t *testing.T) {
	model := newTestModelWithRows(testAggregateRows)
	model.level = levelDrillDown
	model.searchQuery = "urgent"
	model.drillFilter = query.MessageFilter{Sender: "alice@example.com"}
	model.drillViewType = query.ViewSenders
	model.viewType = query.ViewLabels
	model.cursor = 0

	// Drill down to message list — search should be cleared
	newModel, _ := model.handleAggregateKeys(keyEnter())
	m := newModel.(Model)

	if m.searchQuery != "" {
		t.Errorf("expected searchQuery cleared, got %q", m.searchQuery)
	}

	// Populate messages and go back
	m.messages = []query.MessageSummary{{ID: 1}}
	newModel2, _ := m.handleMessageListKeys(keyEsc())
	m2 := newModel2.(Model)

	if m2.searchQuery != "urgent" {
		t.Errorf("expected searchQuery restored to %q, got %q", "urgent", m2.searchQuery)
	}
}

// TestStaleSearchResponseIgnoredAfterDrillDown verifies that a search response
// from the aggregate level is ignored after drill-down because searchRequestID
// was incremented.
func TestStaleSearchResponseIgnoredAfterDrillDown(t *testing.T) {
	model := newTestModelWithRows(testAggregateRows)
	model.level = levelAggregates
	model.searchQuery = "important"
	model.searchRequestID = 5 // Simulate prior searches
	model.cursor = 0

	// Capture the pre-drill searchRequestID (this is what an in-flight response would carry)
	staleRequestID := model.searchRequestID

	// Drill down — clears search and increments searchRequestID
	newModel, _ := model.handleAggregateKeys(keyEnter())
	m := newModel.(Model)

	// Populate the message list with expected data
	m.messages = []query.MessageSummary{{ID: 100, Subject: "Drilled message"}}
	m.loading = false

	// Simulate a stale search response arriving with the old requestID
	staleResponse := searchResultsMsg{
		messages:  []query.MessageSummary{{ID: 999, Subject: "Stale search result"}},
		requestID: staleRequestID,
	}
	newModel2, _ := m.Update(staleResponse)
	m2 := newModel2.(Model)

	// The stale response should be ignored — messages unchanged
	if len(m2.messages) != 1 {
		t.Errorf("expected 1 message (stale ignored), got %d", len(m2.messages))
	}
	if m2.messages[0].ID != 100 {
		t.Errorf("expected message ID 100 (original), got %d", m2.messages[0].ID)
	}
}

// TestPreSearchSnapshotRestoreOnEsc verifies that activating inline search at the
// message list level snapshots state, and Esc restores it instantly without re-query.
func TestPreSearchSnapshotRestoreOnEsc(t *testing.T) {
	originalMsgs := []query.MessageSummary{{ID: 1, Subject: "Msg1"}, {ID: 2, Subject: "Msg2"}}
	originalStats := &query.TotalStats{MessageCount: 100, TotalSize: 5000}

	model := NewBuilder().WithMessages(originalMsgs...).
		WithLevel(levelMessageList).WithSize(100, 24).Build()
	model.messages = originalMsgs
	model.cursor = 1
	model.scrollOffset = 0
	model.contextStats = originalStats

	// Activate inline search — should snapshot
	model.activateInlineSearch("search")

	// Verify snapshot was taken
	if model.preSearchMessages == nil {
		t.Fatal("expected preSearchMessages to be set")
	}

	// Simulate search results arriving — mutates contextStats and replaces messages
	model.searchQuery = "test"
	model.messages = []query.MessageSummary{{ID: 99, Subject: "SearchResult"}}
	model.cursor = 0
	model.contextStats.MessageCount = 1 // Mutate original pointer
	model.searchLoadingMore = true
	model.searchOffset = 50
	model.searchTotalCount = 200

	// Esc from inline search — should restore snapshot
	newModel, _ := model.handleInlineSearchKeys(keyEsc())
	m := newModel.(Model)

	// Messages restored
	if len(m.messages) != 2 {
		t.Errorf("expected 2 messages restored, got %d", len(m.messages))
	}
	if m.messages[0].ID != 1 {
		t.Errorf("expected first message ID=1, got %d", m.messages[0].ID)
	}

	// Cursor restored
	if m.cursor != 1 {
		t.Errorf("expected cursor=1, got %d", m.cursor)
	}

	// Stats restored (deep copy: original mutation shouldn't affect snapshot)
	if m.contextStats == nil {
		t.Fatal("expected contextStats restored")
	}
	if m.contextStats.MessageCount != 100 {
		t.Errorf("expected MessageCount=100, got %d", m.contextStats.MessageCount)
	}

	// Search state fully cleared
	if m.searchQuery != "" {
		t.Errorf("expected searchQuery cleared, got %q", m.searchQuery)
	}
	if m.searchLoadingMore {
		t.Error("expected searchLoadingMore=false after restore")
	}
	if m.searchOffset != 0 {
		t.Errorf("expected searchOffset=0, got %d", m.searchOffset)
	}
	if m.searchTotalCount != 0 {
		t.Errorf("expected searchTotalCount=0, got %d", m.searchTotalCount)
	}
	if m.loading {
		t.Error("expected loading=false after restore")
	}

	// Snapshot cleared
	if m.preSearchMessages != nil {
		t.Error("expected preSearchMessages cleared after restore")
	}
}

// TestTwoStepEscClearsSearchThenGoesBack verifies that the first Esc clears
// the inner search and the second Esc navigates back via goBack.
func TestTwoStepEscClearsSearchThenGoesBack(t *testing.T) {
	// Start at aggregate level, drill down, then search at message list level
	model := newTestModelWithRows(testAggregateRows)
	model.level = levelAggregates
	model.cursor = 0

	// Drill down to message list
	newModel, _ := model.handleAggregateKeys(keyEnter())
	m := newModel.(Model)
	m.messages = []query.MessageSummary{{ID: 1}, {ID: 2}, {ID: 3}}
	m.loading = false

	// Activate search and simulate results
	m.activateInlineSearch("search")
	m.inlineSearchActive = false // Simulate search submitted
	m.searchQuery = "test"
	m.messages = []query.MessageSummary{{ID: 99}}

	// First Esc — should clear search and restore pre-search messages
	newModel2, _ := m.handleMessageListKeys(keyEsc())
	m2 := newModel2.(Model)

	if m2.searchQuery != "" {
		t.Errorf("expected searchQuery cleared after first Esc, got %q", m2.searchQuery)
	}
	if m2.level != levelMessageList {
		t.Errorf("expected still at levelMessageList after first Esc, got %v", m2.level)
	}
	if len(m2.messages) != 3 {
		t.Errorf("expected 3 pre-search messages restored, got %d", len(m2.messages))
	}

	// Second Esc — should goBack to aggregates
	newModel3, _ := m2.handleMessageListKeys(keyEsc())
	m3 := newModel3.(Model)

	if m3.level != levelAggregates {
		t.Errorf("expected levelAggregates after second Esc, got %v", m3.level)
	}
}

// TestHighlightedColumnsAligned verifies that highlighting search terms in
// aggregate rows doesn't break column alignment.
func TestHighlightedColumnsAligned(t *testing.T) {
	rows := []query.AggregateRow{
		{Key: "alice@example.com", Count: 42, TotalSize: 1024000, AttachmentSize: 512},
		{Key: "bob@example.com", Count: 7, TotalSize: 2048, AttachmentSize: 0},
	}
	model := NewBuilder().WithRows(rows...).
		WithLevel(levelAggregates).WithSize(100, 24).Build()

	// Render without search
	noSearchOutput := model.aggregateTableView()
	noSearchLines := strings.Split(noSearchOutput, "\n")

	// Render with search highlighting "alice"
	model.searchQuery = "alice"
	highlightOutput := model.aggregateTableView()
	highlightLines := strings.Split(highlightOutput, "\n")

	// Compare visible widths — should be identical for each corresponding line
	for i := 0; i < len(noSearchLines) && i < len(highlightLines); i++ {
		noSearchWidth := lipgloss.Width(noSearchLines[i])
		highlightWidth := lipgloss.Width(highlightLines[i])
		if noSearchWidth != highlightWidth {
			t.Errorf("line %d: width without search=%d, with highlight=%d\n  no search: %q\n  highlight: %q",
				i, noSearchWidth, highlightWidth, noSearchLines[i], highlightLines[i])
		}
	}
}

// TestViewTypeRestoredAfterEscFromSubAggregate verifies viewType is restored when
// navigating back from sub-aggregate to message list.
func TestViewTypeRestoredAfterEscFromSubAggregate(t *testing.T) {
	model := NewBuilder().
		WithMessages(query.MessageSummary{ID: 1}, query.MessageSummary{ID: 2}).
		WithLevel(levelMessageList).WithViewType(query.ViewSenders).Build()
	model.drillFilter = query.MessageFilter{Sender: "alice@example.com"}
	model.drillViewType = query.ViewSenders
	model.cursor = 1
	model.scrollOffset = 0

	// Press Tab to go to sub-aggregate (changes viewType)
	newModel, _ := model.Update(keyTab())
	m := newModel.(Model)

	if m.level != levelDrillDown {
		t.Fatalf("expected levelDrillDown, got %v", m.level)
	}
	// viewType should have changed to next sub-group view (Recipients, skipping redundant SenderNames)
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
	rows := make([]query.AggregateRow, 10)
	for i := range rows {
		rows[i] = query.AggregateRow{Key: fmt.Sprintf("sender%d@example.com", i), Count: int64(i)}
	}
	model := NewBuilder().WithRows(rows...).WithViewType(query.ViewSenders).Build()
	model.cursor = 5
	model.scrollOffset = 3

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
	model := NewBuilder().WithLevel(levelMessageList).Build()
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
	model := NewBuilder().
		WithMessages(
			query.MessageSummary{ID: 1, Subject: "Test message"},
			query.MessageSummary{ID: 2, Subject: "Another message"},
		).
		WithLevel(levelMessageList).WithViewType(query.ViewRecipients).Build()
	model.drillFilter = query.MessageFilter{
		Sender:    "alice@example.com",
		Recipient: "bob@example.com",
	}
	model.drillViewType = query.ViewSenders
	model.filterKey = "bob@example.com"

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
	tests := []struct {
		name        string
		version     string
		wantVersion bool   // should version appear in title
		wantText    string // expected version text in brackets
	}{
		{"tagged version", "v0.1.0", true, "[v0.1.0]"},
		{"dev version hidden", "dev", false, ""},
		{"empty version hidden", "", false, ""},
		{"unknown version hidden", "unknown", false, ""},
		{"prerelease version", "v1.0.0-rc1", true, "[v1.0.0-rc1]"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			model := NewBuilder().WithSize(100, 20).WithViewType(query.ViewSenders).Build()
			model.version = tt.version

			header := model.headerView()
			lines := strings.Split(header, "\n")

			if len(lines) < 2 {
				t.Fatalf("expected 2 header lines, got %d", len(lines))
			}

			if !strings.Contains(lines[0], "msgvault") {
				t.Errorf("expected title bar to contain 'msgvault', got: %s", lines[0])
			}
			if tt.wantVersion {
				if !strings.Contains(lines[0], tt.wantText) {
					t.Errorf("expected title bar to contain %q, got: %s", tt.wantText, lines[0])
				}
			} else {
				if strings.Contains(lines[0], "[") {
					t.Errorf("expected no version in title bar, got: %s", lines[0])
				}
			}
			if !strings.Contains(lines[0], "All Accounts") {
				t.Errorf("expected title bar to contain 'All Accounts', got: %s", lines[0])
			}
		})
	}
}

// TestHeaderShowsSelectedAccount verifies header shows selected account name.
func TestHeaderShowsSelectedAccount(t *testing.T) {
	accountID := int64(2)
	model := NewBuilder().WithSize(100, 20).
		WithAccounts(
			query.AccountInfo{ID: 1, Identifier: "alice@gmail.com"},
			query.AccountInfo{ID: 2, Identifier: "bob@gmail.com"},
		).
		WithAccountFilter(&accountID).Build()

	header := model.headerView()
	lines := strings.Split(header, "\n")

	if !strings.Contains(lines[0], "bob@gmail.com") {
		t.Errorf("expected title bar to show selected account 'bob@gmail.com', got: %s", lines[0])
	}
}

// TestHeaderShowsViewTypeOnLine2 verifies line 2 shows current view type.
func TestHeaderShowsViewTypeOnLine2(t *testing.T) {
	model := NewBuilder().WithSize(100, 20).WithViewType(query.ViewSenders).
		WithStats(&query.TotalStats{MessageCount: 1000, TotalSize: 5000000, AttachmentCount: 50}).
		Build()

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
	model := NewBuilder().WithSize(100, 20).
		WithLevel(levelMessageList).WithViewType(query.ViewRecipients).Build()
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
	model := NewBuilder().WithSize(100, 20).
		WithLevel(levelDrillDown).WithViewType(query.ViewRecipients).
		WithContextStats(&query.TotalStats{MessageCount: 100, TotalSize: 500000}).
		Build()
	model.drillViewType = query.ViewSenders
	model.drillFilter = query.MessageFilter{Sender: "alice@example.com"}

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
	model := NewBuilder().WithSize(100, 20).Build()
	model.attachmentFilter = true

	header := model.headerView()
	lines := strings.Split(header, "\n")

	if !strings.Contains(lines[0], "[Attachments]") {
		t.Errorf("expected title bar to show '[Attachments]' filter indicator, got: %s", lines[0])
	}
}

// TestViewStructureHasTitleBarFirst verifies View() output starts with title bar.
func TestViewStructureHasTitleBarFirst(t *testing.T) {
	rows := []query.AggregateRow{
		{Key: "alice@example.com", Count: 100, TotalSize: 500000},
	}

	model := NewBuilder().
		WithRows(rows...).
		WithViewType(query.ViewSenders).
		WithSize(100, 30).
		WithPageSize(20).
		WithStats(&query.TotalStats{MessageCount: 1000, TotalSize: 5000000, AttachmentCount: 50}).
		Build()

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
	rows := []query.AggregateRow{
		{Key: "alice@example.com", Count: 100, TotalSize: 500000},
		{Key: "bob@example.com", Count: 50, TotalSize: 250000},
	}

	model := NewBuilder().
		WithRows(rows...).
		WithViewType(query.ViewSenders).
		WithStats(&query.TotalStats{MessageCount: 1000, TotalSize: 5000000, AttachmentCount: 50}).
		Build()

	// Simulate WindowSizeMsg to trigger pageSize calculation (the real code path)
	terminalHeight := 40
	model = resizeModel(t, model, 100, terminalHeight)

	view := model.View()
	lines := strings.Split(view, "\n")
	actualLines := countViewLines(view)

	t.Logf("Terminal height: %d, View lines: %d, pageSize: %d", terminalHeight, actualLines, model.pageSize)
	t.Logf("First line: %q", lines[0])
	t.Logf("Last non-empty line: %q", lines[actualLines-1])

	// View should fit exactly in terminal height
	assertViewFitsHeight(t, view, terminalHeight)

	// First line must be title bar
	if !strings.Contains(lines[0], "msgvault") {
		t.Errorf("First line should be title bar with 'msgvault', got: %q", lines[0])
	}
}

// TestViewFitsTerminalHeightDuringLoading verifies View() output fits during loading state.
func TestViewFitsTerminalHeightDuringLoading(t *testing.T) {
	rows := []query.AggregateRow{
		{Key: "alice@example.com", Count: 100, TotalSize: 500000},
		{Key: "bob@example.com", Count: 50, TotalSize: 250000},
	}

	model := NewBuilder().
		WithRows(rows...).
		WithViewType(query.ViewSenders).
		WithStats(&query.TotalStats{MessageCount: 1000, TotalSize: 5000000, AttachmentCount: 50}).
		WithLoading(true).
		Build()

	terminalHeight := 40
	model = resizeModel(t, model, 100, terminalHeight)

	view := model.View()
	lines := strings.Split(view, "\n")
	actualLines := countViewLines(view)

	t.Logf("Terminal height: %d, View lines: %d, pageSize: %d (loading=%v)", terminalHeight, actualLines, model.pageSize, model.loading)

	assertViewFitsHeight(t, view, terminalHeight)
	if !strings.Contains(lines[0], "msgvault") {
		t.Errorf("First line should be title bar, got: %q", lines[0])
	}
}

// TestViewFitsTerminalHeightWithInlineSearch verifies View() output fits with inline search active.
func TestViewFitsTerminalHeightWithInlineSearch(t *testing.T) {
	rows := []query.AggregateRow{
		{Key: "alice@example.com", Count: 100, TotalSize: 500000},
		{Key: "bob@example.com", Count: 50, TotalSize: 250000},
	}

	model := NewBuilder().
		WithRows(rows...).
		WithViewType(query.ViewSenders).
		WithStats(&query.TotalStats{MessageCount: 1000, TotalSize: 5000000, AttachmentCount: 50}).
		Build()
	model.inlineSearchActive = true // Enable inline search

	terminalHeight := 40
	model = resizeModel(t, model, 100, terminalHeight)

	view := model.View()
	lines := strings.Split(view, "\n")
	actualLines := countViewLines(view)

	t.Logf("Terminal height: %d, View lines: %d, pageSize: %d (inlineSearch=%v)", terminalHeight, actualLines, model.pageSize, model.inlineSearchActive)

	assertViewFitsHeight(t, view, terminalHeight)
	if !strings.Contains(lines[0], "msgvault") {
		t.Errorf("First line should be title bar, got: %q", lines[0])
	}
}

// TestViewFitsTerminalHeightAtMessageList verifies View() output fits at message list level.
func TestViewFitsTerminalHeightAtMessageList(t *testing.T) {
	msgs := []query.MessageSummary{
		{ID: 1, Subject: "Test 1", FromEmail: "alice@example.com", SizeEstimate: 1000},
		{ID: 2, Subject: "Test 2", FromEmail: "bob@example.com", SizeEstimate: 2000},
	}

	model := NewBuilder().
		WithMessages(msgs...).
		WithLevel(levelMessageList).
		WithStats(&query.TotalStats{MessageCount: 1000, TotalSize: 5000000, AttachmentCount: 50}).
		WithContextStats(&query.TotalStats{MessageCount: 2, TotalSize: 3000, AttachmentCount: 0}).
		Build()
	model.filterKey = "alice@example.com"

	terminalHeight := 40
	model = resizeModel(t, model, 100, terminalHeight)

	view := model.View()
	lines := strings.Split(view, "\n")
	actualLines := countViewLines(view)

	t.Logf("Terminal height: %d, View lines: %d, pageSize: %d (level=MessageList)", terminalHeight, actualLines, model.pageSize)

	assertViewFitsHeight(t, view, terminalHeight)
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
	model := NewBuilder().
		WithLoading(true).
		WithSize(0, 0).
		Build()

	view1 := model.View()
	t.Logf("Stage 1 (before resize): View = %q", view1)
	if view1 != "Loading..." {
		t.Errorf("Stage 1: Expected 'Loading...', got %q", view1)
	}

	// Stage 2: After WindowSizeMsg (width/height set, loading=true, no data)
	model = resizeModel(t, model, terminalWidth, terminalHeight)

	view2 := model.View()
	lines2 := strings.Split(view2, "\n")
	actualLines2 := countViewLines(view2)
	t.Logf("Stage 2 (after resize, loading=true, no data): lines=%d, pageSize=%d", actualLines2, model.pageSize)
	t.Logf("  First line: %q", truncateTestString(lines2[0], 60))
	t.Logf("  Last line: %q", truncateTestString(lines2[actualLines2-1], 60))

	if actualLines2 != terminalHeight {
		t.Errorf("Stage 2: View has %d lines but terminal height is %d (loading, no data)", actualLines2, terminalHeight)
	}

	// Stage 3: After stats load (still loading=true, no data)
	model.stats = &query.TotalStats{MessageCount: 1000, TotalSize: 5000000, AttachmentCount: 50}

	view3 := model.View()
	actualLines3 := countViewLines(view3)
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
	actualLines4 := countViewLines(view4)
	t.Logf("Stage 4 (data loaded): lines=%d", actualLines4)
	t.Logf("  First line: %q", truncateTestString(lines4[0], 60))

	if actualLines4 != terminalHeight {
		t.Errorf("Stage 4: View has %d lines but terminal height is %d (data loaded)", actualLines4, terminalHeight)
	}

	// Ensure first line is always title bar at stages 2-4
	for i, lines := range [][]string{lines2, strings.Split(view3, "\n"), lines4} {
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
	rows := []query.AggregateRow{
		{Key: "alice@example.com", Count: 100, TotalSize: 500000},
		{Key: "bob\n@example.com", Count: 50, TotalSize: 250000}, // Embedded newline!
		{Key: "charlie\r\n@example.com", Count: 25, TotalSize: 100000}, // CRLF
		{Key: "david\t@example.com", Count: 10, TotalSize: 50000}, // Tab
	}

	model := NewBuilder().
		WithRows(rows...).
		WithViewType(query.ViewSenders).
		WithStats(&query.TotalStats{MessageCount: 1000, TotalSize: 5000000, AttachmentCount: 50}).
		Build()

	terminalHeight := 40
	model = resizeModel(t, model, 100, terminalHeight)

	view := model.View()
	lines := strings.Split(view, "\n")
	actualLines := countViewLines(view)

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
			rows := []query.AggregateRow{
				{Key: "alice@example.com", Count: 100, TotalSize: 500000},
				{Key: "bob@example.com", Count: 50, TotalSize: 250000},
			}

			model := NewBuilder().
				WithRows(rows...).
				WithViewType(query.ViewSenders).
				WithStats(&query.TotalStats{MessageCount: 1000, TotalSize: 5000000, AttachmentCount: 50}).
				Build()

			model = resizeModel(t, model, size.width, size.height)

			view := model.View()
			lines := strings.Split(view, "\n")
			actualLines := countViewLines(view)

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
	rows := []query.AggregateRow{
		{Key: "alice@example.com", Count: 100, TotalSize: 500000},
	}

	model := NewBuilder().
		WithRows(rows...).
		WithViewType(query.ViewSenders).
		WithStats(&query.TotalStats{MessageCount: 1000, TotalSize: 5000000, AttachmentCount: 50}).
		WithLoading(true).
		Build()

	terminalWidth := 100
	terminalHeight := 24
	model = resizeModel(t, model, terminalWidth, terminalHeight)

	// Simulate multiple spinner frames
	for frame := 0; frame < 10; frame++ {
		model.spinnerFrame = frame

		view := model.View()
		lines := strings.Split(view, "\n")
		actualLines := countViewLines(view)

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
	rows := []query.AggregateRow{
		{Key: "alice@example.com", Count: 100, TotalSize: 500000},
		{Key: "bob@example.com", Count: 50, TotalSize: 250000},
	}

	model := NewBuilder().
		WithRows(rows...).
		WithViewType(query.ViewSenders).
		WithStats(&query.TotalStats{MessageCount: 1000, TotalSize: 5000000, AttachmentCount: 50}).
		Build()

	terminalWidth := 100
	terminalHeight := 55 // User's actual terminal height
	model = resizeModel(t, model, terminalWidth, terminalHeight)

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
	rows := []query.AggregateRow{
		{Key: "alice@example.com", Count: 100, TotalSize: 500000},
	}

	model := NewBuilder().
		WithRows(rows...).
		WithViewType(query.ViewSenders).
		// Very long stats string
		WithStats(&query.TotalStats{MessageCount: 999999999, TotalSize: 999999999999, AttachmentCount: 999999}).
		Build()

	terminalWidth := 80 // Narrower terminal
	terminalHeight := 40
	model = resizeModel(t, model, terminalWidth, terminalHeight)

	view := model.View()
	lines := strings.Split(view, "\n")
	actualLines := countViewLines(view)

	t.Logf("Terminal: %dx%d, View lines: %d", terminalWidth, terminalHeight, actualLines)

	assertViewFitsHeight(t, view, terminalHeight)

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
	rows := []query.AggregateRow{
		{Key: "alice@example.com", Count: 100, TotalSize: 500000, TotalUnique: 1000},
		{Key: "bob@example.com", Count: 50, TotalSize: 250000, TotalUnique: 1000},
	}

	model := NewBuilder().
		WithRows(rows...).
		WithViewType(query.ViewSenders).
		WithSize(100, 30).
		WithPageSize(20).
		Build()

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
	rows := []query.AggregateRow{
		{Key: "alice@example.com", Count: 100, TotalSize: 500000},
		{Key: "bob@example.com", Count: 50, TotalSize: 250000},
	}

	model := NewBuilder().
		WithRows(rows...).
		WithViewType(query.ViewSenders).
		WithSize(100, 30).
		WithPageSize(20).
		Build()

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
		{query.ViewRecipientNames, "RN"},
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
	model := NewBuilder().
		WithMessages(
			query.MessageSummary{ID: 1, Subject: "First message"},
			query.MessageSummary{ID: 2, Subject: "Second message"},
			query.MessageSummary{ID: 3, Subject: "Third message"},
		).
		WithDetail(&query.MessageDetail{ID: 2, Subject: "Second message"}).
		WithLevel(levelMessageDetail).Build()
	model.detailMessageIndex = 1 // Viewing second message
	model.cursor = 1

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
	model := NewBuilder().
		WithMessages(
			query.MessageSummary{ID: 1, Subject: "First message"},
			query.MessageSummary{ID: 2, Subject: "Second message"},
		).
		WithDetail(&query.MessageDetail{ID: 1, Subject: "First message"}).
		WithLevel(levelMessageDetail).Build()
	model.detailMessageIndex = 0 // At first message

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
	model := NewBuilder().
		WithMessages(
			query.MessageSummary{ID: 1, Subject: "First"},
			query.MessageSummary{ID: 2, Subject: "Second"},
			query.MessageSummary{ID: 3, Subject: "Third"},
		).
		WithDetail(&query.MessageDetail{ID: 2, Subject: "Second"}).
		WithLevel(levelMessageDetail).Build()
	model.detailMessageIndex = 1
	model.cursor = 1

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
	model := NewBuilder().WithLevel(levelMessageDetail).Build()
	model.detailMessageIndex = 0

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
	model := NewBuilder().
		WithMessages(query.MessageSummary{ID: 1, Subject: "Only message"}).
		WithLevel(levelMessageDetail).Build()
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
	model := NewBuilder().
		WithMessages(
			query.MessageSummary{ID: 1, Subject: "First"},
			query.MessageSummary{ID: 2, Subject: "Second"},
			query.MessageSummary{ID: 3, Subject: "Third"},
		).
		WithLevel(levelMessageList).
		WithPageSize(10).WithSize(100, 20).Build()

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
	model := NewBuilder().
		WithMessages(
			query.MessageSummary{ID: 1, Subject: "List msg 1"},
			query.MessageSummary{ID: 2, Subject: "List msg 2"},
		).Build()

	// Set up thread view with different messages than the list
	model.threadMessages = []query.MessageSummary{
		{ID: 100, Subject: "Thread msg 1"},
		{ID: 101, Subject: "Thread msg 2"},
		{ID: 102, Subject: "Thread msg 3"},
		{ID: 103, Subject: "Thread msg 4"},
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
	rows := []query.AggregateRow{
		{Key: "alice@example.com", Count: 5},
		{Key: "bob@example.com", Count: 3},
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
			model := NewBuilder().WithRows(rows...).
				WithSize(100, tc.height).WithPageSize(tc.height-5).
				WithLevel(tc.level).Build()

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
	model := NewBuilder().
		WithDetail(&query.MessageDetail{ID: 1, Subject: "Test", BodyText: "Content"}).
		WithLevel(levelMessageDetail).WithSize(100, 20).WithPageSize(15).Build()
	model.detailLineCount = 50
	model.detailScroll = 40 // Near the end

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

	model := NewBuilder().
		WithRows(
			query.AggregateRow{Key: "alice@example.com", Count: 100, TotalSize: 1000000},
			query.AggregateRow{Key: "bob@example.com", Count: 50, TotalSize: 500000},
			query.AggregateRow{Key: "charlie@example.com", Count: 25, TotalSize: 250000},
		).
		WithSize(80, 24).WithPageSize(19).
		WithModal(modalQuitConfirm).Build()

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
	model := NewBuilder().
		WithRows(
			query.AggregateRow{Key: "work", Count: 5},
			query.AggregateRow{Key: "personal", Count: 3},
		).
		WithLevel(levelDrillDown).WithViewType(query.ViewLabels).
		WithPageSize(10).WithSize(100, 20).Build()
	model.drillFilter = query.MessageFilter{Sender: "alice@example.com"}
	model.drillViewType = query.ViewSenders

	// Press 'a' to jump to all messages (with drill filter)
	newModel, cmd := model.handleAggregateKeys(key('a'))
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
	if m.breadcrumbs[0].state.level != levelDrillDown {
		t.Errorf("expected breadcrumb level = levelDrillDown, got %v", m.breadcrumbs[0].state.level)
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

func TestExportAttachmentsModal(t *testing.T) {
	model := NewBuilder().
		WithDetail(&query.MessageDetail{
			ID:      1,
			Subject: "Test Email",
			Attachments: []query.AttachmentInfo{
				{ID: 1, Filename: "file1.pdf", Size: 1024, ContentHash: "abc123"},
				{ID: 2, Filename: "file2.txt", Size: 512, ContentHash: "def456"},
			},
		}).
		WithLevel(levelMessageDetail).
		WithPageSize(10).WithSize(100, 20).Build()

	// Press 'e' to open export modal
	m := applyDetailKey(t, model, key('e'))

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
	m, _ = applyModalKey(t, m, key('j'))
	if m.exportCursor != 1 {
		t.Errorf("expected exportCursor = 1, got %d", m.exportCursor)
	}

	// Test toggle selection with space
	m, _ = applyModalKey(t, m, key(' '))
	if m.exportSelection[1] {
		t.Error("expected attachment 1 to be deselected after space")
	}

	// Test select none
	m, _ = applyModalKey(t, m, key('n'))
	if m.exportSelection[0] || m.exportSelection[1] {
		t.Error("expected all attachments to be deselected after 'n'")
	}

	// Test select all
	m, _ = applyModalKey(t, m, key('a'))
	if !m.exportSelection[0] || !m.exportSelection[1] {
		t.Error("expected all attachments to be selected after 'a'")
	}

	// Test cancel with Esc
	m, _ = applyModalKey(t, m, keyEsc())
	if m.modal != modalNone {
		t.Errorf("expected modalNone after Esc, got %v", m.modal)
	}
	if m.exportSelection != nil {
		t.Error("expected exportSelection to be cleared after Esc")
	}
}

func TestExportAttachmentsNoAttachments(t *testing.T) {
	model := NewBuilder().
		WithDetail(&query.MessageDetail{
			ID:          1,
			Subject:     "Test Email",
			Attachments: []query.AttachmentInfo{}, // No attachments
		}).
		WithLevel(levelMessageDetail).
		WithPageSize(10).WithSize(100, 20).Build()

	// Press 'e' should show flash message, not modal
	m := applyDetailKey(t, model, key('e'))

	if m.modal == modalExportAttachments {
		t.Error("expected modal NOT to open when no attachments")
	}
	if m.flashMessage != "No attachments to export" {
		t.Errorf("expected flash message 'No attachments to export', got '%s'", m.flashMessage)
	}
}

// --- Helper method unit tests ---

func TestNavigateList(t *testing.T) {
	tests := []struct {
		name       string
		key        string
		itemCount  int
		initCursor int
		wantCursor int
		wantHandled bool
	}{
		{"down from top", "j", 5, 0, 1, true},
		{"up from second", "k", 5, 1, 0, true},
		{"down at end", "j", 5, 4, 4, true},
		{"up at top", "k", 5, 0, 0, true},
		{"unhandled key", "x", 5, 0, 0, false},
		{"empty list down", "j", 0, 0, 0, true},
		{"empty list up", "k", 0, 0, 0, true},
		{"home", "home", 5, 3, 0, true},
		{"end", "end", 5, 0, 4, true},
		{"end empty list", "end", 0, 0, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := NewBuilder().WithRows(
				query.AggregateRow{Key: "a"},
			).Build()
			m.cursor = tt.initCursor

			handled := m.navigateList(tt.key, tt.itemCount)
			if handled != tt.wantHandled {
				t.Errorf("navigateList(%q, %d) handled = %v, want %v", tt.key, tt.itemCount, handled, tt.wantHandled)
			}
			if m.cursor != tt.wantCursor {
				t.Errorf("navigateList(%q, %d) cursor = %d, want %d", tt.key, tt.itemCount, m.cursor, tt.wantCursor)
			}
		})
	}
}

func TestOpenAccountSelector(t *testing.T) {
	t.Run("no accounts", func(t *testing.T) {
		m := NewBuilder().Build()
		m.openAccountSelector()
		assertModal(t, m, modalAccountSelector)
		if m.modalCursor != 0 {
			t.Errorf("expected modalCursor 0, got %d", m.modalCursor)
		}
	})

	t.Run("with matching filter", func(t *testing.T) {
		acctID := int64(42)
		m := NewBuilder().WithAccounts(
			query.AccountInfo{ID: 10, Identifier: "a@example.com"},
			query.AccountInfo{ID: 42, Identifier: "b@example.com"},
		).Build()
		m.accountFilter = &acctID
		m.openAccountSelector()
		assertModal(t, m, modalAccountSelector)
		if m.modalCursor != 2 { // index 1 + 1 for "All Accounts"
			t.Errorf("expected modalCursor 2, got %d", m.modalCursor)
		}
	})
}

func TestOpenAttachmentFilter(t *testing.T) {
	m := NewBuilder().Build()

	m.attachmentFilter = false
	m.openAttachmentFilter()
	if m.modalCursor != 0 {
		t.Errorf("expected modalCursor 0 for no filter, got %d", m.modalCursor)
	}

	m.attachmentFilter = true
	m.openAttachmentFilter()
	if m.modalCursor != 1 {
		t.Errorf("expected modalCursor 1 for attachment filter, got %d", m.modalCursor)
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

func TestPushBreadcrumb(t *testing.T) {
	m := NewBuilder().Build()

	if len(m.breadcrumbs) != 0 {
		t.Fatal("expected no breadcrumbs initially")
	}

	m.pushBreadcrumb()
	if len(m.breadcrumbs) != 1 {
		t.Errorf("expected 1 breadcrumb, got %d", len(m.breadcrumbs))
	}

	m.pushBreadcrumb()
	if len(m.breadcrumbs) != 2 {
		t.Errorf("expected 2 breadcrumbs, got %d", len(m.breadcrumbs))
	}
}

func TestSubAggregateDrillDownPreservesSelection(t *testing.T) {
	// Regression test: drilling down from sub-aggregate via Enter should NOT
	// clear the aggregate selection (only top-level Enter does that).
	model := NewBuilder().
		WithRows(
			query.AggregateRow{Key: "alice@example.com", Count: 100, TotalSize: 500000},
			query.AggregateRow{Key: "bob@example.com", Count: 50, TotalSize: 250000},
		).
		Build()

	// Step 1: Drill down from top-level to message list (Enter on alice)
	model.cursor = 0
	m1 := applyAggregateKey(t, model, keyEnter())
	if m1.level != levelMessageList {
		t.Fatalf("expected levelMessageList, got %v", m1.level)
	}

	// Step 2: Go to sub-aggregate view (Tab)
	m1.rows = []query.AggregateRow{
		{Key: "domain1.com", Count: 60, TotalSize: 300000},
		{Key: "domain2.com", Count: 40, TotalSize: 200000},
	}
	m1.loading = false
	m2 := applyMessageListKey(t, m1, keyTab())
	if m2.level != levelDrillDown {
		t.Fatalf("expected levelDrillDown, got %v", m2.level)
	}

	// Step 3: Select an aggregate in sub-aggregate view, then drill down with Enter
	m2.rows = []query.AggregateRow{
		{Key: "domain1.com", Count: 60, TotalSize: 300000},
		{Key: "domain2.com", Count: 40, TotalSize: 200000},
	}
	m2.loading = false
	m2.selection.aggregateKeys["domain2.com"] = true
	m2.cursor = 0

	m3 := applyAggregateKey(t, m2, keyEnter())
	if m3.level != levelMessageList {
		t.Fatalf("expected levelMessageList after sub-agg Enter, got %v", m3.level)
	}

	// The selection should NOT have been cleared by the sub-aggregate Enter
	if len(m3.selection.aggregateKeys) == 0 {
		t.Error("sub-aggregate Enter should not clear aggregate selection")
	}
}

func TestTopLevelDrillDownClearsSelection(t *testing.T) {
	// Top-level Enter should clear selections (contrasts with sub-aggregate behavior)
	model := NewBuilder().
		WithRows(
			query.AggregateRow{Key: "alice@example.com", Count: 100, TotalSize: 500000},
			query.AggregateRow{Key: "bob@example.com", Count: 50, TotalSize: 250000},
		).
		Build()

	// Select bob, then drill into alice via Enter
	model.selection.aggregateKeys["bob@example.com"] = true
	model.cursor = 0

	m := applyAggregateKey(t, model, keyEnter())
	if m.level != levelMessageList {
		t.Fatalf("expected levelMessageList, got %v", m.level)
	}

	// Selection should be cleared
	if len(m.selection.aggregateKeys) != 0 {
		t.Errorf("top-level Enter should clear aggregate selection, got %v", m.selection.aggregateKeys)
	}
	if len(m.selection.messageIDs) != 0 {
		t.Errorf("top-level Enter should clear message selection, got %v", m.selection.messageIDs)
	}
}

// =============================================================================
// Time Granularity Drill-Down Tests
// =============================================================================

func TestTopLevelTimeDrillDown_AllGranularities(t *testing.T) {
	// Test that top-level drill-down from Time view correctly sets both
	// TimePeriod and TimeGranularity on the drillFilter.
	tests := []struct {
		name        string
		granularity query.TimeGranularity
		key         string
	}{
		{"Year", query.TimeYear, "2024"},
		{"Month", query.TimeMonth, "2024-06"},
		{"Day", query.TimeDay, "2024-06-15"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			model := NewBuilder().
				WithRows(query.AggregateRow{Key: tt.key, Count: 87, TotalSize: 500000}).
				WithViewType(query.ViewTime).
				Build()

			model.timeGranularity = tt.granularity
			model.cursor = 0

			m := applyAggregateKey(t, model, keyEnter())

			assertLevel(t, m, levelMessageList)

			if m.drillFilter.TimePeriod != tt.key {
				t.Errorf("drillFilter.TimePeriod = %q, want %q", m.drillFilter.TimePeriod, tt.key)
			}
			if m.drillFilter.TimeGranularity != tt.granularity {
				t.Errorf("drillFilter.TimeGranularity = %v, want %v", m.drillFilter.TimeGranularity, tt.granularity)
			}
		})
	}
}

func TestSubAggregateTimeDrillDown_AllGranularities(t *testing.T) {
	// Regression test: drilling down from sub-aggregate Time view must set
	// TimeGranularity on the drillFilter to match the current view granularity,
	// not the stale value from the original top-level drill.
	tests := []struct {
		name              string
		initialGranularity query.TimeGranularity // Set when top-level drill was created
		subGranularity     query.TimeGranularity // Changed in sub-aggregate view
		key               string
	}{
		{"Month_to_Year", query.TimeMonth, query.TimeYear, "2024"},
		{"Year_to_Month", query.TimeYear, query.TimeMonth, "2024-06"},
		{"Year_to_Day", query.TimeYear, query.TimeDay, "2024-06-15"},
		{"Day_to_Year", query.TimeDay, query.TimeYear, "2023"},
		{"Day_to_Month", query.TimeDay, query.TimeMonth, "2023-12"},
		{"Month_to_Day", query.TimeMonth, query.TimeDay, "2024-01-15"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Start with a model already in sub-aggregate Time view
			// (simulating: top-level sender drill → sub-group by time)
			model := NewBuilder().
				WithRows(query.AggregateRow{Key: tt.key, Count: 87, TotalSize: 500000}).
				WithLevel(levelDrillDown).
				WithViewType(query.ViewTime).
				Build()

			// drillFilter was created during top-level drill with the initial granularity
			model.drillFilter = query.MessageFilter{
				Sender:          "alice@example.com",
				TimeGranularity: tt.initialGranularity,
			}
			model.drillViewType = query.ViewSenders
			// User changed granularity in the sub-aggregate view
			model.timeGranularity = tt.subGranularity
			model.cursor = 0

			m := applyAggregateKey(t, model, keyEnter())

			assertLevel(t, m, levelMessageList)

			if m.drillFilter.TimePeriod != tt.key {
				t.Errorf("drillFilter.TimePeriod = %q, want %q", m.drillFilter.TimePeriod, tt.key)
			}
			if m.drillFilter.TimeGranularity != tt.subGranularity {
				t.Errorf("drillFilter.TimeGranularity = %v, want %v (should match sub-agg granularity, not initial %v)",
					m.drillFilter.TimeGranularity, tt.subGranularity, tt.initialGranularity)
			}
			// Sender filter from original drill should be preserved
			if m.drillFilter.Sender != "alice@example.com" {
				t.Errorf("drillFilter.Sender = %q, want %q (should preserve parent drill filter)",
					m.drillFilter.Sender, "alice@example.com")
			}
		})
	}
}

func TestSubAggregateTimeDrillDown_NonTimeViewPreservesGranularity(t *testing.T) {
	// When sub-aggregate view is NOT Time (e.g., Labels), drilling down should
	// NOT change the drillFilter's TimeGranularity (it may have been set by
	// a previous time drill).
	model := NewBuilder().
		WithRows(query.AggregateRow{Key: "INBOX", Count: 50, TotalSize: 100000}).
		WithLevel(levelDrillDown).
		WithViewType(query.ViewLabels).
		Build()

	model.drillFilter = query.MessageFilter{
		Sender:          "alice@example.com",
		TimePeriod:      "2024",
		TimeGranularity: query.TimeYear,
	}
	model.drillViewType = query.ViewSenders
	model.timeGranularity = query.TimeMonth // Different from drillFilter
	model.cursor = 0

	m := applyAggregateKey(t, model, keyEnter())

	assertLevel(t, m, levelMessageList)

	// TimeGranularity should be unchanged (we drilled by Label, not Time)
	if m.drillFilter.TimeGranularity != query.TimeYear {
		t.Errorf("drillFilter.TimeGranularity = %v, want TimeYear (non-time drill should not change it)",
			m.drillFilter.TimeGranularity)
	}
	if m.drillFilter.Label != "INBOX" {
		t.Errorf("drillFilter.Label = %q, want %q", m.drillFilter.Label, "INBOX")
	}
}

func TestTopLevelTimeDrillDown_GranularityChangedBeforeEnter(t *testing.T) {
	// User starts in Time view with Month, changes to Year, then presses Enter.
	// drillFilter should use the CURRENT granularity (Year), not the initial one.
	model := NewBuilder().
		WithRows(query.AggregateRow{Key: "2024", Count: 200, TotalSize: 1000000}).
		WithViewType(query.ViewTime).
		Build()

	// Default is TimeMonth, user toggles to TimeYear
	model.timeGranularity = query.TimeYear
	model.cursor = 0

	m := applyAggregateKey(t, model, keyEnter())

	assertLevel(t, m, levelMessageList)
	if m.drillFilter.TimeGranularity != query.TimeYear {
		t.Errorf("drillFilter.TimeGranularity = %v, want TimeYear", m.drillFilter.TimeGranularity)
	}
	if m.drillFilter.TimePeriod != "2024" {
		t.Errorf("drillFilter.TimePeriod = %q, want %q", m.drillFilter.TimePeriod, "2024")
	}
}

func TestSubAggregateTimeDrillDown_FullScenario(t *testing.T) {
	// Full user scenario: search sender → drill → sub-group by time → toggle Year → Enter
	// This is the exact bug report scenario.
	model := NewBuilder().
		WithRows(
			query.AggregateRow{Key: "alice@example.com", Count: 200, TotalSize: 1000000},
		).
		WithViewType(query.ViewSenders).
		Build()

	// Step 1: Drill into alice (top-level, creates drillFilter with TimeMonth default)
	model.timeGranularity = query.TimeMonth // default
	model.cursor = 0
	step1 := applyAggregateKey(t, model, keyEnter())
	assertLevel(t, step1, levelMessageList)

	if step1.drillFilter.TimeGranularity != query.TimeMonth {
		t.Fatalf("after top-level drill, TimeGranularity = %v, want TimeMonth", step1.drillFilter.TimeGranularity)
	}

	// Step 2: Tab to sub-aggregate view
	step1.rows = nil
	step1.loading = false
	step2 := applyMessageListKey(t, step1, keyTab())
	assertLevel(t, step2, levelDrillDown)

	// Simulate sub-agg data loaded, switch to Time view, toggle to Year
	step2.rows = []query.AggregateRow{
		{Key: "2024", Count: 87, TotalSize: 400000},
		{Key: "2023", Count: 113, TotalSize: 600000},
	}
	step2.loading = false
	step2.viewType = query.ViewTime
	step2.timeGranularity = query.TimeYear // User toggled granularity

	// Step 3: Enter on "2024" — this was the bug
	step2.cursor = 0
	step3 := applyAggregateKey(t, step2, keyEnter())

	assertLevel(t, step3, levelMessageList)

	// KEY ASSERTION: TimeGranularity must match the sub-agg view (Year), not the
	// stale value from the top-level drill (Month). Otherwise the query generates
	// a month-format expression compared against "2024", returning zero rows.
	if step3.drillFilter.TimeGranularity != query.TimeYear {
		t.Errorf("drillFilter.TimeGranularity = %v, want TimeYear (was stale TimeMonth from top-level drill)",
			step3.drillFilter.TimeGranularity)
	}
	if step3.drillFilter.TimePeriod != "2024" {
		t.Errorf("drillFilter.TimePeriod = %q, want %q", step3.drillFilter.TimePeriod, "2024")
	}
	// Original sender filter should be preserved
	if step3.drillFilter.Sender != "alice@example.com" {
		t.Errorf("drillFilter.Sender = %q, want %q", step3.drillFilter.Sender, "alice@example.com")
	}
}

// TestHeaderUpdateNoticeUnicode verifies update notice alignment with Unicode account names.
func TestHeaderUpdateNoticeUnicode(t *testing.T) {
	accountID := int64(1)
	model := NewBuilder().WithSize(100, 20).
		WithAccounts(query.AccountInfo{ID: 1, Identifier: "日本語ユーザー@example.com"}).
		WithAccountFilter(&accountID).Build()
	model.version = "abc1234"
	model.updateAvailable = "v1.2.3"

	header := model.headerView()
	lines := strings.Split(header, "\n")

	if !strings.Contains(lines[0], "v1.2.3") {
		t.Errorf("expected update notice in header, got: %s", lines[0])
	}
	// Verify the line doesn't exceed terminal width (lipgloss.Width accounts for wide chars)
	lineWidth := lipgloss.Width(lines[0])
	if lineWidth > 100 {
		t.Errorf("header line 1 width %d exceeds terminal width 100", lineWidth)
	}
}

// TestHeaderUpdateNoticeNarrowTerminal verifies update notice is omitted when terminal is too narrow.
func TestHeaderUpdateNoticeNarrowTerminal(t *testing.T) {
	model := NewBuilder().WithSize(40, 20).Build()
	model.version = "abc1234"
	model.updateAvailable = "v1.2.3"

	header := model.headerView()
	lines := strings.Split(header, "\n")

	// At 40 chars wide, the update notice shouldn't fit and should be omitted
	// (title + account already uses ~30 chars, notice needs ~25 more)
	lineWidth := lipgloss.Width(lines[0])
	if lineWidth > 40 {
		t.Errorf("header line 1 width %d exceeds narrow terminal width 40", lineWidth)
	}
}

// === Sender Names View Tests ===

// TestSenderNamesDrillDown verifies that pressing Enter on a SenderNames row
// sets drillFilter.SenderName and transitions to message list.
func TestSenderNamesDrillDown(t *testing.T) {
	rows := []query.AggregateRow{
		{Key: "Alice Smith", Count: 10},
		{Key: "Bob Jones", Count: 5},
	}

	model := NewBuilder().WithRows(rows...).
		WithPageSize(10).WithSize(100, 20).WithViewType(query.ViewSenderNames).Build()

	// Press Enter to drill into first sender name
	newModel, cmd := model.handleAggregateKeys(keyEnter())
	m := newModel.(Model)

	if m.level != levelMessageList {
		t.Errorf("expected levelMessageList, got %v", m.level)
	}
	if m.drillFilter.SenderName != "Alice Smith" {
		t.Errorf("expected drillFilter.SenderName='Alice Smith', got %q", m.drillFilter.SenderName)
	}
	if m.drillViewType != query.ViewSenderNames {
		t.Errorf("expected drillViewType=ViewSenderNames, got %v", m.drillViewType)
	}
	if cmd == nil {
		t.Error("expected command to load messages")
	}
	if len(m.breadcrumbs) != 1 {
		t.Errorf("expected 1 breadcrumb, got %d", len(m.breadcrumbs))
	}
}

// TestSenderNamesDrillDownEmptyKey verifies drilling into an empty sender name
// sets MatchEmptySenderName.
func TestSenderNamesDrillDownEmptyKey(t *testing.T) {
	rows := []query.AggregateRow{
		{Key: "", Count: 3},
	}

	model := NewBuilder().WithRows(rows...).
		WithPageSize(10).WithSize(100, 20).WithViewType(query.ViewSenderNames).Build()

	newModel, _ := model.handleAggregateKeys(keyEnter())
	m := newModel.(Model)

	if !m.drillFilter.MatchEmptySenderName {
		t.Error("expected MatchEmptySenderName=true for empty key")
	}
	if m.drillFilter.SenderName != "" {
		t.Errorf("expected empty SenderName, got %q", m.drillFilter.SenderName)
	}
}

// TestSenderNamesDrillFilterKey verifies drillFilterKey returns the SenderName.
func TestSenderNamesDrillFilterKey(t *testing.T) {
	model := NewBuilder().
		WithRows(query.AggregateRow{Key: "test", Count: 1}).
		WithPageSize(10).WithSize(100, 20).Build()
	model.drillViewType = query.ViewSenderNames
	model.drillFilter = query.MessageFilter{SenderName: "John Doe"}

	key := model.drillFilterKey()
	if key != "John Doe" {
		t.Errorf("expected drillFilterKey='John Doe', got %q", key)
	}

	// Test empty case
	model.drillFilter = query.MessageFilter{MatchEmptySenderName: true}
	key = model.drillFilterKey()
	if key != "(empty)" {
		t.Errorf("expected '(empty)' for MatchEmptySenderName, got %q", key)
	}
}

// TestSenderNamesBreadcrumbPrefix verifies the "N:" prefix in breadcrumbs.
func TestSenderNamesBreadcrumbPrefix(t *testing.T) {
	prefix := viewTypePrefix(query.ViewSenderNames)
	if prefix != "N" {
		t.Errorf("expected prefix 'N', got %q", prefix)
	}

	abbrev := viewTypeAbbrev(query.ViewSenderNames)
	if abbrev != "Sender Name" {
		t.Errorf("expected abbrev 'Sender Name', got %q", abbrev)
	}
}

// TestShiftTabCyclesSenderNames verifies shift+tab cycles backward through
// SenderNames in the correct order.
func TestShiftTabCyclesSenderNames(t *testing.T) {
	model := NewBuilder().
		WithRows(query.AggregateRow{Key: "test", Count: 1}).
		WithPageSize(10).WithSize(100, 20).
		WithViewType(query.ViewSenderNames).Build()

	// Shift+tab from SenderNames should go back to Senders
	m := applyAggregateKey(t, model, keyShiftTab())
	if m.viewType != query.ViewSenders {
		t.Errorf("expected ViewSenders after shift+tab from SenderNames, got %v", m.viewType)
	}
}

// TestSubAggregateFromSenderNames verifies that drilling from SenderNames
// and then tabbing skips SenderNames in the sub-aggregate cycle.
func TestSubAggregateFromSenderNames(t *testing.T) {
	rows := []query.AggregateRow{
		{Key: "Alice Smith", Count: 10},
	}
	msgs := []query.MessageSummary{
		{ID: 1, Subject: "Test"},
	}

	model := NewBuilder().WithRows(rows...).WithMessages(msgs...).
		WithPageSize(10).WithSize(100, 20).WithViewType(query.ViewSenderNames).Build()

	// Drill into the name
	newModel, _ := model.handleAggregateKeys(keyEnter())
	m := newModel.(Model)

	// Tab to sub-aggregate
	m.messages = msgs
	newModel2, _ := m.handleMessageListKeys(keyTab())
	m2 := newModel2.(Model)

	if m2.level != levelDrillDown {
		t.Fatalf("expected levelDrillDown, got %v", m2.level)
	}
	// Should skip SenderNames (the drill view type) and go to Recipients
	if m2.viewType != query.ViewRecipients {
		t.Errorf("expected ViewRecipients (skipping SenderNames), got %v", m2.viewType)
	}
}

// TestHasDrillFilterWithSenderName verifies hasDrillFilter returns true
// for SenderName and MatchEmptySenderName.
func TestHasDrillFilterWithSenderName(t *testing.T) {
	model := NewBuilder().
		WithRows(query.AggregateRow{Key: "test", Count: 1}).
		WithPageSize(10).WithSize(100, 20).Build()

	model.drillFilter = query.MessageFilter{SenderName: "John"}
	if !model.hasDrillFilter() {
		t.Error("expected hasDrillFilter=true for SenderName")
	}

	model.drillFilter = query.MessageFilter{MatchEmptySenderName: true}
	if !model.hasDrillFilter() {
		t.Error("expected hasDrillFilter=true for MatchEmptySenderName")
	}
}

// TestSenderNamesBreadcrumbRoundTrip verifies that drilling into a sender name,
// navigating to message detail, and going back preserves the SenderName filter.
func TestSenderNamesBreadcrumbRoundTrip(t *testing.T) {
	model := NewBuilder().
		WithMessages(
			query.MessageSummary{ID: 1, Subject: "Test message"},
		).
		WithLevel(levelMessageList).WithViewType(query.ViewRecipients).Build()
	model.drillFilter = query.MessageFilter{SenderName: "Alice Smith"}
	model.drillViewType = query.ViewSenderNames

	// Press Enter to go to message detail
	newModel, _ := model.Update(keyEnter())
	m := newModel.(Model)

	if m.level != levelMessageDetail {
		t.Fatalf("expected levelMessageDetail, got %v", m.level)
	}

	// Verify breadcrumb saved SenderName
	if len(m.breadcrumbs) == 0 {
		t.Fatal("expected breadcrumb to be saved")
	}
	bc := m.breadcrumbs[len(m.breadcrumbs)-1]
	if bc.state.drillFilter.SenderName != "Alice Smith" {
		t.Errorf("expected breadcrumb SenderName='Alice Smith', got %q", bc.state.drillFilter.SenderName)
	}

	// Press Esc to go back
	newModel2, _ := m.goBack()
	m2 := newModel2.(Model)

	if m2.drillFilter.SenderName != "Alice Smith" {
		t.Errorf("expected SenderName='Alice Smith' after goBack, got %q", m2.drillFilter.SenderName)
	}
	if m2.drillViewType != query.ViewSenderNames {
		t.Errorf("expected drillViewType=ViewSenderNames, got %v", m2.drillViewType)
	}
}

// =============================================================================
// RecipientNames tests
// =============================================================================

func TestRecipientNamesDrillDown(t *testing.T) {
	rows := []query.AggregateRow{
		{Key: "Bob Jones", Count: 10},
		{Key: "Carol White", Count: 5},
	}

	model := NewBuilder().WithRows(rows...).
		WithPageSize(10).WithSize(100, 20).WithViewType(query.ViewRecipientNames).Build()

	// Press Enter to drill into first recipient name
	newModel, cmd := model.handleAggregateKeys(keyEnter())
	m := newModel.(Model)

	if m.level != levelMessageList {
		t.Errorf("expected levelMessageList, got %v", m.level)
	}
	if m.drillFilter.RecipientName != "Bob Jones" {
		t.Errorf("expected drillFilter.RecipientName='Bob Jones', got %q", m.drillFilter.RecipientName)
	}
	if m.drillViewType != query.ViewRecipientNames {
		t.Errorf("expected drillViewType=ViewRecipientNames, got %v", m.drillViewType)
	}
	if cmd == nil {
		t.Error("expected command to load messages")
	}
	if len(m.breadcrumbs) != 1 {
		t.Errorf("expected 1 breadcrumb, got %d", len(m.breadcrumbs))
	}
}

func TestRecipientNamesDrillDownEmptyKey(t *testing.T) {
	rows := []query.AggregateRow{
		{Key: "", Count: 3},
	}

	model := NewBuilder().WithRows(rows...).
		WithPageSize(10).WithSize(100, 20).WithViewType(query.ViewRecipientNames).Build()

	newModel, _ := model.handleAggregateKeys(keyEnter())
	m := newModel.(Model)

	if !m.drillFilter.MatchEmptyRecipientName {
		t.Error("expected MatchEmptyRecipientName=true for empty key")
	}
	if m.drillFilter.RecipientName != "" {
		t.Errorf("expected empty RecipientName, got %q", m.drillFilter.RecipientName)
	}
}

func TestRecipientNamesDrillFilterKey(t *testing.T) {
	model := NewBuilder().
		WithRows(query.AggregateRow{Key: "test", Count: 1}).
		WithPageSize(10).WithSize(100, 20).Build()
	model.drillViewType = query.ViewRecipientNames
	model.drillFilter = query.MessageFilter{RecipientName: "Jane Doe"}

	key := model.drillFilterKey()
	if key != "Jane Doe" {
		t.Errorf("expected drillFilterKey='Jane Doe', got %q", key)
	}

	// Test empty case
	model.drillFilter = query.MessageFilter{MatchEmptyRecipientName: true}
	key = model.drillFilterKey()
	if key != "(empty)" {
		t.Errorf("expected '(empty)' for MatchEmptyRecipientName, got %q", key)
	}
}

func TestRecipientNamesBreadcrumbPrefix(t *testing.T) {
	prefix := viewTypePrefix(query.ViewRecipientNames)
	if prefix != "RN" {
		t.Errorf("expected prefix 'RN', got %q", prefix)
	}

	abbrev := viewTypeAbbrev(query.ViewRecipientNames)
	if abbrev != "Recipient Name" {
		t.Errorf("expected abbrev 'Recipient Name', got %q", abbrev)
	}
}

func TestShiftTabCyclesRecipientNames(t *testing.T) {
	model := NewBuilder().
		WithRows(query.AggregateRow{Key: "test", Count: 1}).
		WithPageSize(10).WithSize(100, 20).
		WithViewType(query.ViewRecipientNames).Build()

	// Shift+tab from RecipientNames should go back to Recipients
	m := applyAggregateKey(t, model, keyShiftTab())
	if m.viewType != query.ViewRecipients {
		t.Errorf("expected ViewRecipients after shift+tab from RecipientNames, got %v", m.viewType)
	}
}

func TestTabFromRecipientsThenRecipientNames(t *testing.T) {
	model := NewBuilder().
		WithRows(query.AggregateRow{Key: "test", Count: 1}).
		WithPageSize(10).WithSize(100, 20).
		WithViewType(query.ViewRecipients).Build()

	// Tab from Recipients should go to RecipientNames
	m := applyAggregateKey(t, model, keyTab())
	if m.viewType != query.ViewRecipientNames {
		t.Errorf("expected ViewRecipientNames after tab from Recipients, got %v", m.viewType)
	}

	// Tab from RecipientNames should go to Domains
	m.loading = false
	m = applyAggregateKey(t, m, keyTab())
	if m.viewType != query.ViewDomains {
		t.Errorf("expected ViewDomains after tab from RecipientNames, got %v", m.viewType)
	}
}

func TestSubAggregateFromRecipientNames(t *testing.T) {
	rows := []query.AggregateRow{
		{Key: "Bob Jones", Count: 10},
	}
	msgs := []query.MessageSummary{
		{ID: 1, Subject: "Test"},
	}

	model := NewBuilder().WithRows(rows...).WithMessages(msgs...).
		WithPageSize(10).WithSize(100, 20).WithViewType(query.ViewRecipientNames).Build()

	// Drill into the name
	newModel, _ := model.handleAggregateKeys(keyEnter())
	m := newModel.(Model)

	// Tab to sub-aggregate
	m.messages = msgs
	newModel2, _ := m.handleMessageListKeys(keyTab())
	m2 := newModel2.(Model)

	if m2.level != levelDrillDown {
		t.Fatalf("expected levelDrillDown, got %v", m2.level)
	}
	// nextSubGroupView(RecipientNames) = Domains
	if m2.viewType != query.ViewDomains {
		t.Errorf("expected ViewDomains (nextSubGroupView from RecipientNames), got %v", m2.viewType)
	}
}

func TestHasDrillFilterWithRecipientName(t *testing.T) {
	model := NewBuilder().
		WithRows(query.AggregateRow{Key: "test", Count: 1}).
		WithPageSize(10).WithSize(100, 20).Build()

	model.drillFilter = query.MessageFilter{RecipientName: "John"}
	if !model.hasDrillFilter() {
		t.Error("expected hasDrillFilter=true for RecipientName")
	}

	model.drillFilter = query.MessageFilter{MatchEmptyRecipientName: true}
	if !model.hasDrillFilter() {
		t.Error("expected hasDrillFilter=true for MatchEmptyRecipientName")
	}
}

func TestRecipientNamesBreadcrumbRoundTrip(t *testing.T) {
	model := NewBuilder().
		WithMessages(
			query.MessageSummary{ID: 1, Subject: "Test message"},
		).
		WithLevel(levelMessageList).WithViewType(query.ViewRecipients).Build()
	model.drillFilter = query.MessageFilter{RecipientName: "Bob Jones"}
	model.drillViewType = query.ViewRecipientNames

	// Press Enter to go to message detail
	newModel, _ := model.Update(keyEnter())
	m := newModel.(Model)

	if m.level != levelMessageDetail {
		t.Fatalf("expected levelMessageDetail, got %v", m.level)
	}

	// Verify breadcrumb saved RecipientName
	if len(m.breadcrumbs) == 0 {
		t.Fatal("expected breadcrumb to be saved")
	}
	bc := m.breadcrumbs[len(m.breadcrumbs)-1]
	if bc.state.drillFilter.RecipientName != "Bob Jones" {
		t.Errorf("expected breadcrumb RecipientName='Bob Jones', got %q", bc.state.drillFilter.RecipientName)
	}

	// Press Esc to go back
	newModel2, _ := m.goBack()
	m2 := newModel2.(Model)

	if m2.level != levelMessageList {
		t.Errorf("expected levelMessageList after goBack, got %v", m2.level)
	}
	if m2.drillFilter.RecipientName != "Bob Jones" {
		t.Errorf("expected RecipientName preserved after goBack, got %q", m2.drillFilter.RecipientName)
	}
	if m2.drillViewType != query.ViewRecipientNames {
		t.Errorf("expected drillViewType=ViewRecipientNames, got %v", m2.drillViewType)
	}
}

// =============================================================================
// t hotkey tests
// =============================================================================

func TestTKeyJumpsToTimeView(t *testing.T) {
	model := NewBuilder().
		WithRows(query.AggregateRow{Key: "test", Count: 10}).
		WithPageSize(10).WithSize(100, 20).
		WithViewType(query.ViewSenders).Build()

	// Press 't' from Senders view - should jump to Time
	m := applyAggregateKey(t, model, key('t'))
	if m.viewType != query.ViewTime {
		t.Errorf("expected ViewTime after 't' from Senders, got %v", m.viewType)
	}
	if !m.loading {
		t.Error("expected loading=true after 't' key")
	}
}

func TestTKeyJumpsToTimeFromAnyView(t *testing.T) {
	views := []query.ViewType{
		query.ViewSenders,
		query.ViewSenderNames,
		query.ViewRecipients,
		query.ViewRecipientNames,
		query.ViewDomains,
		query.ViewLabels,
	}

	for _, vt := range views {
		model := NewBuilder().
			WithRows(query.AggregateRow{Key: "test", Count: 10}).
			WithPageSize(10).WithSize(100, 20).
			WithViewType(vt).Build()

		m := applyAggregateKey(t, model, key('t'))
		if m.viewType != query.ViewTime {
			t.Errorf("from %v: expected ViewTime after 't', got %v", vt, m.viewType)
		}
	}
}

func TestTKeyCyclesGranularityInTimeView(t *testing.T) {
	model := NewBuilder().
		WithRows(query.AggregateRow{Key: "2024-01", Count: 10}).
		WithPageSize(10).WithSize(100, 20).
		WithViewType(query.ViewTime).Build()
	model.timeGranularity = query.TimeYear

	// Press 't' in Time view - should cycle granularity
	m := applyAggregateKey(t, model, key('t'))
	if m.viewType != query.ViewTime {
		t.Errorf("expected to stay in ViewTime, got %v", m.viewType)
	}
	if m.timeGranularity != query.TimeMonth {
		t.Errorf("expected TimeMonth after cycling from TimeYear, got %v", m.timeGranularity)
	}
}

func TestTKeyResetsSelectionOnJump(t *testing.T) {
	model := NewBuilder().
		WithRows(query.AggregateRow{Key: "test", Count: 10}).
		WithPageSize(10).WithSize(100, 20).
		WithViewType(query.ViewSenders).Build()
	model.selection.aggregateKeys["test"] = true
	model.cursor = 5
	model.scrollOffset = 3

	m := applyAggregateKey(t, model, key('t'))
	if len(m.selection.aggregateKeys) != 0 {
		t.Error("expected selection cleared after 't' jump")
	}
	if m.cursor != 0 {
		t.Errorf("expected cursor=0 after 't' jump, got %d", m.cursor)
	}
	if m.scrollOffset != 0 {
		t.Errorf("expected scrollOffset=0 after 't' jump, got %d", m.scrollOffset)
	}
}

func TestTKeyDoesNotResetSelectionOnCycle(t *testing.T) {
	model := NewBuilder().
		WithRows(query.AggregateRow{Key: "2024", Count: 10}, query.AggregateRow{Key: "2023", Count: 5}).
		WithPageSize(10).WithSize(100, 20).
		WithViewType(query.ViewTime).Build()
	model.timeGranularity = query.TimeYear
	model.selection.aggregateKeys["2024"] = true
	model.cursor = 1
	model.scrollOffset = 0

	// When already in Time view, 't' cycles granularity but preserves selection/cursor
	m := applyAggregateKey(t, model, key('t'))
	if m.viewType != query.ViewTime {
		t.Errorf("expected ViewTime, got %v", m.viewType)
	}
	if m.timeGranularity != query.TimeMonth {
		t.Errorf("expected TimeMonth, got %v", m.timeGranularity)
	}
	if !m.selection.aggregateKeys["2024"] {
		t.Error("expected selection preserved after 't' granularity cycle")
	}
	if m.cursor != 1 {
		t.Errorf("expected cursor=1 preserved, got %d", m.cursor)
	}
}

func TestTKeyNoOpInSubAggregateWhenDrillIsTime(t *testing.T) {
	model := NewBuilder().
		WithRows(query.AggregateRow{Key: "alice@example.com", Count: 10}).
		WithPageSize(10).WithSize(100, 20).
		WithLevel(levelDrillDown).WithViewType(query.ViewSenders).Build()
	model.drillViewType = query.ViewTime
	model.drillFilter = query.MessageFilter{TimePeriod: "2024"}

	// Press 't' in sub-aggregate where drill was from Time — should be a no-op
	m := applyAggregateKey(t, model, key('t'))
	if m.viewType != query.ViewSenders {
		t.Errorf("expected viewType unchanged (ViewSenders), got %v", m.viewType)
	}
	if m.loading {
		t.Error("expected loading=false (no-op)")
	}
}

// TestLoadDataSetsGroupByInStatsOpts verifies that loadData passes the current
// viewType as GroupBy in StatsOptions when search is active. This ensures the
// DuckDB engine searches the correct key columns for 1:N views.
func TestLoadDataSetsGroupByInStatsOpts(t *testing.T) {
	engine := &trackingMockEngine{
		mockEngine: mockEngine{
			rows: []query.AggregateRow{
				{Key: "bob@example.com", Count: 10, TotalSize: 5000},
			},
		},
		contextStats: &query.TotalStats{MessageCount: 10, TotalSize: 5000},
	}

	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})
	model.viewType = query.ViewRecipients
	model.searchQuery = "bob"
	model.level = levelAggregates
	model.width = 100
	model.height = 20

	// Execute the loadData command synchronously
	cmd := model.loadData()
	if cmd == nil {
		t.Fatal("expected loadData to return a command")
	}
	msg := cmd()

	// The command should have called GetTotalStats with GroupBy=ViewRecipients
	if engine.statsCallCount == 0 {
		t.Fatal("expected GetTotalStats to be called during loadData with search active")
	}
	if engine.lastStatsOpts.GroupBy != query.ViewRecipients {
		t.Errorf("expected StatsOptions.GroupBy=ViewRecipients, got %v", engine.lastStatsOpts.GroupBy)
	}
	if engine.lastStatsOpts.SearchQuery != "bob" {
		t.Errorf("expected StatsOptions.SearchQuery='bob', got %q", engine.lastStatsOpts.SearchQuery)
	}

	// Verify the result contains filteredStats
	dlm, ok := msg.(dataLoadedMsg)
	if !ok {
		t.Fatalf("expected dataLoadedMsg, got %T", msg)
	}
	if dlm.filteredStats == nil {
		t.Error("expected filteredStats to be set")
	}
}
