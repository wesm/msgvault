package tui

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/muesli/termenv"
	"github.com/wesm/msgvault/internal/query"
	"github.com/wesm/msgvault/internal/query/querytest"
	"github.com/wesm/msgvault/internal/search"
)

// ansiStart is the escape sequence prefix found in styled terminal output.
const ansiStart = "\x1b["

// colorProfileMu serializes tests that mutate the global lipgloss color profile.
var colorProfileMu sync.Mutex

// forceColorProfile sets lipgloss to ANSI color output for tests that assert
// on styled output. It acquires colorProfileMu to prevent data races with
// parallel tests and restores the original profile via t.Cleanup.
func forceColorProfile(t *testing.T) {
	t.Helper()
	colorProfileMu.Lock()
	orig := lipgloss.ColorProfile()
	lipgloss.SetColorProfile(termenv.ANSI)
	t.Cleanup(func() {
		lipgloss.SetColorProfile(orig)
		colorProfileMu.Unlock()
	})
}

var ansiPattern = regexp.MustCompile(`\x1b\[[0-9;?]*[ -/]*[@-~]`)

func stripANSI(s string) string {
	return ansiPattern.ReplaceAllString(s, "")
}

// newMockEngine creates a querytest.MockEngine configured for TUI testing.
// The messages slice is returned from ListMessages, Search, SearchFast, and
// SearchFastCount, matching the legacy mockEngine behavior.
func newMockEngine(rows []query.AggregateRow, messages []query.MessageSummary, detail *query.MessageDetail, gmailIDs []string) *querytest.MockEngine {
	eng := &querytest.MockEngine{
		AggregateRows:     rows,
		ListResults:       messages,
		SearchResults:     messages,
		SearchFastResults: messages,
		GmailIDs:          gmailIDs,
	}
	eng.GetMessageFunc = func(_ context.Context, _ int64) (*query.MessageDetail, error) {
		return detail, nil
	}
	eng.SearchFastCountFunc = func(_ context.Context, _ *search.Query, _ query.MessageFilter) (int64, error) {
		return int64(len(messages)), nil
	}
	return eng
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
	contextStats      *query.TotalStats
	activeSearchQuery string
	activeSearchMode  *searchModeKind
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

// WithStandardAccounts adds the standard testAccounts (user1@gmail.com, user2@gmail.com).
func (b *TestModelBuilder) WithStandardAccounts() *TestModelBuilder {
	b.accounts = testAccounts
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
	engine := newMockEngine(b.rows, b.messages, b.messageDetail, b.gmailIDs)

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

	if b.activeSearchMode != nil {
		model.inlineSearchActive = true
		model.searchMode = *b.activeSearchMode
		model.searchInput.SetValue(b.activeSearchQuery)
	}

	return model
}

// sendKey sends a key message to the model and returns the updated concrete Model.
func sendKey(t *testing.T, m Model, k tea.KeyMsg) (Model, tea.Cmd) {
	t.Helper()
	newM, cmd := m.Update(k)
	return newM.(Model), cmd
}

// sendMsg sends any tea.Msg through Update and returns the concrete Model.
func sendMsg(t *testing.T, m Model, msg tea.Msg) (Model, tea.Cmd) {
	t.Helper()
	newM, cmd := m.Update(msg)
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

	// testAccounts provides a standard pair of test accounts for deletion tests.
	testAccounts = []query.AccountInfo{
		{ID: 1, Identifier: "user1@gmail.com"},
		{ID: 2, Identifier: "user2@gmail.com"},
	}

	// standardRows provides the common alice/bob pair used in view render tests.
	standardRows = []query.AggregateRow{
		{Key: "alice@example.com", Count: 100, TotalSize: 500000},
		{Key: "bob@example.com", Count: 50, TotalSize: 250000},
	}

	// standardStats provides the common stats object (1000 msgs, ~5MB, 50 attachments).
	standardStats = &query.TotalStats{MessageCount: 1000, TotalSize: 5000000, AttachmentCount: 50}
)

// newTestModel creates a Model with common test defaults.
// The returned model has standard width/height and is ready for testing.
func newTestModel(engine *querytest.MockEngine) Model {
	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})
	model.width = 100
	model.height = 24
	model.pageSize = 10
	return model
}

// newTestModelWithRows creates a test model pre-populated with aggregate rows.
func newTestModelWithRows(rows []query.AggregateRow) Model {
	engine := newMockEngine(rows, nil, nil, nil)
	model := newTestModel(engine)
	model.rows = rows
	return model
}

// newTestModelAtLevel creates a test model at the specified navigation level.
func newTestModelAtLevel(level viewLevel) Model {
	engine := newMockEngine(nil, nil, nil, nil)
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

// makeRows creates n AggregateRows with sequential keys ("row-0", "row-1", ...).
func makeRows(n int) []query.AggregateRow {
	rows := make([]query.AggregateRow, n)
	for i := range rows {
		rows[i] = query.AggregateRow{
			Key:   fmt.Sprintf("row-%d", i),
			Count: int64((i + 1) * 10),
		}
	}
	return rows
}

// makeMessages creates n MessageSummary values with sequential IDs and subjects.
func makeMessages(n int) []query.MessageSummary {
	msgs := make([]query.MessageSummary, n)
	for i := range msgs {
		msgs[i] = query.MessageSummary{
			ID:      int64(i + 1),
			Subject: fmt.Sprintf("Subject %d", i+1),
		}
	}
	return msgs
}

// assertDrillState checks level, drill filter field, and that a command was returned.
func assertDrillState(t *testing.T, m Model, wantLevel viewLevel, wantFilterField, wantFilterValue string) {
	t.Helper()
	if m.level != wantLevel {
		t.Errorf("level: got %v, want %v", m.level, wantLevel)
	}
	var got string
	switch wantFilterField {
	case "Sender":
		got = m.drillFilter.Sender
	case "Recipient":
		got = m.drillFilter.Recipient
	case "SenderName":
		got = m.drillFilter.SenderName
	case "RecipientName":
		got = m.drillFilter.RecipientName
	case "Domain":
		got = m.drillFilter.Domain
	case "Label":
		got = m.drillFilter.Label
	case "TimePeriod":
		got = m.drillFilter.TimePeriod
	default:
		t.Fatalf("unknown filter field %q", wantFilterField)
	}
	if got != wantFilterValue {
		t.Errorf("drillFilter.%s: got %q, want %q", wantFilterField, got, wantFilterValue)
	}
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

// selectRow moves the cursor to the given index and toggles selection with space.
func selectRow(t *testing.T, m Model, index int) Model {
	t.Helper()
	m.cursor = index
	return applyAggregateKey(t, m, key(' '))
}

// assertPendingManifest asserts that pendingManifest is non-nil and its Account
// filter matches wantAccount.
func assertPendingManifest(t *testing.T, m Model, wantAccount string) {
	t.Helper()
	if m.pendingManifest == nil {
		t.Fatal("expected pendingManifest to be set")
	}
	if m.pendingManifest.Filters.Account != wantAccount {
		t.Errorf("expected manifest account=%q, got %q", wantAccount, m.pendingManifest.Filters.Account)
	}
}

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

// assertHeaderLine splits the header into lines, checks the line count is sufficient,
// and asserts that the specified line contains all of the given substrings.
func assertHeaderLine(t *testing.T, model Model, lineIdx int, wantSubstrings ...string) {
	t.Helper()
	header := model.headerView()
	lines := strings.Split(header, "\n")
	if lineIdx >= len(lines) {
		t.Fatalf("header has %d lines, want line %d", len(lines), lineIdx)
	}
	for _, want := range wantSubstrings {
		if !strings.Contains(lines[lineIdx], want) {
			t.Errorf("header line %d missing %q: %q", lineIdx, want, lines[lineIdx])
		}
	}
}

// assertHeaderLineNot asserts that the specified header line does NOT contain the given substring.
func assertHeaderLineNot(t *testing.T, model Model, lineIdx int, notWant string) {
	t.Helper()
	header := model.headerView()
	lines := strings.Split(header, "\n")
	if lineIdx >= len(lines) {
		t.Fatalf("header has %d lines, want line %d", len(lines), lineIdx)
	}
	if strings.Contains(lines[lineIdx], notWant) {
		t.Errorf("header line %d should not contain %q: %q", lineIdx, notWant, lines[lineIdx])
	}
}

// assertState checks level, viewType, and cursor in one call.
func assertState(t *testing.T, m Model, level viewLevel, view query.ViewType, cursor int) {
	t.Helper()
	if m.level != level {
		t.Errorf("want level %v, got %v", level, m.level)
	}
	if m.viewType != view {
		t.Errorf("want viewType %v, got %v", view, m.viewType)
	}
	if m.cursor != cursor {
		t.Errorf("want cursor %d, got %d", cursor, m.cursor)
	}
}

// drillDown presses Enter on the current cursor item and returns the updated Model.
func drillDown(t *testing.T, m Model) Model {
	t.Helper()
	m, _ = sendKey(t, m, keyEnter())
	return m
}

// -----------------------------------------------------------------------------
// Search Helpers - reduce search test boilerplate
// -----------------------------------------------------------------------------

// WithActiveSearch configures the builder's model for inline search state.
func (b *TestModelBuilder) WithActiveSearch(q string, mode searchModeKind) *TestModelBuilder {
	b.activeSearchQuery = q
	b.activeSearchMode = &mode
	return b
}

// applySearchResults simulates the arrival of search results via Update.
func applySearchResults(t *testing.T, m Model, reqID uint64, msgs []query.MessageSummary, total int64) Model {
	t.Helper()
	msg := searchResultsMsg{
		messages:   msgs,
		requestID:  reqID,
		totalCount: total,
	}
	newModel, _ := m.Update(msg)
	return newModel.(Model)
}

// applySearchResultsAppend simulates appended (paginated) search results.
func applySearchResultsAppend(t *testing.T, m Model, reqID uint64, msgs []query.MessageSummary, total int64) Model {
	t.Helper()
	msg := searchResultsMsg{
		messages:   msgs,
		requestID:  reqID,
		totalCount: total,
		append:     true,
	}
	newModel, _ := m.Update(msg)
	return newModel.(Model)
}

// assertContextStats checks contextStats fields. Use -1 for size or attachments to skip that check.
func assertContextStats(t *testing.T, m Model, wantCount int, wantSize int64, wantAttach int) {
	t.Helper()
	if m.contextStats == nil {
		t.Fatal("expected contextStats to be set")
	}
	if m.contextStats.MessageCount != int64(wantCount) {
		t.Errorf("got MessageCount=%d, want %d", m.contextStats.MessageCount, wantCount)
	}
	if wantSize != -1 && m.contextStats.TotalSize != wantSize {
		t.Errorf("got TotalSize=%d, want %d", m.contextStats.TotalSize, wantSize)
	}
	if wantAttach != -1 && m.contextStats.AttachmentCount != int64(wantAttach) {
		t.Errorf("got AttachmentCount=%d, want %d", m.contextStats.AttachmentCount, wantAttach)
	}
}

// =============================================================================
// Tests
// =============================================================================

