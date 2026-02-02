package tui

import (
	"strings"
	"testing"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/wesm/msgvault/internal/query"
)


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
