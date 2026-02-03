package tui

import (
	"strings"
	"testing"

	"github.com/wesm/msgvault/internal/query"
)

func TestSearchModalOpen(t *testing.T) {
	model := NewBuilder().
		WithRows(makeRow("alice@example.com", 10)).
		Build()

	// Press '/' to activate inline search
	model, cmd := sendKey(t, model, key('/'))

	assertInlineSearchActive(t, model, true)
	assertSearchMode(t, model, searchModeFast)
	assertCmd(t, cmd, true)
}

// TestSearchResultsDisplay verifies search results are displayed.
func TestSearchResultsDisplay(t *testing.T) {
	model := NewBuilder().WithPageSize(10).WithSize(100, 20).Build()
	model.searchQuery = "test query"
	model.searchMode = searchModeFast
	model.searchRequestID = 1

	m := applySearchResults(t, model, 1, []query.MessageSummary{
		{ID: 1, Subject: "Result 1"},
		{ID: 2, Subject: "Result 2"},
	}, 0)

	assertLevel(t, m, levelMessageList)
	if len(m.messages) != 2 {
		t.Errorf("expected 2 messages, got %d", len(m.messages))
	}
	assertLoading(t, m, false, false)
}

// TestSearchResultsStale verifies stale search results are ignored.
func TestSearchResultsStale(t *testing.T) {
	model := NewBuilder().WithPageSize(10).WithSize(100, 20).Build()
	model.searchRequestID = 2 // Current request is 2

	m := applySearchResults(t, model, 1, []query.MessageSummary{
		{ID: 1, Subject: "Stale Result"},
	}, 0)

	// Messages should not be updated (still nil/empty)
	if len(m.messages) != 0 {
		t.Errorf("expected 0 messages (stale ignored), got %d", len(m.messages))
	}
}

// TestInlineSearchTabToggle verifies Tab key behavior across different search states.
func TestInlineSearchTabToggle(t *testing.T) {
	tests := []struct {
		name                    string
		level                   viewLevel
		initialMode             searchModeKind
		query                   string
		wantMode                searchModeKind
		wantCmd                 bool
		wantInlineSearchLoading bool
		wantRequestIDIncrement  bool
	}{
		{
			name:                    "toggle fast to deep at message list",
			level:                   levelMessageList,
			initialMode:             searchModeFast,
			query:                   "test query",
			wantMode:                searchModeDeep,
			wantCmd:                 true,
			wantInlineSearchLoading: true,
			wantRequestIDIncrement:  true,
		},
		{
			name:                    "toggle deep to fast at message list",
			level:                   levelMessageList,
			initialMode:             searchModeDeep,
			query:                   "test query",
			wantMode:                searchModeFast,
			wantCmd:                 true,
			wantInlineSearchLoading: true,
			wantRequestIDIncrement:  true,
		},
		{
			name:                    "no search with empty query",
			level:                   levelMessageList,
			initialMode:             searchModeFast,
			query:                   "",
			wantMode:                searchModeDeep,
			wantCmd:                 false,
			wantInlineSearchLoading: false,
			wantRequestIDIncrement:  false,
		},
		{
			name:                    "no effect at aggregate level",
			level:                   levelAggregates,
			initialMode:             searchModeFast,
			query:                   "test query",
			wantMode:                searchModeFast, // unchanged
			wantCmd:                 false,
			wantInlineSearchLoading: false,
			wantRequestIDIncrement:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			model := NewBuilder().WithPageSize(10).WithSize(100, 20).
				WithLevel(tt.level).
				WithActiveSearch(tt.query, tt.initialMode).
				Build()
			initialRequestID := model.searchRequestID

			m, cmd := applyInlineSearchKey(t, model, keyTab())

			assertSearchMode(t, m, tt.wantMode)
			assertCmd(t, cmd, tt.wantCmd)

			if m.inlineSearchLoading != tt.wantInlineSearchLoading {
				t.Errorf("expected inlineSearchLoading=%v, got %v", tt.wantInlineSearchLoading, m.inlineSearchLoading)
			}

			if tt.wantRequestIDIncrement {
				if m.searchRequestID != initialRequestID+1 {
					t.Errorf("expected searchRequestID to increment by 1 (from %d to %d), got %d",
						initialRequestID, initialRequestID+1, m.searchRequestID)
				}
			} else if m.searchRequestID != initialRequestID {
				t.Errorf("expected searchRequestID to remain %d, got %d", initialRequestID, m.searchRequestID)
			}
		})
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

	assertSearchQuery(t, m, "")
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
	m, cmd := applyAggregateKeyWithCmd(t, model, key('/'))

	assertInlineSearchActive(t, m, true)
	assertCmd(t, cmd, true)
}

// TestSearchFromMessageList verifies search from message list view.
func TestSearchFromMessageList(t *testing.T) {
	model := NewBuilder().
		WithMessages(query.MessageSummary{ID: 1, Subject: "Test"}).
		WithPageSize(10).WithSize(100, 20).
		WithLevel(levelMessageList).Build()

	// Press '/' to activate inline search
	m, cmd := applyMessageListKeyWithCmd(t, model, key('/'))

	assertInlineSearchActive(t, m, true)
	assertCmd(t, cmd, true)
}

// TestGKeyCyclesViewType verifies that 'g' cycles through view types at aggregate level.

// TestSearchSetsContextStats verifies search results set contextStats for header metrics.
func TestSearchSetsContextStats(t *testing.T) {
	model := NewBuilder().Build()
	model.searchRequestID = 1

	m := applySearchResults(t, model, 1, make([]query.MessageSummary, 10), 150)

	assertContextStats(t, m, 150, -1, -1)
}

// TestSearchZeroResultsClearsContextStats verifies contextStats is set to zero on empty search.
func TestSearchZeroResultsClearsContextStats(t *testing.T) {
	model := NewBuilder().
		WithContextStats(&query.TotalStats{MessageCount: 500}).
		Build()
	model.searchRequestID = 1

	m := applySearchResults(t, model, 1, []query.MessageSummary{}, 0)

	assertContextStats(t, m, 0, -1, -1)
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

	m := applySearchResultsAppend(t, model, 1, make([]query.MessageSummary, 50), -1)

	if len(m.messages) != 100 {
		t.Errorf("expected 100 messages after append, got %d", len(m.messages))
	}
	assertContextStats(t, m, 100, -1, -1)
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
	assertContextStats(t, m, 100, 1000, 5)

	// Simulate searchResultsMsg arriving with total count
	m2 := applySearchResults(t, m, m.searchRequestID, []query.MessageSummary{{ID: 1}, {ID: 2}}, 100)

	// contextStats should preserve TotalSize and AttachmentCount from drill-down
	assertContextStats(t, m2, 100, 1000, 5)
}

// TestSearchResultsWithoutDrillDownContextStats verifies that when searching
// without a drill-down context, contextStats is created with only MessageCount.
func TestSearchResultsWithoutDrillDownContextStats(t *testing.T) {
	model := newTestModelAtLevel(levelMessageList)
	model.searchRequestID = 1

	m := applySearchResults(t, model, 1, []query.MessageSummary{{ID: 1}, {ID: 2}}, 50)

	assertContextStats(t, m, 50, 0, 0)
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

	// Capture initial request IDs to verify exact increments
	initialLoadRequestID := model.loadRequestID
	initialSearchRequestID := model.searchRequestID

	// Press Enter to drill down
	m, cmd := applyAggregateKeyWithCmd(t, model, keyEnter())

	assertLevel(t, m, levelMessageList)
	assertSearchQuery(t, m, "")
	assertCmd(t, cmd, true) // Should return command to load messages

	if m.loadRequestID != initialLoadRequestID+1 {
		t.Errorf("expected loadRequestID to increment by 1 (from %d to %d), got %d",
			initialLoadRequestID, initialLoadRequestID+1, m.loadRequestID)
	}
	if m.searchRequestID != initialSearchRequestID+1 {
		t.Errorf("expected searchRequestID to increment by 1 (from %d to %d), got %d",
			initialSearchRequestID, initialSearchRequestID+1, m.searchRequestID)
	}
}

// TestDrillDownWithoutSearchQueryUsesLoadMessages verifies that drilling down
// without a search filter uses loadMessages (not search).
func TestDrillDownWithoutSearchQueryUsesLoadMessages(t *testing.T) {
	model := newTestModelWithRows(testAggregateRows)
	model.level = levelAggregates
	model.searchQuery = "" // No search filter
	model.cursor = 0

	// Capture initial request IDs to verify exact increments
	initialLoadRequestID := model.loadRequestID
	initialSearchRequestID := model.searchRequestID

	m, cmd := applyAggregateKeyWithCmd(t, model, keyEnter())

	assertLevel(t, m, levelMessageList)
	assertCmd(t, cmd, true) // Should return command to load messages

	if m.loadRequestID != initialLoadRequestID+1 {
		t.Errorf("expected loadRequestID to increment by 1 (from %d to %d), got %d",
			initialLoadRequestID, initialLoadRequestID+1, m.loadRequestID)
	}
	if m.searchRequestID != initialSearchRequestID+1 {
		t.Errorf("expected searchRequestID to increment by 1 (from %d to %d), got %d",
			initialSearchRequestID, initialSearchRequestID+1, m.searchRequestID)
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

	// Capture initial request IDs to verify exact increments
	initialLoadRequestID := model.loadRequestID
	initialSearchRequestID := model.searchRequestID

	m, cmd := applyAggregateKeyWithCmd(t, model, keyEnter())

	assertLevel(t, m, levelMessageList)
	assertSearchQuery(t, m, "")
	assertCmd(t, cmd, true) // Should return command to load messages

	if m.loadRequestID != initialLoadRequestID+1 {
		t.Errorf("expected loadRequestID to increment by 1 (from %d to %d), got %d",
			initialLoadRequestID, initialLoadRequestID+1, m.loadRequestID)
	}
	if m.searchRequestID != initialSearchRequestID+1 {
		t.Errorf("expected searchRequestID to increment by 1 (from %d to %d), got %d",
			initialSearchRequestID, initialSearchRequestID+1, m.searchRequestID)
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
	m := applyAggregateKey(t, model, keyEnter())

	assertSearchQuery(t, m, "")
	assertLevel(t, m, levelMessageList)

	// Populate messages so Esc handler works
	m.messages = []query.MessageSummary{{ID: 1}}

	// Esc back — should restore outer search from breadcrumb
	m2 := applyMessageListKey(t, m, keyEsc())

	assertLevel(t, m2, levelAggregates)
	assertSearchQuery(t, m2, "important")
}

// TestDrillDownClearsHighlightTerms verifies that highlightTerms produces no
// highlighting after drill-down (since searchQuery is empty).
func TestDrillDownClearsHighlightTerms(t *testing.T) {
	model := newTestModelWithRows(testAggregateRows)
	model.level = levelAggregates
	model.searchQuery = "alice"
	model.cursor = 0

	m := applyAggregateKey(t, model, keyEnter())

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
	m := applyAggregateKey(t, model, keyEnter())

	assertSearchQuery(t, m, "")

	// Populate messages and go back
	m.messages = []query.MessageSummary{{ID: 1}}
	m2 := applyMessageListKey(t, m, keyEsc())

	assertSearchQuery(t, m2, "urgent")
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
	m := applyAggregateKey(t, model, keyEnter())

	// Populate the message list with expected data
	m.messages = []query.MessageSummary{{ID: 100, Subject: "Drilled message"}}
	m.loading = false

	// Simulate a stale search response arriving with the old requestID
	m2 := applySearchResults(t, m, staleRequestID, []query.MessageSummary{{ID: 999, Subject: "Stale search result"}}, 0)

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

	// Activate inline search — should snapshot and return blink command
	cmd := model.activateInlineSearch("search")
	assertCmd(t, cmd, true) // Should return textinput.Blink command

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
	m, _ := applyInlineSearchKey(t, model, keyEsc())

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
	assertSearchQuery(t, m, "")
	if m.searchLoadingMore {
		t.Error("expected searchLoadingMore=false after restore")
	}
	if m.searchOffset != 0 {
		t.Errorf("expected searchOffset=0, got %d", m.searchOffset)
	}
	if m.searchTotalCount != 0 {
		t.Errorf("expected searchTotalCount=0, got %d", m.searchTotalCount)
	}
	assertLoading(t, m, false, false)

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
	m := applyAggregateKey(t, model, keyEnter())
	m.messages = []query.MessageSummary{{ID: 1}, {ID: 2}, {ID: 3}}
	m.loading = false

	// Activate search and simulate results
	cmd := m.activateInlineSearch("search")
	assertCmd(t, cmd, true)      // Should return textinput.Blink command
	m.inlineSearchActive = false // Simulate search submitted
	m.searchQuery = "test"
	m.messages = []query.MessageSummary{{ID: 99}}

	// First Esc — should clear search and restore pre-search messages
	m2 := applyMessageListKey(t, m, keyEsc())

	assertSearchQuery(t, m2, "")
	assertLevel(t, m2, levelMessageList)
	if len(m2.messages) != 3 {
		t.Errorf("expected 3 pre-search messages restored, got %d", len(m2.messages))
	}

	// Second Esc — should goBack to aggregates
	m3 := applyMessageListKey(t, m2, keyEsc())

	assertLevel(t, m3, levelAggregates)
}

// TestHighlightedColumnsAligned verifies that highlighting search terms in
// aggregate rows doesn't break column alignment.
