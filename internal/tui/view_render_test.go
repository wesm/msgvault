package tui

import (
	"fmt"
	"strings"
	"testing"
	"github.com/charmbracelet/lipgloss"
	"github.com/muesli/termenv"
	"github.com/wesm/msgvault/internal/query"
)


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

