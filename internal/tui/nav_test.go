package tui

import (
	"context"
	"fmt"
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/wesm/msgvault/internal/query"
	"github.com/wesm/msgvault/internal/query/querytest"
)


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

	m, _ := sendMsg(t, model, staleMsg)

	// Stale response should be ignored - messages should be unchanged (empty)
	if len(m.messages) != 0 {
		t.Errorf("stale response should be ignored, got %d messages", len(m.messages))
	}

	// Now send a valid response with current request ID
	validMsg := messagesLoadedMsg{
		messages:  []query.MessageSummary{{ID: 1, Subject: "Valid"}},
		requestID: 5, // Current request ID
	}

	m, _ = sendMsg(t, m, validMsg)

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

	m, _ := sendMsg(t, model, staleMsg)

	// Stale response should be ignored
	if m.messageDetail != nil {
		t.Error("stale detail response should be ignored")
	}

	// Now send a valid response with current request ID
	validMsg := messageDetailLoadedMsg{
		detail:    &query.MessageDetail{ID: 1, Subject: "Valid Detail"},
		requestID: 10, // Current request ID
	}

	m, _ = sendMsg(t, m, validMsg)

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
	m, _ := sendMsg(t, model, tea.WindowSizeMsg{Width: 40, Height: 20})

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

	assertLevel(t, m, levelMessageList)
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

	assertLevel(t, m, levelDrillDown)
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

	assertLevel(t, m, levelMessageList)
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

	assertLevel(t, m, levelAggregates)
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

	assertLevel(t, m, levelMessageList)
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

	assertLevel(t, m, levelDrillDown)
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

	assertLevel(t, m, levelDrillDown)
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

	assertLevel(t, m, levelMessageList)
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

	assertLevel(t, m, levelMessageList)
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
	assertLevel(t, m, levelAggregates)
	// Cursor and scroll should reset
	if m.cursor != 0 {
		t.Errorf("expected cursor=0 after 'g' with no drill filter, got %d", m.cursor)
	}
	if m.scrollOffset != 0 {
		t.Errorf("expected scrollOffset=0 after 'g' with no drill filter, got %d", m.scrollOffset)
	}
}

// statsTracker records GetTotalStats calls on a querytest.MockEngine.
type statsTracker struct {
	callCount int
	lastOpts  query.StatsOptions
	result    *query.TotalStats // returned when non-nil; otherwise a default
}

// install wires the tracker into eng.GetTotalStatsFunc.
func (st *statsTracker) install(eng *querytest.MockEngine) {
	eng.GetTotalStatsFunc = func(_ context.Context, opts query.StatsOptions) (*query.TotalStats, error) {
		st.callCount++
		st.lastOpts = opts
		if st.result != nil {
			return st.result, nil
		}
		return &query.TotalStats{MessageCount: 1000, TotalSize: 5000000, AttachmentCount: 50}, nil
	}
}

// TestStatsUpdateOnDrillDown verifies stats are reloaded when drilling into a subgroup.
func TestStatsUpdateOnDrillDown(t *testing.T) {
	engine := newMockEngine(
		[]query.AggregateRow{
			{Key: "alice@example.com", Count: 100, TotalSize: 500000},
			{Key: "bob@example.com", Count: 50, TotalSize: 250000},
		},
		[]query.MessageSummary{{ID: 1, Subject: "Test"}},
		nil, nil,
	)
	tracker := &statsTracker{}
	tracker.install(engine)

	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})
	model.rows = engine.AggregateRows
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
	assertLevel(t, m, levelMessageList)

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
	rows := []query.AggregateRow{
		{Key: "alice@example.com", Count: 100, TotalSize: 500000, AttachmentSize: 100000},
		{Key: "bob@example.com", Count: 50, TotalSize: 250000, AttachmentSize: 50000},
	}
	engine := newMockEngine(rows, []query.MessageSummary{{ID: 1, Subject: "Test"}}, nil, nil)

	model := New(engine, Options{DataDir: "/tmp/test", Version: "test123"})
	model.rows = rows
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
	m := drillDown(t, model)

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
	m, _ := sendKey(t, model, keyTab())

	assertLevel(t, m, levelDrillDown)
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
	rows := makeRows(10)
	model := NewBuilder().WithRows(rows...).WithViewType(query.ViewSenders).Build()
	model.cursor = 5
	model.scrollOffset = 3

	// Drill down to message list (saves breadcrumb with cached rows)
	m, _ := sendKey(t, model, keyEnter())

	assertLevel(t, m, levelMessageList)

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
	m, _ := sendKey(t, model, keyEnter())

	assertLevel(t, m, levelMessageDetail)

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
	m, cmd := sendKey(t, model, keyRight())

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
	m, cmd = sendKey(t, m, keyLeft())

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
	m, cmd := sendKey(t, model, keyLeft())

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
	m, cmd = sendKey(t, m, keyRight())

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
	m, _ := sendKey(t, model, key('l'))

	if m.detailMessageIndex != 2 {
		t.Errorf("expected detailMessageIndex=2 after 'l', got %d", m.detailMessageIndex)
	}

	// Reset and press 'h' to go to previous message in list (lower index)
	m.detailMessageIndex = 1
	m.cursor = 1
	m, _ = sendKey(t, m, key('h'))

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
	assertLevel(t, m, levelMessageList)
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
	m, cmd := sendKey(t, model, keyRight())

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
	m, _ = sendKey(t, m, keyRight())

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
	m, _ = sendKey(t, m, keyLeft())

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
	m, _ = sendKey(t, m, keyLeft())

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
	m, _ := sendKey(t, model, keyDown())

	// detailScroll should be clamped to max (50 - 27 = 23 for detailPageSize)
	maxScroll := model.detailLineCount - m.detailPageSize()
	if maxScroll < 0 {
		maxScroll = 0
	}
	if m.detailScroll > maxScroll {
		t.Errorf("detailScroll=%d exceeds maxScroll=%d after resize", m.detailScroll, maxScroll)
	}
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
	assertLevel(t, m, levelMessageList)

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
	assertLevel(t, m, levelMessageList)

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

			assertState(t, m, levelMessageList, query.ViewTime, 0)

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

	assertLevel(t, m, levelMessageList)
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
	m, _ := sendKey(t, model, keyEnter())

	assertLevel(t, m, levelMessageDetail)

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

	assertLevel(t, m, levelMessageList)
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
	m, _ := sendKey(t, model, keyEnter())

	assertLevel(t, m, levelMessageDetail)

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
	engine := newMockEngine(
		[]query.AggregateRow{
			{Key: "bob@example.com", Count: 10, TotalSize: 5000},
		},
		nil, nil, nil,
	)
	tracker := &statsTracker{result: &query.TotalStats{MessageCount: 10, TotalSize: 5000}}
	tracker.install(engine)

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
	if tracker.callCount == 0 {
		t.Fatal("expected GetTotalStats to be called during loadData with search active")
	}
	if tracker.lastOpts.GroupBy != query.ViewRecipients {
		t.Errorf("expected StatsOptions.GroupBy=ViewRecipients, got %v", tracker.lastOpts.GroupBy)
	}
	if tracker.lastOpts.SearchQuery != "bob" {
		t.Errorf("expected StatsOptions.SearchQuery='bob', got %q", tracker.lastOpts.SearchQuery)
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
