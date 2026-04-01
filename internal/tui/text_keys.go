package tui

import (
	tea "github.com/charmbracelet/bubbletea"
	"github.com/wesm/msgvault/internal/query"
)

// handleTextKeyPress dispatches key events when in Texts mode.
func (m Model) handleTextKeyPress(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	// Modal takes priority
	if m.modal != modalNone {
		return m.handleModalKeys(msg)
	}

	// Inline search takes priority over view keys
	if m.inlineSearchActive {
		return m.handleTextInlineSearchKeys(msg)
	}

	// Check global keys first (q, ?, m)
	newM, cmd, handled := m.handleGlobalKeys(msg)
	if handled {
		return newM, cmd
	}

	// Disable selection/deletion keys in Texts mode (read-only)
	switch msg.String() {
	case " ", "S", "d", "D", "x":
		return m, nil
	}

	switch m.textState.level {
	case textLevelConversations, textLevelAggregate,
		textLevelDrillConversations:
		return m.handleTextListKeys(msg)
	case textLevelTimeline:
		return m.handleTextTimelineKeys(msg)
	}
	return m, nil
}

// handleTextListKeys handles keys in text list views
// (conversations, aggregates, drill-down conversations).
func (m Model) handleTextListKeys(
	msg tea.KeyMsg,
) (tea.Model, tea.Cmd) {
	// Handle list navigation
	if m.navigateList(msg.String(), m.textRowCount()) {
		return m, nil
	}

	switch msg.String() {
	case "tab", "Tab":
		m.cycleTextViewType(true)
		m.loading = true
		return m, m.loadTextData()

	case "shift+tab":
		m.cycleTextViewType(false)
		m.loading = true
		return m, m.loadTextData()

	case "enter":
		return m.textDrillDown()

	case "esc", "backspace":
		return m.textGoBack()

	case "s":
		m.cycleTextSortField()
		m.loading = true
		return m, m.loadTextData()

	case "r", "v":
		if m.textState.filter.SortDirection == query.SortDesc {
			m.textState.filter.SortDirection = query.SortAsc
		} else {
			m.textState.filter.SortDirection = query.SortDesc
		}
		m.loading = true
		return m, m.loadTextData()

	case "t":
		m.textState.viewType = query.TextViewTime
		m.textState.level = textLevelAggregate
		m.textState.cursor = 0
		m.textState.scrollOffset = 0
		m.loading = true
		return m, m.loadTextData()

	case "a":
		// Reset to conversations view (clear filters)
		m.textState = textState{
			viewType: query.TextViewConversations,
		}
		m.loading = true
		return m, m.loadTextConversations()

	case "A":
		m.openAccountSelector()
		return m, nil

	case "/":
		return m, m.activateInlineSearch("search texts")
	}

	return m, nil
}

// handleTextTimelineKeys handles keys in the text timeline view.
func (m Model) handleTextTimelineKeys(
	msg tea.KeyMsg,
) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "esc", "backspace":
		return m.textGoBack()

	case "j", "down":
		m.textMoveCursor(1)
		return m, nil

	case "k", "up":
		m.textMoveCursor(-1)
		return m, nil

	case "pgup", "ctrl+u":
		m.textMoveCursor(-m.visibleRows())
		return m, nil

	case "pgdown", "ctrl+d":
		m.textMoveCursor(m.visibleRows())
		return m, nil

	case "home":
		m.textState.cursor = 0
		m.textState.scrollOffset = 0
		return m, nil

	case "end", "G":
		maxIdx := m.textRowCount() - 1
		if maxIdx < 0 {
			maxIdx = 0
		}
		m.textState.cursor = maxIdx
		return m, nil
	}
	return m, nil
}

// handleTextInlineSearchKeys handles keys when inline search is
// active in Texts mode. Enter commits the search; Esc cancels.
func (m Model) handleTextInlineSearchKeys(
	msg tea.KeyMsg,
) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "enter":
		m.exitInlineSearchMode()
		queryStr := m.searchInput.Value()
		if queryStr == "" {
			return m, nil
		}
		m.loading = true
		return m, m.loadTextSearch(queryStr)

	case "esc":
		m.exitInlineSearchMode()
		m.searchInput.SetValue("")
		return m, nil

	case "ctrl+c":
		m.quitting = true
		return m, tea.Quit

	default:
		var cmd tea.Cmd
		m.searchInput, cmd = m.searchInput.Update(msg)
		return m, cmd
	}
}

// cycleTextViewType cycles through text view types.
func (m *Model) cycleTextViewType(forward bool) {
	if forward {
		m.textState.viewType++
		if m.textState.viewType >= query.TextViewTypeCount {
			m.textState.viewType = 0
		}
	} else {
		if m.textState.viewType == 0 {
			m.textState.viewType = query.TextViewTypeCount - 1
		} else {
			m.textState.viewType--
		}
	}
	if m.textState.viewType == query.TextViewConversations {
		m.textState.level = textLevelConversations
	} else {
		m.textState.level = textLevelAggregate
	}
	m.textState.cursor = 0
	m.textState.scrollOffset = 0
}

// textMoveCursor moves the cursor by delta and adjusts scroll offset.
func (m *Model) textMoveCursor(delta int) {
	m.textState.cursor += delta
	maxIdx := m.textRowCount() - 1
	if maxIdx < 0 {
		maxIdx = 0
	}
	if m.textState.cursor < 0 {
		m.textState.cursor = 0
	}
	if m.textState.cursor > maxIdx {
		m.textState.cursor = maxIdx
	}
	m.textState.scrollOffset = calculateScrollOffset(
		m.textState.cursor,
		m.textState.scrollOffset,
		m.visibleRows(),
	)
}

// textRowCount returns the number of rows in the current text view.
func (m Model) textRowCount() int {
	switch m.textState.level {
	case textLevelConversations, textLevelDrillConversations:
		return len(m.textState.conversations)
	case textLevelAggregate:
		return len(m.textState.aggregateRows)
	case textLevelTimeline:
		return len(m.textState.messages)
	}
	return 0
}

// cycleTextSortField cycles between sort fields for text views.
func (m *Model) cycleTextSortField() {
	switch m.textState.filter.SortField {
	case query.SortByCount:
		m.textState.filter.SortField = query.SortByName
	default:
		m.textState.filter.SortField = query.SortByCount
	}
}

// textDrillDown enters the selected item in text mode.
func (m Model) textDrillDown() (tea.Model, tea.Cmd) {
	switch m.textState.level {
	case textLevelConversations, textLevelDrillConversations:
		if m.textState.cursor >= len(m.textState.conversations) {
			return m, nil
		}
		conv := m.textState.conversations[m.textState.cursor]
		m.textState.breadcrumbs = append(
			m.textState.breadcrumbs,
			textNavSnapshot{
				level:          m.textState.level,
				viewType:       m.textState.viewType,
				cursor:         m.textState.cursor,
				scrollOffset:   m.textState.scrollOffset,
				filter:         m.textState.filter,
				selectedConvID: m.textState.selectedConvID,
			},
		)
		m.textState.selectedConvID = conv.ConversationID
		m.textState.level = textLevelTimeline
		m.textState.cursor = 0
		m.textState.scrollOffset = 0
		m.loading = true
		return m, m.loadTextMessages()

	case textLevelAggregate:
		if m.textState.cursor >= len(m.textState.aggregateRows) {
			return m, nil
		}
		row := m.textState.aggregateRows[m.textState.cursor]
		m.textState.breadcrumbs = append(
			m.textState.breadcrumbs,
			textNavSnapshot{
				level:          m.textState.level,
				viewType:       m.textState.viewType,
				cursor:         m.textState.cursor,
				scrollOffset:   m.textState.scrollOffset,
				filter:         m.textState.filter,
				selectedConvID: m.textState.selectedConvID,
			},
		)
		// Apply aggregate filter and drill to conversations
		switch m.textState.viewType {
		case query.TextViewContacts:
			m.textState.filter.ContactPhone = row.Key
		case query.TextViewContactNames:
			m.textState.filter.ContactName = row.Key
		case query.TextViewSources:
			m.textState.filter.SourceType = row.Key
		case query.TextViewLabels:
			m.textState.filter.Label = row.Key
		}
		m.textState.level = textLevelDrillConversations
		m.textState.cursor = 0
		m.textState.scrollOffset = 0
		m.loading = true
		return m, m.loadTextConversations()
	}
	return m, nil
}

// textGoBack returns to the previous text navigation state.
func (m Model) textGoBack() (tea.Model, tea.Cmd) {
	if len(m.textState.breadcrumbs) == 0 {
		return m, nil
	}
	snap := m.textState.breadcrumbs[len(m.textState.breadcrumbs)-1]
	m.textState.breadcrumbs = m.textState.breadcrumbs[:len(m.textState.breadcrumbs)-1]
	m.textState.level = snap.level
	m.textState.viewType = snap.viewType
	m.textState.cursor = snap.cursor
	m.textState.scrollOffset = snap.scrollOffset
	m.textState.filter = snap.filter
	m.textState.selectedConvID = snap.selectedConvID
	m.loading = true
	return m, m.loadTextData()
}
