package tui

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/lipgloss"
	"github.com/wesm/msgvault/internal/query"
	"github.com/wesm/msgvault/internal/textutil"
)

// renderTextView renders the current Texts mode view.
func (m Model) renderTextView() string {
	header := m.textHeaderView()
	var body string
	switch m.textState.level {
	case textLevelConversations, textLevelDrillConversations:
		body = m.textConversationsView()
	case textLevelAggregate:
		body = m.textAggregateView()
	case textLevelTimeline:
		body = m.textTimelineView()
	default:
		body = m.textConversationsView()
	}
	footer := m.textFooterView()
	return fmt.Sprintf("%s\n%s\n%s", header, body, footer)
}

// textHeaderView renders the Texts mode header (title bar + breadcrumb).
func (m Model) textHeaderView() string {
	line1 := m.textTitleBar()

	breadcrumb := m.textBreadcrumb()
	statsStr := m.textStatsString()

	breadcrumbStyled := statsStyle.Render(" " + breadcrumb + " ")
	statsStyled := statsStyle.Render(statsStr + " ")
	gap := m.width -
		lipgloss.Width(breadcrumbStyled) -
		lipgloss.Width(statsStyled)
	if gap < 0 {
		gap = 0
	}
	line2 := breadcrumbStyled +
		strings.Repeat(" ", gap) + statsStyled

	return line1 + "\n" + line2
}

// textTitleBar builds the title bar for Texts mode.
func (m Model) textTitleBar() string {
	titleText := "msgvault"
	if m.version != "" && m.version != "dev" &&
		m.version != "unknown" {
		titleText = fmt.Sprintf("msgvault [%s]", m.version)
	}

	// Mode indicator
	accountStr := "Texts"
	if m.accountFilter != nil {
		for _, acc := range m.accounts {
			if acc.ID == *m.accountFilter {
				accountStr = "Texts - " + acc.Identifier
				break
			}
		}
	}

	content := fmt.Sprintf("%s - %s", titleText, accountStr)
	return titleBarStyle.Render(padRight(content, m.width-2))
}

// textBreadcrumb builds the breadcrumb for the current text view.
func (m Model) textBreadcrumb() string {
	switch m.textState.level {
	case textLevelConversations:
		return "Conversations"
	case textLevelAggregate:
		return m.textState.viewType.String()
	case textLevelDrillConversations:
		return fmt.Sprintf(
			"%s: %s",
			textViewTypePrefix(m.textState.viewType),
			textDrillKey(m),
		)
	case textLevelTimeline:
		return fmt.Sprintf(
			"Timeline (conv %d)", m.textState.selectedConvID,
		)
	}
	return ""
}

// textDrillKey returns the active drill filter key for breadcrumbs.
func textDrillKey(m Model) string {
	f := m.textState.filter
	if f.ContactPhone != "" {
		return truncateRunes(f.ContactPhone, 30)
	}
	if f.ContactName != "" {
		return truncateRunes(f.ContactName, 30)
	}
	if f.SourceType != "" {
		return f.SourceType
	}
	if f.Label != "" {
		return truncateRunes(f.Label, 30)
	}
	return "?"
}

// textViewTypePrefix returns a short prefix for text view breadcrumbs.
func textViewTypePrefix(vt query.TextViewType) string {
	switch vt {
	case query.TextViewContacts:
		return "Contact"
	case query.TextViewContactNames:
		return "Name"
	case query.TextViewSources:
		return "Source"
	case query.TextViewLabels:
		return "Label"
	case query.TextViewTime:
		return "Time"
	default:
		return "?"
	}
}

// textStatsString builds the stats summary for the Texts header.
func (m Model) textStatsString() string {
	if m.textState.stats != nil {
		return fmt.Sprintf(
			"%d msgs | %s",
			m.textState.stats.MessageCount,
			formatBytes(m.textState.stats.TotalSize),
		)
	}
	return ""
}

// textConversationsView renders the conversations list table.
func (m Model) textConversationsView() string {
	if len(m.textState.conversations) == 0 && !m.loading {
		return m.fillScreen(
			normalRowStyle.Render(
				padRight("No conversations", m.width),
			), 1,
		)
	}

	var sb strings.Builder

	// Column widths
	nameWidth := m.width - 48
	if nameWidth < 15 {
		nameWidth = 15
	}
	if nameWidth > 50 {
		nameWidth = 50
	}

	// Header
	headerRow := fmt.Sprintf(
		"   %-*s %10s %10s  %-16s",
		nameWidth, "Conversation",
		"Source", "Messages", "Last Message",
	)
	sb.WriteString(
		tableHeaderStyle.Render(padRight(headerRow, m.width)),
	)
	sb.WriteString("\n")
	sb.WriteString(
		separatorStyle.Render(strings.Repeat("\u2500", m.width)),
	)
	sb.WriteString("\n")

	// Data rows
	endRow := m.textState.scrollOffset + m.pageSize - 1
	if endRow > len(m.textState.conversations) {
		endRow = len(m.textState.conversations)
	}

	for i := m.textState.scrollOffset; i < endRow; i++ {
		conv := m.textState.conversations[i]
		isCursor := i == m.textState.cursor

		indicator := "   "
		if isCursor {
			indicator = cursorRowStyle.Render("\u25b6  ")
		}

		title := textutil.SanitizeTerminal(conv.Title)
		if title == "" {
			title = fmt.Sprintf("(conv %d)", conv.ConversationID)
		}
		title = truncateRunes(title, nameWidth)
		title = fmt.Sprintf("%-*s", nameWidth, title)

		source := truncateRunes(conv.SourceType, 10)
		msgs := formatCount(conv.MessageCount)
		lastMsg := conv.LastMessageAt.Format("2006-01-02 15:04")

		line := fmt.Sprintf(
			"%s %10s %10s  %-16s",
			title, source, msgs, lastMsg,
		)

		var style lipgloss.Style
		if isCursor {
			style = cursorRowStyle
		} else if i%2 == 0 {
			style = normalRowStyle
		} else {
			style = altRowStyle
		}

		sb.WriteString(indicator)
		sb.WriteString(
			style.Render(padRight(line, m.width-3)),
		)
		sb.WriteString("\n")
	}

	// Fill remaining space
	dataRows := endRow - m.textState.scrollOffset
	for i := dataRows; i < m.pageSize-1; i++ {
		sb.WriteString(
			normalRowStyle.Render(strings.Repeat(" ", m.width)),
		)
		sb.WriteString("\n")
	}

	// Info line
	var infoContent string
	if m.inlineSearchActive {
		infoContent = "/" + m.searchInput.View()
	}
	sb.WriteString(m.renderInfoLine(infoContent, m.loading))

	if m.modal != modalNone {
		return m.overlayModal(sb.String())
	}
	return sb.String()
}

// textAggregateView renders the text aggregate table (contacts, sources, etc.).
func (m Model) textAggregateView() string {
	if len(m.textState.aggregateRows) == 0 && !m.loading {
		return m.fillScreen(
			normalRowStyle.Render(
				padRight("No data", m.width),
			), 1,
		)
	}

	var sb strings.Builder

	// Column widths
	keyWidth := m.width - 43
	if keyWidth < 20 {
		keyWidth = 20
	}
	if keyWidth > 57 {
		keyWidth = 57
	}

	// Sort indicators
	sortInd := func(field query.SortField) string {
		if m.textState.filter.SortField == field {
			if m.textState.filter.SortDirection == query.SortDesc {
				return "\u2193"
			}
			return "\u2191"
		}
		return ""
	}

	viewLabel := m.textState.viewType.String()
	if si := sortInd(query.SortByName); si != "" {
		viewLabel += si
	}
	countLabel := "Count"
	if si := sortInd(query.SortByCount); si != "" {
		countLabel += si
	}
	sizeLabel := "Size"
	if si := sortInd(query.SortBySize); si != "" {
		sizeLabel += si
	}
	attachLabel := "Attchs"

	headerRow := fmt.Sprintf(
		"   %-*s %10s %12s %12s",
		keyWidth, viewLabel,
		countLabel, sizeLabel, attachLabel,
	)
	sb.WriteString(
		tableHeaderStyle.Render(padRight(headerRow, m.width)),
	)
	sb.WriteString("\n")
	sb.WriteString(
		separatorStyle.Render(strings.Repeat("\u2500", m.width)),
	)
	sb.WriteString("\n")

	endRow := m.textState.scrollOffset + m.pageSize - 1
	if endRow > len(m.textState.aggregateRows) {
		endRow = len(m.textState.aggregateRows)
	}

	for i := m.textState.scrollOffset; i < endRow; i++ {
		row := m.textState.aggregateRows[i]
		isCursor := i == m.textState.cursor

		indicator := "   "
		if isCursor {
			indicator = cursorRowStyle.Render("\u25b6  ")
		}

		key := truncateRunes(row.Key, keyWidth)
		key = fmt.Sprintf("%-*s", keyWidth, key)

		line := fmt.Sprintf(
			"%s %10s %12s %12s",
			key,
			formatCount(row.Count),
			formatBytes(row.TotalSize),
			formatBytes(row.AttachmentSize),
		)

		var style lipgloss.Style
		if isCursor {
			style = cursorRowStyle
		} else if i%2 == 0 {
			style = normalRowStyle
		} else {
			style = altRowStyle
		}

		sb.WriteString(indicator)
		sb.WriteString(
			style.Render(padRight(line, m.width-3)),
		)
		sb.WriteString("\n")
	}

	// Fill remaining space
	dataRows := endRow - m.textState.scrollOffset
	if len(m.textState.aggregateRows) == 0 && !m.loading {
		dataRows = 1
	}
	for i := dataRows; i < m.pageSize-1; i++ {
		sb.WriteString(
			normalRowStyle.Render(strings.Repeat(" ", m.width)),
		)
		sb.WriteString("\n")
	}

	// Info line
	var infoContent string
	if m.inlineSearchActive {
		infoContent = "/" + m.searchInput.View()
	}
	sb.WriteString(m.renderInfoLine(infoContent, m.loading))

	if m.modal != modalNone {
		return m.overlayModal(sb.String())
	}
	return sb.String()
}

// textTimelineView renders a chronological message timeline.
func (m Model) textTimelineView() string {
	if len(m.textState.messages) == 0 && !m.loading {
		return m.fillScreen(
			normalRowStyle.Render(
				padRight("No messages", m.width),
			), 1,
		)
	}

	var sb strings.Builder

	// Column widths
	dateWidth := 16
	fromWidth := 20
	bodyWidth := m.width - dateWidth - fromWidth - 9
	if bodyWidth < 10 {
		bodyWidth = 10
	}

	// Header
	headerRow := fmt.Sprintf(
		"   %-*s  %-*s  %-*s",
		dateWidth, "Time",
		fromWidth, "Sender",
		bodyWidth, "Message",
	)
	sb.WriteString(
		tableHeaderStyle.Render(padRight(headerRow, m.width)),
	)
	sb.WriteString("\n")
	sb.WriteString(
		separatorStyle.Render(strings.Repeat("\u2500", m.width)),
	)
	sb.WriteString("\n")

	endRow := m.textState.scrollOffset + m.pageSize - 1
	if endRow > len(m.textState.messages) {
		endRow = len(m.textState.messages)
	}

	for i := m.textState.scrollOffset; i < endRow; i++ {
		msg := m.textState.messages[i]
		isCursor := i == m.textState.cursor

		indicator := "   "
		if isCursor {
			indicator = cursorRowStyle.Render("\u25b6  ")
		}

		dateStr := msg.SentAt.Format("2006-01-02 15:04")

		// Sender: prefer name, then phone, then email
		from := textutil.SanitizeTerminal(msg.FromName)
		if from == "" && msg.FromPhone != "" {
			from = textutil.SanitizeTerminal(msg.FromPhone)
		}
		if from == "" {
			from = textutil.SanitizeTerminal(msg.FromEmail)
		}
		from = truncateRunes(from, fromWidth)
		from = fmt.Sprintf("%-*s", fromWidth, from)

		// Message body: use snippet
		body := textutil.SanitizeTerminal(msg.Snippet)
		if body == "" {
			body = textutil.SanitizeTerminal(msg.Subject)
		}
		body = truncateRunes(body, bodyWidth)
		body = fmt.Sprintf("%-*s", bodyWidth, body)

		line := fmt.Sprintf(
			"%-*s  %s  %s",
			dateWidth, dateStr, from, body,
		)

		var style lipgloss.Style
		if isCursor {
			style = cursorRowStyle
		} else if i%2 == 0 {
			style = normalRowStyle
		} else {
			style = altRowStyle
		}

		sb.WriteString(indicator)
		sb.WriteString(
			style.Render(padRight(line, m.width-3)),
		)
		sb.WriteString("\n")
	}

	// Fill remaining space
	dataRows := endRow - m.textState.scrollOffset
	for i := dataRows; i < m.pageSize-1; i++ {
		sb.WriteString(
			normalRowStyle.Render(strings.Repeat(" ", m.width)),
		)
		sb.WriteString("\n")
	}

	// Info line
	sb.WriteString(m.renderNotificationLine())

	if m.modal != modalNone {
		return m.overlayModal(sb.String())
	}
	return sb.String()
}

// textFooterView renders the footer with keybindings for Texts mode.
func (m Model) textFooterView() string {
	var keys []string
	var posStr string

	switch m.textState.level {
	case textLevelConversations, textLevelDrillConversations:
		keys = []string{
			"\u2191/k", "\u2193/j", "Enter",
			"Tab group", "s sort", "A acct",
			"m email", "? help",
		}
		if m.textState.level == textLevelDrillConversations {
			keys = append([]string{"\u2191/k", "\u2193/j", "Enter",
				"Esc back", "Tab group", "s sort",
				"m email", "? help"}, []string{}...)
		}
		n := len(m.textState.conversations)
		if n > 0 {
			posStr = fmt.Sprintf(
				" %d/%d ", m.textState.cursor+1, n,
			)
		}

	case textLevelAggregate:
		keys = []string{
			"\u2191/k", "\u2193/j", "Enter",
			"Esc back", "Tab group", "s sort",
			"m email", "? help",
		}
		n := len(m.textState.aggregateRows)
		if n > 0 {
			posStr = fmt.Sprintf(
				" %d/%d ", m.textState.cursor+1, n,
			)
		}

	case textLevelTimeline:
		keys = []string{
			"\u2191/\u2193 navigate", "Esc back",
			"m email", "? help",
		}
		n := len(m.textState.messages)
		if n > 0 {
			posStr = fmt.Sprintf(
				" %d/%d ", m.textState.cursor+1, n,
			)
		}
	}

	keysStr := strings.Join(keys, " \u2502 ")
	gap := m.width -
		lipgloss.Width(keysStr) -
		lipgloss.Width(posStr) - 2
	if gap < 0 {
		gap = 0
	}

	return footerStyle.Render(
		keysStr + strings.Repeat(" ", gap) + posStr,
	)
}
