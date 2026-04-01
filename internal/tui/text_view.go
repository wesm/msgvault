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

// measureMaxWidth returns the widest string length in values,
// using headerWidth as the minimum.
func measureMaxWidth(values []string, headerWidth int) int {
	w := headerWidth
	for _, v := range values {
		if len(v) > w {
			w = len(v)
		}
	}
	return w
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

	// Visible row range
	endRow := m.textState.scrollOffset + m.pageSize - 1
	if endRow > len(m.textState.conversations) {
		endRow = len(m.textState.conversations)
	}

	// Measure source column from visible data
	sourceVals := make(
		[]string, 0, endRow-m.textState.scrollOffset,
	)
	for i := m.textState.scrollOffset; i < endRow; i++ {
		sourceVals = append(
			sourceVals,
			m.textState.conversations[i].SourceType,
		)
	}
	sourceWidth := measureMaxWidth(sourceVals, len("Source"))
	if sourceWidth > 16 {
		sourceWidth = 16
	}

	// Fixed column widths
	const (
		indicatorWidth = 3
		msgsWidth      = 10
		lastMsgWidth   = 16
		colSpacing     = 6 // gaps between columns
	)
	fixedTotal := indicatorWidth + sourceWidth +
		msgsWidth + lastMsgWidth + colSpacing
	nameWidth := m.width - fixedTotal
	if nameWidth < 15 {
		nameWidth = 15
	}

	// Header
	headerRow := fmt.Sprintf(
		"   %-*s  %-*s %*s  %-*s",
		nameWidth, "Conversation",
		sourceWidth, "Source",
		msgsWidth, "Messages",
		lastMsgWidth, "Last Message",
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

		source := truncateRunes(conv.SourceType, sourceWidth)
		source = fmt.Sprintf("%-*s", sourceWidth, source)
		msgs := formatCount(conv.MessageCount)
		lastMsg := conv.LastMessageAt.Format(
			"2006-01-02 15:04",
		)

		line := fmt.Sprintf(
			"%s  %s %*s  %-*s",
			title, source,
			msgsWidth, msgs,
			lastMsgWidth, lastMsg,
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

// textAggregateView renders the text aggregate table
// (contacts, sources, etc.).
func (m Model) textAggregateView() string {
	if len(m.textState.aggregateRows) == 0 && !m.loading {
		return m.fillScreen(
			normalRowStyle.Render(
				padRight("No data", m.width),
			), 1,
		)
	}

	var sb strings.Builder

	// Visible row range
	endRow := m.textState.scrollOffset + m.pageSize - 1
	if endRow > len(m.textState.aggregateRows) {
		endRow = len(m.textState.aggregateRows)
	}

	// Fixed column widths
	const (
		indicatorWidth = 3
		countWidth     = 10
		sizeWidth      = 12
		attachWidth    = 12
		colSpacing     = 6 // gaps between columns
	)
	fixedTotal := indicatorWidth + countWidth +
		sizeWidth + attachWidth + colSpacing
	keyWidth := m.width - fixedTotal
	if keyWidth < 20 {
		keyWidth = 20
	}

	// Sort indicators
	sortInd := func(field query.TextSortField) string {
		if m.textState.filter.SortField == field {
			if m.textState.filter.SortDirection == query.SortDesc {
				return "\u2193"
			}
			return "\u2191"
		}
		return ""
	}

	viewLabel := m.textState.viewType.String()
	if si := sortInd(query.TextSortByName); si != "" {
		viewLabel += si
	}
	countLabel := "Count"
	if si := sortInd(query.TextSortByCount); si != "" {
		countLabel += si
	}
	sizeLabel := "Size"
	attachLabel := "Attchs"

	headerRow := fmt.Sprintf(
		"   %-*s %*s %*s %*s",
		keyWidth, viewLabel,
		countWidth, countLabel,
		sizeWidth, sizeLabel,
		attachWidth, attachLabel,
	)
	sb.WriteString(
		tableHeaderStyle.Render(padRight(headerRow, m.width)),
	)
	sb.WriteString("\n")
	sb.WriteString(
		separatorStyle.Render(strings.Repeat("\u2500", m.width)),
	)
	sb.WriteString("\n")

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
			"%s %*s %*s %*s",
			key,
			countWidth, formatCount(row.Count),
			sizeWidth, formatBytes(row.TotalSize),
			attachWidth, formatBytes(row.AttachmentSize),
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

	// Visible row range
	endRow := m.textState.scrollOffset + m.pageSize - 1
	if endRow > len(m.textState.messages) {
		endRow = len(m.textState.messages)
	}

	// Measure sender column from visible data
	senderVals := make(
		[]string, 0, endRow-m.textState.scrollOffset,
	)
	for i := m.textState.scrollOffset; i < endRow; i++ {
		msg := m.textState.messages[i]
		from := msg.FromName
		if from == "" && msg.FromPhone != "" {
			from = msg.FromPhone
		}
		if from == "" {
			from = msg.FromEmail
		}
		senderVals = append(senderVals, from)
	}
	fromWidth := measureMaxWidth(senderVals, len("Sender"))
	if fromWidth > 25 {
		fromWidth = 25
	}
	if fromWidth < 10 {
		fromWidth = 10
	}

	// Fixed column widths
	const (
		indicatorWidth = 3
		dateWidth      = 16
		colSpacing     = 7 // gaps between columns
	)
	fixedTotal := indicatorWidth + dateWidth +
		fromWidth + colSpacing
	bodyWidth := m.width - fixedTotal
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
