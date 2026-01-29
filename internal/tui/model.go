// Package tui provides a terminal user interface for msgvault.
package tui

import (
	"archive/zip"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/wesm/msgvault/internal/deletion"
	"github.com/wesm/msgvault/internal/query"
	"github.com/wesm/msgvault/internal/search"
)

// aggregateLimit is the maximum number of aggregate rows to load for display.
// The true total count is obtained via TotalUnique (COUNT(*) OVER()), so the
// footer can show "N of M" even when M exceeds this limit. This limit only
// affects how many rows are available for scrolling in the UI.
const aggregateLimit = 50000

// viewLevel represents the current navigation depth.
type viewLevel int

const (
	levelAggregates   viewLevel = iota
	levelSubAggregate           // Sub-grouping after drill-down
	levelMessageList
	levelMessageDetail
	levelThreadView // Thread/conversation view showing all messages in a thread
)

// Options configuration for TUI.
type Options struct {
	DataDir string
	Version string
}

// viewState encapsulates the state for a specific view (cursor, sort, filters, data).
type viewState struct {
	level           viewLevel
	viewType        query.ViewType
	timeGranularity query.TimeGranularity
	sortField       query.SortField
	sortDirection   query.SortDirection
	msgSortField     query.MessageSortField
	msgSortDirection query.SortDirection
	cursor          int
	scrollOffset    int
	filterKey       string              // The aggregate key used to filter messages
	allMessages     bool                // True when showing all messages (not filtered by aggregate)
	drillFilter     query.MessageFilter // Filter from parent drill-down
	drillViewType   query.ViewType      // ViewType that created the drill filter
	contextStats    *query.TotalStats   // Contextual stats for header display
	searchQuery     string              // Active search query (for aggregate filtering)
	searchFilter    query.MessageFilter // Context filter applied to search
	
	// Data
	rows         []query.AggregateRow
	messages      []query.MessageSummary
	messageDetail *query.MessageDetail
	
	// Detail view specific
	detailScroll         int
	detailLineCount      int
	detailMessageIndex   int
	detailFromThread     bool
	pendingDetailSubject string
	
	// Thread view specific
	threadConversationID int64
	threadMessages       []query.MessageSummary
	threadCursor         int
	threadScrollOffset   int
	threadTruncated      bool
}

// navigationSnapshot stores state for navigation history.
type navigationSnapshot struct {
	state viewState
}

// modalType represents the type of modal dialog.
type modalType int

const (
	modalNone modalType = iota
	modalDeleteConfirm
	modalDeleteResult
	modalAccountSelector
	modalAttachmentFilter
	modalExportAttachments
	modalExportResult
	modalQuitConfirm
	modalHelp
)

// SearchMode represents the search mode (fast metadata vs deep body search).
type SearchMode int

const (
	SearchModeFast SearchMode = iota // Parquet metadata only (subject, sender, recipient)
	SearchModeDeep                   // SQLite FTS5 (includes body text)
)

// String returns a human-readable name for the search mode.
func (m SearchMode) String() string {
	switch m {
	case SearchModeFast:
		return "Fast"
	case SearchModeDeep:
		return "Deep"
	default:
		return "Fast"
	}
}

// selectionState tracks selected items for batch operations.
type selectionState struct {
	// Selected aggregate keys (sender emails, domains, labels, etc.)
	// Keys are scoped to the current ViewType to prevent collisions.
	AggregateKeys map[string]bool

	// ViewType that the AggregateKeys belong to
	AggregateViewType query.ViewType

	// Selected message IDs
	MessageIDs map[int64]bool
}

// Model is the main TUI model following the Elm architecture.
type Model struct {
	viewState // Embedded state

	// Query engine for data access
	engine query.Engine

	// Version info for title bar
	version string

	// Navigation
	breadcrumbs []navigationSnapshot

	// Global Stats (not view specific)
	stats        *query.TotalStats // Global stats
	accounts     []query.AccountInfo

	// Account filter (nil = all accounts)
	accountFilter *int64

	// Attachment filter (true = show only messages with attachments)
	attachmentFilter bool

	// Pagination config
	pageSize     int // Rows visible per page

	// Selection state
	selection selectionState

	// Modal state
	modal           modalType
	modalCursor     int                // Cursor position within modal (for selector modals)
	modalResult     string             // Result message to display
	pendingManifest *deletion.Manifest // Manifest being confirmed

	// Deletion manager
	deletionMgr *deletion.Manager
	dataDir     string // Base data directory for deletion manifests

	// Terminal dimensions
	width  int
	height int

	// Loading state
	loading      bool
	err          error
	spinnerFrame int  // Current frame index into spinnerFrames
	spinnerActive bool // True when spinner tick is running

	// Request tracking to ignore stale async results
	aggregateRequestID   uint64 // Current request ID for aggregate data
	loadRequestID        uint64 // Current request ID for message list
	detailRequestID      uint64 // Current request ID for message detail
	searchRequestID      uint64 // Current request ID for search results
	
	// Search state
	searchMode        SearchMode          // Fast (Parquet) or Deep (FTS5)
	searchInput       textinput.Model     // Text input for search query
	searchTotalCount  int64               // Total matching messages (for pagination display)
	searchOffset      int                 // Current offset for pagination
	searchLoadingMore bool                // True when loading additional results

	// Navigation restoration state
	restorePosition bool // When true, don't reset cursor/scroll on data load (used by goBack)

	// Inline search state (vim-like search bar on info line)
	inlineSearchActive   bool   // True when inline search bar is active
	inlineSearchDebounce uint64 // Increment to cancel pending debounce timers
	inlineSearchLoading  bool   // True when a debounced search query is in-flight

	// Frozen view: holds the last rendered view during level transitions.
	// When non-empty, View() returns this string instead of rendering fresh content.
	// This prevents visual flashing during async data loads on screen transitions.
	frozenView string

	// Flash message (temporary notification)
	flashMessage   string    // Message to display
	flashExpiresAt time.Time // When the flash message expires

	// Export attachments state
	exportSelection map[int]bool // Selected attachment indices for export
	exportCursor    int          // Cursor position in export modal

	// Quit flag
	quitting bool
}

// New creates a new TUI model with the given options.
func New(engine query.Engine, opts Options) Model {
	ti := textinput.New()
	ti.Placeholder = "search (Tab: deep)"
	ti.CharLimit = 200
	ti.Width = 50

	return Model{
		engine:           engine,
		dataDir:          opts.DataDir,
		version:          opts.Version,
		viewState: viewState{
			level:            levelAggregates,
			viewType:         query.ViewSenders,
			timeGranularity:  query.TimeMonth,
			sortField:        query.SortByCount,
			sortDirection:    query.SortDesc,
			msgSortField:     query.MessageSortByDate,
			msgSortDirection: query.SortDesc,
		},
		pageSize:         20,
		loading:       true,
		spinnerActive: true,
		selection: selectionState{
			AggregateKeys:     make(map[string]bool),
			AggregateViewType: query.ViewSenders, // Match initial viewType
			MessageIDs:        make(map[int64]bool),
		},
		searchInput: ti,
		searchMode:  SearchModeFast,
	}
}

// Init implements tea.Model.
func (m Model) Init() tea.Cmd {
	return tea.Batch(
		m.loadData(),
		m.loadStats(),
		m.loadAccounts(),
		spinnerTick(), // Start spinner for initial load
	)
}

// dataLoadedMsg is sent when aggregate data is loaded.
type dataLoadedMsg struct {
	rows      []query.AggregateRow
	err       error
	requestID uint64 // To detect stale responses
}

// statsLoadedMsg is sent when stats are loaded.
type statsLoadedMsg struct {
	stats *query.TotalStats
	err   error
}

// accountsLoadedMsg is sent when accounts are loaded.
type accountsLoadedMsg struct {
	accounts []query.AccountInfo
	err      error
}

// loadData fetches aggregate data based on current view settings.
func (m Model) loadData() tea.Cmd {
	requestID := m.aggregateRequestID
	return func() (msg tea.Msg) {
		// Recover from panics to prevent TUI from becoming unresponsive
		defer func() {
			if r := recover(); r != nil {
				msg = dataLoadedMsg{err: fmt.Errorf("query panic: %v", r), requestID: requestID}
			}
		}()

		opts := query.AggregateOptions{
			SourceID:            m.accountFilter,
			SortField:           m.sortField,
			SortDirection:       m.sortDirection,
			Limit:               aggregateLimit,
			TimeGranularity:     m.timeGranularity,
			WithAttachmentsOnly: m.attachmentFilter,
			SearchQuery:         m.searchQuery,
		}

		ctx := context.Background()
		var rows []query.AggregateRow
		var err error

		// Use SubAggregate for sub-grouping, regular aggregate for top-level
		if m.level == levelSubAggregate {
			rows, err = m.engine.SubAggregate(ctx, m.drillFilter, m.viewType, opts)
		} else {
			switch m.viewType {
			case query.ViewSenders:
				rows, err = m.engine.AggregateBySender(ctx, opts)
			case query.ViewRecipients:
				rows, err = m.engine.AggregateByRecipient(ctx, opts)
			case query.ViewDomains:
				rows, err = m.engine.AggregateByDomain(ctx, opts)
			case query.ViewLabels:
				rows, err = m.engine.AggregateByLabel(ctx, opts)
			case query.ViewTime:
				rows, err = m.engine.AggregateByTime(ctx, opts)
			}
		}

		return dataLoadedMsg{rows: rows, err: err, requestID: requestID}
	}
}

// loadStats fetches total statistics.
func (m Model) loadStats() tea.Cmd {
	return func() (msg tea.Msg) {
		defer func() {
			if r := recover(); r != nil {
				msg = statsLoadedMsg{err: fmt.Errorf("stats panic: %v", r)}
			}
		}()

		opts := query.StatsOptions{
			SourceID:            m.accountFilter,
			WithAttachmentsOnly: m.attachmentFilter,
		}
		stats, err := m.engine.GetTotalStats(context.Background(), opts)
		return statsLoadedMsg{stats: stats, err: err}
	}
}

// loadAccounts fetches the list of accounts.
func (m Model) loadAccounts() tea.Cmd {
	return func() (msg tea.Msg) {
		defer func() {
			if r := recover(); r != nil {
				msg = accountsLoadedMsg{err: fmt.Errorf("accounts panic: %v", r)}
			}
		}()

		accounts, err := m.engine.ListAccounts(context.Background())
		return accountsLoadedMsg{accounts: accounts, err: err}
	}
}

// messagesLoadedMsg is sent when message list is loaded.
type messagesLoadedMsg struct {
	messages  []query.MessageSummary
	err       error
	requestID uint64 // To detect stale responses
}

// messageDetailLoadedMsg is sent when message detail is loaded.
type messageDetailLoadedMsg struct {
	detail    *query.MessageDetail
	err       error
	requestID uint64 // To detect stale responses
}

// searchResultsMsg is sent when search results are loaded.
type searchResultsMsg struct {
	messages   []query.MessageSummary
	totalCount int64 // Total matching messages (for "N of M" display)
	err        error
	requestID  uint64 // To detect stale responses
	append     bool   // True if these results should be appended (pagination)
}

// threadMessagesLoadedMsg is sent when thread messages are loaded.
type threadMessagesLoadedMsg struct {
	messages       []query.MessageSummary
	conversationID int64
	truncated      bool // True if more messages exist but were limited
	err            error
	requestID      uint64
}

// flashClearMsg clears the flash message after timeout.
type flashClearMsg struct{}

// spinnerTickMsg advances the loading spinner animation.
type spinnerTickMsg struct{}

// searchDebounceMsg fires after debounce delay to trigger inline search.
type searchDebounceMsg struct {
	query      string
	debounceID uint64
}

// exportResultMsg is returned when attachment export completes.
type exportResultMsg struct {
	result string
	err    error
}

// inlineSearchDebounceDelay is the delay before executing inline search (fast mode).
const inlineSearchDebounceDelay = 100 * time.Millisecond

// deepSearchDebounceDelay is the delay before executing inline search (deep FTS mode).
const deepSearchDebounceDelay = 500 * time.Millisecond

// spinnerFrames are the Braille dot animation frames for the loading spinner.
var spinnerFrames = []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"}

// spinnerInterval is how fast the spinner animates.
const spinnerInterval = 80 * time.Millisecond

// flashDuration is how long flash messages are displayed.
const flashDuration = 4 * time.Second

// searchPageSize is the number of results per page for search pagination.
const searchPageSize = 100

// loadSearch executes the search query based on current mode.
func (m Model) loadSearch(queryStr string) tea.Cmd {
	return m.loadSearchWithOffset(queryStr, 0, false)
}

// loadSearchWithOffset executes the search query with pagination.
func (m Model) loadSearchWithOffset(queryStr string, offset int, appendResults bool) tea.Cmd {
	requestID := m.searchRequestID
	return func() (msg tea.Msg) {
		defer func() {
			if r := recover(); r != nil {
				msg = searchResultsMsg{
					err:       fmt.Errorf("search panic: %v", r),
					requestID: requestID,
				}
			}
		}()

		ctx := context.Background()
		q := search.Parse(queryStr)

		var results []query.MessageSummary
		var totalCount int64
		var err error

		if m.searchMode == SearchModeFast {
			// Fast search: Parquet metadata only
			results, err = m.engine.SearchFast(ctx, q, m.searchFilter, searchPageSize, offset)
			if err == nil {
				totalCount, _ = m.engine.SearchFastCount(ctx, q, m.searchFilter)
			}
		} else {
			// Deep search: FTS5 body search
			// Merge context filter into query to honor drill-down context
			mergedQuery := query.MergeFilterIntoQuery(q, m.searchFilter)
			results, err = m.engine.Search(ctx, mergedQuery, searchPageSize, offset)
			// For deep search, estimate total from result count (no separate count query)
			if err == nil && offset == 0 {
				totalCount = int64(len(results))
				if len(results) == searchPageSize {
					totalCount = -1 // Indicate more results available
				}
			}
		}

		return searchResultsMsg{
			messages:   results,
			totalCount: totalCount,
			err:        err,
			requestID:  requestID,
			append:     appendResults,
		}
	}
}

// loadMessages fetches messages based on current filter.
func (m Model) loadMessages() tea.Cmd {
	requestID := m.loadRequestID
	return func() (msg tea.Msg) {
		defer func() {
			if r := recover(); r != nil {
				msg = messagesLoadedMsg{err: fmt.Errorf("messages panic: %v", r), requestID: requestID}
			}
		}()

		// Start with drillFilter if set, otherwise build fresh filter
		var filter query.MessageFilter
		if m.hasDrillFilter() {
			filter = m.drillFilter
		}

		// Override sorting and pagination
		filter.SourceID = m.accountFilter
		filter.SortField = m.msgSortField
		filter.SortDirection = m.msgSortDirection
		filter.Limit = 500
		filter.WithAttachmentsOnly = m.attachmentFilter

		// If not showing all messages and no drill filter, apply simple filter
		if !m.allMessages && !m.hasDrillFilter() {
			switch m.viewType {
			case query.ViewSenders:
				filter.Sender = m.filterKey
				filter.MatchEmptySender = (m.filterKey == "")
			case query.ViewRecipients:
				filter.Recipient = m.filterKey
				filter.MatchEmptyRecipient = (m.filterKey == "")
			case query.ViewDomains:
				filter.Domain = m.filterKey
				filter.MatchEmptyDomain = (m.filterKey == "")
			case query.ViewLabels:
				filter.Label = m.filterKey
				filter.MatchEmptyLabel = (m.filterKey == "")
			case query.ViewTime:
				filter.TimePeriod = m.filterKey
				filter.TimeGranularity = m.timeGranularity
			}
		}

		messages, err := m.engine.ListMessages(context.Background(), filter)
		return messagesLoadedMsg{messages: messages, err: err, requestID: requestID}
	}
}

// hasDrillFilter returns true if drillFilter has any filter criteria set.
func (m Model) hasDrillFilter() bool {
	return m.drillFilter.Sender != "" ||
		m.drillFilter.Recipient != "" ||
		m.drillFilter.Domain != "" ||
		m.drillFilter.Label != "" ||
		m.drillFilter.TimePeriod != "" ||
		m.drillFilter.MatchEmptySender ||
		m.drillFilter.MatchEmptyRecipient ||
		m.drillFilter.MatchEmptyDomain ||
		m.drillFilter.MatchEmptyLabel
}

// drillFilterKey returns the key value from the drillFilter based on drillViewType.
func (m Model) drillFilterKey() string {
	switch m.drillViewType {
	case query.ViewSenders:
		if m.drillFilter.MatchEmptySender {
			return "(empty)"
		}
		return m.drillFilter.Sender
	case query.ViewRecipients:
		if m.drillFilter.MatchEmptyRecipient {
			return "(empty)"
		}
		return m.drillFilter.Recipient
	case query.ViewDomains:
		if m.drillFilter.MatchEmptyDomain {
			return "(empty)"
		}
		return m.drillFilter.Domain
	case query.ViewLabels:
		if m.drillFilter.MatchEmptyLabel {
			return "(empty)"
		}
		return m.drillFilter.Label
	case query.ViewTime:
		return m.drillFilter.TimePeriod
	}
	return ""
}

// loadThreadMessages fetches all messages in a conversation/thread.
// threadMessageLimit is the maximum number of messages to load in a thread view.
const threadMessageLimit = 1000

func (m Model) loadThreadMessages(conversationID int64) tea.Cmd {
	requestID := m.loadRequestID
	return func() (msg tea.Msg) {
		defer func() {
			if r := recover(); r != nil {
				msg = threadMessagesLoadedMsg{
					err:       fmt.Errorf("thread messages panic: %v", r),
					requestID: requestID,
				}
			}
		}()

		filter := query.MessageFilter{
			ConversationID: &conversationID,
			SortField:      query.MessageSortByDate,
			SortDirection:  query.SortAsc,          // Chronological order for threads
			Limit:          threadMessageLimit + 1, // Request one extra to detect truncation
		}
		messages, err := m.engine.ListMessages(context.Background(), filter)

		// Check if truncated (more messages than limit)
		truncated := false
		if len(messages) > threadMessageLimit {
			messages = messages[:threadMessageLimit]
			truncated = true
		}

		return threadMessagesLoadedMsg{
			messages:       messages,
			conversationID: conversationID,
			truncated:      truncated,
			err:            err,
			requestID:      requestID,
		}
	}
}

// loadMessageDetail fetches a single message's full details.
func (m Model) loadMessageDetail(id int64) tea.Cmd {
	requestID := m.detailRequestID
	return func() (msg tea.Msg) {
		defer func() {
			if r := recover(); r != nil {
				msg = messageDetailLoadedMsg{err: fmt.Errorf("message detail panic: %v", r), requestID: requestID}
			}
		}()

		detail, err := m.engine.GetMessage(context.Background(), id)
		return messageDetailLoadedMsg{detail: detail, err: err, requestID: requestID}
	}
}

// spinnerTick returns a command that fires a spinnerTickMsg after the spinner interval.
func spinnerTick() tea.Cmd {
	return tea.Tick(spinnerInterval, func(t time.Time) tea.Msg {
		return spinnerTickMsg{}
	})
}

// startSpinner returns a spinnerTick command if the spinner isn't already active,
// and marks it as active. Call this when loading begins.
func (m *Model) startSpinner() tea.Cmd {
	if m.spinnerActive {
		return nil
	}
	m.spinnerActive = true
	m.spinnerFrame = 0
	return spinnerTick()
}

// Update implements tea.Model.
func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		return m.handleKeyPress(msg)

	case tea.WindowSizeMsg:
		m.frozenView = "" // Clear frozen view on resize to re-render with new dimensions
		m.width = msg.Width
		m.height = msg.Height
		// Clamp dimensions to prevent panics from strings.Repeat with negative count
		if m.width < 0 {
			m.width = 0
		}
		if m.height < 0 {
			m.height = 0
		}
		// Reserve space for: title bar (1) + breadcrumb (1) + table header (1) + separator (1) + footer (1) = 5
		m.pageSize = m.height - 5
		if m.pageSize < 1 {
			m.pageSize = 1
		}
		// Recalculate detail line count if in detail view (width affects wrapping)
		if m.level == levelMessageDetail && m.messageDetail != nil {
			m.updateDetailLineCount()
			m.clampDetailScroll()
		}
		return m, nil

	case dataLoadedMsg:
		// Ignore stale responses from previous loads
		if msg.requestID != m.aggregateRequestID {
			return m, nil
		}
		m.frozenView = "" // Unfreeze view now that data is ready
		m.loading = false
		m.inlineSearchLoading = false
		if msg.err != nil {
			m.err = msg.err
			m.restorePosition = false // Clear flag on error to prevent stale state
		} else {
			m.err = nil // Clear any previous error
			m.rows = msg.rows
			// Only reset position on fresh loads, not when restoring from breadcrumb
			if !m.restorePosition {
				m.cursor = 0
				m.scrollOffset = 0
			}
			m.restorePosition = false // Clear flag after use

			// When search filter is active, calculate stats from filtered aggregate rows
			// so the header metrics reflect the filtered data
			if m.searchQuery != "" {
				var totalCount, totalSize, totalAttachments int64
				for _, row := range msg.rows {
					totalCount += row.Count
					totalSize += row.TotalSize
					totalAttachments += row.AttachmentCount
				}
				m.contextStats = &query.TotalStats{
					MessageCount:    totalCount,
					TotalSize:       totalSize,
					AttachmentCount: totalAttachments,
				}
			} else if m.level == levelAggregates {
				// Clear contextStats when no search filter at top level
				m.contextStats = nil
			}
		}
		return m, nil

	case statsLoadedMsg:
		if msg.err == nil {
			m.stats = msg.stats
		}
		return m, nil

	case accountsLoadedMsg:
		if msg.err == nil {
			m.accounts = msg.accounts
		}
		return m, nil

	case messagesLoadedMsg:
		// Ignore stale responses from previous loads
		if msg.requestID != m.loadRequestID {
			return m, nil
		}
		m.frozenView = "" // Unfreeze view now that data is ready
		m.loading = false
		m.inlineSearchLoading = false
		if msg.err != nil {
			m.err = msg.err
			m.restorePosition = false // Clear flag on error to prevent stale state
		} else {
			m.err = nil // Clear any previous error
			m.messages = msg.messages
			// Only reset position on fresh loads, not when restoring from breadcrumb
			if !m.restorePosition {
				m.cursor = 0
				m.scrollOffset = 0
			}
			m.restorePosition = false // Clear flag after use
		}
		return m, nil

	case messageDetailLoadedMsg:
		// Ignore stale responses from previous loads
		if msg.requestID != m.detailRequestID {
			return m, nil
		}
		m.frozenView = "" // Unfreeze view now that data is ready
		m.loading = false
		if msg.err != nil {
			m.err = msg.err
		} else {
			m.err = nil // Clear any previous error
			m.messageDetail = msg.detail
			m.detailScroll = 0
			m.pendingDetailSubject = "" // Clear pending subject
			m.updateDetailLineCount()   // Calculate line count for scroll bounds
		}
		return m, nil

	case threadMessagesLoadedMsg:
		// Ignore stale responses from previous loads
		if msg.requestID != m.loadRequestID {
			return m, nil
		}
		m.frozenView = "" // Unfreeze view now that data is ready
		m.loading = false
		if msg.err != nil {
			m.err = msg.err
		} else {
			m.err = nil
			m.threadMessages = msg.messages
			m.threadConversationID = msg.conversationID
			m.threadTruncated = msg.truncated
			// Reset cursor/scroll for thread view
			m.threadCursor = 0
			m.threadScrollOffset = 0
		}
		return m, nil

	case searchResultsMsg:
		// Ignore stale responses from previous searches
		if msg.requestID != m.searchRequestID {
			return m, nil
		}
		m.frozenView = "" // Unfreeze view now that data is ready
		m.loading = false
		m.inlineSearchLoading = false
		m.searchLoadingMore = false
		if msg.err != nil {
			m.err = msg.err
		} else {
			m.err = nil // Clear any previous error
			if msg.append {
				// Pagination: append new results to existing
				m.messages = append(m.messages, msg.messages...)
				m.searchOffset += len(msg.messages)
				// Update contextStats when total is unknown so header reflects loaded count
				if m.searchTotalCount == -1 && m.contextStats != nil {
					m.contextStats.MessageCount = int64(len(m.messages))
				}
			} else {
				// New search: replace results
				m.messages = msg.messages
				m.searchOffset = len(msg.messages)
				m.searchTotalCount = msg.totalCount
				m.cursor = 0
				m.scrollOffset = 0
					// Set contextStats for search results to update header metrics
				// Preserve TotalSize/AttachmentCount if already set from drill-down
				// (drill-down sets these from the aggregate row before loading search results)
				hasDrillDownStats := m.contextStats != nil &&
					(m.contextStats.TotalSize > 0 || m.contextStats.AttachmentCount > 0)
				if msg.totalCount > 0 {
					if hasDrillDownStats {
						// Preserve drill-down stats, only update MessageCount
						m.contextStats.MessageCount = msg.totalCount
					} else {
						m.contextStats = &query.TotalStats{
							MessageCount: msg.totalCount,
						}
					}
				} else if msg.totalCount == -1 {
					// Unknown total, use loaded count
					if hasDrillDownStats {
						m.contextStats.MessageCount = int64(len(msg.messages))
					} else {
						m.contextStats = &query.TotalStats{
							MessageCount: int64(len(msg.messages)),
						}
					}
				} else {
					// Zero results: clear stale contextStats from previous view
					m.contextStats = &query.TotalStats{
						MessageCount: 0,
					}
				}
				// Transition to message list view showing search results
				m.level = levelMessageList
			}
		}
		return m, nil

	case flashClearMsg:
		// Clear flash message if it hasn't been updated since the timer started
		if time.Now().After(m.flashExpiresAt) || m.flashExpiresAt.IsZero() {
			m.flashMessage = ""
		}
		return m, nil

	case exportResultMsg:
		// Export completed - show result modal
		m.loading = false
		m.modal = modalExportResult
		if msg.err != nil {
			m.modalResult = fmt.Sprintf("Export failed: %v", msg.err)
		} else {
			m.modalResult = msg.result
		}
		return m, nil

	case searchDebounceMsg:
		// Ignore stale debounce timers (user typed more since timer started)
		if msg.debounceID != m.inlineSearchDebounce {
			return m, nil
		}
		// Execute inline search for live updates
		if m.inlineSearchActive {
			m.searchQuery = msg.query
			if m.searchQuery == "" {
				m.contextStats = nil
			}
			m.inlineSearchLoading = true
			spinCmd := m.startSpinner()

			if m.level == levelMessageList {
				// Message list: use search engine for live results
				m.searchFilter = m.drillFilter
				m.searchFilter.SourceID = m.accountFilter
				m.searchFilter.WithAttachmentsOnly = m.attachmentFilter
				m.searchRequestID++
				if msg.query == "" {
					// Empty query: reload unfiltered messages
					m.loadRequestID++
					return m, tea.Batch(spinCmd, m.loadMessages())
				}
				return m, tea.Batch(spinCmd, m.loadSearch(msg.query))
			}
			// Aggregate views: reload aggregates with search filter
			m.aggregateRequestID++
			return m, tea.Batch(spinCmd, m.loadData())
		}
		return m, nil

	case spinnerTickMsg:
		// Only advance if still loading (any loading state)
		if m.loading || m.inlineSearchLoading || m.searchLoadingMore {
			m.spinnerFrame = (m.spinnerFrame + 1) % len(spinnerFrames)
			return m, spinnerTick()
		}
		m.spinnerActive = false
		return m, nil
	}

	return m, nil
}

// handleKeyPress processes keyboard input.
func (m Model) handleKeyPress(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	// Handle based on current view level
	switch m.level {
	case levelAggregates:
		return m.handleAggregateKeys(msg)
	case levelSubAggregate:
		return m.handleSubAggregateKeys(msg)
	case levelMessageList:
		return m.handleMessageListKeys(msg)
	case levelMessageDetail:
		return m.handleMessageDetailKeys(msg)
	case levelThreadView:
		return m.handleThreadViewKeys(msg)
	}
	return m, nil
}

// handleInlineSearchKeys handles keys when inline search bar is active.
func (m Model) handleInlineSearchKeys(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "enter":
		// Finalize search, exit inline mode, keep results
		m.inlineSearchActive = false
		m.inlineSearchLoading = false
		queryStr := m.searchInput.Value()
		if queryStr == "" {
			// Empty search clears filter - reload without flashing "Loading..."
			m.searchQuery = ""
			m.contextStats = nil
			if m.level == levelMessageList {
				m.loadRequestID++
				return m, m.loadMessages()
			}
			m.aggregateRequestID++
			return m, m.loadData()
		}
		m.searchQuery = queryStr
		// In message list view, execute search to show results
		if m.level == levelMessageList {
			m.searchFilter = m.drillFilter
			m.searchFilter.SourceID = m.accountFilter
			m.searchFilter.WithAttachmentsOnly = m.attachmentFilter
			m.searchRequestID++
			m.loading = true
			spinCmd := m.startSpinner()
			return m, tea.Batch(spinCmd, m.loadSearch(queryStr))
		}
		// In aggregate views, results already showing from debounced search
		return m, nil

	case "esc":
		// Cancel search, exit inline mode, clear partial search
		m.inlineSearchActive = false
		m.inlineSearchLoading = false
		m.searchInput.SetValue("")
		// Clear search filter and reload without flashing "Loading..."
		m.searchQuery = ""
		m.contextStats = nil
		if m.level == levelMessageList {
			m.loadRequestID++
			return m, m.loadMessages()
		}
		m.aggregateRequestID++
		return m, m.loadData()

	case "ctrl+c":
		m.quitting = true
		return m, tea.Quit

	case "tab":
		// Toggle search mode between Fast and Deep (only at message list level)
		if m.level != levelMessageList {
			return m, nil // Tab has no effect at aggregate levels
		}
		if m.searchMode == SearchModeFast {
			m.searchMode = SearchModeDeep
			m.searchInput.Placeholder = "search (Tab: fast)"
		} else {
			m.searchMode = SearchModeFast
			m.searchInput.Placeholder = "search (Tab: deep)"
		}
		// Invalidate any pending debounce timers from old mode
		m.inlineSearchDebounce++
		// Re-trigger search immediately with new mode (no debounce for explicit mode toggle)
		if query := m.searchInput.Value(); query != "" {
			m.searchQuery = query
			m.inlineSearchLoading = true
			spinCmd := m.startSpinner()
			m.searchFilter = m.drillFilter
			m.searchFilter.SourceID = m.accountFilter
			m.searchFilter.WithAttachmentsOnly = m.attachmentFilter
			m.searchRequestID++
			return m, tea.Batch(spinCmd, m.loadSearch(query))
		}
		return m, nil

	default:
		// Pass key to text input
		var cmd tea.Cmd
		m.searchInput, cmd = m.searchInput.Update(msg)

		// Trigger debounced search (Fast: 100ms, Deep: 500ms)
		query := m.searchInput.Value()
		m.inlineSearchDebounce++
		debounceID := m.inlineSearchDebounce

		delay := inlineSearchDebounceDelay
		if m.searchMode == SearchModeDeep {
			delay = deepSearchDebounceDelay
		}

		// Show loading spinner immediately while waiting for debounce
		var spinCmd tea.Cmd
		if query != "" {
			m.inlineSearchLoading = true
			spinCmd = m.startSpinner()
		} else {
			m.inlineSearchLoading = false
		}

		debounceCmd := tea.Tick(delay, func(t time.Time) tea.Msg {
			return searchDebounceMsg{query: query, debounceID: debounceID}
		})

		return m, tea.Batch(cmd, spinCmd, debounceCmd)
	}
}

// handleAggregateKeys handles keys in the aggregate view.
func (m Model) handleAggregateKeys(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	// Handle inline search first (takes priority over modal)
	if m.inlineSearchActive {
		return m.handleInlineSearchKeys(msg)
	}

	// Handle modal
	if m.modal != modalNone {
		return m.handleModalKeys(msg)
	}

	switch msg.String() {
	// Quit - show confirmation modal (Ctrl+C exits immediately)
	case "q":
		m.modal = modalQuitConfirm
		return m, nil
	case "ctrl+c":
		m.quitting = true
		return m, tea.Quit

	// Esc clears search filter at top level
	case "esc":
		if m.searchQuery != "" {
			m.searchQuery = ""
			m.contextStats = nil
			m.searchInput.SetValue("")
			m.aggregateRequestID++
			return m, m.loadData()
		}

	// Account selector
	case "A":
		m.modal = modalAccountSelector
		// Set cursor to current selection, default to 0 if not found
		m.modalCursor = 0 // Default to "All Accounts"
		if m.accountFilter != nil {
			for i, acc := range m.accounts {
				if acc.ID == *m.accountFilter {
					m.modalCursor = i + 1 // +1 because 0 is "All Accounts"
					break
				}
			}
		}
		// Clamp to valid range in case accounts list changed
		if m.modalCursor > len(m.accounts) {
			m.modalCursor = 0
		}
		return m, nil

	// Attachment filter
	case "f":
		m.modal = modalAttachmentFilter
		if m.attachmentFilter {
			m.modalCursor = 1 // "With Attachments"
		} else {
			m.modalCursor = 0 // "All Messages"
		}
		return m, nil

	// Search - activate inline search bar (Fast only at aggregate level)
	case "/":
		m.inlineSearchActive = true
		m.searchMode = SearchModeFast
		m.searchInput.Placeholder = "search"
		m.searchInput.SetValue("") // Clear previous search
		m.searchInput.Focus()
		return m, textinput.Blink

	// Selection
	case " ": // Space to toggle selection
		if len(m.rows) > 0 && m.cursor < len(m.rows) {
			key := m.rows[m.cursor].Key
			if m.selection.AggregateKeys[key] {
				delete(m.selection.AggregateKeys, key)
			} else {
				m.selection.AggregateKeys[key] = true
			}
		}

	case "S": // Select all visible
		endRow := m.scrollOffset + m.pageSize
		if endRow > len(m.rows) {
			endRow = len(m.rows)
		}
		for i := m.scrollOffset; i < endRow; i++ {
			m.selection.AggregateKeys[m.rows[i].Key] = true
		}

	case "x": // Clear selection
		m.selection.AggregateKeys = make(map[string]bool)
		m.selection.MessageIDs = make(map[int64]bool)

	case "a": // Jump to all messages view
		m.frozenView = m.renderView() // Freeze screen until data loads
		m.breadcrumbs = append(m.breadcrumbs, navigationSnapshot{state: m.viewState})
		m.allMessages = true // Show all messages, not filtered by aggregate
		m.filterKey = ""
		m.level = levelMessageList
		m.cursor = 0
		m.scrollOffset = 0
		m.messages = nil // Clear stale messages from previous view
		m.loading = true
		m.err = nil

		// If there's an active search query, show search results instead of all messages
		if m.searchQuery != "" {
			m.searchRequestID++
			return m, m.loadSearch(m.searchQuery)
		}
		m.loadRequestID++
		return m, m.loadMessages()

	case "d", "D": // Stage for deletion (selection or current row)
		if !m.HasSelection() && len(m.rows) > 0 && m.cursor < len(m.rows) {
			// No selection - select current row first
			m.selection.AggregateKeys[m.rows[m.cursor].Key] = true
		}
		return m.stageForDeletion()

	// Drill down - go to message list for selected aggregate
	case "enter":
		if len(m.rows) > 0 && m.cursor < len(m.rows) {
			m.frozenView = m.renderView() // Freeze screen until data loads

			// Save current state to breadcrumb
			m.breadcrumbs = append(m.breadcrumbs, navigationSnapshot{state: m.viewState})

			// Build drill filter from selected row
			selectedRow := m.rows[m.cursor]
			m.drillViewType = m.viewType
			m.drillFilter = query.MessageFilter{
				SourceID:            m.accountFilter,
				WithAttachmentsOnly: m.attachmentFilter,
				TimeGranularity:     m.timeGranularity,
			}
			// Set contextual stats from selected row
			m.contextStats = &query.TotalStats{
				MessageCount:    selectedRow.Count,
				TotalSize:       selectedRow.TotalSize,
				AttachmentSize:  selectedRow.AttachmentSize,
				AttachmentCount: selectedRow.AttachmentCount,
			}
			key := selectedRow.Key
			switch m.viewType {
			case query.ViewSenders:
				m.drillFilter.Sender = key
				m.drillFilter.MatchEmptySender = (key == "")
			case query.ViewRecipients:
				m.drillFilter.Recipient = key
				m.drillFilter.MatchEmptyRecipient = (key == "")
			case query.ViewDomains:
				m.drillFilter.Domain = key
				m.drillFilter.MatchEmptyDomain = (key == "")
			case query.ViewLabels:
				m.drillFilter.Label = key
				m.drillFilter.MatchEmptyLabel = (key == "")
			case query.ViewTime:
				m.drillFilter.TimePeriod = key
			}

			// Go directly to message list (like moneyflow)
			m.filterKey = key
			m.allMessages = false
			m.level = levelMessageList
			m.cursor = 0
			m.scrollOffset = 0
			m.messages = nil // Clear stale messages from previous drill-down
			m.loading = true
			m.err = nil
			// Clear selection - it's scoped to previous view
			m.selection.AggregateKeys = make(map[string]bool)
			m.selection.MessageIDs = make(map[int64]bool)

			// If search query is active, use search with drill filter applied
			// This ensures message list matches the filtered aggregate counts
			if m.searchQuery != "" {
				// Merge drillFilter into searchFilter, preserving account/attachment filters
				m.searchFilter = m.drillFilter
				m.searchFilter.SourceID = m.accountFilter
				m.searchFilter.WithAttachmentsOnly = m.attachmentFilter
				m.searchRequestID++
				return m, m.loadSearch(m.searchQuery)
			}

			m.loadRequestID++
			return m, m.loadMessages()
		}

	// Navigation
	case "up", "k":
		if m.cursor > 0 {
			m.cursor--
			m.ensureCursorVisible()
		}
	case "down", "j":
		if m.cursor < len(m.rows)-1 {
			m.cursor++
			m.ensureCursorVisible()
		}
	case "pgup", "ctrl+u":
		m.cursor -= m.pageSize
		if m.cursor < 0 {
			m.cursor = 0
		}
		m.ensureCursorVisible()
	case "pgdown", "ctrl+d":
		m.cursor += m.pageSize
		if m.cursor >= len(m.rows) {
			m.cursor = len(m.rows) - 1
		}
		if m.cursor < 0 {
			m.cursor = 0
		}
		m.ensureCursorVisible()
	case "home":
		m.cursor = 0
		m.scrollOffset = 0
	case "end", "G":
		m.cursor = len(m.rows) - 1
		if m.cursor < 0 {
			m.cursor = 0
		}
		m.ensureCursorVisible()

	// View switching - 'g' cycles through groupings (like moneyflow), Tab also works
	case "g", "tab":
		m.viewType = (m.viewType + 1) % 5
		m.selection.AggregateKeys = make(map[string]bool)
		m.selection.AggregateViewType = m.viewType
		m.cursor = 0
		m.scrollOffset = 0
		m.loading = true
		m.aggregateRequestID++
		return m, m.loadData()

	case "shift+tab":
		if m.viewType == 0 {
			m.viewType = 4
		} else {
			m.viewType--
		}
		m.selection.AggregateKeys = make(map[string]bool)
		m.selection.AggregateViewType = m.viewType
		m.cursor = 0
		m.scrollOffset = 0
		m.loading = true
		m.aggregateRequestID++
		return m, m.loadData()

	// Time granularity (only in Time view)
	case "t":
		if m.viewType == query.ViewTime {
			m.timeGranularity = (m.timeGranularity + 1) % 3
			m.loading = true
			m.aggregateRequestID++
			return m, m.loadData()
		}

	// Sorting
	case "s":
		m.sortField = (m.sortField + 1) % 4
		m.loading = true
		m.aggregateRequestID++
		return m, m.loadData()

	case "r", "v":
		if m.sortDirection == query.SortDesc {
			m.sortDirection = query.SortAsc
		} else {
			m.sortDirection = query.SortDesc
		}
		m.loading = true
		m.aggregateRequestID++
		return m, m.loadData()

	// Help
	case "?":
		m.modal = modalHelp
		return m, nil
	}

	return m, nil
}

// nextSubGroupView returns the next logical sub-group view type.
// After drilling into Senders, default to Recipients; after Recipients, default to Domains, etc.
func (m Model) nextSubGroupView(current query.ViewType) query.ViewType {
	switch current {
	case query.ViewSenders:
		return query.ViewRecipients
	case query.ViewRecipients:
		return query.ViewDomains
	case query.ViewDomains:
		return query.ViewLabels
	case query.ViewLabels:
		return query.ViewTime
	case query.ViewTime:
		return query.ViewSenders
	default:
		return query.ViewRecipients
	}
}

// handleSubAggregateKeys handles keys in the sub-aggregate view.
func (m Model) handleSubAggregateKeys(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	// Handle inline search first (takes priority over modal)
	if m.inlineSearchActive {
		return m.handleInlineSearchKeys(msg)
	}

	// Handle modal
	if m.modal != modalNone {
		return m.handleModalKeys(msg)
	}

	switch msg.String() {
	// Quit - show confirmation modal (Ctrl+C exits immediately)
	case "q":
		m.modal = modalQuitConfirm
		return m, nil
	case "ctrl+c":
		m.quitting = true
		return m, tea.Quit

	// Back - navigate back if there are breadcrumbs, otherwise clear search
	case "esc":
		// If we have navigation history, go back first (preserves search context)
		if len(m.breadcrumbs) > 0 {
			return m.goBack()
		}
		// At top level with search, clear it
		if m.searchQuery != "" {
			m.searchQuery = ""
			m.contextStats = nil
			m.searchInput.SetValue("")
			m.aggregateRequestID++
			return m, m.loadData()
		}
		return m.goBack()

	// Account selector
	case "A":
		m.modal = modalAccountSelector
		m.modalCursor = 0
		if m.accountFilter != nil {
			for i, acc := range m.accounts {
				if acc.ID == *m.accountFilter {
					m.modalCursor = i + 1
					break
				}
			}
		}
		if m.modalCursor > len(m.accounts) {
			m.modalCursor = 0
		}
		return m, nil

	// Jump to all messages view (with drill filter applied)
	case "a":
		m.frozenView = m.renderView() // Freeze screen until data loads
		m.breadcrumbs = append(m.breadcrumbs, navigationSnapshot{state: m.viewState})
		m.allMessages = false
		m.level = levelMessageList
		m.cursor = 0
		m.scrollOffset = 0
		m.messages = nil // Clear stale messages from previous view
		m.loading = true
		m.err = nil

		// If there's an active search query, show search results instead of all messages
		if m.searchQuery != "" {
			m.searchRequestID++
			return m, m.loadSearch(m.searchQuery)
		}
		m.loadRequestID++
		return m, m.loadMessages()

	// Attachment filter
	case "f":
		m.modal = modalAttachmentFilter
		if m.attachmentFilter {
			m.modalCursor = 1
		} else {
			m.modalCursor = 0
		}
		return m, nil

	// Search - activate inline search bar (Fast only at aggregate level)
	case "/":
		m.inlineSearchActive = true
		m.searchMode = SearchModeFast
		m.searchInput.Placeholder = "search"
		m.searchInput.SetValue("") // Clear previous search
		m.searchInput.Focus()
		return m, textinput.Blink

	// Selection (same as aggregate view)
	case " ":
		if len(m.rows) > 0 && m.cursor < len(m.rows) {
			key := m.rows[m.cursor].Key
			if m.selection.AggregateKeys[key] {
				delete(m.selection.AggregateKeys, key)
			} else {
				m.selection.AggregateKeys[key] = true
			}
		}

	case "S": // Select all visible
		endRow := m.scrollOffset + m.pageSize
		if endRow > len(m.rows) {
			endRow = len(m.rows)
		}
		for i := m.scrollOffset; i < endRow; i++ {
			m.selection.AggregateKeys[m.rows[i].Key] = true
		}

	case "x":
		m.selection.AggregateKeys = make(map[string]bool)
		m.selection.MessageIDs = make(map[int64]bool)

	// Stage for deletion (selection or current row)
	case "d", "D":
		if !m.HasSelection() && len(m.rows) > 0 && m.cursor < len(m.rows) {
			// No selection - select current row first
			m.selection.AggregateKeys[m.rows[m.cursor].Key] = true
		}
		return m.stageForDeletion()

	// Drill down to message list with combined filter
	case "enter":
		if len(m.rows) > 0 && m.cursor < len(m.rows) {
			m.frozenView = m.renderView() // Freeze screen until data loads

			// Save current state (including contextStats before it's overwritten)
			m.breadcrumbs = append(m.breadcrumbs, navigationSnapshot{state: m.viewState})

			// Set contextual stats from selected row
			selectedRow := m.rows[m.cursor]
			m.contextStats = &query.TotalStats{
				MessageCount:    selectedRow.Count,
				TotalSize:       selectedRow.TotalSize,
				AttachmentSize:  selectedRow.AttachmentSize,
				AttachmentCount: selectedRow.AttachmentCount,
			}

			// Add sub-group filter to drill filter
			key := selectedRow.Key
			switch m.viewType {
			case query.ViewSenders:
				m.drillFilter.Sender = key
				m.drillFilter.MatchEmptySender = (key == "")
			case query.ViewRecipients:
				m.drillFilter.Recipient = key
				m.drillFilter.MatchEmptyRecipient = (key == "")
			case query.ViewDomains:
				m.drillFilter.Domain = key
				m.drillFilter.MatchEmptyDomain = (key == "")
			case query.ViewLabels:
				m.drillFilter.Label = key
				m.drillFilter.MatchEmptyLabel = (key == "")
			case query.ViewTime:
				m.drillFilter.TimePeriod = key
			}

			m.filterKey = key
			m.allMessages = false
			m.level = levelMessageList
			m.cursor = 0
			m.scrollOffset = 0
			m.messages = nil // Clear stale messages from previous drill-down
			m.loading = true
			m.err = nil

			// If search query is active, use search with drill filter applied
			// This ensures message list matches the filtered aggregate counts
			if m.searchQuery != "" {
				// Merge drillFilter into searchFilter, preserving account/attachment filters
				m.searchFilter = m.drillFilter
				m.searchFilter.SourceID = m.accountFilter
				m.searchFilter.WithAttachmentsOnly = m.attachmentFilter
				m.searchRequestID++
				return m, m.loadSearch(m.searchQuery)
			}

			m.loadRequestID++
			return m, m.loadMessages()
		}

	// Navigation (same as aggregate view)
	case "up", "k":
		if m.cursor > 0 {
			m.cursor--
			m.ensureCursorVisible()
		}
	case "down", "j":
		if m.cursor < len(m.rows)-1 {
			m.cursor++
			m.ensureCursorVisible()
		}
	case "pgup", "ctrl+u":
		m.cursor -= m.pageSize
		if m.cursor < 0 {
			m.cursor = 0
		}
		m.ensureCursorVisible()
	case "pgdown", "ctrl+d":
		m.cursor += m.pageSize
		if m.cursor >= len(m.rows) {
			m.cursor = len(m.rows) - 1
		}
		if m.cursor < 0 {
			m.cursor = 0
		}
		m.ensureCursorVisible()
	case "home":
		m.cursor = 0
		m.scrollOffset = 0
	case "end", "G":
		m.cursor = len(m.rows) - 1
		if m.cursor < 0 {
			m.cursor = 0
		}
		m.ensureCursorVisible()

	// Sub-grouping view switching - 'g' cycles through groupings (like moneyflow), Tab also works
	// Skips the drill view type (can't sub-group by the same dimension)
	case "g", "tab":
		m.viewType = (m.viewType + 1) % 5
		// Skip the drill view type (can't sub-group by the same dimension)
		if m.viewType == m.drillViewType {
			m.viewType = (m.viewType + 1) % 5
		}
		m.selection.AggregateKeys = make(map[string]bool)
		m.selection.AggregateViewType = m.viewType
		m.cursor = 0
		m.scrollOffset = 0
		m.loading = true
		m.aggregateRequestID++
		return m, m.loadData()

	case "shift+tab":
		if m.viewType == 0 {
			m.viewType = 4
		} else {
			m.viewType--
		}
		// Skip the drill view type
		if m.viewType == m.drillViewType {
			if m.viewType == 0 {
				m.viewType = 4
			} else {
				m.viewType--
			}
		}
		m.selection.AggregateKeys = make(map[string]bool)
		m.selection.AggregateViewType = m.viewType
		m.cursor = 0
		m.scrollOffset = 0
		m.loading = true
		m.aggregateRequestID++
		return m, m.loadData()

	// Time granularity (only in Time view)
	case "t":
		if m.viewType == query.ViewTime {
			m.timeGranularity = (m.timeGranularity + 1) % 3
			m.loading = true
			m.aggregateRequestID++
			return m, m.loadData()
		}

	// Sorting
	case "s":
		m.sortField = (m.sortField + 1) % 4
		m.loading = true
		m.aggregateRequestID++
		return m, m.loadData()

	case "r", "v":
		if m.sortDirection == query.SortDesc {
			m.sortDirection = query.SortAsc
		} else {
			m.sortDirection = query.SortDesc
		}
		m.loading = true
		m.aggregateRequestID++
		return m, m.loadData()

	// Help
	case "?":
		m.modal = modalHelp
		return m, nil
	}

	return m, nil
}

// handleMessageListKeys handles keys in the message list view.
func (m Model) handleMessageListKeys(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	// Handle inline search first (takes priority over modal)
	if m.inlineSearchActive {
		return m.handleInlineSearchKeys(msg)
	}

	// Handle modal
	if m.modal != modalNone {
		return m.handleModalKeys(msg)
	}

	switch msg.String() {
	// Quit - show confirmation modal (Ctrl+C exits immediately)
	case "q":
		m.modal = modalQuitConfirm
		return m, nil
	case "ctrl+c":
		m.quitting = true
		return m, tea.Quit

	// Back - navigate back if there are breadcrumbs, otherwise clear search
	case "esc":
		// If we have navigation history, go back first (preserves search context)
		if len(m.breadcrumbs) > 0 {
			return m.goBack()
		}
		// At top level with search, clear it
		if m.searchQuery != "" {
			m.searchQuery = ""
			m.searchFilter = query.MessageFilter{}
			m.contextStats = nil
			m.searchInput.SetValue("")
			m.loadRequestID++
			return m, m.loadMessages()
		}
		return m.goBack()

	// Selection
	case " ": // Space to toggle selection
		if len(m.messages) > 0 && m.cursor < len(m.messages) {
			id := m.messages[m.cursor].ID
			if m.selection.MessageIDs[id] {
				delete(m.selection.MessageIDs, id)
			} else {
				m.selection.MessageIDs[id] = true
			}
		}

	case "S": // Select all visible (only on-screen items)
		endRow := m.scrollOffset + m.pageSize
		if endRow > len(m.messages) {
			endRow = len(m.messages)
		}
		for i := m.scrollOffset; i < endRow; i++ {
			m.selection.MessageIDs[m.messages[i].ID] = true
		}

	case "x": // Clear selection
		m.selection.AggregateKeys = make(map[string]bool)
		m.selection.MessageIDs = make(map[int64]bool)

	case "d", "D": // Stage for deletion (selection or current row)
		if !m.HasSelection() && len(m.messages) > 0 && m.cursor < len(m.messages) {
			// No selection - select current row first
			m.selection.MessageIDs[m.messages[m.cursor].ID] = true
		}
		return m.stageForDeletion()

	// Attachment filter
	case "f":
		m.modal = modalAttachmentFilter
		if m.attachmentFilter {
			m.modalCursor = 1 // "With Attachments"
		} else {
			m.modalCursor = 0 // "All Messages"
		}
		return m, nil

	// Search - activate inline search bar
	case "/":
		m.inlineSearchActive = true
		m.searchMode = SearchModeFast
		m.searchInput.Placeholder = "search (Tab: deep)"
		m.searchInput.SetValue("") // Clear previous search
		m.searchInput.Focus()
		return m, textinput.Blink

	// Sub-grouping: switch to aggregate breakdown within current filter
	case "tab":
		if m.hasDrillFilter() {
			m.frozenView = m.renderView() // Freeze screen until data loads

			// Save current state to breadcrumb (including viewType for proper restoration)
			m.breadcrumbs = append(m.breadcrumbs, navigationSnapshot{state: m.viewState})

			// Switch to sub-aggregate view
			m.level = levelSubAggregate
			m.viewType = m.nextSubGroupView(m.drillViewType)
			m.cursor = 0
			m.scrollOffset = 0
			m.rows = nil // Clear stale rows from previous view
			m.loading = true
			m.err = nil
			m.selection.AggregateKeys = make(map[string]bool)
			m.selection.AggregateViewType = m.viewType
			m.aggregateRequestID++
			return m, m.loadData()
		}

	// Drill down to message detail
	case "enter":
		if len(m.messages) > 0 && m.cursor < len(m.messages) {
			m.frozenView = m.renderView() // Freeze screen until data loads

			// Save current state (include all fields for proper restoration)
			m.breadcrumbs = append(m.breadcrumbs, navigationSnapshot{state: m.viewState})

			// Store pending subject for breadcrumb while loading
			m.pendingDetailSubject = m.messages[m.cursor].Subject
			m.detailMessageIndex = m.cursor // Track which message we're viewing
			m.detailFromThread = false      // Navigate within list messages
			m.messageDetail = nil           // Clear stale detail
			m.detailLineCount = 0           // Reset line count to avoid stale N/M display
			m.detailScroll = 0              // Reset scroll position
			m.level = levelMessageDetail
			m.loading = true
			m.err = nil         // Clear any previous error
			m.detailRequestID++ // Increment to invalidate stale responses
			return m, m.loadMessageDetail(m.messages[m.cursor].ID)
		}

	// Navigation
	case "up", "k":
		if m.cursor > 0 {
			m.cursor--
			m.ensureCursorVisible()
		}
	case "down", "j":
		if m.cursor < len(m.messages)-1 {
			m.cursor++
			m.ensureCursorVisible()
		}
	case "pgup", "ctrl+u":
		m.cursor -= m.pageSize
		if m.cursor < 0 {
			m.cursor = 0
		}
		m.ensureCursorVisible()
	case "pgdown", "ctrl+d":
		m.cursor += m.pageSize
		if m.cursor >= len(m.messages) {
			m.cursor = len(m.messages) - 1
		}
		if m.cursor < 0 {
			m.cursor = 0
		}
		m.ensureCursorVisible()

		// Load more deep search results when reaching the bottom
		if m.searchQuery != "" && m.searchMode == SearchModeDeep &&
			m.searchTotalCount == -1 && !m.searchLoadingMore && !m.loading &&
			m.cursor >= len(m.messages)-1 && len(m.messages) > 0 {
			m.searchLoadingMore = true
			m.searchRequestID++
			spinCmd := m.startSpinner()
			return m, tea.Batch(spinCmd, m.loadSearchWithOffset(m.searchQuery, m.searchOffset, true))
		}

	// Sub-grouping: 'g' switches to aggregate breakdown within current filter (like tab)
	case "g":
		m.frozenView = m.renderView() // Freeze screen until data loads
		if m.hasDrillFilter() {
			// Save current state to breadcrumb (including viewType for proper restoration)
			m.breadcrumbs = append(m.breadcrumbs, navigationSnapshot{state: m.viewState})

			// Switch to sub-aggregate view
			m.level = levelSubAggregate
			m.viewType = m.nextSubGroupView(m.drillViewType)
			m.cursor = 0
			m.scrollOffset = 0
			m.rows = nil // Clear stale rows from previous view
			m.loading = true
			m.err = nil
			m.selection.AggregateKeys = make(map[string]bool)
			m.selection.AggregateViewType = m.viewType
			m.aggregateRequestID++
			return m, m.loadData()
		}
		// No drill filter (e.g., All Messages or search results) - go back to aggregate view
		// Clear search state (mirroring goBack behavior)
		m.searchQuery = ""
		m.searchFilter = query.MessageFilter{}
		m.contextStats = nil
		m.level = levelAggregates
		m.cursor = 0
		m.scrollOffset = 0
		m.rows = nil // Clear stale rows from previous view
		m.loading = true
		m.err = nil
		m.aggregateRequestID++
		return m, m.loadData()

	case "home":
		m.cursor = 0
		m.scrollOffset = 0
	case "end", "G":
		m.cursor = len(m.messages) - 1
		if m.cursor < 0 {
			m.cursor = 0
		}
		m.ensureCursorVisible()

	// Sorting - use explicit field list to avoid hidden coupling
	case "s":
		msgSortFields := []query.MessageSortField{
			query.MessageSortByDate,
			query.MessageSortBySize,
			query.MessageSortBySubject,
		}
		for i, f := range msgSortFields {
			if f == m.msgSortField {
				m.msgSortField = msgSortFields[(i+1)%len(msgSortFields)]
				break
			}
		}
		m.loading = true
		m.err = nil       // Clear any previous error
		m.loadRequestID++ // Increment to invalidate stale responses
		return m, m.loadMessages()

	case "r", "v":
		if m.msgSortDirection == query.SortDesc {
			m.msgSortDirection = query.SortAsc
		} else {
			m.msgSortDirection = query.SortDesc
		}
		m.loading = true
		m.err = nil       // Clear any previous error
		m.loadRequestID++ // Increment to invalidate stale responses
		return m, m.loadMessages()

	// View thread
	case "T":
		if len(m.messages) > 0 && m.cursor < len(m.messages) {
			convID := m.messages[m.cursor].ConversationID
			if convID > 0 {
				m.frozenView = m.renderView() // Freeze screen until data loads

				// Save current state
				m.breadcrumbs = append(m.breadcrumbs, navigationSnapshot{state: m.viewState})

				m.threadConversationID = convID
				m.threadMessages = nil
				m.threadCursor = 0
				m.threadScrollOffset = 0
				m.level = levelThreadView
				m.loading = true
				m.err = nil
				m.loadRequestID++
				return m, m.loadThreadMessages(convID)
			}
		}

	// Help
	case "?":
		m.modal = modalHelp
		return m, nil
	}

	// Check if we should load more search results
	if cmd := m.maybeLoadMoreSearchResults(); cmd != nil {
		return m, cmd
	}

	return m, nil
}

// maybeLoadMoreSearchResults checks if we're near the end of search results and should load more.
func (m *Model) maybeLoadMoreSearchResults() tea.Cmd {
	// Only paginate search results in fast mode
	if m.searchQuery == "" || m.searchMode != SearchModeFast {
		return nil
	}

	// Don't load more if already loading or no more results
	if m.searchLoadingMore || m.loading {
		return nil
	}

	// Don't load more if we have no messages (empty results)
	if len(m.messages) == 0 {
		return nil
	}

	// Check if total count is known and we have all results
	// Note: searchTotalCount == 0 means no results, so we should not load more
	if m.searchTotalCount >= 0 && int64(len(m.messages)) >= m.searchTotalCount {
		return nil
	}

	// Load more when cursor is within 20 rows of the end
	threshold := 20
	if m.cursor >= len(m.messages)-threshold {
		m.searchLoadingMore = true
		m.searchRequestID++
		return m.loadSearchWithOffset(m.searchQuery, m.searchOffset, true)
	}

	return nil
}

// handleMessageDetailKeys handles keys in the message detail view.
func (m Model) handleMessageDetailKeys(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	// Handle quit confirmation modal
	if m.modal == modalQuitConfirm {
		return m.handleModalKeys(msg)
	}

	switch msg.String() {
	// Quit - show confirmation modal (Ctrl+C exits immediately)
	case "q":
		m.modal = modalQuitConfirm
		return m, nil
	case "ctrl+c":
		m.quitting = true
		return m, tea.Quit

	// Back to message list
	case "esc":
		return m.goBack()

	// Navigate to previous message in list (left = towards first)
	case "left", "h":
		return m.navigateDetailPrev()

	// Navigate to next message in list (right = towards last)
	case "right", "l":
		return m.navigateDetailNext()

	// Scroll content
	case "up", "k":
		// Clamp first in case scroll is out of range after resize
		m.clampDetailScroll()
		if m.detailScroll > 0 {
			m.detailScroll--
		} else {
			return m.showFlash("At top")
		}
	case "down", "j":
		// Clamp first in case scroll is out of range after resize
		m.clampDetailScroll()
		maxScroll := m.detailLineCount - m.detailPageSize()
		if maxScroll < 0 {
			maxScroll = 0
		}
		if m.detailScroll < maxScroll {
			m.detailScroll++
		} else {
			return m.showFlash("At bottom")
		}
	case "pgup", "ctrl+u":
		// Clamp first in case scroll is out of range after resize
		m.clampDetailScroll()
		if m.detailScroll == 0 {
			return m.showFlash("At top")
		}
		m.detailScroll -= m.detailPageSize()
		if m.detailScroll < 0 {
			m.detailScroll = 0
		}
	case "pgdown", "ctrl+d":
		// Clamp first in case scroll is out of range after resize
		m.clampDetailScroll()
		maxScroll := m.detailLineCount - m.detailPageSize()
		if maxScroll < 0 {
			maxScroll = 0
		}
		if m.detailScroll >= maxScroll {
			return m.showFlash("At bottom")
		}
		m.detailScroll += m.detailPageSize()
		m.clampDetailScroll()
	case "home", "g":
		m.detailScroll = 0
	case "end", "G":
		m.detailScroll = m.detailLineCount - m.detailPageSize()
		if m.detailScroll < 0 {
			m.detailScroll = 0
		}

	// View thread
	case "T":
		if m.messageDetail != nil && m.messageDetail.ConversationID > 0 {
			m.frozenView = m.renderView() // Freeze screen until data loads

			// Save current state
			m.breadcrumbs = append(m.breadcrumbs, navigationSnapshot{state: m.viewState})

			m.threadConversationID = m.messageDetail.ConversationID
			m.threadMessages = nil
			m.threadCursor = 0
			m.threadScrollOffset = 0
			m.level = levelThreadView
			m.loading = true
			m.err = nil
			m.loadRequestID++
			return m, m.loadThreadMessages(m.messageDetail.ConversationID)
		}

	// Export attachments
	case "e":
		if m.messageDetail != nil && len(m.messageDetail.Attachments) > 0 {
			m.modal = modalExportAttachments
			m.modalCursor = 0
			// Initialize selection: all attachments selected by default
			m.exportSelection = make(map[int]bool)
			for i := range m.messageDetail.Attachments {
				m.exportSelection[i] = true
			}
			m.exportCursor = 0
		} else {
			return m.showFlash("No attachments to export")
		}
	}

	return m, nil
}

// handleThreadViewKeys handles keys in the thread view.
func (m Model) handleThreadViewKeys(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	// Handle quit confirmation modal
	if m.modal == modalQuitConfirm {
		return m.handleModalKeys(msg)
	}

	switch msg.String() {
	// Quit
	case "q":
		m.modal = modalQuitConfirm
		return m, nil
	case "ctrl+c":
		m.quitting = true
		return m, tea.Quit

	// Back to previous view
	case "esc":
		return m.goBack()

	// Navigation
	case "up", "k":
		if m.threadCursor > 0 {
			m.threadCursor--
			m.ensureThreadCursorVisible()
		}
	case "down", "j":
		if m.threadCursor < len(m.threadMessages)-1 {
			m.threadCursor++
			m.ensureThreadCursorVisible()
		}
	case "pgup", "ctrl+u":
		m.threadCursor -= m.pageSize
		if m.threadCursor < 0 {
			m.threadCursor = 0
		}
		m.ensureThreadCursorVisible()
	case "pgdown", "ctrl+d":
		m.threadCursor += m.pageSize
		if m.threadCursor >= len(m.threadMessages) {
			m.threadCursor = len(m.threadMessages) - 1
		}
		if m.threadCursor < 0 {
			m.threadCursor = 0
		}
		m.ensureThreadCursorVisible()

	// View message detail
	case "enter":
		if len(m.threadMessages) > 0 && m.threadCursor < len(m.threadMessages) {
			m.frozenView = m.renderView() // Freeze screen until data loads

			// Save current thread view state
			m.breadcrumbs = append(m.breadcrumbs, navigationSnapshot{state: m.viewState})

			// Load message detail
			m.pendingDetailSubject = m.threadMessages[m.threadCursor].Subject
			m.detailMessageIndex = m.threadCursor
			m.detailFromThread = true // Navigate within thread messages
			m.messageDetail = nil
			m.detailLineCount = 0
			m.detailScroll = 0
			m.level = levelMessageDetail
			m.loading = true
			m.err = nil
			m.detailRequestID++
			return m, m.loadMessageDetail(m.threadMessages[m.threadCursor].ID)
		}

	// Help
	case "?":
		m.modal = modalHelp
		return m, nil
	}

	return m, nil
}

// ensureThreadCursorVisible adjusts scroll offset to keep cursor visible in thread view.
func (m *Model) ensureThreadCursorVisible() {
	if m.threadCursor < m.threadScrollOffset {
		m.threadScrollOffset = m.threadCursor
	} else if m.threadCursor >= m.threadScrollOffset+m.pageSize {
		m.threadScrollOffset = m.threadCursor - m.pageSize + 1
	}
}

// navigateDetailPrev navigates to the previous message in list/thread order.
// Left arrow moves towards the first item (lower index).
func (m Model) navigateDetailPrev() (tea.Model, tea.Cmd) {
	// Use thread messages if we entered from thread view, otherwise use list messages
	var msgs []query.MessageSummary
	if m.detailFromThread {
		msgs = m.threadMessages
	} else {
		msgs = m.messages
	}

	// Guard against empty message list
	if len(msgs) == 0 {
		return m.showFlash("No messages loaded")
	}

	// Clamp index if it's out of bounds (can happen if list changed)
	if m.detailMessageIndex >= len(msgs) {
		m.detailMessageIndex = len(msgs) - 1
	}

	if m.detailMessageIndex > 0 {
		// Go to previous message
		m.detailMessageIndex--
		// Keep appropriate cursor in sync
		if m.detailFromThread {
			m.threadCursor = m.detailMessageIndex
			// Ensure thread scroll offset keeps cursor visible when returning
			if m.threadCursor < m.threadScrollOffset {
				m.threadScrollOffset = m.threadCursor
			}
		} else {
			m.cursor = m.detailMessageIndex
		}
		m.pendingDetailSubject = msgs[m.detailMessageIndex].Subject
		// Keep old messageDetail visible while new one loads (no flash)
		m.detailScroll = 0
		m.loading = true
		m.err = nil
		m.detailRequestID++
		return m, m.loadMessageDetail(msgs[m.detailMessageIndex].ID)
	}

	// At the first message - show flash notification
	return m.showFlash("At first message")
}

// navigateDetailNext navigates to the next message in list/thread order.
// Right arrow moves towards the last item (higher index).
func (m Model) navigateDetailNext() (tea.Model, tea.Cmd) {
	// Use thread messages if we entered from thread view, otherwise use list messages
	var msgs []query.MessageSummary
	if m.detailFromThread {
		msgs = m.threadMessages
	} else {
		msgs = m.messages
	}

	// Guard against empty message list
	if len(msgs) == 0 {
		return m.showFlash("No messages loaded")
	}

	// Clamp index if it's out of bounds (can happen if list changed)
	if m.detailMessageIndex >= len(msgs) {
		m.detailMessageIndex = len(msgs) - 1
	}

	if m.detailMessageIndex < len(msgs)-1 {
		// Go to next message
		m.detailMessageIndex++
		// Keep appropriate cursor in sync
		if m.detailFromThread {
			m.threadCursor = m.detailMessageIndex
			// Ensure thread scroll offset keeps cursor visible when returning
			if m.threadCursor >= m.threadScrollOffset+m.pageSize {
				m.threadScrollOffset = m.threadCursor - m.pageSize + 1
			}
		} else {
			m.cursor = m.detailMessageIndex
		}
		m.pendingDetailSubject = msgs[m.detailMessageIndex].Subject
		// Keep old messageDetail visible while new one loads (no flash)
		m.detailScroll = 0
		m.loading = true
		m.err = nil
		m.detailRequestID++
		return m, m.loadMessageDetail(msgs[m.detailMessageIndex].ID)
	}

	// At the last message - show flash notification
	return m.showFlash("At last message")
}

// showFlash displays a temporary flash message.
func (m Model) showFlash(message string) (tea.Model, tea.Cmd) {
	m.flashMessage = message
	m.flashExpiresAt = time.Now().Add(flashDuration)
	return m, tea.Tick(flashDuration, func(t time.Time) tea.Msg {
		return flashClearMsg{}
	})
}

// detailPageSize returns the page size for detail view (2 more than table views).
func (m *Model) detailPageSize() int {
	return m.pageSize + 2
}

// clampDetailScroll ensures detailScroll stays within valid bounds.
func (m *Model) clampDetailScroll() {
	maxScroll := m.detailLineCount - m.detailPageSize()
	if maxScroll < 0 {
		maxScroll = 0
	}
	if m.detailScroll > maxScroll {
		m.detailScroll = maxScroll
	}
}

// updateDetailLineCount recalculates the line count for scroll bounds.
// This is called when message detail is loaded or window is resized.
func (m *Model) updateDetailLineCount() {
	if m.messageDetail == nil {
		m.detailLineCount = 0
		return
	}
	lines := m.buildDetailLines()
	m.detailLineCount = len(lines)
}

// goBack returns to the previous view level.
func (m Model) goBack() (tea.Model, tea.Cmd) {
	if len(m.breadcrumbs) == 0 {
		return m, nil
	}

	m.frozenView = "" // Clear frozen view on navigation back

	// Pop breadcrumb
	bc := m.breadcrumbs[len(m.breadcrumbs)-1]
	m.breadcrumbs = m.breadcrumbs[:len(m.breadcrumbs)-1]

	// Track current cursor if we need to preserve it (e.g. from detail navigation)
	currentCursor := m.cursor
	currentLevel := m.level

	// Restore complete state from snapshot
	m.viewState = bc.state

	// Special case: returning from detail view to message list
	// The user might have navigated left/right in detail view.
	// If we simply restore `bc.state`, we revert to the cursor when we entered detail view.
	// We want to keep the NEW cursor if we just navigated.
	if currentLevel == levelMessageDetail && m.level == levelMessageList {
		m.cursor = currentCursor
		m.ensureCursorVisible()
	}

	m.err = nil       // Clear any stale error
	m.loading = false // Data is restored from snapshot

	return m, nil
}

// handleModalKeys handles keys when a modal is displayed.
func (m Model) handleModalKeys(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch m.modal {
	case modalDeleteConfirm:
		switch msg.String() {
		case "y", "Y":
			// Confirm deletion - save manifest
			return m.confirmDeletion()
		case "n", "N", "esc":
			// Cancel
			m.modal = modalNone
			m.pendingManifest = nil
		}

	case modalDeleteResult:
		// Any key dismisses the result
		m.modal = modalNone
		m.modalResult = ""

	case modalQuitConfirm:
		switch msg.String() {
		case "y", "Y", "enter":
			m.quitting = true
			return m, tea.Quit
		case "n", "N", "esc", "q":
			m.modal = modalNone
		}

	case modalAccountSelector:
		maxIdx := len(m.accounts) // 0 = All Accounts, then accounts
		switch msg.String() {
		case "up", "k":
			if m.modalCursor > 0 {
				m.modalCursor--
			}
		case "down", "j":
			if m.modalCursor < maxIdx {
				m.modalCursor++
			}
		case "enter":
			// Apply selection with bounds check
			if m.modalCursor == 0 || m.modalCursor > len(m.accounts) {
				m.accountFilter = nil // All accounts (or fallback if out of bounds)
			} else {
				accID := m.accounts[m.modalCursor-1].ID
				m.accountFilter = &accID
			}
			m.modal = modalNone
			m.loading = true
			m.aggregateRequestID++
			// Reload data with new account filter
			return m, tea.Batch(m.loadData(), m.loadStats())
		case "esc":
			m.modal = modalNone
		}

	case modalAttachmentFilter:
		switch msg.String() {
		case "up", "k":
			if m.modalCursor > 0 {
				m.modalCursor--
			}
		case "down", "j":
			if m.modalCursor < 1 {
				m.modalCursor++
			}
		case "enter":
			// Apply selection
			m.attachmentFilter = (m.modalCursor == 1)
			m.modal = modalNone
			m.loading = true
			// Reload data and stats based on view level
			if m.level == levelMessageList {
				m.loadRequestID++
				return m, tea.Batch(m.loadMessages(), m.loadStats())
			}
			// In aggregate view, reload aggregates and stats
			m.aggregateRequestID++
			return m, tea.Batch(m.loadData(), m.loadStats())
		case "esc":
			m.modal = modalNone
		}

	case modalExportAttachments:
		if m.messageDetail == nil || len(m.messageDetail.Attachments) == 0 {
			m.modal = modalNone
			return m, nil
		}
		maxIdx := len(m.messageDetail.Attachments) - 1
		switch msg.String() {
		case "up", "k":
			if m.exportCursor > 0 {
				m.exportCursor--
			}
		case "down", "j":
			if m.exportCursor < maxIdx {
				m.exportCursor++
			}
		case " ": // Space toggles selection
			m.exportSelection[m.exportCursor] = !m.exportSelection[m.exportCursor]
		case "a": // Select all
			for i := range m.messageDetail.Attachments {
				m.exportSelection[i] = true
			}
		case "n": // Select none
			for i := range m.messageDetail.Attachments {
				m.exportSelection[i] = false
			}
		case "enter":
			// Export selected attachments
			return m.exportAttachments()
		case "esc":
			m.modal = modalNone
			m.exportSelection = nil
		}

	case modalExportResult:
		// Any key closes the result modal
		m.modal = modalNone
		m.modalResult = ""

	case modalHelp:
		// Any key closes help
		m.modal = modalNone
	}

	return m, nil
}

// stageForDeletion prepares messages for deletion.
func (m Model) stageForDeletion() (tea.Model, tea.Cmd) {
	// Collect Gmail IDs to delete
	gmailIDSet := make(map[string]bool)

	ctx := context.Background()

	// From selected aggregates - resolve to Gmail IDs via query engine
	if len(m.selection.AggregateKeys) > 0 {
		// Resolve each selected aggregate key to message IDs
		for key := range m.selection.AggregateKeys {
			filter := query.MessageFilter{
				SourceID: m.accountFilter,
				// Limit: 0 means no limit - get all matching messages
			}

			// Set filter based on the view type the selections belong to
			switch m.selection.AggregateViewType {
			case query.ViewSenders:
				filter.Sender = key
			case query.ViewRecipients:
				filter.Recipient = key
			case query.ViewDomains:
				filter.Domain = key
			case query.ViewLabels:
				filter.Label = key
			case query.ViewTime:
				filter.TimePeriod = key
				filter.TimeGranularity = m.timeGranularity
			}

			// Fetch Gmail IDs for this aggregate
			ids, err := m.engine.GetGmailIDsByFilter(ctx, filter)
			if err != nil {
				m.modal = modalDeleteResult
				m.modalResult = fmt.Sprintf("Error loading messages: %v", err)
				return m, nil
			}

			for _, id := range ids {
				gmailIDSet[id] = true
			}
		}
	}

	// From selected message IDs - resolve via current messages list
	if len(m.selection.MessageIDs) > 0 && m.level == levelMessageList {
		for _, msg := range m.messages {
			if m.selection.MessageIDs[msg.ID] {
				gmailIDSet[msg.SourceMessageID] = true
			}
		}
	}

	// Convert set to slice
	gmailIDs := make([]string, 0, len(gmailIDSet))
	for id := range gmailIDSet {
		gmailIDs = append(gmailIDs, id)
	}

	if len(gmailIDs) == 0 {
		m.modal = modalDeleteResult
		m.modalResult = "No messages selected. Use Space to select, S for all visible."
		return m, nil
	}

	// Build description based on what's selected
	var description string
	if len(m.selection.AggregateKeys) == 1 {
		// Single aggregate selected
		for key := range m.selection.AggregateKeys {
			description = fmt.Sprintf("%s-%s", m.selection.AggregateViewType.String(), key)
			break
		}
	} else if len(m.selection.AggregateKeys) > 1 {
		description = fmt.Sprintf("%s-multiple(%d)", m.selection.AggregateViewType.String(), len(m.selection.AggregateKeys))
	} else if m.level == levelMessageList {
		description = fmt.Sprintf("%s-%s", m.viewType.String(), m.filterKey)
	} else {
		description = "selection"
	}
	if len(description) > 30 {
		description = description[:30]
	}

	// Create manifest
	manifest := deletion.NewManifest(description, gmailIDs)
	manifest.CreatedBy = "tui"

	// Set account in filters
	if m.accountFilter != nil {
		// Find account email from source ID
		// If not found (shouldn't happen), Account stays empty and UI will show warning
		for _, acc := range m.accounts {
			if acc.ID == *m.accountFilter {
				manifest.Filters.Account = acc.Identifier
				break
			}
		}
	} else if len(m.accounts) == 1 {
		// Only one account configured - use it
		manifest.Filters.Account = m.accounts[0].Identifier
	}
	// If multiple accounts and no filter, leave Account empty
	// (will require --account flag when executing)

	// Set filters for context (use first selected aggregate if any)
	if len(m.selection.AggregateKeys) > 0 {
		for key := range m.selection.AggregateKeys {
			switch m.selection.AggregateViewType {
			case query.ViewSenders:
				manifest.Filters.Sender = key
			case query.ViewRecipients:
				manifest.Filters.Recipient = key
			case query.ViewDomains:
				manifest.Filters.SenderDomain = key
			case query.ViewLabels:
				manifest.Filters.Label = key
			}
			break // Just use first key for filter context
		}
	} else if m.level == levelMessageList {
		switch m.viewType {
		case query.ViewSenders:
			manifest.Filters.Sender = m.filterKey
		case query.ViewRecipients:
			manifest.Filters.Recipient = m.filterKey
		case query.ViewDomains:
			manifest.Filters.SenderDomain = m.filterKey
		case query.ViewLabels:
			manifest.Filters.Label = m.filterKey
		}
	}

	// Store pending manifest and show confirmation
	m.pendingManifest = manifest
	m.modal = modalDeleteConfirm

	return m, nil
}

// confirmDeletion saves the manifest and shows result.
func (m Model) confirmDeletion() (tea.Model, tea.Cmd) {
	if m.pendingManifest == nil {
		m.modal = modalNone
		return m, nil
	}

	// Initialize deletion manager if needed
	if m.deletionMgr == nil {
		deletionsDir := filepath.Join(m.dataDir, "deletions")
		mgr, err := deletion.NewManager(deletionsDir)
		if err != nil {
			m.modal = modalDeleteResult
			m.modalResult = fmt.Sprintf("Error: %v", err)
			m.pendingManifest = nil
			return m, nil
		}
		m.deletionMgr = mgr
	}

	// Save manifest
	if err := m.deletionMgr.SaveManifest(m.pendingManifest); err != nil {
		m.modal = modalDeleteResult
		m.modalResult = fmt.Sprintf("Error saving manifest: %v", err)
		m.pendingManifest = nil
		return m, nil
	}

	// Show success
	m.modal = modalDeleteResult
	m.modalResult = fmt.Sprintf("Staged %d messages for deletion.\nBatch ID: %s\nRun 'msgvault delete-staged' to execute.",
		len(m.pendingManifest.GmailIDs), m.pendingManifest.ID)

	// Clear selection
	m.selection.AggregateKeys = make(map[string]bool)
	m.selection.MessageIDs = make(map[int64]bool)
	m.pendingManifest = nil

	return m, nil
}

// HasSelection returns true if any items are selected.
func (m Model) HasSelection() bool {
	return len(m.selection.AggregateKeys) > 0 || len(m.selection.MessageIDs) > 0
}

// SelectionCount returns the number of selected items.
func (m Model) SelectionCount() int {
	return len(m.selection.AggregateKeys) + len(m.selection.MessageIDs)
}

// ensureCursorVisible adjusts scroll offset to keep cursor in view.
func (m *Model) ensureCursorVisible() {
	if m.cursor < m.scrollOffset {
		m.scrollOffset = m.cursor
	}
	if m.cursor >= m.scrollOffset+m.pageSize {
		m.scrollOffset = m.cursor - m.pageSize + 1
	}
}

// View implements tea.Model.
func (m Model) View() string {
	if m.quitting {
		return ""
	}

	if m.width == 0 {
		return "Loading..."
	}

	// If view is frozen (during level transitions), return the cached view
	// to prevent flashing while async data loads complete.
	if m.frozenView != "" {
		return m.frozenView
	}

	return m.renderView()
}

// renderView renders the current view based on the active level.
// Separated from View() so transitions can capture the current output
// before changing state (for the frozenView pattern).
func (m Model) renderView() string {
	switch m.level {
	case levelAggregates, levelSubAggregate:
		return fmt.Sprintf("%s\n%s\n%s",
			m.headerView(),
			m.aggregateTableView(),
			m.footerView(),
		)
	case levelMessageList:
		return fmt.Sprintf("%s\n%s\n%s",
			m.headerView(),
			m.messageListView(),
			m.footerView(),
		)
	case levelMessageDetail:
		return fmt.Sprintf("%s\n%s\n%s",
			m.headerView(),
			m.messageDetailView(),
			m.footerView(),
		)
	case levelThreadView:
		return fmt.Sprintf("%s\n%s\n%s",
			m.headerView(),
			m.threadView(),
			m.footerView(),
		)
	}

	return ""
}

// exportAttachments exports selected attachments to a zip file asynchronously.
func (m Model) exportAttachments() (tea.Model, tea.Cmd) {
	if m.messageDetail == nil || len(m.messageDetail.Attachments) == 0 {
		m.modal = modalNone
		return m.showFlash("No attachments to export")
	}

	// Count selected attachments
	var selectedAttachments []query.AttachmentInfo
	for i, att := range m.messageDetail.Attachments {
		if m.exportSelection[i] {
			selectedAttachments = append(selectedAttachments, att)
		}
	}

	if len(selectedAttachments) == 0 {
		return m.showFlash("No attachments selected")
	}

	// Capture parameters for async export
	attachmentsDir := filepath.Join(m.dataDir, "attachments")
	subject := m.messageDetail.Subject
	if subject == "" {
		subject = "attachments"
	}
	subject = sanitizeFilename(subject)
	if len(subject) > 50 {
		subject = subject[:50]
	}
	zipFilename := fmt.Sprintf("%s_%d.zip", subject, m.messageDetail.ID)

	// Set loading state and close modal
	m.modal = modalNone
	m.loading = true
	m.exportSelection = nil

	// Return command that performs the export asynchronously
	cmd := func() tea.Msg {
		return doExportAttachments(zipFilename, attachmentsDir, selectedAttachments)
	}

	return m, cmd
}

// doExportAttachments performs the actual export work and returns the result message.
func doExportAttachments(zipFilename, attachmentsDir string, attachments []query.AttachmentInfo) exportResultMsg {
	zipFile, err := os.Create(zipFilename)
	if err != nil {
		return exportResultMsg{err: fmt.Errorf("failed to create zip file: %w", err)}
	}
	// Don't defer Close - we need to handle errors and avoid double-close

	zipWriter := zip.NewWriter(zipFile)

	var exportedCount int
	var totalSize int64
	var errors []string
	var writeError bool

	usedNames := make(map[string]int)
	for _, att := range attachments {
		if att.ContentHash == "" {
			errors = append(errors, fmt.Sprintf("%s: missing content hash", att.Filename))
			continue
		}

		storagePath := filepath.Join(attachmentsDir, att.ContentHash[:2], att.ContentHash)

		srcFile, err := os.Open(storagePath)
		if err != nil {
			errors = append(errors, fmt.Sprintf("%s: %v", att.Filename, err))
			continue
		}

		// Use filepath.Base to prevent Zip Slip (path traversal) attacks
		filename := filepath.Base(att.Filename)
		if filename == "" || filename == "." {
			filename = att.ContentHash
		}
		baseKey := filename
		if count, exists := usedNames[baseKey]; exists {
			ext := filepath.Ext(filename)
			base := filename[:len(filename)-len(ext)]
			filename = fmt.Sprintf("%s_%d%s", base, count+1, ext)
			usedNames[baseKey] = count + 1
		} else {
			usedNames[baseKey] = 1
		}

		w, err := zipWriter.Create(filename)
		if err != nil {
			srcFile.Close()
			errors = append(errors, fmt.Sprintf("%s: zip write error: %v", att.Filename, err))
			writeError = true
			continue
		}

		n, err := io.Copy(w, srcFile)
		srcFile.Close()
		if err != nil {
			errors = append(errors, fmt.Sprintf("%s: zip write error: %v", att.Filename, err))
			writeError = true
			continue
		}

		exportedCount++
		totalSize += n
	}

	// Close zip writer first - check for errors as this finalizes the archive
	if err := zipWriter.Close(); err != nil {
		errors = append(errors, fmt.Sprintf("zip finalization error: %v", err))
		writeError = true
	}
	if err := zipFile.Close(); err != nil {
		errors = append(errors, fmt.Sprintf("file close error: %v", err))
		writeError = true
	}

	// Build result message
	if exportedCount == 0 || writeError {
		os.Remove(zipFilename)
		if writeError {
			return exportResultMsg{result: "Export failed due to write errors. Zip file removed.\n\nErrors:\n" + strings.Join(errors, "\n")}
		}
		return exportResultMsg{result: "No attachments exported.\n\nErrors:\n" + strings.Join(errors, "\n")}
	}

	cwd, _ := os.Getwd()
	fullPath := filepath.Join(cwd, zipFilename)
	result := fmt.Sprintf("Exported %d attachment(s) (%s)\n\nSaved to:\n%s",
		exportedCount, formatBytesLong(totalSize), fullPath)
	if len(errors) > 0 {
		result += "\n\nErrors:\n" + strings.Join(errors, "\n")
	}
	return exportResultMsg{result: result}
}

// sanitizeFilename removes or replaces characters that are invalid in filenames.
func sanitizeFilename(s string) string {
	var result []rune
	for _, r := range s {
		switch r {
		case '/', '\\', ':', '*', '?', '"', '<', '>', '|', '\n', '\r', '\t':
			result = append(result, '_')
		default:
			result = append(result, r)
		}
	}
	return string(result)
}


// formatBytesLong formats bytes with full precision for export results.
func formatBytesLong(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}
