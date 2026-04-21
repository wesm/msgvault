package web

import (
	"bytes"
	"fmt"
	"html"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/wesm/msgvault/internal/query"
	"github.com/wesm/msgvault/internal/search"
	"github.com/wesm/msgvault/internal/web/templates"
)

func (h *Handler) handleDashboard(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	stats, err := h.engine.GetTotalStats(ctx, query.StatsOptions{})
	if err != nil {
		slog.Error("failed to get stats", "error", err)
		http.Error(w, "Failed to load stats", http.StatusInternalServerError)
		return
	}

	accounts, err := h.engine.ListAccounts(ctx)
	if err != nil {
		slog.Error("failed to list accounts", "error", err)
		http.Error(w, "Failed to load accounts", http.StatusInternalServerError)
		return
	}

	data := templates.DashboardData{
		Stats:        stats,
		Accounts:     accounts,
		HasScheduler: h.scheduler != nil,
	}

	var buf bytes.Buffer
	if err := templates.Dashboard(data).Render(ctx, &buf); err != nil {
		slog.Error("failed to render dashboard", "error", err)
		http.Error(w, "Failed to render page", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = buf.WriteTo(w)
}

// resolveSchedulerKey maps an account identifier to the scheduler key,
// checking both bare email (Gmail) and "import:" prefix (Apple Mail).
func (h *Handler) resolveSchedulerKey(identifier string) (string, bool) {
	if h.scheduler == nil {
		return "", false
	}
	if h.scheduler.IsScheduled(identifier) {
		return identifier, true
	}
	key := "import:" + identifier
	if h.scheduler.IsScheduled(key) {
		return key, true
	}
	return "", false
}

func (h *Handler) handleTriggerSync(w http.ResponseWriter, r *http.Request) {
	identifier := chi.URLParam(r, "identifier")
	if identifier == "" {
		http.Error(w, "missing identifier", http.StatusBadRequest)
		return
	}

	key, ok := h.resolveSchedulerKey(identifier)
	if !ok {
		http.Error(w, "account not scheduled", http.StatusNotFound)
		return
	}

	if err := h.scheduler.TriggerSync(key); err != nil {
		slog.Error("failed to trigger sync", "identifier", identifier, "error", err)
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusConflict)
		_, _ = fmt.Fprintf(w, `<span class="sync-error">%s</span>`, html.EscapeString(err.Error()))
		return
	}

	slog.Info("sync triggered via web UI", "identifier", identifier, "key", key)
	h.writeSyncPolling(w, identifier)
}

func (h *Handler) handleSyncStatus(w http.ResponseWriter, r *http.Request) {
	identifier := chi.URLParam(r, "identifier")
	if identifier == "" {
		http.Error(w, "missing identifier", http.StatusBadRequest)
		return
	}

	key, ok := h.resolveSchedulerKey(identifier)
	if !ok {
		http.Error(w, "account not scheduled", http.StatusNotFound)
		return
	}

	// Check if sync is still running.
	running := false
	for _, st := range h.scheduler.Status() {
		if st.Email == key {
			running = st.Running
			break
		}
	}

	if running {
		h.writeSyncPolling(w, identifier)
		return
	}

	// Sync finished — fetch updated timestamp.
	ctx := r.Context()
	syncTime := "just now"
	accounts, err := h.engine.ListAccounts(ctx)
	if err == nil {
		for _, a := range accounts {
			if a.Identifier == identifier {
				syncTime = templates.FormatSyncTime(a.LastSyncWithData)
				break
			}
		}
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = fmt.Fprintf(w,
		`<span class="account-sync">last sync: %s</span>`+
			`<button class="btn-sync" hx-post="/sync/%s" hx-target="#sync-status-%s" hx-swap="innerHTML">sync</button>`,
		html.EscapeString(syncTime), url.PathEscape(identifier), templates.SafeID(identifier))
}

// writeSyncPolling writes an HTML fragment with an animated indicator
// that polls GET /sync/{id}/status every 2s until the sync completes.
func (h *Handler) writeSyncPolling(w http.ResponseWriter, identifier string) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = fmt.Fprintf(w,
		`<span class="sync-running" hx-get="/sync/%s/status" hx-target="#sync-status-%s" hx-swap="innerHTML" hx-trigger="every 2s">`+
			`<span class="sync-spinner"></span> syncing`+
			`</span>`,
		url.PathEscape(identifier), templates.SafeID(identifier))
}

func (h *Handler) handleBrowse(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	viewType := parseViewType(r)
	opts := parseAggregateOptions(r)

	rows, err := h.engine.Aggregate(ctx, viewType, opts)
	if err != nil {
		slog.Error("failed to aggregate", "error", err, "view", viewType)
		http.Error(w, "Failed to load data", http.StatusInternalServerError)
		return
	}

	stats, err := h.engine.GetTotalStats(ctx, query.StatsOptions{
		SourceID:              opts.SourceID,
		WithAttachmentsOnly:   opts.WithAttachmentsOnly,
		HideDeletedFromSource: opts.HideDeletedFromSource,
	})
	if err != nil {
		slog.Error("failed to get stats", "error", err)
	}

	accounts, err := h.engine.ListAccounts(ctx)
	if err != nil {
		slog.Error("failed to list accounts", "error", err)
	}

	data := templates.BrowseData{
		Stats:       stats,
		Rows:        rows,
		ViewType:    viewTypeToString(viewType),
		ViewLabel:   viewType.String(),
		SortField:   sortFieldToString(opts.SortField),
		SortDir:     sortDirToString(opts.SortDirection),
		Granularity: timeGranularityToString(opts.TimeGranularity),
		AccountID:   r.URL.Query().Get("account"),
		Attachments: opts.WithAttachmentsOnly,
		HideDeleted: opts.HideDeletedFromSource,
		Accounts:    accounts,
	}

	var buf bytes.Buffer
	if err := templates.Aggregates(data).Render(ctx, &buf); err != nil {
		slog.Error("failed to render aggregates", "error", err)
		http.Error(w, "Failed to render page", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = buf.WriteTo(w)
}

func (h *Handler) handleDrill(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	viewType := parseViewType(r)
	opts := parseAggregateOptions(r)
	filter := parseDrillFilter(r)

	rows, err := h.engine.SubAggregate(ctx, filter, viewType, opts)
	if err != nil {
		slog.Error("failed to sub-aggregate", "error", err, "view", viewType)
		http.Error(w, "Failed to load data", http.StatusInternalServerError)
		return
	}

	stats, err := h.engine.GetTotalStats(ctx, query.StatsOptions{
		SourceID:              opts.SourceID,
		WithAttachmentsOnly:   opts.WithAttachmentsOnly,
		HideDeletedFromSource: opts.HideDeletedFromSource,
	})
	if err != nil {
		slog.Error("failed to get stats", "error", err)
	}

	// Build drill filters map from current request params (deterministic order)
	drillFilters := make(map[string]string)
	drillKeys := []string{"sender", "sender_name", "recipient", "recipient_name", "domain", "label", "time_period"}
	for _, key := range drillKeys {
		if _, ok := r.URL.Query()[key]; ok {
			drillFilters[key] = r.URL.Query().Get(key)
		}
	}

	// Build breadcrumbs with full state preservation
	browseURL := templates.BrowseData{
		ViewType:    viewTypeToString(viewType),
		SortField:   sortFieldToString(opts.SortField),
		SortDir:     sortDirToString(opts.SortDirection),
		Granularity: timeGranularityToString(opts.TimeGranularity),
		AccountID:   r.URL.Query().Get("account"),
		Attachments: opts.WithAttachmentsOnly,
		HideDeleted: opts.HideDeletedFromSource,
	}
	breadcrumbs := []templates.Breadcrumb{
		{Label: "Browse", URL: browseURL.ViewTabURL(viewTypeToString(viewType))},
	}
	for _, key := range drillKeys {
		if v, ok := drillFilters[key]; ok {
			label := key + ": " + v
			if v == "" {
				label = key + ": (empty)"
			}
			breadcrumbs = append(breadcrumbs, templates.Breadcrumb{Label: label})
		}
	}

	accounts, err := h.engine.ListAccounts(ctx)
	if err != nil {
		slog.Error("failed to list accounts", "error", err)
	}

	data := templates.BrowseData{
		Stats:        stats,
		Rows:         rows,
		ViewType:     viewTypeToString(viewType),
		ViewLabel:    viewType.String(),
		SortField:    sortFieldToString(opts.SortField),
		SortDir:      sortDirToString(opts.SortDirection),
		Granularity:  timeGranularityToString(opts.TimeGranularity),
		AccountID:    r.URL.Query().Get("account"),
		Attachments:  opts.WithAttachmentsOnly,
		HideDeleted:  opts.HideDeletedFromSource,
		Accounts:     accounts,
		DrillFilters: drillFilters,
		Breadcrumbs:  breadcrumbs,
	}

	var buf bytes.Buffer
	if err := templates.Aggregates(data).Render(ctx, &buf); err != nil {
		slog.Error("failed to render drill-down", "error", err)
		http.Error(w, "Failed to render page", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = buf.WriteTo(w)
}

func (h *Handler) handleMessages(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	filter := parseMessageFilter(r)
	page := parsePage(r)

	// Fetch one extra row to detect if there are more pages
	pageSize := filter.Pagination.Limit
	filter.Pagination.Limit = pageSize + 1

	messages, err := h.engine.ListMessages(ctx, filter)
	if err != nil {
		slog.Error("failed to list messages", "error", err)
		http.Error(w, "Failed to load messages", http.StatusInternalServerError)
		return
	}

	hasMore := len(messages) > pageSize
	if hasMore {
		messages = messages[:pageSize]
	}

	// Build filter map for template URL construction
	filters := make(map[string]string)
	filterKeys := []string{"sender", "sender_name", "recipient", "recipient_name", "domain", "label", "time_period", "granularity", "conversation"}
	for _, key := range filterKeys {
		if _, ok := r.URL.Query()[key]; ok {
			filters[key] = r.URL.Query().Get(key)
		}
	}

	data := templates.MessagesData{
		Messages:    messages,
		Page:        page,
		PageSize:    pageSize,
		HasMore:     hasMore,
		SortField:   messageSortFieldToString(filter.Sorting.Field),
		SortDir:     sortDirToString(filter.Sorting.Direction),
		Filters:     filters,
		AccountID:   r.URL.Query().Get("account"),
		Attachments: filter.WithAttachmentsOnly,
		HideDeleted: filter.HideDeletedFromSource,
	}

	var buf bytes.Buffer
	if err := templates.Messages(data).Render(ctx, &buf); err != nil {
		slog.Error("failed to render messages", "error", err)
		http.Error(w, "Failed to render page", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = buf.WriteTo(w)
}

func (h *Handler) handleMessageDetail(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	idStr := chi.URLParam(r, "id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		http.Error(w, "Invalid message ID", http.StatusBadRequest)
		return
	}

	msg, err := h.engine.GetMessage(ctx, id)
	if err != nil {
		slog.Error("failed to get message", "error", err, "id", id)
		http.Error(w, "Failed to load message", http.StatusInternalServerError)
		return
	}
	if msg == nil {
		http.Error(w, "Message not found", http.StatusNotFound)
		return
	}

	// Build back URL from referer, restricted to same-origin paths only.
	// Only allow relative paths (starting with /) or same-host URLs to prevent
	// javascript: URI injection via templ.SafeURL.
	// Skip referers that point to another message detail page (e.g. from prev/next
	// navigation) — those would make "Back to messages" loop between messages.
	backURL := "/messages"
	if ref := r.Header.Get("Referer"); ref != "" {
		if u, err := url.Parse(ref); err == nil {
			var refPath string
			if u.Scheme == "" && u.Host == "" && strings.HasPrefix(u.Path, "/") {
				refPath = u.Path
			} else if u.Host == r.Host && (u.Scheme == "http" || u.Scheme == "https") {
				refPath = u.Path
			}
			// Only use referer if it's not another message detail page
			if refPath != "" && !strings.HasPrefix(refPath, "/messages/") {
				backURL = u.RequestURI()
			}
		}
	}

	data := templates.MessageDetailData{
		Message: msg,
		BackURL: backURL,
	}

	var buf bytes.Buffer
	if err := templates.MessageDetailPage(data).Render(ctx, &buf); err != nil {
		slog.Error("failed to render message detail", "error", err)
		http.Error(w, "Failed to render page", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = buf.WriteTo(w)
}

func (h *Handler) handleSearch(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	queryStr := r.URL.Query().Get("q")
	mode := r.URL.Query().Get("mode")
	if mode != "deep" {
		mode = "fast"
	}
	page := parsePage(r)
	pageSize := defaultPageSize
	hideDeleted := parseBool(r, "hide_deleted")
	attachments := parseBool(r, "attachments")

	sortField := parseMessageSortField(r)
	sortDir := parseSortDirection(r)

	data := templates.SearchData{
		Query:       queryStr,
		Mode:        mode,
		Page:        page,
		PageSize:    pageSize,
		HideDeleted: hideDeleted,
		Attachments: attachments,
		SortField:   messageSortFieldToString(sortField),
		SortDir:     sortDirToString(sortDir),
	}

	if queryStr != "" {
		parsed := search.Parse(queryStr)
		// Apply hide_deleted from the search parser too
		if hideDeleted {
			parsed.HideDeleted = true
		}
		if attachments {
			t := true
			parsed.HasAttachment = &t
		}
		offset := (page - 1) * pageSize

		filter := query.MessageFilter{
			HideDeletedFromSource: hideDeleted,
			WithAttachmentsOnly:   attachments,
			Sorting: query.MessageSorting{
				Field:     sortField,
				Direction: sortDir,
			},
		}

		var messages []query.MessageSummary
		var err error

		if mode == "deep" {
			messages, err = h.engine.Search(ctx, parsed, pageSize+1, offset)
		} else {
			result, searchErr := h.engine.SearchFastWithStats(
				ctx, parsed, queryStr, filter,
				query.ViewSenders, pageSize+1, offset,
			)
			if searchErr == nil {
				messages = result.Messages
				data.Stats = result.Stats
			}
			err = searchErr
		}

		if err != nil {
			slog.Error("search failed", "error", err, "query", queryStr, "mode", mode)
			http.Error(w, fmt.Sprintf("Search failed: %v", err), http.StatusInternalServerError)
			return
		}

		if len(messages) > pageSize {
			data.HasMore = true
			messages = messages[:pageSize]
		}
		data.Messages = messages
	} else {
		// No query: show 100 most recent messages across all accounts
		recentFilter := query.MessageFilter{
			Sorting: query.MessageSorting{
				Field:     query.MessageSortByDate,
				Direction: query.SortDesc,
			},
			Pagination: query.Pagination{
				Limit: 100,
			},
		}
		recent, err := h.engine.ListMessages(ctx, recentFilter)
		if err != nil {
			slog.Error("failed to fetch recent messages", "error", err)
		} else {
			data.Messages = recent
			data.ShowRecent = true
		}
	}

	// Ensure stats bar is always shown (deep search doesn't return stats)
	if data.Stats == nil {
		stats, statsErr := h.engine.GetTotalStats(ctx, query.StatsOptions{
			HideDeletedFromSource: hideDeleted,
			WithAttachmentsOnly:   attachments,
		})
		if statsErr != nil {
			slog.Error("failed to get stats for search page", "error", statsErr)
		} else {
			data.Stats = stats
		}
	}

	var buf bytes.Buffer
	if err := templates.Search(data).Render(ctx, &buf); err != nil {
		slog.Error("failed to render search", "error", err)
		http.Error(w, "Failed to render page", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = buf.WriteTo(w)
}

// validContentHash matches a SHA-256 hex string (64 lowercase hex chars).
var validContentHash = regexp.MustCompile(`^[0-9a-f]{64}$`)

func (h *Handler) handleAttachmentDownload(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	idStr := chi.URLParam(r, "id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		http.Error(w, "Invalid attachment ID", http.StatusBadRequest)
		return
	}

	att, err := h.engine.GetAttachment(ctx, id)
	if err != nil {
		slog.Error("failed to get attachment", "error", err, "id", id)
		http.Error(w, "Failed to load attachment", http.StatusInternalServerError)
		return
	}
	if att == nil {
		http.Error(w, "Attachment not found", http.StatusNotFound)
		return
	}

	if att.ContentHash == "" || !validContentHash.MatchString(att.ContentHash) {
		http.Error(w, "Attachment not available for download", http.StatusNotFound)
		return
	}

	if h.attachmentsDir == "" {
		http.Error(w, "Attachment storage not configured", http.StatusServiceUnavailable)
		return
	}

	filePath := filepath.Join(h.attachmentsDir, att.ContentHash[:2], att.ContentHash)

	f, err := os.Open(filePath)
	if err != nil {
		slog.Error("failed to open attachment file", "error", err, "path", filePath)
		http.Error(w, "Attachment file not found", http.StatusNotFound)
		return
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		http.Error(w, "Failed to read attachment", http.StatusInternalServerError)
		return
	}

	filename := sanitizeFilename(att.Filename)
	if filename == "" {
		filename = "attachment"
	}

	// Determine content type: use stored MIME type if valid, otherwise
	// fall back to application/octet-stream. Never let the browser sniff.
	contentType := "application/octet-stream"
	if att.MimeType != "" && isValidMimeType(att.MimeType) {
		contentType = att.MimeType
	}

	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, filename))
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("X-Content-Type-Options", "nosniff")

	http.ServeContent(w, r, "", fi.ModTime(), f)
}

// isValidMimeType checks that a MIME type string is safe to use as a
// Content-Type header value (no control chars, reasonable format).
func isValidMimeType(mt string) bool {
	for _, c := range mt {
		if c < 0x20 || c == 0x7f {
			return false
		}
	}
	return strings.Contains(mt, "/")
}

// sanitizeFilename removes path separators, quotes, and control characters
// from a filename for use in Content-Disposition headers.
func sanitizeFilename(name string) string {
	var b strings.Builder
	b.Grow(len(name))
	for _, c := range name {
		switch {
		case c < 0x20 || c == 0x7f: // control characters
			continue
		case c == '/' || c == '\\':
			b.WriteRune('_')
		case c == '"':
			b.WriteRune('\'')
		default:
			b.WriteRune(c)
		}
	}
	return b.String()
}
