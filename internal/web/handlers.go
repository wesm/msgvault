package web

import (
	"bytes"
	"log/slog"
	"net/http"

	"github.com/wesm/msgvault/internal/query"
	"github.com/wesm/msgvault/internal/web/templates"
)

func (h *Handler) handlePlaceholder(title, page string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var buf bytes.Buffer
		if err := templates.Placeholder(title, page).Render(r.Context(), &buf); err != nil {
			slog.Error("failed to render placeholder", "error", err)
			http.Error(w, "Failed to render page", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = buf.WriteTo(w)
	}
}

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
		Stats:    stats,
		Accounts: accounts,
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
