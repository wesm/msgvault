package api

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/wesm/msgvault/internal/query"
	"github.com/wesm/msgvault/internal/search"
)

// AggregateRowResponse is the JSON representation of a query.AggregateRow.
type AggregateRowResponse struct {
	Key             string `json:"key"`
	Count           int64  `json:"count"`
	TotalSize       int64  `json:"total_size_bytes"`
	AttachmentSize  int64  `json:"attachment_size_bytes"`
	AttachmentCount int64  `json:"attachment_count"`
}

// AggregateResponse is the response for GET /api/v1/aggregate.
type AggregateResponse struct {
	GroupBy     string                 `json:"group_by"`
	TotalUnique int64                  `json:"total_unique"`
	Rows        []AggregateRowResponse `json:"rows"`
}

// EngineMsgSummary is the JSON representation of a query.MessageSummary.
type EngineMsgSummary struct {
	ID              int64    `json:"id"`
	Subject         string   `json:"subject"`
	From            string   `json:"from"`
	SentAt          string   `json:"sent_at"`
	Snippet         string   `json:"snippet"`
	Labels          []string `json:"labels"`
	HasAttachments  bool     `json:"has_attachments"`
	SizeBytes       int64    `json:"size_bytes"`
	AttachmentCount int      `json:"attachment_count"`
}

// handleAggregate returns grouped aggregate stats using the query engine.
//
//	GET /api/v1/aggregate
//	  ?group_by=sender|sender_names|recipients|recipient_names|domain|label|time
//	  &granularity=year|month|day   (time view only; default year)
//	  &sort=count|size|attachments|name
//	  &dir=asc|desc
//	  &limit=N                       (default 100)
//	  &q=search_query
//	  &after=YYYY-MM-DD
//	  &before=YYYY-MM-DD
//	  &attachments_only=true
//	  &hide_deleted=true
func (s *Server) handleAggregate(w http.ResponseWriter, r *http.Request) {
	if s.engine == nil {
		writeError(w, http.StatusServiceUnavailable, "engine_unavailable", "Query engine not available — start msgvault with 'serve' command")
		return
	}

	q := r.URL.Query()

	viewType, ok := parseViewType(q.Get("group_by"))
	if !ok {
		writeError(w, http.StatusBadRequest, "invalid_group_by",
			"group_by must be one of: sender, sender_names, recipients, recipient_names, domain, label, time")
		return
	}

	opts := query.DefaultAggregateOptions()

	if v := q.Get("sort"); v != "" {
		opts.SortField = parseSortField(v)
	}
	if q.Get("dir") == "asc" {
		opts.SortDirection = query.SortAsc
	}
	if v := q.Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			opts.Limit = n
		}
	}
	if v := q.Get("granularity"); v != "" {
		opts.TimeGranularity = parseGranularity(v)
	}
	if q.Get("attachments_only") == "true" {
		opts.WithAttachmentsOnly = true
	}
	if q.Get("hide_deleted") == "true" {
		opts.HideDeletedFromSource = true
	}
	opts.SearchQuery = q.Get("q")

	if v := q.Get("after"); v != "" {
		if t, err := time.Parse("2006-01-02", v); err == nil {
			opts.After = &t
		}
	}
	if v := q.Get("before"); v != "" {
		if t, err := time.Parse("2006-01-02", v); err == nil {
			opts.Before = &t
		}
	}

	rows, err := s.engine.Aggregate(r.Context(), viewType, opts)
	if err != nil {
		s.logger.Error("aggregate query failed", "group_by", q.Get("group_by"), "error", err)
		writeError(w, http.StatusInternalServerError, "query_error", "Aggregate query failed")
		return
	}

	var totalUnique int64
	apiRows := make([]AggregateRowResponse, len(rows))
	for i, row := range rows {
		apiRows[i] = AggregateRowResponse{
			Key:             row.Key,
			Count:           row.Count,
			TotalSize:       row.TotalSize,
			AttachmentSize:  row.AttachmentSize,
			AttachmentCount: row.AttachmentCount,
		}
		if i == 0 {
			totalUnique = row.TotalUnique
		}
	}

	writeJSON(w, http.StatusOK, AggregateResponse{
		GroupBy:     q.Get("group_by"),
		TotalUnique: totalUnique,
		Rows:        apiRows,
	})
}

// handleEngineMessages returns a filtered, paginated list of messages.
//
//	GET /api/v1/engine/messages
//	  ?sender=email&sender_name=Name&domain=example.com&label=INBOX&recipient=email&recipient_name=Name
//	  &after=YYYY-MM-DD&before=YYYY-MM-DD
//	  &period=YYYY|YYYY-MM|YYYY-MM-DD  (auto-detects granularity from string length)
//	  &attachments_only=true&hide_deleted=true
//	  &page=1&page_size=50
//	  &sort=date|size|subject&dir=asc|desc
func (s *Server) handleEngineMessages(w http.ResponseWriter, r *http.Request) {
	if s.engine == nil {
		writeError(w, http.StatusServiceUnavailable, "engine_unavailable", "Query engine not available")
		return
	}

	q := r.URL.Query()

	page, _ := strconv.Atoi(q.Get("page"))
	if page < 1 {
		page = 1
	}
	pageSize, _ := strconv.Atoi(q.Get("page_size"))
	if pageSize < 1 || pageSize > 200 {
		pageSize = 50
	}

	filter := query.MessageFilter{
		Sender:        q.Get("sender"),
		SenderName:    q.Get("sender_name"),
		Domain:        q.Get("domain"),
		Label:         q.Get("label"),
		Recipient:     q.Get("recipient"),
		RecipientName: q.Get("recipient_name"),
		Pagination: query.Pagination{
			Limit:  pageSize,
			Offset: (page - 1) * pageSize,
		},
		Sorting: query.MessageSorting{
			Field:     parseMessageSortField(q.Get("sort")),
			Direction: parseSortDirection(q.Get("dir")),
		},
	}

	if q.Get("attachments_only") == "true" {
		filter.WithAttachmentsOnly = true
	}
	if v := q.Get("file_type"); v != "" {
		filter.MimeCategory = v
		filter.WithAttachmentsOnly = true // implicit
	}
	if q.Get("hide_deleted") == "true" {
		filter.HideDeletedFromSource = true
	}
	if v := q.Get("period"); v != "" {
		filter.TimeRange = query.TimeRange{Period: v}
	} else {
		if v := q.Get("after"); v != "" {
			if t, err := time.Parse("2006-01-02", v); err == nil {
				filter.After = &t
			}
		}
		if v := q.Get("before"); v != "" {
			if t, err := time.Parse("2006-01-02", v); err == nil {
				filter.Before = &t
			}
		}
	}

	msgs, err := s.engine.ListMessages(r.Context(), filter)
	if err != nil {
		s.logger.Error("engine list messages failed", "error", err)
		writeError(w, http.StatusInternalServerError, "query_error", "Message query failed")
		return
	}

	summaries := make([]EngineMsgSummary, len(msgs))
	for i, m := range msgs {
		summaries[i] = toEngineMsgSummary(m)
	}

	// Count total matching messages for pagination (ignores Limit/Offset).
	total, _ := s.engine.CountMessages(r.Context(), filter)

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"page":      page,
		"page_size": pageSize,
		"total":     total,
		"messages":  summaries,
	})
}

// handleEngineSearch searches messages using the engine (Gmail-style query syntax).
//
//	GET /api/v1/engine/search?q=from:foo subject:bar&page=1&page_size=50
func (s *Server) handleEngineSearch(w http.ResponseWriter, r *http.Request) {
	if s.engine == nil {
		writeError(w, http.StatusServiceUnavailable, "engine_unavailable", "Query engine not available")
		return
	}

	queryStr := r.URL.Query().Get("q")
	if queryStr == "" {
		writeError(w, http.StatusBadRequest, "missing_query", "Query parameter 'q' is required")
		return
	}

	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	if page < 1 {
		page = 1
	}
	pageSize, _ := strconv.Atoi(r.URL.Query().Get("page_size"))
	if pageSize < 1 || pageSize > 200 {
		pageSize = 50
	}

	parsedQ := search.Parse(queryStr)
	filter := query.MessageFilter{}
	limit := pageSize
	offset := (page - 1) * pageSize

	result, err := s.engine.SearchFastWithStats(r.Context(), parsedQ, queryStr, filter, query.ViewSenders, limit, offset)
	if err != nil {
		s.logger.Error("engine search failed", "q", queryStr, "error", err)
		writeError(w, http.StatusInternalServerError, "search_error", "Search failed")
		return
	}

	summaries := make([]EngineMsgSummary, len(result.Messages))
	for i, m := range result.Messages {
		summaries[i] = toEngineMsgSummary(m)
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"query":     queryStr,
		"total":     result.TotalCount,
		"page":      page,
		"page_size": pageSize,
		"messages":  summaries,
	})
}

// toEngineMsgSummary converts a query.MessageSummary to the API response type.
func toEngineMsgSummary(m query.MessageSummary) EngineMsgSummary {
	from := m.FromEmail
	if m.FromName != "" {
		from = m.FromName + " <" + m.FromEmail + ">"
	}
	labels := m.Labels
	if labels == nil {
		labels = []string{}
	}
	return EngineMsgSummary{
		ID:              m.ID,
		Subject:         m.Subject,
		From:            from,
		SentAt:          m.SentAt.UTC().Format(time.RFC3339),
		Snippet:         m.Snippet,
		Labels:          labels,
		HasAttachments:  m.HasAttachments,
		SizeBytes:       m.SizeEstimate,
		AttachmentCount: m.AttachmentCount,
	}
}

// parseViewType maps a group_by string to query.ViewType.
func parseViewType(s string) (query.ViewType, bool) {
	switch strings.ToLower(s) {
	case "sender", "senders":
		return query.ViewSenders, true
	case "sender_names":
		return query.ViewSenderNames, true
	case "recipients", "recipient":
		return query.ViewRecipients, true
	case "recipient_names":
		return query.ViewRecipientNames, true
	case "domain", "domains":
		return query.ViewDomains, true
	case "label", "labels":
		return query.ViewLabels, true
	case "time":
		return query.ViewTime, true
	default:
		return 0, false
	}
}

func parseSortField(s string) query.SortField {
	switch strings.ToLower(s) {
	case "size":
		return query.SortBySize
	case "attachments":
		return query.SortByAttachmentSize
	case "name":
		return query.SortByName
	default:
		return query.SortByCount
	}
}

func parseGranularity(s string) query.TimeGranularity {
	switch strings.ToLower(s) {
	case "month":
		return query.TimeMonth
	case "day":
		return query.TimeDay
	default:
		return query.TimeYear
	}
}

func parseMessageSortField(s string) query.MessageSortField {
	switch strings.ToLower(s) {
	case "size":
		return query.MessageSortBySize
	case "subject":
		return query.MessageSortBySubject
	default:
		return query.MessageSortByDate
	}
}

func parseSortDirection(s string) query.SortDirection {
	if strings.ToLower(s) == "asc" {
		return query.SortAsc
	}
	return query.SortDesc
}
