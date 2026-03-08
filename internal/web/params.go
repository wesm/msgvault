package web

import (
	"net/http"
	"strconv"

	"github.com/wesm/msgvault/internal/query"
)

func parseViewType(r *http.Request) query.ViewType {
	switch r.URL.Query().Get("view") {
	case "senders":
		return query.ViewSenders
	case "sender_names":
		return query.ViewSenderNames
	case "recipients":
		return query.ViewRecipients
	case "recipient_names":
		return query.ViewRecipientNames
	case "domains":
		return query.ViewDomains
	case "labels":
		return query.ViewLabels
	case "time":
		return query.ViewTime
	default:
		return query.ViewSenders
	}
}

func viewTypeToString(v query.ViewType) string {
	switch v {
	case query.ViewSenders:
		return "senders"
	case query.ViewSenderNames:
		return "sender_names"
	case query.ViewRecipients:
		return "recipients"
	case query.ViewRecipientNames:
		return "recipient_names"
	case query.ViewDomains:
		return "domains"
	case query.ViewLabels:
		return "labels"
	case query.ViewTime:
		return "time"
	default:
		return "senders"
	}
}

func parseSortField(r *http.Request) query.SortField {
	switch r.URL.Query().Get("sort") {
	case "count":
		return query.SortByCount
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

func sortFieldToString(f query.SortField) string {
	switch f {
	case query.SortByCount:
		return "count"
	case query.SortBySize:
		return "size"
	case query.SortByAttachmentSize:
		return "attachments"
	case query.SortByName:
		return "name"
	default:
		return "count"
	}
}

func parseSortDirection(r *http.Request) query.SortDirection {
	if r.URL.Query().Get("dir") == "asc" {
		return query.SortAsc
	}
	return query.SortDesc
}

func sortDirToString(d query.SortDirection) string {
	if d == query.SortAsc {
		return "asc"
	}
	return "desc"
}

func parseTimeGranularity(r *http.Request) query.TimeGranularity {
	switch r.URL.Query().Get("granularity") {
	case "year":
		return query.TimeYear
	case "month":
		return query.TimeMonth
	case "day":
		return query.TimeDay
	default:
		return query.TimeYear
	}
}

func timeGranularityToString(g query.TimeGranularity) string {
	switch g {
	case query.TimeYear:
		return "year"
	case query.TimeMonth:
		return "month"
	case query.TimeDay:
		return "day"
	default:
		return "month"
	}
}

func parseOptionalInt64(r *http.Request, key string) *int64 {
	s := r.URL.Query().Get(key)
	if s == "" {
		return nil
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return nil
	}
	return &v
}

func parseBool(r *http.Request, key string) bool {
	return r.URL.Query().Get(key) == "1"
}

func parseAggregateOptions(r *http.Request) query.AggregateOptions {
	opts := query.DefaultAggregateOptions()
	opts.SortField = parseSortField(r)
	opts.SortDirection = parseSortDirection(r)
	opts.SourceID = parseOptionalInt64(r, "account")
	opts.WithAttachmentsOnly = parseBool(r, "attachments")
	opts.HideDeletedFromSource = parseBool(r, "hide_deleted")
	opts.TimeGranularity = parseTimeGranularity(r)
	opts.Limit = 500
	return opts
}

func parseMessageSortField(r *http.Request) query.MessageSortField {
	switch r.URL.Query().Get("sort") {
	case "date":
		return query.MessageSortByDate
	case "size":
		return query.MessageSortBySize
	case "subject":
		return query.MessageSortBySubject
	default:
		return query.MessageSortByDate
	}
}

func messageSortFieldToString(f query.MessageSortField) string {
	switch f {
	case query.MessageSortByDate:
		return "date"
	case query.MessageSortBySize:
		return "size"
	case query.MessageSortBySubject:
		return "subject"
	default:
		return "date"
	}
}

const defaultPageSize = 100

func parsePage(r *http.Request) int {
	s := r.URL.Query().Get("page")
	if s == "" {
		return 1
	}
	p, err := strconv.Atoi(s)
	if err != nil || p < 1 {
		return 1
	}
	return p
}

// parseBaseMessageFilter extracts the shared filter fields from a request.
// Used by parseMessageFilter, parseDrillFilter, and search handlers.
func parseBaseMessageFilter(r *http.Request) query.MessageFilter {
	q := r.URL.Query()
	f := query.MessageFilter{
		Sender:                q.Get("sender"),
		SenderName:            q.Get("sender_name"),
		Recipient:             q.Get("recipient"),
		RecipientName:         q.Get("recipient_name"),
		Domain:                q.Get("domain"),
		Label:                 q.Get("label"),
		SourceID:              parseOptionalInt64(r, "account"),
		WithAttachmentsOnly:   parseBool(r, "attachments"),
		HideDeletedFromSource: parseBool(r, "hide_deleted"),
	}

	// Handle empty-key filters: when a filter param is present but empty,
	// set EmptyValueTargets so the query engine filters for NULL/empty values.
	emptyTargets := map[string]query.ViewType{
		"sender":         query.ViewSenders,
		"sender_name":    query.ViewSenderNames,
		"recipient":      query.ViewRecipients,
		"recipient_name": query.ViewRecipientNames,
		"domain":         query.ViewDomains,
		"label":          query.ViewLabels,
	}
	for param, viewType := range emptyTargets {
		if _, ok := q[param]; ok && q.Get(param) == "" {
			f.SetEmptyTarget(viewType)
		}
	}

	timePeriod := q.Get("time_period")
	if timePeriod != "" {
		f.TimeRange = query.TimeRange{
			Period:      timePeriod,
			Granularity: parseTimeGranularity(r),
		}
	}

	return f
}

func parseMessageFilter(r *http.Request) query.MessageFilter {
	f := parseBaseMessageFilter(r)
	f.Sorting = query.MessageSorting{
		Field:     parseMessageSortField(r),
		Direction: parseSortDirection(r),
	}

	convID := parseOptionalInt64(r, "conversation")
	if convID != nil {
		f.ConversationID = convID
	}

	page := parsePage(r)
	f.Pagination = query.Pagination{
		Limit:  defaultPageSize,
		Offset: (page - 1) * defaultPageSize,
	}

	return f
}

func parseDrillFilter(r *http.Request) query.MessageFilter {
	return parseBaseMessageFilter(r)
}
