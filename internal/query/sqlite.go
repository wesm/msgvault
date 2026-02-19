package query

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/wesm/msgvault/internal/search"
)

// SQLiteEngine implements Engine using direct SQLite queries.
type SQLiteEngine struct {
	db *sql.DB

	// FTS availability cache - thread-safe with mutex.
	// Only caches successful checks; errors cause retries on next call.
	ftsMu      sync.Mutex
	ftsResult  bool
	ftsChecked bool
}

// NewSQLiteEngine creates a new SQLite-backed query engine.
func NewSQLiteEngine(db *sql.DB) *SQLiteEngine {
	return &SQLiteEngine{db: db}
}

// hasFTSTable checks if the messages_fts table exists.
// Result is cached after first successful check. Errors cause retries on next call.
// Thread-safe via mutex.
func (e *SQLiteEngine) hasFTSTable(ctx context.Context) bool {
	e.ftsMu.Lock()
	defer e.ftsMu.Unlock()

	// Fast path: already successfully checked
	if e.ftsChecked {
		return e.ftsResult
	}

	var count int
	err := e.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM sqlite_master
		WHERE type='table' AND name='messages_fts'
	`).Scan(&count)

	if err != nil {
		// On error (canceled context, temporary DB issue), return false
		// but don't cache so next call can retry
		return false
	}

	// Cache successful result
	e.ftsResult = count > 0
	e.ftsChecked = true
	return e.ftsResult
}

// Close is a no-op for SQLiteEngine since it doesn't own the connection.
func (e *SQLiteEngine) Close() error {
	return nil
}

// escapeSQLiteLike escapes LIKE wildcard characters (%, _, \) with \.
func escapeSQLiteLike(s string) string {
	r := strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)
	return r.Replace(s)
}

// aggDimension describes the variable parts of an aggregate query for a given ViewType.
type aggDimension struct {
	keyExpr   string // SQL expression for the grouping key
	joins     string // JOIN clauses for the dimension table(s)
	whereExpr string // additional WHERE condition (e.g., key IS NOT NULL)
}

// aggDimensionForView returns the SQL dimension definition for a given ViewType.
func aggDimensionForView(view ViewType, timeGranularity TimeGranularity) (aggDimension, error) {
	switch view {
	case ViewSenders:
		return aggDimension{
			keyExpr: "p.email_address",
			joins: `JOIN message_recipients mr ON mr.message_id = m.id AND mr.recipient_type = 'from'
				JOIN participants p ON p.id = mr.participant_id`,
			whereExpr: "p.email_address IS NOT NULL",
		}, nil
	case ViewSenderNames:
		return aggDimension{
			keyExpr: "COALESCE(NULLIF(TRIM(p.display_name), ''), p.email_address)",
			joins: `JOIN message_recipients mr ON mr.message_id = m.id AND mr.recipient_type = 'from'
				JOIN participants p ON p.id = mr.participant_id`,
			whereExpr: "COALESCE(NULLIF(TRIM(p.display_name), ''), p.email_address) IS NOT NULL",
		}, nil
	case ViewRecipients:
		return aggDimension{
			keyExpr: "p.email_address",
			joins: `JOIN message_recipients mr ON mr.message_id = m.id AND mr.recipient_type IN ('to', 'cc', 'bcc')
				JOIN participants p ON p.id = mr.participant_id`,
			whereExpr: "p.email_address IS NOT NULL",
		}, nil
	case ViewRecipientNames:
		return aggDimension{
			keyExpr: "COALESCE(NULLIF(TRIM(p.display_name), ''), p.email_address)",
			joins: `JOIN message_recipients mr ON mr.message_id = m.id AND mr.recipient_type IN ('to', 'cc', 'bcc')
				JOIN participants p ON p.id = mr.participant_id`,
			whereExpr: "COALESCE(NULLIF(TRIM(p.display_name), ''), p.email_address) IS NOT NULL",
		}, nil
	case ViewDomains:
		return aggDimension{
			keyExpr: "p.domain",
			joins: `JOIN message_recipients mr ON mr.message_id = m.id AND mr.recipient_type = 'from'
				JOIN participants p ON p.id = mr.participant_id`,
			whereExpr: "p.domain IS NOT NULL AND p.domain != ''",
		}, nil
	case ViewLabels:
		return aggDimension{
			keyExpr: "l.name",
			joins: `JOIN message_labels ml ON ml.message_id = m.id
				JOIN labels l ON l.id = ml.label_id`,
			whereExpr: "",
		}, nil
	case ViewTime:
		var timeExpr string
		switch timeGranularity {
		case TimeYear:
			timeExpr = "strftime('%Y', m.sent_at)"
		case TimeMonth:
			timeExpr = "strftime('%Y-%m', m.sent_at)"
		case TimeDay:
			timeExpr = "strftime('%Y-%m-%d', m.sent_at)"
		default:
			return aggDimension{}, fmt.Errorf("unsupported time granularity: %d", timeGranularity)
		}
		return aggDimension{
			keyExpr:   timeExpr,
			joins:     "",
			whereExpr: "m.sent_at IS NOT NULL",
		}, nil
	default:
		return aggDimension{}, fmt.Errorf("unsupported view type: %v", view)
	}
}

// buildAggregateSQL builds a complete aggregate query from a dimension and filter parts.
func buildAggregateSQL(dim aggDimension, filterJoins string, filterWhere string, sort string) string {
	allJoins := dim.joins
	if filterJoins != "" {
		allJoins += "\n" + filterJoins
	}

	allWhere := filterWhere
	if dim.whereExpr != "" {
		allWhere += " AND " + dim.whereExpr
	}

	return fmt.Sprintf(`
		SELECT key, count, total_size, attachment_size, attachment_count, total_unique
		FROM (
			SELECT
				%s as key,
				COUNT(*) as count,
				COALESCE(SUM(m.size_estimate), 0) as total_size,
				COALESCE(SUM(att.att_size), 0) as attachment_size,
				COALESCE(SUM(att.att_count), 0) as attachment_count,
				COUNT(*) OVER() as total_unique
			FROM messages m
			%s
			LEFT JOIN (
				SELECT message_id, SUM(size) as att_size, COUNT(*) as att_count
				FROM attachments
				GROUP BY message_id
			) att ON att.message_id = m.id
			WHERE %s
			GROUP BY key
		)
		%s
		LIMIT ?
	`, dim.keyExpr, allJoins, allWhere, sort)
}

// optsToFilterConditions converts AggregateOptions into WHERE conditions and args.
func optsToFilterConditions(opts AggregateOptions, prefix string) ([]string, []interface{}) {
	var conditions []string
	var args []interface{}

	if opts.SourceID != nil {
		conditions = append(conditions, prefix+"source_id = ?")
		args = append(args, *opts.SourceID)
	}
	if opts.After != nil {
		conditions = append(conditions, prefix+"sent_at >= ?")
		args = append(args, opts.After.Format("2006-01-02 15:04:05"))
	}
	if opts.Before != nil {
		conditions = append(conditions, prefix+"sent_at < ?")
		args = append(args, opts.Before.Format("2006-01-02 15:04:05"))
	}
	if opts.WithAttachmentsOnly {
		conditions = append(conditions, prefix+"has_attachments = 1")
	}
	if opts.HideDeletedFromSource {
		conditions = append(conditions, prefix+"deleted_from_source_at IS NULL")
	}

	return conditions, args
}

// sortClause returns ORDER BY clause for aggregates.
// Always includes a secondary sort by key to ensure deterministic ordering when
// primary sort values are equal (e.g., two labels with the same count).
// Returns an error if the SortField is not a valid enum value.
func sortClause(opts AggregateOptions) (string, error) {
	var field string
	switch opts.SortField {
	case SortByCount:
		field = "count"
	case SortBySize:
		field = "total_size"
	case SortByAttachmentSize:
		field = "attachment_size"
	case SortByName:
		field = "key"
	default:
		return "", fmt.Errorf("unsupported sort field: %d", opts.SortField)
	}

	dir := "DESC"
	if opts.SortDirection == SortAsc {
		dir = "ASC"
	}

	// Secondary sort by key ensures deterministic ordering for ties
	if field == "key" {
		return fmt.Sprintf("ORDER BY %s %s", field, dir), nil
	}
	return fmt.Sprintf("ORDER BY %s %s, key ASC", field, dir), nil
}

// buildFilterJoinsAndConditions builds JOIN and WHERE clauses from a MessageFilter.
// Returns joinClauses (already joined by \n), conditions (slice), and args.
// This is used for SubAggregate to apply drill-down filters before sub-grouping.
func buildFilterJoinsAndConditions(filter MessageFilter, tableAlias string) (string, []string, []interface{}) {
	var joins []string
	var conditions []string
	var args []interface{}

	prefix := ""
	if tableAlias != "" {
		prefix = tableAlias + "."
	}

	// Include all messages (deleted messages shown with indicator in TUI)

	if filter.SourceID != nil {
		conditions = append(conditions, prefix+"source_id = ?")
		args = append(args, *filter.SourceID)
	}

	if filter.ConversationID != nil {
		conditions = append(conditions, prefix+"conversation_id = ?")
		args = append(args, *filter.ConversationID)
	}

	if filter.After != nil {
		conditions = append(conditions, prefix+"sent_at >= ?")
		args = append(args, filter.After.Format("2006-01-02 15:04:05"))
	}

	if filter.Before != nil {
		conditions = append(conditions, prefix+"sent_at < ?")
		args = append(args, filter.Before.Format("2006-01-02 15:04:05"))
	}

	if filter.WithAttachmentsOnly {
		conditions = append(conditions, prefix+"has_attachments = 1")
	}
	if filter.HideDeletedFromSource {
		conditions = append(conditions, prefix+"deleted_from_source_at IS NULL")
	}

	// Sender filter
	if filter.Sender != "" {
		joins = append(joins, `
			JOIN message_recipients mr_filter_from ON mr_filter_from.message_id = m.id AND mr_filter_from.recipient_type = 'from'
			JOIN participants p_filter_from ON p_filter_from.id = mr_filter_from.participant_id
		`)
		conditions = append(conditions, "p_filter_from.email_address = ?")
		args = append(args, filter.Sender)
	} else if filter.MatchesEmpty(ViewSenders) {
		joins = append(joins, `
			LEFT JOIN message_recipients mr_filter_from ON mr_filter_from.message_id = m.id AND mr_filter_from.recipient_type = 'from'
			LEFT JOIN participants p_filter_from ON p_filter_from.id = mr_filter_from.participant_id
		`)
		conditions = append(conditions, "(mr_filter_from.id IS NULL OR p_filter_from.email_address IS NULL OR p_filter_from.email_address = '')")
	}

	// Sender name filter
	if filter.SenderName != "" {
		if filter.Sender == "" && !filter.MatchesEmpty(ViewSenders) {
			joins = append(joins, `
				JOIN message_recipients mr_filter_from ON mr_filter_from.message_id = m.id AND mr_filter_from.recipient_type = 'from'
				JOIN participants p_filter_from ON p_filter_from.id = mr_filter_from.participant_id
			`)
		}
		conditions = append(conditions, "COALESCE(NULLIF(TRIM(p_filter_from.display_name), ''), p_filter_from.email_address) = ?")
		args = append(args, filter.SenderName)
	} else if filter.MatchesEmpty(ViewSenderNames) {
		conditions = append(conditions, `NOT EXISTS (
			SELECT 1 FROM message_recipients mr_sn
			JOIN participants p_sn ON p_sn.id = mr_sn.participant_id
			WHERE mr_sn.message_id = m.id
			  AND mr_sn.recipient_type = 'from'
			  AND COALESCE(NULLIF(TRIM(p_sn.display_name), ''), p_sn.email_address) IS NOT NULL
		)`)
	}

	// Recipient filter
	if filter.Recipient != "" {
		joins = append(joins, `
			JOIN message_recipients mr_filter_to ON mr_filter_to.message_id = m.id AND mr_filter_to.recipient_type IN ('to', 'cc', 'bcc')
			JOIN participants p_filter_to ON p_filter_to.id = mr_filter_to.participant_id
		`)
		conditions = append(conditions, "p_filter_to.email_address = ?")
		args = append(args, filter.Recipient)
	} else if filter.MatchesEmpty(ViewRecipients) {
		joins = append(joins, `
			LEFT JOIN message_recipients mr_filter_to ON mr_filter_to.message_id = m.id AND mr_filter_to.recipient_type IN ('to', 'cc', 'bcc')
		`)
		conditions = append(conditions, "mr_filter_to.id IS NULL")
	}

	// Recipient name filter — reuses the Recipient filter's join when present,
	// ensuring both predicates apply to the same participant row.
	if filter.RecipientName != "" {
		if filter.Recipient == "" && filter.MatchesEmpty(ViewRecipients) {
			// MatchEmptyRecipient LEFT JOINs mr without participants — add
			// the participants join so the p_filter_to alias is available.
			// (This combination is contradictory and will return 0 rows.)
			joins = append(joins, `
				JOIN participants p_filter_to ON p_filter_to.id = mr_filter_to.participant_id
			`)
		} else if filter.Recipient == "" && !filter.MatchesEmpty(ViewRecipients) {
			joins = append(joins, `
				JOIN message_recipients mr_filter_to ON mr_filter_to.message_id = m.id AND mr_filter_to.recipient_type IN ('to', 'cc', 'bcc')
				JOIN participants p_filter_to ON p_filter_to.id = mr_filter_to.participant_id
			`)
		}
		conditions = append(conditions, "COALESCE(NULLIF(TRIM(p_filter_to.display_name), ''), p_filter_to.email_address) = ?")
		args = append(args, filter.RecipientName)
	} else if filter.MatchesEmpty(ViewRecipientNames) {
		conditions = append(conditions, `NOT EXISTS (
			SELECT 1 FROM message_recipients mr_rn
			JOIN participants p_rn ON p_rn.id = mr_rn.participant_id
			WHERE mr_rn.message_id = m.id
			  AND mr_rn.recipient_type IN ('to', 'cc', 'bcc')
			  AND COALESCE(NULLIF(TRIM(p_rn.display_name), ''), p_rn.email_address) IS NOT NULL
		)`)
	}

	// Domain filter
	// Note: MatchEmptySenderName uses NOT EXISTS (no join), so it doesn't provide p_filter_from.
	if filter.Domain != "" {
		if filter.Sender == "" && !filter.MatchesEmpty(ViewSenders) && filter.SenderName == "" {
			joins = append(joins, `
				JOIN message_recipients mr_filter_from ON mr_filter_from.message_id = m.id AND mr_filter_from.recipient_type = 'from'
				JOIN participants p_filter_from ON p_filter_from.id = mr_filter_from.participant_id
			`)
		}
		conditions = append(conditions, "p_filter_from.domain = ?")
		args = append(args, filter.Domain)
	} else if filter.MatchesEmpty(ViewDomains) {
		if filter.Sender == "" && !filter.MatchesEmpty(ViewSenders) && filter.SenderName == "" {
			joins = append(joins, `
				LEFT JOIN message_recipients mr_filter_from ON mr_filter_from.message_id = m.id AND mr_filter_from.recipient_type = 'from'
				LEFT JOIN participants p_filter_from ON p_filter_from.id = mr_filter_from.participant_id
			`)
		}
		conditions = append(conditions, "(p_filter_from.domain IS NULL OR p_filter_from.domain = '')")
	}

	// Label filter - case-insensitive exact match
	if filter.Label != "" {
		joins = append(joins, `
			JOIN message_labels ml_filter ON ml_filter.message_id = m.id
			JOIN labels l_filter ON l_filter.id = ml_filter.label_id
		`)
		conditions = append(conditions, "LOWER(l_filter.name) = LOWER(?)")
		args = append(args, filter.Label)
	} else if filter.MatchesEmpty(ViewLabels) {
		conditions = append(conditions, "NOT EXISTS (SELECT 1 FROM message_labels ml WHERE ml.message_id = m.id)")
	}

	// Time period filter
	if filter.TimeRange.Period != "" {
		granularity := filter.TimeRange.Granularity
		if granularity == TimeYear && len(filter.TimeRange.Period) > 4 {
			switch len(filter.TimeRange.Period) {
			case 7:
				granularity = TimeMonth
			case 10:
				granularity = TimeDay
			}
		}

		var timeExpr string
		switch granularity {
		case TimeYear:
			timeExpr = "strftime('%Y', " + prefix + "sent_at)"
		case TimeMonth:
			timeExpr = "strftime('%Y-%m', " + prefix + "sent_at)"
		case TimeDay:
			timeExpr = "strftime('%Y-%m-%d', " + prefix + "sent_at)"
		default:
			timeExpr = "strftime('%Y-%m', " + prefix + "sent_at)"
		}
		conditions = append(conditions, fmt.Sprintf("%s = ?", timeExpr))
		args = append(args, filter.TimeRange.Period)
	}

	return strings.Join(joins, "\n"), conditions, args
}

// SubAggregate performs aggregation on a filtered subset of messages.
// This is used for sub-grouping after drill-down.
func (e *SQLiteEngine) SubAggregate(ctx context.Context, filter MessageFilter, groupBy ViewType, opts AggregateOptions) ([]AggregateRow, error) {
	filterJoins, filterConditions, args := buildFilterJoinsAndConditions(filter, "m")

	// Add opts-based conditions
	optsConds, optsArgs := optsToFilterConditions(opts, "m.")
	filterConditions = append(filterConditions, optsConds...)
	args = append(args, optsArgs...)

	searchJoins, searchConds, searchArgs :=
		e.buildAggregateSearchParts(ctx, opts.SearchQuery, groupBy)
	filterConditions = append(filterConditions, searchConds...)
	args = append(args, searchArgs...)
	if searchJoins != "" {
		filterJoins += "\n" + searchJoins
	}

	return e.executeAggregate(ctx, groupBy, opts, filterJoins, filterConditions, args)
}

// Aggregate performs grouping based on the provided ViewType.
func (e *SQLiteEngine) Aggregate(ctx context.Context, groupBy ViewType, opts AggregateOptions) ([]AggregateRow, error) {
	conditions, args := optsToFilterConditions(opts, "m.")

	searchJoins, searchConds, searchArgs :=
		e.buildAggregateSearchParts(ctx, opts.SearchQuery, groupBy)
	conditions = append(conditions, searchConds...)
	args = append(args, searchArgs...)

	return e.executeAggregate(
		ctx, groupBy, opts, searchJoins, conditions, args,
	)
}

// buildAggregateSearchParts parses a search query for aggregate views
// and returns (joins, conditions, args). For Labels view with label
// search, filters the grouping column directly.
func (e *SQLiteEngine) buildAggregateSearchParts(
	ctx context.Context, searchQuery string, groupBy ViewType,
) (string, []string, []interface{}) {
	if searchQuery == "" {
		return "", nil, nil
	}

	q := search.Parse(searchQuery)

	var conditions []string
	var args []interface{}

	// For Labels view with label search, filter the grouping
	// column (l.name) directly instead of adding a conflicting
	// label join. Strip labels from the parsed query before
	// building the generic parts.
	if groupBy == ViewLabels && len(q.Labels) > 0 {
		var labelParts []string
		for _, label := range q.Labels {
			labelParts = append(labelParts,
				`LOWER(l.name) LIKE LOWER(?) ESCAPE '\'`)
			args = append(args,
				"%"+escapeSQLiteLike(label)+"%")
		}
		conditions = append(conditions,
			"("+strings.Join(labelParts, " OR ")+")")
		q.Labels = nil
	}

	searchConds, searchArgs, searchJns, ftsJoin :=
		e.buildSearchQueryParts(ctx, q)
	conditions = append(conditions, searchConds...)
	args = append(args, searchArgs...)
	var joinParts []string
	if ftsJoin != "" {
		joinParts = append(joinParts, ftsJoin)
	}
	joinParts = append(joinParts, searchJns...)

	var joins string
	if len(joinParts) > 0 {
		joins = strings.Join(joinParts, "\n")
	}
	return joins, conditions, args
}

// executeAggregate is the shared implementation for Aggregate and SubAggregate.
func (e *SQLiteEngine) executeAggregate(ctx context.Context, groupBy ViewType, opts AggregateOptions, filterJoins string, filterConditions []string, args []interface{}) ([]AggregateRow, error) {
	dim, err := aggDimensionForView(groupBy, opts.TimeGranularity)
	if err != nil {
		return nil, err
	}

	sort, err := sortClause(opts)
	if err != nil {
		return nil, err
	}

	limit := opts.Limit
	if limit == 0 {
		limit = 100
	}

	filterWhere := "1=1"
	if len(filterConditions) > 0 {
		filterWhere = strings.Join(filterConditions, " AND ")
	}

	query := buildAggregateSQL(dim, filterJoins, filterWhere, sort)
	args = append(args, limit)
	return e.executeAggregateQuery(ctx, query, args)
}

// executeAggregateQuery runs an aggregate query and returns the results.
// Expects 6 columns: key, count, total_size, attachment_size, attachment_count, total_unique
func (e *SQLiteEngine) executeAggregateQuery(ctx context.Context, query string, args []interface{}) ([]AggregateRow, error) {
	rows, err := e.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("aggregate query: %w", err)
	}
	defer rows.Close()

	var results []AggregateRow
	for rows.Next() {
		var row AggregateRow
		if err := rows.Scan(&row.Key, &row.Count, &row.TotalSize, &row.AttachmentSize, &row.AttachmentCount, &row.TotalUnique); err != nil {
			return nil, fmt.Errorf("scan aggregate row: %w", err)
		}
		results = append(results, row)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate aggregate rows: %w", err)
	}

	return results, nil
}

// ListMessages retrieves messages matching the filter.
func (e *SQLiteEngine) ListMessages(ctx context.Context, filter MessageFilter) ([]MessageSummary, error) {
	filterJoins, conditions, args := buildFilterJoinsAndConditions(filter, "m")

	// Build ORDER BY with validation
	var orderBy string
	switch filter.Sorting.Field {
	case MessageSortByDate:
		orderBy = "m.sent_at"
	case MessageSortBySize:
		orderBy = "m.size_estimate"
	case MessageSortBySubject:
		orderBy = "m.subject"
	default:
		return nil, fmt.Errorf("unsupported message sort field: %d", filter.Sorting.Field)
	}
	if filter.Sorting.Direction == SortDesc {
		orderBy += " DESC"
	} else {
		orderBy += " ASC"
	}

	limit := filter.Pagination.Limit
	if limit == 0 {
		limit = 500
	}

	whereClause := "1=1"
	if len(conditions) > 0 {
		whereClause = strings.Join(conditions, " AND ")
	}

	query := fmt.Sprintf(`
		SELECT DISTINCT
			m.id,
			m.source_message_id,
			m.conversation_id,
			COALESCE(conv.source_conversation_id, ''),
			COALESCE(m.subject, ''),
			COALESCE(m.snippet, ''),
			COALESCE(p_sender.email_address, ''),
			COALESCE(p_sender.display_name, ''),
			m.sent_at,
			COALESCE(m.size_estimate, 0),
			m.has_attachments,
			m.attachment_count,
			m.deleted_from_source_at
		FROM messages m
		LEFT JOIN message_recipients mr_sender ON mr_sender.message_id = m.id AND mr_sender.recipient_type = 'from'
		LEFT JOIN participants p_sender ON p_sender.id = mr_sender.participant_id
		LEFT JOIN conversations conv ON conv.id = m.conversation_id
		%s
		WHERE %s
		ORDER BY %s
		LIMIT ? OFFSET ?
	`, filterJoins, whereClause, orderBy)

	args = append(args, limit, filter.Pagination.Offset)

	rows, err := e.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list messages: %w", err)
	}
	defer rows.Close()

	var results []MessageSummary
	for rows.Next() {
		var msg MessageSummary
		var sentAt sql.NullTime
		var deletedAt sql.NullTime
		if err := rows.Scan(
			&msg.ID,
			&msg.SourceMessageID,
			&msg.ConversationID,
			&msg.SourceConversationID,
			&msg.Subject,
			&msg.Snippet,
			&msg.FromEmail,
			&msg.FromName,
			&sentAt,
			&msg.SizeEstimate,
			&msg.HasAttachments,
			&msg.AttachmentCount,
			&deletedAt,
		); err != nil {
			return nil, fmt.Errorf("scan message: %w", err)
		}
		if sentAt.Valid {
			msg.SentAt = sentAt.Time
		}
		if deletedAt.Valid {
			msg.DeletedAt = &deletedAt.Time
		}
		results = append(results, msg)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate messages: %w", err)
	}

	// Fetch labels for each message (batch would be more efficient but this is simpler)
	if len(results) > 0 {
		if err := e.fetchLabelsForMessages(ctx, results); err != nil {
			return nil, fmt.Errorf("fetch labels: %w", err)
		}
	}

	return results, nil
}

// fetchLabelsForMessages adds labels to message summaries.
func (e *SQLiteEngine) fetchLabelsForMessages(ctx context.Context, messages []MessageSummary) error {
	return fetchLabelsForMessageList(ctx, e.db, "", messages)
}

// GetMessage retrieves a full message by internal ID.
func (e *SQLiteEngine) GetMessage(ctx context.Context, id int64) (*MessageDetail, error) {
	return e.getMessageByQuery(ctx, "m.id = ?", id)
}

// GetMessageBySourceID retrieves a full message by source message ID (e.g., Gmail ID).
// Note: This searches across all accounts and returns the first match. For Gmail,
// message IDs are unique per account but theoretically could collide across accounts.
// In practice, Gmail IDs are random enough that collisions are astronomically unlikely.
// If you need to guarantee uniqueness, use the internal ID from GetMessage instead.
func (e *SQLiteEngine) GetMessageBySourceID(ctx context.Context, sourceMessageID string) (*MessageDetail, error) {
	return e.getMessageByQuery(ctx, "m.source_message_id = ?", sourceMessageID)
}

func (e *SQLiteEngine) getMessageByQuery(ctx context.Context, whereClause string, args ...interface{}) (*MessageDetail, error) {
	return getMessageByQueryShared(ctx, e.db, "", whereClause, args...)
}

// GetAttachment retrieves attachment metadata by ID.
func (e *SQLiteEngine) GetAttachment(ctx context.Context, id int64) (*AttachmentInfo, error) {
	var att AttachmentInfo
	err := e.db.QueryRowContext(ctx, `
		SELECT id, COALESCE(filename, ''), COALESCE(mime_type, ''), COALESCE(size, 0), COALESCE(content_hash, '')
		FROM attachments
		WHERE id = ?
	`, id).Scan(&att.ID, &att.Filename, &att.MimeType, &att.Size, &att.ContentHash)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get attachment: %w", err)
	}
	return &att, nil
}

// ListAccounts returns all source accounts.
func (e *SQLiteEngine) ListAccounts(ctx context.Context) ([]AccountInfo, error) {
	rows, err := e.db.QueryContext(ctx, `
		SELECT id, source_type, identifier, COALESCE(display_name, '')
		FROM sources
		ORDER BY identifier
	`)
	if err != nil {
		return nil, fmt.Errorf("list accounts: %w", err)
	}
	defer rows.Close()

	var accounts []AccountInfo
	for rows.Next() {
		var acc AccountInfo
		if err := rows.Scan(&acc.ID, &acc.SourceType, &acc.Identifier, &acc.DisplayName); err != nil {
			return nil, fmt.Errorf("scan account: %w", err)
		}
		accounts = append(accounts, acc)
	}

	return accounts, rows.Err()
}

// GetTotalStats returns overall statistics.
func (e *SQLiteEngine) GetTotalStats(ctx context.Context, opts StatsOptions) (*TotalStats, error) {
	stats := &TotalStats{}

	// Build search conditions when SearchQuery is set.
	var searchConditions []string
	var searchArgs []interface{}
	var searchJoins []string
	var searchFTSJoin string
	if opts.SearchQuery != "" {
		q := search.Parse(opts.SearchQuery)
		searchConditions, searchArgs, searchJoins, searchFTSJoin = e.buildSearchQueryParts(ctx, q)
	}

	// Build WHERE clause for messages — always use m. prefix since we alias
	// the messages table for compatibility with search joins.
	var conditions []string
	var args []interface{}
	// Include all messages (deleted messages shown with indicator in TUI)
	if opts.SourceID != nil {
		conditions = append(conditions, "m.source_id = ?")
		args = append(args, *opts.SourceID)
	}
	if opts.WithAttachmentsOnly {
		conditions = append(conditions, "m.has_attachments = 1")
	}
	if opts.HideDeletedFromSource {
		conditions = append(conditions, "m.deleted_from_source_at IS NULL")
	}
	// Merge search conditions
	conditions = append(conditions, searchConditions...)
	args = append(args, searchArgs...)

	whereClause := "1=1"
	if len(conditions) > 0 {
		whereClause = strings.Join(conditions, " AND ")
	}

	// Build join clause for search
	joinClause := ""
	if searchFTSJoin != "" {
		joinClause += searchFTSJoin + "\n"
	}
	if len(searchJoins) > 0 {
		joinClause += strings.Join(searchJoins, "\n")
	}

	// Message stats — when search joins are present, use a subquery to get
	// distinct matching IDs first, avoiding duplicates from 1:N joins.
	var msgQuery string
	if joinClause != "" {
		msgQuery = fmt.Sprintf(`
			SELECT
				COUNT(*),
				COALESCE(SUM(size_estimate), 0)
			FROM messages
			WHERE id IN (
				SELECT DISTINCT m.id FROM messages m
				%s
				WHERE %s
			)
		`, joinClause, whereClause)
	} else {
		msgQuery = fmt.Sprintf(`
			SELECT
				COUNT(*),
				COALESCE(SUM(size_estimate), 0)
			FROM messages m
			WHERE %s
		`, whereClause)
	}

	if err := e.db.QueryRowContext(ctx, msgQuery, args...).Scan(&stats.MessageCount, &stats.TotalSize); err != nil {
		return nil, fmt.Errorf("message stats: %w", err)
	}

	// Attachment stats — use IN subquery only when search joins are present
	// (to de-duplicate 1:N join rows). Without joins, a direct query is faster.
	var attQuery string
	if joinClause != "" {
		attQuery = fmt.Sprintf(`
			SELECT COUNT(*), COALESCE(SUM(a.size), 0)
			FROM attachments a
			WHERE a.message_id IN (
				SELECT DISTINCT m.id FROM messages m
				%s
				WHERE %s
			)
		`, joinClause, whereClause)
	} else {
		attQuery = fmt.Sprintf(`
			SELECT COUNT(*), COALESCE(SUM(a.size), 0)
			FROM attachments a
			JOIN messages m ON m.id = a.message_id
			WHERE %s
		`, whereClause)
	}

	if err := e.db.QueryRowContext(ctx, attQuery, args...).Scan(&stats.AttachmentCount, &stats.AttachmentSize); err != nil {
		return nil, fmt.Errorf("attachment stats: %w", err)
	}

	// Label count - filter by source when sourceID is provided
	var labelQuery string
	if opts.SourceID != nil {
		labelQuery = "SELECT COUNT(*) FROM labels WHERE source_id = ?"
		if err := e.db.QueryRowContext(ctx, labelQuery, *opts.SourceID).Scan(&stats.LabelCount); err != nil {
			return nil, fmt.Errorf("label count: %w", err)
		}
	} else {
		labelQuery = "SELECT COUNT(*) FROM labels"
		if err := e.db.QueryRowContext(ctx, labelQuery).Scan(&stats.LabelCount); err != nil {
			return nil, fmt.Errorf("label count: %w", err)
		}
	}

	// Account count - verify source exists when filtering by sourceID
	if opts.SourceID != nil {
		if err := e.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM sources WHERE id = ?", *opts.SourceID).Scan(&stats.AccountCount); err != nil {
			return nil, fmt.Errorf("account count: %w", err)
		}
	} else {
		if err := e.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM sources").Scan(&stats.AccountCount); err != nil {
			return nil, fmt.Errorf("account count: %w", err)
		}
	}

	return stats, nil
}

// GetGmailIDsByFilter returns Gmail message IDs (source_message_id) matching a filter.
// This is more efficient than ListMessages when you only need the IDs.
func (e *SQLiteEngine) GetGmailIDsByFilter(ctx context.Context, filter MessageFilter) ([]string, error) {
	var conditions []string
	var args []interface{}

	// Always exclude deleted messages
	conditions = append(conditions, "m.deleted_from_source_at IS NULL")

	if filter.SourceID != nil {
		conditions = append(conditions, "m.source_id = ?")
		args = append(args, *filter.SourceID)
	}

	// Build JOIN clauses based on filter type
	var joins []string

	if filter.Sender != "" {
		joins = append(joins, `
			JOIN message_recipients mr_from ON mr_from.message_id = m.id AND mr_from.recipient_type = 'from'
			JOIN participants p_from ON p_from.id = mr_from.participant_id
		`)
		conditions = append(conditions, "p_from.email_address = ?")
		args = append(args, filter.Sender)
	}

	if filter.SenderName != "" {
		if filter.Sender == "" {
			joins = append(joins, `
				JOIN message_recipients mr_from ON mr_from.message_id = m.id AND mr_from.recipient_type = 'from'
				JOIN participants p_from ON p_from.id = mr_from.participant_id
			`)
		}
		conditions = append(conditions, "COALESCE(NULLIF(TRIM(p_from.display_name), ''), p_from.email_address) = ?")
		args = append(args, filter.SenderName)
	}

	if filter.Recipient != "" {
		joins = append(joins, `
			JOIN message_recipients mr_to ON mr_to.message_id = m.id AND mr_to.recipient_type IN ('to', 'cc', 'bcc')
			JOIN participants p_to ON p_to.id = mr_to.participant_id
		`)
		conditions = append(conditions, "p_to.email_address = ?")
		args = append(args, filter.Recipient)
	}

	if filter.RecipientName != "" {
		if filter.Recipient == "" {
			// Always add the full join chain — GetGmailIDsByFilter does not
			// have a standalone MatchEmptyRecipient handler, so mr_to may
			// not exist yet.
			joins = append(joins, `
				JOIN message_recipients mr_to ON mr_to.message_id = m.id AND mr_to.recipient_type IN ('to', 'cc', 'bcc')
				JOIN participants p_to ON p_to.id = mr_to.participant_id
			`)
		}
		conditions = append(conditions, "COALESCE(NULLIF(TRIM(p_to.display_name), ''), p_to.email_address) = ?")
		args = append(args, filter.RecipientName)
	}

	if filter.Domain != "" {
		if filter.Sender == "" && filter.SenderName == "" { // Don't duplicate the join
			joins = append(joins, `
				JOIN message_recipients mr_from ON mr_from.message_id = m.id AND mr_from.recipient_type = 'from'
				JOIN participants p_from ON p_from.id = mr_from.participant_id
			`)
		}
		conditions = append(conditions, "p_from.domain = ?")
		args = append(args, filter.Domain)
	}

	if filter.Label != "" {
		joins = append(joins, `
			JOIN message_labels ml ON ml.message_id = m.id
			JOIN labels l ON l.id = ml.label_id
		`)
		conditions = append(conditions, "LOWER(l.name) = LOWER(?)")
		args = append(args, filter.Label)
	}

	if filter.TimeRange.Period != "" {
		// Infer granularity from TimePeriod format if not explicitly set
		granularity := filter.TimeRange.Granularity
		if granularity == TimeYear && len(filter.TimeRange.Period) > 4 {
			switch len(filter.TimeRange.Period) {
			case 7:
				granularity = TimeMonth
			case 10:
				granularity = TimeDay
			}
		}

		var timeExpr string
		switch granularity {
		case TimeYear:
			timeExpr = "strftime('%Y', m.sent_at)"
		case TimeMonth:
			timeExpr = "strftime('%Y-%m', m.sent_at)"
		case TimeDay:
			timeExpr = "strftime('%Y-%m-%d', m.sent_at)"
		default:
			timeExpr = "strftime('%Y-%m', m.sent_at)"
		}
		conditions = append(conditions, fmt.Sprintf("%s = ?", timeExpr))
		args = append(args, filter.TimeRange.Period)
	}

	// Build query - only add LIMIT if explicitly set
	query := fmt.Sprintf(`
		SELECT DISTINCT m.source_message_id
		FROM messages m
		%s
		WHERE %s
		ORDER BY m.sent_at DESC, m.id DESC
	`, strings.Join(joins, "\n"), strings.Join(conditions, " AND "))

	// Only add LIMIT if explicitly set (0 means no limit)
	if filter.Pagination.Limit > 0 {
		query += " LIMIT ?"
		args = append(args, filter.Pagination.Limit)
	}

	rows, err := e.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("get gmail ids: %w", err)
	}
	defer rows.Close()

	return collectGmailIDs(rows)
}

// Search performs a Gmail-style search query.
// buildSearchQueryParts builds the WHERE conditions, args, joins, and FTS join
// for a search query. This is shared between Search and SearchFastCount.
func (e *SQLiteEngine) buildSearchQueryParts(ctx context.Context, q *search.Query) (conditions []string, args []interface{}, joins []string, ftsJoin string) {
	// Include all messages (deleted messages shown with indicator in TUI)

	// From filter - uses EXISTS to avoid join multiplication in aggregates.
	// Handles both exact addresses and @domain patterns.
	if len(q.FromAddrs) > 0 {
		var fromParts []string
		for _, addr := range q.FromAddrs {
			if strings.HasPrefix(addr, "@") {
				fromParts = append(fromParts,
					"LOWER(p_from.email_address) LIKE ?")
				args = append(args, "%"+addr)
			} else {
				fromParts = append(fromParts,
					"LOWER(p_from.email_address) = LOWER(?)")
				args = append(args, addr)
			}
		}
		conditions = append(conditions, fmt.Sprintf(`EXISTS (
			SELECT 1 FROM message_recipients mr_from
			JOIN participants p_from ON p_from.id = mr_from.participant_id
			WHERE mr_from.message_id = m.id
			  AND mr_from.recipient_type = 'from'
			  AND (%s)
		)`, strings.Join(fromParts, " OR ")))
	}

	// To filter - EXISTS to avoid join multiplication
	if len(q.ToAddrs) > 0 {
		placeholders := make([]string, len(q.ToAddrs))
		for i, addr := range q.ToAddrs {
			placeholders[i] = "?"
			args = append(args, addr)
		}
		conditions = append(conditions, fmt.Sprintf(`EXISTS (
			SELECT 1 FROM message_recipients mr_to
			JOIN participants p_to ON p_to.id = mr_to.participant_id
			WHERE mr_to.message_id = m.id
			  AND mr_to.recipient_type = 'to'
			  AND LOWER(p_to.email_address) IN (%s)
		)`, strings.Join(placeholders, ",")))
	}

	// CC filter - EXISTS to avoid join multiplication
	if len(q.CcAddrs) > 0 {
		placeholders := make([]string, len(q.CcAddrs))
		for i, addr := range q.CcAddrs {
			placeholders[i] = "?"
			args = append(args, addr)
		}
		conditions = append(conditions, fmt.Sprintf(`EXISTS (
			SELECT 1 FROM message_recipients mr_cc
			JOIN participants p_cc ON p_cc.id = mr_cc.participant_id
			WHERE mr_cc.message_id = m.id
			  AND mr_cc.recipient_type = 'cc'
			  AND LOWER(p_cc.email_address) IN (%s)
		)`, strings.Join(placeholders, ",")))
	}

	// BCC filter - EXISTS to avoid join multiplication
	if len(q.BccAddrs) > 0 {
		placeholders := make([]string, len(q.BccAddrs))
		for i, addr := range q.BccAddrs {
			placeholders[i] = "?"
			args = append(args, addr)
		}
		conditions = append(conditions, fmt.Sprintf(`EXISTS (
			SELECT 1 FROM message_recipients mr_bcc
			JOIN participants p_bcc ON p_bcc.id = mr_bcc.participant_id
			WHERE mr_bcc.message_id = m.id
			  AND mr_bcc.recipient_type = 'bcc'
			  AND LOWER(p_bcc.email_address) IN (%s)
		)`, strings.Join(placeholders, ",")))
	}

	// Label filter - case-insensitive substring match using EXISTS
	// so each label term can match a different row in message_labels.
	for _, label := range q.Labels {
		conditions = append(conditions, `EXISTS (
			SELECT 1 FROM message_labels ml_lbl
			JOIN labels l_lbl ON l_lbl.id = ml_lbl.label_id
			WHERE ml_lbl.message_id = m.id
			  AND LOWER(l_lbl.name) LIKE LOWER(?) ESCAPE '\'
		)`)
		args = append(args, "%"+escapeSQLiteLike(label)+"%")
	}

	// Subject filter
	if len(q.SubjectTerms) > 0 {
		for _, term := range q.SubjectTerms {
			conditions = append(conditions, "m.subject LIKE ?")
			args = append(args, "%"+term+"%")
		}
	}

	// Has attachment filter
	if q.HasAttachment != nil && *q.HasAttachment {
		conditions = append(conditions, "m.has_attachments = 1")
	}

	// Date range filters
	if q.AfterDate != nil {
		conditions = append(conditions, "m.sent_at >= ?")
		args = append(args, q.AfterDate.Format("2006-01-02 15:04:05"))
	}
	if q.BeforeDate != nil {
		conditions = append(conditions, "m.sent_at < ?")
		args = append(args, q.BeforeDate.Format("2006-01-02 15:04:05"))
	}

	// Size filters
	if q.LargerThan != nil {
		conditions = append(conditions, "m.size_estimate > ?")
		args = append(args, *q.LargerThan)
	}
	if q.SmallerThan != nil {
		conditions = append(conditions, "m.size_estimate < ?")
		args = append(args, *q.SmallerThan)
	}

	// Full-text search: use FTS5 if available, fall back to LIKE
	if len(q.TextTerms) > 0 {
		if e.hasFTSTable(ctx) {
			// Use FTS5 for efficient full-text search
			ftsJoin = "JOIN messages_fts fts ON fts.rowid = m.id"
			// Build FTS match expression
			ftsTerms := make([]string, len(q.TextTerms))
			for i, term := range q.TextTerms {
				// Escape special characters for FTS5
				term = strings.ReplaceAll(term, "\"", "\"\"")
				if strings.Contains(term, " ") {
					ftsTerms[i] = fmt.Sprintf("\"%s\"", term)
				} else {
					ftsTerms[i] = term
				}
			}
			conditions = append(conditions, "messages_fts MATCH ?")
			args = append(args, strings.Join(ftsTerms, " "))
		} else {
			// Fall back to LIKE-based search on subject/snippet only
			// Body text is in a separate table; use FTS for body search
			for _, term := range q.TextTerms {
				likeTerm := "%" + term + "%"
				conditions = append(conditions, "(m.subject LIKE ? OR m.snippet LIKE ?)")
				args = append(args, likeTerm, likeTerm)
			}
		}
	}

	// Account filter
	if q.AccountID != nil {
		conditions = append(conditions, "m.source_id = ?")
		args = append(args, *q.AccountID)
	}

	// Hide-deleted filter
	if q.HideDeleted {
		conditions = append(conditions, "m.deleted_from_source_at IS NULL")
	}

	return conditions, args, joins, ftsJoin
}

func (e *SQLiteEngine) Search(ctx context.Context, q *search.Query, limit, offset int) ([]MessageSummary, error) {
	conditions, args, joins, ftsJoin := e.buildSearchQueryParts(ctx, q)
	return e.executeSearchQuery(ctx, conditions, args, joins, ftsJoin, limit, offset)
}

// SearchFast searches using the same FTS5 path as Search but merges
// MessageFilter context into the query (drill-down filters, hide-deleted, etc.).
func (e *SQLiteEngine) SearchFast(ctx context.Context, q *search.Query, filter MessageFilter, limit, offset int) ([]MessageSummary, error) {
	mergedQuery := MergeFilterIntoQuery(q, filter)
	conditions, args, joins, ftsJoin := e.buildSearchQueryParts(ctx, mergedQuery)
	return e.executeSearchQuery(ctx, conditions, args, joins, ftsJoin, limit, offset)
}

// executeSearchQuery runs a search query built from conditions/joins and returns
// paginated MessageSummary results. Shared by Search and SearchFast.
func (e *SQLiteEngine) executeSearchQuery(ctx context.Context, conditions []string, args []interface{}, joins []string, ftsJoin string, limit, offset int) ([]MessageSummary, error) {
	if limit == 0 {
		limit = 100
	}

	whereClause := strings.Join(conditions, " AND ")
	if whereClause == "" {
		whereClause = "1=1"
	}

	query := fmt.Sprintf(`
		SELECT DISTINCT
			m.id,
			m.source_message_id,
			m.conversation_id,
			COALESCE(conv.source_conversation_id, ''),
			COALESCE(m.subject, ''),
			COALESCE(m.snippet, ''),
			COALESCE(p_sender.email_address, ''),
			COALESCE(p_sender.display_name, ''),
			m.sent_at,
			COALESCE(m.size_estimate, 0),
			m.has_attachments,
			m.attachment_count,
			m.deleted_from_source_at
		FROM messages m
		LEFT JOIN message_recipients mr_sender ON mr_sender.message_id = m.id AND mr_sender.recipient_type = 'from'
		LEFT JOIN participants p_sender ON p_sender.id = mr_sender.participant_id
		LEFT JOIN conversations conv ON conv.id = m.conversation_id
		%s
		%s
		WHERE %s
		ORDER BY m.sent_at DESC
		LIMIT ? OFFSET ?
	`, ftsJoin, strings.Join(joins, "\n"), whereClause)

	args = append(args, limit, offset)

	rows, err := e.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("search messages: %w", err)
	}
	defer rows.Close()

	var results []MessageSummary
	for rows.Next() {
		var msg MessageSummary
		var sentAt sql.NullTime
		var deletedAt sql.NullTime
		if err := rows.Scan(
			&msg.ID,
			&msg.SourceMessageID,
			&msg.ConversationID,
			&msg.SourceConversationID,
			&msg.Subject,
			&msg.Snippet,
			&msg.FromEmail,
			&msg.FromName,
			&sentAt,
			&msg.SizeEstimate,
			&msg.HasAttachments,
			&msg.AttachmentCount,
			&deletedAt,
		); err != nil {
			return nil, fmt.Errorf("scan message: %w", err)
		}
		if sentAt.Valid {
			msg.SentAt = sentAt.Time
		}
		if deletedAt.Valid {
			msg.DeletedAt = &deletedAt.Time
		}
		results = append(results, msg)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate messages: %w", err)
	}

	// Fetch labels for results
	if len(results) > 0 {
		if err := e.fetchLabelsForMessages(ctx, results); err != nil {
			return nil, fmt.Errorf("fetch labels: %w", err)
		}
	}

	return results, nil
}

// MergeFilterIntoQuery combines a MessageFilter context with a search.Query.
// Context filters are appended to existing query filters.
//
// Note on semantics: Appending to FromAddrs/ToAddrs produces OR semantics
// within each dimension (IN clause). Labels use per-term EXISTS subqueries
// with AND semantics (message must have all labels). Context filters widen
// the search within other constraints.
func MergeFilterIntoQuery(q *search.Query, filter MessageFilter) *search.Query {
	// Copy all fields from original query (preserves any future non-slice fields)
	merged := *q

	// Deep copy slices to avoid mutating original (shallow copy + append can
	// mutate if original slice has spare capacity)
	merged.TextTerms = append([]string(nil), q.TextTerms...)
	merged.FromAddrs = append([]string(nil), q.FromAddrs...)
	merged.ToAddrs = append([]string(nil), q.ToAddrs...)
	merged.CcAddrs = append([]string(nil), q.CcAddrs...)
	merged.BccAddrs = append([]string(nil), q.BccAddrs...)
	merged.SubjectTerms = append([]string(nil), q.SubjectTerms...)
	merged.Labels = append([]string(nil), q.Labels...)

	// Account filter - always apply if set
	if filter.SourceID != nil {
		merged.AccountID = filter.SourceID
	}

	// Sender filter - append to existing from: filters
	if filter.Sender != "" {
		merged.FromAddrs = append(merged.FromAddrs, filter.Sender)
	}

	// Recipient filter - append to existing to: filters
	if filter.Recipient != "" {
		merged.ToAddrs = append(merged.ToAddrs, filter.Recipient)
	}

	// Label filter - append to existing label: filters
	if filter.Label != "" {
		merged.Labels = append(merged.Labels, filter.Label)
	}

	// Attachment filter - set if context requires attachments
	if filter.WithAttachmentsOnly {
		hasAttachment := true
		merged.HasAttachment = &hasAttachment
	}

	// Domain filter - add as @domain pattern (handled specially in Search)
	if filter.Domain != "" {
		merged.FromAddrs = append(merged.FromAddrs, "@"+filter.Domain)
	}

	// Hide-deleted filter
	if filter.HideDeletedFromSource {
		merged.HideDeleted = true
	}

	return &merged
}

// SearchFastCount returns the total count of messages matching a search query.
// Uses the same query logic as SearchFast to ensure consistent counts.
func (e *SQLiteEngine) SearchFastCount(ctx context.Context, q *search.Query, filter MessageFilter) (int64, error) {
	mergedQuery := MergeFilterIntoQuery(q, filter)
	conditions, args, joins, ftsJoin := e.buildSearchQueryParts(ctx, mergedQuery)

	whereClause := strings.Join(conditions, " AND ")
	if whereClause == "" {
		whereClause = "1=1"
	}

	query := fmt.Sprintf(`
		SELECT COUNT(DISTINCT m.id)
		FROM messages m
		%s
		%s
		WHERE %s
	`, ftsJoin, strings.Join(joins, "\n"), whereClause)

	var count int64
	if err := e.db.QueryRowContext(ctx, query, args...).Scan(&count); err != nil {
		return 0, fmt.Errorf("search fast count: %w", err)
	}
	return count, nil
}

// SearchFastWithStats delegates to SearchFast + SearchFastCount + GetTotalStats.
// SQLite doesn't benefit from temp table materialization, so we just call the
// existing methods independently.
func (e *SQLiteEngine) SearchFastWithStats(ctx context.Context, q *search.Query, queryStr string,
	filter MessageFilter, statsGroupBy ViewType, limit, offset int) (*SearchFastResult, error) {

	results, err := e.SearchFast(ctx, q, filter, limit, offset)
	if err != nil {
		return nil, err
	}

	// Best-effort count: don't abort the search if count fails.
	count, countErr := e.SearchFastCount(ctx, q, filter)
	if countErr != nil {
		log.Printf("warning: search count failed (using -1): %v", countErr)
		count = -1
	}

	statsOpts := StatsOptions{
		SourceID:              filter.SourceID,
		WithAttachmentsOnly:   filter.WithAttachmentsOnly,
		HideDeletedFromSource: filter.HideDeletedFromSource,
		SearchQuery:           queryStr,
		GroupBy:               statsGroupBy,
	}
	stats, _ := e.GetTotalStats(ctx, statsOpts)

	return &SearchFastResult{
		Messages:   results,
		TotalCount: count,
		Stats:      stats,
	}, nil
}
