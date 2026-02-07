package query

import (
	"bytes"
	"compress/zlib"
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"

	"github.com/wesm/msgvault/internal/mime"
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

	// Label filter
	if filter.Label != "" {
		joins = append(joins, `
			JOIN message_labels ml_filter ON ml_filter.message_id = m.id
			JOIN labels l_filter ON l_filter.id = ml_filter.label_id
		`)
		conditions = append(conditions, "l_filter.name = ?")
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

	return e.executeAggregate(ctx, groupBy, opts, filterJoins, filterConditions, args)
}

// Aggregate performs grouping based on the provided ViewType.
func (e *SQLiteEngine) Aggregate(ctx context.Context, groupBy ViewType, opts AggregateOptions) ([]AggregateRow, error) {
	conditions, args := optsToFilterConditions(opts, "m.")
	return e.executeAggregate(ctx, groupBy, opts, "", conditions, args)
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
	if len(messages) == 0 {
		return nil
	}

	// Build message ID list
	ids := make([]interface{}, len(messages))
	placeholders := make([]string, len(messages))
	idToIndex := make(map[int64]int)
	for i, msg := range messages {
		ids[i] = msg.ID
		placeholders[i] = "?"
		idToIndex[msg.ID] = i
	}

	query := fmt.Sprintf(`
		SELECT ml.message_id, l.name
		FROM message_labels ml
		JOIN labels l ON l.id = ml.label_id
		WHERE ml.message_id IN (%s)
	`, strings.Join(placeholders, ","))

	rows, err := e.db.QueryContext(ctx, query, ids...)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var msgID int64
		var labelName string
		if err := rows.Scan(&msgID, &labelName); err != nil {
			return err
		}
		if idx, ok := idToIndex[msgID]; ok {
			messages[idx].Labels = append(messages[idx].Labels, labelName)
		}
	}

	return rows.Err()
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
	// Always exclude soft-deleted messages, consistent with list/aggregate queries
	query := fmt.Sprintf(`
		SELECT
			m.id,
			m.source_message_id,
			m.conversation_id,
			COALESCE(conv.source_conversation_id, ''),
			COALESCE(m.subject, ''),
			COALESCE(m.snippet, ''),
			m.sent_at,
			m.received_at,
			COALESCE(m.size_estimate, 0),
			m.has_attachments
		FROM messages m
		LEFT JOIN conversations conv ON conv.id = m.conversation_id
		WHERE %s
	`, whereClause)

	var msg MessageDetail
	var sentAt, receivedAt sql.NullTime
	err := e.db.QueryRowContext(ctx, query, args...).Scan(
		&msg.ID,
		&msg.SourceMessageID,
		&msg.ConversationID,
		&msg.SourceConversationID,
		&msg.Subject,
		&msg.Snippet,
		&sentAt,
		&receivedAt,
		&msg.SizeEstimate,
		&msg.HasAttachments,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get message: %w", err)
	}

	if sentAt.Valid {
		msg.SentAt = sentAt.Time
	}
	if receivedAt.Valid {
		t := receivedAt.Time
		msg.ReceivedAt = &t
	}

	// Fetch body from separate table (PK lookup, avoids scanning large body B-tree)
	var bodyText, bodyHTML sql.NullString
	err = e.db.QueryRowContext(ctx, `
		SELECT body_text, body_html FROM message_bodies WHERE message_id = ?
	`, msg.ID).Scan(&bodyText, &bodyHTML)
	if err == nil {
		if bodyText.Valid {
			msg.BodyText = bodyText.String
		}
		if bodyHTML.Valid {
			msg.BodyHTML = bodyHTML.String
		}
	} else if err != sql.ErrNoRows {
		return nil, fmt.Errorf("get message body: %w", err)
	}

	// If body is empty, try to extract from raw MIME
	if msg.BodyText == "" && msg.BodyHTML == "" {
		if body, err := e.extractBodyFromRaw(ctx, msg.ID); err == nil && body != "" {
			msg.BodyText = body
		}
	}

	// Fetch participants
	if err := e.fetchParticipants(ctx, &msg); err != nil {
		return nil, fmt.Errorf("fetch participants: %w", err)
	}

	// Fetch labels
	if err := e.fetchLabels(ctx, &msg); err != nil {
		return nil, fmt.Errorf("fetch labels: %w", err)
	}

	// Fetch attachments
	if err := e.fetchAttachments(ctx, &msg); err != nil {
		return nil, fmt.Errorf("fetch attachments: %w", err)
	}

	return &msg, nil
}

// extractBodyFromRaw extracts text body from compressed MIME data.
func (e *SQLiteEngine) extractBodyFromRaw(ctx context.Context, messageID int64) (string, error) {
	var compressed []byte
	var compression sql.NullString

	err := e.db.QueryRowContext(ctx, `
		SELECT raw_data, compression FROM message_raw WHERE message_id = ?
	`, messageID).Scan(&compressed, &compression)
	if err != nil {
		return "", err
	}

	var rawData []byte
	if compression.Valid && compression.String == "zlib" {
		r, err := zlib.NewReader(bytes.NewReader(compressed))
		if err != nil {
			return "", err
		}
		defer r.Close()
		rawData, err = io.ReadAll(r)
		if err != nil {
			return "", err
		}
	} else {
		rawData = compressed
	}

	// Parse MIME and extract text
	parsed, err := mime.Parse(rawData)
	if err != nil {
		return "", err
	}

	return parsed.GetBodyText(), nil
}

func (e *SQLiteEngine) fetchParticipants(ctx context.Context, msg *MessageDetail) error {
	rows, err := e.db.QueryContext(ctx, `
		SELECT mr.recipient_type, p.email_address, COALESCE(mr.display_name, p.display_name, '')
		FROM message_recipients mr
		JOIN participants p ON p.id = mr.participant_id
		WHERE mr.message_id = ?
	`, msg.ID)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var recipType, email, name string
		if err := rows.Scan(&recipType, &email, &name); err != nil {
			return err
		}
		addr := Address{Email: email, Name: name}
		switch recipType {
		case "from":
			msg.From = append(msg.From, addr)
		case "to":
			msg.To = append(msg.To, addr)
		case "cc":
			msg.Cc = append(msg.Cc, addr)
		case "bcc":
			msg.Bcc = append(msg.Bcc, addr)
		}
	}

	return rows.Err()
}

func (e *SQLiteEngine) fetchLabels(ctx context.Context, msg *MessageDetail) error {
	rows, err := e.db.QueryContext(ctx, `
		SELECT l.name
		FROM message_labels ml
		JOIN labels l ON l.id = ml.label_id
		WHERE ml.message_id = ?
	`, msg.ID)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return err
		}
		msg.Labels = append(msg.Labels, name)
	}

	return rows.Err()
}

func (e *SQLiteEngine) fetchAttachments(ctx context.Context, msg *MessageDetail) error {
	rows, err := e.db.QueryContext(ctx, `
		SELECT id, COALESCE(filename, ''), COALESCE(mime_type, ''), COALESCE(size, 0), COALESCE(content_hash, '')
		FROM attachments
		WHERE message_id = ?
	`, msg.ID)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var att AttachmentInfo
		if err := rows.Scan(&att.ID, &att.Filename, &att.MimeType, &att.Size, &att.ContentHash); err != nil {
			return err
		}
		msg.Attachments = append(msg.Attachments, att)
	}

	return rows.Err()
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
		conditions = append(conditions, "l.name = ?")
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

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan gmail id: %w", err)
		}
		ids = append(ids, id)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate gmail ids: %w", err)
	}

	return ids, nil
}

// Search performs a Gmail-style search query.
// buildSearchQueryParts builds the WHERE conditions, args, joins, and FTS join
// for a search query. This is shared between Search and SearchFastCount.
func (e *SQLiteEngine) buildSearchQueryParts(ctx context.Context, q *search.Query) (conditions []string, args []interface{}, joins []string, ftsJoin string) {
	// Include all messages (deleted messages shown with indicator in TUI)

	// From filter - handles both exact addresses and @domain patterns
	if len(q.FromAddrs) > 0 {
		joins = append(joins, `
			JOIN message_recipients mr_from ON mr_from.message_id = m.id AND mr_from.recipient_type = 'from'
			JOIN participants p_from ON p_from.id = mr_from.participant_id
		`)
		var exactAddrs []string
		var domainPatterns []string
		for _, addr := range q.FromAddrs {
			if strings.HasPrefix(addr, "@") {
				// Domain pattern: match emails ending with @domain
				domainPatterns = append(domainPatterns, addr)
			} else {
				exactAddrs = append(exactAddrs, addr)
			}
		}
		var fromConditions []string
		if len(exactAddrs) > 0 {
			placeholders := make([]string, len(exactAddrs))
			for i, addr := range exactAddrs {
				placeholders[i] = "?"
				args = append(args, addr)
			}
			fromConditions = append(fromConditions, fmt.Sprintf("LOWER(p_from.email_address) IN (%s)", strings.Join(placeholders, ",")))
		}
		for _, domain := range domainPatterns {
			// Use LIKE for domain suffix matching (e.g., @example.com matches alice@example.com)
			args = append(args, "%"+domain)
			fromConditions = append(fromConditions, "LOWER(p_from.email_address) LIKE ?")
		}
		if len(fromConditions) > 0 {
			conditions = append(conditions, "("+strings.Join(fromConditions, " OR ")+")")
		}
	}

	// To filter
	if len(q.ToAddrs) > 0 {
		joins = append(joins, `
			JOIN message_recipients mr_to ON mr_to.message_id = m.id AND mr_to.recipient_type = 'to'
			JOIN participants p_to ON p_to.id = mr_to.participant_id
		`)
		placeholders := make([]string, len(q.ToAddrs))
		for i, addr := range q.ToAddrs {
			placeholders[i] = "?"
			args = append(args, addr)
		}
		conditions = append(conditions, fmt.Sprintf("LOWER(p_to.email_address) IN (%s)", strings.Join(placeholders, ",")))
	}

	// CC filter
	if len(q.CcAddrs) > 0 {
		joins = append(joins, `
			JOIN message_recipients mr_cc ON mr_cc.message_id = m.id AND mr_cc.recipient_type = 'cc'
			JOIN participants p_cc ON p_cc.id = mr_cc.participant_id
		`)
		placeholders := make([]string, len(q.CcAddrs))
		for i, addr := range q.CcAddrs {
			placeholders[i] = "?"
			args = append(args, addr)
		}
		conditions = append(conditions, fmt.Sprintf("LOWER(p_cc.email_address) IN (%s)", strings.Join(placeholders, ",")))
	}

	// BCC filter
	if len(q.BccAddrs) > 0 {
		joins = append(joins, `
			JOIN message_recipients mr_bcc ON mr_bcc.message_id = m.id AND mr_bcc.recipient_type = 'bcc'
			JOIN participants p_bcc ON p_bcc.id = mr_bcc.participant_id
		`)
		placeholders := make([]string, len(q.BccAddrs))
		for i, addr := range q.BccAddrs {
			placeholders[i] = "?"
			args = append(args, addr)
		}
		conditions = append(conditions, fmt.Sprintf("LOWER(p_bcc.email_address) IN (%s)", strings.Join(placeholders, ",")))
	}

	// Label filter
	if len(q.Labels) > 0 {
		joins = append(joins, `
			JOIN message_labels ml ON ml.message_id = m.id
			JOIN labels l ON l.id = ml.label_id
		`)
		placeholders := make([]string, len(q.Labels))
		for i, label := range q.Labels {
			placeholders[i] = "?"
			args = append(args, label)
		}
		conditions = append(conditions, fmt.Sprintf("l.name IN (%s)", strings.Join(placeholders, ",")))
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

	return conditions, args, joins, ftsJoin
}

func (e *SQLiteEngine) Search(ctx context.Context, q *search.Query, limit, offset int) ([]MessageSummary, error) {
	conditions, args, joins, ftsJoin := e.buildSearchQueryParts(ctx, q)

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

// SearchFast searches message metadata only (no body text).
// For SQLite, this falls back to regular Search since FTS5 is fast enough
// and provides better results than metadata-only search.
// The filter parameter is applied to narrow the search scope.
func (e *SQLiteEngine) SearchFast(ctx context.Context, q *search.Query, filter MessageFilter, limit, offset int) ([]MessageSummary, error) {
	// Merge filter context into query - always AND with existing filters
	mergedQuery := MergeFilterIntoQuery(q, filter)

	// For SQLite, use the existing Search which has FTS5
	// and add contextual filtering through the merged query
	return e.Search(ctx, mergedQuery, limit, offset)
}

// MergeFilterIntoQuery combines a MessageFilter context with a search.Query.
// Context filters are appended to existing query filters.
//
// Note on semantics: Appending to FromAddrs/ToAddrs/Labels produces IN clauses
// with OR semantics within each dimension. This means if user searches "from:alice"
// and context has Sender=bob, the result matches alice OR bob. For strict AND
// intersection, we would need separate WHERE conditions per context filter.
// Current behavior: context widens the search within other constraints.
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

	return &merged
}

// SearchFastCount returns the total count of messages matching a search query.
// Uses the same query logic as Search to ensure consistent counts.
func (e *SQLiteEngine) SearchFastCount(ctx context.Context, q *search.Query, filter MessageFilter) (int64, error) {
	mergedQuery := MergeFilterIntoQuery(q, filter)

	// Build query using same logic as Search for consistency
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
		SourceID:            filter.SourceID,
		WithAttachmentsOnly: filter.WithAttachmentsOnly,
		SearchQuery:         queryStr,
		GroupBy:             statsGroupBy,
	}
	stats, _ := e.GetTotalStats(ctx, statsOpts)

	return &SearchFastResult{
		Messages:   results,
		TotalCount: count,
		Stats:      stats,
	}, nil
}
