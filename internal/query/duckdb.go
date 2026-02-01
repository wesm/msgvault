package query

import (
	"bytes"
	"compress/zlib"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	_ "github.com/marcboeker/go-duckdb"
	"github.com/wesm/msgvault/internal/mime"
	"github.com/wesm/msgvault/internal/search"
)

// DuckDBEngine implements Engine using DuckDB for fast Parquet queries.
// It uses a hybrid approach:
//   - DuckDB with Parquet for fast aggregate queries
//   - DuckDB's sqlite_scan for list queries (ListMessages, ListAccounts)
//   - Direct SQLite for FTS search and message body retrieval (sqlite_scan can't use FTS5)
//
// Deletion handling: The Python ETL excludes deleted messages (deleted_from_source_at IS NOT NULL)
// when building Parquet files. However, messages deleted AFTER the Parquet build will still
// appear in aggregates until the next `build-parquet --full-rebuild`. For the full deletion
// index solution, see beads issue msgvault-ozj.
type DuckDBEngine struct {
	db           *sql.DB
	analyticsDir string
	sqlitePath   string        // Path to SQLite database for sqlite_scan queries
	sqliteDB     *sql.DB       // Direct SQLite connection for FTS and body retrieval
	sqliteEngine *SQLiteEngine // Reusable engine for FTS cache, created once if sqliteDB is set
}

// NewDuckDBEngine creates a new DuckDB-backed query engine.
// analyticsDir should point to ~/.msgvault/analytics/
// sqlitePath should point to ~/.msgvault/msgvault.db
// sqliteDB is a direct SQLite connection for FTS search and body retrieval
//
// The engine uses a hybrid approach:
//   - DuckDB's sqlite_scan for list queries (ListMessages, ListAccounts, etc.)
//   - Direct SQLite (sqliteDB) for FTS search and message body retrieval
//
// If sqlitePath is empty, only aggregate queries and GetTotalStats will work.
// If sqliteDB is nil, Search will fall back to LIKE queries and body extraction
// from raw MIME may be slower.
func NewDuckDBEngine(analyticsDir string, sqlitePath string, sqliteDB *sql.DB) (*DuckDBEngine, error) {
	// Open in-memory DuckDB
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("open duckdb: %w", err)
	}

	// Constrain to single connection to ensure session settings (SET threads, ATTACH)
	// are applied consistently. DuckDB session settings don't propagate across
	// pooled connections, so limiting to one connection avoids inconsistent behavior.
	db.SetMaxOpenConns(1)

	// Enable multithreading for better query performance.
	// Use GOMAXPROCS(0) instead of NumCPU() to respect container CPU limits.
	threads := runtime.GOMAXPROCS(0)
	if _, err := db.Exec(fmt.Sprintf("SET threads = %d", threads)); err != nil {
		db.Close()
		return nil, fmt.Errorf("set threads: %w", err)
	}

	// Install and load SQLite extension if we have a SQLite path
	if sqlitePath != "" {
		if _, err := db.Exec("INSTALL sqlite; LOAD sqlite;"); err != nil {
			db.Close()
			return nil, fmt.Errorf("load sqlite extension: %w", err)
		}

		// Attach SQLite database as read-only
		// Escape single quotes in path to prevent SQL injection
		escapedPath := strings.ReplaceAll(sqlitePath, "'", "''")
		attachSQL := fmt.Sprintf("ATTACH '%s' AS sqlite_db (TYPE sqlite, READ_ONLY)", escapedPath)
		if _, err := db.Exec(attachSQL); err != nil {
			db.Close()
			return nil, fmt.Errorf("attach sqlite database: %w", err)
		}
	}

	// Create reusable SQLiteEngine if we have a direct connection
	// This preserves FTS cache across calls
	var sqliteEngine *SQLiteEngine
	if sqliteDB != nil {
		sqliteEngine = NewSQLiteEngine(sqliteDB)
	}

	return &DuckDBEngine{
		db:           db,
		analyticsDir: analyticsDir,
		sqlitePath:   sqlitePath,
		sqliteDB:     sqliteDB,
		sqliteEngine: sqliteEngine,
	}, nil
}

// Close releases DuckDB resources.
func (e *DuckDBEngine) Close() error {
	return e.db.Close()
}

// hasSQLite returns true if SQLite is available for detail queries.
func (e *DuckDBEngine) hasSQLite() bool {
	return e.sqlitePath != ""
}

// parquetGlob returns the glob pattern for reading message Parquet files.
func (e *DuckDBEngine) parquetGlob() string {
	return filepath.Join(e.analyticsDir, "messages", "**", "*.parquet")
}

// parquetPath returns the path pattern for a specific Parquet table.
func (e *DuckDBEngine) parquetPath(table string) string {
	return filepath.Join(e.analyticsDir, table, "*.parquet")
}

// parquetCTEs returns common CTEs for reading all Parquet tables.
// This is used by aggregate queries that need to join across tables.
func (e *DuckDBEngine) parquetCTEs() string {
	return fmt.Sprintf(`
		msg AS (
			SELECT * FROM read_parquet('%s', hive_partitioning=true)
		),
		mr AS (
			SELECT * FROM read_parquet('%s')
		),
		p AS (
			SELECT * FROM read_parquet('%s')
		),
		lbl AS (
			SELECT * FROM read_parquet('%s')
		),
		ml AS (
			SELECT * FROM read_parquet('%s')
		),
		att AS (
			SELECT message_id, SUM(size) as attachment_size, COUNT(*) as attachment_count
			FROM read_parquet('%s')
			GROUP BY message_id
		),
		src AS (
			SELECT * FROM read_parquet('%s')
		)
	`, e.parquetGlob(),
		e.parquetPath("message_recipients"),
		e.parquetPath("participants"),
		e.parquetPath("labels"),
		e.parquetPath("message_labels"),
		e.parquetPath("attachments"),
		e.parquetPath("sources"))
}

// escapeILIKE escapes ILIKE wildcard characters (% and _) in user input.
func escapeILIKE(s string) string {
	s = strings.ReplaceAll(s, "\\", "\\\\") // Escape backslash first
	s = strings.ReplaceAll(s, "%", "\\%")
	s = strings.ReplaceAll(s, "_", "\\_")
	return s
}

// buildWhereClause builds WHERE conditions for Parquet queries.
// Column references use msg. prefix to be explicit since aggregate queries join multiple CTEs.
// buildAggregateSearchConditions builds SQL conditions for a search query in aggregate views.
// Returns conditions and args that can be appended to existing conditions.
// buildAggregateSearchConditions builds WHERE conditions for aggregate search.
// keyColumns are SQL expressions for the grouping dimension that text terms
// should filter on (e.g. "p.email_address", "p.display_name"). When nil,
// text terms search subject + sender (the default for Senders/Time views).
func (e *DuckDBEngine) buildAggregateSearchConditions(searchQuery string, keyColumns ...string) ([]string, []interface{}) {
	if searchQuery == "" {
		return nil, nil
	}

	var conditions []string
	var args []interface{}

	q := search.Parse(searchQuery)

	// Text terms: filter on the view's grouping key columns when provided,
	// otherwise fall back to subject + sender search.
	for _, term := range q.TextTerms {
		termPattern := "%" + escapeILIKE(term) + "%"
		if len(keyColumns) > 0 {
			// Filter on the grouping dimension's columns
			var parts []string
			for _, col := range keyColumns {
				parts = append(parts, col+` ILIKE ? ESCAPE '\'`)
				args = append(args, termPattern)
			}
			conditions = append(conditions, "("+strings.Join(parts, " OR ")+")")
		} else {
			// Default: search subject and sender (for Senders/Time views)
			conditions = append(conditions, `(
				msg.subject ILIKE ? ESCAPE '\' OR
				EXISTS (
					SELECT 1 FROM mr mr_search
					JOIN p p_search ON p_search.id = mr_search.participant_id
					WHERE mr_search.message_id = msg.id
					  AND mr_search.recipient_type = 'from'
					  AND (p_search.email_address ILIKE ? ESCAPE '\' OR p_search.display_name ILIKE ? ESCAPE '\')
				)
			)`)
			args = append(args, termPattern, termPattern, termPattern)
		}
	}

	// from: filter - match sender email
	for _, from := range q.FromAddrs {
		fromPattern := "%" + escapeILIKE(from) + "%"
		conditions = append(conditions, `EXISTS (
			SELECT 1 FROM mr mr_from
			JOIN p p_from ON p_from.id = mr_from.participant_id
			WHERE mr_from.message_id = msg.id
			  AND mr_from.recipient_type = 'from'
			  AND p_from.email_address ILIKE ? ESCAPE '\'
		)`)
		args = append(args, fromPattern)
	}

	// to: filter - match recipient email (to or cc, consistent with SearchFast)
	for _, to := range q.ToAddrs {
		toPattern := "%" + escapeILIKE(to) + "%"
		conditions = append(conditions, `EXISTS (
			SELECT 1 FROM mr mr_to
			JOIN p p_to ON p_to.id = mr_to.participant_id
			WHERE mr_to.message_id = msg.id
			  AND mr_to.recipient_type IN ('to', 'cc')
			  AND p_to.email_address ILIKE ? ESCAPE '\'
		)`)
		args = append(args, toPattern)
	}

	// subject: filter
	for _, subj := range q.SubjectTerms {
		subjPattern := "%" + escapeILIKE(subj) + "%"
		conditions = append(conditions, "msg.subject ILIKE ? ESCAPE '\\'")
		args = append(args, subjPattern)
	}

	// label: filter - exact match (consistent with SearchFast)
	for _, label := range q.Labels {
		conditions = append(conditions, `EXISTS (
			SELECT 1 FROM ml ml_label
			JOIN l l_label ON l_label.id = ml_label.label_id
			WHERE ml_label.message_id = msg.id
			  AND l_label.name = ?
		)`)
		args = append(args, label)
	}

	// has:attachment filter
	if q.HasAttachment != nil && *q.HasAttachment {
		conditions = append(conditions, "msg.has_attachments = 1")
	}

	// Date filters from search query
	if q.AfterDate != nil {
		conditions = append(conditions, "msg.sent_at >= CAST(? AS TIMESTAMP)")
		args = append(args, q.AfterDate.Format("2006-01-02 15:04:05"))
	}
	if q.BeforeDate != nil {
		conditions = append(conditions, "msg.sent_at < CAST(? AS TIMESTAMP)")
		args = append(args, q.BeforeDate.Format("2006-01-02 15:04:05"))
	}

	// Size filters
	if q.LargerThan != nil {
		conditions = append(conditions, "msg.size_estimate > ?")
		args = append(args, *q.LargerThan)
	}
	if q.SmallerThan != nil {
		conditions = append(conditions, "msg.size_estimate < ?")
		args = append(args, *q.SmallerThan)
	}

	return conditions, args
}

// buildWhereClause builds WHERE conditions for aggregate queries.
// keyColumns are passed through to buildAggregateSearchConditions to control
// which columns text search terms filter on.
func (e *DuckDBEngine) buildWhereClause(opts AggregateOptions, keyColumns ...string) (string, []interface{}) {
	var conditions []string
	var args []interface{}

	if opts.SourceID != nil {
		conditions = append(conditions, "msg.source_id = ?")
		args = append(args, *opts.SourceID)
	}

	if opts.After != nil {
		conditions = append(conditions, "msg.sent_at >= CAST(? AS TIMESTAMP)")
		args = append(args, opts.After.Format("2006-01-02 15:04:05"))
	}

	if opts.Before != nil {
		conditions = append(conditions, "msg.sent_at < CAST(? AS TIMESTAMP)")
		args = append(args, opts.Before.Format("2006-01-02 15:04:05"))
	}

	if opts.WithAttachmentsOnly {
		conditions = append(conditions, "msg.has_attachments = 1")
	}

	// Text search filter for aggregates - filter on view's key columns
	searchConds, searchArgs := e.buildAggregateSearchConditions(opts.SearchQuery, keyColumns...)
	conditions = append(conditions, searchConds...)
	args = append(args, searchArgs...)

	if len(conditions) == 0 {
		return "1=1", args
	}
	return strings.Join(conditions, " AND "), args
}

// sortClause returns ORDER BY clause for aggregates.
func (e *DuckDBEngine) sortClause(opts AggregateOptions) string {
	field := "count"
	switch opts.SortField {
	case SortBySize:
		field = "total_size"
	case SortByAttachmentSize:
		field = "attachment_size"
	case SortByName:
		field = "key"
	}

	dir := "DESC"
	if opts.SortDirection == SortAsc {
		dir = "ASC"
	}

	return fmt.Sprintf("ORDER BY %s %s", field, dir)
}

// AggregateBySender groups messages by sender email.
func (e *DuckDBEngine) AggregateBySender(ctx context.Context, opts AggregateOptions) ([]AggregateRow, error) {
	where, args := e.buildWhereClause(opts)

	limit := opts.Limit
	if limit == 0 {
		limit = 100
	}

	// Join messages -> message_recipients (from) -> participants for sender email
	query := fmt.Sprintf(`
		WITH %s
		SELECT key, count, total_size, attachment_size, attachment_count, total_unique
		FROM (
			SELECT
				p.email_address as key,
				COUNT(*) as count,
				COALESCE(SUM(msg.size_estimate), 0) as total_size,
				CAST(COALESCE(SUM(att.attachment_size), 0) AS BIGINT) as attachment_size,
				CAST(COALESCE(SUM(att.attachment_count), 0) AS BIGINT) as attachment_count,
				COUNT(*) OVER() as total_unique
			FROM msg
			JOIN mr ON mr.message_id = msg.id AND mr.recipient_type = 'from'
			JOIN p ON p.id = mr.participant_id
			LEFT JOIN att ON att.message_id = msg.id
			WHERE %s AND p.email_address IS NOT NULL
			GROUP BY p.email_address
		)
		%s
		LIMIT ?
	`, e.parquetCTEs(), where, e.sortClause(opts))

	args = append(args, limit)
	return e.executeAggregateQuery(ctx, query, args)
}

// AggregateBySenderName groups messages by sender display name.
// Uses COALESCE(display_name, email_address) so senders without a display name
// fall back to their email address.
func (e *DuckDBEngine) AggregateBySenderName(ctx context.Context, opts AggregateOptions) ([]AggregateRow, error) {
	where, args := e.buildWhereClause(opts)

	limit := opts.Limit
	if limit == 0 {
		limit = 100
	}

	query := fmt.Sprintf(`
		WITH %s
		SELECT key, count, total_size, attachment_size, attachment_count, total_unique
		FROM (
			SELECT
				COALESCE(NULLIF(TRIM(p.display_name), ''), p.email_address) as key,
				COUNT(*) as count,
				COALESCE(SUM(msg.size_estimate), 0) as total_size,
				CAST(COALESCE(SUM(att.attachment_size), 0) AS BIGINT) as attachment_size,
				CAST(COALESCE(SUM(att.attachment_count), 0) AS BIGINT) as attachment_count,
				COUNT(*) OVER() as total_unique
			FROM msg
			JOIN mr ON mr.message_id = msg.id AND mr.recipient_type = 'from'
			JOIN p ON p.id = mr.participant_id
			LEFT JOIN att ON att.message_id = msg.id
			WHERE %s AND COALESCE(NULLIF(TRIM(p.display_name), ''), p.email_address) IS NOT NULL
			GROUP BY COALESCE(NULLIF(TRIM(p.display_name), ''), p.email_address)
		)
		%s
		LIMIT ?
	`, e.parquetCTEs(), where, e.sortClause(opts))

	args = append(args, limit)
	return e.executeAggregateQuery(ctx, query, args)
}

// AggregateByRecipient groups messages by recipient email.
// Includes to, cc recipients (bcc not exported to Parquet for privacy).
func (e *DuckDBEngine) AggregateByRecipient(ctx context.Context, opts AggregateOptions) ([]AggregateRow, error) {
	where, args := e.buildWhereClause(opts, "p.email_address", "p.display_name")

	limit := opts.Limit
	if limit == 0 {
		limit = 100
	}

	// Join messages -> message_recipients (to/cc) -> participants for recipient email
	query := fmt.Sprintf(`
		WITH %s
		SELECT key, count, total_size, attachment_size, attachment_count, total_unique
		FROM (
			SELECT
				p.email_address as key,
				COUNT(*) as count,
				COALESCE(SUM(msg.size_estimate), 0) as total_size,
				CAST(COALESCE(SUM(att.attachment_size), 0) AS BIGINT) as attachment_size,
				CAST(COALESCE(SUM(att.attachment_count), 0) AS BIGINT) as attachment_count,
				COUNT(*) OVER() as total_unique
			FROM msg
			JOIN mr ON mr.message_id = msg.id AND mr.recipient_type IN ('to', 'cc')
			JOIN p ON p.id = mr.participant_id
			LEFT JOIN att ON att.message_id = msg.id
			WHERE %s AND p.email_address IS NOT NULL
			GROUP BY p.email_address
		)
		%s
		LIMIT ?
	`, e.parquetCTEs(), where, e.sortClause(opts))

	args = append(args, limit)
	return e.executeAggregateQuery(ctx, query, args)
}

// AggregateByRecipientName groups messages by recipient display name.
// Uses COALESCE(display_name, email_address) so recipients without a display name
// fall back to their email address.
func (e *DuckDBEngine) AggregateByRecipientName(ctx context.Context, opts AggregateOptions) ([]AggregateRow, error) {
	where, args := e.buildWhereClause(opts, "p.email_address", "p.display_name")

	limit := opts.Limit
	if limit == 0 {
		limit = 100
	}

	query := fmt.Sprintf(`
		WITH %s
		SELECT key, count, total_size, attachment_size, attachment_count, total_unique
		FROM (
			SELECT
				COALESCE(NULLIF(TRIM(p.display_name), ''), p.email_address) as key,
				COUNT(*) as count,
				COALESCE(SUM(msg.size_estimate), 0) as total_size,
				CAST(COALESCE(SUM(att.attachment_size), 0) AS BIGINT) as attachment_size,
				CAST(COALESCE(SUM(att.attachment_count), 0) AS BIGINT) as attachment_count,
				COUNT(*) OVER() as total_unique
			FROM msg
			JOIN mr ON mr.message_id = msg.id AND mr.recipient_type IN ('to', 'cc')
			JOIN p ON p.id = mr.participant_id
			LEFT JOIN att ON att.message_id = msg.id
			WHERE %s AND COALESCE(NULLIF(TRIM(p.display_name), ''), p.email_address) IS NOT NULL
			GROUP BY COALESCE(NULLIF(TRIM(p.display_name), ''), p.email_address)
		)
		%s
		LIMIT ?
	`, e.parquetCTEs(), where, e.sortClause(opts))

	args = append(args, limit)
	return e.executeAggregateQuery(ctx, query, args)
}

// AggregateByDomain groups messages by sender domain.
func (e *DuckDBEngine) AggregateByDomain(ctx context.Context, opts AggregateOptions) ([]AggregateRow, error) {
	where, args := e.buildWhereClause(opts)

	limit := opts.Limit
	if limit == 0 {
		limit = 100
	}

	// Join messages -> message_recipients (from) -> participants for sender domain
	query := fmt.Sprintf(`
		WITH %s
		SELECT key, count, total_size, attachment_size, attachment_count, total_unique
		FROM (
			SELECT
				p.domain as key,
				COUNT(*) as count,
				COALESCE(SUM(msg.size_estimate), 0) as total_size,
				CAST(COALESCE(SUM(att.attachment_size), 0) AS BIGINT) as attachment_size,
				CAST(COALESCE(SUM(att.attachment_count), 0) AS BIGINT) as attachment_count,
				COUNT(*) OVER() as total_unique
			FROM msg
			JOIN mr ON mr.message_id = msg.id AND mr.recipient_type = 'from'
			JOIN p ON p.id = mr.participant_id
			LEFT JOIN att ON att.message_id = msg.id
			WHERE %s AND p.domain IS NOT NULL AND p.domain != ''
			GROUP BY p.domain
		)
		%s
		LIMIT ?
	`, e.parquetCTEs(), where, e.sortClause(opts))

	args = append(args, limit)
	return e.executeAggregateQuery(ctx, query, args)
}

// AggregateByLabel groups messages by label.
func (e *DuckDBEngine) AggregateByLabel(ctx context.Context, opts AggregateOptions) ([]AggregateRow, error) {
	where, args := e.buildWhereClause(opts, "lbl.name")

	limit := opts.Limit
	if limit == 0 {
		limit = 100
	}

	// Join messages -> message_labels -> labels for label name
	query := fmt.Sprintf(`
		WITH %s
		SELECT key, count, total_size, attachment_size, attachment_count, total_unique
		FROM (
			SELECT
				lbl.name as key,
				COUNT(*) as count,
				COALESCE(SUM(msg.size_estimate), 0) as total_size,
				CAST(COALESCE(SUM(att.attachment_size), 0) AS BIGINT) as attachment_size,
				CAST(COALESCE(SUM(att.attachment_count), 0) AS BIGINT) as attachment_count,
				COUNT(*) OVER() as total_unique
			FROM msg
			JOIN ml ON ml.message_id = msg.id
			JOIN lbl ON lbl.id = ml.label_id
			LEFT JOIN att ON att.message_id = msg.id
			WHERE %s AND lbl.name IS NOT NULL
			GROUP BY lbl.name
		)
		%s
		LIMIT ?
	`, e.parquetCTEs(), where, e.sortClause(opts))

	args = append(args, limit)
	return e.executeAggregateQuery(ctx, query, args)
}

// AggregateByTime groups messages by time period.
func (e *DuckDBEngine) AggregateByTime(ctx context.Context, opts AggregateOptions) ([]AggregateRow, error) {
	where, args := e.buildWhereClause(opts)

	limit := opts.Limit
	if limit == 0 {
		limit = 100
	}

	// Build time grouping expression based on granularity
	var timeExpr string
	switch opts.TimeGranularity {
	case TimeYear:
		timeExpr = "CAST(msg.year AS VARCHAR)"
	case TimeMonth:
		timeExpr = "CAST(msg.year AS VARCHAR) || '-' || LPAD(CAST(msg.month AS VARCHAR), 2, '0')"
	case TimeDay:
		timeExpr = "strftime(msg.sent_at, '%Y-%m-%d')"
	default:
		timeExpr = "CAST(msg.year AS VARCHAR) || '-' || LPAD(CAST(msg.month AS VARCHAR), 2, '0')"
	}

	// Time aggregation with attachment stats from separate table
	query := fmt.Sprintf(`
		WITH %s
		SELECT key, count, total_size, attachment_size, attachment_count, total_unique
		FROM (
			SELECT
				%s as key,
				COUNT(*) as count,
				COALESCE(SUM(msg.size_estimate), 0) as total_size,
				CAST(COALESCE(SUM(att.attachment_size), 0) AS BIGINT) as attachment_size,
				CAST(COALESCE(SUM(att.attachment_count), 0) AS BIGINT) as attachment_count,
				COUNT(*) OVER() as total_unique
			FROM msg
			LEFT JOIN att ON att.message_id = msg.id
			WHERE %s AND msg.sent_at IS NOT NULL
			GROUP BY key
		)
		%s
		LIMIT ?
	`, e.parquetCTEs(), timeExpr, where, e.sortClause(opts))

	args = append(args, limit)
	return e.executeAggregateQuery(ctx, query, args)
}

// buildFilterConditions builds WHERE conditions from a MessageFilter.
// Uses EXISTS subqueries for join-based filters (sender, recipient, label),
// which become semi-joins and avoid duplicates without needing DISTINCT.
func (e *DuckDBEngine) buildFilterConditions(filter MessageFilter) (string, []interface{}) {
	var conditions []string
	var args []interface{}

	if filter.SourceID != nil {
		conditions = append(conditions, "msg.source_id = ?")
		args = append(args, *filter.SourceID)
	}

	if filter.ConversationID != nil {
		conditions = append(conditions, "msg.conversation_id = ?")
		args = append(args, *filter.ConversationID)
	}

	if filter.After != nil {
		conditions = append(conditions, "msg.sent_at >= CAST(? AS TIMESTAMP)")
		args = append(args, filter.After.Format("2006-01-02 15:04:05"))
	}

	if filter.Before != nil {
		conditions = append(conditions, "msg.sent_at < CAST(? AS TIMESTAMP)")
		args = append(args, filter.Before.Format("2006-01-02 15:04:05"))
	}

	if filter.WithAttachmentsOnly {
		conditions = append(conditions, "msg.has_attachments = true")
	}

	// Sender filter - use EXISTS subquery (becomes semi-join)
	if filter.Sender != "" {
		conditions = append(conditions, `EXISTS (
			SELECT 1 FROM mr
			JOIN p ON p.id = mr.participant_id
			WHERE mr.message_id = msg.id
			  AND mr.recipient_type = 'from'
			  AND p.email_address = ?
		)`)
		args = append(args, filter.Sender)
	} else if filter.MatchEmptySender {
		conditions = append(conditions, `NOT EXISTS (
			SELECT 1 FROM mr
			JOIN p ON p.id = mr.participant_id
			WHERE mr.message_id = msg.id
			  AND mr.recipient_type = 'from'
			  AND p.email_address IS NOT NULL
			  AND p.email_address != ''
		)`)
	}

	// Sender name filter - use EXISTS subquery (becomes semi-join)
	if filter.SenderName != "" {
		conditions = append(conditions, `EXISTS (
			SELECT 1 FROM mr
			JOIN p ON p.id = mr.participant_id
			WHERE mr.message_id = msg.id
			  AND mr.recipient_type = 'from'
			  AND COALESCE(NULLIF(TRIM(p.display_name), ''), p.email_address) = ?
		)`)
		args = append(args, filter.SenderName)
	} else if filter.MatchEmptySenderName {
		conditions = append(conditions, `NOT EXISTS (
			SELECT 1 FROM mr
			JOIN p ON p.id = mr.participant_id
			WHERE mr.message_id = msg.id
			  AND mr.recipient_type = 'from'
			  AND COALESCE(NULLIF(TRIM(p.display_name), ''), p.email_address) IS NOT NULL
		)`)
	}

	// Recipient filter - use EXISTS subquery (becomes semi-join)
	if filter.Recipient != "" {
		conditions = append(conditions, `EXISTS (
			SELECT 1 FROM mr
			JOIN p ON p.id = mr.participant_id
			WHERE mr.message_id = msg.id
			  AND mr.recipient_type IN ('to', 'cc')
			  AND p.email_address = ?
		)`)
		args = append(args, filter.Recipient)
	} else if filter.MatchEmptyRecipient {
		conditions = append(conditions, "NOT EXISTS (SELECT 1 FROM mr WHERE mr.message_id = msg.id AND mr.recipient_type IN ('to', 'cc'))")
	}

	// Recipient name filter - use EXISTS subquery (becomes semi-join)
	if filter.RecipientName != "" {
		conditions = append(conditions, `EXISTS (
			SELECT 1 FROM mr
			JOIN p ON p.id = mr.participant_id
			WHERE mr.message_id = msg.id
			  AND mr.recipient_type IN ('to', 'cc')
			  AND COALESCE(NULLIF(TRIM(p.display_name), ''), p.email_address) = ?
		)`)
		args = append(args, filter.RecipientName)
	} else if filter.MatchEmptyRecipientName {
		conditions = append(conditions, `NOT EXISTS (
			SELECT 1 FROM mr
			JOIN p ON p.id = mr.participant_id
			WHERE mr.message_id = msg.id
			  AND mr.recipient_type IN ('to', 'cc')
			  AND COALESCE(NULLIF(TRIM(p.display_name), ''), p.email_address) IS NOT NULL
		)`)
	}

	// Domain filter - use EXISTS subquery (becomes semi-join)
	if filter.Domain != "" {
		conditions = append(conditions, `EXISTS (
			SELECT 1 FROM mr
			JOIN p ON p.id = mr.participant_id
			WHERE mr.message_id = msg.id
			  AND mr.recipient_type = 'from'
			  AND p.domain = ?
		)`)
		args = append(args, filter.Domain)
	} else if filter.MatchEmptyDomain {
		conditions = append(conditions, `NOT EXISTS (
			SELECT 1 FROM mr
			JOIN p ON p.id = mr.participant_id
			WHERE mr.message_id = msg.id
			  AND mr.recipient_type = 'from'
			  AND p.domain IS NOT NULL
			  AND p.domain != ''
		)`)
	}

	// Label filter - use EXISTS subquery (becomes semi-join)
	if filter.Label != "" {
		conditions = append(conditions, `EXISTS (
			SELECT 1 FROM ml
			JOIN lbl ON lbl.id = ml.label_id
			WHERE ml.message_id = msg.id
			  AND lbl.name = ?
		)`)
		args = append(args, filter.Label)
	} else if filter.MatchEmptyLabel {
		conditions = append(conditions, "NOT EXISTS (SELECT 1 FROM ml WHERE ml.message_id = msg.id)")
	}

	// Time period filter
	if filter.TimePeriod != "" {
		granularity := filter.TimeGranularity
		if granularity == TimeYear && len(filter.TimePeriod) > 4 {
			switch len(filter.TimePeriod) {
			case 7:
				granularity = TimeMonth
			case 10:
				granularity = TimeDay
			}
		}

		var timeExpr string
		switch granularity {
		case TimeYear:
			timeExpr = "CAST(msg.year AS VARCHAR)"
		case TimeMonth:
			timeExpr = "CAST(msg.year AS VARCHAR) || '-' || LPAD(CAST(msg.month AS VARCHAR), 2, '0')"
		case TimeDay:
			timeExpr = "strftime(msg.sent_at, '%Y-%m-%d')"
		default:
			timeExpr = "CAST(msg.year AS VARCHAR) || '-' || LPAD(CAST(msg.month AS VARCHAR), 2, '0')"
		}
		conditions = append(conditions, fmt.Sprintf("%s = ?", timeExpr))
		args = append(args, filter.TimePeriod)
	}

	if len(conditions) == 0 {
		return "1=1", args
	}
	return strings.Join(conditions, " AND "), args
}

// SubAggregate performs aggregation on a filtered subset of messages.
// This is used for sub-grouping after drill-down.
func (e *DuckDBEngine) SubAggregate(ctx context.Context, filter MessageFilter, groupBy ViewType, opts AggregateOptions) ([]AggregateRow, error) {
	where, args := e.buildFilterConditions(filter)

	// Add opts-based conditions (source_id, date range, attachment filter)
	if opts.SourceID != nil {
		where += " AND msg.source_id = ?"
		args = append(args, *opts.SourceID)
	}
	if opts.After != nil {
		where += " AND msg.sent_at >= CAST(? AS TIMESTAMP)"
		args = append(args, opts.After.Format("2006-01-02 15:04:05"))
	}
	if opts.Before != nil {
		where += " AND msg.sent_at < CAST(? AS TIMESTAMP)"
		args = append(args, opts.Before.Format("2006-01-02 15:04:05"))
	}
	if opts.WithAttachmentsOnly {
		where += " AND msg.has_attachments = true"
	}

	limit := opts.Limit
	if limit == 0 {
		limit = 100
	}

	// For 1:N views (Recipients, RecipientNames, Labels), search must filter
	// on the grouping key column to avoid inflated counts from summing across groups.
	// For 1:1 views (Senders, SenderNames, Domains, Time), the default
	// subject+sender search is correct and more useful.
	var searchKeyColumns []string
	switch groupBy {
	case ViewRecipients, ViewRecipientNames:
		searchKeyColumns = []string{"p_agg.email_address", "p_agg.display_name"}
	case ViewLabels:
		searchKeyColumns = []string{"lbl_agg.name"}
	}

	// Add search query conditions (for filtered drill-down)
	searchConds, searchArgs := e.buildAggregateSearchConditions(opts.SearchQuery, searchKeyColumns...)
	for _, cond := range searchConds {
		where += " AND " + cond
	}
	args = append(args, searchArgs...)

	var query string
	switch groupBy {
	case ViewSenders:
		query = fmt.Sprintf(`
			WITH %s
			SELECT key, count, total_size, attachment_size, attachment_count, total_unique
			FROM (
				SELECT
					p_agg.email_address as key,
					COUNT(*) as count,
					COALESCE(SUM(msg.size_estimate), 0) as total_size,
					CAST(COALESCE(SUM(att.attachment_size), 0) AS BIGINT) as attachment_size,
					CAST(COALESCE(SUM(att.attachment_count), 0) AS BIGINT) as attachment_count,
					COUNT(*) OVER() as total_unique
				FROM msg
				JOIN mr mr_agg ON mr_agg.message_id = msg.id AND mr_agg.recipient_type = 'from'
				JOIN p p_agg ON p_agg.id = mr_agg.participant_id
				LEFT JOIN att ON att.message_id = msg.id
				WHERE %s AND p_agg.email_address IS NOT NULL
				GROUP BY p_agg.email_address
			)
			%s
			LIMIT ?
		`, e.parquetCTEs(), where, e.sortClause(opts))

	case ViewSenderNames:
		query = fmt.Sprintf(`
			WITH %s
			SELECT key, count, total_size, attachment_size, attachment_count, total_unique
			FROM (
				SELECT
					COALESCE(NULLIF(TRIM(p_agg.display_name), ''), p_agg.email_address) as key,
					COUNT(*) as count,
					COALESCE(SUM(msg.size_estimate), 0) as total_size,
					CAST(COALESCE(SUM(att.attachment_size), 0) AS BIGINT) as attachment_size,
					CAST(COALESCE(SUM(att.attachment_count), 0) AS BIGINT) as attachment_count,
					COUNT(*) OVER() as total_unique
				FROM msg
				JOIN mr mr_agg ON mr_agg.message_id = msg.id AND mr_agg.recipient_type = 'from'
				JOIN p p_agg ON p_agg.id = mr_agg.participant_id
				LEFT JOIN att ON att.message_id = msg.id
				WHERE %s AND COALESCE(NULLIF(TRIM(p_agg.display_name), ''), p_agg.email_address) IS NOT NULL
				GROUP BY COALESCE(NULLIF(TRIM(p_agg.display_name), ''), p_agg.email_address)
			)
			%s
			LIMIT ?
		`, e.parquetCTEs(), where, e.sortClause(opts))

	case ViewRecipients:
		query = fmt.Sprintf(`
			WITH %s
			SELECT key, count, total_size, attachment_size, attachment_count, total_unique
			FROM (
				SELECT
					p_agg.email_address as key,
					COUNT(*) as count,
					COALESCE(SUM(msg.size_estimate), 0) as total_size,
					CAST(COALESCE(SUM(att.attachment_size), 0) AS BIGINT) as attachment_size,
					CAST(COALESCE(SUM(att.attachment_count), 0) AS BIGINT) as attachment_count,
					COUNT(*) OVER() as total_unique
				FROM msg
				JOIN mr mr_agg ON mr_agg.message_id = msg.id AND mr_agg.recipient_type IN ('to', 'cc')
				JOIN p p_agg ON p_agg.id = mr_agg.participant_id
				LEFT JOIN att ON att.message_id = msg.id
				WHERE %s AND p_agg.email_address IS NOT NULL
				GROUP BY p_agg.email_address
			)
			%s
			LIMIT ?
		`, e.parquetCTEs(), where, e.sortClause(opts))

	case ViewRecipientNames:
		query = fmt.Sprintf(`
			WITH %s
			SELECT key, count, total_size, attachment_size, attachment_count, total_unique
			FROM (
				SELECT
					COALESCE(NULLIF(TRIM(p_agg.display_name), ''), p_agg.email_address) as key,
					COUNT(*) as count,
					COALESCE(SUM(msg.size_estimate), 0) as total_size,
					CAST(COALESCE(SUM(att.attachment_size), 0) AS BIGINT) as attachment_size,
					CAST(COALESCE(SUM(att.attachment_count), 0) AS BIGINT) as attachment_count,
					COUNT(*) OVER() as total_unique
				FROM msg
				JOIN mr mr_agg ON mr_agg.message_id = msg.id AND mr_agg.recipient_type IN ('to', 'cc')
				JOIN p p_agg ON p_agg.id = mr_agg.participant_id
				LEFT JOIN att ON att.message_id = msg.id
				WHERE %s AND COALESCE(NULLIF(TRIM(p_agg.display_name), ''), p_agg.email_address) IS NOT NULL
				GROUP BY COALESCE(NULLIF(TRIM(p_agg.display_name), ''), p_agg.email_address)
			)
			%s
			LIMIT ?
		`, e.parquetCTEs(), where, e.sortClause(opts))

	case ViewDomains:
		query = fmt.Sprintf(`
			WITH %s
			SELECT key, count, total_size, attachment_size, attachment_count, total_unique
			FROM (
				SELECT
					p_agg.domain as key,
					COUNT(*) as count,
					COALESCE(SUM(msg.size_estimate), 0) as total_size,
					CAST(COALESCE(SUM(att.attachment_size), 0) AS BIGINT) as attachment_size,
					CAST(COALESCE(SUM(att.attachment_count), 0) AS BIGINT) as attachment_count,
					COUNT(*) OVER() as total_unique
				FROM msg
				JOIN mr mr_agg ON mr_agg.message_id = msg.id AND mr_agg.recipient_type = 'from'
				JOIN p p_agg ON p_agg.id = mr_agg.participant_id
				LEFT JOIN att ON att.message_id = msg.id
				WHERE %s AND p_agg.domain IS NOT NULL
				GROUP BY p_agg.domain
			)
			%s
			LIMIT ?
		`, e.parquetCTEs(), where, e.sortClause(opts))

	case ViewLabels:
		query = fmt.Sprintf(`
			WITH %s
			SELECT key, count, total_size, attachment_size, attachment_count, total_unique
			FROM (
				SELECT
					lbl_agg.name as key,
					COUNT(*) as count,
					COALESCE(SUM(msg.size_estimate), 0) as total_size,
					CAST(COALESCE(SUM(att.attachment_size), 0) AS BIGINT) as attachment_size,
					CAST(COALESCE(SUM(att.attachment_count), 0) AS BIGINT) as attachment_count,
					COUNT(*) OVER() as total_unique
				FROM msg
				JOIN ml ml_agg ON ml_agg.message_id = msg.id
				JOIN lbl lbl_agg ON lbl_agg.id = ml_agg.label_id
				LEFT JOIN att ON att.message_id = msg.id
				WHERE %s AND lbl_agg.name IS NOT NULL
				GROUP BY lbl_agg.name
			)
			%s
			LIMIT ?
		`, e.parquetCTEs(), where, e.sortClause(opts))

	case ViewTime:
		var timeExpr string
		switch opts.TimeGranularity {
		case TimeYear:
			timeExpr = "CAST(msg.year AS VARCHAR)"
		case TimeMonth:
			timeExpr = "CAST(msg.year AS VARCHAR) || '-' || LPAD(CAST(msg.month AS VARCHAR), 2, '0')"
		case TimeDay:
			timeExpr = "strftime(msg.sent_at, '%Y-%m-%d')"
		default:
			timeExpr = "CAST(msg.year AS VARCHAR) || '-' || LPAD(CAST(msg.month AS VARCHAR), 2, '0')"
		}
		query = fmt.Sprintf(`
			WITH %s
			SELECT key, count, total_size, attachment_size, attachment_count, total_unique
			FROM (
				SELECT
					%s as key,
					COUNT(*) as count,
					COALESCE(SUM(msg.size_estimate), 0) as total_size,
					CAST(COALESCE(SUM(att.attachment_size), 0) AS BIGINT) as attachment_size,
					CAST(COALESCE(SUM(att.attachment_count), 0) AS BIGINT) as attachment_count,
					COUNT(*) OVER() as total_unique
				FROM msg
				LEFT JOIN att ON att.message_id = msg.id
				WHERE %s AND msg.sent_at IS NOT NULL
				GROUP BY key
			)
			%s
			LIMIT ?
		`, e.parquetCTEs(), timeExpr, where, e.sortClause(opts))

	default:
		return nil, fmt.Errorf("unsupported groupBy view type: %v", groupBy)
	}

	args = append(args, limit)
	return e.executeAggregateQuery(ctx, query, args)
}

// executeAggregateQuery runs an aggregate query and returns the results.
// Expects 6 columns: key, count, total_size, attachment_size, attachment_count, total_unique
func (e *DuckDBEngine) executeAggregateQuery(ctx context.Context, query string, args []interface{}) ([]AggregateRow, error) {
	rows, err := e.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("aggregate query: %w", err)
	}
	defer rows.Close()

	var results []AggregateRow
	for rows.Next() {
		var row AggregateRow
		// SQL uses CAST(... AS BIGINT) so we can scan directly into int64
		var attachmentSize sql.NullInt64
		var attachmentCount sql.NullInt64
		if err := rows.Scan(&row.Key, &row.Count, &row.TotalSize, &attachmentSize, &attachmentCount, &row.TotalUnique); err != nil {
			return nil, fmt.Errorf("scan aggregate row: %w", err)
		}
		if attachmentSize.Valid {
			row.AttachmentSize = attachmentSize.Int64
		}
		if attachmentCount.Valid {
			row.AttachmentCount = attachmentCount.Int64
		}
		results = append(results, row)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate aggregate rows: %w", err)
	}

	return results, nil
}

// GetTotalStats returns overall statistics from Parquet.
func (e *DuckDBEngine) GetTotalStats(ctx context.Context, opts StatsOptions) (*TotalStats, error) {
	stats := &TotalStats{}

	var conditions []string
	var args []interface{}

	if opts.SourceID != nil {
		conditions = append(conditions, "msg.source_id = ?")
		args = append(args, *opts.SourceID)
	}

	if opts.WithAttachmentsOnly {
		conditions = append(conditions, "msg.has_attachments = 1")
	}

	// Search filter — uses EXISTS subqueries so no row multiplication
	if opts.SearchQuery != "" {
		searchConds, searchArgs := e.buildAggregateSearchConditions(opts.SearchQuery)
		conditions = append(conditions, searchConds...)
		args = append(args, searchArgs...)
	}

	whereClause := "1=1"
	if len(conditions) > 0 {
		whereClause = strings.Join(conditions, " AND ")
	}

	// Message stats - join with attachment aggregates
	msgQuery := fmt.Sprintf(`
		WITH %s
		SELECT
			COUNT(*) as message_count,
			COALESCE(SUM(msg.size_estimate), 0) as total_size,
			CAST(COALESCE(SUM(att.attachment_count), 0) AS BIGINT) as attachment_count,
			CAST(COALESCE(SUM(att.attachment_size), 0) AS BIGINT) as attachment_size,
			COUNT(DISTINCT msg.source_id) as account_count
		FROM msg
		LEFT JOIN att ON att.message_id = msg.id
		WHERE %s
	`, e.parquetCTEs(), whereClause)

	var attachmentSize sql.NullFloat64
	err := e.db.QueryRowContext(ctx, msgQuery, args...).Scan(
		&stats.MessageCount,
		&stats.TotalSize,
		&stats.AttachmentCount,
		&attachmentSize,
		&stats.AccountCount,
	)
	if err != nil {
		return nil, fmt.Errorf("stats query: %w", err)
	}

	if attachmentSize.Valid {
		stats.AttachmentSize = int64(attachmentSize.Float64)
	}

	// Label count from joined tables
	labelQuery := fmt.Sprintf(`
		WITH %s
		SELECT COUNT(DISTINCT lbl.name)
		FROM msg
		JOIN ml ON ml.message_id = msg.id
		JOIN lbl ON lbl.id = ml.label_id
		WHERE %s
	`, e.parquetCTEs(), whereClause)

	if err := e.db.QueryRowContext(ctx, labelQuery, args...).Scan(&stats.LabelCount); err != nil {
		// Non-fatal: label count is informational, but log for debugging
		log.Printf("warning: label count query failed (using 0): %v", err)
		stats.LabelCount = 0
	}

	return stats, nil
}

// ListAccounts returns accounts from SQLite via DuckDB's sqlite_scan.
func (e *DuckDBEngine) ListAccounts(ctx context.Context) ([]AccountInfo, error) {
	if !e.hasSQLite() {
		return nil, fmt.Errorf("ListAccounts requires SQLite: pass sqlitePath to NewDuckDBEngine")
	}

	rows, err := e.db.QueryContext(ctx, `
		SELECT id, source_type, identifier, COALESCE(display_name, '')
		FROM sqlite_db.sources
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

// ListMessages retrieves messages from Parquet files for fast filtered queries.
// Joins normalized Parquet tables to reconstruct denormalized view.
func (e *DuckDBEngine) ListMessages(ctx context.Context, filter MessageFilter) ([]MessageSummary, error) {
	where, args := e.buildFilterConditions(filter)

	// Build ORDER BY
	var orderBy string
	switch filter.SortField {
	case MessageSortByDate:
		orderBy = "msg.sent_at"
	case MessageSortBySize:
		orderBy = "msg.size_estimate"
	case MessageSortBySubject:
		orderBy = "msg.subject"
	default:
		orderBy = "msg.sent_at"
	}
	if filter.SortDirection == SortDesc {
		orderBy += " DESC"
	} else {
		orderBy += " ASC"
	}

	limit := filter.Limit
	if limit == 0 {
		limit = 500
	}

	// Optimized query structure:
	// 1. filtered_msgs: filter and paginate message IDs first (EXISTS becomes semi-join)
	// 2. msg_sender: only compute sender info for the filtered messages
	// 3. Final SELECT: join filtered messages with sender info
	query := fmt.Sprintf(`
		WITH %s,
		filtered_msgs AS (
			SELECT msg.id
			FROM msg
			WHERE %s
			ORDER BY %s
			LIMIT ? OFFSET ?
		),
		msg_sender AS (
			SELECT mr.message_id,
				   FIRST(p.email_address) as from_email,
				   FIRST(COALESCE(mr.display_name, p.display_name, '')) as from_name
			FROM mr
			JOIN p ON p.id = mr.participant_id
			WHERE mr.recipient_type = 'from'
			  AND mr.message_id IN (SELECT id FROM filtered_msgs)
			GROUP BY mr.message_id
		)
		SELECT
			msg.id,
			COALESCE(msg.source_message_id, '') as source_message_id,
			COALESCE(msg.conversation_id, 0) as conversation_id,
			COALESCE(msg.subject, '') as subject,
			COALESCE(msg.snippet, '') as snippet,
			COALESCE(ms.from_email, '') as from_email,
			COALESCE(ms.from_name, '') as from_name,
			msg.sent_at,
			COALESCE(msg.size_estimate, 0) as size_estimate,
			COALESCE(msg.has_attachments, false) as has_attachments,
			msg.deleted_from_source_at
		FROM msg
		JOIN filtered_msgs fm ON fm.id = msg.id
		LEFT JOIN msg_sender ms ON ms.message_id = msg.id
		ORDER BY %s
	`, e.parquetCTEs(), where, orderBy, orderBy)

	args = append(args, limit, filter.Offset)

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
			&msg.Subject,
			&msg.Snippet,
			&msg.FromEmail,
			&msg.FromName,
			&sentAt,
			&msg.SizeEstimate,
			&msg.HasAttachments,
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

	return results, nil
}

// parseLabelsJSON parses JSON array format into string slice.
// We use to_json(labels) in the SQL query to get proper JSON encoding,
// which handles commas, quotes, and special characters in label names.
func parseLabelsJSON(s string) []string {
	if s == "" || s == "[]" || s == "null" {
		return nil
	}
	var labels []string
	if err := json.Unmarshal([]byte(s), &labels); err != nil {
		// Fallback: if JSON parsing fails, return empty
		return nil
	}
	return labels
}

// fetchLabelsForMessages adds labels to message summaries.
func (e *DuckDBEngine) fetchLabelsForMessages(ctx context.Context, messages []MessageSummary) error {
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
		FROM sqlite_db.message_labels ml
		JOIN sqlite_db.labels l ON l.id = ml.label_id
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

// GetMessage retrieves a full message from SQLite.
// Uses direct SQLite connection when available for better BLOB handling.
func (e *DuckDBEngine) GetMessage(ctx context.Context, id int64) (*MessageDetail, error) {
	// Prefer direct SQLite for body/BLOB retrieval
	if e.sqliteEngine != nil {
		return e.sqliteEngine.GetMessage(ctx, id)
	}

	// Fall back to sqlite_scan
	if !e.hasSQLite() {
		return nil, fmt.Errorf("GetMessage requires SQLite: pass sqlitePath to NewDuckDBEngine")
	}

	return e.getMessageByQuery(ctx, "m.id = ?", id)
}

// GetMessageBySourceID retrieves a message by source ID from SQLite.
// Uses direct SQLite connection when available for better BLOB handling.
func (e *DuckDBEngine) GetMessageBySourceID(ctx context.Context, sourceMessageID string) (*MessageDetail, error) {
	// Prefer direct SQLite for body/BLOB retrieval
	if e.sqliteEngine != nil {
		return e.sqliteEngine.GetMessageBySourceID(ctx, sourceMessageID)
	}

	// Fall back to sqlite_scan
	if !e.hasSQLite() {
		return nil, fmt.Errorf("GetMessageBySourceID requires SQLite: pass sqlitePath to NewDuckDBEngine")
	}

	return e.getMessageByQuery(ctx, "m.source_message_id = ?", sourceMessageID)
}

func (e *DuckDBEngine) getMessageByQuery(ctx context.Context, whereClause string, args ...interface{}) (*MessageDetail, error) {
	query := fmt.Sprintf(`
		SELECT
			m.id,
			m.source_message_id,
			m.conversation_id,
			COALESCE(m.subject, ''),
			COALESCE(m.snippet, ''),
			m.sent_at,
			m.received_at,
			COALESCE(m.size_estimate, 0),
			m.has_attachments
		FROM sqlite_db.messages m
		WHERE %s
	`, whereClause)

	var msg MessageDetail
	var sentAt, receivedAt sql.NullTime
	err := e.db.QueryRowContext(ctx, query, args...).Scan(
		&msg.ID,
		&msg.SourceMessageID,
		&msg.ConversationID,
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

	// Fetch body from separate table (PK lookup)
	var bodyText, bodyHTML sql.NullString
	err = e.db.QueryRowContext(ctx, `
		SELECT body_text, body_html FROM sqlite_db.message_bodies WHERE message_id = ?
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
	if err := e.fetchMessageLabels(ctx, &msg); err != nil {
		return nil, fmt.Errorf("fetch labels: %w", err)
	}

	// Fetch attachments
	if err := e.fetchAttachments(ctx, &msg); err != nil {
		return nil, fmt.Errorf("fetch attachments: %w", err)
	}

	return &msg, nil
}

// extractBodyFromRaw extracts text body from compressed MIME data.
func (e *DuckDBEngine) extractBodyFromRaw(ctx context.Context, messageID int64) (string, error) {
	var compressed []byte
	var compression sql.NullString

	err := e.db.QueryRowContext(ctx, `
		SELECT raw_data, compression FROM sqlite_db.message_raw WHERE message_id = ?
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

func (e *DuckDBEngine) fetchParticipants(ctx context.Context, msg *MessageDetail) error {
	rows, err := e.db.QueryContext(ctx, `
		SELECT mr.recipient_type, p.email_address, COALESCE(mr.display_name, p.display_name, '')
		FROM sqlite_db.message_recipients mr
		JOIN sqlite_db.participants p ON p.id = mr.participant_id
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

func (e *DuckDBEngine) fetchMessageLabels(ctx context.Context, msg *MessageDetail) error {
	rows, err := e.db.QueryContext(ctx, `
		SELECT l.name
		FROM sqlite_db.message_labels ml
		JOIN sqlite_db.labels l ON l.id = ml.label_id
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

func (e *DuckDBEngine) fetchAttachments(ctx context.Context, msg *MessageDetail) error {
	rows, err := e.db.QueryContext(ctx, `
		SELECT id, COALESCE(filename, ''), COALESCE(mime_type, ''), COALESCE(size, 0), COALESCE(content_hash, '')
		FROM sqlite_db.attachments
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

// Search performs a Gmail-style search query.
// Uses direct SQLite connection for FTS5 support when available,
// falls back to LIKE queries via sqlite_scan otherwise.
func (e *DuckDBEngine) Search(ctx context.Context, q *search.Query, limit, offset int) ([]MessageSummary, error) {
	// Prefer direct SQLite for FTS5 support
	if e.sqliteEngine != nil {
		return e.sqliteEngine.Search(ctx, q, limit, offset)
	}

	// Fall back to sqlite_scan with LIKE queries (no FTS)
	if !e.hasSQLite() {
		return nil, fmt.Errorf("Search requires SQLite: pass sqlitePath to NewDuckDBEngine")
	}

	var conditions []string
	var args []interface{}
	var joins []string

	// Include all messages (deleted messages shown with indicator in TUI)

	// From filter
	if len(q.FromAddrs) > 0 {
		joins = append(joins, `
			JOIN sqlite_db.message_recipients mr_from ON mr_from.message_id = m.id AND mr_from.recipient_type = 'from'
			JOIN sqlite_db.participants p_from ON p_from.id = mr_from.participant_id
		`)
		placeholders := make([]string, len(q.FromAddrs))
		for i, addr := range q.FromAddrs {
			placeholders[i] = "?"
			args = append(args, addr)
		}
		conditions = append(conditions, fmt.Sprintf("LOWER(p_from.email_address) IN (%s)", strings.Join(placeholders, ",")))
	}

	// To filter
	if len(q.ToAddrs) > 0 {
		joins = append(joins, `
			JOIN sqlite_db.message_recipients mr_to ON mr_to.message_id = m.id AND mr_to.recipient_type = 'to'
			JOIN sqlite_db.participants p_to ON p_to.id = mr_to.participant_id
		`)
		placeholders := make([]string, len(q.ToAddrs))
		for i, addr := range q.ToAddrs {
			placeholders[i] = "?"
			args = append(args, addr)
		}
		conditions = append(conditions, fmt.Sprintf("LOWER(p_to.email_address) IN (%s)", strings.Join(placeholders, ",")))
	}

	// Label filter
	if len(q.Labels) > 0 {
		joins = append(joins, `
			JOIN sqlite_db.message_labels ml ON ml.message_id = m.id
			JOIN sqlite_db.labels l ON l.id = ml.label_id
		`)
		placeholders := make([]string, len(q.Labels))
		for i, label := range q.Labels {
			placeholders[i] = "?"
			args = append(args, label)
		}
		conditions = append(conditions, fmt.Sprintf("l.name IN (%s)", strings.Join(placeholders, ",")))
	}

	// Subject filter (case-insensitive with ILIKE)
	if len(q.SubjectTerms) > 0 {
		for _, term := range q.SubjectTerms {
			conditions = append(conditions, "m.subject ILIKE ?")
			args = append(args, "%"+term+"%")
		}
	}

	// Has attachment filter
	if q.HasAttachment != nil && *q.HasAttachment {
		conditions = append(conditions, "m.has_attachments = 1")
	}

	// Date range filters
	if q.AfterDate != nil {
		conditions = append(conditions, "m.sent_at >= CAST(? AS TIMESTAMP)")
		args = append(args, q.AfterDate.Format("2006-01-02 15:04:05"))
	}
	if q.BeforeDate != nil {
		conditions = append(conditions, "m.sent_at < CAST(? AS TIMESTAMP)")
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

	// Full-text search: use ILIKE fallback (FTS5 not available via sqlite_scan)
	// Only search subject/snippet; body is in separate table, use FTS for body search
	if len(q.TextTerms) > 0 {
		for _, term := range q.TextTerms {
			likeTerm := "%" + term + "%"
			conditions = append(conditions, "(m.subject ILIKE ? OR m.snippet ILIKE ?)")
			args = append(args, likeTerm, likeTerm)
		}
	}

	// Account filter
	if q.AccountID != nil {
		conditions = append(conditions, "m.source_id = ?")
		args = append(args, *q.AccountID)
	}

	if limit == 0 {
		limit = 100
	}

	query := fmt.Sprintf(`
		SELECT DISTINCT
			m.id,
			m.source_message_id,
			m.conversation_id,
			COALESCE(m.subject, ''),
			COALESCE(m.snippet, ''),
			COALESCE(p_sender.email_address, ''),
			COALESCE(p_sender.display_name, ''),
			m.sent_at,
			COALESCE(m.size_estimate, 0),
			m.has_attachments,
			m.attachment_count,
			m.deleted_from_source_at
		FROM sqlite_db.messages m
		LEFT JOIN sqlite_db.message_recipients mr_sender ON mr_sender.message_id = m.id AND mr_sender.recipient_type = 'from'
		LEFT JOIN sqlite_db.participants p_sender ON p_sender.id = mr_sender.participant_id
		%s
		WHERE %s
		ORDER BY m.sent_at DESC
		LIMIT ? OFFSET ?
	`, strings.Join(joins, "\n"), strings.Join(conditions, " AND "))

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

// GetGmailIDsByFilter returns Gmail IDs matching a filter from Parquet files.
// Uses EXISTS subqueries for efficient semi-join filtering.
func (e *DuckDBEngine) GetGmailIDsByFilter(ctx context.Context, filter MessageFilter) ([]string, error) {
	if e.analyticsDir == "" {
		return nil, fmt.Errorf("GetGmailIDsByFilter requires Parquet data: pass analyticsDir to NewDuckDBEngine")
	}

	var conditions []string
	var args []interface{}

	// Always exclude deleted messages
	conditions = append(conditions, "msg.deleted_from_source_at IS NULL")

	if filter.SourceID != nil {
		conditions = append(conditions, "msg.source_id = ?")
		args = append(args, *filter.SourceID)
	}

	// Use EXISTS subqueries for filtering (becomes semi-joins, no duplicates)
	if filter.Sender != "" {
		conditions = append(conditions, `EXISTS (
			SELECT 1 FROM mr
			JOIN p ON p.id = mr.participant_id
			WHERE mr.message_id = msg.id
			  AND mr.recipient_type = 'from'
			  AND p.email_address = ?
		)`)
		args = append(args, filter.Sender)
	}

	if filter.SenderName != "" {
		conditions = append(conditions, `EXISTS (
			SELECT 1 FROM mr
			JOIN p ON p.id = mr.participant_id
			WHERE mr.message_id = msg.id
			  AND mr.recipient_type = 'from'
			  AND COALESCE(NULLIF(TRIM(p.display_name), ''), p.email_address) = ?
		)`)
		args = append(args, filter.SenderName)
	}

	if filter.Recipient != "" {
		conditions = append(conditions, `EXISTS (
			SELECT 1 FROM mr
			JOIN p ON p.id = mr.participant_id
			WHERE mr.message_id = msg.id
			  AND mr.recipient_type IN ('to', 'cc', 'bcc')
			  AND p.email_address = ?
		)`)
		args = append(args, filter.Recipient)
	}

	if filter.RecipientName != "" {
		conditions = append(conditions, `EXISTS (
			SELECT 1 FROM mr
			JOIN p ON p.id = mr.participant_id
			WHERE mr.message_id = msg.id
			  AND mr.recipient_type IN ('to', 'cc')
			  AND COALESCE(NULLIF(TRIM(p.display_name), ''), p.email_address) = ?
		)`)
		args = append(args, filter.RecipientName)
	}

	if filter.Domain != "" {
		conditions = append(conditions, `EXISTS (
			SELECT 1 FROM mr
			JOIN p ON p.id = mr.participant_id
			WHERE mr.message_id = msg.id
			  AND mr.recipient_type = 'from'
			  AND p.domain = ?
		)`)
		args = append(args, filter.Domain)
	}

	if filter.Label != "" {
		conditions = append(conditions, `EXISTS (
			SELECT 1 FROM ml
			JOIN lbl ON lbl.id = ml.label_id
			WHERE ml.message_id = msg.id
			  AND lbl.name = ?
		)`)
		args = append(args, filter.Label)
	}

	if filter.TimePeriod != "" {
		granularity := filter.TimeGranularity
		if granularity == TimeYear && len(filter.TimePeriod) > 4 {
			switch len(filter.TimePeriod) {
			case 7:
				granularity = TimeMonth
			case 10:
				granularity = TimeDay
			}
		}

		var timeExpr string
		switch granularity {
		case TimeYear:
			timeExpr = "strftime(msg.sent_at, '%Y')"
		case TimeMonth:
			timeExpr = "strftime(msg.sent_at, '%Y-%m')"
		case TimeDay:
			timeExpr = "strftime(msg.sent_at, '%Y-%m-%d')"
		default:
			timeExpr = "strftime(msg.sent_at, '%Y-%m')"
		}
		conditions = append(conditions, fmt.Sprintf("%s = ?", timeExpr))
		args = append(args, filter.TimePeriod)
	}

	// Build query
	query := fmt.Sprintf(`
		WITH %s
		SELECT msg.source_message_id
		FROM msg
		WHERE %s
	`, e.parquetCTEs(), strings.Join(conditions, " AND "))

	// Only add LIMIT if explicitly set (0 means no limit)
	if filter.Limit > 0 {
		query += " LIMIT ?"
		args = append(args, filter.Limit)
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

// HasParquetData checks if Parquet files exist and are usable.
func HasParquetData(analyticsDir string) bool {
	pattern := filepath.Join(analyticsDir, "messages", "**", "*.parquet")
	matches, err := filepath.Glob(filepath.Join(analyticsDir, "messages", "*", "*.parquet"))
	if err != nil {
		return false
	}
	_ = pattern // Used in queries, not glob
	return len(matches) > 0
}

// ParquetSyncState represents the sync state from _last_sync.json.
type ParquetSyncState struct {
	LastMessageID int64     `json:"last_message_id"`
	LastSyncAt    time.Time `json:"last_sync_at,omitempty"`
}

// SearchFast searches message metadata in Parquet files (no body text).
// This is much faster than FTS search for large archives.
// Searches: subject, sender email/name (case-insensitive).
func (e *DuckDBEngine) SearchFast(ctx context.Context, q *search.Query, filter MessageFilter, limit, offset int) ([]MessageSummary, error) {
	conditions, args := e.buildSearchConditions(q, filter)

	if limit == 0 {
		limit = 100
	}

	// Query with JOINs to reconstruct denormalized view
	query := fmt.Sprintf(`
		WITH %s,
		msg_labels AS (
			SELECT ml.message_id, LIST(lbl.name ORDER BY lbl.name) as labels
			FROM ml
			JOIN lbl ON lbl.id = ml.label_id
			GROUP BY ml.message_id
		),
		msg_sender AS (
			SELECT mr.message_id,
				   FIRST(p.email_address) as from_email,
				   FIRST(COALESCE(mr.display_name, p.display_name, '')) as from_name
			FROM mr
			JOIN p ON p.id = mr.participant_id
			WHERE mr.recipient_type = 'from'
			GROUP BY mr.message_id
		)
		SELECT
			COALESCE(msg.id, 0) as id,
			COALESCE(msg.source_message_id, '') as source_message_id,
			COALESCE(msg.conversation_id, 0) as conversation_id,
			COALESCE(msg.subject, '') as subject,
			COALESCE(msg.snippet, '') as snippet,
			COALESCE(ms.from_email, '') as from_email,
			COALESCE(ms.from_name, '') as from_name,
			msg.sent_at,
			COALESCE(msg.size_estimate, 0) as size_estimate,
			COALESCE(msg.has_attachments, false) as has_attachments,
			COALESCE(att.attachment_count, 0) as attachment_count,
			CAST(COALESCE(to_json(mlbl.labels), '[]') AS VARCHAR) as labels,
			msg.deleted_from_source_at
		FROM msg
		LEFT JOIN msg_sender ms ON ms.message_id = msg.id
		LEFT JOIN att ON att.message_id = msg.id
		LEFT JOIN msg_labels mlbl ON mlbl.message_id = msg.id
		WHERE %s
		ORDER BY msg.sent_at DESC
		LIMIT ? OFFSET ?
	`, e.parquetCTEs(), strings.Join(conditions, " AND "))

	args = append(args, limit, offset)

	rows, err := e.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("search fast: %w", err)
	}
	defer rows.Close()

	var results []MessageSummary
	for rows.Next() {
		var msg MessageSummary
		var sentAt sql.NullTime
		var deletedAt sql.NullTime
		var labelsJSON string
		if err := rows.Scan(
			&msg.ID,
			&msg.SourceMessageID,
			&msg.ConversationID,
			&msg.Subject,
			&msg.Snippet,
			&msg.FromEmail,
			&msg.FromName,
			&sentAt,
			&msg.SizeEstimate,
			&msg.HasAttachments,
			&msg.AttachmentCount,
			&labelsJSON,
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
		// Parse labels from JSON array format
		msg.Labels = parseLabelsJSON(labelsJSON)
		results = append(results, msg)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate messages: %w", err)
	}

	return results, nil
}

// SearchFastCount returns the total count of messages matching a search query.
// This is used for pagination UI to show "N of M results".
func (e *DuckDBEngine) SearchFastCount(ctx context.Context, q *search.Query, filter MessageFilter) (int64, error) {
	conditions, args := e.buildSearchConditions(q, filter)

	// Count with JOINs for filters that need them
	query := fmt.Sprintf(`
		WITH %s,
		msg_sender AS (
			SELECT mr.message_id, FIRST(p.email_address) as from_email, FIRST(p.display_name) as from_name
			FROM mr
			JOIN p ON p.id = mr.participant_id
			WHERE mr.recipient_type = 'from'
			GROUP BY mr.message_id
		)
		SELECT COUNT(*) as cnt
		FROM msg
		LEFT JOIN msg_sender ms ON ms.message_id = msg.id
		WHERE %s
	`, e.parquetCTEs(), strings.Join(conditions, " AND "))

	var count int64
	if err := e.db.QueryRowContext(ctx, query, args...).Scan(&count); err != nil {
		return 0, fmt.Errorf("search fast count: %w", err)
	}
	return count, nil
}

// buildSearchConditions builds WHERE conditions for search queries.
// Shared by SearchFast and SearchFastCount.
// Note: These conditions reference msg and ms (msg_sender) CTEs.
func (e *DuckDBEngine) buildSearchConditions(q *search.Query, filter MessageFilter) ([]string, []interface{}) {
	var conditions []string
	var args []interface{}

	// Apply basic filter conditions (ignoring join flags for search - we handle those differently)
	if filter.SourceID != nil {
		conditions = append(conditions, "msg.source_id = ?")
		args = append(args, *filter.SourceID)
	}
	if filter.After != nil {
		conditions = append(conditions, "msg.sent_at >= CAST(? AS TIMESTAMP)")
		args = append(args, filter.After.Format("2006-01-02 15:04:05"))
	}
	if filter.Before != nil {
		conditions = append(conditions, "msg.sent_at < CAST(? AS TIMESTAMP)")
		args = append(args, filter.Before.Format("2006-01-02 15:04:05"))
	}
	if filter.WithAttachmentsOnly {
		conditions = append(conditions, "msg.has_attachments = true")
	}
	if filter.Sender != "" {
		conditions = append(conditions, "ms.from_email = ?")
		args = append(args, filter.Sender)
	}
	if filter.Domain != "" {
		conditions = append(conditions, "ms.from_email ILIKE ?")
		args = append(args, "%@"+filter.Domain)
	}
	// Recipient filter - use EXISTS subquery for drill-down context
	if filter.Recipient != "" {
		conditions = append(conditions, `EXISTS (
			SELECT 1 FROM mr
			JOIN p ON p.id = mr.participant_id
			WHERE mr.message_id = msg.id
			  AND mr.recipient_type IN ('to', 'cc', 'bcc')
			  AND p.email_address = ?
		)`)
		args = append(args, filter.Recipient)
	}
	// Label filter - use EXISTS subquery for drill-down context
	if filter.Label != "" {
		conditions = append(conditions, `EXISTS (
			SELECT 1 FROM ml
			JOIN lbl ON lbl.id = ml.label_id
			WHERE ml.message_id = msg.id
			  AND lbl.name = ?
		)`)
		args = append(args, filter.Label)
	}
	if filter.TimePeriod != "" {
		granularity := filter.TimeGranularity
		if granularity == TimeYear && len(filter.TimePeriod) > 4 {
			switch len(filter.TimePeriod) {
			case 7:
				granularity = TimeMonth
			case 10:
				granularity = TimeDay
			}
		}
		var timeExpr string
		switch granularity {
		case TimeYear:
			timeExpr = "CAST(msg.year AS VARCHAR)"
		case TimeMonth:
			timeExpr = "CAST(msg.year AS VARCHAR) || '-' || LPAD(CAST(msg.month AS VARCHAR), 2, '0')"
		case TimeDay:
			timeExpr = "strftime(msg.sent_at, '%Y-%m-%d')"
		default:
			timeExpr = "CAST(msg.year AS VARCHAR) || '-' || LPAD(CAST(msg.month AS VARCHAR), 2, '0')"
		}
		conditions = append(conditions, fmt.Sprintf("%s = ?", timeExpr))
		args = append(args, filter.TimePeriod)
	}

	// Text search terms - search subject and from fields only (fast path)
	if len(q.TextTerms) > 0 {
		for _, term := range q.TextTerms {
			termPattern := "%" + escapeILIKE(term) + "%"
			conditions = append(conditions, `(
				msg.subject ILIKE ? ESCAPE '\' OR
				ms.from_email ILIKE ? ESCAPE '\' OR
				ms.from_name ILIKE ? ESCAPE '\'
			)`)
			args = append(args, termPattern, termPattern, termPattern)
		}
	}

	// From filter
	if len(q.FromAddrs) > 0 {
		for _, addr := range q.FromAddrs {
			conditions = append(conditions, "ms.from_email ILIKE ? ESCAPE '\\'")
			args = append(args, "%"+escapeILIKE(addr)+"%")
		}
	}

	// To filter - use EXISTS subquery to check recipients
	if len(q.ToAddrs) > 0 {
		for _, addr := range q.ToAddrs {
			conditions = append(conditions, `EXISTS (
				SELECT 1 FROM mr
				JOIN p ON p.id = mr.participant_id
				WHERE mr.message_id = msg.id AND mr.recipient_type IN ('to', 'cc')
				AND p.email_address ILIKE ? ESCAPE '\'
			)`)
			args = append(args, "%"+escapeILIKE(addr)+"%")
		}
	}

	// Subject filter
	if len(q.SubjectTerms) > 0 {
		for _, term := range q.SubjectTerms {
			conditions = append(conditions, "msg.subject ILIKE ? ESCAPE '\\'")
			args = append(args, "%"+escapeILIKE(term)+"%")
		}
	}

	// Label filter - use EXISTS subquery
	if len(q.Labels) > 0 {
		for _, label := range q.Labels {
			conditions = append(conditions, `EXISTS (
				SELECT 1 FROM ml
				JOIN lbl ON lbl.id = ml.label_id
				WHERE ml.message_id = msg.id AND lbl.name = ?
			)`)
			args = append(args, label)
		}
	}

	// Has attachment filter
	if q.HasAttachment != nil && *q.HasAttachment {
		conditions = append(conditions, "msg.has_attachments = 1")
	}

	// Date range filters
	if q.AfterDate != nil {
		conditions = append(conditions, "msg.sent_at >= CAST(? AS TIMESTAMP)")
		args = append(args, q.AfterDate.Format("2006-01-02 15:04:05"))
	}
	if q.BeforeDate != nil {
		conditions = append(conditions, "msg.sent_at < CAST(? AS TIMESTAMP)")
		args = append(args, q.BeforeDate.Format("2006-01-02 15:04:05"))
	}

	// Size filters
	if q.LargerThan != nil {
		conditions = append(conditions, "msg.size_estimate > ?")
		args = append(args, *q.LargerThan)
	}
	if q.SmallerThan != nil {
		conditions = append(conditions, "msg.size_estimate < ?")
		args = append(args, *q.SmallerThan)
	}

	// Account filter
	if q.AccountID != nil {
		conditions = append(conditions, "msg.source_id = ?")
		args = append(args, *q.AccountID)
	}

	// Default conditions if none specified
	if len(conditions) == 0 {
		conditions = append(conditions, "1=1")
	}

	return conditions, args
}
