// Package query - PostgreSQL query engine implementation.
//
// PostgreSQLEngine implements the Engine interface for PostgreSQL backends.
// It parallels SQLiteEngine but uses PostgreSQL-specific SQL:
//   - $N placeholders instead of ?
//   - to_char() instead of strftime()
//   - information_schema instead of sqlite_master
//   - tsvector/@@/ts_rank instead of FTS5 MATCH/rank
//
// This implementation is scaffolded to compile and handle the simpler
// read paths (GetMessage, GetAttachment, ListAccounts). The aggregate
// and search methods that depend on SQLite-specific constructs return
// a clearly-marked error pending follow-up work to parameterize the
// shared query builders.
package query

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/wesm/msgvault/internal/search"
)

// ErrNotImplemented is returned by PostgreSQLEngine methods that require
// query-builder parameterization not yet complete. Tracked for follow-up.
var ErrNotImplemented = errors.New("PostgreSQLEngine: method not yet implemented")

// PostgreSQLEngine implements Engine using direct PostgreSQL queries via pgx.
type PostgreSQLEngine struct {
	db *sql.DB
}

// NewPostgreSQLEngine creates a new PostgreSQL-backed query engine.
func NewPostgreSQLEngine(db *sql.DB) *PostgreSQLEngine {
	return &PostgreSQLEngine{db: db}
}

// Close is a no-op since PostgreSQLEngine doesn't own the connection.
func (e *PostgreSQLEngine) Close() error {
	return nil
}

// rebind converts ? placeholders to $1, $2, ... for PostgreSQL.
// Correctly handles quoted strings — only converts ? outside single quotes.
func rebindPg(query string) string {
	var b strings.Builder
	b.Grow(len(query) + 16)
	n := 1
	inQuote := false
	for i := 0; i < len(query); i++ {
		ch := query[i]
		if ch == '\'' {
			inQuote = !inQuote
			b.WriteByte(ch)
		} else if ch == '?' && !inQuote {
			fmt.Fprintf(&b, "$%d", n)
			n++
		} else {
			b.WriteByte(ch)
		}
	}
	return b.String()
}

// Aggregate performs grouping based on the provided ViewType.
// Requires dialect-aware time expression handling; see ErrNotImplemented.
func (e *PostgreSQLEngine) Aggregate(ctx context.Context, groupBy ViewType, opts AggregateOptions) ([]AggregateRow, error) {
	return nil, ErrNotImplemented
}

// SubAggregate performs aggregation on a filtered subset of messages.
func (e *PostgreSQLEngine) SubAggregate(ctx context.Context, filter MessageFilter, groupBy ViewType, opts AggregateOptions) ([]AggregateRow, error) {
	return nil, ErrNotImplemented
}

// ListMessages returns messages matching the filter.
func (e *PostgreSQLEngine) ListMessages(ctx context.Context, filter MessageFilter) ([]MessageSummary, error) {
	return nil, ErrNotImplemented
}

// GetMessage retrieves a full message by internal ID.
func (e *PostgreSQLEngine) GetMessage(ctx context.Context, id int64) (*MessageDetail, error) {
	return e.getMessageByQuery(ctx, "m.id = ?", id)
}

// GetMessageBySourceID retrieves a full message by source message ID.
func (e *PostgreSQLEngine) GetMessageBySourceID(ctx context.Context, sourceMessageID string) (*MessageDetail, error) {
	return e.getMessageByQuery(ctx, "m.source_message_id = ?", sourceMessageID)
}

func (e *PostgreSQLEngine) getMessageByQuery(ctx context.Context, whereClause string, args ...interface{}) (*MessageDetail, error) {
	query := rebindPg(fmt.Sprintf(`
		SELECT
			m.id,
			m.source_message_id,
			m.conversation_id,
			COALESCE(conv.source_conversation_id, ''),
			COALESCE(m.subject, ''),
			COALESCE(m.snippet, ''),
			m.sent_at,
			COALESCE(m.size_estimate, 0),
			m.has_attachments,
			COALESCE(mb.body_text, ''),
			COALESCE(mb.body_html, '')
		FROM messages m
		LEFT JOIN conversations conv ON conv.id = m.conversation_id
		LEFT JOIN message_bodies mb ON mb.message_id = m.id
		WHERE %s
		LIMIT 1
	`, whereClause))

	var msg MessageDetail
	var sentAt sql.NullTime
	err := e.db.QueryRowContext(ctx, query, args...).Scan(
		&msg.ID,
		&msg.SourceMessageID,
		&msg.ConversationID,
		&msg.SourceConversationID,
		&msg.Subject,
		&msg.Snippet,
		&sentAt,
		&msg.SizeEstimate,
		&msg.HasAttachments,
		&msg.BodyText,
		&msg.BodyHTML,
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
	return &msg, nil
}

// GetAttachment retrieves attachment metadata by ID.
func (e *PostgreSQLEngine) GetAttachment(ctx context.Context, id int64) (*AttachmentInfo, error) {
	var att AttachmentInfo
	err := e.db.QueryRowContext(ctx, `
		SELECT id, COALESCE(filename, ''), COALESCE(mime_type, ''), COALESCE(size, 0), COALESCE(content_hash, '')
		FROM attachments
		WHERE id = $1
	`, id).Scan(&att.ID, &att.Filename, &att.MimeType, &att.Size, &att.ContentHash)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get attachment: %w", err)
	}
	return &att, nil
}

// Search performs a Gmail-style search query using tsvector FTS.
func (e *PostgreSQLEngine) Search(ctx context.Context, query *search.Query, limit, offset int) ([]MessageSummary, error) {
	return nil, ErrNotImplemented
}

// SearchFast searches message metadata only (no body text).
func (e *PostgreSQLEngine) SearchFast(ctx context.Context, query *search.Query, filter MessageFilter, limit, offset int) ([]MessageSummary, error) {
	return nil, ErrNotImplemented
}

// SearchFastCount returns the total count of messages matching a search query.
func (e *PostgreSQLEngine) SearchFastCount(ctx context.Context, query *search.Query, filter MessageFilter) (int64, error) {
	return 0, ErrNotImplemented
}

// SearchFastWithStats performs a fast metadata search with stats.
func (e *PostgreSQLEngine) SearchFastWithStats(ctx context.Context, query *search.Query, queryStr string,
	filter MessageFilter, statsGroupBy ViewType, limit, offset int) (*SearchFastResult, error) {
	return nil, ErrNotImplemented
}

// GetGmailIDsByFilter returns Gmail message IDs matching a filter.
func (e *PostgreSQLEngine) GetGmailIDsByFilter(ctx context.Context, filter MessageFilter) ([]string, error) {
	return nil, ErrNotImplemented
}

// ListAccounts returns all source accounts.
func (e *PostgreSQLEngine) ListAccounts(ctx context.Context) ([]AccountInfo, error) {
	rows, err := e.db.QueryContext(ctx, `
		SELECT id, source_type, identifier, COALESCE(display_name, '')
		FROM sources
		ORDER BY identifier
	`)
	if err != nil {
		return nil, fmt.Errorf("list accounts: %w", err)
	}
	defer func() { _ = rows.Close() }()

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
func (e *PostgreSQLEngine) GetTotalStats(ctx context.Context, opts StatsOptions) (*TotalStats, error) {
	stats := &TotalStats{}

	// Basic stats without search filtering (search filtering TBD with query builder parameterization)
	if opts.SearchQuery != "" {
		return nil, ErrNotImplemented
	}

	// Message stats
	msgConds := []string{"(message_type = 'email' OR message_type IS NULL OR message_type = '')"}
	var args []interface{}
	argIdx := 1
	if opts.SourceID != nil {
		msgConds = append(msgConds, fmt.Sprintf("source_id = $%d", argIdx))
		args = append(args, *opts.SourceID)
		argIdx++
	}
	if opts.WithAttachmentsOnly {
		msgConds = append(msgConds, "has_attachments = TRUE")
	}
	if opts.HideDeletedFromSource {
		msgConds = append(msgConds, "deleted_from_source_at IS NULL")
	}

	msgQuery := fmt.Sprintf(`SELECT COUNT(*), COALESCE(SUM(size_estimate), 0) FROM messages WHERE %s`,
		strings.Join(msgConds, " AND "))
	if err := e.db.QueryRowContext(ctx, msgQuery, args...).Scan(&stats.MessageCount, &stats.TotalSize); err != nil {
		return nil, fmt.Errorf("message stats: %w", err)
	}

	// Attachment stats
	attQuery := `SELECT COUNT(*), COALESCE(SUM(size), 0) FROM attachments`
	if opts.SourceID != nil {
		attQuery = `SELECT COUNT(*), COALESCE(SUM(a.size), 0) FROM attachments a
			JOIN messages m ON m.id = a.message_id WHERE m.source_id = $1`
		if err := e.db.QueryRowContext(ctx, attQuery, *opts.SourceID).Scan(&stats.AttachmentCount, &stats.AttachmentSize); err != nil {
			return nil, fmt.Errorf("attachment stats: %w", err)
		}
	} else {
		if err := e.db.QueryRowContext(ctx, attQuery).Scan(&stats.AttachmentCount, &stats.AttachmentSize); err != nil {
			return nil, fmt.Errorf("attachment stats: %w", err)
		}
	}

	// Label count
	if opts.SourceID != nil {
		if err := e.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM labels WHERE source_id = $1", *opts.SourceID).Scan(&stats.LabelCount); err != nil {
			return nil, fmt.Errorf("label count: %w", err)
		}
	} else {
		if err := e.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM labels").Scan(&stats.LabelCount); err != nil {
			return nil, fmt.Errorf("label count: %w", err)
		}
	}

	// Account count
	if opts.SourceID != nil {
		if err := e.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM sources WHERE id = $1", *opts.SourceID).Scan(&stats.AccountCount); err != nil {
			return nil, fmt.Errorf("account count: %w", err)
		}
	} else {
		if err := e.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM sources").Scan(&stats.AccountCount); err != nil {
			return nil, fmt.Errorf("account count: %w", err)
		}
	}

	return stats, nil
}
