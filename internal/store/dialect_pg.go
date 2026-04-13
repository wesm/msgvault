package store

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib" // Register pgx driver for database/sql
)

// PostgreSQLDialect implements Dialect for PostgreSQL.
type PostgreSQLDialect struct{}

func (d *PostgreSQLDialect) DriverName() string { return "pgx" }

// Rebind converts ? placeholders to PostgreSQL $1, $2, ... numbered placeholders.
// Correctly handles quoted strings — only converts ? outside single quotes.
func (d *PostgreSQLDialect) Rebind(query string) string {
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

// Now returns the PostgreSQL expression for the current timestamp.
func (d *PostgreSQLDialect) Now() string { return "NOW()" }

// InsertOrIgnore rewrites INSERT OR IGNORE INTO to INSERT INTO and, if the
// statement appears complete (ends with ")" after VALUES), appends
// " ON CONFLICT DO NOTHING". Prefix-only strings (ending with "VALUES ")
// do not get the suffix here — callers use InsertOrIgnoreSuffix() instead,
// to be appended after the VALUES tuples are assembled.
func (d *PostgreSQLDialect) InsertOrIgnore(sql string) string {
	s := strings.Replace(sql, "INSERT OR IGNORE INTO", "INSERT INTO", 1)
	// If the input is a complete statement (ends with ")" — i.e., VALUES tuples
	// already closed), append the conflict clause. If it ends with "VALUES "
	// (prefix form used by insertInChunks), leave the suffix to the caller.
	trimmed := strings.TrimRight(s, " \t\n\r")
	if strings.HasSuffix(trimmed, ")") {
		return trimmed + " ON CONFLICT DO NOTHING"
	}
	return s
}

// InsertOrIgnoreSuffix returns the PostgreSQL suffix for conflict-ignoring batch inserts.
func (d *PostgreSQLDialect) InsertOrIgnoreSuffix() string {
	return " ON CONFLICT DO NOTHING"
}

// UpdateOrIgnore rewrites UPDATE OR IGNORE to a standard UPDATE.
// PostgreSQL has no UPDATE OR IGNORE equivalent. The single call site
// (mergeLabelByName) tolerates constraint violations being raised; the
// caller follows up with a DELETE to clean up conflicts.
func (d *PostgreSQLDialect) UpdateOrIgnore(sql string) string {
	return strings.Replace(sql, "UPDATE OR IGNORE", "UPDATE", 1)
}

// FTSUpsertSQL returns the SQL to update the tsvector column on messages.
// Parameters: $1=subject, $2=messageID (for rowid compat), $3=subject, $4=bodyText,
// $5=fromAddr, $6=toAddrs, $7=ccAddrs
// Note: For compatibility with the SQLite call convention (which passes messageID
// as both params 1 and 2), param $1 is the messageID and $2 is also messageID.
// We use $3-$7 for the text fields and $1 for the WHERE clause.
func (d *PostgreSQLDialect) FTSUpsertSQL() string {
	return `UPDATE messages SET search_fts =
		setweight(to_tsvector('simple', COALESCE($3, '')), 'A') ||
		setweight(to_tsvector('simple', COALESCE($5, '')), 'B') ||
		to_tsvector('simple', COALESCE($4, '')) ||
		to_tsvector('simple', COALESCE($6, '')) ||
		to_tsvector('simple', COALESCE($7, ''))
	WHERE id = $1`
}

// FTSSearchClause returns SQL fragments for tsvector full-text search.
// PostgreSQL stores the tsvector on the messages table — no JOIN needed.
func (d *PostgreSQLDialect) FTSSearchClause(paramIndex int) (join, where, orderBy string) {
	p := fmt.Sprintf("$%d", paramIndex)
	return "",
		fmt.Sprintf("m.search_fts @@ plainto_tsquery('simple', %s)", p),
		fmt.Sprintf("ts_rank(m.search_fts, plainto_tsquery('simple', %s)) DESC", p)
}

// FTSDeleteSQL returns the SQL to clear tsvector data for messages belonging to a source.
func (d *PostgreSQLDialect) FTSDeleteSQL() string {
	return `UPDATE messages SET search_fts = NULL WHERE source_id = $1`
}

// FTSBackfillBatchSQL returns the SQL to populate tsvector for a range of message IDs.
// Parameters: $1=fromID, $2=toID
func (d *PostgreSQLDialect) FTSBackfillBatchSQL() string {
	return `UPDATE messages m SET search_fts =
		setweight(to_tsvector('simple', COALESCE(m.subject, '')), 'A') ||
		to_tsvector('simple', COALESCE(mb.body_text, '')) ||
		setweight(to_tsvector('simple', COALESCE(
			CASE WHEN m.message_type != 'email' AND m.message_type IS NOT NULL AND m.message_type != ''
			     THEN (SELECT COALESCE(p.phone_number, p.email_address) FROM participants p WHERE p.id = m.sender_id)
			END,
			(SELECT STRING_AGG(p.email_address, ' ') FROM message_recipients mr JOIN participants p ON p.id = mr.participant_id WHERE mr.message_id = m.id AND mr.recipient_type = 'from'),
			''
		)), 'B') ||
		to_tsvector('simple', COALESCE((SELECT STRING_AGG(p.email_address, ' ') FROM message_recipients mr JOIN participants p ON p.id = mr.participant_id WHERE mr.message_id = m.id AND mr.recipient_type = 'to'), '')) ||
		to_tsvector('simple', COALESCE((SELECT STRING_AGG(p.email_address, ' ') FROM message_recipients mr JOIN participants p ON p.id = mr.participant_id WHERE mr.message_id = m.id AND mr.recipient_type = 'cc'), ''))
	FROM message_bodies mb
	WHERE mb.message_id = m.id AND m.id >= $1 AND m.id < $2`
}

// FTSAvailable reports whether tsvector search is available.
// PostgreSQL always supports tsvector — check that the column exists.
func (d *PostgreSQLDialect) FTSAvailable(db *sql.DB) (bool, error) {
	var count int
	err := db.QueryRow(`
		SELECT COUNT(*) FROM information_schema.columns
		WHERE table_name = 'messages' AND column_name = 'search_fts'
	`).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

// FTSNeedsBackfill reports whether the tsvector column needs population.
func (d *PostgreSQLDialect) FTSNeedsBackfill(db *sql.DB) (bool, error) {
	var msgMax int64
	if err := db.QueryRow("SELECT COALESCE(MAX(id), 0) FROM messages").Scan(&msgMax); err != nil || msgMax == 0 {
		return false, nil
	}
	var populatedMax int64
	if err := db.QueryRow("SELECT COALESCE(MAX(id), 0) FROM messages WHERE search_fts IS NOT NULL").Scan(&populatedMax); err != nil {
		return false, nil
	}
	return populatedMax < msgMax-msgMax/10, nil
}

// FTSClearSQL returns the SQL to clear all tsvector data.
func (d *PostgreSQLDialect) FTSClearSQL() string {
	return "UPDATE messages SET search_fts = NULL"
}

// SchemaFTS returns the embedded filename containing PostgreSQL FTS DDL.
func (d *PostgreSQLDialect) SchemaFTS() string {
	return "schema_pg.sql"
}

// InitConn performs PostgreSQL-specific connection initialization.
func (d *PostgreSQLDialect) InitConn(db *sql.DB) error {
	_, err := db.Exec("SET statement_timeout = '30s'")
	return err
}

// SchemaFiles returns the schema files to execute during InitSchema.
func (d *PostgreSQLDialect) SchemaFiles() []string {
	return []string{"schema.sql"}
}

// CheckpointWAL is a no-op for PostgreSQL (no WAL checkpoint needed).
func (d *PostgreSQLDialect) CheckpointWAL(db *sql.DB) error { return nil }

// SchemaStaleCheck returns the SQL to check whether migrations are needed.
// PostgreSQL uses information_schema instead of pragma_table_info.
func (d *PostgreSQLDialect) SchemaStaleCheck() string {
	return `SELECT COUNT(*) FROM information_schema.columns
		WHERE table_name = 'conversations' AND column_name = 'conversation_type'`
}

// IsDuplicateColumnError returns true if the error is a "column already exists" error.
// PostgreSQL SQLSTATE 42701 = duplicate_column.
func (d *PostgreSQLDialect) IsDuplicateColumnError(err error) bool {
	return isPgError(err, "42701")
}

// IsConflictError returns true if the error is a unique constraint violation.
// PostgreSQL SQLSTATE 23505 = unique_violation.
func (d *PostgreSQLDialect) IsConflictError(err error) bool {
	return isPgError(err, "23505")
}

// IsNoSuchTableError returns true if the error indicates a missing table.
// PostgreSQL SQLSTATE 42P01 = undefined_table.
func (d *PostgreSQLDialect) IsNoSuchTableError(err error) bool {
	return isPgError(err, "42P01")
}

// IsNoSuchModuleError always returns false for PostgreSQL (no module concept).
func (d *PostgreSQLDialect) IsNoSuchModuleError(err error) bool { return false }

// IsReturningError always returns false for PostgreSQL (RETURNING always supported).
func (d *PostgreSQLDialect) IsReturningError(err error) bool { return false }

// isPgError checks if err is a pgconn.PgError with the given SQLSTATE code.
func isPgError(err error, code string) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == code
	}
	return false
}
