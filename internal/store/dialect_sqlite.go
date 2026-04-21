package store

import (
	"database/sql"
	"fmt"
)

// SQLiteDialect implements Dialect for SQLite (the default backend).
type SQLiteDialect struct{}

func (d *SQLiteDialect) DriverName() string { return "sqlite3" }

// Rebind is a no-op for SQLite — it uses ? placeholders natively.
func (d *SQLiteDialect) Rebind(query string) string { return query }

// Now returns the SQLite expression for the current UTC timestamp.
func (d *SQLiteDialect) Now() string { return "datetime('now')" }

// InsertOrIgnore is a no-op for SQLite — the syntax is native.
func (d *SQLiteDialect) InsertOrIgnore(sql string) string { return sql }

// InsertOrIgnorePrefix is a no-op for SQLite — OR IGNORE stays in the prefix.
func (d *SQLiteDialect) InsertOrIgnorePrefix(sql string) string { return sql }

// InsertOrIgnoreSuffix returns "" for SQLite — OR IGNORE is in the statement prefix.
func (d *SQLiteDialect) InsertOrIgnoreSuffix() string { return "" }

// FTSUpsert inserts or replaces an FTS5 row. FTS5 requires rowid to be
// specified explicitly so the virtual table's rowid matches messages.id;
// the dialect owns this detail so callers don't pass messageID twice.
func (d *SQLiteDialect) FTSUpsert(q querier, doc FTSDoc) error {
	_, err := q.Exec(
		`INSERT OR REPLACE INTO messages_fts(rowid, message_id, subject, body, from_addr, to_addr, cc_addr)
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
		doc.MessageID, doc.MessageID, doc.Subject, doc.Body,
		doc.FromAddr, doc.ToAddrs, doc.CcAddrs,
	)
	return err
}

// FTSSearchClause returns SQL fragments for FTS5 full-text search.
// "rank" is an implicit FTS5 column, so orderArgCount is 0.
func (d *SQLiteDialect) FTSSearchClause() (join, where, orderBy string, orderArgCount int) {
	return "JOIN messages_fts fts ON fts.rowid = m.id",
		"messages_fts MATCH ?",
		"rank",
		0
}

// FTSDeleteSQL returns the SQL to delete a message's FTS5 entry.
func (d *SQLiteDialect) FTSDeleteSQL() string {
	return `DELETE FROM messages_fts WHERE message_id IN (
		SELECT id FROM messages WHERE source_id = ?
	)`
}

// FTSBackfillBatchSQL returns the SQL to backfill FTS5 for a range of message IDs.
// Parameters: fromID(?), toID(?)
func (d *SQLiteDialect) FTSBackfillBatchSQL() string {
	return `INSERT OR REPLACE INTO messages_fts (rowid, message_id, subject, body, from_addr, to_addr, cc_addr)
		SELECT m.id, m.id, COALESCE(m.subject, ''), COALESCE(mb.body_text, ''),
			COALESCE(
				CASE WHEN m.message_type != 'email' AND m.message_type IS NOT NULL AND m.message_type != ''
				     THEN (SELECT COALESCE(p.phone_number, p.email_address) FROM participants p WHERE p.id = m.sender_id)
				END,
				(SELECT GROUP_CONCAT(p.email_address, ' ') FROM message_recipients mr JOIN participants p ON p.id = mr.participant_id WHERE mr.message_id = m.id AND mr.recipient_type = 'from'),
				''
			),
			COALESCE((SELECT GROUP_CONCAT(p.email_address, ' ') FROM message_recipients mr JOIN participants p ON p.id = mr.participant_id WHERE mr.message_id = m.id AND mr.recipient_type = 'to'), ''),
			COALESCE((SELECT GROUP_CONCAT(p.email_address, ' ') FROM message_recipients mr JOIN participants p ON p.id = mr.participant_id WHERE mr.message_id = m.id AND mr.recipient_type = 'cc'), '')
		FROM messages m
		LEFT JOIN message_bodies mb ON mb.message_id = m.id
		WHERE m.id >= ? AND m.id < ?`
}

// FTSAvailable probes for FTS5 by querying the virtual table.
// Checking sqlite_master alone is insufficient: a binary built without FTS5
// support will fail with "no such module: fts5" even if the table exists.
func (d *SQLiteDialect) FTSAvailable(db *sql.DB) bool {
	var probe int
	err := db.QueryRow("SELECT 1 FROM messages_fts LIMIT 1").Scan(&probe)
	return err == nil || err == sql.ErrNoRows
}

// FTSNeedsBackfill reports whether the FTS5 table needs population.
// Uses MAX(id) comparisons (instant B-tree lookups) instead of COUNT(*).
func (d *SQLiteDialect) FTSNeedsBackfill(db *sql.DB) bool {
	var msgMax int64
	if err := db.QueryRow("SELECT COALESCE(MAX(id), 0) FROM messages").Scan(&msgMax); err != nil || msgMax == 0 {
		return false
	}
	var ftsMax int64
	if err := db.QueryRow("SELECT COALESCE(MAX(rowid), 0) FROM messages_fts").Scan(&ftsMax); err != nil {
		return false
	}
	return ftsMax < msgMax-msgMax/10
}

// FTSClearSQL returns the SQL to clear all FTS5 data.
func (d *SQLiteDialect) FTSClearSQL() string {
	return "DELETE FROM messages_fts"
}

// SchemaFTS returns the embedded filename containing FTS5 virtual table DDL.
func (d *SQLiteDialect) SchemaFTS() string {
	return "schema_sqlite.sql"
}

// InitConn is a no-op for SQLite — PRAGMAs are set via DSN parameters.
func (d *SQLiteDialect) InitConn(db *sql.DB) error { return nil }

// SchemaFiles returns the schema files to execute during InitSchema.
func (d *SQLiteDialect) SchemaFiles() []string {
	return []string{"schema.sql"}
}

// CheckpointWAL forces a WAL checkpoint using TRUNCATE mode.
func (d *SQLiteDialect) CheckpointWAL(db *sql.DB) error {
	var busy, log, checkpointed int
	err := db.QueryRow("PRAGMA wal_checkpoint(TRUNCATE)").Scan(&busy, &log, &checkpointed)
	if err != nil {
		return err
	}
	if busy != 0 {
		return fmt.Errorf(
			"WAL checkpoint incomplete: database busy "+
				"(log=%d, checkpointed=%d)", log, checkpointed,
		)
	}
	return nil
}

// SchemaStaleCheck returns the SQL to check whether the most recent migration column exists.
func (d *SQLiteDialect) SchemaStaleCheck() string {
	return "SELECT COUNT(*) FROM pragma_table_info('conversations') WHERE name = 'conversation_type'"
}

// IsDuplicateColumnError returns true if the error is "duplicate column name" from ALTER TABLE.
func (d *SQLiteDialect) IsDuplicateColumnError(err error) bool {
	return isSQLiteError(err, "duplicate column name")
}

// IsConflictError returns true if the error is a UNIQUE constraint violation.
func (d *SQLiteDialect) IsConflictError(err error) bool {
	return isSQLiteError(err, "UNIQUE constraint failed")
}

// IsNoSuchTableError returns true if the error indicates a missing table.
func (d *SQLiteDialect) IsNoSuchTableError(err error) bool {
	return isSQLiteError(err, "no such table")
}

// IsNoSuchModuleError returns true if the error indicates a missing module (e.g., fts5).
func (d *SQLiteDialect) IsNoSuchModuleError(err error) bool {
	return isSQLiteError(err, "no such module: fts5")
}

// IsReturningError returns true if the error indicates RETURNING is not supported.
func (d *SQLiteDialect) IsReturningError(err error) bool {
	return isSQLiteError(err, "RETURNING")
}
