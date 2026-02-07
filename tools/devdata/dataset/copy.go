package dataset

import (
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3" // SQLite driver

	"github.com/wesm/msgvault/internal/store"
)

// CopyResult holds the summary of a dataset copy operation.
type CopyResult struct {
	Messages      int64
	Conversations int64
	Participants  int64
	Labels        int64
	DBSize        int64
	Elapsed       time.Duration
}

// CopySubset copies rowCount most recent messages (and all referenced data) from
// srcDBPath into a new database in dstDir. The destination schema is initialized
// using the embedded store schema.
func CopySubset(srcDBPath, dstDir string, rowCount int) (*CopyResult, error) {
	start := time.Now()

	// Create destination directory
	if err := os.MkdirAll(dstDir, 0700); err != nil {
		return nil, fmt.Errorf("create destination directory: %w", err)
	}

	dstDBPath := filepath.Join(dstDir, "msgvault.db")

	// Phase 1: Create destination DB with schema using store.Open + InitSchema
	st, err := store.Open(dstDBPath)
	if err != nil {
		os.RemoveAll(dstDir)
		return nil, fmt.Errorf("create destination database: %w", err)
	}
	if err := st.InitSchema(); err != nil {
		st.Close()
		os.RemoveAll(dstDir)
		return nil, fmt.Errorf("initialize schema: %w", err)
	}
	st.Close()

	// Phase 2: Re-open with foreign keys OFF for bulk copy
	dsn := dstDBPath + "?_journal_mode=WAL&_busy_timeout=5000&_foreign_keys=OFF"
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		os.RemoveAll(dstDir)
		return nil, fmt.Errorf("reopen database: %w", err)
	}
	defer db.Close()

	// Sanitize source path for ATTACH: reject null bytes, escape single quotes
	if strings.ContainsRune(srcDBPath, 0) {
		os.RemoveAll(dstDir)
		return nil, fmt.Errorf("source database path contains null byte")
	}
	escapedSrcPath := strings.ReplaceAll(srcDBPath, "'", "''")

	// Attach source database
	attachSQL := fmt.Sprintf("ATTACH DATABASE '%s' AS src", escapedSrcPath)
	if _, err := db.Exec(attachSQL); err != nil {
		os.RemoveAll(dstDir)
		return nil, fmt.Errorf("attach source database: %w", err)
	}

	// Begin transaction for bulk copy
	tx, err := db.Begin()
	if err != nil {
		os.RemoveAll(dstDir)
		return nil, fmt.Errorf("begin transaction: %w", err)
	}

	result, err := copyData(tx, rowCount)
	if err != nil {
		tx.Rollback()
		db.Exec("DETACH DATABASE src")
		os.RemoveAll(dstDir)
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		db.Exec("DETACH DATABASE src")
		os.RemoveAll(dstDir)
		return nil, fmt.Errorf("commit transaction: %w", err)
	}

	// Re-enable foreign keys and verify integrity
	if _, err := db.Exec("PRAGMA foreign_keys = ON"); err != nil {
		os.RemoveAll(dstDir)
		return nil, fmt.Errorf("enable foreign keys: %w", err)
	}

	rows, err := db.Query("PRAGMA foreign_key_check")
	if err != nil {
		os.RemoveAll(dstDir)
		return nil, fmt.Errorf("foreign key check: %w", err)
	}
	var violations []string
	for rows.Next() {
		var table, rowid, parent, fkid string
		if err := rows.Scan(&table, &rowid, &parent, &fkid); err == nil {
			violations = append(violations, fmt.Sprintf("%s(rowid=%s) -> %s", table, rowid, parent))
		}
	}
	rows.Close()
	if len(violations) > 0 {
		os.RemoveAll(dstDir)
		return nil, fmt.Errorf("foreign key violations: %s", strings.Join(violations, "; "))
	}

	// Update denormalized conversation counts
	if err := updateConversationCounts(db); err != nil {
		os.RemoveAll(dstDir)
		return nil, fmt.Errorf("update conversation counts: %w", err)
	}

	// Populate FTS5 index (ignore errors - FTS5 may not be available)
	_ = populateFTS(db)

	// Detach source
	db.Exec("DETACH DATABASE src")

	// Get final DB size
	if info, err := os.Stat(dstDBPath); err == nil {
		result.DBSize = info.Size()
	}

	result.Elapsed = time.Since(start)
	return result, nil
}

// copyData executes the INSERT INTO ... SELECT statements in dependency order.
func copyData(tx *sql.Tx, rowCount int) (*CopyResult, error) {
	result := &CopyResult{}

	// a. Sources (all rows — tiny table)
	if _, err := tx.Exec("INSERT INTO sources SELECT * FROM src.sources"); err != nil {
		return nil, fmt.Errorf("copy sources: %w", err)
	}

	// b. Select message IDs (the N most recent)
	if _, err := tx.Exec(`
		CREATE TEMP TABLE selected_messages AS
		SELECT id FROM src.messages ORDER BY sent_at DESC LIMIT ?`, rowCount); err != nil {
		return nil, fmt.Errorf("select messages: %w", err)
	}

	// Count actual selected messages
	if err := tx.QueryRow("SELECT COUNT(*) FROM selected_messages").Scan(&result.Messages); err != nil {
		return nil, fmt.Errorf("count selected messages: %w", err)
	}

	// c. Conversations referenced by selected messages
	res, err := tx.Exec(`
		INSERT INTO conversations SELECT * FROM src.conversations
		WHERE id IN (SELECT DISTINCT conversation_id FROM src.messages
		             WHERE id IN (SELECT id FROM selected_messages))`)
	if err != nil {
		return nil, fmt.Errorf("copy conversations: %w", err)
	}
	result.Conversations, _ = res.RowsAffected()

	// d. Participants referenced by selected messages (senders + recipients)
	res, err = tx.Exec(`
		INSERT INTO participants SELECT * FROM src.participants
		WHERE id IN (
			SELECT sender_id FROM src.messages WHERE id IN (SELECT id FROM selected_messages)
			UNION
			SELECT participant_id FROM src.message_recipients WHERE message_id IN (SELECT id FROM selected_messages)
		)`)
	if err != nil {
		return nil, fmt.Errorf("copy participants: %w", err)
	}
	result.Participants, _ = res.RowsAffected()

	// e. Participant identifiers for copied participants
	if _, err := tx.Exec(`
		INSERT INTO participant_identifiers SELECT * FROM src.participant_identifiers
		WHERE participant_id IN (SELECT id FROM participants)`); err != nil {
		return nil, fmt.Errorf("copy participant_identifiers: %w", err)
	}

	// f. Conversation participants for copied conversations + participants
	if _, err := tx.Exec(`
		INSERT INTO conversation_participants SELECT * FROM src.conversation_participants
		WHERE conversation_id IN (SELECT id FROM conversations)
		  AND participant_id IN (SELECT id FROM participants)`); err != nil {
		return nil, fmt.Errorf("copy conversation_participants: %w", err)
	}

	// g. Messages
	if _, err := tx.Exec(`
		INSERT INTO messages SELECT * FROM src.messages
		WHERE id IN (SELECT id FROM selected_messages)`); err != nil {
		return nil, fmt.Errorf("copy messages: %w", err)
	}

	// h. Message bodies
	if _, err := tx.Exec(`
		INSERT INTO message_bodies SELECT * FROM src.message_bodies
		WHERE message_id IN (SELECT id FROM selected_messages)`); err != nil {
		return nil, fmt.Errorf("copy message_bodies: %w", err)
	}

	// i. Message raw
	if _, err := tx.Exec(`
		INSERT INTO message_raw SELECT * FROM src.message_raw
		WHERE message_id IN (SELECT id FROM selected_messages)`); err != nil {
		return nil, fmt.Errorf("copy message_raw: %w", err)
	}

	// j. Message recipients
	if _, err := tx.Exec(`
		INSERT INTO message_recipients SELECT * FROM src.message_recipients
		WHERE message_id IN (SELECT id FROM selected_messages)`); err != nil {
		return nil, fmt.Errorf("copy message_recipients: %w", err)
	}

	// k. Reactions
	if _, err := tx.Exec(`
		INSERT INTO reactions SELECT * FROM src.reactions
		WHERE message_id IN (SELECT id FROM selected_messages)`); err != nil {
		return nil, fmt.Errorf("copy reactions: %w", err)
	}

	// l. Attachments
	if _, err := tx.Exec(`
		INSERT INTO attachments SELECT * FROM src.attachments
		WHERE message_id IN (SELECT id FROM selected_messages)`); err != nil {
		return nil, fmt.Errorf("copy attachments: %w", err)
	}

	// m. Labels (all for copied sources)
	res, err = tx.Exec(`
		INSERT INTO labels SELECT * FROM src.labels
		WHERE source_id IN (SELECT id FROM sources)`)
	if err != nil {
		return nil, fmt.Errorf("copy labels: %w", err)
	}
	result.Labels, _ = res.RowsAffected()

	// n. Message labels (intersection of copied messages and copied labels)
	if _, err := tx.Exec(`
		INSERT INTO message_labels SELECT * FROM src.message_labels
		WHERE message_id IN (SELECT id FROM selected_messages)
		  AND label_id IN (SELECT id FROM labels)`); err != nil {
		return nil, fmt.Errorf("copy message_labels: %w", err)
	}

	// Clean up temp table
	tx.Exec("DROP TABLE IF EXISTS selected_messages")

	return result, nil
}

// updateConversationCounts updates the denormalized counts on conversations
// to be consistent with the subset of messages actually copied.
func updateConversationCounts(db *sql.DB) error {
	_, err := db.Exec(`
		UPDATE conversations SET
			message_count = (SELECT COUNT(*) FROM messages WHERE conversation_id = conversations.id),
			participant_count = (SELECT COUNT(*) FROM conversation_participants WHERE conversation_id = conversations.id),
			last_message_at = (SELECT MAX(sent_at) FROM messages WHERE conversation_id = conversations.id)`)
	return err
}

// populateFTS rebuilds the FTS5 index from the copied data.
// Matches the query structure from store.backfillFTSBatch.
func populateFTS(db *sql.DB) error {
	_, err := db.Exec(`
		INSERT OR REPLACE INTO messages_fts(rowid, message_id, subject, body, from_addr, to_addr, cc_addr)
		SELECT m.id, m.id, COALESCE(m.subject, ''), COALESCE(mb.body_text, ''),
			COALESCE((SELECT GROUP_CONCAT(p.email_address, ' ')
			          FROM message_recipients mr JOIN participants p ON p.id = mr.participant_id
			          WHERE mr.message_id = m.id AND mr.recipient_type = 'from'), ''),
			COALESCE((SELECT GROUP_CONCAT(p.email_address, ' ')
			          FROM message_recipients mr JOIN participants p ON p.id = mr.participant_id
			          WHERE mr.message_id = m.id AND mr.recipient_type = 'to'), ''),
			COALESCE((SELECT GROUP_CONCAT(p.email_address, ' ')
			          FROM message_recipients mr JOIN participants p ON p.id = mr.participant_id
			          WHERE mr.message_id = m.id AND mr.recipient_type = 'cc'), '')
		FROM messages m
		LEFT JOIN message_bodies mb ON mb.message_id = m.id`)
	return err
}

// CopyFileIfExists copies a single file from src to dst.
// Returns nil if the source file does not exist.
// Both paths must be validated by the caller to prevent path traversal.
func CopyFileIfExists(src, dst string) error {
	// Validate paths are absolute
	if !filepath.IsAbs(src) || !filepath.IsAbs(dst) {
		return fmt.Errorf("paths must be absolute: src=%q, dst=%q", src, dst)
	}

	srcFile, err := os.Open(src)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("open source file %s: %w", src, err)
	}
	defer srcFile.Close()

	dstFile, err := os.Create(dst)
	if err != nil {
		return fmt.Errorf("create destination file %s: %w", dst, err)
	}
	defer dstFile.Close()

	if _, err := io.Copy(dstFile, srcFile); err != nil {
		return fmt.Errorf("copy %s to %s: %w", src, dst, err)
	}

	return nil
}
