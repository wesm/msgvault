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
//
// Security: CopySubset validates srcDBPath for control characters and
// canonicalizes it before use in SQL, but does NOT perform path-traversal
// checks (e.g. verifying that srcDBPath or dstDir are within the home
// directory). Callers must validate path containment before calling this
// function — see newdata.go's runNewData for the expected validation pattern.
func CopySubset(srcDBPath, dstDir string, rowCount int) (*CopyResult, error) {
	start := time.Now()

	// Reject if destination already contains a database — overwriting an
	// existing dataset could corrupt it (InitSchema may partially modify it).
	dstDBPath := filepath.Join(dstDir, "msgvault.db")
	if _, err := os.Stat(dstDBPath); err == nil {
		return nil, fmt.Errorf("destination database already exists: %s", dstDBPath)
	}

	// Track whether we created the directory so cleanup only removes what we made.
	createdDir := false
	if _, err := os.Stat(dstDir); os.IsNotExist(err) {
		createdDir = true
	}

	// Create destination directory
	if err := os.MkdirAll(dstDir, 0700); err != nil {
		return nil, fmt.Errorf("create destination directory: %w", err)
	}

	// cleanup removes what CopySubset created: the entire directory if we
	// created it, or just the database file if the directory pre-existed.
	cleanup := func() {
		if createdDir {
			_ = os.RemoveAll(dstDir)
		} else {
			_ = os.Remove(dstDBPath)
		}
	}

	// Phase 1: Create destination DB with schema using store.Open + InitSchema
	st, err := store.Open(dstDBPath)
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("create destination database: %w", err)
	}
	if err := st.InitSchema(); err != nil {
		_ = st.Close()
		cleanup()
		return nil, fmt.Errorf("initialize schema: %w", err)
	}
	if err := st.Close(); err != nil {
		cleanup()
		return nil, fmt.Errorf("close schema database: %w", err)
	}

	// Phase 2: Re-open with foreign keys OFF for bulk copy
	dsn := dstDBPath + "?_journal_mode=WAL&_busy_timeout=5000&_foreign_keys=OFF"
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("reopen database: %w", err)
	}
	// NOTE: On error paths, cleanup() may remove the DB file before this
	// deferred Close runs. That is harmless — Close on a deleted file is a no-op.
	defer db.Close()

	// Canonicalize source path for ATTACH (defense in depth — caller should
	// also validate, but CopySubset is public and must not trust its inputs).
	srcDBPath, err = filepath.Abs(filepath.Clean(srcDBPath))
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("canonicalize source path: %w", err)
	}
	// Reject control characters (null, newline, tab, etc.) that have no
	// business in a filesystem path and could interfere with SQL parsing.
	for _, r := range srcDBPath {
		if r < 0x20 || r == 0x7F {
			cleanup()
			return nil, fmt.Errorf("source database path contains control character (0x%02X)", r)
		}
	}
	escapedSrcPath := strings.ReplaceAll(srcDBPath, "'", "''")

	// Attach source database
	attachSQL := fmt.Sprintf("ATTACH DATABASE '%s' AS src", escapedSrcPath)
	if _, err := db.Exec(attachSQL); err != nil {
		cleanup()
		return nil, fmt.Errorf("attach source database: %w", err)
	}

	// Begin transaction for bulk copy
	tx, err := db.Begin()
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("begin transaction: %w", err)
	}

	result, err := copyData(tx, rowCount)
	if err != nil {
		_ = tx.Rollback()
		_, _ = db.Exec("DETACH DATABASE src")
		cleanup()
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		_, _ = db.Exec("DETACH DATABASE src")
		cleanup()
		return nil, fmt.Errorf("commit transaction: %w", err)
	}

	// Verify referential integrity. PRAGMA foreign_key_check is a standalone
	// integrity scan that works regardless of the foreign_keys setting.
	// We enable foreign_keys here so subsequent operations (if any) would
	// enforce FK constraints, but the connection is about to close.
	if _, err := db.Exec("PRAGMA foreign_keys = ON"); err != nil {
		cleanup()
		return nil, fmt.Errorf("enable foreign keys: %w", err)
	}

	rows, err := db.Query("PRAGMA foreign_key_check")
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("foreign key check: %w", err)
	}
	var violations []string
	for rows.Next() {
		var table, rowid, parent, fkid string
		if err := rows.Scan(&table, &rowid, &parent, &fkid); err != nil {
			violations = append(violations, fmt.Sprintf("scan error: %v", err))
		} else {
			violations = append(violations, fmt.Sprintf("%s(rowid=%s) -> %s", table, rowid, parent))
		}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		cleanup()
		return nil, fmt.Errorf("iterate foreign key check: %w", err)
	}
	if err := rows.Close(); err != nil {
		cleanup()
		return nil, fmt.Errorf("close foreign key check rows: %w", err)
	}

	if len(violations) > 0 {
		cleanup()
		return nil, fmt.Errorf("foreign key violations: %s", strings.Join(violations, "; "))
	}

	// Update denormalized conversation counts
	if err := updateConversationCounts(db); err != nil {
		cleanup()
		return nil, fmt.Errorf("update conversation counts: %w", err)
	}

	// Populate FTS5 index. If the messages_fts table doesn't exist (FTS5
	// extension not available), that's expected and safe to ignore. Other
	// errors (e.g. corrupt data, encoding issues) are logged as warnings.
	if ftsErr := populateFTS(db); ftsErr != nil {
		errMsg := ftsErr.Error()
		if !strings.Contains(errMsg, "no such table") && !strings.Contains(errMsg, "messages_fts") {
			fmt.Fprintf(os.Stderr, "devdata: warning: FTS index population failed: %v\n", ftsErr)
		}
	}

	// Detach source
	if _, err := db.Exec("DETACH DATABASE src"); err != nil {
		cleanup()
		return nil, fmt.Errorf("detach source database: %w", err)
	}

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
	if result.Conversations, err = res.RowsAffected(); err != nil {
		return nil, fmt.Errorf("conversations rows affected: %w", err)
	}

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
	if result.Participants, err = res.RowsAffected(); err != nil {
		return nil, fmt.Errorf("participants rows affected: %w", err)
	}

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
	if result.Labels, err = res.RowsAffected(); err != nil {
		return nil, fmt.Errorf("labels rows affected: %w", err)
	}

	// n. Message labels (intersection of copied messages and copied labels)
	if _, err := tx.Exec(`
		INSERT INTO message_labels SELECT * FROM src.message_labels
		WHERE message_id IN (SELECT id FROM selected_messages)
		  AND label_id IN (SELECT id FROM labels)`); err != nil {
		return nil, fmt.Errorf("copy message_labels: %w", err)
	}

	// Clean up temp table. On rollback this DROP won't execute, but that's
	// fine — temp tables are connection-scoped and cleaned up on db.Close().
	if _, err := tx.Exec("DROP TABLE IF EXISTS selected_messages"); err != nil {
		return nil, fmt.Errorf("drop temp table: %w", err)
	}

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

func isSafeFilename(filename string) bool {
	// Reject absolute paths and those with null bytes or path separators
	if filepath.IsAbs(filename) || strings.ContainsAny(filename, "\x00/\\") {
		return false
	}
	// Clean and check for traversal (ensures no ".." escapes)
	cleaned := filepath.Clean(filename)
	return filepath.IsLocal(cleaned)
}

// CopyFileIfExists copies a single file from src to dst.
// Returns nil if the source file does not exist.
// Both paths must be absolute. srcContainDir is the root directory that src
// must resolve within after symlink resolution (e.g. the source dataset root).
// dstContainDir is the root directory that dst must be within (e.g. the
// destination dataset root). Both checks prevent symlink escapes.
func CopyFileIfExists(src, dst, srcContainDir, dstContainDir string) error {
	// Validate paths are absolute
	if !filepath.IsAbs(src) || !filepath.IsAbs(dst) {
		return fmt.Errorf("paths must be absolute: src=%q, dst=%q", src, dst)
	}
	if !filepath.IsAbs(srcContainDir) {
		return fmt.Errorf("srcContainDir must be absolute: %q", srcContainDir)
	}
	if !filepath.IsAbs(dstContainDir) {
		return fmt.Errorf("dstContainDir must be absolute: %q", dstContainDir)
	}

	// Verify destination is within dstContainDir.
	// Resolve the parent directory of dst (which must exist) to handle
	// filesystem symlinks (e.g. macOS /var -> /private/var).
	dstParent := filepath.Dir(dst)
	resolvedDstParent, err := filepath.EvalSymlinks(dstParent)
	if err != nil {
		return fmt.Errorf("resolve destination parent directory %s: %w", dstParent, err)
	}
	resolvedDst := filepath.Join(resolvedDstParent, filepath.Base(dst))
	resolvedDstContainDir, err := filepath.EvalSymlinks(dstContainDir)
	if err != nil {
		return fmt.Errorf("resolve destination contain directory %s: %w", dstContainDir, err)
	}
	dstRel, err := filepath.Rel(resolvedDstContainDir, resolvedDst)
	if err != nil || !isSafeFilename(dstRel) {
		return fmt.Errorf("destination file %s is outside %s", dst, dstContainDir)
	}

	// Resolve symlinks in src and verify containment within srcContainDir.
	resolvedSrc, err := filepath.EvalSymlinks(src)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("resolve source file %s: %w", src, err)
	}
	resolvedSrcContainDir, err := filepath.EvalSymlinks(srcContainDir)
	if err != nil {
		return fmt.Errorf("resolve source contain directory %s: %w", srcContainDir, err)
	}
	srcRel, err := filepath.Rel(resolvedSrcContainDir, resolvedSrc)
	if err != nil || !isSafeFilename(srcRel) {
		return fmt.Errorf("source file %s resolves outside %s (symlink escape)", src, srcContainDir)
	}

	srcFile, err := os.Open(resolvedSrc)
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

	if _, err := io.Copy(dstFile, srcFile); err != nil {
		_ = dstFile.Close()
		return fmt.Errorf("copy %s to %s: %w", src, dst, err)
	}

	if err := dstFile.Sync(); err != nil {
		_ = dstFile.Close()
		return fmt.Errorf("sync destination file %s: %w", dst, err)
	}

	if err := dstFile.Close(); err != nil {
		return fmt.Errorf("close destination file %s: %w", dst, err)
	}

	return nil
}
