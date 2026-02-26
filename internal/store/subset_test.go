package store

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

// createTestSourceDB creates a source database with schema and test
// data. Returns the path to the database.
func createTestSourceDB(t *testing.T, dir string, msgCount int) string {
	t.Helper()

	dbPath := filepath.Join(dir, "msgvault.db")

	st, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := st.InitSchema(); err != nil {
		t.Fatalf("InitSchema: %v", err)
	}
	_ = st.Close()

	db, err := sql.Open("sqlite3", dbPath+"?_foreign_keys=OFF")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	_, err = db.Exec(`INSERT INTO sources (id, source_type, identifier)
		VALUES (1, 'gmail', 'test@example.com')`)
	if err != nil {
		t.Fatalf("insert source: %v", err)
	}

	_, err = db.Exec(`
		INSERT INTO participants
			(id, email_address, display_name, domain)
		VALUES
			(1, 'alice@example.com', 'Alice', 'example.com'),
			(2, 'bob@example.com', 'Bob', 'example.com'),
			(3, 'charlie@example.com', 'Charlie', 'example.com')`)
	if err != nil {
		t.Fatalf("insert participants: %v", err)
	}

	_, err = db.Exec(`
		INSERT INTO participant_identifiers
			(id, participant_id, identifier_type, identifier_value)
		VALUES
			(1, 1, 'email', 'alice@example.com'),
			(2, 2, 'email', 'bob@example.com'),
			(3, 3, 'email', 'charlie@example.com')`)
	if err != nil {
		t.Fatalf("insert participant_identifiers: %v", err)
	}

	_, err = db.Exec(`
		INSERT INTO conversations
			(id, source_id, conversation_type, title,
			 message_count, participant_count)
		VALUES
			(1, 1, 'email_thread', 'Thread 1', 5, 2),
			(2, 1, 'email_thread', 'Thread 2', 5, 2)`)
	if err != nil {
		t.Fatalf("insert conversations: %v", err)
	}

	_, err = db.Exec(`
		INSERT INTO conversation_participants
			(conversation_id, participant_id)
		VALUES (1, 1), (1, 2), (2, 2), (2, 3)`)
	if err != nil {
		t.Fatalf("insert conversation_participants: %v", err)
	}

	_, err = db.Exec(`
		INSERT INTO labels (id, source_id, name, label_type)
		VALUES
			(1, 1, 'INBOX', 'system'),
			(2, 1, 'SENT', 'system'),
			(3, 1, 'Work', 'user')`)
	if err != nil {
		t.Fatalf("insert labels: %v", err)
	}

	for i := 1; i <= msgCount; i++ {
		convID := 1
		senderID := 1
		if i > msgCount/2 {
			convID = 2
			senderID = 2
		}

		_, err = db.Exec(`
			INSERT INTO messages
				(id, conversation_id, source_id, source_message_id,
				 message_type, sent_at, sender_id, subject)
			VALUES (?, ?, 1, ?,
				'email',
				datetime('2024-01-01', '+' || ? || ' hours'),
				?, ?)`,
			i, convID, fmt.Sprintf("msg_%d", i),
			i, senderID, "Subject "+string(rune('A'+i%26)))
		if err != nil {
			t.Fatalf("insert message %d: %v", i, err)
		}

		_, err = db.Exec(
			`INSERT INTO message_bodies (message_id, body_text)
			 VALUES (?, ?)`,
			i, "Body of message "+string(rune('A'+i%26)))
		if err != nil {
			t.Fatalf("insert message_body %d: %v", i, err)
		}

		_, err = db.Exec(
			`INSERT INTO message_recipients
				(message_id, participant_id, recipient_type)
			 VALUES (?, ?, 'from')`,
			i, senderID)
		if err != nil {
			t.Fatalf("insert message_recipient from %d: %v", i, err)
		}

		toID := 2
		if senderID == 2 {
			toID = 3
		}
		_, err = db.Exec(
			`INSERT INTO message_recipients
				(message_id, participant_id, recipient_type)
			 VALUES (?, ?, 'to')`,
			i, toID)
		if err != nil {
			t.Fatalf("insert message_recipient to %d: %v", i, err)
		}

		labelID := (i % 3) + 1
		_, err = db.Exec(
			`INSERT INTO message_labels (message_id, label_id)
			 VALUES (?, ?)`,
			i, labelID)
		if err != nil {
			t.Fatalf("insert message_label %d: %v", i, err)
		}
	}

	return dbPath
}

func TestCopySubset_Basic(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := filepath.Join(t.TempDir(), "dst")

	srcDB := createTestSourceDB(t, srcDir, 10)

	result, err := CopySubset(srcDB, dstDir, 5)
	if err != nil {
		t.Fatalf("CopySubset: %v", err)
	}

	if result.Messages != 5 {
		t.Errorf("Messages = %d, want 5", result.Messages)
	}

	db, err := sql.Open("sqlite3", filepath.Join(dstDir, "msgvault.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	var count int64

	if err := db.QueryRow(
		"SELECT COUNT(*) FROM messages",
	).Scan(&count); err != nil {
		t.Fatalf("count messages: %v", err)
	}
	if count != 5 {
		t.Errorf("destination messages = %d, want 5", count)
	}

	if err := db.QueryRow(
		"SELECT COUNT(*) FROM participants",
	).Scan(&count); err != nil {
		t.Fatalf("count participants: %v", err)
	}
	if count == 0 {
		t.Error("expected participants to be copied")
	}

	if err := db.QueryRow(
		"SELECT COUNT(*) FROM conversations",
	).Scan(&count); err != nil {
		t.Fatalf("count conversations: %v", err)
	}
	if count == 0 {
		t.Error("expected conversations to be copied")
	}

	if err := db.QueryRow(
		"SELECT COUNT(*) FROM labels",
	).Scan(&count); err != nil {
		t.Fatalf("count labels: %v", err)
	}
	if count == 0 {
		t.Error("expected labels to be copied")
	}

	if err := db.QueryRow(
		"SELECT COUNT(*) FROM message_labels",
	).Scan(&count); err != nil {
		t.Fatalf("count message_labels: %v", err)
	}
	if count == 0 {
		t.Error("expected message_labels to be copied")
	}

	if err := db.QueryRow(
		"SELECT COUNT(*) FROM message_bodies",
	).Scan(&count); err != nil {
		t.Fatalf("count message_bodies: %v", err)
	}
	if count != 5 {
		t.Errorf("destination message_bodies = %d, want 5", count)
	}

	fkRows, err := db.Query("PRAGMA foreign_key_check")
	if err != nil {
		t.Fatal(err)
	}
	hasViolation := fkRows.Next()
	_ = fkRows.Close()
	if hasViolation {
		t.Error("foreign key violations found in destination database")
	}
}

func TestCopySubset_AllRows(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := filepath.Join(t.TempDir(), "dst")

	srcDB := createTestSourceDB(t, srcDir, 5)

	result, err := CopySubset(srcDB, dstDir, 100)
	if err != nil {
		t.Fatalf("CopySubset: %v", err)
	}

	if result.Messages != 5 {
		t.Errorf("Messages = %d, want 5 (all available)", result.Messages)
	}
}

func TestCopySubset_FTSPopulated(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := filepath.Join(t.TempDir(), "dst")

	srcDB := createTestSourceDB(t, srcDir, 5)

	_, err := CopySubset(srcDB, dstDir, 5)
	if err != nil {
		t.Fatalf("CopySubset: %v", err)
	}

	db, err := sql.Open("sqlite3", filepath.Join(dstDir, "msgvault.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	var count int64
	err = db.QueryRow("SELECT COUNT(*) FROM messages_fts").Scan(&count)
	if err != nil {
		t.Skip("FTS5 not available")
	}
	if count == 0 {
		t.Error("expected FTS index to be populated")
	}
}

func TestCopySubset_ConversationCounts(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := filepath.Join(t.TempDir(), "dst")

	srcDB := createTestSourceDB(t, srcDir, 10)

	_, err := CopySubset(srcDB, dstDir, 5)
	if err != nil {
		t.Fatalf("CopySubset: %v", err)
	}

	db, err := sql.Open("sqlite3", filepath.Join(dstDir, "msgvault.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	rows, err := db.Query(`
		SELECT c.id, c.message_count,
			(SELECT COUNT(*) FROM messages m
			 WHERE m.conversation_id = c.id) AS actual_count
		FROM conversations c`)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var id, denormalized, actual int64
		if err := rows.Scan(&id, &denormalized, &actual); err != nil {
			t.Fatal(err)
		}
		if denormalized != actual {
			t.Errorf(
				"conversation %d: denormalized count=%d, actual=%d",
				id, denormalized, actual,
			)
		}
	}
}

func TestCopySubset_DestinationEmptyDir(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := filepath.Join(t.TempDir(), "dst")

	srcDB := createTestSourceDB(t, srcDir, 5)

	if err := os.MkdirAll(dstDir, 0755); err != nil {
		t.Fatal(err)
	}

	result, err := CopySubset(srcDB, dstDir, 5)
	if err != nil {
		t.Fatalf("CopySubset with pre-existing empty dir: %v", err)
	}

	if result.Messages != 5 {
		t.Errorf("Messages = %d, want 5", result.Messages)
	}

	if _, err := os.Stat(
		filepath.Join(dstDir, "msgvault.db"),
	); err != nil {
		t.Errorf("msgvault.db not created: %v", err)
	}
}

func TestCopySubset_DestinationDBExists(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := filepath.Join(t.TempDir(), "dst")

	srcDB := createTestSourceDB(t, srcDir, 5)

	if err := os.MkdirAll(dstDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		filepath.Join(dstDir, "msgvault.db"), []byte("existing"), 0644,
	); err != nil {
		t.Fatal(err)
	}

	_, err := CopySubset(srcDB, dstDir, 5)
	if err == nil {
		t.Fatal("expected error when destination DB exists")
	}
	if !strings.Contains(err.Error(), "destination database already exists") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestCopySubset_SQLInjectionInPath(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := filepath.Join(t.TempDir(), "dst")

	quotedDir := filepath.Join(srcDir, "test'db")
	if err := os.MkdirAll(quotedDir, 0755); err != nil {
		t.Fatal(err)
	}
	srcDB := createTestSourceDB(t, quotedDir, 3)

	result, err := CopySubset(srcDB, dstDir, 3)
	if err != nil {
		t.Fatalf("CopySubset with quoted path: %v", err)
	}
	if result.Messages != 3 {
		t.Errorf("Messages = %d, want 3", result.Messages)
	}
}

func TestCopySubset_NonPositiveRowCount(t *testing.T) {
	for _, n := range []int{0, -1, -100} {
		_, err := CopySubset("/tmp/fake.db", t.TempDir(), n)
		if err == nil {
			t.Errorf("CopySubset(rowCount=%d) = nil error, want error", n)
		}
	}
}

func TestCopySubset_TimestampFallback(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := filepath.Join(t.TempDir(), "dst")

	dbPath := filepath.Join(srcDir, "msgvault.db")
	st, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := st.InitSchema(); err != nil {
		t.Fatalf("InitSchema: %v", err)
	}
	_ = st.Close()

	db, err := sql.Open("sqlite3", dbPath+"?_foreign_keys=OFF")
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec(`
		INSERT INTO sources (id, source_type, identifier)
		VALUES (1, 'gmail', 'test@example.com')`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`
		INSERT INTO participants (id, email_address, domain)
		VALUES (1, 'alice@example.com', 'example.com')`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`
		INSERT INTO conversations
			(id, source_id, conversation_type, title,
			 message_count, participant_count)
		VALUES (1, 1, 'email_thread', 'Thread', 3, 1)`)
	if err != nil {
		t.Fatal(err)
	}

	// msg 1: only received_at (no sent_at), most recent
	_, err = db.Exec(`
		INSERT INTO messages
			(id, conversation_id, source_id, source_message_id,
			 message_type, received_at, sender_id, subject)
		VALUES (1, 1, 1, 'msg_1', 'email', '2025-06-01', 1,
			'Received only')`)
	if err != nil {
		t.Fatal(err)
	}

	// msg 2: only internal_date (no sent_at), second most recent
	_, err = db.Exec(`
		INSERT INTO messages
			(id, conversation_id, source_id, source_message_id,
			 message_type, internal_date, sender_id, subject)
		VALUES (2, 1, 1, 'msg_2', 'email', '2025-05-01', 1,
			'Internal only')`)
	if err != nil {
		t.Fatal(err)
	}

	// msg 3: has sent_at, oldest
	_, err = db.Exec(`
		INSERT INTO messages
			(id, conversation_id, source_id, source_message_id,
			 message_type, sent_at, sender_id, subject)
		VALUES (3, 1, 1, 'msg_3', 'email', '2025-04-01', 1,
			'Sent only')`)
	if err != nil {
		t.Fatal(err)
	}

	for i := 1; i <= 3; i++ {
		_, err = db.Exec(`
			INSERT INTO message_recipients
				(message_id, participant_id, recipient_type)
			VALUES (?, 1, 'from')`, i)
		if err != nil {
			t.Fatal(err)
		}
	}
	_ = db.Close()

	// Request 2 most recent — should get msg 1 and 2 (by fallback
	// timestamps), not just msg 3 (the only one with sent_at).
	result, err := CopySubset(dbPath, dstDir, 2)
	if err != nil {
		t.Fatalf("CopySubset: %v", err)
	}
	if result.Messages != 2 {
		t.Errorf("Messages = %d, want 2", result.Messages)
	}

	dstDB, err := sql.Open("sqlite3",
		filepath.Join(dstDir, "msgvault.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dstDB.Close() }()

	var subjects []string
	rows, err := dstDB.Query("SELECT subject FROM messages")
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatal(err)
		}
		subjects = append(subjects, s)
	}
	_ = rows.Close()

	for _, s := range subjects {
		if s == "Sent only" {
			t.Error("oldest message (sent_at only) should not be selected")
		}
	}

	// last_message_at must use the fallback timestamp, not be NULL
	var lastMsg sql.NullString
	if err := dstDB.QueryRow(
		"SELECT last_message_at FROM conversations",
	).Scan(&lastMsg); err != nil {
		t.Fatal(err)
	}
	if !lastMsg.Valid {
		t.Error("last_message_at is NULL; should use fallback timestamp")
	}
}

func TestCopySubset_TieBreaker(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := filepath.Join(t.TempDir(), "dst")

	dbPath := filepath.Join(srcDir, "msgvault.db")
	st, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := st.InitSchema(); err != nil {
		t.Fatalf("InitSchema: %v", err)
	}
	_ = st.Close()

	db, err := sql.Open("sqlite3", dbPath+"?_foreign_keys=OFF")
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec(`
		INSERT INTO sources (id, source_type, identifier)
		VALUES (1, 'gmail', 'test@example.com')`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`
		INSERT INTO participants (id, email_address, domain)
		VALUES (1, 'alice@example.com', 'example.com')`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`
		INSERT INTO conversations
			(id, source_id, conversation_type, title,
			 message_count, participant_count)
		VALUES (1, 1, 'email_thread', 'Thread', 4, 1)`)
	if err != nil {
		t.Fatal(err)
	}

	// 4 messages with identical timestamps; higher IDs should win
	sameTime := "2025-06-01 12:00:00"
	for i := 1; i <= 4; i++ {
		_, err = db.Exec(`
			INSERT INTO messages
				(id, conversation_id, source_id, source_message_id,
				 message_type, sent_at, sender_id, subject)
			VALUES (?, 1, 1, ?, 'email', ?, 1, ?)`,
			i, fmt.Sprintf("msg_%d", i), sameTime,
			fmt.Sprintf("Msg %d", i))
		if err != nil {
			t.Fatalf("insert message %d: %v", i, err)
		}
		_, err = db.Exec(`
			INSERT INTO message_recipients
				(message_id, participant_id, recipient_type)
			VALUES (?, 1, 'from')`, i)
		if err != nil {
			t.Fatal(err)
		}
	}
	_ = db.Close()

	// Select 2 of 4 — should get IDs 4 and 3 (highest IDs)
	result, err := CopySubset(dbPath, dstDir, 2)
	if err != nil {
		t.Fatalf("CopySubset: %v", err)
	}
	if result.Messages != 2 {
		t.Errorf("Messages = %d, want 2", result.Messages)
	}

	dstDB, err := sql.Open("sqlite3",
		filepath.Join(dstDir, "msgvault.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dstDB.Close() }()

	rows, err := dstDB.Query(
		"SELECT id FROM messages ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	_ = rows.Close()

	if len(ids) != 2 || ids[0] != 3 || ids[1] != 4 {
		t.Errorf("selected IDs = %v, want [3, 4]", ids)
	}
}

func TestCopySubset_ReplyToOrphanNulled(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := filepath.Join(t.TempDir(), "dst")

	dbPath := filepath.Join(srcDir, "msgvault.db")
	st, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := st.InitSchema(); err != nil {
		t.Fatalf("InitSchema: %v", err)
	}
	_ = st.Close()

	db, err := sql.Open("sqlite3", dbPath+"?_foreign_keys=OFF")
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec(`
		INSERT INTO sources (id, source_type, identifier)
		VALUES (1, 'gmail', 'test@example.com')`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`
		INSERT INTO participants (id, email_address, domain)
		VALUES (1, 'alice@example.com', 'example.com')`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`
		INSERT INTO conversations
			(id, source_id, conversation_type, title,
			 message_count, participant_count)
		VALUES (1, 1, 'email_thread', 'Thread', 2, 1)`)
	if err != nil {
		t.Fatal(err)
	}

	// Old parent message (won't be selected with limit 1)
	_, err = db.Exec(`
		INSERT INTO messages
			(id, conversation_id, source_id, source_message_id,
			 message_type, sent_at, sender_id, subject)
		VALUES (1, 1, 1, 'parent', 'email', '2020-01-01', 1,
			'Parent')`)
	if err != nil {
		t.Fatal(err)
	}

	// Recent reply referencing the parent
	_, err = db.Exec(`
		INSERT INTO messages
			(id, conversation_id, source_id, source_message_id,
			 message_type, sent_at, sender_id, subject,
			 reply_to_message_id)
		VALUES (2, 1, 1, 'reply', 'email', '2025-06-01', 1,
			'Reply', 1)`)
	if err != nil {
		t.Fatal(err)
	}

	for i := 1; i <= 2; i++ {
		_, err = db.Exec(`
			INSERT INTO message_recipients
				(message_id, participant_id, recipient_type)
			VALUES (?, 1, 'from')`, i)
		if err != nil {
			t.Fatal(err)
		}
	}
	_ = db.Close()

	// Select only 1 most recent — the reply, not the parent
	result, err := CopySubset(dbPath, dstDir, 1)
	if err != nil {
		t.Fatalf("CopySubset: %v", err)
	}
	if result.Messages != 1 {
		t.Errorf("Messages = %d, want 1", result.Messages)
	}

	dstDB, err := sql.Open("sqlite3",
		filepath.Join(dstDir, "msgvault.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dstDB.Close() }()

	// reply_to_message_id should be nulled out since parent
	// wasn't included
	var replyTo sql.NullInt64
	if err := dstDB.QueryRow(`
		SELECT reply_to_message_id FROM messages
		WHERE subject = 'Reply'`,
	).Scan(&replyTo); err != nil {
		t.Fatal(err)
	}
	if replyTo.Valid {
		t.Errorf(
			"reply_to_message_id = %d, want NULL (parent excluded)",
			replyTo.Int64,
		)
	}

	// FK integrity must pass
	fkRows, err := dstDB.Query("PRAGMA foreign_key_check")
	if err != nil {
		t.Fatal(err)
	}
	hasViolation := fkRows.Next()
	_ = fkRows.Close()
	if hasViolation {
		t.Error("FK violations with orphaned reply_to_message_id")
	}
}

func TestCopySubset_ExcludesSoftDeleted(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := filepath.Join(t.TempDir(), "dst")

	srcDB := createTestSourceDB(t, srcDir, 10)

	// Soft-delete the 5 most recent messages
	db, err := sql.Open("sqlite3", srcDB+"?_foreign_keys=OFF")
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`
		UPDATE messages SET deleted_from_source_at = '2025-01-01'
		WHERE id IN (
			SELECT id FROM messages ORDER BY sent_at DESC LIMIT 5
		)`)
	if err != nil {
		t.Fatalf("soft-delete messages: %v", err)
	}
	_ = db.Close()

	// Request 5 messages — should get the 5 non-deleted ones
	result, err := CopySubset(srcDB, dstDir, 5)
	if err != nil {
		t.Fatalf("CopySubset: %v", err)
	}
	if result.Messages != 5 {
		t.Errorf("Messages = %d, want 5", result.Messages)
	}

	dstDB, err := sql.Open("sqlite3",
		filepath.Join(dstDir, "msgvault.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dstDB.Close() }()

	// None of the copied messages should be soft-deleted
	var deletedCount int64
	if err := dstDB.QueryRow(`
		SELECT COUNT(*) FROM messages
		WHERE deleted_from_source_at IS NOT NULL`,
	).Scan(&deletedCount); err != nil {
		t.Fatal(err)
	}
	if deletedCount != 0 {
		t.Errorf("found %d soft-deleted messages in subset", deletedCount)
	}
}

func TestCopySubset_ReactionParticipants(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := filepath.Join(t.TempDir(), "dst")

	srcDB := createTestSourceDB(t, srcDir, 5)

	// Add a reactor participant who is neither sender nor recipient
	db, err := sql.Open("sqlite3", srcDB+"?_foreign_keys=OFF")
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`
		INSERT INTO participants
			(id, email_address, display_name, domain)
		VALUES (100, 'reactor@example.com', 'Reactor', 'example.com')`)
	if err != nil {
		t.Fatalf("insert reactor: %v", err)
	}
	_, err = db.Exec(`
		INSERT INTO reactions
			(id, message_id, participant_id,
			 reaction_type, reaction_value)
		VALUES (1, 1, 100, 'emoji', 'thumbsup')`)
	if err != nil {
		t.Fatalf("insert reaction: %v", err)
	}
	_ = db.Close()

	result, err := CopySubset(srcDB, dstDir, 5)
	if err != nil {
		t.Fatalf("CopySubset: %v", err)
	}
	if result.Messages != 5 {
		t.Errorf("Messages = %d, want 5", result.Messages)
	}

	dstDB, err := sql.Open("sqlite3",
		filepath.Join(dstDir, "msgvault.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dstDB.Close() }()

	// Reactor participant must be present
	var reactorCount int64
	if err := dstDB.QueryRow(`
		SELECT COUNT(*) FROM participants
		WHERE email_address = 'reactor@example.com'`,
	).Scan(&reactorCount); err != nil {
		t.Fatal(err)
	}
	if reactorCount != 1 {
		t.Errorf("reactor participant count = %d, want 1", reactorCount)
	}

	// Reaction must be present
	var rxnCount int64
	if err := dstDB.QueryRow(
		"SELECT COUNT(*) FROM reactions",
	).Scan(&rxnCount); err != nil {
		t.Fatal(err)
	}
	if rxnCount != 1 {
		t.Errorf("reactions count = %d, want 1", rxnCount)
	}

	// FK integrity
	fkRows, err := dstDB.Query("PRAGMA foreign_key_check")
	if err != nil {
		t.Fatal(err)
	}
	hasViolation := fkRows.Next()
	_ = fkRows.Close()
	if hasViolation {
		t.Error("FK violations with reaction participants")
	}
}

// TestCopySubset_NullSourceIDLabels verifies that user-created labels
// with NULL source_id are preserved when attached to selected messages.
func TestCopySubset_NullSourceIDLabels(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := filepath.Join(t.TempDir(), "dst")

	srcDB := createTestSourceDB(t, srcDir, 5)

	// Add a user-created label with NULL source_id and attach it
	// to message 1.
	db, err := sql.Open("sqlite3", srcDB+"?_foreign_keys=OFF")
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`
		INSERT INTO labels (id, source_id, name, label_type)
		VALUES (100, NULL, 'My Custom Label', 'user')`)
	if err != nil {
		t.Fatalf("insert null-source label: %v", err)
	}
	_, err = db.Exec(`
		INSERT INTO message_labels (message_id, label_id)
		VALUES (1, 100)`)
	if err != nil {
		t.Fatalf("insert message_label: %v", err)
	}
	_ = db.Close()

	result, err := CopySubset(srcDB, dstDir, 5)
	if err != nil {
		t.Fatalf("CopySubset: %v", err)
	}

	// The 3 source-scoped labels + 1 user-created label
	if result.Labels != 4 {
		t.Errorf("Labels = %d, want 4", result.Labels)
	}

	dstDB, err := sql.Open("sqlite3",
		filepath.Join(dstDir, "msgvault.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dstDB.Close() }()

	var labelName string
	err = dstDB.QueryRow(`
		SELECT name FROM labels WHERE source_id IS NULL`,
	).Scan(&labelName)
	if err != nil {
		t.Fatalf("query null-source label: %v", err)
	}
	if labelName != "My Custom Label" {
		t.Errorf("label name = %q, want 'My Custom Label'", labelName)
	}

	// message_labels link must be preserved
	var mlCount int64
	if err := dstDB.QueryRow(`
		SELECT COUNT(*) FROM message_labels WHERE label_id = 100`,
	).Scan(&mlCount); err != nil {
		t.Fatal(err)
	}
	if mlCount != 1 {
		t.Errorf("message_labels for label 100 = %d, want 1", mlCount)
	}

	// FK integrity
	fkRows, err := dstDB.Query("PRAGMA foreign_key_check")
	if err != nil {
		t.Fatal(err)
	}
	hasViolation := fkRows.Next()
	_ = fkRows.Close()
	if hasViolation {
		t.Error("FK violations with null-source-id labels")
	}
}

// TestCopySubset_SourceFKViolationIgnored verifies that pre-existing FK
// violations in the source DB (outside the copied subset) don't cause
// CopySubset to fail. This guards against the regression where src was
// still attached during PRAGMA foreign_key_check.
func TestCopySubset_SourceFKViolationIgnored(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := filepath.Join(t.TempDir(), "dst")

	srcDB := createTestSourceDB(t, srcDir, 5)

	// Inject an FK violation in the source: a message_labels row
	// referencing a non-existent label_id.
	db, err := sql.Open("sqlite3", srcDB+"?_foreign_keys=OFF")
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`
		INSERT INTO message_labels (message_id, label_id)
		VALUES (1, 9999)`)
	if err != nil {
		t.Fatalf("inject FK violation: %v", err)
	}
	_ = db.Close()

	// CopySubset should succeed — FK check must only scan destination
	result, err := CopySubset(srcDB, dstDir, 3)
	if err != nil {
		t.Fatalf("CopySubset failed (source FK leak): %v", err)
	}
	if result.Messages != 3 {
		t.Errorf("Messages = %d, want 3", result.Messages)
	}
}

func TestCopySubset_MissingSourceDB(t *testing.T) {
	dstDir := filepath.Join(t.TempDir(), "dst")
	fakeSrc := filepath.Join(t.TempDir(), "nonexistent.db")

	_, err := CopySubset(fakeSrc, dstDir, 5)
	if err == nil {
		t.Fatal("expected error for missing source DB")
	}
	if !strings.Contains(err.Error(), "source database not found") {
		t.Errorf("unexpected error: %v", err)
	}

	// ATTACH on a missing path would create a file; verify it wasn't
	if _, statErr := os.Stat(fakeSrc); !os.IsNotExist(statErr) {
		t.Errorf("missing source path was created as a side effect")
	}

	// Destination should be cleaned up
	if _, statErr := os.Stat(dstDir); !os.IsNotExist(statErr) {
		t.Errorf("destination directory was not cleaned up")
	}
}

func TestCopySubset_MultiSourceScoping(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := filepath.Join(t.TempDir(), "dst")

	dbPath := filepath.Join(srcDir, "msgvault.db")

	st, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := st.InitSchema(); err != nil {
		t.Fatalf("InitSchema: %v", err)
	}
	_ = st.Close()

	db, err := sql.Open("sqlite3", dbPath+"?_foreign_keys=OFF")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}

	// Two sources: only source 1 will have recent messages
	_, err = db.Exec(`
		INSERT INTO sources (id, source_type, identifier) VALUES
			(1, 'gmail', 'alice@example.com'),
			(2, 'gmail', 'bob@example.com')`)
	if err != nil {
		t.Fatalf("insert sources: %v", err)
	}

	_, err = db.Exec(`
		INSERT INTO participants
			(id, email_address, display_name, domain)
		VALUES
			(1, 'alice@example.com', 'Alice', 'example.com'),
			(2, 'bob@example.com', 'Bob', 'example.com')`)
	if err != nil {
		t.Fatalf("insert participants: %v", err)
	}

	_, err = db.Exec(`
		INSERT INTO conversations
			(id, source_id, conversation_type, title,
			 message_count, participant_count)
		VALUES
			(1, 1, 'email_thread', 'Alice thread', 2, 1),
			(2, 2, 'email_thread', 'Bob thread', 2, 1)`)
	if err != nil {
		t.Fatalf("insert conversations: %v", err)
	}

	// Labels for both sources
	_, err = db.Exec(`
		INSERT INTO labels (id, source_id, name, label_type) VALUES
			(1, 1, 'INBOX', 'system'),
			(2, 1, 'Work', 'user'),
			(3, 2, 'INBOX', 'system'),
			(4, 2, 'Personal', 'user')`)
	if err != nil {
		t.Fatalf("insert labels: %v", err)
	}

	// Source 1 messages: recent (will be selected)
	for i := 1; i <= 3; i++ {
		_, err = db.Exec(`
			INSERT INTO messages
				(id, conversation_id, source_id, source_message_id,
				 message_type, sent_at, sender_id, subject)
			VALUES (?, 1, 1, ?, 'email',
				datetime('2025-01-01', '+' || ? || ' hours'),
				1, ?)`,
			i, fmt.Sprintf("msg_%d", i), i,
			fmt.Sprintf("Alice msg %d", i))
		if err != nil {
			t.Fatalf("insert alice message %d: %v", i, err)
		}
		_, err = db.Exec(
			`INSERT INTO message_recipients
				(message_id, participant_id, recipient_type)
			 VALUES (?, 1, 'from')`, i)
		if err != nil {
			t.Fatalf("insert alice recipient %d: %v", i, err)
		}
	}

	// Source 2 messages: older (won't be selected with limit 3)
	for i := 4; i <= 6; i++ {
		_, err = db.Exec(`
			INSERT INTO messages
				(id, conversation_id, source_id, source_message_id,
				 message_type, sent_at, sender_id, subject)
			VALUES (?, 2, 2, ?, 'email',
				datetime('2020-01-01', '+' || ? || ' hours'),
				2, ?)`,
			i, fmt.Sprintf("msg_%d", i), i,
			fmt.Sprintf("Bob msg %d", i))
		if err != nil {
			t.Fatalf("insert bob message %d: %v", i, err)
		}
		_, err = db.Exec(
			`INSERT INTO message_recipients
				(message_id, participant_id, recipient_type)
			 VALUES (?, 2, 'from')`, i)
		if err != nil {
			t.Fatalf("insert bob recipient %d: %v", i, err)
		}
	}

	_ = db.Close()

	// Select only 3 most recent = all Alice, no Bob
	result, err := CopySubset(dbPath, dstDir, 3)
	if err != nil {
		t.Fatalf("CopySubset: %v", err)
	}

	if result.Sources != 1 {
		t.Errorf("Sources = %d, want 1 (only Alice's)", result.Sources)
	}
	if result.Messages != 3 {
		t.Errorf("Messages = %d, want 3", result.Messages)
	}

	dstDB, err := sql.Open("sqlite3",
		filepath.Join(dstDir, "msgvault.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dstDB.Close() }()

	// Only source 1 should be present
	var srcCount int64
	if err := dstDB.QueryRow(
		"SELECT COUNT(*) FROM sources",
	).Scan(&srcCount); err != nil {
		t.Fatal(err)
	}
	if srcCount != 1 {
		t.Errorf("sources count = %d, want 1", srcCount)
	}

	var identifier string
	if err := dstDB.QueryRow(
		"SELECT identifier FROM sources",
	).Scan(&identifier); err != nil {
		t.Fatal(err)
	}
	if identifier != "alice@example.com" {
		t.Errorf(
			"source identifier = %q, want alice@example.com",
			identifier,
		)
	}

	// Only source 1 labels should be present
	var labelCount int64
	if err := dstDB.QueryRow(
		"SELECT COUNT(*) FROM labels",
	).Scan(&labelCount); err != nil {
		t.Fatal(err)
	}
	if labelCount != 2 {
		t.Errorf("labels count = %d, want 2 (Alice's labels only)",
			labelCount)
	}

	// No Bob conversations
	var convCount int64
	if err := dstDB.QueryRow(
		"SELECT COUNT(*) FROM conversations",
	).Scan(&convCount); err != nil {
		t.Fatal(err)
	}
	if convCount != 1 {
		t.Errorf("conversations = %d, want 1 (Alice's only)",
			convCount)
	}

	// FK integrity check
	fkRows, err := dstDB.Query("PRAGMA foreign_key_check")
	if err != nil {
		t.Fatal(err)
	}
	hasViolation := fkRows.Next()
	_ = fkRows.Close()
	if hasViolation {
		t.Error("foreign key violations in multi-source subset")
	}
}

func TestCopySubset_ControlCharInPath(t *testing.T) {
	dstDir := filepath.Join(t.TempDir(), "dst")
	base := t.TempDir()

	controlPaths := []string{
		filepath.Join(base, "test\ndb", "msgvault.db"),
		filepath.Join(base, "test\tdb", "msgvault.db"),
		filepath.Join(base, "test\x7Fdb", "msgvault.db"),
		filepath.Join(base, "test\x01db", "msgvault.db"),
	}
	for _, p := range controlPaths {
		_, err := CopySubset(p, dstDir, 5)
		if err == nil {
			t.Errorf(
				"CopySubset(%q) = nil error, want control char rejection", p,
			)
		}
	}
}
