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
