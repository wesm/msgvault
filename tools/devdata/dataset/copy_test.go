package dataset

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"

	"github.com/wesm/msgvault/internal/store"
)

// createTestSourceDB creates a source database with schema and test data.
// Returns the path to the database.
func createTestSourceDB(t *testing.T, dir string, messageCount int) string {
	t.Helper()

	dbPath := filepath.Join(dir, "msgvault.db")

	// Use store.Open + InitSchema to create proper schema
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	if err := st.InitSchema(); err != nil {
		t.Fatalf("InitSchema: %v", err)
	}
	st.Close()

	// Insert test data directly
	db, err := sql.Open("sqlite3", dbPath+"?_foreign_keys=OFF")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	// Source
	_, err = db.Exec(`INSERT INTO sources (id, source_type, identifier) VALUES (1, 'gmail', 'test@example.com')`)
	if err != nil {
		t.Fatalf("insert source: %v", err)
	}

	// Participants
	_, err = db.Exec(`
		INSERT INTO participants (id, email_address, display_name, domain) VALUES
			(1, 'alice@example.com', 'Alice', 'example.com'),
			(2, 'bob@example.com', 'Bob', 'example.com'),
			(3, 'charlie@example.com', 'Charlie', 'example.com')`)
	if err != nil {
		t.Fatalf("insert participants: %v", err)
	}

	// Participant identifiers
	_, err = db.Exec(`
		INSERT INTO participant_identifiers (id, participant_id, identifier_type, identifier_value) VALUES
			(1, 1, 'email', 'alice@example.com'),
			(2, 2, 'email', 'bob@example.com'),
			(3, 3, 'email', 'charlie@example.com')`)
	if err != nil {
		t.Fatalf("insert participant_identifiers: %v", err)
	}

	// Conversations
	_, err = db.Exec(`
		INSERT INTO conversations (id, source_id, conversation_type, title, message_count, participant_count) VALUES
			(1, 1, 'email_thread', 'Thread 1', 5, 2),
			(2, 1, 'email_thread', 'Thread 2', 5, 2)`)
	if err != nil {
		t.Fatalf("insert conversations: %v", err)
	}

	// Conversation participants
	_, err = db.Exec(`
		INSERT INTO conversation_participants (conversation_id, participant_id) VALUES
			(1, 1), (1, 2), (2, 2), (2, 3)`)
	if err != nil {
		t.Fatalf("insert conversation_participants: %v", err)
	}

	// Labels
	_, err = db.Exec(`
		INSERT INTO labels (id, source_id, name, label_type) VALUES
			(1, 1, 'INBOX', 'system'),
			(2, 1, 'SENT', 'system'),
			(3, 1, 'Work', 'user')`)
	if err != nil {
		t.Fatalf("insert labels: %v", err)
	}

	// Messages
	for i := 1; i <= messageCount; i++ {
		convID := 1
		senderID := 1
		if i > messageCount/2 {
			convID = 2
			senderID = 2
		}

		_, err = db.Exec(`
			INSERT INTO messages (id, conversation_id, source_id, source_message_id, message_type, sent_at, sender_id, subject)
			VALUES (?, ?, 1, ?, 'email', datetime('2024-01-01', '+' || ? || ' hours'), ?, ?)`,
			i, convID, fmt.Sprintf("msg_%d", i), i, senderID, "Subject "+string(rune('A'+i%26)))
		if err != nil {
			t.Fatalf("insert message %d: %v", i, err)
		}

		// Message body
		_, err = db.Exec(`INSERT INTO message_bodies (message_id, body_text) VALUES (?, ?)`,
			i, "Body of message "+string(rune('A'+i%26)))
		if err != nil {
			t.Fatalf("insert message_body %d: %v", i, err)
		}

		// Message recipients (from)
		_, err = db.Exec(`INSERT INTO message_recipients (message_id, participant_id, recipient_type) VALUES (?, ?, 'from')`,
			i, senderID)
		if err != nil {
			t.Fatalf("insert message_recipient from %d: %v", i, err)
		}

		// Message recipients (to)
		toID := 2
		if senderID == 2 {
			toID = 3
		}
		_, err = db.Exec(`INSERT INTO message_recipients (message_id, participant_id, recipient_type) VALUES (?, ?, 'to')`,
			i, toID)
		if err != nil {
			t.Fatalf("insert message_recipient to %d: %v", i, err)
		}

		// Message labels (alternate between labels)
		labelID := (i % 3) + 1
		_, err = db.Exec(`INSERT INTO message_labels (message_id, label_id) VALUES (?, ?)`, i, labelID)
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

	// Verify destination database
	db, err := sql.Open("sqlite3", filepath.Join(dstDir, "msgvault.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var count int64

	// Messages
	if err := db.QueryRow("SELECT COUNT(*) FROM messages").Scan(&count); err != nil {
		t.Fatalf("count messages: %v", err)
	}
	if count != 5 {
		t.Errorf("destination messages = %d, want 5", count)
	}

	// Participants should be present
	if err := db.QueryRow("SELECT COUNT(*) FROM participants").Scan(&count); err != nil {
		t.Fatalf("count participants: %v", err)
	}
	if count == 0 {
		t.Error("expected participants to be copied")
	}

	// Conversations should be present
	if err := db.QueryRow("SELECT COUNT(*) FROM conversations").Scan(&count); err != nil {
		t.Fatalf("count conversations: %v", err)
	}
	if count == 0 {
		t.Error("expected conversations to be copied")
	}

	// Labels should be present
	if err := db.QueryRow("SELECT COUNT(*) FROM labels").Scan(&count); err != nil {
		t.Fatalf("count labels: %v", err)
	}
	if count == 0 {
		t.Error("expected labels to be copied")
	}

	// Message labels
	if err := db.QueryRow("SELECT COUNT(*) FROM message_labels").Scan(&count); err != nil {
		t.Fatalf("count message_labels: %v", err)
	}
	if count == 0 {
		t.Error("expected message_labels to be copied")
	}

	// Message bodies
	if err := db.QueryRow("SELECT COUNT(*) FROM message_bodies").Scan(&count); err != nil {
		t.Fatalf("count message_bodies: %v", err)
	}
	if count != 5 {
		t.Errorf("destination message_bodies = %d, want 5", count)
	}

	// FK check — foreign_key_check is a standalone integrity scan that
	// works regardless of the foreign_keys setting, so no need to enable it.
	fkRows, err := db.Query("PRAGMA foreign_key_check")
	if err != nil {
		t.Fatal(err)
	}
	hasViolation := fkRows.Next()
	fkRows.Close()
	if hasViolation {
		t.Error("foreign key violations found in destination database")
	}
}

func TestCopySubset_AllRows(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := filepath.Join(t.TempDir(), "dst")

	srcDB := createTestSourceDB(t, srcDir, 5)

	// Request more than available
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
	defer db.Close()

	// Check FTS index has rows
	var count int64
	err = db.QueryRow("SELECT COUNT(*) FROM messages_fts").Scan(&count)
	if err != nil {
		// FTS5 may not be available in test build
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
	defer db.Close()

	// Verify denormalized counts match actual data
	rows, err := db.Query(`
		SELECT c.id, c.message_count,
			(SELECT COUNT(*) FROM messages m WHERE m.conversation_id = c.id) AS actual_count
		FROM conversations c`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var id, denormalized, actual int64
		if err := rows.Scan(&id, &denormalized, &actual); err != nil {
			t.Fatal(err)
		}
		if denormalized != actual {
			t.Errorf("conversation %d: denormalized message_count=%d, actual=%d", id, denormalized, actual)
		}
	}
}

func TestCopySubset_DestinationEmptyDir(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := filepath.Join(t.TempDir(), "dst")

	srcDB := createTestSourceDB(t, srcDir, 5)

	// Create destination directory (but not the database file).
	// MkdirAll is idempotent so CopySubset should succeed.
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

	// Verify database was actually created
	if _, err := os.Stat(filepath.Join(dstDir, "msgvault.db")); err != nil {
		t.Errorf("msgvault.db not created in pre-existing directory: %v", err)
	}
}

func TestCopySubset_DestinationDBExists(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := filepath.Join(t.TempDir(), "dst")

	srcDB := createTestSourceDB(t, srcDir, 5)

	// Create destination directory with an existing database file.
	if err := os.MkdirAll(dstDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dstDir, "msgvault.db"), []byte("existing"), 0644); err != nil {
		t.Fatal(err)
	}

	_, err := CopySubset(srcDB, dstDir, 5)
	if err == nil {
		t.Fatal("CopySubset should fail when destination database already exists")
	}
	if !strings.Contains(err.Error(), "destination database already exists") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestCopySubset_SQLInjectionInPath(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := filepath.Join(t.TempDir(), "dst")

	// Create source DB with a name containing single quotes
	quotedDir := filepath.Join(srcDir, "test'db")
	if err := os.MkdirAll(quotedDir, 0755); err != nil {
		t.Fatal(err)
	}
	srcDB := createTestSourceDB(t, quotedDir, 3)

	// This should work without SQL injection
	result, err := CopySubset(srcDB, dstDir, 3)
	if err != nil {
		t.Fatalf("CopySubset with quoted path: %v", err)
	}
	if result.Messages != 3 {
		t.Errorf("Messages = %d, want 3", result.Messages)
	}
}

func TestCopySubset_ControlCharInPath(t *testing.T) {
	dstDir := filepath.Join(t.TempDir(), "dst")
	base := t.TempDir()

	// Paths with control characters should be rejected.
	// These are expected to fail before any file I/O (rejected by the
	// control character check), so the paths need not exist on disk.
	controlPaths := []string{
		filepath.Join(base, "test\ndb", "msgvault.db"),   // newline
		filepath.Join(base, "test\tdb", "msgvault.db"),   // tab
		filepath.Join(base, "test\x7Fdb", "msgvault.db"), // DEL
		filepath.Join(base, "test\x01db", "msgvault.db"), // SOH
	}
	for _, p := range controlPaths {
		_, err := CopySubset(p, dstDir, 5)
		if err == nil {
			t.Errorf("CopySubset(%q) = nil error, want control character rejection", p)
		}
	}
}

func TestCopyFileIfExists(t *testing.T) {
	dir := t.TempDir()

	// Test with existing file
	srcFile := filepath.Join(dir, "config.toml")
	if err := os.WriteFile(srcFile, []byte("[data]\ntest = true\n"), 0644); err != nil {
		t.Fatal(err)
	}

	dstFile := filepath.Join(dir, "dst-config.toml")
	if err := CopyFileIfExists(srcFile, dstFile, dir, dir); err != nil {
		t.Fatalf("CopyFileIfExists: %v", err)
	}

	content, err := os.ReadFile(dstFile)
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != "[data]\ntest = true\n" {
		t.Errorf("copied content = %q, want original", string(content))
	}

	// Test with non-existent file (should not error)
	if err := CopyFileIfExists(filepath.Join(dir, "nonexistent"), filepath.Join(dir, "out"), dir, dir); err != nil {
		t.Fatalf("CopyFileIfExists for missing file: %v", err)
	}

	// Test with relative paths (should error)
	if err := CopyFileIfExists("relative/path", filepath.Join(dir, "out"), dir, dir); err == nil {
		t.Error("expected error for relative source path")
	}
}

func TestCopyFileIfExists_SymlinkEscape(t *testing.T) {
	// Create a dataset directory and an outside directory
	datasetDir := t.TempDir()
	outsideDir := t.TempDir()

	// Create a file outside the dataset
	outsideFile := filepath.Join(outsideDir, "secret.txt")
	if err := os.WriteFile(outsideFile, []byte("secret"), 0644); err != nil {
		t.Fatal(err)
	}

	// Create a symlink inside the dataset pointing outside
	symlinkPath := filepath.Join(datasetDir, "escape.txt")
	if err := os.Symlink(outsideFile, symlinkPath); err != nil {
		t.Fatal(err)
	}

	dstDir := t.TempDir()
	dstFile := filepath.Join(dstDir, "out.txt")
	err := CopyFileIfExists(symlinkPath, dstFile, datasetDir, dstDir)
	if err == nil {
		t.Error("expected error for symlink escaping containDir")
	}
}
