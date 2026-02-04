package cmd

import (
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/marcboeker/go-duckdb"
	_ "github.com/mattn/go-sqlite3"
)

// setupTestSQLite creates a test SQLite database with realistic email data.
func setupTestSQLite(t *testing.T) (string, func()) {
	t.Helper()

	tmpDir, err := os.MkdirTemp("", "msgvault-build-cache-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}

	dbPath := filepath.Join(tmpDir, "test.db")
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("open sqlite: %v", err)
	}
	defer db.Close()

	// Create schema
	schema := `
		CREATE TABLE sources (
			id INTEGER PRIMARY KEY,
			source_type TEXT NOT NULL DEFAULT 'gmail',
			identifier TEXT NOT NULL UNIQUE,
			display_name TEXT
		);

		CREATE TABLE messages (
			id INTEGER PRIMARY KEY,
			source_id INTEGER NOT NULL REFERENCES sources(id),
			source_message_id TEXT NOT NULL,
			conversation_id INTEGER,
			subject TEXT,
			snippet TEXT,
			sent_at TIMESTAMP,
			received_at TIMESTAMP,
			size_estimate INTEGER,
			has_attachments BOOLEAN DEFAULT FALSE,
			deleted_from_source_at TIMESTAMP,
			UNIQUE(source_id, source_message_id)
		);

		CREATE TABLE participants (
			id INTEGER PRIMARY KEY,
			email_address TEXT NOT NULL UNIQUE,
			domain TEXT,
			display_name TEXT
		);

		CREATE TABLE message_recipients (
			id INTEGER PRIMARY KEY,
			message_id INTEGER NOT NULL REFERENCES messages(id),
			participant_id INTEGER NOT NULL REFERENCES participants(id),
			recipient_type TEXT NOT NULL,
			display_name TEXT
		);

		CREATE TABLE labels (
			id INTEGER PRIMARY KEY,
			source_id INTEGER NOT NULL REFERENCES sources(id),
			source_label_id TEXT,
			name TEXT NOT NULL,
			label_type TEXT
		);

		CREATE TABLE message_labels (
			message_id INTEGER NOT NULL REFERENCES messages(id),
			label_id INTEGER NOT NULL REFERENCES labels(id),
			PRIMARY KEY (message_id, label_id)
		);

		CREATE TABLE attachments (
			id INTEGER PRIMARY KEY,
			message_id INTEGER NOT NULL REFERENCES messages(id),
			filename TEXT,
			mime_type TEXT,
			size INTEGER,
			content_hash TEXT
		);
	`

	if _, err := db.Exec(schema); err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("create schema: %v", err)
	}

	// Insert test data
	testData := `
		-- Source
		INSERT INTO sources (id, identifier, display_name) VALUES (1, 'test@gmail.com', 'Test Account');

		-- Participants
		INSERT INTO participants (id, email_address, domain, display_name) VALUES
			(1, 'alice@example.com', 'example.com', 'Alice Smith'),
			(2, 'bob@company.org', 'company.org', 'Bob Jones'),
			(3, 'carol@example.com', 'example.com', 'Carol White'),
			(4, 'dan@other.net', 'other.net', 'Dan Brown');

		-- Labels
		INSERT INTO labels (id, source_id, name) VALUES
			(1, 1, 'INBOX'),
			(2, 1, 'Work'),
			(3, 1, 'IMPORTANT');

		-- Messages (5 messages across 3 months)
		INSERT INTO messages (id, source_id, source_message_id, conversation_id, subject, snippet, sent_at, size_estimate, has_attachments) VALUES
			(1, 1, 'msg1', 101, 'Hello World', 'Preview 1', '2024-01-15 10:00:00', 1000, 0),
			(2, 1, 'msg2', 101, 'Re: Hello', 'Preview 2', '2024-01-16 11:00:00', 2000, 1),
			(3, 1, 'msg3', 102, 'Follow up', 'Preview 3', '2024-02-01 09:00:00', 1500, 0),
			(4, 1, 'msg4', 103, 'Question', 'Preview 4', '2024-02-15 14:00:00', 3000, 1),
			(5, 1, 'msg5', 104, 'Final', 'Preview 5', '2024-03-01 16:00:00', 500, 0);

		-- Message recipients
		-- msg1: from alice, to bob+carol
		INSERT INTO message_recipients (message_id, participant_id, recipient_type, display_name) VALUES
			(1, 1, 'from', 'Alice Smith'),
			(1, 2, 'to', 'Bob Jones'),
			(1, 3, 'to', 'Carol White');
		-- msg2: from alice, to bob, cc dan
		INSERT INTO message_recipients (message_id, participant_id, recipient_type, display_name) VALUES
			(2, 1, 'from', 'Alice Smith'),
			(2, 2, 'to', 'Bob Jones'),
			(2, 4, 'cc', 'Dan Brown');
		-- msg3: from alice, to bob
		INSERT INTO message_recipients (message_id, participant_id, recipient_type, display_name) VALUES
			(3, 1, 'from', 'Alice Smith'),
			(3, 2, 'to', 'Bob Jones');
		-- msg4: from bob, to alice
		INSERT INTO message_recipients (message_id, participant_id, recipient_type, display_name) VALUES
			(4, 2, 'from', 'Bob Jones'),
			(4, 1, 'to', 'Alice Smith');
		-- msg5: from bob, to alice
		INSERT INTO message_recipients (message_id, participant_id, recipient_type, display_name) VALUES
			(5, 2, 'from', 'Bob Jones'),
			(5, 1, 'to', 'Alice Smith');

		-- Message labels
		INSERT INTO message_labels (message_id, label_id) VALUES
			(1, 1), (1, 2),  -- msg1: INBOX, Work
			(2, 1), (2, 3),  -- msg2: INBOX, IMPORTANT
			(3, 1),          -- msg3: INBOX
			(4, 1), (4, 2),  -- msg4: INBOX, Work
			(5, 1);          -- msg5: INBOX

		-- Attachments
		INSERT INTO attachments (message_id, filename, mime_type, size) VALUES
			(2, 'document.pdf', 'application/pdf', 10000),
			(2, 'image.png', 'image/png', 5000),
			(4, 'report.xlsx', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', 20000);
	`

	if _, err := db.Exec(testData); err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("insert test data: %v", err)
	}

	cleanup := func() {
		os.RemoveAll(tmpDir)
	}

	return tmpDir, cleanup
}

// TestBuildCache_BasicExport tests that buildCache creates all expected Parquet files.
func TestBuildCache_BasicExport(t *testing.T) {
	tmpDir, cleanup := setupTestSQLite(t)
	defer cleanup()

	dbPath := filepath.Join(tmpDir, "test.db")
	analyticsDir := filepath.Join(tmpDir, "analytics")

	result, err := buildCache(dbPath, analyticsDir, false)
	if err != nil {
		t.Fatalf("buildCache: %v", err)
	}

	if result.Skipped {
		t.Fatal("expected export to run, but was skipped")
	}

	if result.ExportedCount != 5 {
		t.Errorf("expected 5 exported messages, got %d", result.ExportedCount)
	}

	// Verify all Parquet directories/files were created
	expectedDirs := []string{
		"messages",
		"sources",
		"participants",
		"message_recipients",
		"labels",
		"message_labels",
		"attachments",
	}

	for _, dir := range expectedDirs {
		path := filepath.Join(analyticsDir, dir)
		if _, err := os.Stat(path); os.IsNotExist(err) {
			t.Errorf("expected directory %s to exist", dir)
		}
	}

	// Verify sync state was saved
	stateFile := filepath.Join(analyticsDir, "_last_sync.json")
	if _, err := os.Stat(stateFile); os.IsNotExist(err) {
		t.Fatal("expected _last_sync.json to exist")
	}

	var state syncState
	data, _ := os.ReadFile(stateFile)
	if err := json.Unmarshal(data, &state); err != nil {
		t.Fatalf("parse sync state: %v", err)
	}

	if state.LastMessageID != 5 {
		t.Errorf("expected LastMessageID=5, got %d", state.LastMessageID)
	}
}

// TestBuildCache_DataIntegrity verifies the exported Parquet data matches SQLite.
func TestBuildCache_DataIntegrity(t *testing.T) {
	tmpDir, cleanup := setupTestSQLite(t)
	defer cleanup()

	dbPath := filepath.Join(tmpDir, "test.db")
	analyticsDir := filepath.Join(tmpDir, "analytics")

	if _, err := buildCache(dbPath, analyticsDir, false); err != nil {
		t.Fatalf("buildCache: %v", err)
	}

	// Open DuckDB to query the Parquet files
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()

	// Helper to count rows in a Parquet file
	countRows := func(pattern string) int64 {
		var count int64
		query := "SELECT COUNT(*) FROM read_parquet('" + pattern + "')"
		if err := db.QueryRow(query).Scan(&count); err != nil {
			t.Fatalf("count %s: %v", pattern, err)
		}
		return count
	}

	// Verify row counts
	tests := []struct {
		name     string
		pattern  string
		expected int64
	}{
		{"messages", filepath.Join(analyticsDir, "messages", "**", "*.parquet"), 5},
		{"sources", filepath.Join(analyticsDir, "sources", "*.parquet"), 1},
		{"participants", filepath.Join(analyticsDir, "participants", "*.parquet"), 4},
		{"message_recipients", filepath.Join(analyticsDir, "message_recipients", "*.parquet"), 12},
		{"labels", filepath.Join(analyticsDir, "labels", "*.parquet"), 3},
		{"message_labels", filepath.Join(analyticsDir, "message_labels", "*.parquet"), 8},
		{"attachments", filepath.Join(analyticsDir, "attachments", "*.parquet"), 3},
	}

	for _, tc := range tests {
		count := countRows(tc.pattern)
		if count != tc.expected {
			t.Errorf("%s: expected %d rows, got %d", tc.name, tc.expected, count)
		}
	}

	// Verify message data integrity
	var subject string
	msgQuery := "SELECT subject FROM read_parquet('" + filepath.Join(analyticsDir, "messages", "**", "*.parquet") + "') WHERE id = 1"
	if err := db.QueryRow(msgQuery).Scan(&subject); err != nil {
		t.Fatalf("query message: %v", err)
	}
	if subject != "Hello World" {
		t.Errorf("expected subject 'Hello World', got %q", subject)
	}

	// Verify participant data
	var email string
	partQuery := "SELECT email_address FROM read_parquet('" + filepath.Join(analyticsDir, "participants", "*.parquet") + "') WHERE id = 1"
	if err := db.QueryRow(partQuery).Scan(&email); err != nil {
		t.Fatalf("query participant: %v", err)
	}
	if email != "alice@example.com" {
		t.Errorf("expected email 'alice@example.com', got %q", email)
	}

	// Verify attachment sizes
	var totalSize int64
	attQuery := "SELECT SUM(size) FROM read_parquet('" + filepath.Join(analyticsDir, "attachments", "*.parquet") + "')"
	if err := db.QueryRow(attQuery).Scan(&totalSize); err != nil {
		t.Fatalf("query attachments: %v", err)
	}
	if totalSize != 35000 { // 10000 + 5000 + 20000
		t.Errorf("expected total attachment size 35000, got %d", totalSize)
	}
}

// TestBuildCache_IncrementalExport tests that incremental exports only add new messages.
func TestBuildCache_IncrementalExport(t *testing.T) {
	tmpDir, cleanup := setupTestSQLite(t)
	defer cleanup()

	dbPath := filepath.Join(tmpDir, "test.db")
	analyticsDir := filepath.Join(tmpDir, "analytics")

	// First export
	result1, err := buildCache(dbPath, analyticsDir, false)
	if err != nil {
		t.Fatalf("first buildCache: %v", err)
	}
	if result1.ExportedCount != 5 {
		t.Errorf("first export: expected 5 messages, got %d", result1.ExportedCount)
	}

	// Add new messages to SQLite
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}

	_, err = db.Exec(`
		INSERT INTO messages (id, source_id, source_message_id, conversation_id, subject, snippet, sent_at, size_estimate, has_attachments) VALUES
			(6, 1, 'msg6', 105, 'New Message 1', 'Preview 6', '2024-03-15 10:00:00', 1200, 0),
			(7, 1, 'msg7', 105, 'New Message 2', 'Preview 7', '2024-03-16 11:00:00', 1300, 0);

		INSERT INTO message_recipients (message_id, participant_id, recipient_type, display_name) VALUES
			(6, 1, 'from', 'Alice Smith'),
			(6, 2, 'to', 'Bob Jones'),
			(7, 2, 'from', 'Bob Jones'),
			(7, 1, 'to', 'Alice Smith');

		INSERT INTO message_labels (message_id, label_id) VALUES
			(6, 1),
			(7, 1);

		INSERT INTO attachments (message_id, filename, mime_type, size) VALUES
			(7, 'notes.txt', 'text/plain', 500);
	`)
	db.Close()
	if err != nil {
		t.Fatalf("insert new messages: %v", err)
	}

	// Second export (incremental)
	result2, err := buildCache(dbPath, analyticsDir, false)
	if err != nil {
		t.Fatalf("second buildCache: %v", err)
	}

	if result2.Skipped {
		t.Fatal("expected incremental export to run, but was skipped")
	}

	// Verify total count includes both old and new
	if result2.ExportedCount != 7 {
		t.Errorf("after incremental: expected 7 total messages, got %d", result2.ExportedCount)
	}

	// Verify junction tables accumulated across incremental runs
	duckdb, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer duckdb.Close()

	countRows := func(pattern string) int64 {
		var count int64
		// Use forward slashes for DuckDB glob patterns (backslashes fail on Windows)
		pattern = filepath.ToSlash(pattern)
		if err := duckdb.QueryRow("SELECT COUNT(*) FROM read_parquet('" + pattern + "')").Scan(&count); err != nil {
			t.Fatalf("count %s: %v", pattern, err)
		}
		return count
	}

	// Messages: 7 total (5 original + 2 new)
	if c := countRows(filepath.Join(analyticsDir, "messages", "**", "*.parquet")); c != 7 {
		t.Errorf("messages: expected 7, got %d", c)
	}

	// Message recipients: 16 total (12 original + 4 new)
	if c := countRows(filepath.Join(analyticsDir, "message_recipients", "*.parquet")); c != 16 {
		t.Errorf("message_recipients: expected 16, got %d", c)
	}

	// Message labels: 10 total (8 original + 2 new)
	if c := countRows(filepath.Join(analyticsDir, "message_labels", "*.parquet")); c != 10 {
		t.Errorf("message_labels: expected 10, got %d", c)
	}

	// Attachments: 4 total (3 original + 1 new)
	if c := countRows(filepath.Join(analyticsDir, "attachments", "*.parquet")); c != 4 {
		t.Errorf("attachments: expected 4, got %d", c)
	}

	// Participants: 4 (overwritten each run, not appended)
	if c := countRows(filepath.Join(analyticsDir, "participants", "*.parquet")); c != 4 {
		t.Errorf("participants: expected 4, got %d", c)
	}

	// Labels: 3 (overwritten each run)
	if c := countRows(filepath.Join(analyticsDir, "labels", "*.parquet")); c != 3 {
		t.Errorf("labels: expected 3, got %d", c)
	}

	// Sources: 1 (overwritten each run)
	if c := countRows(filepath.Join(analyticsDir, "sources", "*.parquet")); c != 1 {
		t.Errorf("sources: expected 1, got %d", c)
	}

	// Verify sync state was updated
	var state syncState
	data, _ := os.ReadFile(filepath.Join(analyticsDir, "_last_sync.json"))
	_ = json.Unmarshal(data, &state)

	if state.LastMessageID != 7 {
		t.Errorf("expected LastMessageID=7, got %d", state.LastMessageID)
	}
}

// TestBuildCache_SkipsWhenNoNewMessages tests that export is skipped when no new messages.
func TestBuildCache_SkipsWhenNoNewMessages(t *testing.T) {
	tmpDir, cleanup := setupTestSQLite(t)
	defer cleanup()

	dbPath := filepath.Join(tmpDir, "test.db")
	analyticsDir := filepath.Join(tmpDir, "analytics")

	// First export
	if _, err := buildCache(dbPath, analyticsDir, false); err != nil {
		t.Fatalf("first buildCache: %v", err)
	}

	// Second export without any new data
	result, err := buildCache(dbPath, analyticsDir, false)
	if err != nil {
		t.Fatalf("second buildCache: %v", err)
	}

	if !result.Skipped {
		t.Error("expected export to be skipped when no new messages")
	}
}

// TestBuildCache_FullRebuild tests that --full-rebuild clears and recreates cache.
func TestBuildCache_FullRebuild(t *testing.T) {
	tmpDir, cleanup := setupTestSQLite(t)
	defer cleanup()

	dbPath := filepath.Join(tmpDir, "test.db")
	analyticsDir := filepath.Join(tmpDir, "analytics")

	// First export
	if _, err := buildCache(dbPath, analyticsDir, false); err != nil {
		t.Fatalf("first buildCache: %v", err)
	}

	// Create a marker file to verify directory is cleared
	markerFile := filepath.Join(analyticsDir, "messages", "marker.txt")
	_ = os.WriteFile(markerFile, []byte("test"), 0644)

	// Full rebuild
	result, err := buildCache(dbPath, analyticsDir, true)
	if err != nil {
		t.Fatalf("full rebuild: %v", err)
	}

	if result.Skipped {
		t.Fatal("full rebuild should not be skipped")
	}

	// Verify marker file was removed
	if _, err := os.Stat(markerFile); !os.IsNotExist(err) {
		t.Error("expected marker file to be removed during full rebuild")
	}

	// Verify data was exported
	if result.ExportedCount != 5 {
		t.Errorf("expected 5 messages after full rebuild, got %d", result.ExportedCount)
	}
}

// TestBuildCache_DeletedMessagesIncluded tests that deleted messages are exported.
func TestBuildCache_DeletedMessagesIncluded(t *testing.T) {
	tmpDir, cleanup := setupTestSQLite(t)
	defer cleanup()

	dbPath := filepath.Join(tmpDir, "test.db")
	analyticsDir := filepath.Join(tmpDir, "analytics")

	// Mark one message as deleted
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	_, err = db.Exec("UPDATE messages SET deleted_from_source_at = '2024-06-01 12:00:00' WHERE id = 3")
	db.Close()
	if err != nil {
		t.Fatalf("mark deleted: %v", err)
	}

	// Export
	result, err := buildCache(dbPath, analyticsDir, false)
	if err != nil {
		t.Fatalf("buildCache: %v", err)
	}

	// All 5 messages should be exported (including deleted)
	if result.ExportedCount != 5 {
		t.Errorf("expected 5 messages (including deleted), got %d", result.ExportedCount)
	}

	// Verify deleted_from_source_at is preserved
	duckdb, _ := sql.Open("duckdb", "")
	defer duckdb.Close()

	var deletedCount int64
	query := "SELECT COUNT(*) FROM read_parquet('" + filepath.Join(analyticsDir, "messages", "**", "*.parquet") + "') WHERE deleted_from_source_at IS NOT NULL"
	if err := duckdb.QueryRow(query).Scan(&deletedCount); err != nil {
		t.Fatalf("query deleted: %v", err)
	}

	if deletedCount != 1 {
		t.Errorf("expected 1 deleted message in Parquet, got %d", deletedCount)
	}
}

// TestBuildCache_MessagesWithoutSentAt tests that messages without sent_at are excluded.
func TestBuildCache_MessagesWithoutSentAt(t *testing.T) {
	tmpDir, cleanup := setupTestSQLite(t)
	defer cleanup()

	dbPath := filepath.Join(tmpDir, "test.db")
	analyticsDir := filepath.Join(tmpDir, "analytics")

	// Add a message without sent_at
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	_, err = db.Exec(`
		INSERT INTO messages (id, source_id, source_message_id, subject, snippet, size_estimate)
		VALUES (6, 1, 'msg6', 'No Date', 'Preview', 100)
	`)
	db.Close()
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	result, err := buildCache(dbPath, analyticsDir, false)
	if err != nil {
		t.Fatalf("buildCache: %v", err)
	}

	// Only 5 messages with sent_at should be exported
	if result.ExportedCount != 5 {
		t.Errorf("expected 5 messages (excluding null sent_at), got %d", result.ExportedCount)
	}
}

// TestBuildCache_EndToEndWithQueryEngine tests the full flow with query engine.
func TestBuildCache_EndToEndWithQueryEngine(t *testing.T) {
	tmpDir, cleanup := setupTestSQLite(t)
	defer cleanup()

	dbPath := filepath.Join(tmpDir, "test.db")
	analyticsDir := filepath.Join(tmpDir, "analytics")

	// Build cache
	if _, err := buildCache(dbPath, analyticsDir, false); err != nil {
		t.Fatalf("buildCache: %v", err)
	}

	// Open DuckDB and test queries that match what the TUI does
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()

	// Build the CTEs like the query engine does
	ctes := `
		WITH
		msg AS (SELECT * FROM read_parquet('` + filepath.Join(analyticsDir, "messages", "**", "*.parquet") + `', hive_partitioning=true)),
		mr AS (SELECT * FROM read_parquet('` + filepath.Join(analyticsDir, "message_recipients", "*.parquet") + `')),
		p AS (SELECT * FROM read_parquet('` + filepath.Join(analyticsDir, "participants", "*.parquet") + `')),
		lbl AS (SELECT * FROM read_parquet('` + filepath.Join(analyticsDir, "labels", "*.parquet") + `')),
		ml AS (SELECT * FROM read_parquet('` + filepath.Join(analyticsDir, "message_labels", "*.parquet") + `')),
		att AS (SELECT message_id, SUM(size) as attachment_size, COUNT(*) as attachment_count FROM read_parquet('` + filepath.Join(analyticsDir, "attachments", "*.parquet") + `') GROUP BY message_id)
	`

	// Test 1: Aggregate by sender (like AggregateBySender)
	senderQuery := ctes + `
		SELECT p.email_address as key, COUNT(*) as count
		FROM msg
		JOIN mr ON mr.message_id = msg.id AND mr.recipient_type = 'from'
		JOIN p ON p.id = mr.participant_id
		GROUP BY p.email_address
		ORDER BY count DESC
	`
	rows, err := db.Query(senderQuery)
	if err != nil {
		t.Fatalf("sender query: %v", err)
	}

	senderCounts := make(map[string]int64)
	for rows.Next() {
		var email string
		var count int64
		_ = rows.Scan(&email, &count)
		senderCounts[email] = count
	}
	rows.Close()

	if senderCounts["alice@example.com"] != 3 {
		t.Errorf("expected alice sent 3 messages, got %d", senderCounts["alice@example.com"])
	}
	if senderCounts["bob@company.org"] != 2 {
		t.Errorf("expected bob sent 2 messages, got %d", senderCounts["bob@company.org"])
	}

	// Test 2: Aggregate by label (like AggregateByLabel)
	labelQuery := ctes + `
		SELECT lbl.name as key, COUNT(*) as count
		FROM msg
		JOIN ml ON ml.message_id = msg.id
		JOIN lbl ON lbl.id = ml.label_id
		GROUP BY lbl.name
		ORDER BY count DESC
	`
	rows, err = db.Query(labelQuery)
	if err != nil {
		t.Fatalf("label query: %v", err)
	}

	labelCounts := make(map[string]int64)
	for rows.Next() {
		var name string
		var count int64
		_ = rows.Scan(&name, &count)
		labelCounts[name] = count
	}
	rows.Close()

	if labelCounts["INBOX"] != 5 {
		t.Errorf("expected INBOX has 5 messages, got %d", labelCounts["INBOX"])
	}
	if labelCounts["Work"] != 2 {
		t.Errorf("expected Work has 2 messages, got %d", labelCounts["Work"])
	}

	// Test 3: Total stats (like GetTotalStats)
	statsQuery := ctes + `
		SELECT
			COUNT(*) as message_count,
			COALESCE(SUM(msg.size_estimate), 0) as total_size,
			COALESCE(SUM(att.attachment_count), 0) as attachment_count,
			COALESCE(SUM(att.attachment_size), 0) as attachment_size
		FROM msg
		LEFT JOIN att ON att.message_id = msg.id
	`
	var msgCount, totalSize, attCount, attSize int64
	if err := db.QueryRow(statsQuery).Scan(&msgCount, &totalSize, &attCount, &attSize); err != nil {
		t.Fatalf("stats query: %v", err)
	}

	if msgCount != 5 {
		t.Errorf("expected 5 messages, got %d", msgCount)
	}
	if totalSize != 8000 { // 1000+2000+1500+3000+500
		t.Errorf("expected total size 8000, got %d", totalSize)
	}
	if attCount != 3 {
		t.Errorf("expected 3 attachments, got %d", attCount)
	}
	if attSize != 35000 { // 10000+5000+20000
		t.Errorf("expected attachment size 35000, got %d", attSize)
	}
}

// TestBuildCache_YearPartitioning tests that messages are partitioned by year.
func TestBuildCache_YearPartitioning(t *testing.T) {
	tmpDir, cleanup := setupTestSQLite(t)
	defer cleanup()

	dbPath := filepath.Join(tmpDir, "test.db")
	analyticsDir := filepath.Join(tmpDir, "analytics")

	// Add messages from different years
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	_, err = db.Exec(`
		INSERT INTO messages (id, source_id, source_message_id, subject, sent_at, size_estimate) VALUES
			(6, 1, 'msg6', 'Old Message', '2020-06-15 10:00:00', 100),
			(7, 1, 'msg7', 'Recent Message', '2025-01-15 10:00:00', 100);
	`)
	db.Close()
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	if _, err := buildCache(dbPath, analyticsDir, false); err != nil {
		t.Fatalf("buildCache: %v", err)
	}

	// Check for year partitions
	years := []string{"2020", "2024", "2025"}
	for _, year := range years {
		pattern := filepath.Join(analyticsDir, "messages", "year="+year, "*.parquet")
		matches, _ := filepath.Glob(pattern)
		if len(matches) == 0 {
			t.Errorf("expected partition for year=%s", year)
		}
	}
}

// TestBuildCache_UTF8Handling tests that invalid UTF-8 is handled gracefully.
func TestBuildCache_UTF8Handling(t *testing.T) {
	tmpDir, cleanup := setupTestSQLite(t)
	defer cleanup()

	dbPath := filepath.Join(tmpDir, "test.db")
	analyticsDir := filepath.Join(tmpDir, "analytics")

	// Insert data with potentially problematic characters
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	// Note: SQLite3 driver may sanitize, but we test the flow
	_, err = db.Exec(`
		UPDATE messages SET subject = 'Test émoji 🎉 and unicode' WHERE id = 1;
		UPDATE participants SET display_name = 'Müller' WHERE id = 1;
	`)
	db.Close()
	if err != nil {
		t.Fatalf("update: %v", err)
	}

	// Should not error
	result, err := buildCache(dbPath, analyticsDir, false)
	if err != nil {
		t.Fatalf("buildCache with unicode: %v", err)
	}

	if result.ExportedCount != 5 {
		t.Errorf("expected 5 messages, got %d", result.ExportedCount)
	}

	// Verify data is readable
	duckdb, _ := sql.Open("duckdb", "")
	defer duckdb.Close()

	var subject string
	query := "SELECT subject FROM read_parquet('" + filepath.Join(analyticsDir, "messages", "**", "*.parquet") + "') WHERE id = 1"
	if err := duckdb.QueryRow(query).Scan(&subject); err != nil {
		t.Fatalf("read unicode subject: %v", err)
	}

	if subject != "Test émoji 🎉 and unicode" {
		t.Errorf("unicode not preserved: got %q", subject)
	}
}

// TestBuildCache_EmptyDatabase tests handling of empty database.
func TestBuildCache_EmptyDatabase(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "msgvault-empty-db-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "empty.db")
	analyticsDir := filepath.Join(tmpDir, "analytics")

	// Create empty database with schema
	db, _ := sql.Open("sqlite3", dbPath)
	_, _ = db.Exec(`
		CREATE TABLE sources (id INTEGER PRIMARY KEY, identifier TEXT);
		CREATE TABLE messages (id INTEGER PRIMARY KEY, source_id INTEGER, source_message_id TEXT, sent_at TIMESTAMP, size_estimate INTEGER, has_attachments BOOLEAN, subject TEXT, snippet TEXT, conversation_id INTEGER, deleted_from_source_at TIMESTAMP);
		CREATE TABLE participants (id INTEGER PRIMARY KEY, email_address TEXT, domain TEXT, display_name TEXT);
		CREATE TABLE message_recipients (message_id INTEGER, participant_id INTEGER, recipient_type TEXT, display_name TEXT);
		CREATE TABLE labels (id INTEGER PRIMARY KEY, name TEXT);
		CREATE TABLE message_labels (message_id INTEGER, label_id INTEGER);
		CREATE TABLE attachments (message_id INTEGER, size INTEGER, filename TEXT);
	`)
	db.Close()

	result, err := buildCache(dbPath, analyticsDir, false)
	if err != nil {
		t.Fatalf("buildCache on empty db: %v", err)
	}

	// Should be skipped (no messages)
	if !result.Skipped {
		t.Error("expected empty database export to be skipped")
	}
}

// BenchmarkBuildCache benchmarks the export performance.
func BenchmarkBuildCache(b *testing.B) {
	// Create a larger test dataset
	tmpDir, err := os.MkdirTemp("", "msgvault-bench-*")
	if err != nil {
		b.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "bench.db")
	analyticsDir := filepath.Join(tmpDir, "analytics")

	db, _ := sql.Open("sqlite3", dbPath)

	// Create schema
	_, _ = db.Exec(`
		CREATE TABLE sources (id INTEGER PRIMARY KEY, identifier TEXT);
		CREATE TABLE messages (id INTEGER PRIMARY KEY, source_id INTEGER, source_message_id TEXT, sent_at TIMESTAMP, size_estimate INTEGER, has_attachments BOOLEAN, subject TEXT, snippet TEXT, conversation_id INTEGER, deleted_from_source_at TIMESTAMP);
		CREATE TABLE participants (id INTEGER PRIMARY KEY, email_address TEXT UNIQUE, domain TEXT, display_name TEXT);
		CREATE TABLE message_recipients (message_id INTEGER, participant_id INTEGER, recipient_type TEXT, display_name TEXT);
		CREATE TABLE labels (id INTEGER PRIMARY KEY, name TEXT);
		CREATE TABLE message_labels (message_id INTEGER, label_id INTEGER);
		CREATE TABLE attachments (message_id INTEGER, size INTEGER, filename TEXT);
		INSERT INTO sources VALUES (1, 'test@gmail.com');
		INSERT INTO labels VALUES (1, 'INBOX'), (2, 'Work');
	`)

	// Insert 1000 participants
	for i := 1; i <= 1000; i++ {
		_, _ = db.Exec("INSERT INTO participants VALUES (?, ?, ?, ?)",
			i, "user"+string(rune('0'+i%10))+"@domain"+string(rune('0'+i%5))+".com",
			"domain"+string(rune('0'+i%5))+".com", "User "+string(rune('0'+i%10)))
	}

	// Insert 10000 messages
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 1; i <= 10000; i++ {
		sentAt := baseTime.Add(time.Duration(i) * time.Hour)
		_, _ = db.Exec("INSERT INTO messages VALUES (?, 1, ?, ?, ?, 0, ?, ?, ?, NULL)",
			i, "msg"+string(rune('0'+i%10)), sentAt, 1000+i%5000,
			"Subject "+string(rune('0'+i%10)), "Snippet", i%100+1)

		// Add sender and recipient
		_, _ = db.Exec("INSERT INTO message_recipients VALUES (?, ?, 'from', NULL)", i, i%1000+1)
		_, _ = db.Exec("INSERT INTO message_recipients VALUES (?, ?, 'to', NULL)", i, (i+1)%1000+1)

		// Add labels
		_, _ = db.Exec("INSERT INTO message_labels VALUES (?, 1)", i)
		if i%3 == 0 {
			_, _ = db.Exec("INSERT INTO message_labels VALUES (?, 2)", i)
		}
	}
	db.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Clear analytics dir between runs
		os.RemoveAll(analyticsDir)
		if _, err := buildCache(dbPath, analyticsDir, true); err != nil {
			b.Fatalf("buildCache: %v", err)
		}
	}
}

// BenchmarkBuildCacheIncremental benchmarks incremental export performance.
func BenchmarkBuildCacheIncremental(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "msgvault-bench-incr-*")
	if err != nil {
		b.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "bench.db")
	analyticsDir := filepath.Join(tmpDir, "analytics")

	db, _ := sql.Open("sqlite3", dbPath)

	// Create schema and initial data (10000 messages)
	_, _ = db.Exec(`
		CREATE TABLE sources (id INTEGER PRIMARY KEY, identifier TEXT);
		CREATE TABLE messages (id INTEGER PRIMARY KEY, source_id INTEGER, source_message_id TEXT, sent_at TIMESTAMP, size_estimate INTEGER, has_attachments BOOLEAN, subject TEXT, snippet TEXT, conversation_id INTEGER, deleted_from_source_at TIMESTAMP);
		CREATE TABLE participants (id INTEGER PRIMARY KEY, email_address TEXT UNIQUE, domain TEXT, display_name TEXT);
		CREATE TABLE message_recipients (message_id INTEGER, participant_id INTEGER, recipient_type TEXT, display_name TEXT);
		CREATE TABLE labels (id INTEGER PRIMARY KEY, name TEXT);
		CREATE TABLE message_labels (message_id INTEGER, label_id INTEGER);
		CREATE TABLE attachments (message_id INTEGER, size INTEGER, filename TEXT);
		INSERT INTO sources VALUES (1, 'test@gmail.com');
		INSERT INTO labels VALUES (1, 'INBOX');
		INSERT INTO participants VALUES (1, 'alice@example.com', 'example.com', 'Alice');
		INSERT INTO participants VALUES (2, 'bob@example.com', 'example.com', 'Bob');
	`)

	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 1; i <= 10000; i++ {
		sentAt := baseTime.Add(time.Duration(i) * time.Hour)
		_, _ = db.Exec("INSERT INTO messages VALUES (?, 1, ?, ?, ?, 0, ?, ?, ?, NULL)",
			i, "msg"+string(rune('0'+i%10)), sentAt, 1000, "Subject", "Snippet", 1)
		_, _ = db.Exec("INSERT INTO message_recipients VALUES (?, 1, 'from', NULL)", i)
		_, _ = db.Exec("INSERT INTO message_recipients VALUES (?, 2, 'to', NULL)", i)
		_, _ = db.Exec("INSERT INTO message_labels VALUES (?, 1)", i)
	}

	// Initial export
	_, _ = buildCache(dbPath, analyticsDir, true)

	// Add 100 new messages for incremental test
	for i := 10001; i <= 10100; i++ {
		sentAt := baseTime.Add(time.Duration(i) * time.Hour)
		_, _ = db.Exec("INSERT INTO messages VALUES (?, 1, ?, ?, ?, 0, ?, ?, ?, NULL)",
			i, "msg"+string(rune('0'+i%10)), sentAt, 1000, "Subject", "Snippet", 1)
		_, _ = db.Exec("INSERT INTO message_recipients VALUES (?, 1, 'from', NULL)", i)
		_, _ = db.Exec("INSERT INTO message_recipients VALUES (?, 2, 'to', NULL)", i)
		_, _ = db.Exec("INSERT INTO message_labels VALUES (?, 1)", i)
	}
	db.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Reset sync state to re-trigger incremental export
		stateFile := filepath.Join(analyticsDir, "_last_sync.json")
		state := syncState{LastMessageID: 10000, LastSyncAt: time.Now()}
		data, _ := json.Marshal(state)
		_ = os.WriteFile(stateFile, data, 0644)

		if _, err := buildCache(dbPath, analyticsDir, false); err != nil {
			b.Fatalf("buildCache: %v", err)
		}
	}
}
