package query

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/wesm/msgvault/internal/search"
)

// testEnv encapsulates the DB, Engine, and Context setup for tests.
type testEnv struct {
	DB     *sql.DB
	Engine *SQLiteEngine
	Ctx    context.Context
	T      *testing.T
}

// newTestEnv creates a test environment with an in-memory SQLite database and test data.
func newTestEnv(t *testing.T) *testEnv {
	t.Helper()
	db := setupTestDB(t)
	t.Cleanup(func() { db.Close() })
	return &testEnv{
		DB:     db,
		Engine: NewSQLiteEngine(db),
		Ctx:    context.Background(),
		T:      t,
	}
}

// MustListMessages calls ListMessages and fails the test on error.
func (e *testEnv) MustListMessages(filter MessageFilter) []MessageSummary {
	e.T.Helper()
	messages, err := e.Engine.ListMessages(e.Ctx, filter)
	if err != nil {
		e.T.Fatalf("ListMessages: %v", err)
	}
	return messages
}

// MustSearch calls Search and fails the test on error.
func (e *testEnv) MustSearch(q *search.Query, limit, offset int) []MessageSummary {
	e.T.Helper()
	results, err := e.Engine.Search(e.Ctx, q, limit, offset)
	if err != nil {
		e.T.Fatalf("Search: %v", err)
	}
	return results
}

// MustGetTotalStats calls GetTotalStats and fails the test on error.
func (e *testEnv) MustGetTotalStats(opts StatsOptions) *TotalStats {
	e.T.Helper()
	stats, err := e.Engine.GetTotalStats(e.Ctx, opts)
	if err != nil {
		e.T.Fatalf("GetTotalStats: %v", err)
	}
	return stats
}

// MarkDeletedByID marks a message as deleted by its internal ID.
func (e *testEnv) MarkDeletedByID(id int64) {
	e.T.Helper()
	_, err := e.DB.Exec("UPDATE messages SET deleted_from_source_at = datetime('now') WHERE id = ?", id)
	if err != nil {
		e.T.Fatalf("mark deleted by id %d: %v", id, err)
	}
}

// MarkDeletedBySourceID marks a message as deleted by its source message ID.
func (e *testEnv) MarkDeletedBySourceID(sourceID string) {
	e.T.Helper()
	_, err := e.DB.Exec("UPDATE messages SET deleted_from_source_at = datetime('now') WHERE source_message_id = ?", sourceID)
	if err != nil {
		e.T.Fatalf("mark deleted by source id %s: %v", sourceID, err)
	}
}

// aggExpectation describes an expected key/count pair in aggregate results.
type aggExpectation struct {
	Key   string
	Count int64
}

// assertAggRows verifies that aggregate rows contain the expected key/count pairs
// in the exact order given. This ensures both correctness and default sort behavior.
func assertAggRows(t *testing.T, rows []AggregateRow, want []aggExpectation) {
	t.Helper()
	if len(rows) != len(want) {
		t.Errorf("expected %d aggregate rows, got %d", len(want), len(rows))
	}
	for i := range want {
		if i >= len(rows) {
			break
		}
		if rows[i].Key != want[i].Key {
			t.Errorf("row[%d]: expected key %q, got %q", i, want[i].Key, rows[i].Key)
		}
		if rows[i].Count != want[i].Count {
			t.Errorf("row[%d] (key %q): expected count %d, got %d", i, rows[i].Key, want[i].Count, rows[i].Count)
		}
	}
}

// EnableFTS creates the FTS5 virtual table and rebuilds it.
// Skips the test if FTS5 is not available in this SQLite build.
func (e *testEnv) EnableFTS() {
	e.T.Helper()
	_, err := e.DB.Exec(`
		CREATE VIRTUAL TABLE messages_fts USING fts5(message_id UNINDEXED, subject, body, from_addr, to_addr, cc_addr, tokenize='unicode61 remove_diacritics 1');
	`)
	if err != nil {
		e.T.Skipf("FTS5 not available in this SQLite build: %v", err)
	}

	// Populate FTS with test data (matching production UpsertFTS behavior)
	_, err = e.DB.Exec(`
		INSERT INTO messages_fts (rowid, message_id, subject, body, from_addr, to_addr, cc_addr)
		SELECT m.id, m.id, COALESCE(m.subject, ''), COALESCE(mb.body_text, ''),
			COALESCE((SELECT p.email_address FROM message_recipients mr JOIN participants p ON p.id = mr.participant_id WHERE mr.message_id = m.id AND mr.recipient_type = 'from' LIMIT 1), ''),
			COALESCE((SELECT GROUP_CONCAT(p.email_address, ' ') FROM message_recipients mr JOIN participants p ON p.id = mr.participant_id WHERE mr.message_id = m.id AND mr.recipient_type = 'to'), ''),
			COALESCE((SELECT GROUP_CONCAT(p.email_address, ' ') FROM message_recipients mr JOIN participants p ON p.id = mr.participant_id WHERE mr.message_id = m.id AND mr.recipient_type = 'cc'), '')
		FROM messages m
		LEFT JOIN message_bodies mb ON mb.message_id = m.id
	`)
	if err != nil {
		e.T.Fatalf("populate FTS: %v", err)
	}

	// Re-create engine to clear cached FTS state
	e.Engine = NewSQLiteEngine(e.DB)
}

// setupTestDB creates an in-memory SQLite database with test data.
func setupTestDB(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}

	// Create schema
	schema := `
		CREATE TABLE sources (
			id INTEGER PRIMARY KEY,
			source_type TEXT NOT NULL,
			identifier TEXT NOT NULL,
			display_name TEXT
		);

		CREATE TABLE participants (
			id INTEGER PRIMARY KEY,
			email_address TEXT,
			display_name TEXT,
			domain TEXT
		);

		CREATE TABLE conversations (
			id INTEGER PRIMARY KEY,
			source_id INTEGER NOT NULL,
			source_conversation_id TEXT,
			conversation_type TEXT NOT NULL,
			title TEXT
		);

		CREATE TABLE messages (
			id INTEGER PRIMARY KEY,
			conversation_id INTEGER NOT NULL,
			source_id INTEGER NOT NULL,
			source_message_id TEXT,
			message_type TEXT NOT NULL,
			sent_at DATETIME,
			received_at DATETIME,
			subject TEXT,
			snippet TEXT,
			size_estimate INTEGER,
			has_attachments BOOLEAN DEFAULT FALSE,
			attachment_count INTEGER DEFAULT 0,
			deleted_from_source_at DATETIME
		);

		CREATE TABLE message_bodies (
			message_id INTEGER PRIMARY KEY REFERENCES messages(id) ON DELETE CASCADE,
			body_text TEXT,
			body_html TEXT
		);

		CREATE TABLE message_recipients (
			id INTEGER PRIMARY KEY,
			message_id INTEGER NOT NULL,
			participant_id INTEGER NOT NULL,
			recipient_type TEXT NOT NULL,
			display_name TEXT
		);

		CREATE TABLE message_labels (
			message_id INTEGER NOT NULL,
			label_id INTEGER NOT NULL,
			PRIMARY KEY (message_id, label_id)
		);

		CREATE TABLE labels (
			id INTEGER PRIMARY KEY,
			source_id INTEGER,
			source_label_id TEXT,
			name TEXT NOT NULL,
			label_type TEXT
		);

		CREATE TABLE attachments (
			id INTEGER PRIMARY KEY,
			message_id INTEGER NOT NULL,
			filename TEXT,
			mime_type TEXT,
			size INTEGER,
			content_hash TEXT,
			storage_path TEXT
		);

		CREATE TABLE message_raw (
			message_id INTEGER PRIMARY KEY,
			raw_data BLOB NOT NULL,
			raw_format TEXT NOT NULL,
			compression TEXT
		);
	`

	if _, err := db.Exec(schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}

	// Insert test data
	testData := `
		-- Source account
		INSERT INTO sources (id, source_type, identifier, display_name) VALUES
			(1, 'gmail', 'test@gmail.com', 'Test Account');

		-- Participants
		INSERT INTO participants (id, email_address, display_name, domain) VALUES
			(1, 'alice@example.com', 'Alice Smith', 'example.com'),
			(2, 'bob@company.org', 'Bob Jones', 'company.org'),
			(3, 'carol@example.com', 'Carol White', 'example.com');

		-- Conversation
		INSERT INTO conversations (id, source_id, source_conversation_id, conversation_type, title) VALUES
			(1, 1, 'thread1', 'email_thread', 'Test Thread');

		-- Messages (3 from Alice, 2 from Bob)
		INSERT INTO messages (id, conversation_id, source_id, source_message_id, message_type, sent_at, subject, snippet, size_estimate, has_attachments, attachment_count) VALUES
			(1, 1, 1, 'msg1', 'email', '2024-01-15 10:00:00', 'Hello World', 'Preview 1', 1000, 0, 0),
			(2, 1, 1, 'msg2', 'email', '2024-01-16 11:00:00', 'Re: Hello', 'Preview 2', 2000, 1, 2),
			(3, 1, 1, 'msg3', 'email', '2024-02-01 09:00:00', 'Follow up', 'Preview 3', 1500, 0, 0),
			(4, 1, 1, 'msg4', 'email', '2024-02-15 14:00:00', 'Question', 'Preview 4', 3000, 1, 1),
			(5, 1, 1, 'msg5', 'email', '2024-03-01 16:00:00', 'Final', 'Preview 5', 500, 0, 0);

		-- Message bodies
		INSERT INTO message_bodies (message_id, body_text) VALUES
			(1, 'Message body 1'),
			(2, 'Message body 2'),
			(3, 'Message body 3'),
			(4, 'Message body 4'),
			(5, 'Message body 5');

		-- Message recipients (from)
		INSERT INTO message_recipients (message_id, participant_id, recipient_type, display_name) VALUES
			(1, 1, 'from', 'Alice'),
			(2, 1, 'from', 'Alice'),
			(3, 1, 'from', 'Alice'),
			(4, 2, 'from', 'Bob'),
			(5, 2, 'from', 'Bob');

		-- Message recipients (to)
		INSERT INTO message_recipients (message_id, participant_id, recipient_type, display_name) VALUES
			(1, 2, 'to', 'Bob'),
			(1, 3, 'to', 'Carol'),
			(2, 2, 'to', 'Bob'),
			(3, 2, 'to', 'Bob'),
			(4, 1, 'to', 'Alice'),
			(5, 1, 'to', 'Alice');

		-- Labels
		INSERT INTO labels (id, source_id, source_label_id, name, label_type) VALUES
			(1, 1, 'INBOX', 'INBOX', 'system'),
			(2, 1, 'IMPORTANT', 'IMPORTANT', 'system'),
			(3, 1, 'work', 'Work', 'user');

		-- Message labels
		INSERT INTO message_labels (message_id, label_id) VALUES
			(1, 1), (1, 3),
			(2, 1), (2, 2),
			(3, 1),
			(4, 1), (4, 3),
			(5, 1);

		-- Attachments
		INSERT INTO attachments (id, message_id, filename, mime_type, size, content_hash, storage_path) VALUES
			(1, 2, 'doc.pdf', 'application/pdf', 10000, 'hash1', 'ab/hash1'),
			(2, 2, 'image.png', 'image/png', 5000, 'hash2', 'cd/hash2'),
			(3, 4, 'report.xlsx', 'application/xlsx', 20000, 'hash3', 'ef/hash3');
	`

	if _, err := db.Exec(testData); err != nil {
		t.Fatalf("insert test data: %v", err)
	}

	return db
}

func TestAggregateBySender(t *testing.T) {
	env := newTestEnv(t)

	rows, err := env.Engine.AggregateBySender(env.Ctx, DefaultAggregateOptions())
	if err != nil {
		t.Fatalf("AggregateBySender: %v", err)
	}

	// Alice should have more messages (3) and be first with default sort (count desc)
	assertAggRows(t, rows, []aggExpectation{
		{"alice@example.com", 3},
		{"bob@company.org", 2},
	})
}

func TestAggregateBySenderName(t *testing.T) {
	env := newTestEnv(t)

	rows, err := env.Engine.AggregateBySenderName(env.Ctx, DefaultAggregateOptions())
	if err != nil {
		t.Fatalf("AggregateBySenderName: %v", err)
	}

	// Test data has participants: Alice Smith (3 msgs), Bob Jones (2 msgs)
	// COALESCE(display_name, email_address) -> "Alice Smith", "Bob Jones"
	assertAggRows(t, rows, []aggExpectation{
		{"Alice Smith", 3},
		{"Bob Jones", 2},
	})
}

func TestAggregateBySenderName_FallbackToEmail(t *testing.T) {
	env := newTestEnv(t)

	// Add a participant with no display_name
	_, err := env.DB.Exec(`
		INSERT INTO participants (id, email_address, display_name, domain) VALUES
			(10, 'noname@test.com', NULL, 'test.com');
		INSERT INTO messages (id, conversation_id, source_id, source_message_id, message_type, sent_at, subject, snippet, size_estimate, has_attachments) VALUES
			(10, 1, 1, 'msg10', 'email', '2024-05-01 10:00:00', 'No Name Test', 'Test', 100, 0);
		INSERT INTO message_recipients (message_id, participant_id, recipient_type) VALUES
			(10, 10, 'from');
	`)
	if err != nil {
		t.Fatalf("insert test data: %v", err)
	}

	rows, err := env.Engine.AggregateBySenderName(env.Ctx, DefaultAggregateOptions())
	if err != nil {
		t.Fatalf("AggregateBySenderName: %v", err)
	}

	// Should have 3 rows: Alice Smith (3), Bob Jones (2), noname@test.com (1)
	if len(rows) != 3 {
		t.Errorf("expected 3 sender names, got %d", len(rows))
	}

	// Check that the NULL display_name falls back to email
	var found bool
	for _, r := range rows {
		if r.Key == "noname@test.com" {
			found = true
			if r.Count != 1 {
				t.Errorf("expected noname@test.com count 1, got %d", r.Count)
			}
		}
	}
	if !found {
		t.Error("expected noname@test.com in results (display_name fallback)")
	}
}

func TestAggregateBySenderName_EmptyStringFallback(t *testing.T) {
	env := newTestEnv(t)

	// Add participants with empty-string and whitespace-only display_name
	_, err := env.DB.Exec(`
		INSERT INTO participants (id, email_address, display_name, domain) VALUES
			(11, 'empty@test.com', '', 'test.com'),
			(12, 'spaces@test.com', '   ', 'test.com');
		INSERT INTO messages (id, conversation_id, source_id, source_message_id, message_type, sent_at, subject, snippet, size_estimate, has_attachments) VALUES
			(11, 1, 1, 'msg11', 'email', '2024-05-01 10:00:00', 'Empty Name', 'Test', 100, 0),
			(12, 1, 1, 'msg12', 'email', '2024-05-02 10:00:00', 'Spaces Name', 'Test', 100, 0);
		INSERT INTO message_recipients (message_id, participant_id, recipient_type) VALUES
			(11, 11, 'from'),
			(12, 12, 'from');
	`)
	if err != nil {
		t.Fatalf("insert test data: %v", err)
	}

	rows, err := env.Engine.AggregateBySenderName(env.Ctx, DefaultAggregateOptions())
	if err != nil {
		t.Fatalf("AggregateBySenderName: %v", err)
	}

	// Empty and whitespace display_name should fall back to email.
	// Expected: Alice Smith (3), Bob Jones (2), empty@test.com (1), spaces@test.com (1)
	if len(rows) != 4 {
		t.Errorf("expected 4 sender names, got %d", len(rows))
		for _, r := range rows {
			t.Logf("  key=%q count=%d", r.Key, r.Count)
		}
	}

	// Verify empty-string display_name falls back to email
	foundEmpty := false
	foundSpaces := false
	for _, r := range rows {
		if r.Key == "empty@test.com" {
			foundEmpty = true
		}
		if r.Key == "spaces@test.com" {
			foundSpaces = true
		}
		// Should NOT appear as "" or "   "
		if r.Key == "" || r.Key == "   " {
			t.Errorf("unexpected empty/whitespace key: %q", r.Key)
		}
	}
	if !foundEmpty {
		t.Error("expected empty@test.com in results (empty-string display_name fallback)")
	}
	if !foundSpaces {
		t.Error("expected spaces@test.com in results (whitespace display_name fallback)")
	}
}

func TestListMessages_SenderNameFilter(t *testing.T) {
	env := newTestEnv(t)

	// Filter by sender display name "Alice Smith"
	filter := MessageFilter{SenderName: "Alice Smith"}
	messages := env.MustListMessages(filter)

	// Alice Smith sent messages 1, 2, 3
	if len(messages) != 3 {
		t.Errorf("expected 3 messages from Alice Smith, got %d", len(messages))
	}
}

func TestListMessages_MatchEmptySenderName(t *testing.T) {
	env := newTestEnvWithEmptyBuckets(t)

	// Filter for messages with empty sender name
	filter := MessageFilter{MatchEmptySenderName: true}
	messages := env.MustListMessages(filter)

	// msg6 has no sender at all -> COALESCE(display_name, email_address) is NULL
	if len(messages) != 1 {
		t.Errorf("expected 1 message with empty sender name, got %d", len(messages))
	}
	if len(messages) > 0 && messages[0].Subject != "No Sender" {
		t.Errorf("expected 'No Sender', got %q", messages[0].Subject)
	}
}

func TestSubAggregateBySenderName(t *testing.T) {
	env := newTestEnv(t)

	// Filter by recipient alice@example.com, sub-aggregate by sender name
	filter := MessageFilter{Recipient: "alice@example.com"}
	results, err := env.Engine.SubAggregate(env.Ctx, filter, ViewSenderNames, DefaultAggregateOptions())
	if err != nil {
		t.Fatalf("SubAggregate: %v", err)
	}

	// Messages to alice are 4, 5 (from Bob Jones)
	if len(results) != 1 {
		t.Errorf("expected 1 sender name to alice, got %d", len(results))
	}
	if len(results) > 0 && results[0].Key != "Bob Jones" {
		t.Errorf("expected 'Bob Jones', got %q", results[0].Key)
	}
}

func TestSubAggregate_MatchEmptySenderName(t *testing.T) {
	env := newTestEnvWithEmptyBuckets(t)

	// MatchEmptySenderName: match messages with no resolved sender name.
	// msg6 has no 'from' recipient at all.
	filter := MessageFilter{MatchEmptySenderName: true}
	results, err := env.Engine.SubAggregate(env.Ctx, filter, ViewLabels, DefaultAggregateOptions())
	if err != nil {
		t.Fatalf("SubAggregate with MatchEmptySenderName: %v", err)
	}

	// msg6 has no labels assigned to it in the empty-buckets fixture,
	// so we expect 0 label sub-aggregates for the empty-sender-name bucket.
	if len(results) != 0 {
		t.Errorf("expected 0 label sub-aggregates for empty sender name, got %d", len(results))
		for _, r := range results {
			t.Logf("  key=%q count=%d", r.Key, r.Count)
		}
	}
}

func TestMatchEmptySenderName_MixedFromRecipients(t *testing.T) {
	env := newTestEnv(t)

	// Add a message with two 'from' recipients: one with a valid name, one with NULL email.
	// MatchEmptySenderName should NOT match this because at least one from has a valid name.
	_, err := env.DB.Exec(`
		INSERT INTO participants (id, email_address, display_name, domain) VALUES
			(30, NULL, NULL, NULL);
		INSERT INTO messages (id, conversation_id, source_id, source_message_id, message_type, sent_at, subject, snippet, size_estimate, has_attachments) VALUES
			(30, 1, 1, 'msg30', 'email', '2024-06-01 10:00:00', 'Mixed From', 'test', 100, 0);
		INSERT INTO message_recipients (message_id, participant_id, recipient_type) VALUES
			(30, 1, 'from'),
			(30, 30, 'from');
	`)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	// MatchEmptySenderName should NOT match msg30 because participant 1 (Alice Smith)
	// has a valid COALESCE(NULLIF(TRIM(display_name), ''), email_address).
	filter := MessageFilter{MatchEmptySenderName: true}
	messages := env.MustListMessages(filter)

	for _, m := range messages {
		if m.Subject == "Mixed From" {
			t.Error("MatchEmptySenderName should not match message with at least one valid from sender")
		}
	}
}

func TestMatchEmptySenderName_CombinedWithDomain(t *testing.T) {
	env := newTestEnvWithEmptyBuckets(t)

	// Combining MatchEmptySenderName with Domain should not cause SQL errors.
	// This tests that domain filters correctly add their own join even when
	// MatchEmptySenderName uses NOT EXISTS (no shared join).
	filter := MessageFilter{
		MatchEmptySenderName: true,
		Domain:               "example.com",
	}
	messages := env.MustListMessages(filter)

	// No message can have both no sender name AND domain=example.com, so expect 0.
	if len(messages) != 0 {
		t.Errorf("expected 0 messages for MatchEmptySenderName+Domain, got %d", len(messages))
	}
}

func TestListMessages_MatchEmptySenderName_NotExists(t *testing.T) {
	env := newTestEnv(t)

	// Add a message with no 'from' recipient at all
	_, err := env.DB.Exec(`
		INSERT INTO messages (id, conversation_id, source_id, source_message_id, message_type, sent_at, subject, snippet, size_estimate, has_attachments) VALUES
			(20, 1, 1, 'msg20', 'email', '2024-06-01 10:00:00', 'Ghost Message', 'no sender', 100, 0);
	`)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	// MatchEmptySenderName should find msg20 (no from) but NOT regular messages
	filter := MessageFilter{MatchEmptySenderName: true}
	messages := env.MustListMessages(filter)

	if len(messages) != 1 {
		t.Errorf("expected 1 message with empty sender name, got %d", len(messages))
	}
	if len(messages) > 0 && messages[0].Subject != "Ghost Message" {
		t.Errorf("expected 'Ghost Message', got %q", messages[0].Subject)
	}

	// Verify normal messages with valid senders are NOT matched
	for _, m := range messages {
		if m.Subject == "Hello World" || m.Subject == "Re: Hello" {
			t.Errorf("should not match message with valid sender: %q", m.Subject)
		}
	}
}

func TestGetGmailIDsByFilter_SenderName(t *testing.T) {
	env := newTestEnv(t)

	filter := MessageFilter{SenderName: "Alice Smith"}
	ids, err := env.Engine.GetGmailIDsByFilter(env.Ctx, filter)
	if err != nil {
		t.Fatalf("GetGmailIDsByFilter: %v", err)
	}

	// Alice Smith sent messages msg1, msg2, msg3
	if len(ids) != 3 {
		t.Errorf("expected 3 gmail IDs for Alice Smith, got %d", len(ids))
	}
}

func TestAggregateByRecipient(t *testing.T) {
	env := newTestEnv(t)

	rows, err := env.Engine.AggregateByRecipient(env.Ctx, DefaultAggregateOptions())
	if err != nil {
		t.Fatalf("AggregateByRecipient: %v", err)
	}

	assertAggRows(t, rows, []aggExpectation{
		{"bob@company.org", 3},
		{"alice@example.com", 2},
		{"carol@example.com", 1},
	})
}

func TestAggregateByDomain(t *testing.T) {
	env := newTestEnv(t)

	rows, err := env.Engine.AggregateByDomain(env.Ctx, DefaultAggregateOptions())
	if err != nil {
		t.Fatalf("AggregateByDomain: %v", err)
	}

	assertAggRows(t, rows, []aggExpectation{
		{"example.com", 3},
		{"company.org", 2},
	})
}

func TestAggregateByLabel(t *testing.T) {
	env := newTestEnv(t)

	rows, err := env.Engine.AggregateByLabel(env.Ctx, DefaultAggregateOptions())
	if err != nil {
		t.Fatalf("AggregateByLabel: %v", err)
	}

	assertAggRows(t, rows, []aggExpectation{
		{"INBOX", 5},
		{"Work", 2},
		{"IMPORTANT", 1},
	})
}

func TestAggregateByTime(t *testing.T) {
	env := newTestEnv(t)

	// Test monthly granularity
	opts := DefaultAggregateOptions()
	opts.TimeGranularity = TimeMonth

	rows, err := env.Engine.AggregateByTime(env.Ctx, opts)
	if err != nil {
		t.Fatalf("AggregateByTime: %v", err)
	}

	if len(rows) != 3 {
		t.Errorf("expected 3 months, got %d", len(rows))
	}

	// Check we have the right months (sorted by count desc by default)
	months := make(map[string]int64)
	for _, row := range rows {
		months[row.Key] = row.Count
	}

	if months["2024-01"] != 2 {
		t.Errorf("expected 2024-01 count 2, got %d", months["2024-01"])
	}
	if months["2024-02"] != 2 {
		t.Errorf("expected 2024-02 count 2, got %d", months["2024-02"])
	}
	if months["2024-03"] != 1 {
		t.Errorf("expected 2024-03 count 1, got %d", months["2024-03"])
	}
}

func TestAggregateWithDateFilter(t *testing.T) {
	env := newTestEnv(t)

	opts := DefaultAggregateOptions()
	after := time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)
	opts.After = &after

	rows, err := env.Engine.AggregateBySender(env.Ctx, opts)
	if err != nil {
		t.Fatalf("AggregateBySender with date filter: %v", err)
	}

	// Only messages from Feb and Mar: 1 from Alice, 2 from Bob
	if len(rows) != 2 {
		t.Errorf("expected 2 senders after filter, got %d", len(rows))
	}

	// Bob should be first now (2 vs 1)
	if rows[0].Key != "bob@company.org" {
		t.Errorf("expected bob first after filter, got %s", rows[0].Key)
	}
}

func TestListMessages(t *testing.T) {
	env := newTestEnv(t)

	// List all messages
	messages := env.MustListMessages(MessageFilter{})
	if len(messages) != 5 {
		t.Errorf("expected 5 messages, got %d", len(messages))
	}

	// Filter by sender
	messages = env.MustListMessages(MessageFilter{Sender: "alice@example.com"})
	if len(messages) != 3 {
		t.Errorf("expected 3 messages from alice, got %d", len(messages))
	}

	// Filter by label
	messages = env.MustListMessages(MessageFilter{Label: "Work"})
	if len(messages) != 2 {
		t.Errorf("expected 2 messages with Work label, got %d", len(messages))
	}
}

func TestListMessagesWithLabels(t *testing.T) {
	env := newTestEnv(t)

	messages := env.MustListMessages(MessageFilter{})

	// Message 1 should have INBOX and Work labels
	msg1 := messages[len(messages)-1] // Oldest first with default sort
	if len(msg1.Labels) != 2 {
		t.Errorf("expected 2 labels on msg1, got %d", len(msg1.Labels))
	}
}

func TestGetMessage(t *testing.T) {
	env := newTestEnv(t)

	msg, err := env.Engine.GetMessage(env.Ctx, 1)
	if err != nil {
		t.Fatalf("GetMessage: %v", err)
	}

	if msg == nil {
		t.Fatal("expected message, got nil")
	}

	if msg.Subject != "Hello World" {
		t.Errorf("expected subject 'Hello World', got %q", msg.Subject)
	}

	if len(msg.From) != 1 || msg.From[0].Email != "alice@example.com" {
		t.Errorf("expected from alice, got %v", msg.From)
	}

	if len(msg.To) != 2 {
		t.Errorf("expected 2 recipients, got %d", len(msg.To))
	}

	if len(msg.Labels) != 2 {
		t.Errorf("expected 2 labels, got %d", len(msg.Labels))
	}

	// Verify body text is fetched from message_bodies table
	if msg.BodyText != "Message body 1" {
		t.Errorf("expected body text 'Message body 1', got %q", msg.BodyText)
	}
}

func TestGetMessageWithAttachments(t *testing.T) {
	env := newTestEnv(t)

	msg, err := env.Engine.GetMessage(env.Ctx, 2)
	if err != nil {
		t.Fatalf("GetMessage: %v", err)
	}

	if len(msg.Attachments) != 2 {
		t.Errorf("expected 2 attachments, got %d", len(msg.Attachments))
	}

	// Check attachment details
	found := false
	for _, att := range msg.Attachments {
		if att.Filename == "doc.pdf" {
			found = true
			if att.MimeType != "application/pdf" {
				t.Errorf("expected mime type application/pdf, got %s", att.MimeType)
			}
			if att.Size != 10000 {
				t.Errorf("expected size 10000, got %d", att.Size)
			}
		}
	}
	if !found {
		t.Error("expected to find doc.pdf attachment")
	}
}

func TestGetMessageBySourceID(t *testing.T) {
	env := newTestEnv(t)

	msg, err := env.Engine.GetMessageBySourceID(env.Ctx, "msg3")
	if err != nil {
		t.Fatalf("GetMessageBySourceID: %v", err)
	}

	if msg == nil {
		t.Fatal("expected message, got nil")
	}

	if msg.Subject != "Follow up" {
		t.Errorf("expected subject 'Follow up', got %q", msg.Subject)
	}
}

func TestListAccounts(t *testing.T) {
	env := newTestEnv(t)

	accounts, err := env.Engine.ListAccounts(env.Ctx)
	if err != nil {
		t.Fatalf("ListAccounts: %v", err)
	}

	if len(accounts) != 1 {
		t.Errorf("expected 1 account, got %d", len(accounts))
	}

	if accounts[0].Identifier != "test@gmail.com" {
		t.Errorf("expected test@gmail.com, got %s", accounts[0].Identifier)
	}
}

func TestGetTotalStats(t *testing.T) {
	env := newTestEnv(t)

	stats := env.MustGetTotalStats(StatsOptions{})

	if stats.MessageCount != 5 {
		t.Errorf("expected 5 messages, got %d", stats.MessageCount)
	}

	if stats.AttachmentCount != 3 {
		t.Errorf("expected 3 attachments, got %d", stats.AttachmentCount)
	}

	expectedSize := int64(1000 + 2000 + 1500 + 3000 + 500)
	if stats.TotalSize != expectedSize {
		t.Errorf("expected total size %d, got %d", expectedSize, stats.TotalSize)
	}

	expectedAttSize := int64(10000 + 5000 + 20000)
	if stats.AttachmentSize != expectedAttSize {
		t.Errorf("expected attachment size %d, got %d", expectedAttSize, stats.AttachmentSize)
	}
}

func TestDeletedMessagesIncludedWithFlag(t *testing.T) {
	env := newTestEnv(t)

	env.MarkDeletedByID(1)

	// Aggregates should INCLUDE deleted messages (for TUI visibility)
	rows, err := env.Engine.AggregateBySender(env.Ctx, DefaultAggregateOptions())
	if err != nil {
		t.Fatalf("AggregateBySender: %v", err)
	}

	// Alice should still have 3 (deleted messages are included)
	for _, row := range rows {
		if row.Key == "alice@example.com" && row.Count != 3 {
			t.Errorf("expected alice count 3 (including deleted), got %d", row.Count)
		}
	}

	// ListMessages should INCLUDE deleted with DeletedAt field set
	messages := env.MustListMessages(MessageFilter{})

	if len(messages) != 5 {
		t.Errorf("expected 5 messages (including deleted), got %d", len(messages))
	}

	// Find the deleted message and check DeletedAt is set
	var foundDeleted bool
	for _, msg := range messages {
		if msg.ID == 1 {
			if msg.DeletedAt == nil {
				t.Error("expected DeletedAt to be set for deleted message")
			}
			foundDeleted = true
		} else {
			if msg.DeletedAt != nil {
				t.Errorf("expected DeletedAt to be nil for non-deleted message %d", msg.ID)
			}
		}
	}
	if !foundDeleted {
		t.Error("deleted message not found in results")
	}

	// Stats should INCLUDE deleted messages
	stats := env.MustGetTotalStats(StatsOptions{})

	if stats.MessageCount != 5 {
		t.Errorf("expected 5 messages in stats (including deleted), got %d", stats.MessageCount)
	}
}

func TestSortingOptions(t *testing.T) {
	env := newTestEnv(t)

	// Sort by size descending
	opts := DefaultAggregateOptions()
	opts.SortField = SortBySize

	rows, err := env.Engine.AggregateBySender(env.Ctx, opts)
	if err != nil {
		t.Fatalf("AggregateBySender: %v", err)
	}

	// Bob has larger total size (3000 + 500 = 3500) vs Alice (1000 + 2000 + 1500 = 4500)
	// Wait, Alice should have more. Let me check.
	// Actually Alice: 1000 + 2000 + 1500 = 4500
	// Bob: 3000 + 500 = 3500
	// So Alice should be first when sorted by size desc
	if rows[0].Key != "alice@example.com" {
		t.Errorf("expected alice first by size, got %s", rows[0].Key)
	}

	// Sort ascending
	opts.SortDirection = SortAsc

	rows, err = env.Engine.AggregateBySender(env.Ctx, opts)
	if err != nil {
		t.Fatalf("AggregateBySender: %v", err)
	}

	// Bob should be first now
	if rows[0].Key != "bob@company.org" {
		t.Errorf("expected bob first by size asc, got %s", rows[0].Key)
	}
}

func TestGetMessageIncludesDeleted(t *testing.T) {
	env := newTestEnv(t)

	env.MarkDeletedByID(1)

	// GetMessage should RETURN deleted message (so user can still view it)
	msg, err := env.Engine.GetMessage(env.Ctx, 1)
	if err != nil {
		t.Fatalf("GetMessage: %v", err)
	}
	if msg == nil {
		t.Error("expected deleted message to be returned, got nil")
	}

	// GetMessage should still work for non-deleted message
	msg, err = env.Engine.GetMessage(env.Ctx, 2)
	if err != nil {
		t.Fatalf("GetMessage: %v", err)
	}
	if msg == nil {
		t.Error("expected message, got nil")
	}
}

func TestGetMessageBySourceIDIncludesDeleted(t *testing.T) {
	env := newTestEnv(t)

	env.MarkDeletedBySourceID("msg3")

	// GetMessageBySourceID should RETURN deleted message (so user can still view it)
	msg, err := env.Engine.GetMessageBySourceID(env.Ctx, "msg3")
	if err != nil {
		t.Fatalf("GetMessageBySourceID: %v", err)
	}
	if msg == nil {
		t.Error("expected deleted message to be returned, got nil")
	}

	// GetMessageBySourceID should still work for non-deleted message
	msg, err = env.Engine.GetMessageBySourceID(env.Ctx, "msg2")
	if err != nil {
		t.Fatalf("GetMessageBySourceID: %v", err)
	}
	if msg == nil {
		t.Error("expected message, got nil")
	}
}

func TestGetTotalStatsWithSourceID(t *testing.T) {
	env := newTestEnv(t)

	// Add a second source account with its own labels and messages
	_, err := env.DB.Exec(`
		INSERT INTO sources (id, source_type, identifier, display_name) VALUES
			(2, 'gmail', 'other@gmail.com', 'Other Account');

		INSERT INTO labels (id, source_id, source_label_id, name, label_type) VALUES
			(4, 2, 'INBOX', 'INBOX', 'system'),
			(5, 2, 'personal', 'Personal', 'user');

		INSERT INTO conversations (id, source_id, source_conversation_id, conversation_type, title) VALUES
			(2, 2, 'thread2', 'email_thread', 'Other Thread');

		INSERT INTO messages (id, conversation_id, source_id, source_message_id, message_type, sent_at, subject, size_estimate, has_attachments, attachment_count) VALUES
			(6, 2, 2, 'msg6', 'email', '2024-01-20 10:00:00', 'Other msg', 500, 0, 0);
	`)
	if err != nil {
		t.Fatalf("insert second account: %v", err)
	}

	// Stats for all accounts
	allStats := env.MustGetTotalStats(StatsOptions{})

	if allStats.MessageCount != 6 {
		t.Errorf("expected 6 total messages, got %d", allStats.MessageCount)
	}
	if allStats.LabelCount != 5 {
		t.Errorf("expected 5 total labels, got %d", allStats.LabelCount)
	}
	if allStats.AccountCount != 2 {
		t.Errorf("expected 2 accounts, got %d", allStats.AccountCount)
	}

	// Stats for source 1 only
	sourceID := int64(1)
	source1Stats := env.MustGetTotalStats(StatsOptions{SourceID: &sourceID})

	if source1Stats.MessageCount != 5 {
		t.Errorf("expected 5 messages for source 1, got %d", source1Stats.MessageCount)
	}
	if source1Stats.LabelCount != 3 {
		t.Errorf("expected 3 labels for source 1, got %d", source1Stats.LabelCount)
	}
	if source1Stats.AccountCount != 1 {
		t.Errorf("expected account count 1 when filtering by source, got %d", source1Stats.AccountCount)
	}
}

func TestGetTotalStatsWithInvalidSourceID(t *testing.T) {
	env := newTestEnv(t)

	// Request stats for a non-existent source ID
	nonExistentID := int64(9999)
	stats := env.MustGetTotalStats(StatsOptions{SourceID: &nonExistentID})

	// All counts should be 0 for non-existent source
	if stats.MessageCount != 0 {
		t.Errorf("expected 0 messages for non-existent source, got %d", stats.MessageCount)
	}
	if stats.LabelCount != 0 {
		t.Errorf("expected 0 labels for non-existent source, got %d", stats.LabelCount)
	}
	if stats.AccountCount != 0 {
		t.Errorf("expected 0 account count for non-existent source, got %d", stats.AccountCount)
	}
	if stats.AttachmentCount != 0 {
		t.Errorf("expected 0 attachments for non-existent source, got %d", stats.AttachmentCount)
	}
}

func TestWithAttachmentsOnlyAggregate(t *testing.T) {
	env := newTestEnv(t)

	// Without filter - should get all messages
	opts := DefaultAggregateOptions()
	allRows, err := env.Engine.AggregateBySender(env.Ctx, opts)
	if err != nil {
		t.Fatalf("AggregateBySender: %v", err)
	}

	// Alice has 3 messages total, Bob has 2 (bob@company.org per test data)
	var aliceAll, bobAll int64
	for _, row := range allRows {
		if row.Key == "alice@example.com" {
			aliceAll = row.Count
		}
		if row.Key == "bob@company.org" {
			bobAll = row.Count
		}
	}
	if aliceAll != 3 {
		t.Errorf("expected Alice count 3, got %d", aliceAll)
	}
	if bobAll != 2 {
		t.Errorf("expected Bob count 2, got %d", bobAll)
	}

	// With attachment filter - should only count messages with attachments
	opts.WithAttachmentsOnly = true
	attRows, err := env.Engine.AggregateBySender(env.Ctx, opts)
	if err != nil {
		t.Fatalf("AggregateBySender with attachment filter: %v", err)
	}

	// Check attachment counts per sender
	// Test data: msg2 (Alice) has attachments, msg4 (Bob) has attachments
	var aliceAtt, bobAtt int64
	for _, row := range attRows {
		if row.Key == "alice@example.com" {
			aliceAtt = row.Count
		}
		if row.Key == "bob@company.org" {
			bobAtt = row.Count
		}
	}

	// Alice has 1 message with attachments (msg2)
	if aliceAtt != 1 {
		t.Errorf("expected Alice attachment count 1, got %d", aliceAtt)
	}
	// Bob has 1 message with attachments (msg4)
	if bobAtt != 1 {
		t.Errorf("expected Bob attachment count 1, got %d", bobAtt)
	}
}

func TestWithAttachmentsOnlyStats(t *testing.T) {
	env := newTestEnv(t)

	// Stats without filter
	allStats := env.MustGetTotalStats(StatsOptions{})
	if allStats.MessageCount != 5 {
		t.Errorf("expected 5 total messages, got %d", allStats.MessageCount)
	}

	// Stats with attachment filter
	attStats := env.MustGetTotalStats(StatsOptions{WithAttachmentsOnly: true})

	// Only 2 messages have attachments in setupTestDB
	if attStats.MessageCount != 2 {
		t.Errorf("expected 2 messages with attachments, got %d", attStats.MessageCount)
	}

	// Attachment stats should still be non-zero (counting actual attachments)
	if attStats.AttachmentCount == 0 {
		t.Error("expected non-zero attachment count for messages with attachments")
	}
}

func TestListMessagesTimePeriodInference(t *testing.T) {
	env := newTestEnv(t)

	// Test with month period - should infer TimeMonth granularity
	filter := MessageFilter{
		TimePeriod: "2024-01",
		// TimeGranularity is zero (TimeYear) by default
	}

	messages := env.MustListMessages(filter)

	// Should get messages from January 2024 (msg1, msg2)
	if len(messages) != 2 {
		t.Errorf("expected 2 messages for 2024-01, got %d", len(messages))
	}

	// Test with day period
	messages = env.MustListMessages(MessageFilter{TimePeriod: "2024-01-15"})
	if len(messages) != 1 {
		t.Errorf("expected 1 message for 2024-01-15, got %d", len(messages))
	}

	// Test with year period - should match the explicit granularity
	messages = env.MustListMessages(MessageFilter{TimePeriod: "2024", TimeGranularity: TimeYear})
	if len(messages) != 5 {
		t.Errorf("expected 5 messages for 2024, got %d", len(messages))
	}
}

func TestSearch_WithoutFTS(t *testing.T) {
	// This test uses the standard test DB which doesn't have FTS
	// to verify the LIKE fallback works
	env := newTestEnv(t)

	// Search for text in subject/body using LIKE fallback
	q := &search.Query{
		TextTerms: []string{"Hello"},
	}

	results := env.MustSearch(q, 100, 0)

	// Should find messages with "Hello" in subject
	if len(results) != 2 {
		t.Errorf("expected 2 messages with 'Hello' in subject, got %d", len(results))
	}
}

func TestSearch_FromFilter(t *testing.T) {
	env := newTestEnv(t)

	q := &search.Query{
		FromAddrs: []string{"alice@example.com"},
	}

	results := env.MustSearch(q, 100, 0)

	// Alice sent 3 messages
	if len(results) != 3 {
		t.Errorf("expected 3 messages from alice, got %d", len(results))
	}

	for _, msg := range results {
		if msg.FromEmail != "alice@example.com" {
			t.Errorf("expected from alice@example.com, got %s", msg.FromEmail)
		}
	}
}

func TestSearch_LabelFilter(t *testing.T) {
	env := newTestEnv(t)

	q := &search.Query{
		Labels: []string{"Work"},
	}

	results := env.MustSearch(q, 100, 0)

	// 2 messages have the Work label (msg1 and msg4)
	if len(results) != 2 {
		t.Errorf("expected 2 messages with Work label, got %d", len(results))
	}
}

func TestSearch_DateRangeFilter(t *testing.T) {
	env := newTestEnv(t)

	after := time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)
	before := time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC)

	q := &search.Query{
		AfterDate:  &after,
		BeforeDate: &before,
	}

	results := env.MustSearch(q, 100, 0)

	// Messages in February 2024 (msg3 and msg4)
	if len(results) != 2 {
		t.Errorf("expected 2 messages in Feb 2024, got %d", len(results))
	}
}

func TestSearch_HasAttachment(t *testing.T) {
	env := newTestEnv(t)

	hasAtt := true
	q := &search.Query{
		HasAttachment: &hasAtt,
	}

	results := env.MustSearch(q, 100, 0)

	// 2 messages have attachments (msg2 and msg4)
	if len(results) != 2 {
		t.Errorf("expected 2 messages with attachments, got %d", len(results))
	}

	for _, msg := range results {
		if !msg.HasAttachments {
			t.Errorf("expected message %d to have attachments", msg.ID)
		}
	}
}

func TestSearch_CombinedFilters(t *testing.T) {
	env := newTestEnv(t)

	// Search for alice's messages with Work label
	q := &search.Query{
		FromAddrs: []string{"alice@example.com"},
		Labels:    []string{"Work"},
	}

	results := env.MustSearch(q, 100, 0)

	// Only msg1 is from Alice with Work label
	if len(results) != 1 {
		t.Errorf("expected 1 message from alice with Work label, got %d", len(results))
	}
}

func TestSearch_SizeFilter(t *testing.T) {
	env := newTestEnv(t)

	largerThan := int64(2500)
	q := &search.Query{
		LargerThan: &largerThan,
	}

	results := env.MustSearch(q, 100, 0)

	// Only msg4 has size 3000 which is > 2500
	if len(results) != 1 {
		t.Errorf("expected 1 message larger than 2500, got %d", len(results))
	}

	if results[0].SizeEstimate <= largerThan {
		t.Errorf("expected message size > %d, got %d", largerThan, results[0].SizeEstimate)
	}
}

// TestSearch_EmptyQuery verifies that searching with no filters returns all messages.
// This tests the fix for the empty WHERE clause bug (fixed with "1=1" fallback).
func TestSearch_EmptyQuery(t *testing.T) {
	env := newTestEnv(t)

	// Empty query - no text terms, no filters
	q := &search.Query{}

	results := env.MustSearch(q, 100, 0)

	// Should return all 5 messages from test data
	if len(results) != 5 {
		t.Errorf("expected 5 messages with empty query, got %d", len(results))
	}
}

func TestHasFTSTable(t *testing.T) {
	env := newTestEnv(t)

	// Our test DB doesn't have FTS
	if env.Engine.hasFTSTable(env.Ctx) {
		t.Error("expected hasFTSTable to return false for test DB without FTS")
	}

	// Try to create FTS table - skip if FTS5 not available in this SQLite build
	_, err := env.DB.Exec(`
		CREATE VIRTUAL TABLE messages_fts USING fts5(subject, snippet);
	`)
	if err != nil {
		t.Skipf("FTS5 not available in this SQLite build: %v", err)
	}

	// Create a new engine to get a fresh cache (old engine has cached "false")
	engine2 := NewSQLiteEngine(env.DB)

	// Now it should detect FTS
	if !engine2.hasFTSTable(env.Ctx) {
		t.Error("expected hasFTSTable to return true after creating FTS table")
	}
}

func TestHasFTSTable_ErrorDoesNotCache(t *testing.T) {
	env := newTestEnv(t)

	// Create FTS table so we can detect if error path was taken
	// (if FTS exists and canceled call returns false, error path was taken)
	_, err := env.DB.Exec(`
		CREATE VIRTUAL TABLE messages_fts USING fts5(subject, snippet);
	`)
	if err != nil {
		t.Skipf("FTS5 not available, cannot verify error-does-not-cache behavior: %v", err)
	}

	// Re-create engine after FTS table creation to get fresh (uncached) state
	env.Engine = NewSQLiteEngine(env.DB)

	// First call with canceled context should fail and return false without caching
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	firstResult := env.Engine.hasFTSTable(canceledCtx)

	// If FTS exists but canceled call returned true, the driver didn't respect
	// the canceled context - we can't test the error-retry behavior
	if firstResult {
		t.Skip("SQLite driver does not error on canceled context; cannot test error-retry behavior")
	}

	// The canceled call returned false even though FTS exists - error path was taken.
	// Now verify the retry with valid context succeeds (key test: errors don't cache)
	validCtx := context.Background()
	secondResult := env.Engine.hasFTSTable(validCtx)

	if !secondResult {
		t.Error("hasFTSTable retry returned false, but FTS is available; error was incorrectly cached")
	}

	// Verify it's now cached - third call should return same result
	thirdResult := env.Engine.hasFTSTable(validCtx)
	if !thirdResult {
		t.Error("hasFTSTable cached result is false, expected true")
	}
}

func TestSearch_WithFTS(t *testing.T) {
	env := newTestEnv(t)
	env.EnableFTS()

	// Search using FTS
	q := &search.Query{
		TextTerms: []string{"World"},
	}

	results := env.MustSearch(q, 100, 0)

	// Should find msg1 with "Hello World" in subject
	if len(results) != 1 {
		t.Errorf("expected 1 message with 'World', got %d", len(results))
	}

	if results[0].Subject != "Hello World" {
		t.Errorf("expected subject 'Hello World', got %s", results[0].Subject)
	}
}

// setupTestDBWithEmptyBuckets creates a test DB with messages that have
// empty senders, recipients, domains, and labels for testing MatchEmpty* filters.
func newTestEnvWithEmptyBuckets(t *testing.T) *testEnv {
	t.Helper()

	env := newTestEnv(t)

	// Add additional test data with empty values
	extraData := `
		-- Participant with empty domain
		INSERT INTO participants (id, email_address, display_name, domain) VALUES
			(4, 'nodomain@', 'No Domain User', '');

		-- Message with no sender (msg6)
		INSERT INTO messages (id, conversation_id, source_id, source_message_id, message_type, sent_at, subject, snippet, size_estimate, has_attachments) VALUES
			(6, 1, 1, 'msg6', 'email', '2024-04-01 10:00:00', 'No Sender', 'Message with no sender', 100, 0);
		-- No 'from' recipient for msg6

		-- Message with no recipients (msg7)
		INSERT INTO messages (id, conversation_id, source_id, source_message_id, message_type, sent_at, subject, snippet, size_estimate, has_attachments) VALUES
			(7, 1, 1, 'msg7', 'email', '2024-04-02 10:00:00', 'No Recipients', 'Message with no recipients', 100, 0);
		INSERT INTO message_recipients (message_id, participant_id, recipient_type, display_name) VALUES
			(7, 1, 'from', 'Alice');
		-- No 'to' recipients for msg7

		-- Message with empty domain sender (msg8)
		INSERT INTO messages (id, conversation_id, source_id, source_message_id, message_type, sent_at, subject, snippet, size_estimate, has_attachments) VALUES
			(8, 1, 1, 'msg8', 'email', '2024-04-03 10:00:00', 'Empty Domain', 'Message from empty domain', 100, 0);
		INSERT INTO message_recipients (message_id, participant_id, recipient_type, display_name) VALUES
			(8, 4, 'from', 'No Domain'),
			(8, 1, 'to', 'Alice');

		-- Message with no labels (msg9)
		INSERT INTO messages (id, conversation_id, source_id, source_message_id, message_type, sent_at, subject, snippet, size_estimate, has_attachments) VALUES
			(9, 1, 1, 'msg9', 'email', '2024-04-04 10:00:00', 'No Labels', 'Message with no labels', 100, 0);
		INSERT INTO message_recipients (message_id, participant_id, recipient_type, display_name) VALUES
			(9, 1, 'from', 'Alice'),
			(9, 2, 'to', 'Bob');
		-- No labels for msg9
	`

	if _, err := env.DB.Exec(extraData); err != nil {
		t.Fatalf("insert empty bucket test data: %v", err)
	}

	return env
}

func TestListMessages_MatchEmptySender(t *testing.T) {
	env := newTestEnvWithEmptyBuckets(t)

	// Filter for messages with empty/no sender
	filter := MessageFilter{
		MatchEmptySender: true,
	}

	messages := env.MustListMessages(filter)

	// Should only find msg6 (no sender)
	if len(messages) != 1 {
		t.Errorf("expected 1 message with empty sender, got %d", len(messages))
	}

	if len(messages) > 0 && messages[0].Subject != "No Sender" {
		t.Errorf("expected 'No Sender' message, got %q", messages[0].Subject)
	}
}

func TestListMessages_MatchEmptyRecipient(t *testing.T) {
	env := newTestEnvWithEmptyBuckets(t)

	// Filter for messages with no recipients
	filter := MessageFilter{
		MatchEmptyRecipient: true,
	}

	messages := env.MustListMessages(filter)

	// Should find msg6 (no recipients since no to/cc/bcc) and msg7 (explicitly no recipients)
	// msg6 has no sender AND no recipients, msg7 has a sender but no to/cc/bcc
	if len(messages) != 2 {
		t.Errorf("expected 2 messages with empty recipients, got %d", len(messages))
	}
}

func TestListMessages_MatchEmptyDomain(t *testing.T) {
	env := newTestEnvWithEmptyBuckets(t)

	// Filter for messages with empty domain
	filter := MessageFilter{
		MatchEmptyDomain: true,
	}

	messages := env.MustListMessages(filter)

	// Should find msg6 (no sender, so no domain) and msg8 (sender with empty domain)
	if len(messages) != 2 {
		t.Errorf("expected 2 messages with empty domain, got %d", len(messages))
	}
}

func TestListMessages_MatchEmptyLabel(t *testing.T) {
	env := newTestEnvWithEmptyBuckets(t)

	// Filter for messages with no labels
	filter := MessageFilter{
		MatchEmptyLabel: true,
	}

	messages := env.MustListMessages(filter)

	// Should find msg6, msg7, msg8, msg9 (the new messages have no labels)
	if len(messages) != 4 {
		t.Errorf("expected 4 messages with no labels, got %d", len(messages))
	}
}

func TestListMessages_MatchEmptyFiltersAreIndependent(t *testing.T) {
	env := newTestEnvWithEmptyBuckets(t)

	// Test data setup (from setupTestDBWithEmptyBuckets):
	// - msg9 ("No Labels"): sender=alice, recipient=bob, domain=example.com, NO labels
	// - msg6 ("No Sender"): NO sender, no recipients, no domain, no labels
	// - msg7 ("No Recipients"): sender=alice, NO recipients, domain=example.com, no labels

	// Test 1: Combined filter - MatchEmptyLabel AND specific Sender
	// Should find messages from alice that have no labels (msg7, msg9)
	messages := env.MustListMessages(MessageFilter{
		MatchEmptyLabel: true,
		Sender:          "alice@example.com",
	})

	// msg7 and msg9 are from alice and have no labels
	if len(messages) != 2 {
		t.Errorf("expected 2 messages with MatchEmptyLabel + alice sender, got %d", len(messages))
	}

	foundMsg9 := false
	foundMsg7 := false
	for _, msg := range messages {
		if msg.Subject == "No Labels" {
			foundMsg9 = true
		}
		if msg.Subject == "No Recipients" {
			foundMsg7 = true
		}
	}
	if !foundMsg9 {
		t.Error("expected 'No Labels' (msg9) with MatchEmptyLabel + alice sender")
	}
	if !foundMsg7 {
		t.Error("expected 'No Recipients' (msg7) with MatchEmptyLabel + alice sender")
	}

	// Test 2: Multiple MatchEmpty flags create restrictive AND condition
	// MatchEmptyLabel AND MatchEmptySender should find only messages with
	// no labels AND no sender (only msg6)
	messages = env.MustListMessages(MessageFilter{
		MatchEmptyLabel:  true,
		MatchEmptySender: true,
	})

	// Only msg6 has both no labels AND no sender
	if len(messages) != 1 {
		t.Errorf("expected 1 message with MatchEmptyLabel + MatchEmptySender, got %d", len(messages))
	}
	if len(messages) > 0 && messages[0].Subject != "No Sender" {
		t.Errorf("expected 'No Sender' message, got %q", messages[0].Subject)
	}

	// Test 3: MatchEmptyLabel alone should NOT restrict by sender
	// (proving filters are independent - setting one doesn't affect others)
	messages = env.MustListMessages(MessageFilter{MatchEmptyLabel: true})

	// Should find all 4 messages with no labels (msg6, msg7, msg8, msg9)
	if len(messages) != 4 {
		t.Errorf("expected 4 messages with no labels, got %d", len(messages))
	}
}

func TestSearch_CaseInsensitiveFallback(t *testing.T) {
	// Test that the LIKE fallback is case-insensitive (when FTS is unavailable)
	env := newTestEnv(t)

	// Verify FTS is NOT available - this test specifically covers the LIKE fallback path
	if env.Engine.hasFTSTable(env.Ctx) {
		t.Skip("FTS is available; this test covers the non-FTS fallback path")
	}

	// Search for "hello" (lowercase) - should match "Hello World" subject
	q := &search.Query{
		TextTerms: []string{"hello"},
	}

	results := env.MustSearch(q, 100, 0)

	// Should find messages with "Hello" in subject (case-insensitive)
	if len(results) != 2 {
		t.Errorf("expected 2 messages with 'hello' (case-insensitive), got %d", len(results))
	}

	// Search for "WORLD" (uppercase) - should match "Hello World"
	q = &search.Query{
		TextTerms: []string{"WORLD"},
	}

	results = env.MustSearch(q, 100, 0)

	if len(results) != 1 {
		t.Errorf("expected 1 message with 'WORLD' (case-insensitive), got %d", len(results))
	}

	if len(results) > 0 && results[0].Subject != "Hello World" {
		t.Errorf("expected 'Hello World', got %q", results[0].Subject)
	}
}

func TestSearch_SubjectTermsCaseInsensitive(t *testing.T) {
	// Test that subject search is case-insensitive (LIKE fallback path)
	env := newTestEnv(t)

	// Verify FTS is NOT available - subject terms use LIKE when FTS is unavailable
	if env.Engine.hasFTSTable(env.Ctx) {
		t.Skip("FTS is available; this test covers the non-FTS fallback path")
	}

	// Search subject for "HELLO" (uppercase) - should match "Hello World"
	q := &search.Query{
		SubjectTerms: []string{"HELLO"},
	}

	results := env.MustSearch(q, 100, 0)

	// Should find messages with "Hello" in subject
	if len(results) != 2 {
		t.Errorf("expected 2 messages with 'HELLO' in subject (case-insensitive), got %d", len(results))
	}
}

func TestSubAggregateBySender(t *testing.T) {
	env := newTestEnv(t)

	// Filter by recipient alice@example.com and sub-aggregate by sender
	// Messages 4 and 5 are sent to alice, both from bob@company.org
	filter := MessageFilter{
		Recipient: "alice@example.com",
	}

	results, err := env.Engine.SubAggregate(env.Ctx, filter, ViewSenders, DefaultAggregateOptions())
	if err != nil {
		t.Fatalf("SubAggregate: %v", err)
	}

	// bob@company.org sends to alice@example.com (2 messages)
	if len(results) != 1 {
		t.Errorf("expected 1 sender to alice@example.com, got %d", len(results))
	}

	if len(results) > 0 && results[0].Key != "bob@company.org" {
		t.Errorf("expected bob@company.org, got %s", results[0].Key)
	}

	if len(results) > 0 && results[0].Count != 2 {
		t.Errorf("expected count 2, got %d", results[0].Count)
	}
}

func TestSubAggregateByRecipient(t *testing.T) {
	env := newTestEnv(t)

	// Filter by sender alice@example.com and sub-aggregate by recipients
	// Alice sends messages 1, 2, 3 to bob@company.org (and msg 1 also to carol)
	filter := MessageFilter{
		Sender: "alice@example.com",
	}

	results, err := env.Engine.SubAggregate(env.Ctx, filter, ViewRecipients, DefaultAggregateOptions())
	if err != nil {
		t.Fatalf("SubAggregate: %v", err)
	}

	// alice sends to bob@company.org (3 messages) and carol@example.com (1 message)
	if len(results) != 2 {
		t.Errorf("expected 2 recipients for alice@example.com, got %d", len(results))
	}

	// bob@company.org should be in the results with count 3
	var foundBob bool
	for _, r := range results {
		if r.Key == "bob@company.org" {
			foundBob = true
			if r.Count != 3 {
				t.Errorf("expected bob@company.org count 3, got %d", r.Count)
			}
		}
	}
	if !foundBob {
		t.Error("expected bob@company.org in recipients for alice@example.com")
	}
}

func TestSubAggregateByLabel(t *testing.T) {
	env := newTestEnv(t)

	// Filter by sender alice@example.com and sub-aggregate by labels
	// Alice's messages have INBOX (all 3), IMPORTANT (1), Work (1)
	filter := MessageFilter{
		Sender: "alice@example.com",
	}

	results, err := env.Engine.SubAggregate(env.Ctx, filter, ViewLabels, DefaultAggregateOptions())
	if err != nil {
		t.Fatalf("SubAggregate: %v", err)
	}

	// alice's messages have INBOX, IMPORTANT, Work labels
	if len(results) != 3 {
		t.Errorf("expected 3 labels for alice@example.com's messages, got %d", len(results))
	}

	// INBOX should have count 3 (all of alice's messages)
	for _, r := range results {
		if r.Key == "INBOX" && r.Count != 3 {
			t.Errorf("expected INBOX count 3, got %d", r.Count)
		}
	}
}

func TestSubAggregateIncludesDeletedMessages(t *testing.T) {
	env := newTestEnv(t)

	// Filter by sender alice@example.com (messages 1, 2, 3)
	filter := MessageFilter{
		Sender: "alice@example.com",
	}

	resultsBefore, err := env.Engine.SubAggregate(env.Ctx, filter, ViewRecipients, DefaultAggregateOptions())
	if err != nil {
		t.Fatalf("SubAggregate before: %v", err)
	}

	// Mark message 1 (alice's message) as deleted
	env.MarkDeletedByID(1)

	resultsAfter, err := env.Engine.SubAggregate(env.Ctx, filter, ViewRecipients, DefaultAggregateOptions())
	if err != nil {
		t.Fatalf("SubAggregate after: %v", err)
	}

	// Total count should be THE SAME since deleted messages are included in aggregates
	var totalBefore, totalAfter int64
	for _, r := range resultsBefore {
		totalBefore += r.Count
	}
	for _, r := range resultsAfter {
		totalAfter += r.Count
	}

	if totalAfter != totalBefore {
		t.Errorf("expected same message count (deleted included), before=%d after=%d", totalBefore, totalAfter)
	}
}

func TestSubAggregateByTime(t *testing.T) {
	env := newTestEnv(t)

	// Filter by sender alice@example.com and sub-aggregate by time
	// Alice's messages: msg1 (2024-01), msg2 (2024-01), msg3 (2024-02)
	filter := MessageFilter{
		Sender: "alice@example.com",
	}

	opts := DefaultAggregateOptions()
	opts.TimeGranularity = TimeMonth

	results, err := env.Engine.SubAggregate(env.Ctx, filter, ViewTime, opts)
	if err != nil {
		t.Fatalf("SubAggregate: %v", err)
	}

	// Should have 2 time periods: 2024-01 (2 msgs) and 2024-02 (1 msg)
	if len(results) != 2 {
		t.Errorf("expected 2 time periods for alice@example.com's messages, got %d", len(results))
	}

	// Keys should be in YYYY-MM format
	for _, r := range results {
		if len(r.Key) != 7 || r.Key[4] != '-' {
			t.Errorf("expected YYYY-MM format, got %q", r.Key)
		}
	}
}

func TestMergeFilterIntoQuery_EmptyFilter(t *testing.T) {
	// Empty filter should return query unchanged
	q := &search.Query{
		TextTerms: []string{"test", "query"},
		FromAddrs: []string{"alice@example.com"},
		Labels:    []string{"inbox"},
	}
	filter := MessageFilter{}

	merged := MergeFilterIntoQuery(q, filter)

	if len(merged.TextTerms) != 2 || merged.TextTerms[0] != "test" || merged.TextTerms[1] != "query" {
		t.Errorf("TextTerms: got %v, want [test query]", merged.TextTerms)
	}
	if len(merged.FromAddrs) != 1 || merged.FromAddrs[0] != "alice@example.com" {
		t.Errorf("FromAddrs: got %v, want [alice@example.com]", merged.FromAddrs)
	}
	if len(merged.Labels) != 1 || merged.Labels[0] != "inbox" {
		t.Errorf("Labels: got %v, want [inbox]", merged.Labels)
	}
}

func TestMergeFilterIntoQuery_SourceID(t *testing.T) {
	q := &search.Query{}
	sourceID := int64(42)
	filter := MessageFilter{SourceID: &sourceID}

	merged := MergeFilterIntoQuery(q, filter)

	if merged.AccountID == nil || *merged.AccountID != 42 {
		t.Errorf("AccountID: got %v, want 42", merged.AccountID)
	}
}

func TestMergeFilterIntoQuery_SenderAppends(t *testing.T) {
	// Sender from filter should be appended, not replace existing from: filters
	q := &search.Query{
		FromAddrs: []string{"alice@example.com"},
	}
	filter := MessageFilter{Sender: "bob@example.com"}

	merged := MergeFilterIntoQuery(q, filter)

	if len(merged.FromAddrs) != 2 {
		t.Fatalf("FromAddrs: got %d items, want 2", len(merged.FromAddrs))
	}
	if merged.FromAddrs[0] != "alice@example.com" {
		t.Errorf("FromAddrs[0]: got %q, want alice@example.com", merged.FromAddrs[0])
	}
	if merged.FromAddrs[1] != "bob@example.com" {
		t.Errorf("FromAddrs[1]: got %q, want bob@example.com", merged.FromAddrs[1])
	}
}

func TestMergeFilterIntoQuery_RecipientAppends(t *testing.T) {
	q := &search.Query{
		ToAddrs: []string{"recipient1@example.com"},
	}
	filter := MessageFilter{Recipient: "recipient2@example.com"}

	merged := MergeFilterIntoQuery(q, filter)

	if len(merged.ToAddrs) != 2 {
		t.Fatalf("ToAddrs: got %d items, want 2", len(merged.ToAddrs))
	}
	if merged.ToAddrs[0] != "recipient1@example.com" || merged.ToAddrs[1] != "recipient2@example.com" {
		t.Errorf("ToAddrs: got %v, want [recipient1@example.com recipient2@example.com]", merged.ToAddrs)
	}
}

func TestMergeFilterIntoQuery_LabelAppends(t *testing.T) {
	q := &search.Query{
		Labels: []string{"inbox"},
	}
	filter := MessageFilter{Label: "important"}

	merged := MergeFilterIntoQuery(q, filter)

	if len(merged.Labels) != 2 {
		t.Fatalf("Labels: got %d items, want 2", len(merged.Labels))
	}
	if merged.Labels[0] != "inbox" || merged.Labels[1] != "important" {
		t.Errorf("Labels: got %v, want [inbox important]", merged.Labels)
	}
}

func TestMergeFilterIntoQuery_Attachments(t *testing.T) {
	q := &search.Query{}
	filter := MessageFilter{WithAttachmentsOnly: true}

	merged := MergeFilterIntoQuery(q, filter)

	if merged.HasAttachment == nil || !*merged.HasAttachment {
		t.Errorf("HasAttachment: got %v, want true", merged.HasAttachment)
	}
}

func TestMergeFilterIntoQuery_Domain(t *testing.T) {
	// Domain should be translated to from:@domain
	q := &search.Query{}
	filter := MessageFilter{Domain: "example.com"}

	merged := MergeFilterIntoQuery(q, filter)

	if len(merged.FromAddrs) != 1 || merged.FromAddrs[0] != "@example.com" {
		t.Errorf("FromAddrs: got %v, want [@example.com]", merged.FromAddrs)
	}
}

func TestMergeFilterIntoQuery_MultipleFilters(t *testing.T) {
	// Test multiple filters applied together
	q := &search.Query{
		TextTerms: []string{"search", "term"},
		FromAddrs: []string{"alice@example.com"},
	}
	sourceID := int64(1)
	filter := MessageFilter{
		SourceID:            &sourceID,
		Sender:              "bob@example.com",
		Recipient:           "carol@example.com",
		Label:               "starred",
		WithAttachmentsOnly: true,
		Domain:              "domain.com",
	}

	merged := MergeFilterIntoQuery(q, filter)

	// TextTerms preserved
	if len(merged.TextTerms) != 2 || merged.TextTerms[0] != "search" || merged.TextTerms[1] != "term" {
		t.Errorf("TextTerms: got %v, want [search term]", merged.TextTerms)
	}
	// AccountID set
	if merged.AccountID == nil || *merged.AccountID != 1 {
		t.Errorf("AccountID: got %v, want 1", merged.AccountID)
	}
	// FromAddrs: original + sender + domain
	if len(merged.FromAddrs) != 3 {
		t.Fatalf("FromAddrs: got %d items, want 3", len(merged.FromAddrs))
	}
	// ToAddrs: recipient
	if len(merged.ToAddrs) != 1 || merged.ToAddrs[0] != "carol@example.com" {
		t.Errorf("ToAddrs: got %v, want [carol@example.com]", merged.ToAddrs)
	}
	// Labels
	if len(merged.Labels) != 1 || merged.Labels[0] != "starred" {
		t.Errorf("Labels: got %v, want [starred]", merged.Labels)
	}
	// HasAttachment
	if merged.HasAttachment == nil || !*merged.HasAttachment {
		t.Errorf("HasAttachment: got %v, want true", merged.HasAttachment)
	}
}

func TestMergeFilterIntoQuery_DoesNotMutateOriginal(t *testing.T) {
	// Verify original query is not modified
	q := &search.Query{
		FromAddrs: []string{"original@example.com"},
	}
	filter := MessageFilter{Sender: "added@example.com"}

	_ = MergeFilterIntoQuery(q, filter)

	// Original should be unchanged
	if len(q.FromAddrs) != 1 || q.FromAddrs[0] != "original@example.com" {
		t.Errorf("Original query was mutated: FromAddrs=%v", q.FromAddrs)
	}
}

func TestMergeFilterIntoQuery_SliceAliasingMutation(t *testing.T) {
	// Test that slice aliasing doesn't cause mutation even with spare capacity
	// Create a slice with extra capacity that could be mutated by append
	backing := make([]string, 1, 10) // capacity 10, but only 1 element
	backing[0] = "original@example.com"

	q := &search.Query{
		FromAddrs: backing[:1], // slice with spare capacity
	}
	filter := MessageFilter{Sender: "added@example.com"}

	merged := MergeFilterIntoQuery(q, filter)

	// Merged should have both addresses
	if len(merged.FromAddrs) != 2 {
		t.Fatalf("Merged FromAddrs: got %d items, want 2", len(merged.FromAddrs))
	}

	// Original should still have only one address (no aliasing mutation)
	if len(q.FromAddrs) != 1 {
		t.Errorf("Original query was mutated via slice aliasing: FromAddrs=%v", q.FromAddrs)
	}
	if q.FromAddrs[0] != "original@example.com" {
		t.Errorf("Original FromAddrs[0] was changed: got %q, want original@example.com", q.FromAddrs[0])
	}
}

func TestSearchWithDomainFilter(t *testing.T) {
	env := newTestEnv(t)

	// Test search with @domain pattern (should match alice@example.com, bob@example.com)
	q := &search.Query{
		FromAddrs: []string{"@example.com"},
	}

	results := env.MustSearch(q, 100, 0)

	// Test data has alice@example.com and bob@example.com as senders
	// Should match messages from both
	if len(results) == 0 {
		t.Error("Expected results for @example.com domain search, got none")
	}

	// All results should be from example.com domain
	for _, r := range results {
		if r.FromEmail != "" && !strings.HasSuffix(r.FromEmail, "@example.com") {
			t.Errorf("Result from non-matching domain: %s", r.FromEmail)
		}
	}
}

func TestSearchMixedExactAndDomainFilter(t *testing.T) {
	env := newTestEnv(t)

	// Test search with both exact address and @domain pattern
	q := &search.Query{
		FromAddrs: []string{"alice@example.com", "@other.com"},
	}

	results := env.MustSearch(q, 100, 0)

	// Should match messages from alice@example.com OR anyone @other.com
	// In test data, we have alice@example.com
	if len(results) == 0 {
		t.Fatal("Expected at least one result, got 0")
	}
	foundAlice := false
	for _, r := range results {
		if r.FromEmail == "alice@example.com" {
			foundAlice = true
		}
	}
	if !foundAlice {
		t.Error("Expected to find messages from alice@example.com")
	}
}

func TestListMessages_ConversationIDFilter(t *testing.T) {
	env := newTestEnv(t)

	// Add a second conversation with different messages
	_, err := env.DB.Exec(`
		INSERT INTO conversations (id, source_id, source_conversation_id, conversation_type, title) VALUES
			(2, 1, 'thread2', 'email_thread', 'Second Thread');

		-- Add messages to the new conversation
		INSERT INTO messages (id, conversation_id, source_id, source_message_id, message_type, sent_at, subject, snippet, size_estimate, has_attachments) VALUES
			(10, 2, 1, 'msg10', 'email', '2024-04-01 10:00:00', 'Thread 2 Message 1', 'Thread 2 preview 1', 100, 0),
			(11, 2, 1, 'msg11', 'email', '2024-04-02 11:00:00', 'Thread 2 Message 2', 'Thread 2 preview 2', 200, 0);

		INSERT INTO message_recipients (message_id, participant_id, recipient_type, display_name) VALUES
			(10, 1, 'from', 'Alice'),
			(10, 2, 'to', 'Bob'),
			(11, 2, 'from', 'Bob'),
			(11, 1, 'to', 'Alice');
	`)
	if err != nil {
		t.Fatalf("insert second conversation: %v", err)
	}

	// Filter by first conversation (id=1) - should get original 5 messages
	convID1 := int64(1)
	filter1 := MessageFilter{
		ConversationID: &convID1,
	}

	messages1 := env.MustListMessages(filter1)

	if len(messages1) != 5 {
		t.Errorf("expected 5 messages in conversation 1, got %d", len(messages1))
	}

	// Verify all messages are from conversation 1
	for _, msg := range messages1 {
		if msg.ConversationID != 1 {
			t.Errorf("expected conversation_id=1, got %d for message %d", msg.ConversationID, msg.ID)
		}
	}

	// Filter by second conversation (id=2) - should get 2 messages
	convID2 := int64(2)
	filter2 := MessageFilter{
		ConversationID: &convID2,
	}

	messages2 := env.MustListMessages(filter2)

	if len(messages2) != 2 {
		t.Errorf("expected 2 messages in conversation 2, got %d", len(messages2))
	}

	// Verify all messages are from conversation 2
	for _, msg := range messages2 {
		if msg.ConversationID != 2 {
			t.Errorf("expected conversation_id=2, got %d for message %d", msg.ConversationID, msg.ID)
		}
	}

	// Test chronological ordering (ascending by date for thread view)
	filter2Asc := MessageFilter{
		ConversationID: &convID2,
		SortField:      MessageSortByDate,
		SortDirection:  SortAsc,
	}

	messagesAsc := env.MustListMessages(filter2Asc)

	if len(messagesAsc) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(messagesAsc))
	}

	// First message should be earlier (Thread 2 Message 1 from April 1)
	if messagesAsc[0].Subject != "Thread 2 Message 1" {
		t.Errorf("expected first message to be 'Thread 2 Message 1', got %q", messagesAsc[0].Subject)
	}
	if messagesAsc[1].Subject != "Thread 2 Message 2" {
		t.Errorf("expected second message to be 'Thread 2 Message 2', got %q", messagesAsc[1].Subject)
	}
}

// TestSearchFastCountMatchesSearch verifies that SearchFastCount returns the same
// count as the number of results from Search for various query types.
func TestSearchFastCountMatchesSearch(t *testing.T) {
	env := newTestEnv(t)

	tests := []struct {
		name  string
		query *search.Query
	}{
		{
			name:  "from filter",
			query: &search.Query{FromAddrs: []string{"alice@example.com"}},
		},
		{
			name:  "to filter",
			query: &search.Query{ToAddrs: []string{"bob@example.com"}},
		},
		{
			name:  "label filter",
			query: &search.Query{Labels: []string{"INBOX"}},
		},
		{
			name:  "subject filter",
			query: &search.Query{SubjectTerms: []string{"Test"}},
		},
		{
			name:  "combined filters",
			query: &search.Query{FromAddrs: []string{"alice@example.com"}, Labels: []string{"INBOX"}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Get search results
			results, err := env.Engine.Search(env.Ctx, tc.query, 1000, 0)
			if err != nil {
				t.Fatalf("Search: %v", err)
			}

			// Get count
			count, err := env.Engine.SearchFastCount(env.Ctx, tc.query, MessageFilter{})
			if err != nil {
				t.Fatalf("SearchFastCount: %v", err)
			}

			if int64(len(results)) != count {
				t.Errorf("SearchFastCount mismatch: got %d, want %d (Search returned %d results)",
					count, len(results), len(results))
			}
		})
	}
}

// =============================================================================
// RecipientName tests
// =============================================================================

func TestAggregateByRecipientName(t *testing.T) {
	env := newTestEnv(t)

	rows, err := env.Engine.AggregateByRecipientName(env.Ctx, DefaultAggregateOptions())
	if err != nil {
		t.Fatalf("AggregateByRecipientName: %v", err)
	}

	// Test data recipients: Bob Jones (msgs 1,2,3), Carol White (msg 1), Alice Smith (msgs 4,5)
	assertAggRows(t, rows, []aggExpectation{
		{"Bob Jones", 3},
		{"Alice Smith", 2},
		{"Carol White", 1},
	})
}

func TestAggregateByRecipientName_FallbackToEmail(t *testing.T) {
	env := newTestEnv(t)

	// Add a recipient participant with no display_name
	_, err := env.DB.Exec(`
		INSERT INTO participants (id, email_address, display_name, domain) VALUES
			(10, 'noname@test.com', NULL, 'test.com');
		INSERT INTO messages (id, conversation_id, source_id, source_message_id, message_type, sent_at, subject, snippet, size_estimate, has_attachments) VALUES
			(10, 1, 1, 'msg10', 'email', '2024-05-01 10:00:00', 'No Name Recipient', 'Test', 100, 0);
		INSERT INTO message_recipients (message_id, participant_id, recipient_type) VALUES
			(10, 1, 'from'),
			(10, 10, 'to');
	`)
	if err != nil {
		t.Fatalf("insert test data: %v", err)
	}

	rows, err := env.Engine.AggregateByRecipientName(env.Ctx, DefaultAggregateOptions())
	if err != nil {
		t.Fatalf("AggregateByRecipientName: %v", err)
	}

	// Check that the NULL display_name falls back to email
	var found bool
	for _, r := range rows {
		if r.Key == "noname@test.com" {
			found = true
			if r.Count != 1 {
				t.Errorf("expected noname@test.com count 1, got %d", r.Count)
			}
		}
	}
	if !found {
		t.Error("expected noname@test.com in results (display_name fallback)")
	}
}

func TestAggregateByRecipientName_EmptyStringFallback(t *testing.T) {
	env := newTestEnv(t)

	// Add recipient participants with empty-string and whitespace-only display_name
	_, err := env.DB.Exec(`
		INSERT INTO participants (id, email_address, display_name, domain) VALUES
			(11, 'empty@test.com', '', 'test.com'),
			(12, 'spaces@test.com', '   ', 'test.com');
		INSERT INTO messages (id, conversation_id, source_id, source_message_id, message_type, sent_at, subject, snippet, size_estimate, has_attachments) VALUES
			(11, 1, 1, 'msg11', 'email', '2024-05-01 10:00:00', 'Empty Rcpt Name', 'Test', 100, 0),
			(12, 1, 1, 'msg12', 'email', '2024-05-02 10:00:00', 'Spaces Rcpt Name', 'Test', 100, 0);
		INSERT INTO message_recipients (message_id, participant_id, recipient_type) VALUES
			(11, 1, 'from'),
			(11, 11, 'to'),
			(12, 1, 'from'),
			(12, 12, 'cc');
	`)
	if err != nil {
		t.Fatalf("insert test data: %v", err)
	}

	rows, err := env.Engine.AggregateByRecipientName(env.Ctx, DefaultAggregateOptions())
	if err != nil {
		t.Fatalf("AggregateByRecipientName: %v", err)
	}

	// Verify empty-string display_name falls back to email
	foundEmpty := false
	foundSpaces := false
	for _, r := range rows {
		if r.Key == "empty@test.com" {
			foundEmpty = true
		}
		if r.Key == "spaces@test.com" {
			foundSpaces = true
		}
	}
	if !foundEmpty {
		t.Error("expected empty@test.com in results (empty display_name fallback)")
	}
	if !foundSpaces {
		t.Error("expected spaces@test.com in results (whitespace display_name fallback)")
	}
}

func TestListMessages_RecipientNameFilter(t *testing.T) {
	env := newTestEnv(t)

	// Filter by recipient display name "Bob Jones"
	filter := MessageFilter{RecipientName: "Bob Jones"}
	messages := env.MustListMessages(filter)

	// Bob Jones received messages 1, 2, 3
	if len(messages) != 3 {
		t.Errorf("expected 3 messages to Bob Jones, got %d", len(messages))
	}
}

func TestListMessages_MatchEmptyRecipientName(t *testing.T) {
	env := newTestEnvWithEmptyBuckets(t)

	// Filter for messages with empty recipient name
	filter := MessageFilter{MatchEmptyRecipientName: true}
	messages := env.MustListMessages(filter)

	// Messages with no to/cc recipients should match
	if len(messages) == 0 {
		t.Fatal("expected at least 1 message with empty recipient name, got 0")
	}
	found := false
	for _, m := range messages {
		if m.Subject == "No Recipients" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected 'No Recipients' message in results")
		for _, m := range messages {
			t.Logf("  got: %q", m.Subject)
		}
	}
}

func TestSubAggregateByRecipientName(t *testing.T) {
	env := newTestEnv(t)

	// Filter by sender alice@example.com, sub-aggregate by recipient name
	filter := MessageFilter{Sender: "alice@example.com"}
	results, err := env.Engine.SubAggregate(env.Ctx, filter, ViewRecipientNames, DefaultAggregateOptions())
	if err != nil {
		t.Fatalf("SubAggregate: %v", err)
	}

	// Alice sent msgs 1,2,3 — recipients are Bob Jones (3 msgs) and Carol White (1 msg)
	if len(results) != 2 {
		t.Errorf("expected 2 recipient names from alice, got %d", len(results))
		for _, r := range results {
			t.Logf("  key=%q count=%d", r.Key, r.Count)
		}
	}
}

func TestGetGmailIDsByFilter_RecipientName(t *testing.T) {
	env := newTestEnv(t)

	filter := MessageFilter{RecipientName: "Bob Jones"}
	ids, err := env.Engine.GetGmailIDsByFilter(env.Ctx, filter)
	if err != nil {
		t.Fatalf("GetGmailIDsByFilter: %v", err)
	}

	// Bob Jones received messages msg1, msg2, msg3
	if len(ids) != 3 {
		t.Errorf("expected 3 gmail IDs for Bob Jones, got %d", len(ids))
	}
}

func TestMatchEmptyRecipientName_CombinedWithSender(t *testing.T) {
	env := newTestEnv(t)

	// Combining MatchEmptyRecipientName with Sender should not cause SQL errors.
	filter := MessageFilter{
		MatchEmptyRecipientName: true,
		Sender:                  "alice@example.com",
	}
	messages := env.MustListMessages(filter)

	// All of Alice's messages have recipients, so expect 0.
	if len(messages) != 0 {
		t.Errorf("expected 0 messages for MatchEmptyRecipientName+Sender, got %d", len(messages))
	}
}

func TestCombinedRecipientAndRecipientNameFilter(t *testing.T) {
	env := newTestEnv(t)

	// Both Recipient (email) and RecipientName (display name) set — must match
	// the same participant row, not different recipients.
	// Bob Jones has email bob@company.org, display_name "Bob Jones".
	filter := MessageFilter{
		Recipient:     "bob@company.org",
		RecipientName: "Bob Jones",
	}
	messages := env.MustListMessages(filter)

	// Messages 1,2,3 all have Bob Jones as a to recipient
	if len(messages) != 3 {
		t.Errorf("expected 3 messages matching both Recipient+RecipientName for Bob, got %d", len(messages))
	}
}

func TestCombinedRecipientAndRecipientName_Mismatch(t *testing.T) {
	env := newTestEnv(t)

	// Recipient email matches Bob but RecipientName matches Alice — since both
	// predicates must apply to the same participant row, this should return 0.
	filter := MessageFilter{
		Recipient:     "bob@company.org",
		RecipientName: "Alice Smith",
	}
	messages := env.MustListMessages(filter)

	if len(messages) != 0 {
		t.Errorf("expected 0 messages for mismatched Recipient+RecipientName, got %d", len(messages))
	}
}

func TestCombinedRecipientAndRecipientName_NoOvercount(t *testing.T) {
	env := newTestEnv(t)

	// Message 1 has two 'to' recipients: Bob and Carol.
	// Filtering by Recipient=bob + RecipientName=Bob Jones should still return
	// msg1 exactly once (not duplicated by Carol's join row).
	filter := MessageFilter{
		Recipient:     "bob@company.org",
		RecipientName: "Bob Jones",
	}
	messages := env.MustListMessages(filter)

	seen := make(map[int64]int)
	for _, m := range messages {
		seen[m.ID]++
	}
	for id, count := range seen {
		if count > 1 {
			t.Errorf("message ID %d returned %d times (expected once)", id, count)
		}
	}
}

// TestRecipientName_WithMatchEmptyRecipient verifies that combining RecipientName
// with MatchEmptyRecipient does not cause a SQL error (the combination is
// contradictory and should return 0 results).
func TestRecipientName_WithMatchEmptyRecipient(t *testing.T) {
	env := newTestEnv(t)

	filter := MessageFilter{
		RecipientName:       "Bob Jones",
		MatchEmptyRecipient: true,
	}

	// Should not error — the combination is contradictory so 0 rows expected.
	messages := env.MustListMessages(filter)
	if len(messages) != 0 {
		t.Errorf("expected 0 messages for contradictory RecipientName+MatchEmptyRecipient, got %d", len(messages))
	}
}

// TestGetGmailIDsByFilter_RecipientName_WithMatchEmptyRecipient verifies that
// GetGmailIDsByFilter does not produce SQL errors when RecipientName is combined
// with MatchEmptyRecipient. (GetGmailIDsByFilter does not implement
// MatchEmptyRecipient, so RecipientName is applied normally.)
func TestGetGmailIDsByFilter_RecipientName_WithMatchEmptyRecipient(t *testing.T) {
	env := newTestEnv(t)

	filter := MessageFilter{
		RecipientName:       "Bob Jones",
		MatchEmptyRecipient: true,
	}
	ids, err := env.Engine.GetGmailIDsByFilter(env.Ctx, filter)
	if err != nil {
		t.Fatalf("GetGmailIDsByFilter: %v", err)
	}
	// MatchEmptyRecipient is not supported in GetGmailIDsByFilter, so
	// RecipientName filters normally — Bob Jones received msg1, msg2, msg3.
	if len(ids) != 3 {
		t.Errorf("expected 3 gmail IDs, got %d", len(ids))
	}
}

// TestSubAggregate_RecipientName_WithRecipient verifies that combined
// Recipient+RecipientName works correctly in SubAggregate (buildFilterJoinsAndConditions).
func TestSubAggregate_RecipientName_WithRecipient(t *testing.T) {
	env := newTestEnv(t)

	filter := MessageFilter{
		Recipient:     "bob@company.org",
		RecipientName: "Bob Jones",
	}
	opts := AggregateOptions{Limit: 100}
	rows, err := env.Engine.SubAggregate(env.Ctx, filter, ViewSenders, opts)
	if err != nil {
		t.Fatalf("SubAggregate: %v", err)
	}

	// Bob Jones received msg1, msg2, msg3 — all from Alice
	if len(rows) != 1 {
		t.Errorf("expected 1 sender for Bob Jones, got %d", len(rows))
	}
	if len(rows) > 0 && rows[0].Key != "alice@example.com" {
		t.Errorf("expected sender alice@example.com, got %s", rows[0].Key)
	}
}
