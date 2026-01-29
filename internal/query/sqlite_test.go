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

// EnableFTS creates the FTS5 virtual table and rebuilds it.
// Skips the test if FTS5 is not available in this SQLite build.
func (e *testEnv) EnableFTS() {
	e.T.Helper()
	_, err := e.DB.Exec(`
		CREATE VIRTUAL TABLE messages_fts USING fts5(subject, body_text, snippet, content=messages, content_rowid=id);
		INSERT INTO messages_fts(messages_fts) VALUES('rebuild');
	`)
	if err != nil {
		e.T.Skipf("FTS5 not available in this SQLite build: %v", err)
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
			body_text TEXT,
			body_html TEXT,
			snippet TEXT,
			size_estimate INTEGER,
			has_attachments BOOLEAN DEFAULT FALSE,
			attachment_count INTEGER DEFAULT 0,
			deleted_from_source_at DATETIME
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
		INSERT INTO messages (id, conversation_id, source_id, source_message_id, message_type, sent_at, subject, body_text, snippet, size_estimate, has_attachments, attachment_count) VALUES
			(1, 1, 1, 'msg1', 'email', '2024-01-15 10:00:00', 'Hello World', 'Message body 1', 'Preview 1', 1000, 0, 0),
			(2, 1, 1, 'msg2', 'email', '2024-01-16 11:00:00', 'Re: Hello', 'Message body 2', 'Preview 2', 2000, 1, 2),
			(3, 1, 1, 'msg3', 'email', '2024-02-01 09:00:00', 'Follow up', 'Message body 3', 'Preview 3', 1500, 0, 0),
			(4, 1, 1, 'msg4', 'email', '2024-02-15 14:00:00', 'Question', 'Message body 4', 'Preview 4', 3000, 1, 1),
			(5, 1, 1, 'msg5', 'email', '2024-03-01 16:00:00', 'Final', 'Message body 5', 'Preview 5', 500, 0, 0);

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

	if len(rows) != 2 {
		t.Errorf("expected 2 senders, got %d", len(rows))
	}

	// Alice should have more messages (3) and be first with default sort (count desc)
	if rows[0].Key != "alice@example.com" {
		t.Errorf("expected alice first, got %s", rows[0].Key)
	}
	if rows[0].Count != 3 {
		t.Errorf("expected alice count 3, got %d", rows[0].Count)
	}

	if rows[1].Key != "bob@company.org" {
		t.Errorf("expected bob second, got %s", rows[1].Key)
	}
	if rows[1].Count != 2 {
		t.Errorf("expected bob count 2, got %d", rows[1].Count)
	}
}

func TestAggregateByRecipient(t *testing.T) {
	env := newTestEnv(t)

	rows, err := env.Engine.AggregateByRecipient(env.Ctx, DefaultAggregateOptions())
	if err != nil {
		t.Fatalf("AggregateByRecipient: %v", err)
	}

	// Should have Bob (3 messages as recipient), Alice (2), Carol (1)
	if len(rows) != 3 {
		t.Errorf("expected 3 recipients, got %d", len(rows))
	}

	if rows[0].Key != "bob@company.org" {
		t.Errorf("expected bob first, got %s", rows[0].Key)
	}
	if rows[0].Count != 3 {
		t.Errorf("expected bob count 3, got %d", rows[0].Count)
	}
}

func TestAggregateByDomain(t *testing.T) {
	env := newTestEnv(t)

	rows, err := env.Engine.AggregateByDomain(env.Ctx, DefaultAggregateOptions())
	if err != nil {
		t.Fatalf("AggregateByDomain: %v", err)
	}

	if len(rows) != 2 {
		t.Errorf("expected 2 domains, got %d", len(rows))
	}

	// example.com has 3 messages from Alice
	if rows[0].Key != "example.com" {
		t.Errorf("expected example.com first, got %s", rows[0].Key)
	}
	if rows[0].Count != 3 {
		t.Errorf("expected example.com count 3, got %d", rows[0].Count)
	}
}

func TestAggregateByLabel(t *testing.T) {
	env := newTestEnv(t)

	rows, err := env.Engine.AggregateByLabel(env.Ctx, DefaultAggregateOptions())
	if err != nil {
		t.Fatalf("AggregateByLabel: %v", err)
	}

	if len(rows) != 3 {
		t.Errorf("expected 3 labels, got %d", len(rows))
	}

	// INBOX should have all 5 messages
	if rows[0].Key != "INBOX" {
		t.Errorf("expected INBOX first, got %s", rows[0].Key)
	}
	if rows[0].Count != 5 {
		t.Errorf("expected INBOX count 5, got %d", rows[0].Count)
	}
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
	messages, err := env.Engine.ListMessages(env.Ctx, MessageFilter{})
	if err != nil {
		t.Fatalf("ListMessages: %v", err)
	}

	if len(messages) != 5 {
		t.Errorf("expected 5 messages, got %d", len(messages))
	}

	// Filter by sender
	messages, err = env.Engine.ListMessages(env.Ctx, MessageFilter{Sender: "alice@example.com"})
	if err != nil {
		t.Fatalf("ListMessages by sender: %v", err)
	}

	if len(messages) != 3 {
		t.Errorf("expected 3 messages from alice, got %d", len(messages))
	}

	// Filter by label
	messages, err = env.Engine.ListMessages(env.Ctx, MessageFilter{Label: "Work"})
	if err != nil {
		t.Fatalf("ListMessages by label: %v", err)
	}

	if len(messages) != 2 {
		t.Errorf("expected 2 messages with Work label, got %d", len(messages))
	}
}

func TestListMessagesWithLabels(t *testing.T) {
	env := newTestEnv(t)

	messages, err := env.Engine.ListMessages(env.Ctx, MessageFilter{})
	if err != nil {
		t.Fatalf("ListMessages: %v", err)
	}

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

	stats, err := env.Engine.GetTotalStats(env.Ctx, StatsOptions{})
	if err != nil {
		t.Fatalf("GetTotalStats: %v", err)
	}

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

	// Mark one message as deleted
	_, err := env.DB.Exec("UPDATE messages SET deleted_from_source_at = datetime('now') WHERE id = 1")
	if err != nil {
		t.Fatalf("mark deleted: %v", err)
	}

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
	messages, err := env.Engine.ListMessages(env.Ctx, MessageFilter{})
	if err != nil {
		t.Fatalf("ListMessages: %v", err)
	}

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
	stats, err := env.Engine.GetTotalStats(env.Ctx, StatsOptions{})
	if err != nil {
		t.Fatalf("GetTotalStats: %v", err)
	}

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

	// Mark message 1 as deleted
	_, err := env.DB.Exec("UPDATE messages SET deleted_from_source_at = datetime('now') WHERE id = 1")
	if err != nil {
		t.Fatalf("mark deleted: %v", err)
	}

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

	// Mark message 3 (source_message_id = 'msg3') as deleted
	_, err := env.DB.Exec("UPDATE messages SET deleted_from_source_at = datetime('now') WHERE source_message_id = 'msg3'")
	if err != nil {
		t.Fatalf("mark deleted: %v", err)
	}

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
	allStats, err := env.Engine.GetTotalStats(env.Ctx, StatsOptions{})
	if err != nil {
		t.Fatalf("GetTotalStats (all): %v", err)
	}

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
	source1Stats, err := env.Engine.GetTotalStats(env.Ctx, StatsOptions{SourceID: &sourceID})
	if err != nil {
		t.Fatalf("GetTotalStats (source 1): %v", err)
	}

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
	stats, err := env.Engine.GetTotalStats(env.Ctx, StatsOptions{SourceID: &nonExistentID})
	if err != nil {
		t.Fatalf("GetTotalStats with invalid source: %v", err)
	}

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
	allStats, err := env.Engine.GetTotalStats(env.Ctx, StatsOptions{})
	if err != nil {
		t.Fatalf("GetTotalStats: %v", err)
	}
	if allStats.MessageCount != 5 {
		t.Errorf("expected 5 total messages, got %d", allStats.MessageCount)
	}

	// Stats with attachment filter
	attStats, err := env.Engine.GetTotalStats(env.Ctx, StatsOptions{WithAttachmentsOnly: true})
	if err != nil {
		t.Fatalf("GetTotalStats with attachment filter: %v", err)
	}

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

	messages, err := env.Engine.ListMessages(env.Ctx, filter)
	if err != nil {
		t.Fatalf("ListMessages: %v", err)
	}

	// Should get messages from January 2024 (msg1, msg2)
	if len(messages) != 2 {
		t.Errorf("expected 2 messages for 2024-01, got %d", len(messages))
	}

	// Test with day period
	filter = MessageFilter{
		TimePeriod: "2024-01-15",
	}

	messages, err = env.Engine.ListMessages(env.Ctx, filter)
	if err != nil {
		t.Fatalf("ListMessages: %v", err)
	}

	// Should get only msg1 from 2024-01-15
	if len(messages) != 1 {
		t.Errorf("expected 1 message for 2024-01-15, got %d", len(messages))
	}

	// Test with year period - should match the explicit granularity
	filter = MessageFilter{
		TimePeriod:      "2024",
		TimeGranularity: TimeYear,
	}

	messages, err = env.Engine.ListMessages(env.Ctx, filter)
	if err != nil {
		t.Fatalf("ListMessages: %v", err)
	}

	// Should get all 5 messages from 2024
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

	results, err := env.Engine.Search(env.Ctx, q, 100, 0)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}

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

	results, err := env.Engine.Search(env.Ctx, q, 100, 0)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}

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

	results, err := env.Engine.Search(env.Ctx, q, 100, 0)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}

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

	results, err := env.Engine.Search(env.Ctx, q, 100, 0)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}

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

	results, err := env.Engine.Search(env.Ctx, q, 100, 0)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}

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

	results, err := env.Engine.Search(env.Ctx, q, 100, 0)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}

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

	results, err := env.Engine.Search(env.Ctx, q, 100, 0)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}

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

	results, err := env.Engine.Search(env.Ctx, q, 100, 0)
	if err != nil {
		t.Fatalf("Search with empty query: %v", err)
	}

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
		CREATE VIRTUAL TABLE messages_fts USING fts5(subject, body_text, snippet, content=messages, content_rowid=id);
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
		CREATE VIRTUAL TABLE messages_fts USING fts5(subject, body_text, snippet, content=messages, content_rowid=id);
	`)
	if err != nil {
		t.Skipf("FTS5 not available, cannot verify error-does-not-cache behavior: %v", err)
	}

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

	results, err := env.Engine.Search(env.Ctx, q, 100, 0)
	if err != nil {
		t.Fatalf("Search with FTS: %v", err)
	}

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

	messages, err := env.Engine.ListMessages(env.Ctx, filter)
	if err != nil {
		t.Fatalf("ListMessages: %v", err)
	}

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

	messages, err := env.Engine.ListMessages(env.Ctx, filter)
	if err != nil {
		t.Fatalf("ListMessages: %v", err)
	}

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

	messages, err := env.Engine.ListMessages(env.Ctx, filter)
	if err != nil {
		t.Fatalf("ListMessages: %v", err)
	}

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

	messages, err := env.Engine.ListMessages(env.Ctx, filter)
	if err != nil {
		t.Fatalf("ListMessages: %v", err)
	}

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
	filter := MessageFilter{
		MatchEmptyLabel: true,
		Sender:          "alice@example.com",
	}
	messages, err := env.Engine.ListMessages(env.Ctx, filter)
	if err != nil {
		t.Fatalf("ListMessages with MatchEmptyLabel + Sender: %v", err)
	}

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
	filter = MessageFilter{
		MatchEmptyLabel:  true,
		MatchEmptySender: true,
	}
	messages, err = env.Engine.ListMessages(env.Ctx, filter)
	if err != nil {
		t.Fatalf("ListMessages with MatchEmptyLabel + MatchEmptySender: %v", err)
	}

	// Only msg6 has both no labels AND no sender
	if len(messages) != 1 {
		t.Errorf("expected 1 message with MatchEmptyLabel + MatchEmptySender, got %d", len(messages))
	}
	if len(messages) > 0 && messages[0].Subject != "No Sender" {
		t.Errorf("expected 'No Sender' message, got %q", messages[0].Subject)
	}

	// Test 3: MatchEmptyLabel alone should NOT restrict by sender
	// (proving filters are independent - setting one doesn't affect others)
	filter = MessageFilter{
		MatchEmptyLabel: true,
	}
	messages, err = env.Engine.ListMessages(env.Ctx, filter)
	if err != nil {
		t.Fatalf("ListMessages with MatchEmptyLabel only: %v", err)
	}

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

	results, err := env.Engine.Search(env.Ctx, q, 100, 0)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}

	// Should find messages with "Hello" in subject (case-insensitive)
	if len(results) != 2 {
		t.Errorf("expected 2 messages with 'hello' (case-insensitive), got %d", len(results))
	}

	// Search for "WORLD" (uppercase) - should match "Hello World"
	q = &search.Query{
		TextTerms: []string{"WORLD"},
	}

	results, err = env.Engine.Search(env.Ctx, q, 100, 0)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}

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

	results, err := env.Engine.Search(env.Ctx, q, 100, 0)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}

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
	_, err = env.DB.Exec("UPDATE messages SET deleted_from_source_at = datetime('now') WHERE id = 1")
	if err != nil {
		t.Fatalf("mark deleted: %v", err)
	}

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

	results, err := env.Engine.Search(env.Ctx, q, 100, 0)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}

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

	results, err := env.Engine.Search(env.Ctx, q, 100, 0)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}

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

	messages1, err := env.Engine.ListMessages(env.Ctx, filter1)
	if err != nil {
		t.Fatalf("ListMessages for conversation 1: %v", err)
	}

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

	messages2, err := env.Engine.ListMessages(env.Ctx, filter2)
	if err != nil {
		t.Fatalf("ListMessages for conversation 2: %v", err)
	}

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

	messagesAsc, err := env.Engine.ListMessages(env.Ctx, filter2Asc)
	if err != nil {
		t.Fatalf("ListMessages with asc sort: %v", err)
	}

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
