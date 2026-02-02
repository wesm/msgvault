package query

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/wesm/msgvault/internal/search"
)

// testEnv encapsulates the DB, Engine, and Context setup for tests.
type testEnv struct {
	DB     *sql.DB
	Engine *SQLiteEngine
	Ctx    context.Context
	T      *testing.T

	// Auto-increment counters for builder helpers.
	nextParticipantID int64
	nextMessageID     int64
}

// newTestEnv creates a test environment with an in-memory SQLite database and test data.
func newTestEnv(t *testing.T) *testEnv {
	t.Helper()
	db := setupTestDB(t)
	t.Cleanup(func() { db.Close() })
	return &testEnv{
		DB:                db,
		Engine:            NewSQLiteEngine(db),
		Ctx:               context.Background(),
		T:                 t,
		nextParticipantID: 100,
		nextMessageID:     100,
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

// setupTestDB creates an in-memory SQLite database with the production schema and test data.
func setupTestDB(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}

	// Load the production schema to ensure tests run against the real schema.
	schema, err := os.ReadFile("../store/schema.sql")
	if err != nil {
		t.Fatalf("read schema.sql: %v", err)
	}

	if _, err := db.Exec(string(schema)); err != nil {
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

// ---------------------------------------------------------------------------
// Test data builder helpers
// ---------------------------------------------------------------------------

// participantOpts configures a participant to insert.
type participantOpts struct {
	Email       string
	DisplayName *string // nil = NULL
	Domain      string
}

// AddParticipant inserts a participant and returns its ID.
func (e *testEnv) AddParticipant(opts participantOpts) int64 {
	e.T.Helper()
	id := e.nextParticipantID
	e.nextParticipantID++

	var displayName interface{} = nil
	if opts.DisplayName != nil {
		displayName = *opts.DisplayName
	}

	_, err := e.DB.Exec(
		`INSERT INTO participants (id, email_address, display_name, domain) VALUES (?, ?, ?, ?)`,
		id, opts.Email, displayName, opts.Domain,
	)
	if err != nil {
		e.T.Fatalf("AddParticipant: %v", err)
	}
	return id
}

// messageOpts configures a message to insert.
type messageOpts struct {
	Subject        string
	SentAt         string // e.g. "2024-05-01 10:00:00"
	SizeEstimate   int
	HasAttachments bool
	FromID         int64   // participant ID for 'from' recipient; 0 = no from
	ToIDs          []int64 // participant IDs for 'to' recipients
	CcIDs          []int64 // participant IDs for 'cc' recipients
}

// AddMessage inserts a message with its from/to/cc recipients and returns the message ID.
func (e *testEnv) AddMessage(opts messageOpts) int64 {
	e.T.Helper()
	id := e.nextMessageID
	e.nextMessageID++

	sourceMessageID := fmt.Sprintf("msg%d", id)
	sentAt := opts.SentAt
	if sentAt == "" {
		sentAt = "2024-05-01 10:00:00"
	}
	size := opts.SizeEstimate
	if size == 0 {
		size = 100
	}

	_, err := e.DB.Exec(
		`INSERT INTO messages (id, conversation_id, source_id, source_message_id, message_type, sent_at, subject, snippet, size_estimate, has_attachments) VALUES (?, 1, 1, ?, 'email', ?, ?, 'test', ?, ?)`,
		id, sourceMessageID, sentAt, opts.Subject, size, opts.HasAttachments,
	)
	if err != nil {
		e.T.Fatalf("AddMessage: %v", err)
	}

	if opts.FromID != 0 {
		_, err = e.DB.Exec(
			`INSERT INTO message_recipients (message_id, participant_id, recipient_type) VALUES (?, ?, 'from')`,
			id, opts.FromID,
		)
		if err != nil {
			e.T.Fatalf("AddMessage from recipient: %v", err)
		}
	}

	for _, toID := range opts.ToIDs {
		_, err = e.DB.Exec(
			`INSERT INTO message_recipients (message_id, participant_id, recipient_type) VALUES (?, ?, 'to')`,
			id, toID,
		)
		if err != nil {
			e.T.Fatalf("AddMessage to recipient: %v", err)
		}
	}

	for _, ccID := range opts.CcIDs {
		_, err = e.DB.Exec(
			`INSERT INTO message_recipients (message_id, participant_id, recipient_type) VALUES (?, ?, 'cc')`,
			id, ccID,
		)
		if err != nil {
			e.T.Fatalf("AddMessage cc recipient: %v", err)
		}
	}

	return id
}

// strPtr returns a pointer to a string (helper for participantOpts.DisplayName).
func strPtr(s string) *string { return &s }

// newTestEnvWithEmptyBuckets creates a test DB with messages that have
// empty senders, recipients, domains, and labels for testing MatchEmpty* filters.
func newTestEnvWithEmptyBuckets(t *testing.T) *testEnv {
	t.Helper()

	env := newTestEnv(t)

	// Participant with empty domain
	emptyDomainID := env.AddParticipant(participantOpts{
		Email:       "nodomain@",
		DisplayName: strPtr("No Domain User"),
		Domain:      "",
	})

	// Message with no sender (msg6)
	env.AddMessage(messageOpts{
		Subject: "No Sender",
		SentAt:  "2024-04-01 10:00:00",
	})

	// Message with no recipients (msg7)
	env.AddMessage(messageOpts{
		Subject: "No Recipients",
		SentAt:  "2024-04-02 10:00:00",
		FromID:  1, // Alice
	})

	// Message with empty domain sender (msg8)
	env.AddMessage(messageOpts{
		Subject: "Empty Domain",
		SentAt:  "2024-04-03 10:00:00",
		FromID:  emptyDomainID,
		ToIDs:   []int64{1}, // Alice
	})

	// Message with no labels (msg9)
	env.AddMessage(messageOpts{
		Subject: "No Labels",
		SentAt:  "2024-04-04 10:00:00",
		FromID:  1, // Alice
		ToIDs:   []int64{2}, // Bob
	})

	return env
}
