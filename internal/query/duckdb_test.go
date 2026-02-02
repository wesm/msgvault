package query

import (
	"context"
	"runtime"
	"strings"
	"testing"

	_ "github.com/marcboeker/go-duckdb"
	_ "github.com/mattn/go-sqlite3"
	"github.com/wesm/msgvault/internal/search"
)

// newParquetEngine creates a DuckDBEngine backed by the standard Parquet test data.
// It registers cleanup via t.Cleanup so callers don't need defer.
func newParquetEngine(t *testing.T) *DuckDBEngine {
	t.Helper()
	return buildStandardTestData(t).BuildEngine()
}

// newEmptyBucketsEngine creates a DuckDBEngine backed by Parquet test data
// that includes messages with empty senders, recipients, domains, and labels.
func newEmptyBucketsEngine(t *testing.T) *DuckDBEngine {
	t.Helper()
	return buildEmptyBucketsTestData(t).BuildEngine()
}

// newSQLiteEngine creates a DuckDBEngine backed by the standard SQLite test data.
func newSQLiteEngine(t *testing.T) *DuckDBEngine {
	t.Helper()
	sqliteDB := setupTestDB(t)
	t.Cleanup(func() { sqliteDB.Close() })
	engine, err := NewDuckDBEngine("", "", sqliteDB)
	if err != nil {
		t.Fatalf("NewDuckDBEngine: %v", err)
	}
	t.Cleanup(func() { engine.Close() })
	return engine
}

// searchFast is a test helper that parses a query string and calls SearchFast.
func searchFast(t *testing.T, engine *DuckDBEngine, queryStr string, filter MessageFilter) []MessageSummary {
	t.Helper()
	q := search.Parse(queryStr)
	results, err := engine.SearchFast(context.Background(), q, filter, 100, 0)
	if err != nil {
		t.Fatalf("SearchFast(%q): %v", queryStr, err)
	}
	return results
}

// requireAggregateRow finds an AggregateRow by key or fails the test.
func requireAggregateRow(t *testing.T, rows []AggregateRow, key string) AggregateRow {
	t.Helper()
	for _, r := range rows {
		if r.Key == key {
			return r
		}
	}
	t.Fatalf("aggregate row %q not found in %d rows", key, len(rows))
	return AggregateRow{}
}

// assertMessageIDs checks that the returned messages have exactly the expected IDs (order-independent).
func assertMessageIDs(t *testing.T, messages []MessageSummary, wantIDs []int64) {
	t.Helper()
	got := make(map[int64]bool)
	for _, msg := range messages {
		if got[msg.ID] {
			t.Errorf("duplicate message ID %d", msg.ID)
		}
		got[msg.ID] = true
	}
	want := make(map[int64]bool)
	for _, id := range wantIDs {
		want[id] = true
	}
	for id := range want {
		if !got[id] {
			t.Errorf("missing expected message ID %d", id)
		}
	}
	for id := range got {
		if !want[id] {
			t.Errorf("unexpected message ID %d", id)
		}
	}
}

// assertStringIDs checks that the returned string IDs match expected (order-independent).
func assertStringIDs(t *testing.T, got []string, want []string) {
	t.Helper()
	gotSet := make(map[string]bool)
	for _, id := range got {
		if gotSet[id] {
			t.Errorf("duplicate ID %s", id)
		}
		gotSet[id] = true
	}
	wantSet := make(map[string]bool)
	for _, id := range want {
		wantSet[id] = true
	}
	for id := range wantSet {
		if !gotSet[id] {
			t.Errorf("missing expected ID %s", id)
		}
	}
	for id := range gotSet {
		if !wantSet[id] {
			t.Errorf("unexpected ID %s", id)
		}
	}
}

// assertSubjects checks that the returned messages have exactly the expected subjects (order-independent).
func assertSubjects(t *testing.T, messages []MessageSummary, want ...string) {
	t.Helper()
	got := make(map[string]bool)
	for _, msg := range messages {
		got[msg.Subject] = true
	}
	for _, s := range want {
		if !got[s] {
			t.Errorf("expected subject %q not found in results", s)
		}
	}
	if len(messages) != len(want) {
		t.Errorf("expected %d messages, got %d", len(want), len(messages))
	}
}

// buildStandardTestData creates a TestDataBuilder with the standard test data set:
// 1 source, 4 participants, 5 messages, 3 labels, and 3 attachments.
func buildStandardTestData(t *testing.T) *TestDataBuilder {
	t.Helper()
	b := NewTestDataBuilder(t)

	// Source
	b.AddSource("test@gmail.com")

	// Participants: alice(1), bob(2), carol(3), dan(4)
	b.AddParticipant("alice@example.com", "example.com", "Alice")
	b.AddParticipant("bob@company.org", "company.org", "Bob")
	b.AddParticipant("carol@example.com", "example.com", "Carol")
	b.AddParticipant("dan@other.net", "other.net", "Dan")

	// Messages
	convAB := int64(101) // shared conversation for msg1+msg2
	msg1 := b.AddMessage(MessageOpt{Subject: "Hello World", SentAt: makeDate(2024, 1, 15), SizeEstimate: 1000, ConversationID: convAB})
	msg2 := b.AddMessage(MessageOpt{Subject: "Re: Hello", SentAt: makeDate(2024, 1, 16), SizeEstimate: 2000, HasAttachments: true, ConversationID: convAB})
	msg3 := b.AddMessage(MessageOpt{Subject: "Follow up", SentAt: makeDate(2024, 2, 1), SizeEstimate: 1500, ConversationID: 102})
	msg4 := b.AddMessage(MessageOpt{Subject: "Question", SentAt: makeDate(2024, 2, 15), SizeEstimate: 3000, HasAttachments: true, ConversationID: 103})
	msg5 := b.AddMessage(MessageOpt{Subject: "Final", SentAt: makeDate(2024, 3, 1), SizeEstimate: 500, ConversationID: 104})

	// Recipients
	b.AddFrom(msg1, 1, "Alice"); b.AddTo(msg1, 2, "Bob"); b.AddTo(msg1, 3, "Carol")
	b.AddFrom(msg2, 1, "Alice"); b.AddTo(msg2, 2, "Bob"); b.AddCc(msg2, 4, "Dan")
	b.AddFrom(msg3, 1, "Alice"); b.AddTo(msg3, 2, "Bob")
	b.AddFrom(msg4, 2, "Bob");   b.AddTo(msg4, 1, "Alice")
	b.AddFrom(msg5, 2, "Bob");   b.AddTo(msg5, 1, "Alice")

	// Labels: INBOX(1), Work(2), IMPORTANT(3)
	inbox := b.AddLabel("INBOX")
	work := b.AddLabel("Work")
	important := b.AddLabel("IMPORTANT")

	// Message labels
	b.AddMessageLabel(msg1, inbox); b.AddMessageLabel(msg1, work)
	b.AddMessageLabel(msg2, inbox); b.AddMessageLabel(msg2, important)
	b.AddMessageLabel(msg3, inbox)
	b.AddMessageLabel(msg4, inbox); b.AddMessageLabel(msg4, work)
	b.AddMessageLabel(msg5, inbox)

	// Attachments
	b.AddAttachment(msg2, 10000, "document.pdf")
	b.AddAttachment(msg2, 5000, "image.png")
	b.AddAttachment(msg4, 20000, "report.xlsx")

	return b
}

// TestDuckDBEngine_SQLiteEngineReuse verifies that DuckDBEngine reuses a single
// SQLiteEngine instance for GetMessage, GetMessageBySourceID, and Search,
// preserving the FTS availability cache across calls.
//
// Note: DuckDB's Search/GetMessage/GetMessageBySourceID delegate to the shared
// sqliteEngine when sqliteDB is provided. Empty-bucket filters (MatchEmpty*)
// and case-insensitive search are tested in sqlite_test.go since the same
// SQLiteEngine code handles both direct SQLite and DuckDB-delegated calls.
func TestDuckDBEngine_SQLiteEngineReuse(t *testing.T) {
	// Set up test SQLite database
	sqliteDB := setupTestDB(t)
	defer sqliteDB.Close()

	// Create DuckDBEngine with sqliteDB but no Parquet (empty analytics dir)
	// We pass empty string for analyticsDir since we're only testing the SQLite path
	engine, err := NewDuckDBEngine("", "", sqliteDB)
	if err != nil {
		t.Fatalf("NewDuckDBEngine: %v", err)
	}
	defer engine.Close()

	// Verify sqliteEngine was created
	if engine.sqliteEngine == nil {
		t.Fatal("expected sqliteEngine to be created when sqliteDB is provided")
	}

	// Capture the sqliteEngine pointer to verify it's the same instance used
	sharedEngine := engine.sqliteEngine

	// Verify FTS cache is not yet checked
	if sharedEngine.ftsChecked {
		t.Error("expected ftsChecked to be false before any Search call")
	}

	ctx := context.Background()

	// Test GetMessage - should use sqliteEngine (doesn't trigger FTS check)
	msg, err := engine.GetMessage(ctx, 1)
	if err != nil {
		t.Fatalf("GetMessage: %v", err)
	}
	if msg == nil {
		t.Fatal("expected message, got nil")
	}
	if msg.Subject != "Hello World" {
		t.Errorf("expected subject 'Hello World', got %q", msg.Subject)
	}

	// Test GetMessageBySourceID - should use same sqliteEngine
	msg, err = engine.GetMessageBySourceID(ctx, "msg3")
	if err != nil {
		t.Fatalf("GetMessageBySourceID: %v", err)
	}
	if msg == nil {
		t.Fatal("expected message, got nil")
	}
	if msg.Subject != "Follow up" {
		t.Errorf("expected subject 'Follow up', got %q", msg.Subject)
	}

	// Test Search with text terms - triggers FTS availability check
	q := &search.Query{
		TextTerms: []string{"Hello"},
	}
	results, err := engine.Search(ctx, q, 100, 0)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("expected 2 messages with 'Hello', got %d", len(results))
	}

	// Verify FTS cache was checked on the shared engine instance
	// This proves Search used the shared sqliteEngine, not a new instance
	if !sharedEngine.ftsChecked {
		t.Error("expected ftsChecked to be true after Search with text terms")
	}

	// Verify it's still the same instance
	if engine.sqliteEngine != sharedEngine {
		t.Error("sqliteEngine pointer changed; expected same instance to be reused")
	}
}

// TestDuckDBEngine_SearchFromAddrs verifies address-based search filtering
// through the shared sqliteEngine path.
func TestDuckDBEngine_SearchFromAddrs(t *testing.T) {
	engine := newSQLiteEngine(t)
	ctx := context.Background()

	// Search by sender address
	q := &search.Query{
		FromAddrs: []string{"alice@example.com"},
	}
	results, err := engine.Search(ctx, q, 100, 0)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}

	// Alice sent 3 messages in the test data
	if len(results) != 3 {
		t.Errorf("expected 3 messages from alice, got %d", len(results))
	}

	for _, msg := range results {
		if msg.FromEmail != "alice@example.com" {
			t.Errorf("expected from alice@example.com, got %s", msg.FromEmail)
		}
	}
}

// TestDuckDBEngine_SQLiteEngineFTSCacheReuse verifies that the FTS availability
// cache is checked once and reused across multiple Search calls.
//
// Note: This test verifies that:
// 1. The first Search triggers FTS cache check (ftsChecked becomes true)
// 2. The cached result persists across searches
// 3. The sqliteEngine pointer remains the same
//
// While we cannot instrument a counter without modifying production code,
// the combination of these checks provides confidence that reuse works:
// - If Search created per-call engines, ftsChecked on sharedEngine would stay false
// - The pointer check ensures engine.sqliteEngine wasn't swapped
func TestDuckDBEngine_SQLiteEngineFTSCacheReuse(t *testing.T) {
	sqliteDB := setupTestDB(t)
	defer sqliteDB.Close()

	engine, err := NewDuckDBEngine("", "", sqliteDB)
	if err != nil {
		t.Fatalf("NewDuckDBEngine: %v", err)
	}
	defer engine.Close()

	// Capture the shared engine to verify cache state
	sharedEngine := engine.sqliteEngine
	if sharedEngine == nil {
		t.Fatal("expected sqliteEngine to be created")
	}

	ctx := context.Background()

	// Verify FTS cache starts unchecked
	if sharedEngine.ftsChecked {
		t.Error("expected ftsChecked to be false before first Search")
	}

	// First search - should trigger FTS availability check on shared engine
	q := &search.Query{
		TextTerms: []string{"Hello"},
	}
	results, err := engine.Search(ctx, q, 100, 0)
	if err != nil {
		t.Fatalf("Search 1: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("Search 1: expected 2 messages, got %d", len(results))
	}

	// Verify FTS cache is now set on the shared engine
	// This proves the first Search used the shared sqliteEngine
	if !sharedEngine.ftsChecked {
		t.Error("expected ftsChecked to be true after first Search")
	}

	// Capture the cached result
	cachedFTSResult := sharedEngine.ftsResult

	// Additional searches - verify cache state remains consistent
	// (If per-call engines were created, they wouldn't affect sharedEngine)
	for i := 2; i <= 3; i++ {
		q := &search.Query{
			TextTerms: []string{"Hello"},
		}
		results, err := engine.Search(ctx, q, 100, 0)
		if err != nil {
			t.Fatalf("Search %d: %v", i, err)
		}
		if len(results) != 2 {
			t.Errorf("Search %d: expected 2 messages, got %d", i, len(results))
		}

		// Verify the cache state hasn't changed
		if !sharedEngine.ftsChecked {
			t.Errorf("Search %d: ftsChecked became false; cache was reset", i)
		}
		if sharedEngine.ftsResult != cachedFTSResult {
			t.Errorf("Search %d: ftsResult changed from %v to %v", i, cachedFTSResult, sharedEngine.ftsResult)
		}
	}

	// Verify it's still the exact same sqliteEngine instance
	// This catches if DuckDBEngine.Search swapped the pointer
	if engine.sqliteEngine != sharedEngine {
		t.Error("sqliteEngine pointer changed during searches; expected same instance")
	}
}

// TestDuckDBEngine_NoSQLiteDB verifies behavior when sqliteDB is nil.
func TestDuckDBEngine_NoSQLiteDB(t *testing.T) {
	// Create engine without sqliteDB
	engine, err := NewDuckDBEngine("", "", nil)
	if err != nil {
		t.Fatalf("NewDuckDBEngine: %v", err)
	}
	defer engine.Close()

	// sqliteEngine should be nil
	if engine.sqliteEngine != nil {
		t.Error("expected sqliteEngine to be nil when sqliteDB is nil")
	}

	ctx := context.Background()

	// GetMessage should return error (no SQLite path configured)
	_, err = engine.GetMessage(ctx, 1)
	if err == nil {
		t.Error("expected error from GetMessage without SQLite, got nil")
	}

	// GetMessageBySourceID should return error
	_, err = engine.GetMessageBySourceID(ctx, "msg1")
	if err == nil {
		t.Error("expected error from GetMessageBySourceID without SQLite, got nil")
	}

	// Search should return error
	q := &search.Query{TextTerms: []string{"test"}}
	_, err = engine.Search(ctx, q, 100, 0)
	if err == nil {
		t.Error("expected error from Search without SQLite, got nil")
	}
}

// TestDuckDBEngine_GetMessageWithAttachments verifies attachment retrieval
// through the shared sqliteEngine path.
func TestDuckDBEngine_GetMessageWithAttachments(t *testing.T) {
	engine := newSQLiteEngine(t)
	ctx := context.Background()

	// Message 2 has 2 attachments
	msg, err := engine.GetMessage(ctx, 2)
	if err != nil {
		t.Fatalf("GetMessage: %v", err)
	}

	if len(msg.Attachments) != 2 {
		t.Errorf("expected 2 attachments, got %d", len(msg.Attachments))
	}

	// Verify attachment details
	found := false
	for _, att := range msg.Attachments {
		if att.Filename == "doc.pdf" {
			found = true
			if att.MimeType != "application/pdf" {
				t.Errorf("expected mime type application/pdf, got %s", att.MimeType)
			}
		}
	}
	if !found {
		t.Error("expected to find doc.pdf attachment")
	}
}

// TestDuckDBEngine_DeletedMessagesExcluded verifies that deleted messages
// are excluded when using the sqliteEngine path.
func TestDuckDBEngine_DeletedMessagesIncluded(t *testing.T) {
	sqliteDB := setupTestDB(t)
	t.Cleanup(func() { sqliteDB.Close() })

	// Mark message 1 as deleted
	_, err := sqliteDB.Exec("UPDATE messages SET deleted_from_source_at = datetime('now') WHERE id = 1")
	if err != nil {
		t.Fatalf("mark deleted: %v", err)
	}

	engine, err := NewDuckDBEngine("", "", sqliteDB)
	if err != nil {
		t.Fatalf("NewDuckDBEngine: %v", err)
	}
	t.Cleanup(func() { engine.Close() })

	ctx := context.Background()

	// GetMessage should RETURN deleted message (so user can still view it)
	msg, err := engine.GetMessage(ctx, 1)
	if err != nil {
		t.Fatalf("GetMessage: %v", err)
	}
	if msg == nil {
		t.Error("expected deleted message to be returned, got nil")
	}

	// Non-deleted message should still work
	msg, err = engine.GetMessage(ctx, 2)
	if err != nil {
		t.Fatalf("GetMessage: %v", err)
	}
	if msg == nil {
		t.Error("expected message 2, got nil")
	}
}

// TestDuckDBEngine_AggregateByRecipient verifies that recipient aggregation
// includes both to and cc recipients using list_concat.
func TestDuckDBEngine_AggregateByRecipient(t *testing.T) {
	engine := newParquetEngine(t)
	ctx := context.Background()
	results, err := engine.AggregateByRecipient(ctx, DefaultAggregateOptions())
	if err != nil {
		t.Fatalf("AggregateByRecipient: %v", err)
	}

	// Expected recipients from test data (includes cc):
	assertAggregateCounts(t, results, map[string]int64{
		"bob@company.org":   3, // to in msgs 1,2,3
		"carol@example.com": 1, // to in msg 1
		"alice@example.com": 2, // to in msgs 4,5
		"dan@other.net":     1, // cc in msg 2
	})
}

// TestDuckDBEngine_AggregateByRecipient_SearchFiltersOnKey verifies that
// searching in Recipients view filters on recipient email/name, not subject/sender.
// This reproduces a bug where the search applied to subject/sender instead of
// the recipient grouping key, causing inflated counts when summed across groups.
func TestDuckDBEngine_AggregateByRecipient_SearchFiltersOnKey(t *testing.T) {
	engine := newParquetEngine(t)
	ctx := context.Background()

	// Search for "bob" — should return only bob@company.org as a recipient
	// Test data: bob is a recipient (to) in msgs 1,2,3
	opts := DefaultAggregateOptions()
	opts.SearchQuery = "bob"
	rows, err := engine.AggregateByRecipient(ctx, opts)
	if err != nil {
		t.Fatalf("AggregateByRecipient (search 'bob'): %v", err)
	}

	// Should only match bob@company.org as recipient, not bob as sender
	if len(rows) != 1 {
		t.Fatalf("expected 1 recipient matching 'bob', got %d", len(rows))
	}
	if rows[0].Key != "bob@company.org" {
		t.Errorf("expected bob@company.org, got %s", rows[0].Key)
	}
	if rows[0].Count != 3 {
		t.Errorf("expected count=3 for bob, got %d", rows[0].Count)
	}

	// Search for "dan" — should return only dan@other.net (cc recipient in msg 2)
	opts.SearchQuery = "dan"
	rows, err = engine.AggregateByRecipient(ctx, opts)
	if err != nil {
		t.Fatalf("AggregateByRecipient (search 'dan'): %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 recipient matching 'dan', got %d", len(rows))
	}
	if rows[0].Key != "dan@other.net" {
		t.Errorf("expected dan@other.net, got %s", rows[0].Key)
	}

	// Verify totals don't exceed baseline
	baseOpts := DefaultAggregateOptions()
	baseRows, err := engine.AggregateByRecipient(ctx, baseOpts)
	if err != nil {
		t.Fatalf("AggregateByRecipient (no search): %v", err)
	}
	var baseTotal, searchTotal int64
	for _, r := range baseRows {
		baseTotal += r.Count
	}
	opts.SearchQuery = "a" // matches alice, carol, dan (display names with 'a')
	rows, err = engine.AggregateByRecipient(ctx, opts)
	if err != nil {
		t.Fatalf("AggregateByRecipient (search 'a'): %v", err)
	}
	for _, r := range rows {
		searchTotal += r.Count
	}
	if searchTotal > baseTotal {
		t.Errorf("search inflated total count: baseline=%d, withSearch=%d", baseTotal, searchTotal)
	}
}

// TestDuckDBEngine_AggregateByLabel_SearchFiltersOnKey verifies that
// searching in Labels view filters on label name, not subject/sender.
func TestDuckDBEngine_AggregateByLabel_SearchFiltersOnKey(t *testing.T) {
	engine := newParquetEngine(t)
	ctx := context.Background()

	// Search for "work" — should return only the Work label
	opts := DefaultAggregateOptions()
	opts.SearchQuery = "work"
	rows, err := engine.AggregateByLabel(ctx, opts)
	if err != nil {
		t.Fatalf("AggregateByLabel (search 'work'): %v", err)
	}

	if len(rows) != 1 {
		t.Fatalf("expected 1 label matching 'work', got %d", len(rows))
	}
	if rows[0].Key != "Work" {
		t.Errorf("expected 'Work', got %s", rows[0].Key)
	}
}

// TestDuckDBEngine_AggregateByDomain_SearchFiltersOnKey verifies that
// searching in Domains view filters on domain, not subject/sender.
func TestDuckDBEngine_AggregateByDomain_SearchFiltersOnKey(t *testing.T) {
	engine := newParquetEngine(t)
	ctx := context.Background()

	// Search for "company" — should return only company.org
	opts := DefaultAggregateOptions()
	opts.SearchQuery = "company"
	rows, err := engine.AggregateByDomain(ctx, opts)
	if err != nil {
		t.Fatalf("AggregateByDomain (search 'company'): %v", err)
	}

	if len(rows) != 1 {
		t.Fatalf("expected 1 domain matching 'company', got %d", len(rows))
	}
	if rows[0].Key != "company.org" {
		t.Errorf("expected 'company.org', got %s", rows[0].Key)
	}
}

// TestDuckDBEngine_AggregateBySender verifies sender aggregation from Parquet.
func TestDuckDBEngine_AggregateBySender(t *testing.T) {
	engine := newParquetEngine(t)
	ctx := context.Background()
	results, err := engine.AggregateBySender(ctx, DefaultAggregateOptions())
	if err != nil {
		t.Fatalf("AggregateBySender: %v", err)
	}

	assertAggregateCounts(t, results, map[string]int64{
		"alice@example.com": 3,
		"bob@company.org":   2,
	})
}

func TestDuckDBEngine_AggregateBySenderName(t *testing.T) {
	engine := newParquetEngine(t)
	ctx := context.Background()
	results, err := engine.AggregateBySenderName(ctx, DefaultAggregateOptions())
	if err != nil {
		t.Fatalf("AggregateBySenderName: %v", err)
	}

	assertAggregateCounts(t, results, map[string]int64{
		"Alice": 3,
		"Bob":   2,
	})
}

func TestDuckDBEngine_SubAggregateBySenderName(t *testing.T) {
	engine := newParquetEngine(t)
	ctx := context.Background()

	// Filter by recipient alice, sub-aggregate by sender name
	filter := MessageFilter{Recipient: "alice@example.com"}
	results, err := engine.SubAggregate(ctx, filter, ViewSenderNames, DefaultAggregateOptions())
	if err != nil {
		t.Fatalf("SubAggregate: %v", err)
	}

	// Messages to alice are 4, 5 (from Bob)
	if len(results) != 1 {
		t.Errorf("expected 1 sender name, got %d", len(results))
	}
	if len(results) > 0 && results[0].Key != "Bob" {
		t.Errorf("expected 'Bob', got %q", results[0].Key)
	}
}

func TestDuckDBEngine_ListMessages_SenderNameFilter(t *testing.T) {
	engine := newParquetEngine(t)
	ctx := context.Background()

	filter := MessageFilter{SenderName: "Alice"}
	results, err := engine.ListMessages(ctx, filter)
	if err != nil {
		t.Fatalf("ListMessages: %v", err)
	}

	// Alice sent messages 1, 2, 3
	if len(results) != 3 {
		t.Errorf("expected 3 messages from Alice, got %d", len(results))
	}
}

func TestDuckDBEngine_GetGmailIDsByFilter_SenderName(t *testing.T) {
	engine := newParquetEngine(t)
	ctx := context.Background()

	filter := MessageFilter{SenderName: "Alice"}
	ids, err := engine.GetGmailIDsByFilter(ctx, filter)
	if err != nil {
		t.Fatalf("GetGmailIDsByFilter: %v", err)
	}

	if len(ids) != 3 {
		t.Errorf("expected 3 gmail IDs for Alice, got %d", len(ids))
	}
}

func TestDuckDBEngine_AggregateBySenderName_EmptyStringFallback(t *testing.T) {
	// Build Parquet data with an empty-string and whitespace display_name
	engine := createEngineFromBuilder(t, newParquetBuilder(t).
		addTable("messages", "messages/year=2024", "data.parquet", messagesCols, `
			(1::BIGINT, 1::BIGINT, 'msg1', 100::BIGINT, 'Hello', 'Snippet', TIMESTAMP '2024-01-15 10:00:00', 1000::BIGINT, false, NULL::TIMESTAMP, 2024, 1),
			(2::BIGINT, 1::BIGINT, 'msg2', 101::BIGINT, 'World', 'Snippet', TIMESTAMP '2024-01-16 10:00:00', 1000::BIGINT, false, NULL::TIMESTAMP, 2024, 1)
		`).
		addTable("sources", "sources", "sources.parquet", sourcesCols, `
			(1::BIGINT, 'test@gmail.com')
		`).
		addTable("participants", "participants", "participants.parquet", participantsCols, `
			(1::BIGINT, 'empty@test.com', 'test.com', ''),
			(2::BIGINT, 'spaces@test.com', 'test.com', '   ')
		`).
		addTable("message_recipients", "message_recipients", "message_recipients.parquet", messageRecipientsCols, `
			(1::BIGINT, 1::BIGINT, 'from', 'Empty'),
			(2::BIGINT, 2::BIGINT, 'from', 'Spaces')
		`).
		addEmptyTable("labels", "labels", "labels.parquet", labelsCols, `(1::BIGINT, 'x')`).
		addEmptyTable("message_labels", "message_labels", "message_labels.parquet", messageLabelsCols, `(1::BIGINT, 1::BIGINT)`).
		addEmptyTable("attachments", "attachments", "attachments.parquet", attachmentsCols, `(1::BIGINT, 100::BIGINT, 'x')`))

	ctx := context.Background()
	results, err := engine.AggregateBySenderName(ctx, DefaultAggregateOptions())
	if err != nil {
		t.Fatalf("AggregateBySenderName: %v", err)
	}

	// Both '' and '   ' display_name should fall back to email
	if len(results) != 2 {
		t.Errorf("expected 2 sender names, got %d", len(results))
		for _, r := range results {
			t.Logf("  key=%q count=%d", r.Key, r.Count)
		}
	}

	for _, r := range results {
		if r.Key == "" || r.Key == "   " {
			t.Errorf("unexpected empty/whitespace key: %q", r.Key)
		}
	}
	requireAggregateRow(t, results, "empty@test.com")
	requireAggregateRow(t, results, "spaces@test.com")
}

func TestDuckDBEngine_ListMessages_MatchEmptySenderName(t *testing.T) {
	// Build Parquet data with a message that has no sender
	engine := createEngineFromBuilder(t, newParquetBuilder(t).
		addTable("messages", "messages/year=2024", "data.parquet", messagesCols, `
			(1::BIGINT, 1::BIGINT, 'msg1', 100::BIGINT, 'Has Sender', 'Snippet', TIMESTAMP '2024-01-15 10:00:00', 1000::BIGINT, false, NULL::TIMESTAMP, 2024, 1),
			(2::BIGINT, 1::BIGINT, 'msg2', 101::BIGINT, 'No Sender', 'Snippet', TIMESTAMP '2024-01-16 10:00:00', 1000::BIGINT, false, NULL::TIMESTAMP, 2024, 1)
		`).
		addTable("sources", "sources", "sources.parquet", sourcesCols, `
			(1::BIGINT, 'test@gmail.com')
		`).
		addTable("participants", "participants", "participants.parquet", participantsCols, `
			(1::BIGINT, 'alice@test.com', 'test.com', 'Alice')
		`).
		addTable("message_recipients", "message_recipients", "message_recipients.parquet", messageRecipientsCols, `
			(1::BIGINT, 1::BIGINT, 'from', 'Alice')
		`).
		addEmptyTable("labels", "labels", "labels.parquet", labelsCols, `(1::BIGINT, 'x')`).
		addEmptyTable("message_labels", "message_labels", "message_labels.parquet", messageLabelsCols, `(1::BIGINT, 1::BIGINT)`).
		addEmptyTable("attachments", "attachments", "attachments.parquet", attachmentsCols, `(1::BIGINT, 100::BIGINT, 'x')`))

	ctx := context.Background()
	// msg2 has no 'from' recipient, so MatchEmptySenderName should find it
	results, err := engine.ListMessages(ctx, MessageFilter{MatchEmptySenderName: true})
	if err != nil {
		t.Fatalf("ListMessages: %v", err)
	}

	if len(results) != 1 {
		t.Errorf("expected 1 message with empty sender name, got %d", len(results))
	}
	if len(results) > 0 && results[0].Subject != "No Sender" {
		t.Errorf("expected 'No Sender', got %q", results[0].Subject)
	}
}

// TestDuckDBEngine_AggregateAttachmentFields verifies attachment_count and attachment_size
// are correctly scanned from aggregate queries (attachment_size is DOUBLE, attachment_count is INT).
func TestDuckDBEngine_AggregateAttachmentFields(t *testing.T) {
	engine := newParquetEngine(t)
	ctx := context.Background()
	results, err := engine.AggregateBySender(ctx, DefaultAggregateOptions())
	if err != nil {
		t.Fatalf("AggregateBySender: %v", err)
	}

	// Test data:
	// alice@example.com: attachment_count=0+2+0=2, attachment_size=0+15000+0=15000
	// bob@company.org: attachment_count=1+0=1, attachment_size=20000+0=20000

	if len(results) < 2 {
		t.Fatalf("expected at least 2 results, got %d", len(results))
	}

	alice := requireAggregateRow(t, results, "alice@example.com")
	bob := requireAggregateRow(t, results, "bob@company.org")

	// Verify alice's attachment fields
	if alice.AttachmentCount != 2 {
		t.Errorf("alice AttachmentCount = %d, want 2", alice.AttachmentCount)
	}
	if alice.AttachmentSize != 15000 {
		t.Errorf("alice AttachmentSize = %d, want 15000", alice.AttachmentSize)
	}

	// Verify bob's attachment fields
	if bob.AttachmentCount != 1 {
		t.Errorf("bob AttachmentCount = %d, want 1", bob.AttachmentCount)
	}
	if bob.AttachmentSize != 20000 {
		t.Errorf("bob AttachmentSize = %d, want 20000", bob.AttachmentSize)
	}
}

// TestDuckDBEngine_AggregateByLabel verifies label aggregation from Parquet.
func TestDuckDBEngine_AggregateByLabel(t *testing.T) {
	engine := newParquetEngine(t)
	ctx := context.Background()
	results, err := engine.AggregateByLabel(ctx, DefaultAggregateOptions())
	if err != nil {
		t.Fatalf("AggregateByLabel: %v", err)
	}

	assertAggregateCounts(t, results, map[string]int64{
		"INBOX":     5,
		"Work":      2,
		"IMPORTANT": 1,
	})
}

// TestDuckDBEngine_SubAggregateByRecipient verifies sub-aggregation includes cc.
func TestDuckDBEngine_SubAggregateByRecipient(t *testing.T) {
	engine := newParquetEngine(t)
	ctx := context.Background()

	// Filter by sender alice@example.com (msgs 1,2,3) and sub-aggregate by recipients
	filter := MessageFilter{
		Sender: "alice@example.com",
	}

	results, err := engine.SubAggregate(ctx, filter, ViewRecipients, DefaultAggregateOptions())
	if err != nil {
		t.Fatalf("SubAggregate: %v", err)
	}

	// Expected recipients for alice's messages:
	// - bob@company.org: to in msgs 1,2,3 = 3
	// - carol@example.com: to in msg 1 = 1
	// - dan@other.net: cc in msg 2 = 1 (THIS TESTS CC INCLUSION IN SUBAGGREGATE)

	if len(results) != 3 {
		t.Errorf("expected 3 recipients for alice's messages, got %d", len(results))
		for _, r := range results {
			t.Logf("  %s: %d", r.Key, r.Count)
		}
	}

	// Verify dan@other.net (cc) is included
	dan := requireAggregateRow(t, results, "dan@other.net")
	if dan.Count != 1 {
		t.Errorf("expected dan@other.net count 1, got %d", dan.Count)
	}
}

// TestDuckDBEngine_AggregateByTime verifies time-based aggregation from Parquet.
func TestDuckDBEngine_AggregateByTime(t *testing.T) {
	engine := newParquetEngine(t)
	ctx := context.Background()

	opts := DefaultAggregateOptions()
	opts.TimeGranularity = TimeMonth

	results, err := engine.AggregateByTime(ctx, opts)
	if err != nil {
		t.Fatalf("AggregateByTime: %v", err)
	}

	assertAggregateCounts(t, results, map[string]int64{
		"2024-01": 2,
		"2024-02": 2,
		"2024-03": 1,
	})
}

// TestDuckDBEngine_SearchFast verifies SearchFast with various query types,
// filters, and context filters using table-driven subtests.
func TestDuckDBEngine_SearchFast(t *testing.T) {
	engine := newParquetEngine(t)

	tests := []struct {
		name         string
		query        string
		filter       MessageFilter
		wantSubjects []string
	}{
		// Subject search
		{"Subject", "Hello", MessageFilter{}, []string{"Hello World", "Re: Hello"}},

		// Operator filters
		{"FromFilter", "from:bob", MessageFilter{}, []string{"Question", "Final"}},
		{"LabelFilter", "label:Work", MessageFilter{}, []string{"Hello World", "Question"}},
		{"HasAttachment", "has:attachment", MessageFilter{}, []string{"Re: Hello", "Question"}},
		{"ToFilter_Bob", "to:bob", MessageFilter{}, []string{"Hello World", "Re: Hello", "Follow up"}},
		{"ToFilter_Carol", "to:carol", MessageFilter{}, []string{"Hello World"}},

		// Context filters (search + MessageFilter)
		{"ContextFilter_SenderAlice", "Hello", MessageFilter{Sender: "alice@example.com"}, []string{"Hello World", "Re: Hello"}},
		{"RecipientContextFilter", "Hello", MessageFilter{Recipient: "bob@company.org"}, []string{"Hello World", "Re: Hello"}},
		{"LabelContextFilter", "Hello", MessageFilter{Label: "Work"}, []string{"Hello World"}},
		{"DomainContextFilter", "Question", MessageFilter{Domain: "company.org"}, []string{"Question"}},
		{"DomainContextFilter_CaseInsensitive", "Hello", MessageFilter{Domain: "EXAMPLE.COM"}, []string{"Hello World", "Re: Hello"}},

		// Case-insensitive text search
		{"CaseInsensitive_Lower", "hello", MessageFilter{}, []string{"Hello World", "Re: Hello"}},
		{"CaseInsensitive_Upper", "HELLO", MessageFilter{}, []string{"Hello World", "Re: Hello"}},
		{"CaseInsensitive_Mixed", "HeLLo", MessageFilter{}, []string{"Hello World", "Re: Hello"}},
		{"CaseInsensitive_Sender_Upper", "ALICE", MessageFilter{}, []string{"Hello World", "Re: Hello", "Follow up"}},
		{"CaseInsensitive_Sender_Lower", "alice", MessageFilter{}, []string{"Hello World", "Re: Hello", "Follow up"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			results := searchFast(t, engine, tt.query, tt.filter)
			assertSubjects(t, results, tt.wantSubjects...)

			// Field-level assertions for specific cases
			switch tt.name {
			case "FromFilter":
				for _, r := range results {
					if r.FromEmail != "bob@company.org" {
						t.Errorf("from:bob result has FromEmail=%q, want bob@company.org", r.FromEmail)
					}
				}
			case "HasAttachment":
				for _, r := range results {
					if !r.HasAttachments {
						t.Errorf("has:attachment result %q has HasAttachments=false", r.Subject)
					}
				}
			}
		})
	}

	// Sender text search: matches sender OR recipient fields, so verify minimum count
	// and that at least one result is from bob
	t.Run("SenderTextSearch", func(t *testing.T) {
		results := searchFast(t, engine, "bob", MessageFilter{})
		if len(results) < 2 {
			t.Errorf("expected at least 2 results for 'bob', got %d", len(results))
		}
		foundFromBob := false
		for _, r := range results {
			if r.FromEmail == "bob@company.org" {
				foundFromBob = true
				break
			}
		}
		if !foundFromBob {
			t.Error("expected at least one message from bob@company.org")
		}
	})
}

// TestDuckDBEngine_ListMessages_DateFilter verifies that After/Before date filters
// work with DuckDB's TIMESTAMP column (regression: VARCHAR params need CAST).
func TestDuckDBEngine_ListMessages_DateFilter(t *testing.T) {
	engine := newParquetEngine(t)
	ctx := context.Background()

	// Test data: msg1-3 Jan 2024, msg4 Feb 2024, msg5 Mar 2024
	feb1 := makeDate(2024, 2, 1)
	mar1 := makeDate(2024, 3, 1)

	// After Feb 1 (>=): msg3 (Feb 1 09:00), msg4 (Feb 15), msg5 (Mar 1) = 3
	results, err := engine.ListMessages(ctx, MessageFilter{After: &feb1})
	if err != nil {
		t.Fatalf("ListMessages with After: %v", err)
	}
	if len(results) != 3 {
		t.Errorf("After Feb 1: expected 3 messages, got %d", len(results))
	}

	// Before Feb 1 (<): msg1 (Jan 15), msg2 (Jan 16) = 2
	results, err = engine.ListMessages(ctx, MessageFilter{Before: &feb1})
	if err != nil {
		t.Fatalf("ListMessages with Before: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("Before Feb 1: expected 2 messages, got %d", len(results))
	}

	// After Feb 1 AND Before Mar 1: msg3 (Feb 1), msg4 (Feb 15) = 2
	results, err = engine.ListMessages(ctx, MessageFilter{After: &feb1, Before: &mar1})
	if err != nil {
		t.Fatalf("ListMessages with After+Before: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("Feb range: expected 2 messages, got %d", len(results))
	}
}

// TestDuckDBEngine_SearchFast_DateFilter verifies that after:/before: in search
// queries work with DuckDB's TIMESTAMP column.
func TestDuckDBEngine_SearchFast_DateFilter(t *testing.T) {
	engine := newParquetEngine(t)

	// after:2024-02-01 (>=): msg3 (Feb 1), msg4 (Feb 15), msg5 (Mar 1)
	results := searchFast(t, engine, "after:2024-02-01", MessageFilter{})
	if len(results) != 3 {
		t.Errorf("after:2024-02-01: expected 3 results, got %d", len(results))
	}

	// before:2024-02-01 (<): msg1 (Jan 15), msg2 (Jan 16)
	results = searchFast(t, engine, "before:2024-02-01", MessageFilter{})
	if len(results) != 2 {
		t.Errorf("before:2024-02-01: expected 2 results, got %d", len(results))
	}

	// Combined: after:2024-02-01 before:2024-03-01 -> msg3, msg4
	results = searchFast(t, engine, "after:2024-02-01 before:2024-03-01", MessageFilter{})
	if len(results) != 2 {
		t.Errorf("Feb range: expected 2 results, got %d", len(results))
	}
}

// TestDuckDBEngine_AggregateBySender_DateFilter verifies date filters on aggregates.
func TestDuckDBEngine_AggregateBySender_DateFilter(t *testing.T) {
	engine := newParquetEngine(t)
	ctx := context.Background()

	// After Feb 1 (>=): msg3 from alice, msg4 from bob, msg5 from bob
	feb1 := makeDate(2024, 2, 1)
	opts := DefaultAggregateOptions()
	opts.After = &feb1

	results, err := engine.AggregateBySender(ctx, opts)
	if err != nil {
		t.Fatalf("AggregateBySender with After: %v", err)
	}

	assertAggregateCounts(t, results, map[string]int64{
		"alice@example.com": 1,
		"bob@company.org":   2,
	})
}

// TestDuckDBEngine_SubAggregate_DateFilter verifies CAST(? AS TIMESTAMP) in SubAggregate.
func TestDuckDBEngine_SubAggregate_DateFilter(t *testing.T) {
	engine := newParquetEngine(t)
	ctx := context.Background()

	feb1 := makeDate(2024, 2, 1)
	filter := MessageFilter{Sender: "alice@example.com"}
	opts := DefaultAggregateOptions()
	opts.After = &feb1

	// Alice sent msg3 (Feb 1) after Feb 1 — sub-aggregate by recipients
	results, err := engine.SubAggregate(ctx, filter, ViewRecipients, opts)
	if err != nil {
		t.Fatalf("SubAggregate with After: %v", err)
	}

	// msg3 goes to bob -> 1 recipient
	if len(results) != 1 {
		t.Errorf("expected 1 recipient after Feb 1 for alice, got %d", len(results))
	}
}

// TestDuckDBEngine_SearchFastCount_DateFilter verifies CAST(? AS TIMESTAMP) in SearchFastCount.
func TestDuckDBEngine_SearchFastCount_DateFilter(t *testing.T) {
	engine := newParquetEngine(t)
	ctx := context.Background()

	q := search.Parse("after:2024-02-01")
	count, err := engine.SearchFastCount(ctx, q, MessageFilter{})
	if err != nil {
		t.Fatalf("SearchFastCount: %v", err)
	}

	// msg3 (Feb 1), msg4 (Feb 15), msg5 (Mar 1) = 3
	if count != 3 {
		t.Errorf("SearchFastCount after:2024-02-01: expected 3, got %d", count)
	}
}

// TestDuckDBEngine_AggregateByDomain_DateFilter verifies CAST(? AS TIMESTAMP) in buildWhereClause.
func TestDuckDBEngine_AggregateByDomain_DateFilter(t *testing.T) {
	engine := newParquetEngine(t)
	ctx := context.Background()

	feb1 := makeDate(2024, 2, 1)
	opts := DefaultAggregateOptions()
	opts.After = &feb1

	// After Feb 1: msg3 from alice (example.com), msg4+msg5 from bob (company.org)
	results, err := engine.AggregateByDomain(ctx, opts)
	if err != nil {
		t.Fatalf("AggregateByDomain with After: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("expected 2 domains after Feb 1, got %d", len(results))
	}
}

// TestDuckDBEngine_ThreadCount verifies that DuckDB is initialized with the correct
// thread count based on GOMAXPROCS, and that the setting persists (single connection).
func TestDuckDBEngine_ThreadCount(t *testing.T) {
	engine, err := NewDuckDBEngine("", "", nil)
	if err != nil {
		t.Fatalf("NewDuckDBEngine: %v", err)
	}
	defer engine.Close()

	// Query the current thread setting
	var threads int
	err = engine.db.QueryRow("SELECT current_setting('threads')::INT").Scan(&threads)
	if err != nil {
		t.Fatalf("query threads setting: %v", err)
	}

	expected := runtime.GOMAXPROCS(0)
	if threads != expected {
		t.Errorf("expected threads=%d (GOMAXPROCS), got %d", expected, threads)
	}

	// Verify the setting persists across multiple queries (single connection pool)
	for i := 0; i < 3; i++ {
		var check int
		err = engine.db.QueryRow("SELECT current_setting('threads')::INT").Scan(&check)
		if err != nil {
			t.Fatalf("query threads setting (iteration %d): %v", i, err)
		}
		if check != expected {
			t.Errorf("iteration %d: expected threads=%d, got %d", i, expected, check)
		}
	}
}

func TestDuckDBEngine_ListMessages_ConversationIDFilter(t *testing.T) {
	engine := newParquetEngine(t)
	ctx := context.Background()

	// Test data has conversations:
	// - 101: msg1, msg2 (2 messages)
	// - 102: msg3 (1 message)
	// - 103: msg4 (1 message)
	// - 104: msg5 (1 message)

	// Filter by conversation 101 - should get 2 messages
	convID101 := int64(101)
	filter := MessageFilter{
		ConversationID: &convID101,
	}

	messages, err := engine.ListMessages(ctx, filter)
	if err != nil {
		t.Fatalf("ListMessages for conversation 101: %v", err)
	}

	if len(messages) != 2 {
		t.Errorf("expected 2 messages in conversation 101, got %d", len(messages))
	}

	// Verify all messages are from conversation 101
	for _, msg := range messages {
		if msg.ConversationID != 101 {
			t.Errorf("expected conversation_id=101, got %d for message %d", msg.ConversationID, msg.ID)
		}
	}

	// Filter by conversation 102 - should get 1 message
	convID102 := int64(102)
	filter2 := MessageFilter{
		ConversationID: &convID102,
	}

	messages2, err := engine.ListMessages(ctx, filter2)
	if err != nil {
		t.Fatalf("ListMessages for conversation 102: %v", err)
	}

	if len(messages2) != 1 {
		t.Errorf("expected 1 message in conversation 102, got %d", len(messages2))
	}

	if messages2[0].Subject != "Follow up" {
		t.Errorf("expected subject 'Follow up', got %q", messages2[0].Subject)
	}

	// Test chronological ordering for thread view (ascending by date)
	filterAsc := MessageFilter{
		ConversationID: &convID101,
		SortField:      MessageSortByDate,
		SortDirection:  SortAsc,
	}

	messagesAsc, err := engine.ListMessages(ctx, filterAsc)
	if err != nil {
		t.Fatalf("ListMessages with asc sort: %v", err)
	}

	if len(messagesAsc) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(messagesAsc))
	}

	// First message should be earlier (msg1 from Jan 15)
	if messagesAsc[0].Subject != "Hello World" {
		t.Errorf("expected first message to be 'Hello World', got %q", messagesAsc[0].Subject)
	}
	if messagesAsc[1].Subject != "Re: Hello" {
		t.Errorf("expected second message to be 'Re: Hello', got %q", messagesAsc[1].Subject)
	}
}

// TestDuckDBEngine_ListMessages_Filters is a table-driven test for all filter types.
// Test data setup (from setupTestParquet):
//
//	Messages: 1-5 (all in 2024)
//	  msg1: Jan 15, from alice, to bob+carol, labels: INBOX+Work
//	  msg2: Jan 16, from alice, to bob, cc dan, labels: INBOX+IMPORTANT, has_attachments
//	  msg3: Feb 01, from alice, to bob, labels: INBOX
//	  msg4: Feb 15, from bob, to alice, labels: INBOX+Work, has_attachments
//	  msg5: Mar 01, from bob, to alice, labels: INBOX
//
//	Participants: alice@example.com, bob@company.org, carol@example.com, dan@other.net
func TestDuckDBEngine_ListMessages_Filters(t *testing.T) {
	engine := newParquetEngine(t)
	ctx := context.Background()

	tests := []struct {
		name    string
		filter  MessageFilter
		wantIDs []int64 // expected message IDs
	}{
		// Sender filters
		{"sender=alice", MessageFilter{Sender: "alice@example.com"}, []int64{1, 2, 3}},
		{"sender=bob", MessageFilter{Sender: "bob@company.org"}, []int64{4, 5}},

		// Recipient filters
		{"recipient=bob", MessageFilter{Recipient: "bob@company.org"}, []int64{1, 2, 3}},
		{"recipient=alice", MessageFilter{Recipient: "alice@example.com"}, []int64{4, 5}},

		// Domain filters
		{"domain=example.com", MessageFilter{Domain: "example.com"}, []int64{1, 2, 3}},
		{"domain=company.org", MessageFilter{Domain: "company.org"}, []int64{4, 5}},

		// Label filters
		{"label=INBOX", MessageFilter{Label: "INBOX"}, []int64{1, 2, 3, 4, 5}},
		{"label=IMPORTANT", MessageFilter{Label: "IMPORTANT"}, []int64{2}},
		{"label=Work", MessageFilter{Label: "Work"}, []int64{1, 4}},

		// Time filters
		{"time=2024", MessageFilter{TimePeriod: "2024", TimeGranularity: TimeYear}, []int64{1, 2, 3, 4, 5}},
		{"time=2024-01", MessageFilter{TimePeriod: "2024-01", TimeGranularity: TimeMonth}, []int64{1, 2}},
		{"time=2024-02", MessageFilter{TimePeriod: "2024-02", TimeGranularity: TimeMonth}, []int64{3, 4}},
		{"time=2024-03", MessageFilter{TimePeriod: "2024-03", TimeGranularity: TimeMonth}, []int64{5}},

		// Attachment filter
		{"attachments", MessageFilter{WithAttachmentsOnly: true}, []int64{2, 4}},

		// Combined filters
		{"sender=alice+label=INBOX", MessageFilter{Sender: "alice@example.com", Label: "INBOX"}, []int64{1, 2, 3}},
		{"sender=alice+label=IMPORTANT", MessageFilter{Sender: "alice@example.com", Label: "IMPORTANT"}, []int64{2}},
		{"domain=example.com+time=2024-01", MessageFilter{Domain: "example.com", TimePeriod: "2024-01", TimeGranularity: TimeMonth}, []int64{1, 2}},
		{"sender=bob+attachments", MessageFilter{Sender: "bob@company.org", WithAttachmentsOnly: true}, []int64{4}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			messages, err := engine.ListMessages(ctx, tt.filter)
			if err != nil {
				t.Fatalf("ListMessages: %v", err)
			}
			assertMessageIDs(t, messages, tt.wantIDs)
		})
	}
}

func TestDuckDBEngine_GetGmailIDsByFilter(t *testing.T) {
	engine := newParquetEngine(t)
	ctx := context.Background()

	tests := []struct {
		name    string
		filter  MessageFilter
		wantIDs []string
	}{
		{
			name:    "sender=alice",
			filter:  MessageFilter{Sender: "alice@example.com"},
			wantIDs: []string{"msg1", "msg2", "msg3"},
		},
		{
			name:    "sender=bob",
			filter:  MessageFilter{Sender: "bob@company.org"},
			wantIDs: []string{"msg4", "msg5"},
		},
		{
			name:    "recipient=bob",
			filter:  MessageFilter{Recipient: "bob@company.org"},
			wantIDs: []string{"msg1", "msg2", "msg3"},
		},
		{
			name:    "recipient=alice",
			filter:  MessageFilter{Recipient: "alice@example.com"},
			wantIDs: []string{"msg4", "msg5"},
		},
		{
			name:    "domain=example.com",
			filter:  MessageFilter{Domain: "example.com"},
			wantIDs: []string{"msg1", "msg2", "msg3"},
		},
		{
			name:    "domain=company.org",
			filter:  MessageFilter{Domain: "company.org"},
			wantIDs: []string{"msg4", "msg5"},
		},
		{
			name:    "label=INBOX",
			filter:  MessageFilter{Label: "INBOX"},
			wantIDs: []string{"msg1", "msg2", "msg3", "msg4", "msg5"},
		},
		{
			name:    "label=Work",
			filter:  MessageFilter{Label: "Work"},
			wantIDs: []string{"msg1", "msg4"},
		},
		{
			name:    "time_period=2024-01",
			filter:  MessageFilter{TimePeriod: "2024-01", TimeGranularity: TimeMonth},
			wantIDs: []string{"msg1", "msg2"},
		},
		{
			name:    "time_period=2024-02",
			filter:  MessageFilter{TimePeriod: "2024-02", TimeGranularity: TimeMonth},
			wantIDs: []string{"msg3", "msg4"},
		},
		{
			name:    "sender+label",
			filter:  MessageFilter{Sender: "alice@example.com", Label: "INBOX"},
			wantIDs: []string{"msg1", "msg2", "msg3"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ids, err := engine.GetGmailIDsByFilter(ctx, tt.filter)
			if err != nil {
				t.Fatalf("GetGmailIDsByFilter: %v", err)
			}
			assertStringIDs(t, ids, tt.wantIDs)
		})
	}
}

// buildEmptyBucketsTestData creates a TestDataBuilder with messages that have
// empty senders, recipients, domains, and labels for testing MatchEmpty* filters.
func buildEmptyBucketsTestData(t *testing.T) *TestDataBuilder {
	t.Helper()
	b := NewTestDataBuilder(t)

	// Source
	b.AddSource("test@gmail.com")

	// Participants: alice(1), bob(2), nodomain(3)
	alice := b.AddParticipant("alice@example.com", "example.com", "Alice")
	bob := b.AddParticipant("bob@company.org", "company.org", "Bob")
	nodomain := b.AddParticipant("nodomain", "", "No Domain")

	// Messages
	msg1 := b.AddMessage(MessageOpt{Subject: "Normal 1", SentAt: makeDate(2024, 1, 15), SizeEstimate: 1000})
	msg2 := b.AddMessage(MessageOpt{Subject: "Normal 2", SentAt: makeDate(2024, 1, 16), SizeEstimate: 2000})
	msg3 := b.AddMessage(MessageOpt{Subject: "No Sender", SentAt: makeDate(2024, 1, 17), SizeEstimate: 1500})
	msg4 := b.AddMessage(MessageOpt{Subject: "No Recipients", SentAt: makeDate(2024, 1, 18), SizeEstimate: 3000})
	msg5 := b.AddMessage(MessageOpt{Subject: "No Labels", SentAt: makeDate(2024, 1, 19), SizeEstimate: 500})
	msg6 := b.AddMessage(MessageOpt{Subject: "Empty Domain", SentAt: makeDate(2024, 1, 20), SizeEstimate: 600})

	// Recipients
	b.AddFrom(msg1, alice, "Alice"); b.AddTo(msg1, bob, "Bob")
	b.AddFrom(msg2, bob, "Bob");     b.AddTo(msg2, alice, "Alice")
	b.AddTo(msg3, bob, "Bob") // no sender
	b.AddFrom(msg4, alice, "Alice")  // no recipients
	b.AddFrom(msg5, alice, "Alice"); b.AddTo(msg5, bob, "Bob") // no labels
	b.AddFrom(msg6, nodomain, "No Domain"); b.AddTo(msg6, bob, "Bob") // empty domain

	// Labels: INBOX(1), Work(2)
	inbox := b.AddLabel("INBOX")
	work := b.AddLabel("Work")

	// Message labels (msg5 intentionally has none)
	b.AddMessageLabel(msg1, inbox)
	b.AddMessageLabel(msg2, work)
	b.AddMessageLabel(msg3, inbox)
	b.AddMessageLabel(msg4, inbox)
	b.AddMessageLabel(msg6, inbox)

	// No attachments
	b.SetEmptyAttachments()

	return b
}

// TestDuckDBEngine_ListMessages_MatchEmptySender verifies that MatchEmptySender
// finds messages with no 'from' entry in message_recipients.
func TestDuckDBEngine_ListMessages_MatchEmptySender(t *testing.T) {
	engine := newEmptyBucketsEngine(t)
	ctx := context.Background()

	filter := MessageFilter{
		MatchEmptySender: true,
	}

	messages, err := engine.ListMessages(ctx, filter)
	if err != nil {
		t.Fatalf("ListMessages with MatchEmptySender: %v", err)
	}

	// Only msg3 has no sender
	if len(messages) != 1 {
		t.Errorf("expected 1 message with no sender, got %d", len(messages))
		for _, m := range messages {
			t.Logf("  got: id=%d subject=%q", m.ID, m.Subject)
		}
	}

	if len(messages) > 0 && messages[0].Subject != "No Sender" {
		t.Errorf("expected 'No Sender', got %q", messages[0].Subject)
	}
}

// TestDuckDBEngine_ListMessages_MatchEmptyRecipient verifies that MatchEmptyRecipient
// finds messages with no 'to' or 'cc' entries in message_recipients.
func TestDuckDBEngine_ListMessages_MatchEmptyRecipient(t *testing.T) {
	engine := newEmptyBucketsEngine(t)
	ctx := context.Background()

	filter := MessageFilter{
		MatchEmptyRecipient: true,
	}

	messages, err := engine.ListMessages(ctx, filter)
	if err != nil {
		t.Fatalf("ListMessages with MatchEmptyRecipient: %v", err)
	}

	// Only msg4 has no recipients
	if len(messages) != 1 {
		t.Errorf("expected 1 message with no recipients, got %d", len(messages))
		for _, m := range messages {
			t.Logf("  got: id=%d subject=%q", m.ID, m.Subject)
		}
	}

	if len(messages) > 0 && messages[0].Subject != "No Recipients" {
		t.Errorf("expected 'No Recipients', got %q", messages[0].Subject)
	}
}

// TestDuckDBEngine_ListMessages_MatchEmptyDomain verifies that MatchEmptyDomain
// finds messages where the sender has no domain.
func TestDuckDBEngine_ListMessages_MatchEmptyDomain(t *testing.T) {
	engine := newEmptyBucketsEngine(t)
	ctx := context.Background()

	filter := MessageFilter{
		MatchEmptyDomain: true,
	}

	messages, err := engine.ListMessages(ctx, filter)
	if err != nil {
		t.Fatalf("ListMessages with MatchEmptyDomain: %v", err)
	}

	// msg3 has no sender (so no domain), msg6 has sender with empty domain
	if len(messages) != 2 {
		t.Errorf("expected 2 messages with no domain, got %d", len(messages))
		for _, m := range messages {
			t.Logf("  got: id=%d subject=%q", m.ID, m.Subject)
		}
	}

	subjects := make(map[string]bool)
	for _, m := range messages {
		subjects[m.Subject] = true
	}
	if !subjects["No Sender"] {
		t.Error("expected 'No Sender' in results")
	}
	if !subjects["Empty Domain"] {
		t.Error("expected 'Empty Domain' in results")
	}
}

// TestDuckDBEngine_ListMessages_MatchEmptyLabel verifies that MatchEmptyLabel
// finds messages with no labels.
func TestDuckDBEngine_ListMessages_MatchEmptyLabel(t *testing.T) {
	engine := newEmptyBucketsEngine(t)
	ctx := context.Background()

	filter := MessageFilter{
		MatchEmptyLabel: true,
	}

	messages, err := engine.ListMessages(ctx, filter)
	if err != nil {
		t.Fatalf("ListMessages with MatchEmptyLabel: %v", err)
	}

	// Only msg5 has no labels
	if len(messages) != 1 {
		t.Errorf("expected 1 message with no labels, got %d", len(messages))
		for _, m := range messages {
			t.Logf("  got: id=%d subject=%q", m.ID, m.Subject)
		}
	}

	if len(messages) > 0 && messages[0].Subject != "No Labels" {
		t.Errorf("expected 'No Labels', got %q", messages[0].Subject)
	}
}

// TestDuckDBEngine_ListMessages_MatchEmptyCombined verifies that multiple
// MatchEmpty* flags create restrictive AND conditions.
func TestDuckDBEngine_ListMessages_MatchEmptyCombined(t *testing.T) {
	engine := newEmptyBucketsEngine(t)
	ctx := context.Background()

	// Test: MatchEmptyLabel AND specific sender
	// Only msg5 has no labels, and it's from alice
	filter := MessageFilter{
		Sender:          "alice@example.com",
		MatchEmptyLabel: true,
	}

	messages, err := engine.ListMessages(ctx, filter)
	if err != nil {
		t.Fatalf("ListMessages with Sender + MatchEmptyLabel: %v", err)
	}

	if len(messages) != 1 {
		t.Errorf("expected 1 message (alice with no labels), got %d", len(messages))
	}

	if len(messages) > 0 && messages[0].Subject != "No Labels" {
		t.Errorf("expected 'No Labels', got %q", messages[0].Subject)
	}
}

// TestDuckDBEngine_GetGmailIDsByFilter_NoParquet verifies error when analyticsDir is empty.
func TestDuckDBEngine_GetGmailIDsByFilter_NoParquet(t *testing.T) {
	// Create engine without Parquet
	engine, err := NewDuckDBEngine("", "", nil)
	if err != nil {
		t.Fatalf("NewDuckDBEngine: %v", err)
	}
	defer engine.Close()

	ctx := context.Background()
	_, err = engine.GetGmailIDsByFilter(ctx, MessageFilter{Sender: "test@example.com"})
	if err == nil {
		t.Fatal("expected error when calling GetGmailIDsByFilter without Parquet")
	}
	if !strings.Contains(err.Error(), "requires Parquet") {
		t.Errorf("expected 'requires Parquet' error, got: %v", err)
	}
}

// TestDuckDBEngine_GetGmailIDsByFilter_NonExistent verifies empty results for non-existent values.
func TestDuckDBEngine_GetGmailIDsByFilter_NonExistent(t *testing.T) {
	engine := newParquetEngine(t)
	ctx := context.Background()

	tests := []struct {
		name   string
		filter MessageFilter
	}{
		{"nonexistent_sender", MessageFilter{Sender: "nobody@nowhere.com"}},
		{"nonexistent_recipient", MessageFilter{Recipient: "nobody@nowhere.com"}},
		{"nonexistent_domain", MessageFilter{Domain: "nowhere.com"}},
		{"nonexistent_label", MessageFilter{Label: "NONEXISTENT"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ids, err := engine.GetGmailIDsByFilter(ctx, tt.filter)
			if err != nil {
				t.Fatalf("GetGmailIDsByFilter: %v", err)
			}
			if len(ids) != 0 {
				t.Errorf("expected 0 results for non-existent filter, got %d: %v", len(ids), ids)
			}
		})
	}
}

// TestDuckDBEngine_GetGmailIDsByFilter_EmptyFilter verifies that empty filter returns all messages.
func TestDuckDBEngine_GetGmailIDsByFilter_EmptyFilter(t *testing.T) {
	engine := newParquetEngine(t)
	ctx := context.Background()

	// Empty filter - should return all 5 messages
	ids, err := engine.GetGmailIDsByFilter(ctx, MessageFilter{})
	if err != nil {
		t.Fatalf("GetGmailIDsByFilter with empty filter: %v", err)
	}

	assertStringIDs(t, ids, []string{"msg1", "msg2", "msg3", "msg4", "msg5"})
}

// TestDuckDBEngine_GetGmailIDsByFilter_CombinedNoMatch verifies empty results for
// combined filters that match nothing.
func TestDuckDBEngine_GetGmailIDsByFilter_CombinedNoMatch(t *testing.T) {
	engine := newParquetEngine(t)
	ctx := context.Background()

	// Alice sent messages but none have IMPORTANT label in test data
	// (Actually msg2 has IMPORTANT, so let's use a different combo)
	// Bob sent msg4 and msg5, only msg4 has Work label
	// So bob + IMPORTANT should match nothing
	filter := MessageFilter{
		Sender: "bob@company.org",
		Label:  "IMPORTANT",
	}

	ids, err := engine.GetGmailIDsByFilter(ctx, filter)
	if err != nil {
		t.Fatalf("GetGmailIDsByFilter: %v", err)
	}

	if len(ids) != 0 {
		t.Errorf("expected 0 results for bob+IMPORTANT, got %d: %v", len(ids), ids)
	}
}

// =============================================================================
// Search Query Filter Tests
// =============================================================================

// TestEscapeILIKE verifies that ILIKE wildcard characters are escaped.
func TestEscapeILIKE(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"hello", "hello"},
		{"100%", "100\\%"},
		{"test_email", "test\\_email"},
		{"50% off!", "50\\% off!"},
		{"foo_bar_baz", "foo\\_bar\\_baz"},
		{"a\\b", "a\\\\b"},
		{"100%_test\\path", "100\\%\\_test\\\\path"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := escapeILIKE(tt.input)
			if got != tt.want {
				t.Errorf("escapeILIKE(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

// TestBuildWhereClause_SearchOperators tests that buildWhereClause handles
// various search operators correctly.
func TestBuildWhereClause_SearchOperators(t *testing.T) {
	engine := &DuckDBEngine{}

	tests := []struct {
		name        string
		searchQuery string
		wantClauses []string // Substrings that should appear in the WHERE clause
	}{
		{
			name:        "text terms",
			searchQuery: "hello world",
			wantClauses: []string{"msg.subject ILIKE", "ESCAPE"},
		},
		{
			name:        "from operator",
			searchQuery: "from:alice",
			wantClauses: []string{"recipient_type = 'from'", "email_address ILIKE"},
		},
		{
			name:        "to operator",
			searchQuery: "to:bob",
			wantClauses: []string{"recipient_type IN ('to', 'cc', 'bcc')", "email_address ILIKE"},
		},
		{
			name:        "subject operator",
			searchQuery: "subject:urgent",
			wantClauses: []string{"msg.subject ILIKE"},
		},
		{
			name:        "has attachment",
			searchQuery: "has:attachment",
			wantClauses: []string{"msg.has_attachments = 1"},
		},
		{
			name:        "label operator",
			searchQuery: "label:INBOX",
			wantClauses: []string{"l_label.name = ?"},  // Exact match, consistent with SearchFast
		},
		{
			name:        "combined operators",
			searchQuery: "from:alice subject:meeting has:attachment",
			wantClauses: []string{
				"recipient_type = 'from'",
				"msg.subject ILIKE",
				"msg.has_attachments = 1",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := AggregateOptions{SearchQuery: tt.searchQuery}
			where, _ := engine.buildWhereClause(opts)

			for _, want := range tt.wantClauses {
				if !strings.Contains(where, want) {
					t.Errorf("buildWhereClause(%q) missing %q\ngot: %s", tt.searchQuery, want, where)
				}
			}
		})
	}
}

// TestBuildWhereClause_EscapedArgs verifies that wildcards in search terms
// are properly escaped in the query arguments.
func TestBuildWhereClause_EscapedArgs(t *testing.T) {
	engine := &DuckDBEngine{}

	opts := AggregateOptions{SearchQuery: "100%_off"}
	_, args := engine.buildWhereClause(opts)

	// The escaped pattern should appear in args
	found := false
	for _, arg := range args {
		if s, ok := arg.(string); ok && strings.Contains(s, "100\\%\\_off") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected escaped pattern '100\\%%\\_off' in args, got: %v", args)
	}
}

// TestAggregateBySender_WithSearchQuery verifies that aggregate queries respect
// search query filters.
func TestAggregateBySender_WithSearchQuery(t *testing.T) {
	engine := newParquetEngine(t)
	ctx := context.Background()

	// Test data has:
	// - alice sends msg1, msg2, msg3 (subjects: Hello World, Re: Hello, Follow up)
	// - bob sends msg4, msg5 (subjects: Question, Final)

	tests := []struct {
		name        string
		searchQuery string
		wantSenders []string
	}{
		{
			name:        "text search matching alice messages",
			searchQuery: "Hello",
			wantSenders: []string{"alice@example.com"}, // Only alice has "Hello" in subjects
		},
		{
			name:        "has:attachment filter",
			searchQuery: "has:attachment",
			wantSenders: []string{"alice@example.com", "bob@company.org"}, // msg2 (alice) and msg4 (bob)
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := AggregateOptions{
				SearchQuery: tt.searchQuery,
				Limit:       100,
			}
			rows, err := engine.AggregateBySender(ctx, opts)
			if err != nil {
				t.Fatalf("AggregateBySender: %v", err)
			}

			gotSenders := make(map[string]bool)
			for _, row := range rows {
				gotSenders[row.Key] = true
			}

			for _, want := range tt.wantSenders {
				if !gotSenders[want] {
					t.Errorf("expected sender %q in results, got: %v", want, rows)
				}
			}
		})
	}
}

// TestBuildSearchConditions_EscapedWildcards verifies that buildSearchConditions
// escapes ILIKE wildcards and uses ESCAPE clause for all text patterns.
func TestBuildSearchConditions_EscapedWildcards(t *testing.T) {
	engine := &DuckDBEngine{}

	tests := []struct {
		name         string
		query        *search.Query
		wantClauses  []string // Substrings in WHERE clause
		wantInArgs   []string // Substrings that should appear in args
	}{
		{
			name: "TextTerms with wildcards",
			query: &search.Query{
				TextTerms: []string{"100%_off"},
			},
			wantClauses: []string{"ESCAPE '\\'"},
			wantInArgs:  []string{"100\\%\\_off"},
		},
		{
			name: "from: with wildcards",
			query: &search.Query{
				FromAddrs: []string{"test_user%"},
			},
			wantClauses: []string{"ms.from_email ILIKE", "ESCAPE"},
			wantInArgs:  []string{"test\\_user\\%"},
		},
		{
			name: "to: with wildcards",
			query: &search.Query{
				ToAddrs: []string{"bob_smith%"},
			},
			wantClauses: []string{"email_address ILIKE", "ESCAPE"},
			wantInArgs:  []string{"bob\\_smith\\%"},
		},
		{
			name: "subject: with wildcards",
			query: &search.Query{
				SubjectTerms: []string{"50%_discount"},
			},
			wantClauses: []string{"msg.subject ILIKE", "ESCAPE"},
			wantInArgs:  []string{"50\\%\\_discount"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conditions, args := engine.buildSearchConditions(tt.query, MessageFilter{})
			where := strings.Join(conditions, " AND ")

			// Check WHERE clause contains expected patterns
			for _, want := range tt.wantClauses {
				if !strings.Contains(where, want) {
					t.Errorf("buildSearchConditions missing %q in WHERE\ngot: %s", want, where)
				}
			}

			// Check args contain escaped patterns
			for _, wantArg := range tt.wantInArgs {
				found := false
				for _, arg := range args {
					if s, ok := arg.(string); ok && strings.Contains(s, wantArg) {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("expected escaped pattern %q in args, got: %v", wantArg, args)
				}
			}
		})
	}
}

// =============================================================================
// RecipientName tests
// =============================================================================

func TestDuckDBEngine_AggregateByRecipientName(t *testing.T) {
	engine := newParquetEngine(t)
	ctx := context.Background()
	results, err := engine.AggregateByRecipientName(ctx, DefaultAggregateOptions())
	if err != nil {
		t.Fatalf("AggregateByRecipientName: %v", err)
	}

	assertAggregateCounts(t, results, map[string]int64{
		"Bob":   3, // msgs 1,2,3
		"Alice": 2, // msgs 4,5
		"Carol": 1, // msg 1
		"Dan":   1, // msg 2 cc
	})
}

func TestDuckDBEngine_SubAggregateByRecipientName(t *testing.T) {
	engine := newParquetEngine(t)
	ctx := context.Background()

	// Filter by sender alice, sub-aggregate by recipient name
	filter := MessageFilter{Sender: "alice@example.com"}
	results, err := engine.SubAggregate(ctx, filter, ViewRecipientNames, DefaultAggregateOptions())
	if err != nil {
		t.Fatalf("SubAggregate: %v", err)
	}

	// Alice sent msgs 1,2,3 — recipients: Bob (3), Carol (1), Dan (1 via cc)
	if len(results) != 3 {
		t.Errorf("expected 3 recipient names, got %d", len(results))
		for _, r := range results {
			t.Logf("  key=%q count=%d", r.Key, r.Count)
		}
	}
}

func TestDuckDBEngine_ListMessages_RecipientNameFilter(t *testing.T) {
	engine := newParquetEngine(t)
	ctx := context.Background()

	filter := MessageFilter{RecipientName: "Bob"}
	results, err := engine.ListMessages(ctx, filter)
	if err != nil {
		t.Fatalf("ListMessages: %v", err)
	}

	// Bob received messages 1, 2, 3
	if len(results) != 3 {
		t.Errorf("expected 3 messages to Bob, got %d", len(results))
	}
}

func TestDuckDBEngine_GetGmailIDsByFilter_RecipientName(t *testing.T) {
	engine := newParquetEngine(t)
	ctx := context.Background()

	filter := MessageFilter{RecipientName: "Alice"}
	ids, err := engine.GetGmailIDsByFilter(ctx, filter)
	if err != nil {
		t.Fatalf("GetGmailIDsByFilter: %v", err)
	}

	// Alice received msgs 4, 5
	if len(ids) != 2 {
		t.Errorf("expected 2 gmail IDs for Alice, got %d", len(ids))
	}
}

func TestDuckDBEngine_AggregateByRecipientName_EmptyStringFallback(t *testing.T) {
	// Build Parquet data with empty-string and whitespace display_names on recipients
	engine := createEngineFromBuilder(t, newParquetBuilder(t).
		addTable("messages", "messages/year=2024", "data.parquet", messagesCols, `
			(1::BIGINT, 1::BIGINT, 'msg1', 100::BIGINT, 'Hello', 'Snippet', TIMESTAMP '2024-01-15 10:00:00', 1000::BIGINT, false, NULL::TIMESTAMP, 2024, 1),
			(2::BIGINT, 1::BIGINT, 'msg2', 101::BIGINT, 'World', 'Snippet', TIMESTAMP '2024-01-16 10:00:00', 1000::BIGINT, false, NULL::TIMESTAMP, 2024, 1)
		`).
		addTable("sources", "sources", "sources.parquet", sourcesCols, `
			(1::BIGINT, 'test@gmail.com')
		`).
		addTable("participants", "participants", "participants.parquet", participantsCols, `
			(1::BIGINT, 'sender@test.com', 'test.com', 'Sender'),
			(2::BIGINT, 'empty@test.com', 'test.com', ''),
			(3::BIGINT, 'spaces@test.com', 'test.com', '   ')
		`).
		addTable("message_recipients", "message_recipients", "message_recipients.parquet", messageRecipientsCols, `
			(1::BIGINT, 1::BIGINT, 'from', 'Sender'),
			(1::BIGINT, 2::BIGINT, 'to', 'Empty'),
			(2::BIGINT, 1::BIGINT, 'from', 'Sender'),
			(2::BIGINT, 3::BIGINT, 'cc', 'Spaces')
		`).
		addEmptyTable("labels", "labels", "labels.parquet", labelsCols, `(1::BIGINT, 'x')`).
		addEmptyTable("message_labels", "message_labels", "message_labels.parquet", messageLabelsCols, `(1::BIGINT, 1::BIGINT)`).
		addEmptyTable("attachments", "attachments", "attachments.parquet", attachmentsCols, `(1::BIGINT, 100::BIGINT, 'x')`))

	ctx := context.Background()
	results, err := engine.AggregateByRecipientName(ctx, DefaultAggregateOptions())
	if err != nil {
		t.Fatalf("AggregateByRecipientName: %v", err)
	}

	// Both '' and '   ' display_name should fall back to email
	if len(results) != 2 {
		t.Errorf("expected 2 recipient names, got %d", len(results))
		for _, r := range results {
			t.Logf("  key=%q count=%d", r.Key, r.Count)
		}
	}

	for _, r := range results {
		if r.Key == "" || r.Key == "   " {
			t.Errorf("unexpected empty/whitespace key: %q", r.Key)
		}
	}
	requireAggregateRow(t, results, "empty@test.com")
	requireAggregateRow(t, results, "spaces@test.com")
}

func TestDuckDBEngine_ListMessages_MatchEmptyRecipientName(t *testing.T) {
	// Build Parquet data with a message that has no recipients
	engine := createEngineFromBuilder(t, newParquetBuilder(t).
		addTable("messages", "messages/year=2024", "data.parquet", messagesCols, `
			(1::BIGINT, 1::BIGINT, 'msg1', 100::BIGINT, 'Has Recipient', 'Snippet', TIMESTAMP '2024-01-15 10:00:00', 1000::BIGINT, false, NULL::TIMESTAMP, 2024, 1),
			(2::BIGINT, 1::BIGINT, 'msg2', 101::BIGINT, 'No Recipient', 'Snippet', TIMESTAMP '2024-01-16 10:00:00', 1000::BIGINT, false, NULL::TIMESTAMP, 2024, 1)
		`).
		addTable("sources", "sources", "sources.parquet", sourcesCols, `
			(1::BIGINT, 'test@gmail.com')
		`).
		addTable("participants", "participants", "participants.parquet", participantsCols, `
			(1::BIGINT, 'alice@test.com', 'test.com', 'Alice'),
			(2::BIGINT, 'bob@test.com', 'test.com', 'Bob')
		`).
		addTable("message_recipients", "message_recipients", "message_recipients.parquet", messageRecipientsCols, `
			(1::BIGINT, 1::BIGINT, 'from', 'Alice'),
			(1::BIGINT, 2::BIGINT, 'to', 'Bob')
		`).
		addEmptyTable("labels", "labels", "labels.parquet", labelsCols, `(1::BIGINT, 'x')`).
		addEmptyTable("message_labels", "message_labels", "message_labels.parquet", messageLabelsCols, `(1::BIGINT, 1::BIGINT)`).
		addEmptyTable("attachments", "attachments", "attachments.parquet", attachmentsCols, `(1::BIGINT, 100::BIGINT, 'x')`))

	ctx := context.Background()
	filter := MessageFilter{MatchEmptyRecipientName: true}
	results, err := engine.ListMessages(ctx, filter)
	if err != nil {
		t.Fatalf("ListMessages: %v", err)
	}

	// msg2 has no to/cc recipients -> should match
	if len(results) != 1 {
		t.Errorf("expected 1 message with empty recipient name, got %d", len(results))
	}
	if len(results) > 0 && results[0].Subject != "No Recipient" {
		t.Errorf("expected 'No Recipient', got %q", results[0].Subject)
	}
}

func TestDuckDBEngine_GetTotalStats_GroupByRecipients(t *testing.T) {
	if runtime.GOOS == "linux" {
		t.Skip("skipping DuckDB test on Linux CI")
	}
	engine := newParquetEngine(t)

	// Search "bob" with GroupBy=ViewRecipients should search recipient key columns.
	// Bob is a recipient (to) on msgs 1,2,3 — searching "bob" should match those.
	stats, err := engine.GetTotalStats(context.Background(), StatsOptions{
		SearchQuery: "bob",
		GroupBy:     ViewRecipients,
	})
	if err != nil {
		t.Fatalf("GetTotalStats: %v", err)
	}
	if stats.MessageCount != 3 {
		t.Errorf("expected 3 messages for recipient search 'bob', got %d", stats.MessageCount)
	}
}

func TestDuckDBEngine_GetTotalStats_GroupByLabels(t *testing.T) {
	if runtime.GOOS == "linux" {
		t.Skip("skipping DuckDB test on Linux CI")
	}
	engine := newParquetEngine(t)

	// Search "work" with GroupBy=ViewLabels should search label key columns.
	// "Work" label is on msgs 1,4.
	stats, err := engine.GetTotalStats(context.Background(), StatsOptions{
		SearchQuery: "work",
		GroupBy:     ViewLabels,
	})
	if err != nil {
		t.Fatalf("GetTotalStats: %v", err)
	}
	if stats.MessageCount != 2 {
		t.Errorf("expected 2 messages for label search 'work', got %d", stats.MessageCount)
	}
}

func TestDuckDBEngine_GetTotalStats_GroupByDefault(t *testing.T) {
	if runtime.GOOS == "linux" {
		t.Skip("skipping DuckDB test on Linux CI")
	}
	engine := newParquetEngine(t)

	// Search "alice" with default GroupBy (senders) should search subject+sender.
	// Alice is sender on msgs 1,2,3.
	stats, err := engine.GetTotalStats(context.Background(), StatsOptions{
		SearchQuery: "alice",
	})
	if err != nil {
		t.Fatalf("GetTotalStats: %v", err)
	}
	if stats.MessageCount != 3 {
		t.Errorf("expected 3 messages for sender search 'alice', got %d", stats.MessageCount)
	}
}
