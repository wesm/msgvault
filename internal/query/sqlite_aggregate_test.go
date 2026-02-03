package query

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/wesm/msgvault/internal/testutil/dbtest"
)

func TestAggregations(t *testing.T) {
	type testCase struct {
		name    string
		aggName string
		view    ViewType
		want    []aggExpectation
	}

	tests := []testCase{
		{
			name:    "BySender",
			aggName: "AggregateBySender",
			view:    ViewSenders,
			want:    []aggExpectation{{"alice@example.com", 3}, {"bob@company.org", 2}},
		},
		{
			name:    "BySenderName",
			aggName: "AggregateBySenderName",
			view:    ViewSenderNames,
			want:    []aggExpectation{{"Alice Smith", 3}, {"Bob Jones", 2}},
		},
		{
			name:    "ByRecipient",
			aggName: "AggregateByRecipient",
			view:    ViewRecipients,
			want:    []aggExpectation{{"bob@company.org", 3}, {"alice@example.com", 2}, {"carol@example.com", 1}},
		},
		{
			name:    "ByDomain",
			aggName: "AggregateByDomain",
			view:    ViewDomains,
			want:    []aggExpectation{{"example.com", 3}, {"company.org", 2}},
		},
		{
			name:    "ByLabel",
			aggName: "AggregateByLabel",
			view:    ViewLabels,
			want:    []aggExpectation{{"INBOX", 5}, {"Work", 2}, {"IMPORTANT", 1}},
		},
		{
			name:    "ByRecipientName",
			aggName: "AggregateByRecipientName",
			view:    ViewRecipientNames,
			want:    []aggExpectation{{"Bob Jones", 3}, {"Alice Smith", 2}, {"Carol White", 1}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			env := newTestEnv(t)
			rows, err := env.Engine.Aggregate(env.Ctx, tc.view, DefaultAggregateOptions())
			if err != nil {
				t.Fatalf("%s: %v", tc.aggName, err)
			}
			assertAggRows(t, rows, tc.want)
		})
	}
}

func TestAggregateBySenderName_FallbackToEmail(t *testing.T) {
	env := newTestEnv(t)

	noNameID := env.AddParticipant(dbtest.ParticipantOpts{Email: dbtest.StrPtr("noname@test.com"), DisplayName: nil, Domain: "test.com"})
	env.AddMessage(dbtest.MessageOpts{Subject: "No Name Test", SentAt: "2024-05-01 10:00:00", FromID: noNameID})

	rows, err := env.Engine.Aggregate(env.Ctx, ViewSenderNames, DefaultAggregateOptions())
	if err != nil {
		t.Fatalf("AggregateBySenderName: %v", err)
	}

	if len(rows) != 3 {
		t.Errorf("expected 3 sender names, got %d", len(rows))
	}

	assertRow(t, rows, "noname@test.com", 1)
}

func TestAggregateBySenderName_EmptyStringFallback(t *testing.T) {
	env := newTestEnv(t)

	emptyID := env.AddParticipant(dbtest.ParticipantOpts{Email: dbtest.StrPtr("empty@test.com"), DisplayName: dbtest.StrPtr(""), Domain: "test.com"})
	spacesID := env.AddParticipant(dbtest.ParticipantOpts{Email: dbtest.StrPtr("spaces@test.com"), DisplayName: dbtest.StrPtr("   "), Domain: "test.com"})
	env.AddMessage(dbtest.MessageOpts{Subject: "Empty Name", SentAt: "2024-05-01 10:00:00", FromID: emptyID})
	env.AddMessage(dbtest.MessageOpts{Subject: "Spaces Name", SentAt: "2024-05-02 10:00:00", FromID: spacesID})

	rows, err := env.Engine.Aggregate(env.Ctx, ViewSenderNames, DefaultAggregateOptions())
	if err != nil {
		t.Fatalf("AggregateBySenderName: %v", err)
	}

	if len(rows) != 4 {
		t.Errorf("expected 4 sender names, got %d", len(rows))
		for _, r := range rows {
			t.Logf("  key=%q count=%d", r.Key, r.Count)
		}
	}

	for _, r := range rows {
		if r.Key == "" || r.Key == "   " {
			t.Errorf("unexpected empty/whitespace key: %q", r.Key)
		}
	}
	assertRowsContain(t, rows, []aggExpectation{
		{"empty@test.com", 1},
		{"spaces@test.com", 1},
	})
}

func TestAggregateByTime(t *testing.T) {
	env := newTestEnv(t)

	opts := DefaultAggregateOptions()
	opts.TimeGranularity = TimeMonth

	rows, err := env.Engine.Aggregate(env.Ctx, ViewTime, opts)
	if err != nil {
		t.Fatalf("AggregateByTime: %v", err)
	}

	assertAggRows(t, rows, []aggExpectation{
		{"2024-01", 2},
		{"2024-02", 2},
		{"2024-03", 1},
	})
}

func TestAggregateWithDateFilter(t *testing.T) {
	env := newTestEnv(t)

	opts := DefaultAggregateOptions()
	after := time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)
	opts.After = &after

	rows, err := env.Engine.Aggregate(env.Ctx, ViewSenders, opts)
	if err != nil {
		t.Fatalf("AggregateBySender with date filter: %v", err)
	}

	assertAggRows(t, rows, []aggExpectation{
		{"bob@company.org", 2},
		{"alice@example.com", 1},
	})
}

func TestSortingOptions(t *testing.T) {
	env := newTestEnv(t)

	t.Run("SortBySizeDesc", func(t *testing.T) {
		opts := DefaultAggregateOptions()
		opts.SortField = SortBySize
		rows, err := env.Engine.Aggregate(env.Ctx, ViewSenders, opts)
		if err != nil {
			t.Fatalf("AggregateBySender: %v", err)
		}
		assertAggRows(t, rows, []aggExpectation{
			{"alice@example.com", 3},
			{"bob@company.org", 2},
		})
	})

	t.Run("SortBySizeAsc", func(t *testing.T) {
		opts := DefaultAggregateOptions()
		opts.SortField = SortBySize
		opts.SortDirection = SortAsc
		rows, err := env.Engine.Aggregate(env.Ctx, ViewSenders, opts)
		if err != nil {
			t.Fatalf("AggregateBySender: %v", err)
		}
		assertAggRows(t, rows, []aggExpectation{
			{"bob@company.org", 2},
			{"alice@example.com", 3},
		})
	})
}

func TestWithAttachmentsOnlyAggregate(t *testing.T) {
	env := newTestEnv(t)

	opts := DefaultAggregateOptions()
	allRows, err := env.Engine.Aggregate(env.Ctx, ViewSenders, opts)
	if err != nil {
		t.Fatalf("AggregateBySender: %v", err)
	}

	assertRowsContain(t, allRows, []aggExpectation{
		{"alice@example.com", 3},
		{"bob@company.org", 2},
	})

	opts.WithAttachmentsOnly = true
	attRows, err := env.Engine.Aggregate(env.Ctx, ViewSenders, opts)
	if err != nil {
		t.Fatalf("AggregateBySender with attachment filter: %v", err)
	}

	assertRowsContain(t, attRows, []aggExpectation{
		{"alice@example.com", 1},
		{"bob@company.org", 1},
	})
}

// =============================================================================
// SubAggregate tests
// =============================================================================

func TestSubAggregates(t *testing.T) {
	tests := []struct {
		name   string
		filter MessageFilter
		view   ViewType
		want   []aggExpectation
	}{
		{
			name:   "BySender",
			filter: MessageFilter{Recipient: "alice@example.com"},
			view:   ViewSenders,
			want:   []aggExpectation{{"bob@company.org", 2}},
		},
		{
			name:   "BySenderName",
			filter: MessageFilter{Recipient: "alice@example.com"},
			view:   ViewSenderNames,
			want:   []aggExpectation{{"Bob Jones", 2}},
		},
		{
			name:   "ByRecipient",
			filter: MessageFilter{Sender: "alice@example.com"},
			view:   ViewRecipients,
			want:   []aggExpectation{{"bob@company.org", 3}, {"carol@example.com", 1}},
		},
		{
			name:   "ByLabel",
			filter: MessageFilter{Sender: "alice@example.com"},
			view:   ViewLabels,
			want:   []aggExpectation{{"INBOX", 3}, {"IMPORTANT", 1}, {"Work", 1}},
		},
		{
			name:   "ByRecipientName",
			filter: MessageFilter{Sender: "alice@example.com"},
			view:   ViewRecipientNames,
			want:   []aggExpectation{{"Bob Jones", 3}, {"Carol White", 1}},
		},
		{
			name:   "RecipientNameWithRecipient",
			filter: MessageFilter{Recipient: "bob@company.org", RecipientName: "Bob Jones"},
			view:   ViewSenders,
			want:   []aggExpectation{{"alice@example.com", 3}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			env := newTestEnv(t)
			results, err := env.Engine.SubAggregate(env.Ctx, tc.filter, tc.view, DefaultAggregateOptions())
			if err != nil {
				t.Fatalf("SubAggregate: %v", err)
			}
			assertAggRows(t, results, tc.want)
		})
	}
}

func TestSubAggregate_MatchEmptySenderName(t *testing.T) {
	env := newTestEnvWithEmptyBuckets(t)

	filter := MessageFilter{EmptyValueTargets: map[ViewType]bool{ViewSenderNames: true}}
	results, err := env.Engine.SubAggregate(env.Ctx, filter, ViewLabels, DefaultAggregateOptions())
	if err != nil {
		t.Fatalf("SubAggregate with MatchEmptySenderName: %v", err)
	}

	if len(results) != 0 {
		t.Errorf("expected 0 label sub-aggregates for empty sender name, got %d", len(results))
		for _, r := range results {
			t.Logf("  key=%q count=%d", r.Key, r.Count)
		}
	}
}

func TestSubAggregateIncludesDeletedMessages(t *testing.T) {
	env := newTestEnv(t)

	filter := MessageFilter{Sender: "alice@example.com"}
	resultsBefore, err := env.Engine.SubAggregate(env.Ctx, filter, ViewRecipients, DefaultAggregateOptions())
	if err != nil {
		t.Fatalf("SubAggregate before: %v", err)
	}

	env.MarkDeletedByID(1)

	resultsAfter, err := env.Engine.SubAggregate(env.Ctx, filter, ViewRecipients, DefaultAggregateOptions())
	if err != nil {
		t.Fatalf("SubAggregate after: %v", err)
	}

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

	filter := MessageFilter{Sender: "alice@example.com"}
	opts := DefaultAggregateOptions()
	opts.TimeGranularity = TimeMonth

	results, err := env.Engine.SubAggregate(env.Ctx, filter, ViewTime, opts)
	if err != nil {
		t.Fatalf("SubAggregate: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("expected 2 time periods for alice@example.com's messages, got %d", len(results))
	}

	for _, r := range results {
		if len(r.Key) != 7 || r.Key[4] != '-' {
			t.Errorf("expected YYYY-MM format, got %q", r.Key)
		}
	}
}

// =============================================================================
// RecipientName aggregate tests
// =============================================================================

func TestAggregateByRecipientName_FallbackToEmail(t *testing.T) {
	env := newTestEnv(t)

	// Resolve participant IDs dynamically to avoid coupling to seed order.
	aliceID := env.MustLookupParticipant("alice@example.com")

	noNameID := env.AddParticipant(dbtest.ParticipantOpts{Email: dbtest.StrPtr("noname@test.com"), DisplayName: nil, Domain: "test.com"})
	env.AddMessage(dbtest.MessageOpts{Subject: "No Name Recipient", SentAt: "2024-05-01 10:00:00", FromID: aliceID, ToIDs: []int64{noNameID}})

	rows, err := env.Engine.Aggregate(env.Ctx, ViewRecipientNames, DefaultAggregateOptions())
	if err != nil {
		t.Fatalf("AggregateByRecipientName: %v", err)
	}

	assertRow(t, rows, "noname@test.com", 1)
}

func TestAggregateByRecipientName_EmptyStringFallback(t *testing.T) {
	env := newTestEnv(t)

	// Resolve participant IDs dynamically to avoid coupling to seed order.
	aliceID := env.MustLookupParticipant("alice@example.com")

	emptyID := env.AddParticipant(dbtest.ParticipantOpts{Email: dbtest.StrPtr("empty@test.com"), DisplayName: dbtest.StrPtr(""), Domain: "test.com"})
	spacesID := env.AddParticipant(dbtest.ParticipantOpts{Email: dbtest.StrPtr("spaces@test.com"), DisplayName: dbtest.StrPtr("   "), Domain: "test.com"})
	env.AddMessage(dbtest.MessageOpts{Subject: "Empty Rcpt Name", SentAt: "2024-05-01 10:00:00", FromID: aliceID, ToIDs: []int64{emptyID}})
	env.AddMessage(dbtest.MessageOpts{Subject: "Spaces Rcpt Name", SentAt: "2024-05-02 10:00:00", FromID: aliceID, CcIDs: []int64{spacesID}})

	rows, err := env.Engine.Aggregate(env.Ctx, ViewRecipientNames, DefaultAggregateOptions())
	if err != nil {
		t.Fatalf("AggregateByRecipientName: %v", err)
	}

	assertRowsContain(t, rows, []aggExpectation{
		{"empty@test.com", 1},
		{"spaces@test.com", 1},
	})
}

// =============================================================================
// Invalid ViewType tests
// =============================================================================

// TestSQLiteEngine_Aggregate_InvalidViewType verifies that invalid ViewType values
// return a clear error from the Aggregate API.
func TestSQLiteEngine_Aggregate_InvalidViewType(t *testing.T) {
	env := newTestEnv(t)

	tests := []struct {
		name     string
		viewType ViewType
	}{
		{name: "ViewTypeCount", viewType: ViewTypeCount},
		{name: "NegativeValue", viewType: ViewType(-1)},
		{name: "LargeValue", viewType: ViewType(999)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := env.Engine.Aggregate(env.Ctx, tt.viewType, DefaultAggregateOptions())
			if err == nil {
				t.Fatal("expected error for invalid ViewType, got nil")
			}
			if !strings.Contains(err.Error(), "unsupported view type") {
				t.Errorf("expected 'unsupported view type' error, got: %v", err)
			}
		})
	}
}

// TestAggregateDeterministicOrderOnTies verifies that when aggregate values tie
// (e.g., two labels with equal counts), results are sorted deterministically by key ASC.
// This prevents flaky tests and non-deterministic UI ordering.
func TestAggregateDeterministicOrderOnTies(t *testing.T) {
	tdb := dbtest.NewTestDB(t, "../store/schema.sql")

	// Create minimal test data using helpers, explicitly threading IDs to avoid
	// implicit coupling to helper defaults or auto-increment assumptions.
	sourceID := tdb.AddSource(dbtest.SourceOpts{Identifier: "test@gmail.com", DisplayName: "Test Account"})
	convID := tdb.AddConversation(dbtest.ConversationOpts{SourceID: sourceID, Title: "Test Thread"})
	aliceID := tdb.AddParticipant(dbtest.ParticipantOpts{Email: dbtest.StrPtr("alice@example.com"), DisplayName: dbtest.StrPtr("Alice"), Domain: "example.com"})
	bobID := tdb.AddParticipant(dbtest.ParticipantOpts{Email: dbtest.StrPtr("bob@example.com"), DisplayName: dbtest.StrPtr("Bob"), Domain: "example.com"})

	// Create labels with names that would sort differently than insertion order
	// "Zebra" inserted first, "Apple" inserted second - both will have count=1
	zebraID := tdb.AddLabel(dbtest.LabelOpts{Name: "Zebra"})
	appleID := tdb.AddLabel(dbtest.LabelOpts{Name: "Apple"})

	// Add one message with both labels
	msgID := tdb.AddMessage(dbtest.MessageOpts{
		Subject:        "Test",
		SentAt:         "2024-01-01 10:00:00",
		FromID:         aliceID,
		ToIDs:          []int64{bobID},
		SourceID:       sourceID,
		ConversationID: convID,
	})
	tdb.AddMessageLabel(msgID, zebraID)
	tdb.AddMessageLabel(msgID, appleID)

	env := &testEnv{
		TestDB: tdb,
		Engine: NewSQLiteEngine(tdb.DB),
		Ctx:    context.Background(),
	}

	// Default sort is by count DESC. Both labels have count=1, so they should
	// be ordered by key ASC as secondary sort: Apple before Zebra.
	opts := DefaultAggregateOptions()
	rows, err := env.Engine.Aggregate(env.Ctx, ViewLabels, opts)
	if err != nil {
		t.Fatalf("Aggregate: %v", err)
	}

	// Verify exact order: Apple (count=1) then Zebra (count=1)
	assertAggRows(t, rows, []aggExpectation{
		{"Apple", 1},
		{"Zebra", 1},
	})
}

// TestSQLiteEngine_SubAggregate_InvalidViewType verifies that invalid ViewType values
// return a clear error from the SubAggregate API.
func TestSQLiteEngine_SubAggregate_InvalidViewType(t *testing.T) {
	env := newTestEnv(t)

	tests := []struct {
		name     string
		viewType ViewType
	}{
		{name: "ViewTypeCount", viewType: ViewTypeCount},
		{name: "NegativeValue", viewType: ViewType(-1)},
		{name: "LargeValue", viewType: ViewType(999)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter := MessageFilter{Sender: "alice@example.com"}
			_, err := env.Engine.SubAggregate(env.Ctx, filter, tt.viewType, DefaultAggregateOptions())
			if err == nil {
				t.Fatal("expected error for invalid ViewType, got nil")
			}
			if !strings.Contains(err.Error(), "unsupported view type") {
				t.Errorf("expected 'unsupported view type' error, got: %v", err)
			}
		})
	}
}
