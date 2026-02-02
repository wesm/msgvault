package query

import (
	"context"
	"testing"
	"time"
)

func TestAggregations(t *testing.T) {
	env := newTestEnv(t)

	tests := []struct {
		name    string
		aggFunc func(context.Context, AggregateOptions) ([]AggregateRow, error)
		want    []aggExpectation
	}{
		{
			name:    "BySender",
			aggFunc: env.Engine.AggregateBySender,
			want:    []aggExpectation{{"alice@example.com", 3}, {"bob@company.org", 2}},
		},
		{
			name:    "BySenderName",
			aggFunc: env.Engine.AggregateBySenderName,
			want:    []aggExpectation{{"Alice Smith", 3}, {"Bob Jones", 2}},
		},
		{
			name:    "ByRecipient",
			aggFunc: env.Engine.AggregateByRecipient,
			want:    []aggExpectation{{"bob@company.org", 3}, {"alice@example.com", 2}, {"carol@example.com", 1}},
		},
		{
			name:    "ByDomain",
			aggFunc: env.Engine.AggregateByDomain,
			want:    []aggExpectation{{"example.com", 3}, {"company.org", 2}},
		},
		{
			name:    "ByLabel",
			aggFunc: env.Engine.AggregateByLabel,
			want:    []aggExpectation{{"INBOX", 5}, {"Work", 2}, {"IMPORTANT", 1}},
		},
		{
			name:    "ByRecipientName",
			aggFunc: env.Engine.AggregateByRecipientName,
			want:    []aggExpectation{{"Bob Jones", 3}, {"Alice Smith", 2}, {"Carol White", 1}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rows, err := tc.aggFunc(env.Ctx, DefaultAggregateOptions())
			if err != nil {
				t.Fatalf("Aggregate%s: %v", tc.name, err)
			}
			assertAggRows(t, rows, tc.want)
		})
	}
}

func TestAggregateBySenderName_FallbackToEmail(t *testing.T) {
	env := newTestEnv(t)

	noNameID := env.AddParticipant(participantOpts{Email: strPtr("noname@test.com"), DisplayName: nil, Domain: "test.com"})
	env.AddMessage(messageOpts{Subject: "No Name Test", SentAt: "2024-05-01 10:00:00", FromID: noNameID})

	rows, err := env.Engine.AggregateBySenderName(env.Ctx, DefaultAggregateOptions())
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

	emptyID := env.AddParticipant(participantOpts{Email: strPtr("empty@test.com"), DisplayName: strPtr(""), Domain: "test.com"})
	spacesID := env.AddParticipant(participantOpts{Email: strPtr("spaces@test.com"), DisplayName: strPtr("   "), Domain: "test.com"})
	env.AddMessage(messageOpts{Subject: "Empty Name", SentAt: "2024-05-01 10:00:00", FromID: emptyID})
	env.AddMessage(messageOpts{Subject: "Spaces Name", SentAt: "2024-05-02 10:00:00", FromID: spacesID})

	rows, err := env.Engine.AggregateBySenderName(env.Ctx, DefaultAggregateOptions())
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

	rows, err := env.Engine.AggregateByTime(env.Ctx, opts)
	if err != nil {
		t.Fatalf("AggregateByTime: %v", err)
	}

	if len(rows) != 3 {
		t.Errorf("expected 3 months, got %d", len(rows))
	}

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

	if len(rows) != 2 {
		t.Errorf("expected 2 senders after filter, got %d", len(rows))
	}

	if rows[0].Key != "bob@company.org" {
		t.Errorf("expected bob first after filter, got %s", rows[0].Key)
	}
}

func TestSortingOptions(t *testing.T) {
	env := newTestEnv(t)

	opts := DefaultAggregateOptions()
	opts.SortField = SortBySize

	rows, err := env.Engine.AggregateBySender(env.Ctx, opts)
	if err != nil {
		t.Fatalf("AggregateBySender: %v", err)
	}

	if rows[0].Key != "alice@example.com" {
		t.Errorf("expected alice first by size, got %s", rows[0].Key)
	}

	opts.SortDirection = SortAsc

	rows, err = env.Engine.AggregateBySender(env.Ctx, opts)
	if err != nil {
		t.Fatalf("AggregateBySender: %v", err)
	}

	if rows[0].Key != "bob@company.org" {
		t.Errorf("expected bob first by size asc, got %s", rows[0].Key)
	}
}

func TestWithAttachmentsOnlyAggregate(t *testing.T) {
	env := newTestEnv(t)

	opts := DefaultAggregateOptions()
	allRows, err := env.Engine.AggregateBySender(env.Ctx, opts)
	if err != nil {
		t.Fatalf("AggregateBySender: %v", err)
	}

	assertRowsContain(t, allRows, []aggExpectation{
		{"alice@example.com", 3},
		{"bob@company.org", 2},
	})

	opts.WithAttachmentsOnly = true
	attRows, err := env.Engine.AggregateBySender(env.Ctx, opts)
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

func TestSubAggregateBySender(t *testing.T) {
	env := newTestEnv(t)

	filter := MessageFilter{Recipient: "alice@example.com"}
	results, err := env.Engine.SubAggregate(env.Ctx, filter, ViewSenders, DefaultAggregateOptions())
	if err != nil {
		t.Fatalf("SubAggregate: %v", err)
	}

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

func TestSubAggregateBySenderName(t *testing.T) {
	env := newTestEnv(t)

	filter := MessageFilter{Recipient: "alice@example.com"}
	results, err := env.Engine.SubAggregate(env.Ctx, filter, ViewSenderNames, DefaultAggregateOptions())
	if err != nil {
		t.Fatalf("SubAggregate: %v", err)
	}

	if len(results) != 1 {
		t.Errorf("expected 1 sender name to alice, got %d", len(results))
	}
	if len(results) > 0 && results[0].Key != "Bob Jones" {
		t.Errorf("expected 'Bob Jones', got %q", results[0].Key)
	}
}

func TestSubAggregate_MatchEmptySenderName(t *testing.T) {
	env := newTestEnvWithEmptyBuckets(t)

	filter := MessageFilter{MatchEmptySenderName: true}
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

func TestSubAggregateByRecipient(t *testing.T) {
	env := newTestEnv(t)

	filter := MessageFilter{Sender: "alice@example.com"}
	results, err := env.Engine.SubAggregate(env.Ctx, filter, ViewRecipients, DefaultAggregateOptions())
	if err != nil {
		t.Fatalf("SubAggregate: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("expected 2 recipients for alice@example.com, got %d", len(results))
	}

	assertRow(t, results, "bob@company.org", 3)
}

func TestSubAggregateByLabel(t *testing.T) {
	env := newTestEnv(t)

	filter := MessageFilter{Sender: "alice@example.com"}
	results, err := env.Engine.SubAggregate(env.Ctx, filter, ViewLabels, DefaultAggregateOptions())
	if err != nil {
		t.Fatalf("SubAggregate: %v", err)
	}

	if len(results) != 3 {
		t.Errorf("expected 3 labels for alice@example.com's messages, got %d", len(results))
	}

	for _, r := range results {
		if r.Key == "INBOX" && r.Count != 3 {
			t.Errorf("expected INBOX count 3, got %d", r.Count)
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

	noNameID := env.AddParticipant(participantOpts{Email: strPtr("noname@test.com"), DisplayName: nil, Domain: "test.com"})
	env.AddMessage(messageOpts{Subject: "No Name Recipient", SentAt: "2024-05-01 10:00:00", FromID: 1, ToIDs: []int64{noNameID}})

	rows, err := env.Engine.AggregateByRecipientName(env.Ctx, DefaultAggregateOptions())
	if err != nil {
		t.Fatalf("AggregateByRecipientName: %v", err)
	}

	assertRow(t, rows, "noname@test.com", 1)
}

func TestAggregateByRecipientName_EmptyStringFallback(t *testing.T) {
	env := newTestEnv(t)

	emptyID := env.AddParticipant(participantOpts{Email: strPtr("empty@test.com"), DisplayName: strPtr(""), Domain: "test.com"})
	spacesID := env.AddParticipant(participantOpts{Email: strPtr("spaces@test.com"), DisplayName: strPtr("   "), Domain: "test.com"})
	env.AddMessage(messageOpts{Subject: "Empty Rcpt Name", SentAt: "2024-05-01 10:00:00", FromID: 1, ToIDs: []int64{emptyID}})
	env.AddMessage(messageOpts{Subject: "Spaces Rcpt Name", SentAt: "2024-05-02 10:00:00", FromID: 1, CcIDs: []int64{spacesID}})

	rows, err := env.Engine.AggregateByRecipientName(env.Ctx, DefaultAggregateOptions())
	if err != nil {
		t.Fatalf("AggregateByRecipientName: %v", err)
	}

	assertRowsContain(t, rows, []aggExpectation{
		{"empty@test.com", 1},
		{"spaces@test.com", 1},
	})
}

func TestSubAggregateByRecipientName(t *testing.T) {
	env := newTestEnv(t)

	filter := MessageFilter{Sender: "alice@example.com"}
	results, err := env.Engine.SubAggregate(env.Ctx, filter, ViewRecipientNames, DefaultAggregateOptions())
	if err != nil {
		t.Fatalf("SubAggregate: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("expected 2 recipient names from alice, got %d", len(results))
		for _, r := range results {
			t.Logf("  key=%q count=%d", r.Key, r.Count)
		}
	}
}

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

	if len(rows) != 1 {
		t.Errorf("expected 1 sender for Bob Jones, got %d", len(rows))
	}
	if len(rows) > 0 && rows[0].Key != "alice@example.com" {
		t.Errorf("expected sender alice@example.com, got %s", rows[0].Key)
	}
}
