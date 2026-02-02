package query

import (
	"testing"
	"time"
)

func TestAggregateBySender(t *testing.T) {
	env := newTestEnv(t)

	rows, err := env.Engine.AggregateBySender(env.Ctx, DefaultAggregateOptions())
	if err != nil {
		t.Fatalf("AggregateBySender: %v", err)
	}

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

	assertAggRows(t, rows, []aggExpectation{
		{"Alice Smith", 3},
		{"Bob Jones", 2},
	})
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

	foundEmpty := false
	foundSpaces := false
	for _, r := range rows {
		if r.Key == "empty@test.com" {
			foundEmpty = true
		}
		if r.Key == "spaces@test.com" {
			foundSpaces = true
		}
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

	opts.WithAttachmentsOnly = true
	attRows, err := env.Engine.AggregateBySender(env.Ctx, opts)
	if err != nil {
		t.Fatalf("AggregateBySender with attachment filter: %v", err)
	}

	var aliceAtt, bobAtt int64
	for _, row := range attRows {
		if row.Key == "alice@example.com" {
			aliceAtt = row.Count
		}
		if row.Key == "bob@company.org" {
			bobAtt = row.Count
		}
	}

	if aliceAtt != 1 {
		t.Errorf("expected Alice attachment count 1, got %d", aliceAtt)
	}
	if bobAtt != 1 {
		t.Errorf("expected Bob attachment count 1, got %d", bobAtt)
	}
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

func TestAggregateByRecipientName(t *testing.T) {
	env := newTestEnv(t)

	rows, err := env.Engine.AggregateByRecipientName(env.Ctx, DefaultAggregateOptions())
	if err != nil {
		t.Fatalf("AggregateByRecipientName: %v", err)
	}

	assertAggRows(t, rows, []aggExpectation{
		{"Bob Jones", 3},
		{"Alice Smith", 2},
		{"Carol White", 1},
	})
}

func TestAggregateByRecipientName_FallbackToEmail(t *testing.T) {
	env := newTestEnv(t)

	noNameID := env.AddParticipant(participantOpts{Email: strPtr("noname@test.com"), DisplayName: nil, Domain: "test.com"})
	env.AddMessage(messageOpts{Subject: "No Name Recipient", SentAt: "2024-05-01 10:00:00", FromID: 1, ToIDs: []int64{noNameID}})

	rows, err := env.Engine.AggregateByRecipientName(env.Ctx, DefaultAggregateOptions())
	if err != nil {
		t.Fatalf("AggregateByRecipientName: %v", err)
	}

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

	emptyID := env.AddParticipant(participantOpts{Email: strPtr("empty@test.com"), DisplayName: strPtr(""), Domain: "test.com"})
	spacesID := env.AddParticipant(participantOpts{Email: strPtr("spaces@test.com"), DisplayName: strPtr("   "), Domain: "test.com"})
	env.AddMessage(messageOpts{Subject: "Empty Rcpt Name", SentAt: "2024-05-01 10:00:00", FromID: 1, ToIDs: []int64{emptyID}})
	env.AddMessage(messageOpts{Subject: "Spaces Rcpt Name", SentAt: "2024-05-02 10:00:00", FromID: 1, CcIDs: []int64{spacesID}})

	rows, err := env.Engine.AggregateByRecipientName(env.Ctx, DefaultAggregateOptions())
	if err != nil {
		t.Fatalf("AggregateByRecipientName: %v", err)
	}

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
