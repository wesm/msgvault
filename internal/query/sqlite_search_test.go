package query

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/wesm/msgvault/internal/search"
)

func TestSearch_WithoutFTS(t *testing.T) {
	env := newTestEnv(t)

	q := &search.Query{TextTerms: []string{"Hello"}}
	results := env.MustSearch(q, 100, 0)

	if len(results) != 2 {
		t.Errorf("expected 2 messages with 'Hello' in subject, got %d", len(results))
	}
}

func TestSearch_FromFilter(t *testing.T) {
	env := newTestEnv(t)

	q := &search.Query{FromAddrs: []string{"alice@example.com"}}
	results := env.MustSearch(q, 100, 0)

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

	q := &search.Query{Labels: []string{"Work"}}
	results := env.MustSearch(q, 100, 0)

	if len(results) != 2 {
		t.Errorf("expected 2 messages with Work label, got %d", len(results))
	}
}

func TestSearch_DateRangeFilter(t *testing.T) {
	env := newTestEnv(t)

	after := time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)
	before := time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC)

	q := &search.Query{AfterDate: &after, BeforeDate: &before}
	results := env.MustSearch(q, 100, 0)

	if len(results) != 2 {
		t.Errorf("expected 2 messages in Feb 2024, got %d", len(results))
	}
}

func TestSearch_HasAttachment(t *testing.T) {
	env := newTestEnv(t)

	hasAtt := true
	q := &search.Query{HasAttachment: &hasAtt}
	results := env.MustSearch(q, 100, 0)

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

	q := &search.Query{
		FromAddrs: []string{"alice@example.com"},
		Labels:    []string{"Work"},
	}
	results := env.MustSearch(q, 100, 0)

	if len(results) != 1 {
		t.Errorf("expected 1 message from alice with Work label, got %d", len(results))
	}
}

func TestSearch_SizeFilter(t *testing.T) {
	env := newTestEnv(t)

	largerThan := int64(2500)
	q := &search.Query{LargerThan: &largerThan}
	results := env.MustSearch(q, 100, 0)

	if len(results) != 1 {
		t.Errorf("expected 1 message larger than 2500, got %d", len(results))
	}

	if results[0].SizeEstimate <= largerThan {
		t.Errorf("expected message size > %d, got %d", largerThan, results[0].SizeEstimate)
	}
}

func TestSearch_EmptyQuery(t *testing.T) {
	env := newTestEnv(t)

	q := &search.Query{}
	results := env.MustSearch(q, 100, 0)

	if len(results) != 5 {
		t.Errorf("expected 5 messages with empty query, got %d", len(results))
	}
}

func TestSearch_CaseInsensitiveFallback(t *testing.T) {
	env := newTestEnv(t)

	if env.Engine.hasFTSTable(env.Ctx) {
		t.Skip("FTS is available; this test covers the non-FTS fallback path")
	}

	q := &search.Query{TextTerms: []string{"hello"}}
	results := env.MustSearch(q, 100, 0)

	if len(results) != 2 {
		t.Errorf("expected 2 messages with 'hello' (case-insensitive), got %d", len(results))
	}

	q = &search.Query{TextTerms: []string{"WORLD"}}
	results = env.MustSearch(q, 100, 0)

	if len(results) != 1 {
		t.Errorf("expected 1 message with 'WORLD' (case-insensitive), got %d", len(results))
	}

	if len(results) > 0 && results[0].Subject != "Hello World" {
		t.Errorf("expected 'Hello World', got %q", results[0].Subject)
	}
}

func TestSearch_SubjectTermsCaseInsensitive(t *testing.T) {
	env := newTestEnv(t)

	if env.Engine.hasFTSTable(env.Ctx) {
		t.Skip("FTS is available; this test covers the non-FTS fallback path")
	}

	q := &search.Query{SubjectTerms: []string{"HELLO"}}
	results := env.MustSearch(q, 100, 0)

	if len(results) != 2 {
		t.Errorf("expected 2 messages with 'HELLO' in subject (case-insensitive), got %d", len(results))
	}
}

func TestSearch_WithFTS(t *testing.T) {
	env := newTestEnv(t)
	env.EnableFTS()

	q := &search.Query{TextTerms: []string{"World"}}
	results := env.MustSearch(q, 100, 0)

	if len(results) != 1 {
		t.Errorf("expected 1 message with 'World', got %d", len(results))
	}

	if results[0].Subject != "Hello World" {
		t.Errorf("expected subject 'Hello World', got %s", results[0].Subject)
	}
}

func TestHasFTSTable(t *testing.T) {
	env := newTestEnv(t)

	if env.Engine.hasFTSTable(env.Ctx) {
		t.Error("expected hasFTSTable to return false for test DB without FTS")
	}

	_, err := env.DB.Exec(`
		CREATE VIRTUAL TABLE messages_fts USING fts5(subject, snippet);
	`)
	if err != nil {
		t.Skipf("FTS5 not available in this SQLite build: %v", err)
	}

	engine2 := NewSQLiteEngine(env.DB)

	if !engine2.hasFTSTable(env.Ctx) {
		t.Error("expected hasFTSTable to return true after creating FTS table")
	}
}

func TestHasFTSTable_ErrorDoesNotCache(t *testing.T) {
	env := newTestEnv(t)

	_, err := env.DB.Exec(`
		CREATE VIRTUAL TABLE messages_fts USING fts5(subject, snippet);
	`)
	if err != nil {
		t.Skipf("FTS5 not available, cannot verify error-does-not-cache behavior: %v", err)
	}

	env.Engine = NewSQLiteEngine(env.DB)

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	firstResult := env.Engine.hasFTSTable(canceledCtx)

	if firstResult {
		t.Skip("SQLite driver does not error on canceled context; cannot test error-retry behavior")
	}

	validCtx := context.Background()
	secondResult := env.Engine.hasFTSTable(validCtx)

	if !secondResult {
		t.Error("hasFTSTable retry returned false, but FTS is available; error was incorrectly cached")
	}

	thirdResult := env.Engine.hasFTSTable(validCtx)
	if !thirdResult {
		t.Error("hasFTSTable cached result is false, expected true")
	}
}

func TestSearchWithDomainFilter(t *testing.T) {
	env := newTestEnv(t)

	q := &search.Query{FromAddrs: []string{"@example.com"}}
	results := env.MustSearch(q, 100, 0)

	if len(results) == 0 {
		t.Error("Expected results for @example.com domain search, got none")
	}

	for _, r := range results {
		if r.FromEmail != "" && !strings.HasSuffix(r.FromEmail, "@example.com") {
			t.Errorf("Result from non-matching domain: %s", r.FromEmail)
		}
	}
}

func TestSearchMixedExactAndDomainFilter(t *testing.T) {
	env := newTestEnv(t)

	q := &search.Query{FromAddrs: []string{"alice@example.com", "@other.com"}}
	results := env.MustSearch(q, 100, 0)

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
			results, err := env.Engine.Search(env.Ctx, tc.query, 1000, 0)
			if err != nil {
				t.Fatalf("Search: %v", err)
			}

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
// MergeFilterIntoQuery tests
// =============================================================================

func TestMergeFilterIntoQuery_EmptyFilter(t *testing.T) {
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
	q := &search.Query{FromAddrs: []string{"alice@example.com"}}
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
	q := &search.Query{ToAddrs: []string{"recipient1@example.com"}}
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
	q := &search.Query{Labels: []string{"inbox"}}
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
	q := &search.Query{}
	filter := MessageFilter{Domain: "example.com"}

	merged := MergeFilterIntoQuery(q, filter)

	if len(merged.FromAddrs) != 1 || merged.FromAddrs[0] != "@example.com" {
		t.Errorf("FromAddrs: got %v, want [@example.com]", merged.FromAddrs)
	}
}

func TestMergeFilterIntoQuery_MultipleFilters(t *testing.T) {
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

	if len(merged.TextTerms) != 2 || merged.TextTerms[0] != "search" || merged.TextTerms[1] != "term" {
		t.Errorf("TextTerms: got %v, want [search term]", merged.TextTerms)
	}
	if merged.AccountID == nil || *merged.AccountID != 1 {
		t.Errorf("AccountID: got %v, want 1", merged.AccountID)
	}
	if len(merged.FromAddrs) != 3 {
		t.Fatalf("FromAddrs: got %d items, want 3", len(merged.FromAddrs))
	}
	if len(merged.ToAddrs) != 1 || merged.ToAddrs[0] != "carol@example.com" {
		t.Errorf("ToAddrs: got %v, want [carol@example.com]", merged.ToAddrs)
	}
	if len(merged.Labels) != 1 || merged.Labels[0] != "starred" {
		t.Errorf("Labels: got %v, want [starred]", merged.Labels)
	}
	if merged.HasAttachment == nil || !*merged.HasAttachment {
		t.Errorf("HasAttachment: got %v, want true", merged.HasAttachment)
	}
}

func TestMergeFilterIntoQuery_DoesNotMutateOriginal(t *testing.T) {
	q := &search.Query{FromAddrs: []string{"original@example.com"}}
	filter := MessageFilter{Sender: "added@example.com"}

	_ = MergeFilterIntoQuery(q, filter)

	if len(q.FromAddrs) != 1 || q.FromAddrs[0] != "original@example.com" {
		t.Errorf("Original query was mutated: FromAddrs=%v", q.FromAddrs)
	}
}

func TestMergeFilterIntoQuery_SliceAliasingMutation(t *testing.T) {
	backing := make([]string, 1, 10)
	backing[0] = "original@example.com"

	q := &search.Query{FromAddrs: backing[:1]}
	filter := MessageFilter{Sender: "added@example.com"}

	merged := MergeFilterIntoQuery(q, filter)

	if len(merged.FromAddrs) != 2 {
		t.Fatalf("Merged FromAddrs: got %d items, want 2", len(merged.FromAddrs))
	}

	if len(q.FromAddrs) != 1 {
		t.Errorf("Original query was mutated via slice aliasing: FromAddrs=%v", q.FromAddrs)
	}
	if q.FromAddrs[0] != "original@example.com" {
		t.Errorf("Original FromAddrs[0] was changed: got %q, want original@example.com", q.FromAddrs[0])
	}
}
