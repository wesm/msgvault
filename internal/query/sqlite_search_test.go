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
	assertSearchCount(t, env, q, 2)
}

func TestSearch_FromFilter(t *testing.T) {
	env := newTestEnv(t)
	q := &search.Query{FromAddrs: []string{"alice@example.com"}}
	results := assertSearchCount(t, env, q, 3)
	assertAllResults(t, results, "FromEmail=alice@example.com", func(m MessageSummary) bool {
		return m.FromEmail == "alice@example.com"
	})
}

func TestSearch_LabelFilter(t *testing.T) {
	env := newTestEnv(t)
	q := &search.Query{Labels: []string{"Work"}}
	assertSearchCount(t, env, q, 2)
}

func TestSearch_DateRangeFilter(t *testing.T) {
	env := newTestEnv(t)
	after := time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)
	before := time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC)
	q := &search.Query{AfterDate: &after, BeforeDate: &before}
	assertSearchCount(t, env, q, 2)
}

func TestSearch_HasAttachment(t *testing.T) {
	env := newTestEnv(t)
	hasAtt := true
	q := &search.Query{HasAttachment: &hasAtt}
	results := assertSearchCount(t, env, q, 2)
	assertAllResults(t, results, "HasAttachments=true", func(m MessageSummary) bool {
		return m.HasAttachments
	})
}

func TestSearch_CombinedFilters(t *testing.T) {
	env := newTestEnv(t)
	q := &search.Query{
		FromAddrs: []string{"alice@example.com"},
		Labels:    []string{"Work"},
	}
	assertSearchCount(t, env, q, 1)
}

func TestSearch_SizeFilter(t *testing.T) {
	env := newTestEnv(t)
	largerThan := int64(2500)
	q := &search.Query{LargerThan: &largerThan}
	results := assertSearchCount(t, env, q, 1)
	assertAllResults(t, results, "SizeEstimate>2500", func(m MessageSummary) bool {
		return m.SizeEstimate > largerThan
	})
}

func TestSearch_EmptyQuery(t *testing.T) {
	env := newTestEnv(t)
	q := &search.Query{}
	assertSearchCount(t, env, q, 5)
}

func TestSearch_CaseInsensitiveFallback(t *testing.T) {
	env := newTestEnv(t)

	if env.Engine.hasFTSTable(env.Ctx) {
		t.Skip("FTS is available; this test covers the non-FTS fallback path")
	}

	q := &search.Query{TextTerms: []string{"hello"}}
	assertSearchCount(t, env, q, 2)

	q = &search.Query{TextTerms: []string{"WORLD"}}
	results := assertSearchCount(t, env, q, 1)

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
	assertSearchCount(t, env, q, 2)
}

func TestSearch_WithFTS(t *testing.T) {
	env := newTestEnv(t)
	env.EnableFTS()

	q := &search.Query{TextTerms: []string{"World"}}
	results := assertSearchCount(t, env, q, 1)

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
	results := assertSearchCount(t, env, q, 3)
	assertAllResults(t, results, "FromEmail ends with @example.com", func(m MessageSummary) bool {
		return m.FromEmail == "" || strings.HasSuffix(m.FromEmail, "@example.com")
	})
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

func TestMergeFilterIntoQuery(t *testing.T) {
	sourceID42 := int64(42)
	sourceID1 := int64(1)

	tests := []struct {
		name    string
		initial *search.Query
		filter  MessageFilter
		check   func(*testing.T, *search.Query)
	}{
		{
			name: "EmptyFilter",
			initial: &search.Query{
				TextTerms: []string{"test", "query"},
				FromAddrs: []string{"alice@example.com"},
				Labels:    []string{"inbox"},
			},
			filter: MessageFilter{},
			check: func(t *testing.T, q *search.Query) {
				if len(q.TextTerms) != 2 || q.TextTerms[0] != "test" || q.TextTerms[1] != "query" {
					t.Errorf("TextTerms: got %v, want [test query]", q.TextTerms)
				}
				if len(q.FromAddrs) != 1 || q.FromAddrs[0] != "alice@example.com" {
					t.Errorf("FromAddrs: got %v, want [alice@example.com]", q.FromAddrs)
				}
				if len(q.Labels) != 1 || q.Labels[0] != "inbox" {
					t.Errorf("Labels: got %v, want [inbox]", q.Labels)
				}
			},
		},
		{
			name:    "SourceID",
			initial: &search.Query{},
			filter:  MessageFilter{SourceID: &sourceID42},
			check: func(t *testing.T, q *search.Query) {
				if q.AccountID == nil || *q.AccountID != 42 {
					t.Errorf("AccountID: got %v, want 42", q.AccountID)
				}
			},
		},
		{
			name:    "SenderAppends",
			initial: &search.Query{FromAddrs: []string{"alice@example.com"}},
			filter:  MessageFilter{Sender: "bob@example.com"},
			check: func(t *testing.T, q *search.Query) {
				if len(q.FromAddrs) != 2 {
					t.Fatalf("FromAddrs: got %d items, want 2", len(q.FromAddrs))
				}
				if q.FromAddrs[0] != "alice@example.com" || q.FromAddrs[1] != "bob@example.com" {
					t.Errorf("FromAddrs: got %v, want [alice@example.com bob@example.com]", q.FromAddrs)
				}
			},
		},
		{
			name:    "RecipientAppends",
			initial: &search.Query{ToAddrs: []string{"recipient1@example.com"}},
			filter:  MessageFilter{Recipient: "recipient2@example.com"},
			check: func(t *testing.T, q *search.Query) {
				if len(q.ToAddrs) != 2 {
					t.Fatalf("ToAddrs: got %d items, want 2", len(q.ToAddrs))
				}
				if q.ToAddrs[0] != "recipient1@example.com" || q.ToAddrs[1] != "recipient2@example.com" {
					t.Errorf("ToAddrs: got %v, want [recipient1@example.com recipient2@example.com]", q.ToAddrs)
				}
			},
		},
		{
			name:    "LabelAppends",
			initial: &search.Query{Labels: []string{"inbox"}},
			filter:  MessageFilter{Label: "important"},
			check: func(t *testing.T, q *search.Query) {
				if len(q.Labels) != 2 {
					t.Fatalf("Labels: got %d items, want 2", len(q.Labels))
				}
				if q.Labels[0] != "inbox" || q.Labels[1] != "important" {
					t.Errorf("Labels: got %v, want [inbox important]", q.Labels)
				}
			},
		},
		{
			name:    "Attachments",
			initial: &search.Query{},
			filter:  MessageFilter{WithAttachmentsOnly: true},
			check: func(t *testing.T, q *search.Query) {
				if q.HasAttachment == nil || !*q.HasAttachment {
					t.Errorf("HasAttachment: got %v, want true", q.HasAttachment)
				}
			},
		},
		{
			name:    "Domain",
			initial: &search.Query{},
			filter:  MessageFilter{Domain: "example.com"},
			check: func(t *testing.T, q *search.Query) {
				if len(q.FromAddrs) != 1 || q.FromAddrs[0] != "@example.com" {
					t.Errorf("FromAddrs: got %v, want [@example.com]", q.FromAddrs)
				}
			},
		},
		{
			name: "MultipleFilters",
			initial: &search.Query{
				TextTerms: []string{"search", "term"},
				FromAddrs: []string{"alice@example.com"},
			},
			filter: MessageFilter{
				SourceID:            &sourceID1,
				Sender:              "bob@example.com",
				Recipient:           "carol@example.com",
				Label:               "starred",
				WithAttachmentsOnly: true,
				Domain:              "domain.com",
			},
			check: func(t *testing.T, q *search.Query) {
				if len(q.TextTerms) != 2 || q.TextTerms[0] != "search" || q.TextTerms[1] != "term" {
					t.Errorf("TextTerms: got %v, want [search term]", q.TextTerms)
				}
				if q.AccountID == nil || *q.AccountID != 1 {
					t.Errorf("AccountID: got %v, want 1", q.AccountID)
				}
				if len(q.FromAddrs) != 3 {
					t.Fatalf("FromAddrs: got %d items, want 3", len(q.FromAddrs))
				}
				if len(q.ToAddrs) != 1 || q.ToAddrs[0] != "carol@example.com" {
					t.Errorf("ToAddrs: got %v, want [carol@example.com]", q.ToAddrs)
				}
				if len(q.Labels) != 1 || q.Labels[0] != "starred" {
					t.Errorf("Labels: got %v, want [starred]", q.Labels)
				}
				if q.HasAttachment == nil || !*q.HasAttachment {
					t.Errorf("HasAttachment: got %v, want true", q.HasAttachment)
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			merged := MergeFilterIntoQuery(tc.initial, tc.filter)
			tc.check(t, merged)
		})
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
