package chat

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/wesm/msgvault/internal/query"
	"github.com/wesm/msgvault/internal/query/querytest"
	"github.com/wesm/msgvault/internal/search"
	"github.com/wesm/msgvault/internal/testutil"
)

func mockLLMResponse(t *testing.T, plan queryPlan) string {
	t.Helper()
	b, err := json.Marshal(plan)
	if err != nil {
		t.Fatalf("failed to marshal query plan: %v", err)
	}
	return string(b)
}

func TestExtractJSON(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"bare json", `{"search_text": "hello"}`, `{"search_text": "hello"}`},
		{"with fences", "```json\n{\"from\": \"alice\"}\n```", `{"from": "alice"}`},
		{"text before", "Here is the plan:\n{\"from\": \"bob\"}", `{"from": "bob"}`},
		{"text after", "{\"from\": \"bob\"}\nDone.", `{"from": "bob"}`},
		{"no json", "no json here", "no json here"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractJSON(tt.input)
			if got != tt.want {
				t.Errorf("extractJSON() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestPlanToQuery(t *testing.T) {
	boolPtr := func(b bool) *bool { return &b }

	tests := []struct {
		name string
		plan queryPlan
		check func(t *testing.T, q *search.Query)
	}{
		{
			name: "text terms",
			plan: queryPlan{SearchText: "budget report"},
			check: func(t *testing.T, q *search.Query) {
				if len(q.TextTerms) != 2 || q.TextTerms[0] != "budget" {
					t.Errorf("TextTerms = %v", q.TextTerms)
				}
			},
		},
		{
			name: "from address",
			plan: queryPlan{From: "alice@example.com"},
			check: func(t *testing.T, q *search.Query) {
				if len(q.FromAddrs) != 1 || q.FromAddrs[0] != "alice@example.com" {
					t.Errorf("FromAddrs = %v", q.FromAddrs)
				}
			},
		},
		{
			name: "date range",
			plan: queryPlan{After: "2024-01-01", Before: "2024-12-31"},
			check: func(t *testing.T, q *search.Query) {
				if q.AfterDate == nil || q.AfterDate.Format("2006-01-02") != "2024-01-01" {
					t.Errorf("AfterDate = %v", q.AfterDate)
				}
				if q.BeforeDate == nil || q.BeforeDate.Format("2006-01-02") != "2024-12-31" {
					t.Errorf("BeforeDate = %v", q.BeforeDate)
				}
			},
		},
		{
			name: "has attachment",
			plan: queryPlan{HasAttachment: boolPtr(true)},
			check: func(t *testing.T, q *search.Query) {
				if q.HasAttachment == nil || !*q.HasAttachment {
					t.Error("HasAttachment should be true")
				}
			},
		},
		{
			name: "invalid date ignored",
			plan: queryPlan{After: "not-a-date"},
			check: func(t *testing.T, q *search.Query) {
				if q.AfterDate != nil {
					t.Error("AfterDate should be nil for invalid date")
				}
			},
		},
		{
			name: "empty plan",
			plan: queryPlan{},
			check: func(t *testing.T, q *search.Query) {
				if len(q.TextTerms) != 0 || len(q.FromAddrs) != 0 {
					t.Error("empty plan should produce empty query")
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := planToQuery(&tt.plan)
			tt.check(t, q)
		})
	}
}

func TestMergeResults(t *testing.T) {
	mk := func(id int64) query.MessageSummary {
		return query.MessageSummary{ID: id}
	}

	t.Run("dedup", func(t *testing.T) {
		a := []query.MessageSummary{mk(1), mk(2)}
		b := []query.MessageSummary{mk(2), mk(3)}
		got := mergeResults(a, b, 10)
		if len(got) != 3 {
			t.Errorf("expected 3, got %d", len(got))
		}
	})

	t.Run("respects limit", func(t *testing.T) {
		a := []query.MessageSummary{mk(1), mk(2)}
		b := []query.MessageSummary{mk(3), mk(4), mk(5)}
		got := mergeResults(a, b, 3)
		if len(got) != 3 {
			t.Errorf("expected 3, got %d", len(got))
		}
	})

	t.Run("empty inputs", func(t *testing.T) {
		got := mergeResults(nil, nil, 10)
		if len(got) != 0 {
			t.Errorf("expected 0, got %d", len(got))
		}
	})
}

func TestFormatContext(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		got := formatContext(nil)
		if !strings.Contains(got, "No emails found") {
			t.Error("expected no-results message")
		}
	})

	t.Run("formats messages", func(t *testing.T) {
		msgs := []RetrievedMessage{{
			From:    "Alice <alice@example.com>",
			To:      "bob@example.com",
			Date:    "2024-03-15",
			Subject: "Test",
			Labels:  "INBOX",
			Body:    "Hello world",
		}}
		got := formatContext(msgs)
		if !strings.Contains(got, "Alice") || !strings.Contains(got, "Hello world") {
			t.Error("expected message content in output")
		}
	})
}

func TestSessionAsk(t *testing.T) {
	detail := testutil.NewMessageDetail(1).
		WithSubject("Q1 Budget").
		WithFrom(query.Address{Email: "alice@example.com", Name: "Alice"}).
		WithTo(query.Address{Email: "me@example.com"}).
		WithSentAt(time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC)).
		WithBodyText("Here is the budget report.").
		WithLabels("INBOX").
		BuildPtr()

	eng := &querytest.MockEngine{
		SearchFastResults: []query.MessageSummary{{ID: 1}},
		Messages:          map[int64]*query.MessageDetail{1: detail},
	}

	llm := &stubLLM{
		chatResp:   mockLLMResponse(t, queryPlan{SearchText: "budget", From: "alice", Reasoning: "test query"}),
		streamResp: "Based on the retrieved email, Alice sent you a budget report.",
	}

	session := newTestSession(eng, llm)

	err := session.Ask(context.Background(), "What did Alice send about the budget?")
	if err != nil {
		t.Fatalf("Ask() error: %v", err)
	}

	if len(session.history) != 2 {
		t.Errorf("expected 2 history entries, got %d", len(session.history))
	}
}

func TestSessionAsk_LLMError(t *testing.T) {
	llm := &stubLLM{chatErr: fmt.Errorf("connection refused")}
	session := newTestSession(nil, llm)

	err := session.Ask(context.Background(), "test")
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "connection refused") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestSessionAsk_CancelledContext(t *testing.T) {
	eng := &querytest.MockEngine{
		SearchFastResults: []query.MessageSummary{{ID: 1}},
		Messages: map[int64]*query.MessageDetail{
			1: testutil.NewMessageDetail(1).WithSubject("S").
				WithFrom(query.Address{Email: "a@b.com"}).BuildPtr(),
		},
	}
	llm := &stubLLM{
		chatResp:  mockLLMResponse(t, queryPlan{SearchText: "test", Reasoning: "r"}),
		streamErr: context.Canceled,
	}

	session := newTestSession(eng, llm)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancel

	err := session.Ask(ctx, "test question")
	if err == nil {
		t.Fatal("expected error from cancelled context")
	}

	for _, m := range session.history {
		if m.Role == "assistant" {
			t.Error("cancelled request should not append assistant to history")
		}
	}
}

func TestNewOllamaClient_URLValidation(t *testing.T) {
	tests := []struct {
		name    string
		url     string
		wantErr bool
	}{
		{"valid http", "http://localhost:11434", false},
		{"valid https", "https://ollama.example.com", false},
		{"no scheme", "localhost:11434", false},
		{"empty uses default", "", false},
		{"with path", "http://localhost:11434/v1", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewOllamaClient(tt.url, "test-model")
			if (err != nil) != tt.wantErr {
				t.Errorf("NewOllamaClient(%q) error = %v, wantErr %v", tt.url, err, tt.wantErr)
			}
		})
	}
}
