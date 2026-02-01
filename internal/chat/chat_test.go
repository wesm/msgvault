package chat

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/wesm/msgvault/internal/query"
	"github.com/wesm/msgvault/internal/search"
)

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

// stubLLM is a test double for LLMClient.
type stubLLM struct {
	chatResp    string
	chatErr     error
	streamResp  string
	streamErr   error
}

func (s *stubLLM) Chat(_ context.Context, _ []Message) (string, error) {
	return s.chatResp, s.chatErr
}

func (s *stubLLM) ChatStream(_ context.Context, _ []Message, onToken func(string)) error {
	if s.streamErr != nil {
		return s.streamErr
	}
	onToken(s.streamResp)
	return nil
}

// stubEngine implements the minimal query.Engine methods used by chat.
type stubEngine struct {
	searchFastResults []query.MessageSummary
	searchResults     []query.MessageSummary
	messages          map[int64]*query.MessageDetail
}

func (e *stubEngine) SearchFast(_ context.Context, _ *search.Query, _ query.MessageFilter, _, _ int) ([]query.MessageSummary, error) {
	return e.searchFastResults, nil
}

func (e *stubEngine) Search(_ context.Context, _ *search.Query, _, _ int) ([]query.MessageSummary, error) {
	return e.searchResults, nil
}

func (e *stubEngine) GetMessage(_ context.Context, id int64) (*query.MessageDetail, error) {
	if m, ok := e.messages[id]; ok {
		return m, nil
	}
	return nil, fmt.Errorf("not found")
}

// Unused interface methods — return zero values.
func (e *stubEngine) AggregateBySender(context.Context, query.AggregateOptions) ([]query.AggregateRow, error) { return nil, nil }
func (e *stubEngine) AggregateBySenderName(context.Context, query.AggregateOptions) ([]query.AggregateRow, error) { return nil, nil }
func (e *stubEngine) AggregateByRecipient(context.Context, query.AggregateOptions) ([]query.AggregateRow, error) { return nil, nil }
func (e *stubEngine) AggregateByRecipientName(context.Context, query.AggregateOptions) ([]query.AggregateRow, error) { return nil, nil }
func (e *stubEngine) AggregateByDomain(context.Context, query.AggregateOptions) ([]query.AggregateRow, error) { return nil, nil }
func (e *stubEngine) AggregateByLabel(context.Context, query.AggregateOptions) ([]query.AggregateRow, error) { return nil, nil }
func (e *stubEngine) AggregateByTime(context.Context, query.AggregateOptions) ([]query.AggregateRow, error) { return nil, nil }
func (e *stubEngine) SubAggregate(context.Context, query.MessageFilter, query.ViewType, query.AggregateOptions) ([]query.AggregateRow, error) { return nil, nil }
func (e *stubEngine) ListMessages(context.Context, query.MessageFilter) ([]query.MessageSummary, error) { return nil, nil }
func (e *stubEngine) GetMessageBySourceID(context.Context, string) (*query.MessageDetail, error) { return nil, nil }
func (e *stubEngine) SearchFastCount(context.Context, *search.Query, query.MessageFilter) (int64, error) { return 0, nil }
func (e *stubEngine) GetGmailIDsByFilter(context.Context, query.MessageFilter) ([]string, error) { return nil, nil }
func (e *stubEngine) ListAccounts(context.Context) ([]query.AccountInfo, error) { return nil, nil }
func (e *stubEngine) GetTotalStats(context.Context, query.StatsOptions) (*query.TotalStats, error) { return nil, nil }
func (e *stubEngine) Close() error { return nil }

func TestSessionAsk(t *testing.T) {
	planJSON, _ := json.Marshal(queryPlan{
		SearchText: "budget",
		From:       "alice",
		Reasoning:  "test query",
	})

	eng := &stubEngine{
		searchFastResults: []query.MessageSummary{{ID: 1}},
		messages: map[int64]*query.MessageDetail{
			1: {
				ID:       1,
				Subject:  "Q1 Budget",
				From:     []query.Address{{Email: "alice@example.com", Name: "Alice"}},
				To:       []query.Address{{Email: "me@example.com"}},
				SentAt:   time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC),
				BodyText: "Here is the budget report.",
				Labels:   []string{"INBOX"},
			},
		},
	}

	llm := &stubLLM{
		chatResp:   string(planJSON),
		streamResp: "Based on the retrieved email, Alice sent you a budget report.",
	}

	session := NewSession(eng, llm, Config{MaxResults: 10, MaxBodyLen: 500})

	err := session.Ask(context.Background(), "What did Alice send about the budget?")
	if err != nil {
		t.Fatalf("Ask() error: %v", err)
	}

	if len(session.history) != 2 {
		t.Errorf("expected 2 history entries, got %d", len(session.history))
	}
}

func TestSessionAsk_LLMError(t *testing.T) {
	eng := &stubEngine{}
	llm := &stubLLM{chatErr: fmt.Errorf("connection refused")}

	session := NewSession(eng, llm, Config{MaxResults: 10})
	err := session.Ask(context.Background(), "test")
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "connection refused") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestSessionAsk_CancelledContext(t *testing.T) {
	// A pre-cancelled context should cause Ask to fail without appending
	// an assistant response to history.
	planJSON, _ := json.Marshal(queryPlan{SearchText: "test", Reasoning: "r"})
	eng := &stubEngine{
		searchFastResults: []query.MessageSummary{{ID: 1}},
		messages: map[int64]*query.MessageDetail{
			1: {ID: 1, Subject: "S", From: []query.Address{{Email: "a@b.com"}}, SentAt: time.Now()},
		},
	}
	llm := &stubLLM{
		chatResp:  string(planJSON),
		streamErr: context.Canceled,
	}

	session := NewSession(eng, llm, Config{MaxResults: 5})

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancel

	err := session.Ask(ctx, "test question")
	if err == nil {
		t.Fatal("expected error from cancelled context")
	}

	// History should have the user message but no assistant response.
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
