package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/wesm/msgvault/internal/query"
	"github.com/wesm/msgvault/internal/search"
)

// stubEngine implements query.Engine for testing.
type stubEngine struct {
	searchFastResults []query.MessageSummary
	searchResults     []query.MessageSummary
	listResults       []query.MessageSummary
	messages          map[int64]*query.MessageDetail
	stats             *query.TotalStats
	accounts          []query.AccountInfo
	aggregateRows     []query.AggregateRow
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
func (e *stubEngine) ListMessages(_ context.Context, _ query.MessageFilter) ([]query.MessageSummary, error) {
	return e.listResults, nil
}
func (e *stubEngine) GetTotalStats(_ context.Context, _ query.StatsOptions) (*query.TotalStats, error) {
	return e.stats, nil
}
func (e *stubEngine) ListAccounts(_ context.Context) ([]query.AccountInfo, error) {
	return e.accounts, nil
}
func (e *stubEngine) AggregateBySender(_ context.Context, _ query.AggregateOptions) ([]query.AggregateRow, error) {
	return e.aggregateRows, nil
}
func (e *stubEngine) AggregateBySenderName(_ context.Context, _ query.AggregateOptions) ([]query.AggregateRow, error) {
	return e.aggregateRows, nil
}
func (e *stubEngine) AggregateByRecipient(_ context.Context, _ query.AggregateOptions) ([]query.AggregateRow, error) {
	return e.aggregateRows, nil
}
func (e *stubEngine) AggregateByDomain(_ context.Context, _ query.AggregateOptions) ([]query.AggregateRow, error) {
	return e.aggregateRows, nil
}
func (e *stubEngine) AggregateByLabel(_ context.Context, _ query.AggregateOptions) ([]query.AggregateRow, error) {
	return e.aggregateRows, nil
}
func (e *stubEngine) AggregateByTime(_ context.Context, _ query.AggregateOptions) ([]query.AggregateRow, error) {
	return e.aggregateRows, nil
}
func (e *stubEngine) SubAggregate(_ context.Context, _ query.MessageFilter, _ query.ViewType, _ query.AggregateOptions) ([]query.AggregateRow, error) {
	return nil, nil
}
func (e *stubEngine) GetMessageBySourceID(_ context.Context, _ string) (*query.MessageDetail, error) {
	return nil, nil
}
func (e *stubEngine) SearchFastCount(_ context.Context, _ *search.Query, _ query.MessageFilter) (int64, error) {
	return 0, nil
}
func (e *stubEngine) GetGmailIDsByFilter(_ context.Context, _ query.MessageFilter) ([]string, error) {
	return nil, nil
}
func (e *stubEngine) Close() error { return nil }

func callTool(t *testing.T, h *handlers, name string, args map[string]any) *mcp.CallToolResult {
	t.Helper()
	req := mcp.CallToolRequest{}
	req.Params.Name = name
	req.Params.Arguments = args
	var handler func(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error)
	switch name {
	case "search_messages":
		handler = h.searchMessages
	case "get_message":
		handler = h.getMessage
	case "list_messages":
		handler = h.listMessages
	case "get_stats":
		handler = h.getStats
	case "aggregate":
		handler = h.aggregate
	default:
		t.Fatalf("unknown tool: %s", name)
	}
	result, err := handler(context.Background(), req)
	if err != nil {
		t.Fatalf("handler returned error: %v", err)
	}
	return result
}

func resultText(t *testing.T, r *mcp.CallToolResult) string {
	t.Helper()
	if len(r.Content) == 0 {
		t.Fatal("empty content")
	}
	tc, ok := r.Content[0].(mcp.TextContent)
	if !ok {
		t.Fatalf("expected TextContent, got %T", r.Content[0])
	}
	return tc.Text
}

func TestSearchMessages(t *testing.T) {
	now := time.Now()
	eng := &stubEngine{
		searchFastResults: []query.MessageSummary{
			{ID: 1, Subject: "Hello", FromEmail: "alice@example.com", SentAt: now},
		},
	}
	h := &handlers{engine: eng}

	t.Run("valid query", func(t *testing.T) {
		r := callTool(t, h, "search_messages", map[string]any{"query": "from:alice"})
		if r.IsError {
			t.Fatalf("unexpected error: %s", resultText(t, r))
		}
		var msgs []query.MessageSummary
		if err := json.Unmarshal([]byte(resultText(t, r)), &msgs); err != nil {
			t.Fatal(err)
		}
		if len(msgs) != 1 || msgs[0].Subject != "Hello" {
			t.Fatalf("unexpected result: %v", msgs)
		}
	})

	t.Run("missing query", func(t *testing.T) {
		r := callTool(t, h, "search_messages", map[string]any{})
		if !r.IsError {
			t.Fatal("expected error for missing query")
		}
	})
}

func TestSearchFallbackToFTS(t *testing.T) {
	now := time.Now()
	eng := &stubEngine{
		searchFastResults: nil, // fast returns nothing
		searchResults: []query.MessageSummary{
			{ID: 2, Subject: "Body match", FromEmail: "bob@example.com", SentAt: now},
		},
	}
	h := &handlers{engine: eng}

	r := callTool(t, h, "search_messages", map[string]any{"query": "important meeting notes"})
	if r.IsError {
		t.Fatalf("unexpected error: %s", resultText(t, r))
	}
	var msgs []query.MessageSummary
	if err := json.Unmarshal([]byte(resultText(t, r)), &msgs); err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 1 || msgs[0].ID != 2 {
		t.Fatalf("expected FTS fallback result, got: %v", msgs)
	}
}

func TestGetMessage(t *testing.T) {
	eng := &stubEngine{
		messages: map[int64]*query.MessageDetail{
			42: {ID: 42, Subject: "Test Message", BodyText: "Hello world"},
		},
	}
	h := &handlers{engine: eng}

	t.Run("found", func(t *testing.T) {
		r := callTool(t, h, "get_message", map[string]any{"id": float64(42)})
		if r.IsError {
			t.Fatalf("unexpected error: %s", resultText(t, r))
		}
		var msg query.MessageDetail
		if err := json.Unmarshal([]byte(resultText(t, r)), &msg); err != nil {
			t.Fatal(err)
		}
		if msg.Subject != "Test Message" {
			t.Fatalf("unexpected subject: %s", msg.Subject)
		}
	})

	t.Run("not found", func(t *testing.T) {
		r := callTool(t, h, "get_message", map[string]any{"id": float64(999)})
		if !r.IsError {
			t.Fatal("expected error for not-found message")
		}
	})

	t.Run("missing id", func(t *testing.T) {
		r := callTool(t, h, "get_message", map[string]any{})
		if !r.IsError {
			t.Fatal("expected error for missing id")
		}
	})

	t.Run("non-integer id", func(t *testing.T) {
		r := callTool(t, h, "get_message", map[string]any{"id": float64(1.9)})
		if !r.IsError {
			t.Fatal("expected error for non-integer id")
		}
	})

	t.Run("negative id", func(t *testing.T) {
		r := callTool(t, h, "get_message", map[string]any{"id": float64(-1)})
		if !r.IsError {
			t.Fatal("expected error for negative id")
		}
	})

	t.Run("overflow id", func(t *testing.T) {
		r := callTool(t, h, "get_message", map[string]any{"id": float64(1e19)})
		if !r.IsError {
			t.Fatal("expected error for overflow id")
		}
	})
}

func TestGetStats(t *testing.T) {
	eng := &stubEngine{
		stats: &query.TotalStats{
			MessageCount: 1000,
			TotalSize:    5000000,
			AccountCount: 2,
		},
		accounts: []query.AccountInfo{
			{ID: 1, Identifier: "alice@gmail.com"},
			{ID: 2, Identifier: "bob@gmail.com"},
		},
	}
	h := &handlers{engine: eng}

	r := callTool(t, h, "get_stats", map[string]any{})
	if r.IsError {
		t.Fatalf("unexpected error: %s", resultText(t, r))
	}

	var resp struct {
		Stats    query.TotalStats   `json:"stats"`
		Accounts []query.AccountInfo `json:"accounts"`
	}
	if err := json.Unmarshal([]byte(resultText(t, r)), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Stats.MessageCount != 1000 {
		t.Fatalf("unexpected message count: %d", resp.Stats.MessageCount)
	}
	if len(resp.Accounts) != 2 {
		t.Fatalf("unexpected account count: %d", len(resp.Accounts))
	}
}

func TestAggregate(t *testing.T) {
	eng := &stubEngine{
		aggregateRows: []query.AggregateRow{
			{Key: "alice@example.com", Count: 100, TotalSize: 50000},
			{Key: "bob@example.com", Count: 50, TotalSize: 25000},
		},
	}
	h := &handlers{engine: eng}

	for _, groupBy := range []string{"sender", "recipient", "domain", "label", "time"} {
		t.Run(groupBy, func(t *testing.T) {
			r := callTool(t, h, "aggregate", map[string]any{"group_by": groupBy})
			if r.IsError {
				t.Fatalf("unexpected error: %s", resultText(t, r))
			}
			var rows []query.AggregateRow
			if err := json.Unmarshal([]byte(resultText(t, r)), &rows); err != nil {
				t.Fatal(err)
			}
			if len(rows) != 2 {
				t.Fatalf("expected 2 rows, got %d", len(rows))
			}
		})
	}

	t.Run("invalid group_by", func(t *testing.T) {
		r := callTool(t, h, "aggregate", map[string]any{"group_by": "invalid"})
		if !r.IsError {
			t.Fatal("expected error for invalid group_by")
		}
	})

	t.Run("missing group_by", func(t *testing.T) {
		r := callTool(t, h, "aggregate", map[string]any{})
		if !r.IsError {
			t.Fatal("expected error for missing group_by")
		}
	})
}

func TestListMessages(t *testing.T) {
	now := time.Now()
	eng := &stubEngine{
		listResults: []query.MessageSummary{
			{ID: 1, Subject: "Test", FromEmail: "alice@example.com", SentAt: now},
		},
	}
	h := &handlers{engine: eng}

	t.Run("valid filters", func(t *testing.T) {
		r := callTool(t, h, "list_messages", map[string]any{
			"from":  "alice@example.com",
			"after": "2024-01-01",
			"limit": float64(10),
		})
		if r.IsError {
			t.Fatalf("unexpected error: %s", resultText(t, r))
		}
		var msgs []query.MessageSummary
		if err := json.Unmarshal([]byte(resultText(t, r)), &msgs); err != nil {
			t.Fatal(err)
		}
		if len(msgs) != 1 {
			t.Fatalf("expected 1 message, got %d", len(msgs))
		}
	})

	t.Run("invalid after date", func(t *testing.T) {
		r := callTool(t, h, "list_messages", map[string]any{"after": "not-a-date"})
		if !r.IsError {
			t.Fatal("expected error for invalid after date")
		}
	})

	t.Run("invalid before date", func(t *testing.T) {
		r := callTool(t, h, "list_messages", map[string]any{"before": "2024/01/01"})
		if !r.IsError {
			t.Fatal("expected error for invalid before date")
		}
	})
}

func TestAggregateInvalidDates(t *testing.T) {
	eng := &stubEngine{}
	h := &handlers{engine: eng}

	t.Run("invalid after", func(t *testing.T) {
		r := callTool(t, h, "aggregate", map[string]any{"group_by": "sender", "after": "bad"})
		if !r.IsError {
			t.Fatal("expected error for invalid after date")
		}
	})

	t.Run("invalid before", func(t *testing.T) {
		r := callTool(t, h, "aggregate", map[string]any{"group_by": "sender", "before": "bad"})
		if !r.IsError {
			t.Fatal("expected error for invalid before date")
		}
	})
}

func TestIntArgClamping(t *testing.T) {
	tests := []struct {
		name string
		val  float64
		want int
	}{
		{"negative clamped to 0", -5, 0},
		{"zero stays zero", 0, 0},
		{"normal value", 50, 50},
		{"above max clamped", 5000, maxLimit},
		{"huge float clamped", 1e18, maxLimit},
		{"NaN clamped to 0", math.NaN(), 0},
		{"Inf clamped", math.Inf(1), maxLimit},
		{"negative Inf clamped to 0", math.Inf(-1), 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := intArg(map[string]any{"x": tt.val}, "x", 20)
			if got != tt.want {
				t.Fatalf("intArg(%v) = %d, want %d", tt.val, got, tt.want)
			}
		})
	}
}
