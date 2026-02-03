package mcp

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/wesm/msgvault/internal/query"
	"github.com/wesm/msgvault/internal/query/querytest"
	"github.com/wesm/msgvault/internal/testutil"
)

// toolHandler is the function signature for MCP tool handler methods.
type toolHandler func(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error)

// callToolDirect invokes a handler directly with the given arguments and returns the raw result.
func callToolDirect(t *testing.T, name string, fn toolHandler, args map[string]any) *mcp.CallToolResult {
	t.Helper()
	req := mcp.CallToolRequest{}
	req.Params.Name = name
	req.Params.Arguments = args
	result, err := fn(context.Background(), req)
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

// runTool invokes a handler, asserts no error, and unmarshals the JSON result into T.
func runTool[T any](t *testing.T, name string, fn toolHandler, args map[string]any) T {
	t.Helper()
	r := callToolDirect(t, name, fn, args)
	if r.IsError {
		t.Fatalf("unexpected error: %s", resultText(t, r))
	}
	var out T
	if err := json.Unmarshal([]byte(resultText(t, r)), &out); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	return out
}

// runToolExpectError invokes a handler and asserts it returns an error result.
func runToolExpectError(t *testing.T, name string, fn toolHandler, args map[string]any) *mcp.CallToolResult {
	t.Helper()
	r := callToolDirect(t, name, fn, args)
	if !r.IsError {
		t.Fatal("expected error result")
	}
	return r
}

func TestSearchMessages(t *testing.T) {
	eng := &querytest.MockEngine{
		SearchFastResults: []query.MessageSummary{
			testutil.NewMessageSummary(1).WithSubject("Hello").WithFromEmail("alice@example.com").Build(),
		},
	}
	h := &handlers{engine: eng}

	t.Run("valid query", func(t *testing.T) {
		msgs := runTool[[]query.MessageSummary](t, "search_messages", h.searchMessages, map[string]any{"query": "from:alice"})
		if len(msgs) != 1 || msgs[0].Subject != "Hello" {
			t.Fatalf("unexpected result: %v", msgs)
		}
	})

	t.Run("missing query", func(t *testing.T) {
		runToolExpectError(t, "search_messages", h.searchMessages, map[string]any{})
	})
}

func TestSearchFallbackToFTS(t *testing.T) {
	eng := &querytest.MockEngine{
		SearchFastResults: nil, // fast returns nothing
		SearchResults: []query.MessageSummary{
			testutil.NewMessageSummary(2).WithSubject("Body match").WithFromEmail("bob@example.com").Build(),
		},
	}
	h := &handlers{engine: eng}

	msgs := runTool[[]query.MessageSummary](t, "search_messages", h.searchMessages, map[string]any{"query": "important meeting notes"})
	if len(msgs) != 1 || msgs[0].ID != 2 {
		t.Fatalf("expected FTS fallback result, got: %v", msgs)
	}
}

func TestGetMessage(t *testing.T) {
	eng := &querytest.MockEngine{
		Messages: map[int64]*query.MessageDetail{
			42: testutil.NewMessageDetail(42).WithSubject("Test Message").WithBodyText("Hello world").BuildPtr(),
		},
	}
	h := &handlers{engine: eng}

	t.Run("found", func(t *testing.T) {
		msg := runTool[query.MessageDetail](t, "get_message", h.getMessage, map[string]any{"id": float64(42)})
		if msg.Subject != "Test Message" {
			t.Fatalf("unexpected subject: %s", msg.Subject)
		}
	})

	errorCases := []struct {
		name string
		args map[string]any
	}{
		{"not found", map[string]any{"id": float64(999)}},
		{"missing id", map[string]any{}},
		{"non-integer id", map[string]any{"id": float64(1.9)}},
		{"negative id", map[string]any{"id": float64(-1)}},
		{"overflow id", map[string]any{"id": float64(1e19)}},
	}
	for _, tt := range errorCases {
		t.Run(tt.name, func(t *testing.T) {
			runToolExpectError(t, "get_message", h.getMessage, tt.args)
		})
	}
}

func TestGetStats(t *testing.T) {
	eng := &querytest.MockEngine{
		Stats: &query.TotalStats{
			MessageCount: 1000,
			TotalSize:    5000000,
			AccountCount: 2,
		},
		Accounts: []query.AccountInfo{
			{ID: 1, Identifier: "alice@gmail.com"},
			{ID: 2, Identifier: "bob@gmail.com"},
		},
	}
	h := &handlers{engine: eng}

	resp := runTool[struct {
		Stats    query.TotalStats    `json:"stats"`
		Accounts []query.AccountInfo `json:"accounts"`
	}](t, "get_stats", h.getStats, map[string]any{})

	if resp.Stats.MessageCount != 1000 {
		t.Fatalf("unexpected message count: %d", resp.Stats.MessageCount)
	}
	if len(resp.Accounts) != 2 {
		t.Fatalf("unexpected account count: %d", len(resp.Accounts))
	}
}

func TestAggregate(t *testing.T) {
	eng := &querytest.MockEngine{
		AggregateRows: []query.AggregateRow{
			{Key: "alice@example.com", Count: 100, TotalSize: 50000},
			{Key: "bob@example.com", Count: 50, TotalSize: 25000},
		},
	}
	h := &handlers{engine: eng}

	for _, groupBy := range []string{"sender", "recipient", "domain", "label", "time"} {
		t.Run(groupBy, func(t *testing.T) {
			rows := runTool[[]query.AggregateRow](t, "aggregate", h.aggregate, map[string]any{"group_by": groupBy})
			if len(rows) != 2 {
				t.Fatalf("expected 2 rows, got %d", len(rows))
			}
		})
	}

	errorCases := []struct {
		name string
		args map[string]any
	}{
		{"invalid group_by", map[string]any{"group_by": "invalid"}},
		{"missing group_by", map[string]any{}},
	}
	for _, tt := range errorCases {
		t.Run(tt.name, func(t *testing.T) {
			runToolExpectError(t, "aggregate", h.aggregate, tt.args)
		})
	}
}

func TestListMessages(t *testing.T) {
	eng := &querytest.MockEngine{
		ListResults: []query.MessageSummary{
			testutil.NewMessageSummary(1).WithSubject("Test").WithFromEmail("alice@example.com").Build(),
		},
	}
	h := &handlers{engine: eng}

	t.Run("valid filters", func(t *testing.T) {
		msgs := runTool[[]query.MessageSummary](t, "list_messages", h.listMessages, map[string]any{
			"from":  "alice@example.com",
			"after": "2024-01-01",
			"limit": float64(10),
		})
		if len(msgs) != 1 {
			t.Fatalf("expected 1 message, got %d", len(msgs))
		}
	})

	errorCases := []struct {
		name string
		args map[string]any
	}{
		{"invalid after date", map[string]any{"after": "not-a-date"}},
		{"invalid before date", map[string]any{"before": "2024/01/01"}},
	}
	for _, tt := range errorCases {
		t.Run(tt.name, func(t *testing.T) {
			runToolExpectError(t, "list_messages", h.listMessages, tt.args)
		})
	}
}

func TestAggregateInvalidDates(t *testing.T) {
	eng := &querytest.MockEngine{}
	h := &handlers{engine: eng}

	errorCases := []struct {
		name string
		args map[string]any
	}{
		{"invalid after", map[string]any{"group_by": "sender", "after": "bad"}},
		{"invalid before", map[string]any{"group_by": "sender", "before": "bad"}},
	}
	for _, tt := range errorCases {
		t.Run(tt.name, func(t *testing.T) {
			runToolExpectError(t, "aggregate", h.aggregate, tt.args)
		})
	}
}

// createAttachmentFixture creates a content-addressed file under dir using the given hash.
func createAttachmentFixture(t *testing.T, dir string, hash string, content []byte) {
	t.Helper()
	hashDir := filepath.Join(dir, hash[:2])
	if err := os.MkdirAll(hashDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(hashDir, hash), content, 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestGetAttachment(t *testing.T) {
	tmpDir := t.TempDir()
	hash := "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"
	content := []byte("hello world PDF content")
	createAttachmentFixture(t, tmpDir, hash, content)

	eng := &querytest.MockEngine{
		Attachments: map[int64]*query.AttachmentInfo{
			10: {ID: 10, Filename: "report.pdf", MimeType: "application/pdf", Size: int64(len(content)), ContentHash: hash},
			11: {ID: 11, Filename: "no-hash.pdf", MimeType: "application/pdf", Size: 100, ContentHash: ""},
		},
	}
	h := &handlers{engine: eng, attachmentsDir: tmpDir}

	t.Run("valid", func(t *testing.T) {
		resp := runTool[struct {
			Filename      string `json:"filename"`
			MimeType      string `json:"mime_type"`
			Size          int64  `json:"size"`
			ContentBase64 string `json:"content_base64"`
		}](t, "get_attachment", h.getAttachment, map[string]any{"attachment_id": float64(10)})

		if resp.Filename != "report.pdf" {
			t.Fatalf("unexpected filename: %s", resp.Filename)
		}
		if resp.MimeType != "application/pdf" {
			t.Fatalf("unexpected mime_type: %s", resp.MimeType)
		}
		decoded, err := base64.StdEncoding.DecodeString(resp.ContentBase64)
		if err != nil {
			t.Fatal(err)
		}
		if string(decoded) != string(content) {
			t.Fatalf("content mismatch: got %q", string(decoded))
		}
	})

	errorCases := []struct {
		name string
		args map[string]any
	}{
		{"missing attachment_id", map[string]any{}},
		{"non-integer id", map[string]any{"attachment_id": float64(1.5)}},
		{"not found", map[string]any{"attachment_id": float64(999)}},
		{"missing hash", map[string]any{"attachment_id": float64(11)}},
	}
	for _, tt := range errorCases {
		t.Run(tt.name, func(t *testing.T) {
			runToolExpectError(t, "get_attachment", h.getAttachment, tt.args)
		})
	}

	t.Run("invalid content hash (path traversal)", func(t *testing.T) {
		eng2 := &querytest.MockEngine{
			Attachments: map[int64]*query.AttachmentInfo{
				30: {ID: 30, Filename: "evil.pdf", MimeType: "application/pdf", Size: 100, ContentHash: "../../etc/passwd"},
			},
		}
		h2 := &handlers{engine: eng2, attachmentsDir: tmpDir}
		r := runToolExpectError(t, "get_attachment", h2.getAttachment, map[string]any{"attachment_id": float64(30)})
		if txt := resultText(t, r); txt != "attachment has invalid content hash" {
			t.Fatalf("unexpected error: %s", txt)
		}
	})

	t.Run("non-hex content hash", func(t *testing.T) {
		eng2 := &querytest.MockEngine{
			Attachments: map[int64]*query.AttachmentInfo{
				31: {ID: 31, Filename: "bad.pdf", MimeType: "application/pdf", Size: 100, ContentHash: "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"},
			},
		}
		h2 := &handlers{engine: eng2, attachmentsDir: tmpDir}
		runToolExpectError(t, "get_attachment", h2.getAttachment, map[string]any{"attachment_id": float64(31)})
	})

	t.Run("attachmentsDir not configured", func(t *testing.T) {
		eng2 := &querytest.MockEngine{
			Attachments: map[int64]*query.AttachmentInfo{
				10: {ID: 10, Filename: "report.pdf", MimeType: "application/pdf", Size: 100, ContentHash: hash},
			},
		}
		h2 := &handlers{engine: eng2, attachmentsDir: ""}
		runToolExpectError(t, "get_attachment", h2.getAttachment, map[string]any{"attachment_id": float64(10)})
	})

	t.Run("oversized attachment", func(t *testing.T) {
		bigHash := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		bigDir := filepath.Join(tmpDir, bigHash[:2])
		if err := os.MkdirAll(bigDir, 0o755); err != nil {
			t.Fatal(err)
		}
		bigFile, err := os.Create(filepath.Join(bigDir, bigHash))
		if err != nil {
			t.Fatal(err)
		}
		if err := bigFile.Truncate(maxAttachmentSize + 1); err != nil {
			bigFile.Close()
			t.Fatal(err)
		}
		bigFile.Close()

		eng2 := &querytest.MockEngine{
			Attachments: map[int64]*query.AttachmentInfo{
				40: {ID: 40, Filename: "huge.bin", MimeType: "application/octet-stream", Size: maxAttachmentSize + 1, ContentHash: bigHash},
			},
		}
		h2 := &handlers{engine: eng2, attachmentsDir: tmpDir}
		r := runToolExpectError(t, "get_attachment", h2.getAttachment, map[string]any{"attachment_id": float64(40)})
		if txt := resultText(t, r); !strings.Contains(txt, "too large") {
			t.Fatalf("expected 'too large' error, got: %s", txt)
		}
	})

	t.Run("file not on disk", func(t *testing.T) {
		eng2 := &querytest.MockEngine{
			Attachments: map[int64]*query.AttachmentInfo{
				20: {ID: 20, Filename: "gone.pdf", MimeType: "application/pdf", Size: 100, ContentHash: "deadbeef1234567890abcdef1234567890abcdef1234567890abcdef12345678"},
			},
		}
		h2 := &handlers{engine: eng2, attachmentsDir: tmpDir}
		runToolExpectError(t, "get_attachment", h2.getAttachment, map[string]any{"attachment_id": float64(20)})
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
