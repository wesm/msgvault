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
	"github.com/wesm/msgvault/internal/export"
	"github.com/wesm/msgvault/internal/query"
	"github.com/wesm/msgvault/internal/query/querytest"
	"github.com/wesm/msgvault/internal/testutil"
)

// toolHandler is the function signature for MCP tool handler methods.
type toolHandler func(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error)

// Response types for runTool generic calls.
type statsResponse struct {
	Stats    query.TotalStats    `json:"stats"`
	Accounts []query.AccountInfo `json:"accounts"`
}

type attachmentMeta struct {
	Filename string `json:"filename"`
	MimeType string `json:"mime_type"`
	Size     int64  `json:"size"`
}

// newTestHandlers creates a handlers instance with the given mock engine.
func newTestHandlers(eng *querytest.MockEngine) *handlers {
	return &handlers{engine: eng}
}

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
			testutil.NewMessageSummary(1).WithSubject("Hello").WithFromEmail("alice@example.com").WithSourceConversationID("thread-abc").Build(),
		},
	}
	h := newTestHandlers(eng)

	t.Run("valid query", func(t *testing.T) {
		msgs := runTool[[]query.MessageSummary](t, "search_messages", h.searchMessages, map[string]any{"query": "from:alice"})
		if len(msgs) != 1 || msgs[0].Subject != "Hello" {
			t.Fatalf("unexpected result: %v", msgs)
		}
		if msgs[0].SourceConversationID != "thread-abc" {
			t.Fatalf("expected SourceConversationID 'thread-abc', got %q", msgs[0].SourceConversationID)
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
	h := newTestHandlers(eng)

	msgs := runTool[[]query.MessageSummary](t, "search_messages", h.searchMessages, map[string]any{"query": "important meeting notes"})
	if len(msgs) != 1 || msgs[0].ID != 2 {
		t.Fatalf("expected FTS fallback result, got: %v", msgs)
	}
}

func TestGetMessage(t *testing.T) {
	eng := &querytest.MockEngine{
		Messages: map[int64]*query.MessageDetail{
			42: testutil.NewMessageDetail(42).WithSubject("Test Message").WithBodyText("Hello world").WithSourceConversationID("thread-xyz").BuildPtr(),
		},
	}
	h := newTestHandlers(eng)

	t.Run("found", func(t *testing.T) {
		msg := runTool[query.MessageDetail](t, "get_message", h.getMessage, map[string]any{"id": float64(42)})
		if msg.Subject != "Test Message" {
			t.Fatalf("unexpected subject: %s", msg.Subject)
		}
		if msg.SourceConversationID != "thread-xyz" {
			t.Fatalf("expected SourceConversationID 'thread-xyz', got %q", msg.SourceConversationID)
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
	h := newTestHandlers(eng)

	resp := runTool[statsResponse](t, "get_stats", h.getStats, map[string]any{})

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
	h := newTestHandlers(eng)

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
			testutil.NewMessageSummary(1).WithSubject("Test").WithFromEmail("alice@example.com").WithSourceConversationID("thread-list").Build(),
		},
	}
	h := newTestHandlers(eng)

	t.Run("valid filters", func(t *testing.T) {
		msgs := runTool[[]query.MessageSummary](t, "list_messages", h.listMessages, map[string]any{
			"from":  "alice@example.com",
			"after": "2024-01-01",
			"limit": float64(10),
		})
		if len(msgs) != 1 {
			t.Fatalf("expected 1 message, got %d", len(msgs))
		}
		if msgs[0].SourceConversationID != "thread-list" {
			t.Fatalf("expected SourceConversationID 'thread-list', got %q", msgs[0].SourceConversationID)
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
	h := newTestHandlers(&querytest.MockEngine{})

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
		r := callToolDirect(t, "get_attachment", h.getAttachment, map[string]any{"attachment_id": float64(10)})
		if r.IsError {
			t.Fatalf("unexpected error: %s", resultText(t, r))
		}

		// Should have 2 content blocks: text metadata + embedded resource.
		if len(r.Content) != 2 {
			t.Fatalf("expected 2 content blocks, got %d", len(r.Content))
		}

		// First block: text with metadata JSON.
		tc, ok := r.Content[0].(mcp.TextContent)
		if !ok {
			t.Fatalf("expected TextContent, got %T", r.Content[0])
		}
		var meta attachmentMeta
		if err := json.Unmarshal([]byte(tc.Text), &meta); err != nil {
			t.Fatalf("unmarshal metadata: %v", err)
		}
		if meta.Filename != "report.pdf" {
			t.Fatalf("unexpected filename: %s", meta.Filename)
		}
		if meta.MimeType != "application/pdf" {
			t.Fatalf("unexpected mime_type: %s", meta.MimeType)
		}
		if meta.Size != int64(len(content)) {
			t.Fatalf("unexpected size: %d", meta.Size)
		}

		// Second block: embedded resource with blob.
		er, ok := r.Content[1].(mcp.EmbeddedResource)
		if !ok {
			t.Fatalf("expected EmbeddedResource, got %T", r.Content[1])
		}
		blob, ok := er.Resource.(mcp.BlobResourceContents)
		if !ok {
			t.Fatalf("expected BlobResourceContents, got %T", er.Resource)
		}
		if blob.MIMEType != "application/pdf" {
			t.Fatalf("unexpected blob MIME type: %s", blob.MIMEType)
		}
		decoded, err := base64.StdEncoding.DecodeString(blob.Blob)
		if err != nil {
			t.Fatal(err)
		}
		if string(decoded) != string(content) {
			t.Fatalf("content mismatch: got %q", string(decoded))
		}
	})

	t.Run("empty mime type defaults to octet-stream", func(t *testing.T) {
		noMimeHash := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
		noMimeContent := []byte("binary data")
		createAttachmentFixture(t, tmpDir, noMimeHash, noMimeContent)

		h2 := &handlers{
			engine: &querytest.MockEngine{
				Attachments: map[int64]*query.AttachmentInfo{
					50: {ID: 50, Filename: "data.bin", MimeType: "", Size: int64(len(noMimeContent)), ContentHash: noMimeHash},
				},
			},
			attachmentsDir: tmpDir,
		}
		r := callToolDirect(t, "get_attachment", h2.getAttachment, map[string]any{"attachment_id": float64(50)})
		if r.IsError {
			t.Fatalf("unexpected error: %s", resultText(t, r))
		}

		var meta attachmentMeta
		tc := r.Content[0].(mcp.TextContent)
		if err := json.Unmarshal([]byte(tc.Text), &meta); err != nil {
			t.Fatalf("unmarshal metadata: %v", err)
		}
		if meta.MimeType != "application/octet-stream" {
			t.Fatalf("expected default mime_type, got %s", meta.MimeType)
		}

		er := r.Content[1].(mcp.EmbeddedResource)
		blob := er.Resource.(mcp.BlobResourceContents)
		if blob.MIMEType != "application/octet-stream" {
			t.Fatalf("expected default blob MIME type, got %s", blob.MIMEType)
		}
	})

	t.Run("filename with spaces and unicode", func(t *testing.T) {
		unicodeHash := "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
		unicodeContent := []byte("unicode file")
		createAttachmentFixture(t, tmpDir, unicodeHash, unicodeContent)

		h2 := &handlers{
			engine: &querytest.MockEngine{
				Attachments: map[int64]*query.AttachmentInfo{
					51: {ID: 51, Filename: "report 2024✓.pdf", MimeType: "application/pdf", Size: int64(len(unicodeContent)), ContentHash: unicodeHash},
				},
			},
			attachmentsDir: tmpDir,
		}
		r := callToolDirect(t, "get_attachment", h2.getAttachment, map[string]any{"attachment_id": float64(51)})
		if r.IsError {
			t.Fatalf("unexpected error: %s", resultText(t, r))
		}

		// Metadata JSON must be valid and preserve the filename exactly.
		var meta attachmentMeta
		tc := r.Content[0].(mcp.TextContent)
		if err := json.Unmarshal([]byte(tc.Text), &meta); err != nil {
			t.Fatalf("metadata is not valid JSON: %v\nraw: %s", err, tc.Text)
		}
		if meta.Filename != "report 2024✓.pdf" {
			t.Fatalf("unexpected filename: %s", meta.Filename)
		}

		// URI must percent-encode spaces and non-ASCII characters.
		er := r.Content[1].(mcp.EmbeddedResource)
		blob := er.Resource.(mcp.BlobResourceContents)
		const wantURI = "attachment:///51/report%202024%E2%9C%93.pdf"
		if blob.URI != wantURI {
			t.Fatalf("unexpected URI:\n got: %s\nwant: %s", blob.URI, wantURI)
		}
	})

	// Error cases using the shared engine/handler.
	sharedErrorCases := []struct {
		name string
		args map[string]any
	}{
		{"missing attachment_id", map[string]any{}},
		{"non-integer id", map[string]any{"attachment_id": float64(1.5)}},
		{"not found", map[string]any{"attachment_id": float64(999)}},
		{"missing hash", map[string]any{"attachment_id": float64(11)}},
	}
	for _, tt := range sharedErrorCases {
		t.Run(tt.name, func(t *testing.T) {
			runToolExpectError(t, "get_attachment", h.getAttachment, tt.args)
		})
	}

	// Error cases requiring custom engine/handler configuration.
	customErrorCases := []struct {
		name        string
		attachments map[int64]*query.AttachmentInfo
		attDir      string
		args        map[string]any
		errContains string // if non-empty, assert error text contains this
	}{
		{
			name:        "invalid content hash (path traversal)",
			attachments: map[int64]*query.AttachmentInfo{30: {ID: 30, Filename: "evil.pdf", MimeType: "application/pdf", Size: 100, ContentHash: "../../etc/passwd"}},
			attDir:      tmpDir,
			args:        map[string]any{"attachment_id": float64(30)},
			errContains: "invalid content hash",
		},
		{
			name:        "non-hex content hash",
			attachments: map[int64]*query.AttachmentInfo{31: {ID: 31, Filename: "bad.pdf", MimeType: "application/pdf", Size: 100, ContentHash: "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"}},
			attDir:      tmpDir,
			args:        map[string]any{"attachment_id": float64(31)},
		},
		{
			name:        "attachmentsDir not configured",
			attachments: map[int64]*query.AttachmentInfo{10: {ID: 10, Filename: "report.pdf", MimeType: "application/pdf", Size: 100, ContentHash: hash}},
			attDir:      "",
			args:        map[string]any{"attachment_id": float64(10)},
		},
		{
			name:        "file not on disk",
			attachments: map[int64]*query.AttachmentInfo{20: {ID: 20, Filename: "gone.pdf", MimeType: "application/pdf", Size: 100, ContentHash: "deadbeef1234567890abcdef1234567890abcdef1234567890abcdef12345678"}},
			attDir:      tmpDir,
			args:        map[string]any{"attachment_id": float64(20)},
		},
	}
	for _, tt := range customErrorCases {
		t.Run(tt.name, func(t *testing.T) {
			h2 := &handlers{
				engine:         &querytest.MockEngine{Attachments: tt.attachments},
				attachmentsDir: tt.attDir,
			}
			r := runToolExpectError(t, "get_attachment", h2.getAttachment, tt.args)
			if tt.errContains != "" {
				if txt := resultText(t, r); !strings.Contains(txt, tt.errContains) {
					t.Fatalf("expected error containing %q, got: %s", tt.errContains, txt)
				}
			}
		})
	}

	t.Run("oversized attachment", func(t *testing.T) {
		bigHash := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		createAttachmentFixture(t, tmpDir, bigHash, nil)
		bigPath := filepath.Join(tmpDir, bigHash[:2], bigHash)
		if err := os.Truncate(bigPath, maxAttachmentSize+1); err != nil {
			t.Fatal(err)
		}

		h2 := &handlers{
			engine: &querytest.MockEngine{
				Attachments: map[int64]*query.AttachmentInfo{
					40: {ID: 40, Filename: "huge.bin", MimeType: "application/octet-stream", Size: maxAttachmentSize + 1, ContentHash: bigHash},
				},
			},
			attachmentsDir: tmpDir,
		}
		r := runToolExpectError(t, "get_attachment", h2.getAttachment, map[string]any{"attachment_id": float64(40)})
		if txt := resultText(t, r); !strings.Contains(txt, "too large") {
			t.Fatalf("expected 'too large' error, got: %s", txt)
		}
	})
}

type exportResponse struct {
	Path     string `json:"path"`
	Filename string `json:"filename"`
	Size     int64  `json:"size"`
}

func TestExportAttachment(t *testing.T) {
	srcDir := t.TempDir()
	hash := "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"
	content := []byte("hello world PDF content")
	createAttachmentFixture(t, srcDir, hash, content)

	eng := &querytest.MockEngine{
		Attachments: map[int64]*query.AttachmentInfo{
			10: {ID: 10, Filename: "report.pdf", MimeType: "application/pdf", Size: int64(len(content)), ContentHash: hash},
		},
	}
	h := &handlers{engine: eng, attachmentsDir: srcDir}

	t.Run("export to custom destination", func(t *testing.T) {
		destDir := t.TempDir()
		resp := runTool[exportResponse](t, "export_attachment", h.exportAttachment, map[string]any{
			"attachment_id": float64(10),
			"destination":   destDir,
		})
		if resp.Filename != "report.pdf" {
			t.Fatalf("unexpected filename: %s", resp.Filename)
		}
		if resp.Size != int64(len(content)) {
			t.Fatalf("unexpected size: %d", resp.Size)
		}
		wantPath := filepath.Join(destDir, "report.pdf")
		if resp.Path != wantPath {
			t.Fatalf("unexpected path: %s (want %s)", resp.Path, wantPath)
		}
		got, err := os.ReadFile(wantPath)
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != string(content) {
			t.Fatalf("content mismatch")
		}
	})

	t.Run("filename collision appends suffix", func(t *testing.T) {
		destDir := t.TempDir()
		// Create existing file to force collision.
		if err := os.WriteFile(filepath.Join(destDir, "report.pdf"), []byte("old"), 0644); err != nil {
			t.Fatal(err)
		}
		resp := runTool[exportResponse](t, "export_attachment", h.exportAttachment, map[string]any{
			"attachment_id": float64(10),
			"destination":   destDir,
		})
		if resp.Filename != "report_1.pdf" {
			t.Fatalf("expected report_1.pdf, got %s", resp.Filename)
		}
		// Original file should be untouched.
		old, _ := os.ReadFile(filepath.Join(destDir, "report.pdf"))
		if string(old) != "old" {
			t.Fatal("original file was overwritten")
		}
	})

	t.Run("directory collision appends suffix", func(t *testing.T) {
		destDir := t.TempDir()
		// Create a directory with the same name as the attachment.
		if err := os.Mkdir(filepath.Join(destDir, "report.pdf"), 0755); err != nil {
			t.Fatal(err)
		}
		resp := runTool[exportResponse](t, "export_attachment", h.exportAttachment, map[string]any{
			"attachment_id": float64(10),
			"destination":   destDir,
		})
		if resp.Filename != "report_1.pdf" {
			t.Fatalf("expected report_1.pdf, got %s", resp.Filename)
		}
	})

	t.Run("default destination is ~/Downloads", func(t *testing.T) {
		// This test only verifies the handler doesn't error when
		// ~/Downloads exists (it does on macOS).
		home, err := os.UserHomeDir()
		if err != nil {
			t.Skip("cannot determine home dir")
		}
		downloads := filepath.Join(home, "Downloads")
		if _, err := os.Stat(downloads); os.IsNotExist(err) {
			t.Skip("~/Downloads does not exist")
		}

		resp := runTool[exportResponse](t, "export_attachment", h.exportAttachment, map[string]any{
			"attachment_id": float64(10),
		})
		if !strings.HasPrefix(resp.Path, downloads) {
			t.Fatalf("expected path under ~/Downloads, got %s", resp.Path)
		}
		// Clean up the file we just wrote.
		os.Remove(resp.Path)
	})

	t.Run("invalid destination", func(t *testing.T) {
		runToolExpectError(t, "export_attachment", h.exportAttachment, map[string]any{
			"attachment_id": float64(10),
			"destination":   "/nonexistent/path/that/does/not/exist",
		})
	})

	t.Run("missing attachment_id", func(t *testing.T) {
		runToolExpectError(t, "export_attachment", h.exportAttachment, map[string]any{})
	})

	t.Run("attachment not found", func(t *testing.T) {
		runToolExpectError(t, "export_attachment", h.exportAttachment, map[string]any{
			"attachment_id": float64(999),
		})
	})
}

func TestSanitizeFilename(t *testing.T) {
	tests := []struct {
		input, want string
	}{
		{"report.pdf", "report.pdf"},
		{"file:name.pdf", "file_name.pdf"},
		{"path/to/file.txt", "path_to_file.txt"},
		{"back\\slash.doc", "back_slash.doc"},
		{"tab\there.txt", "tab_here.txt"},
		{"new\nline.txt", "new_line.txt"},
		{"pipe|star*.txt", "pipe_star_.txt"},
		{"quotes\"angle<>.txt", "quotes_angle__.txt"},
		{"clean-file_v2.pdf", "clean-file_v2.pdf"},
		{"", ""},
	}
	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			got := export.SanitizeFilename(tc.input)
			if got != tc.want {
				t.Fatalf("SanitizeFilename(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

func TestExportAttachment_EdgeFilenames(t *testing.T) {
	srcDir := t.TempDir()
	hash := "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"
	content := []byte("data")
	createAttachmentFixture(t, srcDir, hash, content)

	tests := []struct {
		name         string
		filename     string
		wantFilename string // expected output filename
	}{
		{"empty filename falls back to hash", "", hash},
		{"dot filename falls back to hash", ".", hash},
		{"path traversal stripped by Base", "../evil.pdf", "evil.pdf"},
		{"special chars sanitized", "file:name|v2.pdf", "file_name_v2.pdf"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			destDir := t.TempDir()
			h := &handlers{
				engine: &querytest.MockEngine{
					Attachments: map[int64]*query.AttachmentInfo{
						1: {ID: 1, Filename: tc.filename, MimeType: "application/pdf", Size: int64(len(content)), ContentHash: hash},
					},
				},
				attachmentsDir: srcDir,
			}
			resp := runTool[exportResponse](t, "export_attachment", h.exportAttachment, map[string]any{
				"attachment_id": float64(1),
				"destination":   destDir,
			})
			if resp.Filename != tc.wantFilename {
				t.Fatalf("expected filename %q, got %q", tc.wantFilename, resp.Filename)
			}
		})
	}
}

func TestGetAttachment_RejectsOversizedBeforeFileIO(t *testing.T) {
	// The att.Size metadata from the database tells us this attachment is too
	// large BEFORE we try to open the file. The handler should reject with a
	// "too large" error immediately, without attempting any file I/O.
	//
	// Without the pre-flight check, the handler would try to open the file
	// and produce a misleading "not available" error instead.

	oversizeAtt := &query.AttachmentInfo{
		ID:          99,
		Filename:    "huge.bin",
		MimeType:    "application/octet-stream",
		Size:        maxAttachmentSize + 1,
		ContentHash: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
	}

	h := &handlers{
		engine: &querytest.MockEngine{
			Attachments: map[int64]*query.AttachmentInfo{99: oversizeAtt},
		},
		attachmentsDir: t.TempDir(), // empty dir — file does NOT exist on disk
	}

	// getAttachment should reject based on metadata size, not file I/O error
	r := runToolExpectError(t, "get_attachment", h.getAttachment, map[string]any{
		"attachment_id": float64(99),
	})
	txt := resultText(t, r)
	if !strings.Contains(txt, "too large") {
		t.Fatalf("expected 'too large' rejection from metadata check, got: %s", txt)
	}
}

func TestExportAttachment_RejectsOversizedBeforeFileIO(t *testing.T) {
	oversizeAtt := &query.AttachmentInfo{
		ID:          99,
		Filename:    "huge.bin",
		MimeType:    "application/octet-stream",
		Size:        maxAttachmentSize + 1,
		ContentHash: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
	}

	h := &handlers{
		engine: &querytest.MockEngine{
			Attachments: map[int64]*query.AttachmentInfo{99: oversizeAtt},
		},
		attachmentsDir: t.TempDir(),
	}

	r := runToolExpectError(t, "export_attachment", h.exportAttachment, map[string]any{
		"attachment_id": float64(99),
		"destination":   t.TempDir(),
	})
	txt := resultText(t, r)
	if !strings.Contains(txt, "too large") {
		t.Fatalf("expected 'too large' rejection from metadata check, got: %s", txt)
	}
}

func TestLimitArgClamping(t *testing.T) {
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
			got := limitArg(map[string]any{"x": tt.val}, "x", 20)
			if got != tt.want {
				t.Fatalf("limitArg(%v) = %d, want %d", tt.val, got, tt.want)
			}
		})
	}
}
