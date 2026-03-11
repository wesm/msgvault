package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/wesm/msgvault/internal/config"
	"github.com/wesm/msgvault/internal/query"
	"github.com/wesm/msgvault/internal/query/querytest"
)

// newTestServerWithEngine creates a test server with a mock store and the given engine attached.
func newTestServerWithEngine(t *testing.T, eng query.Engine) *Server {
	t.Helper()
	cfg := &config.Config{
		Server: config.ServerConfig{APIPort: 8080}, // empty APIKey → auth disabled
	}
	sched := newMockScheduler()
	srv := NewServer(cfg, &mockStore{}, sched, testLogger())
	if eng != nil {
		srv.WithEngine(eng)
	}
	return srv
}

// --------------------------------------------------------------------------
// handleAggregate
// --------------------------------------------------------------------------

func TestHandleAggregate_NoEngine(t *testing.T) {
	srv := newTestServerWithEngine(t, nil)

	req := httptest.NewRequest("GET", "/api/v1/aggregate?group_by=sender", nil)
	w := httptest.NewRecorder()
	srv.Router().ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want %d", w.Code, http.StatusServiceUnavailable)
	}
}

func TestHandleAggregate_InvalidGroupBy(t *testing.T) {
	eng := &querytest.MockEngine{}
	srv := newTestServerWithEngine(t, eng)

	req := httptest.NewRequest("GET", "/api/v1/aggregate?group_by=bogus", nil)
	w := httptest.NewRecorder()
	srv.Router().ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", w.Code, http.StatusBadRequest)
	}
}

func TestHandleAggregate_Success(t *testing.T) {
	eng := &querytest.MockEngine{
		AggregateRows: []query.AggregateRow{
			{Key: "alice@example.com", Count: 42, TotalSize: 1024, TotalUnique: 3},
			{Key: "bob@example.com", Count: 7, TotalSize: 256, TotalUnique: 3},
		},
	}
	srv := newTestServerWithEngine(t, eng)

	req := httptest.NewRequest("GET", "/api/v1/aggregate?group_by=sender", nil)
	w := httptest.NewRecorder()
	srv.Router().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
	}

	var resp AggregateResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if resp.GroupBy != "sender" {
		t.Errorf("group_by = %q, want %q", resp.GroupBy, "sender")
	}
	if len(resp.Rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(resp.Rows))
	}
	if resp.Rows[0].Key != "alice@example.com" {
		t.Errorf("rows[0].key = %q, want alice@example.com", resp.Rows[0].Key)
	}
	if resp.Rows[0].Count != 42 {
		t.Errorf("rows[0].count = %d, want 42", resp.Rows[0].Count)
	}
	if resp.TotalUnique != 3 {
		t.Errorf("total_unique = %d, want 3", resp.TotalUnique)
	}
}

func TestHandleAggregate_GroupByVariants(t *testing.T) {
	tests := []struct {
		groupBy string
		wantOK  bool
	}{
		{"sender", true},
		{"senders", true},
		{"sender_names", true},
		{"recipient", true},
		{"recipients", true},
		{"recipient_names", true},
		{"domain", true},
		{"domains", true},
		{"label", true},
		{"labels", true},
		{"time", true},
		{"bogus", false},
		{"", false},
	}

	eng := &querytest.MockEngine{}
	srv := newTestServerWithEngine(t, eng)

	for _, tt := range tests {
		t.Run(tt.groupBy, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/api/v1/aggregate?group_by="+tt.groupBy, nil)
			w := httptest.NewRecorder()
			srv.Router().ServeHTTP(w, req)

			if tt.wantOK && w.Code != http.StatusOK {
				t.Errorf("group_by=%q: status = %d, want 200", tt.groupBy, w.Code)
			}
			if !tt.wantOK && w.Code != http.StatusBadRequest {
				t.Errorf("group_by=%q: status = %d, want 400", tt.groupBy, w.Code)
			}
		})
	}
}

// --------------------------------------------------------------------------
// handleEngineMessages
// --------------------------------------------------------------------------

func TestHandleEngineMessages_NoEngine(t *testing.T) {
	srv := newTestServerWithEngine(t, nil)

	req := httptest.NewRequest("GET", "/api/v1/engine/messages", nil)
	w := httptest.NewRecorder()
	srv.Router().ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want %d", w.Code, http.StatusServiceUnavailable)
	}
}

func TestHandleEngineMessages_Success(t *testing.T) {
	now := time.Date(2024, 3, 15, 10, 0, 0, 0, time.UTC)
	eng := &querytest.MockEngine{
		ListResults: []query.MessageSummary{
			{
				ID:        1,
				Subject:   "Hello World",
				FromEmail: "alice@example.com",
				SentAt:    now,
				Labels:    []string{"INBOX"},
			},
		},
	}
	srv := newTestServerWithEngine(t, eng)

	req := httptest.NewRequest("GET", "/api/v1/engine/messages", nil)
	w := httptest.NewRecorder()
	srv.Router().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
	}

	var resp map[string]interface{}
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	msgs, ok := resp["messages"].([]interface{})
	if !ok {
		t.Fatalf("messages field missing or wrong type")
	}
	if len(msgs) != 1 {
		t.Errorf("len(messages) = %d, want 1", len(msgs))
	}

	// Verify pagination defaults
	if page := resp["page"].(float64); page != 1 {
		t.Errorf("page = %v, want 1", page)
	}
	if pageSize := resp["page_size"].(float64); pageSize != 50 {
		t.Errorf("page_size = %v, want 50", pageSize)
	}
}

func TestHandleEngineMessages_Pagination(t *testing.T) {
	eng := &querytest.MockEngine{}
	srv := newTestServerWithEngine(t, eng)

	req := httptest.NewRequest("GET", "/api/v1/engine/messages?page=3&page_size=10", nil)
	w := httptest.NewRecorder()
	srv.Router().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
	}

	var resp map[string]interface{}
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if page := resp["page"].(float64); page != 3 {
		t.Errorf("page = %v, want 3", page)
	}
	if pageSize := resp["page_size"].(float64); pageSize != 10 {
		t.Errorf("page_size = %v, want 10", pageSize)
	}
}

func TestHandleEngineMessages_PeriodFilter(t *testing.T) {
	eng := &querytest.MockEngine{}
	srv := newTestServerWithEngine(t, eng)

	req := httptest.NewRequest("GET", "/api/v1/engine/messages?period=2024-03", nil)
	w := httptest.NewRecorder()
	srv.Router().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", w.Code, http.StatusOK)
	}
}

func TestHandleEngineMessages_FileTypeFilter(t *testing.T) {
	tests := []struct {
		name           string
		param          string
		wantStatus     int
		wantMimeCat    string
		wantAttachOnly bool
	}{
		// UI-style aliases (plural / capitalized)
		{"images alias", "file_type=Images", http.StatusOK, "image", true},
		{"pdfs alias", "file_type=PDFs", http.StatusOK, "pdf", true},
		{"calendar alias", "file_type=Calendar+invites", http.StatusOK, "calendar", true},
		// Canonical lowercase values accepted directly
		{"image canonical", "file_type=image", http.StatusOK, "image", true},
		{"zip canonical", "file_type=zip", http.StatusOK, "zip", true},
		// Unknown values → 400
		{"unknown type", "file_type=foobar", http.StatusBadRequest, "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var captured query.MessageFilter
			eng := &querytest.MockEngine{
				ListMessagesFunc: func(_ context.Context, f query.MessageFilter) ([]query.MessageSummary, error) {
					captured = f
					return nil, nil
				},
			}
			srv := newTestServerWithEngine(t, eng)

			req := httptest.NewRequest("GET", "/api/v1/engine/messages?"+tt.param, nil)
			w := httptest.NewRecorder()
			srv.Router().ServeHTTP(w, req)

			if w.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d (body: %s)", w.Code, tt.wantStatus, w.Body.String())
			}
			if tt.wantStatus == http.StatusOK {
				if captured.MimeCategory != tt.wantMimeCat {
					t.Errorf("MimeCategory = %q, want %q", captured.MimeCategory, tt.wantMimeCat)
				}
				if captured.WithAttachmentsOnly != tt.wantAttachOnly {
					t.Errorf("WithAttachmentsOnly = %v, want %v", captured.WithAttachmentsOnly, tt.wantAttachOnly)
				}
			}
		})
	}
}

func TestHandleEngineMessages_PageSizeClamped(t *testing.T) {
	eng := &querytest.MockEngine{}
	srv := newTestServerWithEngine(t, eng)

	tests := []struct {
		param        string
		wantPageSize float64
	}{
		{"page_size=0", 50},
		{"page_size=300", 50}, // exceeds max 200 → clamped to default
		{"page_size=100", 100},
	}

	for _, tt := range tests {
		t.Run(tt.param, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/api/v1/engine/messages?"+tt.param, nil)
			w := httptest.NewRecorder()
			srv.Router().ServeHTTP(w, req)

			if w.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200", w.Code)
			}

			var resp map[string]interface{}
			if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if ps := resp["page_size"].(float64); ps != tt.wantPageSize {
				t.Errorf("page_size = %v, want %v", ps, tt.wantPageSize)
			}
		})
	}
}

func TestHandleEngineMessages_CountFailure(t *testing.T) {
	eng := &querytest.MockEngine{
		CountMessagesFunc: func(_ context.Context, _ query.MessageFilter) (int64, error) {
			return 0, fmt.Errorf("database locked")
		},
	}
	srv := newTestServerWithEngine(t, eng)

	req := httptest.NewRequest("GET", "/api/v1/engine/messages", nil)
	w := httptest.NewRecorder()
	srv.Router().ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want %d", w.Code, http.StatusInternalServerError)
	}
}

// --------------------------------------------------------------------------
// handleEngineSearch
// --------------------------------------------------------------------------

func TestHandleEngineSearch_NoEngine(t *testing.T) {
	srv := newTestServerWithEngine(t, nil)

	req := httptest.NewRequest("GET", "/api/v1/engine/search?q=from:alice", nil)
	w := httptest.NewRecorder()
	srv.Router().ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want %d", w.Code, http.StatusServiceUnavailable)
	}
}

func TestHandleEngineSearch_MissingQuery(t *testing.T) {
	eng := &querytest.MockEngine{}
	srv := newTestServerWithEngine(t, eng)

	req := httptest.NewRequest("GET", "/api/v1/engine/search", nil)
	w := httptest.NewRecorder()
	srv.Router().ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", w.Code, http.StatusBadRequest)
	}
}

func TestHandleEngineSearch_Success(t *testing.T) {
	now := time.Date(2024, 1, 20, 8, 0, 0, 0, time.UTC)
	eng := &querytest.MockEngine{
		SearchFastResults: []query.MessageSummary{
			{
				ID:        10,
				Subject:   "Meeting notes",
				FromEmail: "boss@example.com",
				SentAt:    now,
				Labels:    []string{"INBOX", "IMPORTANT"},
			},
		},
	}
	srv := newTestServerWithEngine(t, eng)

	req := httptest.NewRequest("GET", "/api/v1/engine/search?q=from:boss", nil)
	w := httptest.NewRecorder()
	srv.Router().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
	}

	var resp map[string]interface{}
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if q := resp["query"].(string); q != "from:boss" {
		t.Errorf("query = %q, want %q", q, "from:boss")
	}

	msgs, ok := resp["messages"].([]interface{})
	if !ok {
		t.Fatalf("messages missing")
	}
	if len(msgs) != 1 {
		t.Errorf("len(messages) = %d, want 1", len(msgs))
	}

	if total := resp["total"].(float64); total != 1 {
		t.Errorf("total = %v, want 1", total)
	}
}

func TestHandleEngineSearch_Pagination(t *testing.T) {
	eng := &querytest.MockEngine{}
	srv := newTestServerWithEngine(t, eng)

	req := httptest.NewRequest("GET", "/api/v1/engine/search?q=hello&page=2&page_size=25", nil)
	w := httptest.NewRecorder()
	srv.Router().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
	}

	var resp map[string]interface{}
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if page := resp["page"].(float64); page != 2 {
		t.Errorf("page = %v, want 2", page)
	}
	if ps := resp["page_size"].(float64); ps != 25 {
		t.Errorf("page_size = %v, want 25", ps)
	}
}

// --------------------------------------------------------------------------
// toEngineMsgSummary
// --------------------------------------------------------------------------

func TestToEngineMsgSummary_WithName(t *testing.T) {
	now := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	m := query.MessageSummary{
		ID:              5,
		Subject:         "Test subject",
		FromEmail:       "alice@example.com",
		FromName:        "Alice Smith",
		SentAt:          now,
		Snippet:         "hello there",
		Labels:          []string{"INBOX"},
		HasAttachments:  true,
		SizeEstimate:    2048,
		AttachmentCount: 2,
	}

	result := toEngineMsgSummary(m)

	if result.ID != 5 {
		t.Errorf("ID = %d, want 5", result.ID)
	}
	if result.From != "Alice Smith <alice@example.com>" {
		t.Errorf("From = %q", result.From)
	}
	if result.SentAt != now.UTC().Format("2006-01-02T15:04:05Z07:00") {
		t.Errorf("SentAt = %q", result.SentAt)
	}
	if !result.HasAttachments {
		t.Error("HasAttachments should be true")
	}
	if result.SizeBytes != 2048 {
		t.Errorf("SizeBytes = %d, want 2048", result.SizeBytes)
	}
}

func TestToEngineMsgSummary_NilLabels(t *testing.T) {
	m := query.MessageSummary{
		FromEmail: "x@example.com",
		SentAt:    time.Now(),
		Labels:    nil,
	}
	result := toEngineMsgSummary(m)
	if result.Labels == nil {
		t.Error("Labels should be non-nil empty slice, got nil")
	}
	if len(result.Labels) != 0 {
		t.Errorf("Labels should be empty, got %v", result.Labels)
	}
}

func TestToEngineMsgSummary_NoName(t *testing.T) {
	m := query.MessageSummary{
		FromEmail: "noname@example.com",
		SentAt:    time.Now(),
	}
	result := toEngineMsgSummary(m)
	if result.From != "noname@example.com" {
		t.Errorf("From = %q, want %q", result.From, "noname@example.com")
	}
}

// --------------------------------------------------------------------------
// api_key query param auth (PR1 addition to authMiddleware)
// --------------------------------------------------------------------------

func TestAuthMiddleware_APIKeyQueryParam(t *testing.T) {
	cfg := &config.Config{
		Server: config.ServerConfig{
			APIPort: 8080,
			APIKey:  "test-secret",
		},
	}
	sched := newMockScheduler()
	eng := &querytest.MockEngine{}
	srv := NewServer(cfg, &mockStore{}, sched, testLogger())
	srv.WithEngine(eng)

	tests := []struct {
		name       string
		url        string
		wantStatus int
	}{
		{"no auth", "/api/v1/aggregate?group_by=sender", http.StatusUnauthorized},
		{"wrong key", "/api/v1/aggregate?group_by=sender&api_key=wrong", http.StatusUnauthorized},
		{"correct key via query param", "/api/v1/aggregate?group_by=sender&api_key=test-secret", http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", tt.url, nil)
			w := httptest.NewRecorder()
			srv.Router().ServeHTTP(w, req)

			if w.Code != tt.wantStatus {
				t.Errorf("status = %d, want %d", w.Code, tt.wantStatus)
			}
		})
	}
}
