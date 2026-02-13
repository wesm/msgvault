package remote

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestNew_RejectsHTTPWithoutAllowInsecure(t *testing.T) {
	_, err := New(Config{
		URL:    "http://nas:8080",
		APIKey: "key",
	})
	if err == nil {
		t.Fatal("New() should reject http:// without AllowInsecure")
	}
}

func TestNew_AllowsHTTPWithAllowInsecure(t *testing.T) {
	s, err := New(Config{
		URL:           "http://nas:8080",
		APIKey:        "key",
		AllowInsecure: true,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if s == nil {
		t.Fatal("New() returned nil store")
	}
}

func TestNew_AllowsHTTPS(t *testing.T) {
	s, err := New(Config{
		URL:    "https://nas:8080",
		APIKey: "key",
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if s == nil {
		t.Fatal("New() returned nil store")
	}
}

func TestNew_RejectsEmptyURL(t *testing.T) {
	_, err := New(Config{APIKey: "key"})
	if err == nil {
		t.Fatal("New() should reject empty URL")
	}
}

func TestNew_RejectsInvalidScheme(t *testing.T) {
	_, err := New(Config{
		URL:    "ftp://nas:8080",
		APIKey: "key",
	})
	if err == nil {
		t.Fatal("New() should reject ftp:// scheme")
	}
	if !strings.Contains(err.Error(), "http or https") {
		t.Errorf("error = %q, want mention of http or https", err.Error())
	}
}

func TestNew_TrimsTrailingSlash(t *testing.T) {
	s, err := New(Config{
		URL:           "http://nas:8080/",
		APIKey:        "key",
		AllowInsecure: true,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if s.baseURL != "http://nas:8080" {
		t.Errorf("baseURL = %q, want trailing slash trimmed", s.baseURL)
	}
}

func TestNew_DefaultTimeout(t *testing.T) {
	s, err := New(Config{
		URL:    "https://nas:8080",
		APIKey: "key",
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if s.httpClient.Timeout == 0 {
		t.Error("httpClient.Timeout should have a default, got 0")
	}
}

// newTestStore creates a Store pointing at the given httptest server.
func newTestStore(srv *httptest.Server, apiKey string) *Store {
	return &Store{
		baseURL:    srv.URL,
		apiKey:     apiKey,
		httpClient: srv.Client(),
	}
}

func TestDoRequest_SetsAuthHeader(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got := r.Header.Get("X-API-Key")
		if got != "secret-key" {
			t.Errorf("X-API-Key = %q, want %q", got, "secret-key")
		}
		accept := r.Header.Get("Accept")
		if accept != "application/json" {
			t.Errorf("Accept = %q, want application/json", accept)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	s := newTestStore(srv, "secret-key")
	resp, err := s.doRequest("GET", "/test", nil)
	if err != nil {
		t.Fatalf("doRequest error = %v", err)
	}
	resp.Body.Close()
}

func TestDoRequest_OmitsAuthHeaderWhenEmpty(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("X-API-Key"); got != "" {
			t.Errorf("X-API-Key should be empty, got %q", got)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	s := newTestStore(srv, "")
	resp, err := s.doRequest("GET", "/test", nil)
	if err != nil {
		t.Fatalf("doRequest error = %v", err)
	}
	resp.Body.Close()
}

func TestHandleErrorResponse_JSONBody(t *testing.T) {
	body := `{"error":"not_found","message":"Message 42 not found"}`
	resp := &http.Response{
		StatusCode: 404,
		Body:       http.NoBody,
	}
	// Use a real body
	resp.Body = readCloser(body)

	err := handleErrorResponse(resp)
	if err == nil {
		t.Fatal("handleErrorResponse should return error")
	}
	if !strings.Contains(err.Error(), "404") {
		t.Errorf("error should contain status code, got: %s", err.Error())
	}
	if !strings.Contains(err.Error(), "Message 42 not found") {
		t.Errorf("error should contain API message, got: %s", err.Error())
	}
}

func TestHandleErrorResponse_PlainTextBody(t *testing.T) {
	resp := &http.Response{
		StatusCode: 500,
		Body:       readCloser("internal server error"),
	}

	err := handleErrorResponse(resp)
	if err == nil {
		t.Fatal("handleErrorResponse should return error")
	}
	if !strings.Contains(err.Error(), "500") {
		t.Errorf("error should contain status code, got: %s", err.Error())
	}
	if !strings.Contains(err.Error(), "internal server error") {
		t.Errorf("error should contain body text, got: %s", err.Error())
	}
}

func TestGetStats_ErrorResponse(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"db_error","message":"database locked"}`))
	}))
	defer srv.Close()

	s := newTestStore(srv, "key")
	_, err := s.GetStats()
	if err == nil {
		t.Fatal("GetStats should return error on 500")
	}
	if !strings.Contains(err.Error(), "database locked") {
		t.Errorf("error = %q, want mention of 'database locked'", err.Error())
	}
}

func TestGetStats_Success(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(statsResponse{
			TotalMessages: 100,
			TotalThreads:  50,
			TotalAccounts: 2,
			TotalLabels:   10,
			TotalAttach:   5,
			DatabaseSize:  1024,
		})
	}))
	defer srv.Close()

	s := newTestStore(srv, "key")
	stats, err := s.GetStats()
	if err != nil {
		t.Fatalf("GetStats error = %v", err)
	}
	if stats.MessageCount != 100 {
		t.Errorf("MessageCount = %d, want 100", stats.MessageCount)
	}
	if stats.ThreadCount != 50 {
		t.Errorf("ThreadCount = %d, want 50", stats.ThreadCount)
	}
	if stats.SourceCount != 2 {
		t.Errorf("SourceCount = %d, want 2", stats.SourceCount)
	}
}

func TestGetMessage_NotFound(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	s := newTestStore(srv, "key")
	msg, err := s.GetMessage(999)
	if err != nil {
		t.Fatalf("GetMessage error = %v", err)
	}
	if msg != nil {
		t.Errorf("GetMessage(999) = %v, want nil for not found", msg)
	}
}

func TestGetMessage_Success(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/messages/42" {
			t.Errorf("path = %q, want /api/v1/messages/42", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(messageDetailResponse{
			messageResponse: messageResponse{
				ID:      42,
				Subject: "Test Subject",
				From:    "sender@example.com",
				To:      []string{"receiver@example.com"},
				SentAt:  "2024-01-15T10:30:00Z",
			},
			Body: "Hello, world!",
			Attachments: []attachmentResponse{
				{Filename: "doc.pdf", MimeType: "application/pdf", Size: 1024},
			},
		})
	}))
	defer srv.Close()

	s := newTestStore(srv, "key")
	msg, err := s.GetMessage(42)
	if err != nil {
		t.Fatalf("GetMessage error = %v", err)
	}
	if msg == nil {
		t.Fatal("GetMessage returned nil")
	}
	if msg.Subject != "Test Subject" {
		t.Errorf("Subject = %q, want %q", msg.Subject, "Test Subject")
	}
	if msg.Body != "Hello, world!" {
		t.Errorf("Body = %q, want %q", msg.Body, "Hello, world!")
	}
	if len(msg.Attachments) != 1 {
		t.Fatalf("len(Attachments) = %d, want 1", len(msg.Attachments))
	}
	if msg.Attachments[0].Filename != "doc.pdf" {
		t.Errorf("Attachments[0].Filename = %q, want doc.pdf", msg.Attachments[0].Filename)
	}
}

func TestListMessages_ZeroLimit(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ps := r.URL.Query().Get("page_size")
		if ps == "0" {
			t.Error("page_size should not be 0")
		}
		resp := listMessagesResponse{
			Total:    0,
			Page:     1,
			PageSize: 20,
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer srv.Close()

	s := newTestStore(srv, "test")

	// This previously panicked with divide-by-zero
	msgs, total, err := s.ListMessages(0, 0)
	if err != nil {
		t.Fatalf("ListMessages(0, 0) error = %v", err)
	}
	if total != 0 {
		t.Errorf("total = %d, want 0", total)
	}
	if len(msgs) != 0 {
		t.Errorf("len(msgs) = %d, want 0", len(msgs))
	}
}

func TestListMessages_NegativeLimit(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ps := r.URL.Query().Get("page_size")
		if ps != "20" {
			t.Errorf("page_size = %q, want 20 (default)", ps)
		}
		resp := listMessagesResponse{Total: 0, Page: 1, PageSize: 20}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer srv.Close()

	s := newTestStore(srv, "test")

	_, _, err := s.ListMessages(0, -5)
	if err != nil {
		t.Fatalf("ListMessages(0, -5) error = %v", err)
	}
}

func TestSearchMessages_ZeroLimit(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ps := r.URL.Query().Get("page_size")
		if ps == "0" {
			t.Error("page_size should not be 0")
		}
		resp := searchResponse{
			Query:    "test",
			Total:    0,
			Page:     1,
			PageSize: 20,
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer srv.Close()

	s := newTestStore(srv, "test")

	// This previously panicked with divide-by-zero
	msgs, total, err := s.SearchMessages("test", 0, 0)
	if err != nil {
		t.Fatalf("SearchMessages(test, 0, 0) error = %v", err)
	}
	if total != 0 {
		t.Errorf("total = %d, want 0", total)
	}
	if len(msgs) != 0 {
		t.Errorf("len(msgs) = %d, want 0", len(msgs))
	}
}

func TestSearchMessages_QueryEncoding(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("q")
		if q != "hello world" {
			t.Errorf("q = %q, want %q", q, "hello world")
		}
		resp := searchResponse{Total: 0, Page: 1, PageSize: 20}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer srv.Close()

	s := newTestStore(srv, "test")
	_, _, err := s.SearchMessages("hello world", 0, 20)
	if err != nil {
		t.Fatalf("SearchMessages error = %v", err)
	}
}

func TestListMessages_PageCalculation(t *testing.T) {
	tests := []struct {
		name     string
		offset   int
		limit    int
		wantPage string
		wantSize string
	}{
		{"first page", 0, 20, "1", "20"},
		{"second page", 20, 20, "2", "20"},
		{"third page", 40, 20, "3", "20"},
		{"small pages", 10, 10, "2", "10"},
		{"zero limit defaults", 0, 0, "1", "20"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				page := r.URL.Query().Get("page")
				ps := r.URL.Query().Get("page_size")
				if page != tt.wantPage {
					t.Errorf("page = %q, want %q", page, tt.wantPage)
				}
				if ps != tt.wantSize {
					t.Errorf("page_size = %q, want %q", ps, tt.wantSize)
				}
				resp := listMessagesResponse{Total: 0, Page: 1, PageSize: 20}
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(resp)
			}))
			defer srv.Close()

			s := newTestStore(srv, "test")

			_, _, err := s.ListMessages(tt.offset, tt.limit)
			if err != nil {
				t.Fatalf("ListMessages(%d, %d) error = %v", tt.offset, tt.limit, err)
			}
		})
	}
}

func TestListAccounts_Success(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/accounts" {
			t.Errorf("path = %q, want /api/v1/accounts", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(accountsResponse{
			Accounts: []AccountInfo{
				{Email: "user@gmail.com", Enabled: true, Schedule: "0 2 * * *"},
			},
		})
	}))
	defer srv.Close()

	s := newTestStore(srv, "key")
	accounts, err := s.ListAccounts()
	if err != nil {
		t.Fatalf("ListAccounts error = %v", err)
	}
	if len(accounts) != 1 {
		t.Fatalf("len(accounts) = %d, want 1", len(accounts))
	}
	if accounts[0].Email != "user@gmail.com" {
		t.Errorf("Email = %q, want user@gmail.com", accounts[0].Email)
	}
}

// readCloser wraps a string in an io.ReadCloser.
func readCloser(s string) *readCloserImpl {
	return &readCloserImpl{r: strings.NewReader(s)}
}

type readCloserImpl struct {
	r *strings.Reader
}

func (rc *readCloserImpl) Read(p []byte) (int, error) {
	return rc.r.Read(p)
}

func (rc *readCloserImpl) Close() error {
	return nil
}
