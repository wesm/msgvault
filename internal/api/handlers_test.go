package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/wesm/msgvault/internal/config"
	"github.com/wesm/msgvault/internal/query"
	"github.com/wesm/msgvault/internal/query/querytest"
)

func newTestServerWithMockStore(t *testing.T) (*Server, *mockStore) {
	t.Helper()

	store := &mockStore{
		stats: &StoreStats{
			MessageCount:    10,
			ThreadCount:     5,
			SourceCount:     1,
			LabelCount:      3,
			AttachmentCount: 2,
			DatabaseSize:    1024,
		},
		messages: []APIMessage{
			{
				ID:             1,
				Subject:        "Test Subject",
				From:           "sender@example.com",
				To:             []string{"recipient@example.com"},
				SentAt:         time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
				Snippet:        "This is a test message snippet",
				Labels:         []string{"INBOX"},
				HasAttachments: false,
				SizeEstimate:   1024,
				Body:           "This is the full message body text.",
				Attachments:    nil,
			},
		},
		total: 1,
	}

	cfg := &config.Config{
		Server: config.ServerConfig{APIPort: 8080},
		Accounts: []config.AccountSchedule{
			{Email: "test@gmail.com", Schedule: "0 2 * * *", Enabled: true},
		},
	}

	sched := newMockScheduler()
	sched.scheduled["test@gmail.com"] = true

	srv := NewServer(cfg, store, sched, testLogger())
	return srv, store
}

func TestHandleStats(t *testing.T) {
	srv, _ := newTestServerWithMockStore(t)

	req := httptest.NewRequest("GET", "/api/v1/stats", nil)
	w := httptest.NewRecorder()

	srv.Router().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", w.Code, http.StatusOK)
	}

	var resp StatsResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if resp.TotalMessages != 10 {
		t.Errorf("total_messages = %d, want 10", resp.TotalMessages)
	}
	if resp.TotalAccounts != 1 {
		t.Errorf("total_accounts = %d, want 1", resp.TotalAccounts)
	}
}

func TestHandleListMessages(t *testing.T) {
	srv, _ := newTestServerWithMockStore(t)

	req := httptest.NewRequest("GET", "/api/v1/messages", nil)
	w := httptest.NewRecorder()

	srv.Router().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", w.Code, http.StatusOK)
	}

	var resp map[string]interface{}
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	messages, ok := resp["messages"].([]interface{})
	if !ok {
		t.Fatal("expected messages array in response")
	}

	if len(messages) == 0 {
		t.Error("expected at least 1 message")
	}

	// Check first message structure
	msg := messages[0].(map[string]interface{})
	if msg["subject"] != "Test Subject" {
		t.Errorf("subject = %v, want 'Test Subject'", msg["subject"])
	}

	// Verify RFC3339 time format
	sentAt := msg["sent_at"].(string)
	if _, err := time.Parse(time.RFC3339, sentAt); err != nil {
		t.Errorf("sent_at %q is not RFC3339: %v", sentAt, err)
	}
}

func TestHandleListMessagesPagination(t *testing.T) {
	srv, _ := newTestServerWithMockStore(t)

	req := httptest.NewRequest("GET", "/api/v1/messages?page=1&page_size=10", nil)
	w := httptest.NewRecorder()

	srv.Router().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", w.Code, http.StatusOK)
	}

	var resp map[string]interface{}
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if resp["page"] != float64(1) {
		t.Errorf("page = %v, want 1", resp["page"])
	}
	if resp["page_size"] != float64(10) {
		t.Errorf("page_size = %v, want 10", resp["page_size"])
	}
}

func TestHandleGetMessage(t *testing.T) {
	srv, _ := newTestServerWithMockStore(t)

	req := httptest.NewRequest("GET", "/api/v1/messages/1", nil)
	w := httptest.NewRecorder()

	srv.Router().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", w.Code, http.StatusOK)
	}

	var resp MessageDetail
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if resp.ID != 1 {
		t.Errorf("id = %d, want 1", resp.ID)
	}
	if resp.Subject != "Test Subject" {
		t.Errorf("subject = %q, want 'Test Subject'", resp.Subject)
	}
	if resp.Body != "This is the full message body text." {
		t.Errorf("body = %q, want 'This is the full message body text.'", resp.Body)
	}
}

func TestHandleGetMessageNotFound(t *testing.T) {
	srv, _ := newTestServerWithMockStore(t)

	req := httptest.NewRequest("GET", "/api/v1/messages/99999", nil)
	w := httptest.NewRecorder()

	srv.Router().ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want %d", w.Code, http.StatusNotFound)
	}
}

func TestHandleGetMessageInvalidID(t *testing.T) {
	srv, _ := newTestServerWithMockStore(t)

	req := httptest.NewRequest("GET", "/api/v1/messages/invalid", nil)
	w := httptest.NewRecorder()

	srv.Router().ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", w.Code, http.StatusBadRequest)
	}
}

func TestHandleSearchMissingQuery(t *testing.T) {
	srv, _ := newTestServerWithMockStore(t)

	req := httptest.NewRequest("GET", "/api/v1/search", nil)
	w := httptest.NewRecorder()

	srv.Router().ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", w.Code, http.StatusBadRequest)
	}
}

func TestHandleSearch(t *testing.T) {
	srv, _ := newTestServerWithMockStore(t)

	req := httptest.NewRequest("GET", "/api/v1/search?q=Test", nil)
	w := httptest.NewRecorder()

	srv.Router().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", w.Code, http.StatusOK)
	}

	var resp SearchResult
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if resp.Query != "Test" {
		t.Errorf("query = %q, want 'Test'", resp.Query)
	}
}

func TestHandleTriggerSync(t *testing.T) {
	cfg := &config.Config{
		Server: config.ServerConfig{APIPort: 8080},
		Accounts: []config.AccountSchedule{
			{Email: "test@gmail.com", Schedule: "0 2 * * *", Enabled: true},
		},
	}

	sched := newMockScheduler()
	sched.scheduled["test@gmail.com"] = true

	srv := NewServer(cfg, nil, sched, testLogger())

	req := httptest.NewRequest("POST", "/api/v1/sync/test@gmail.com", nil)
	w := httptest.NewRecorder()

	srv.Router().ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Errorf("status = %d, want %d, body: %s", w.Code, http.StatusAccepted, w.Body.String())
	}
}

func TestHandleTriggerSyncNotFound(t *testing.T) {
	cfg := &config.Config{
		Server: config.ServerConfig{APIPort: 8080},
	}

	sched := newMockScheduler()
	srv := NewServer(cfg, nil, sched, testLogger())

	req := httptest.NewRequest("POST", "/api/v1/sync/unknown@gmail.com", nil)
	w := httptest.NewRecorder()

	srv.Router().ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want %d", w.Code, http.StatusNotFound)
	}
}

func TestHandleTriggerSyncConflict(t *testing.T) {
	cfg := &config.Config{
		Server: config.ServerConfig{APIPort: 8080},
	}

	sched := newMockScheduler()
	sched.scheduled["test@gmail.com"] = true
	sched.triggerFn = func(email string) error {
		return errors.New("sync already running for test@gmail.com")
	}

	srv := NewServer(cfg, nil, sched, testLogger())

	req := httptest.NewRequest("POST", "/api/v1/sync/test@gmail.com", nil)
	w := httptest.NewRecorder()

	srv.Router().ServeHTTP(w, req)

	if w.Code != http.StatusConflict {
		t.Errorf("status = %d, want %d", w.Code, http.StatusConflict)
	}
}

func TestErrorResponseShape(t *testing.T) {
	srv, _ := newTestServerWithMockStore(t)

	// Test with invalid ID to get a 400 error
	req := httptest.NewRequest("GET", "/api/v1/messages/invalid", nil)
	w := httptest.NewRecorder()

	srv.Router().ServeHTTP(w, req)

	var resp ErrorResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode error response: %v", err)
	}

	if resp.Error == "" {
		t.Error("expected error code in response")
	}
	if resp.Message == "" {
		t.Error("expected error message in response")
	}
}

func TestMessageSummaryNilSlices(t *testing.T) {
	store := &mockStore{
		messages: []APIMessage{
			{
				ID:      1,
				Subject: "No recipients",
				To:      nil,
				Labels:  nil,
			},
		},
		total: 1,
	}

	cfg := &config.Config{
		Server: config.ServerConfig{APIPort: 8080},
	}
	sched := newMockScheduler()
	srv := NewServer(cfg, store, sched, testLogger())

	req := httptest.NewRequest("GET", "/api/v1/messages", nil)
	w := httptest.NewRecorder()

	srv.Router().ServeHTTP(w, req)

	// Verify nil slices become empty arrays, not null
	var resp map[string]interface{}
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	messages := resp["messages"].([]interface{})
	msg := messages[0].(map[string]interface{})

	// "to" should be an empty array, not null
	to, ok := msg["to"].([]interface{})
	if !ok {
		t.Fatalf("expected 'to' to be an array, got %T", msg["to"])
	}
	if len(to) != 0 {
		t.Errorf("expected empty 'to' array, got %v", to)
	}

	labels, ok := msg["labels"].([]interface{})
	if !ok {
		t.Fatalf("expected 'labels' to be an array, got %T", msg["labels"])
	}
	if len(labels) != 0 {
		t.Errorf("expected empty 'labels' array, got %v", labels)
	}
}

func TestHandleUploadToken(t *testing.T) {
	// Create temp directory for tokens
	tmpDir, err := os.MkdirTemp("", "msgvault-test-tokens-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := &config.Config{
		Server: config.ServerConfig{APIPort: 8080},
		Data:   config.DataConfig{DataDir: tmpDir},
	}
	sched := newMockScheduler()
	srv := NewServer(cfg, nil, sched, testLogger())

	tokenJSON := `{
		"access_token": "ya29.test",
		"token_type": "Bearer",
		"refresh_token": "1//test-refresh-token",
		"expiry": "2024-12-31T23:59:59Z"
	}`

	req := httptest.NewRequest("POST", "/api/v1/auth/token/test@gmail.com", strings.NewReader(tokenJSON))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	srv.Router().ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Errorf("status = %d, want %d, body: %s", w.Code, http.StatusCreated, w.Body.String())
	}

	// Verify token file was created
	tokenPath := filepath.Join(tmpDir, "tokens", "test@gmail.com.json")
	if _, err := os.Stat(tokenPath); os.IsNotExist(err) {
		t.Errorf("token file was not created at %s", tokenPath)
	}
}

func TestHandleUploadTokenInvalidJSON(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "msgvault-test-tokens-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := &config.Config{
		Server: config.ServerConfig{APIPort: 8080},
		Data:   config.DataConfig{DataDir: tmpDir},
	}
	sched := newMockScheduler()
	srv := NewServer(cfg, nil, sched, testLogger())

	req := httptest.NewRequest("POST", "/api/v1/auth/token/test@gmail.com", strings.NewReader("not valid json"))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	srv.Router().ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", w.Code, http.StatusBadRequest)
	}

	var resp ErrorResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode error response: %v", err)
	}
	if resp.Error != "invalid_json" {
		t.Errorf("error = %q, want 'invalid_json'", resp.Error)
	}
}

func TestHandleUploadTokenMissingRefreshToken(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "msgvault-test-tokens-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := &config.Config{
		Server: config.ServerConfig{APIPort: 8080},
		Data:   config.DataConfig{DataDir: tmpDir},
	}
	sched := newMockScheduler()
	srv := NewServer(cfg, nil, sched, testLogger())

	// Token without refresh_token
	tokenJSON := `{
		"access_token": "ya29.test",
		"token_type": "Bearer"
	}`

	req := httptest.NewRequest("POST", "/api/v1/auth/token/test@gmail.com", strings.NewReader(tokenJSON))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	srv.Router().ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", w.Code, http.StatusBadRequest)
	}

	var resp ErrorResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode error response: %v", err)
	}
	if resp.Error != "invalid_token" {
		t.Errorf("error = %q, want 'invalid_token'", resp.Error)
	}
}

func TestHandleUploadTokenInvalidEmail(t *testing.T) {
	cfg := &config.Config{
		Server: config.ServerConfig{APIPort: 8080},
	}
	sched := newMockScheduler()
	srv := NewServer(cfg, nil, sched, testLogger())

	tokenJSON := `{"refresh_token": "test"}`

	tests := []struct {
		name  string
		email string
	}{
		{"no at sign", "testgmail.com"},
		{"no domain", "test@"},
		{"no dot", "test@gmailcom"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest("POST", "/api/v1/auth/token/"+tc.email, strings.NewReader(tokenJSON))
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()

			srv.Router().ServeHTTP(w, req)

			if w.Code != http.StatusBadRequest {
				t.Errorf("status = %d, want %d for email %q", w.Code, http.StatusBadRequest, tc.email)
			}
		})
	}
}

func TestHandleUploadTokenMissingEmail(t *testing.T) {
	cfg := &config.Config{
		Server: config.ServerConfig{APIPort: 8080},
	}
	sched := newMockScheduler()
	srv := NewServer(cfg, nil, sched, testLogger())

	// Request without email in path - should 404 since route doesn't match
	req := httptest.NewRequest("POST", "/api/v1/auth/token/", strings.NewReader("{}"))
	w := httptest.NewRecorder()

	srv.Router().ServeHTTP(w, req)

	// Chi router will 404 on missing path parameter
	if w.Code != http.StatusNotFound && w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 404 or 400", w.Code)
	}
}

func TestHandleAddAccount(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "msgvault-test-config-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := &config.Config{
		Server:  config.ServerConfig{APIPort: 8080},
		HomeDir: tmpDir,
	}
	sched := newMockScheduler()
	srv := NewServer(cfg, nil, sched, testLogger())

	body := `{"email": "new@gmail.com", "schedule": "0 3 * * *"}`
	req := httptest.NewRequest("POST", "/api/v1/accounts", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	srv.Router().ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d, body: %s", w.Code, http.StatusCreated, w.Body.String())
	}

	// Verify account was added to config
	if len(cfg.Accounts) != 1 {
		t.Fatalf("expected 1 account, got %d", len(cfg.Accounts))
	}
	if cfg.Accounts[0].Email != "new@gmail.com" {
		t.Errorf("email = %q, want 'new@gmail.com'", cfg.Accounts[0].Email)
	}

	// Verify scheduler was notified
	if len(sched.addedAccts) != 1 || sched.addedAccts[0] != "new@gmail.com" {
		t.Errorf("scheduler.AddAccount not called, addedAccts = %v", sched.addedAccts)
	}
}

func TestHandleAddAccountDuplicate(t *testing.T) {
	cfg := &config.Config{
		Server: config.ServerConfig{APIPort: 8080},
		Accounts: []config.AccountSchedule{
			{Email: "existing@gmail.com", Schedule: "0 2 * * *", Enabled: true},
		},
	}
	sched := newMockScheduler()
	srv := NewServer(cfg, nil, sched, testLogger())

	body := `{"email": "existing@gmail.com"}`
	req := httptest.NewRequest("POST", "/api/v1/accounts", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	srv.Router().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", w.Code, http.StatusOK)
	}

	var resp map[string]string
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp["status"] != "exists" {
		t.Errorf("status = %q, want 'exists'", resp["status"])
	}
}

func TestHandleAddAccountInvalidCron(t *testing.T) {
	cfg := &config.Config{
		Server: config.ServerConfig{APIPort: 8080},
	}
	sched := newMockScheduler()
	srv := NewServer(cfg, nil, sched, testLogger())

	body := `{"email": "new@gmail.com", "schedule": "not a cron"}`
	req := httptest.NewRequest("POST", "/api/v1/accounts", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	srv.Router().ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d, body: %s", w.Code, http.StatusBadRequest, w.Body.String())
	}

	var resp ErrorResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Error != "invalid_schedule" {
		t.Errorf("error = %q, want 'invalid_schedule'", resp.Error)
	}
}

func TestHandleAddAccountInvalidEmail(t *testing.T) {
	cfg := &config.Config{
		Server: config.ServerConfig{APIPort: 8080},
	}
	sched := newMockScheduler()
	srv := NewServer(cfg, nil, sched, testLogger())

	tests := []struct {
		name string
		body string
		code int
	}{
		{"empty email", `{"email": ""}`, http.StatusBadRequest},
		{"no at sign", `{"email": "nope"}`, http.StatusBadRequest},
		{"no dot", `{"email": "nope@nope"}`, http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("POST", "/api/v1/accounts", strings.NewReader(tt.body))
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			srv.Router().ServeHTTP(w, req)

			if w.Code != tt.code {
				t.Errorf("status = %d, want %d", w.Code, tt.code)
			}
		})
	}
}

func TestHandleAddAccountSaveFailure(t *testing.T) {
	// Point HomeDir to a file (not a directory) so Save() fails
	tmpFile := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(tmpFile, []byte("x"), 0600); err != nil {
		t.Fatalf("create blocker file: %v", err)
	}

	cfg := &config.Config{
		Server:  config.ServerConfig{APIPort: 8080},
		HomeDir: tmpFile, // Save() will fail: can't mkdir inside a file
	}
	sched := newMockScheduler()
	srv := NewServer(cfg, nil, sched, testLogger())

	body := `{"email": "fail@gmail.com", "schedule": "0 2 * * *"}`
	req := httptest.NewRequest("POST", "/api/v1/accounts", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	srv.Router().ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want %d", w.Code, http.StatusInternalServerError)
	}

	// In-memory state should be rolled back
	if len(cfg.Accounts) != 0 {
		t.Errorf("cfg.Accounts has %d entries, want 0 (rollback failed)", len(cfg.Accounts))
	}
}

func TestSanitizeTokenPath(t *testing.T) {
	tokensDir := "/data/tokens"

	tests := []struct {
		name  string
		email string
	}{
		{"normal email", "user@gmail.com"},
		{"email with plus", "user+tag@gmail.com"},
		{"email with dots", "first.last@gmail.com"},
		{"path traversal attempt", "../../../etc/passwd"},
		{"slash in email", "user/evil@gmail.com"},
		{"backslash in email", "user\\evil@gmail.com"},
		{"null byte", "user\x00evil@gmail.com"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := sanitizeTokenPath(tokensDir, tc.email)

			// Result must be within tokensDir (path traversal prevention)
			cleanResult := filepath.Clean(result)
			cleanTokensDir := filepath.Clean(tokensDir)
			if !strings.HasPrefix(cleanResult, cleanTokensDir+string(os.PathSeparator)) {
				t.Errorf("path %q escapes tokensDir %q", result, tokensDir)
			}

			// Result must end with .json
			if !strings.HasSuffix(result, ".json") {
				t.Errorf("path %q doesn't end with .json", result)
			}

			// Result must not contain path separators in the filename
			base := filepath.Base(result)
			if strings.ContainsAny(base, "/\\") {
				t.Errorf("filename %q contains path separators", base)
			}
		})
	}
}

// newTestServerWithEngine creates a test server with both mock store and mock engine.
func newTestServerWithEngine(t *testing.T, engine *querytest.MockEngine) *Server {
	t.Helper()

	store := &mockStore{
		stats: &StoreStats{
			MessageCount:    10,
			ThreadCount:     5,
			SourceCount:     1,
			LabelCount:      3,
			AttachmentCount: 2,
			DatabaseSize:    1024,
		},
		messages: []APIMessage{
			{
				ID:             1,
				Subject:        "Test Subject",
				From:           "sender@example.com",
				To:             []string{"recipient@example.com"},
				SentAt:         time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
				Snippet:        "This is a test message snippet",
				Labels:         []string{"INBOX"},
				HasAttachments: false,
				SizeEstimate:   1024,
				Body:           "This is the full message body text.",
			},
		},
		total: 1,
	}

	cfg := &config.Config{
		Server: config.ServerConfig{APIPort: 8080},
	}
	sched := newMockScheduler()

	srv := NewServerWithOptions(ServerOptions{
		Config:    cfg,
		Store:     store,
		Engine:    engine,
		Scheduler: sched,
		Logger:    testLogger(),
	})
	return srv
}

func TestHandleAggregates(t *testing.T) {
	engine := &querytest.MockEngine{
		AggregateRows: []query.AggregateRow{
			{Key: "alice@example.com", Count: 100, TotalSize: 50000, AttachmentSize: 10000, AttachmentCount: 5},
			{Key: "bob@example.com", Count: 50, TotalSize: 25000, AttachmentSize: 5000, AttachmentCount: 2},
		},
	}
	srv := newTestServerWithEngine(t, engine)

	req := httptest.NewRequest("GET", "/api/v1/aggregates?view_type=senders", nil)
	w := httptest.NewRecorder()

	srv.Router().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want %d, body: %s", w.Code, http.StatusOK, w.Body.String())
	}

	var resp AggregateResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if resp.ViewType != "senders" {
		t.Errorf("view_type = %q, want 'senders'", resp.ViewType)
	}
	if len(resp.Rows) != 2 {
		t.Errorf("rows count = %d, want 2", len(resp.Rows))
	}
	if resp.Rows[0].Key != "alice@example.com" {
		t.Errorf("first row key = %q, want 'alice@example.com'", resp.Rows[0].Key)
	}
}

func TestHandleAggregatesNoEngine(t *testing.T) {
	// Server without engine
	cfg := &config.Config{
		Server: config.ServerConfig{APIPort: 8080},
	}
	sched := newMockScheduler()
	srv := NewServerWithOptions(ServerOptions{
		Config:    cfg,
		Store:     nil,
		Engine:    nil,
		Scheduler: sched,
		Logger:    testLogger(),
	})

	req := httptest.NewRequest("GET", "/api/v1/aggregates?view_type=senders", nil)
	w := httptest.NewRecorder()

	srv.Router().ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want %d", w.Code, http.StatusServiceUnavailable)
	}
}

func TestHandleAggregatesInvalidViewType(t *testing.T) {
	engine := &querytest.MockEngine{}
	srv := newTestServerWithEngine(t, engine)

	req := httptest.NewRequest("GET", "/api/v1/aggregates?view_type=invalid", nil)
	w := httptest.NewRecorder()

	srv.Router().ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", w.Code, http.StatusBadRequest)
	}
}

func TestHandleSubAggregates(t *testing.T) {
	engine := &querytest.MockEngine{
		AggregateRows: []query.AggregateRow{
			{Key: "INBOX", Count: 80, TotalSize: 40000},
			{Key: "SENT", Count: 20, TotalSize: 10000},
		},
	}
	srv := newTestServerWithEngine(t, engine)

	req := httptest.NewRequest("GET", "/api/v1/aggregates/sub?view_type=labels&sender=alice@example.com", nil)
	w := httptest.NewRecorder()

	srv.Router().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want %d, body: %s", w.Code, http.StatusOK, w.Body.String())
	}

	var resp AggregateResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if resp.ViewType != "labels" {
		t.Errorf("view_type = %q, want 'labels'", resp.ViewType)
	}
	if len(resp.Rows) != 2 {
		t.Errorf("rows count = %d, want 2", len(resp.Rows))
	}
}

func TestHandleFilteredMessages(t *testing.T) {
	engine := &querytest.MockEngine{
		ListResults: []query.MessageSummary{
			{
				ID:             1,
				Subject:        "Test Email",
				FromEmail:      "alice@example.com",
				SentAt:         time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
				Labels:         []string{"INBOX"},
				HasAttachments: false,
				SizeEstimate:   1024,
			},
		},
	}
	srv := newTestServerWithEngine(t, engine)

	req := httptest.NewRequest("GET", "/api/v1/messages/filter?sender=alice@example.com&limit=100", nil)
	w := httptest.NewRecorder()

	srv.Router().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want %d, body: %s", w.Code, http.StatusOK, w.Body.String())
	}

	var resp map[string]interface{}
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	messages, ok := resp["messages"].([]interface{})
	if !ok {
		t.Fatal("expected messages array in response")
	}
	if len(messages) != 1 {
		t.Errorf("messages count = %d, want 1", len(messages))
	}
}

func TestHandleTotalStats(t *testing.T) {
	engine := &querytest.MockEngine{
		Stats: &query.TotalStats{
			MessageCount:    1000,
			TotalSize:       5000000,
			AttachmentCount: 100,
			AttachmentSize:  1000000,
			LabelCount:      10,
			AccountCount:    2,
		},
	}
	srv := newTestServerWithEngine(t, engine)

	req := httptest.NewRequest("GET", "/api/v1/stats/total", nil)
	w := httptest.NewRecorder()

	srv.Router().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want %d, body: %s", w.Code, http.StatusOK, w.Body.String())
	}

	var resp TotalStatsResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if resp.MessageCount != 1000 {
		t.Errorf("message_count = %d, want 1000", resp.MessageCount)
	}
	if resp.TotalSize != 5000000 {
		t.Errorf("total_size = %d, want 5000000", resp.TotalSize)
	}
}

func TestHandleFastSearch(t *testing.T) {
	engine := &querytest.MockEngine{
		SearchFastResults: []query.MessageSummary{
			{
				ID:        1,
				Subject:   "Invoice 12345",
				FromEmail: "billing@example.com",
				SentAt:    time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
			},
		},
		Stats: &query.TotalStats{
			MessageCount: 1,
			TotalSize:    1024,
		},
	}
	srv := newTestServerWithEngine(t, engine)

	req := httptest.NewRequest("GET", "/api/v1/search/fast?q=invoice", nil)
	w := httptest.NewRecorder()

	srv.Router().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want %d, body: %s", w.Code, http.StatusOK, w.Body.String())
	}

	var resp SearchFastResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if resp.Query != "invoice" {
		t.Errorf("query = %q, want 'invoice'", resp.Query)
	}
	if len(resp.Messages) != 1 {
		t.Errorf("messages count = %d, want 1", len(resp.Messages))
	}
}

func TestHandleFastSearchMissingQuery(t *testing.T) {
	engine := &querytest.MockEngine{}
	srv := newTestServerWithEngine(t, engine)

	req := httptest.NewRequest("GET", "/api/v1/search/fast", nil)
	w := httptest.NewRecorder()

	srv.Router().ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", w.Code, http.StatusBadRequest)
	}
}

func TestHandleDeepSearch(t *testing.T) {
	engine := &querytest.MockEngine{
		SearchResults: []query.MessageSummary{
			{
				ID:        1,
				Subject:   "Meeting Notes",
				FromEmail: "team@example.com",
				SentAt:    time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
			},
		},
	}
	srv := newTestServerWithEngine(t, engine)

	req := httptest.NewRequest("GET", "/api/v1/search/deep?q=agenda", nil)
	w := httptest.NewRecorder()

	srv.Router().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want %d, body: %s", w.Code, http.StatusOK, w.Body.String())
	}

	var resp map[string]interface{}
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if resp["query"] != "agenda" {
		t.Errorf("query = %v, want 'agenda'", resp["query"])
	}
}

func TestHandleDeepSearchMissingQuery(t *testing.T) {
	engine := &querytest.MockEngine{}
	srv := newTestServerWithEngine(t, engine)

	req := httptest.NewRequest("GET", "/api/v1/search/deep", nil)
	w := httptest.NewRecorder()

	srv.Router().ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", w.Code, http.StatusBadRequest)
	}
}
