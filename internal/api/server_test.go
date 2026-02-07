package api

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/wesm/msgvault/internal/config"
)

// testLogger returns a logger for tests that discards output
func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

// mockScheduler implements SyncScheduler for tests.
type mockScheduler struct {
	scheduled map[string]bool
	running   bool
	statuses  []AccountStatus
	triggerFn func(email string) error
}

func newMockScheduler() *mockScheduler {
	return &mockScheduler{
		scheduled: make(map[string]bool),
		running:   true,
	}
}

func (m *mockScheduler) IsScheduled(email string) bool {
	return m.scheduled[email]
}

func (m *mockScheduler) TriggerSync(email string) error {
	if m.triggerFn != nil {
		return m.triggerFn(email)
	}
	return nil
}

func (m *mockScheduler) Status() []AccountStatus {
	return m.statuses
}

func (m *mockScheduler) IsRunning() bool {
	return m.running
}

// mockStore implements MessageStore for tests.
type mockStore struct {
	stats    *StoreStats
	messages []APIMessage
	total    int64
}

func (m *mockStore) GetStats() (*StoreStats, error) {
	if m.stats == nil {
		return &StoreStats{}, nil
	}
	return m.stats, nil
}

func (m *mockStore) ListMessages(offset, limit int) ([]APIMessage, int64, error) {
	return m.messages, m.total, nil
}

func (m *mockStore) GetMessage(id int64) (*APIMessage, error) {
	for _, msg := range m.messages {
		if msg.ID == id {
			return &msg, nil
		}
	}
	return nil, nil
}

func (m *mockStore) SearchMessages(query string, offset, limit int) ([]APIMessage, int64, error) {
	return m.messages, m.total, nil
}

func TestHealthEndpoint(t *testing.T) {
	cfg := &config.Config{
		Server: config.ServerConfig{APIPort: 8080},
	}
	sched := newMockScheduler()
	srv := NewServer(cfg, nil, sched, testLogger())

	req := httptest.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()

	srv.Router().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("GET /health status = %d, want %d", w.Code, http.StatusOK)
	}

	var resp map[string]string
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if resp["status"] != "ok" {
		t.Errorf("health status = %q, want 'ok'", resp["status"])
	}
}

func TestAuthMiddleware(t *testing.T) {
	cfg := &config.Config{
		Server: config.ServerConfig{
			APIPort: 8080,
			APIKey:  "secret-key",
		},
	}
	sched := newMockScheduler()
	srv := NewServer(cfg, nil, sched, testLogger())

	tests := []struct {
		name       string
		authHeader string
		wantStatus int
	}{
		{"no auth", "", http.StatusUnauthorized},
		{"wrong key", "wrong-key", http.StatusUnauthorized},
		{"correct key", "secret-key", http.StatusServiceUnavailable}, // 503 because scheduler returns statuses but no store
		{"bearer prefix", "Bearer secret-key", http.StatusServiceUnavailable},
		{"x-api-key header", "secret-key", http.StatusServiceUnavailable},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/api/v1/stats", nil)
			if tt.authHeader != "" {
				if tt.name == "x-api-key header" {
					req.Header.Set("X-API-Key", tt.authHeader)
				} else {
					req.Header.Set("Authorization", tt.authHeader)
				}
			}
			w := httptest.NewRecorder()

			srv.Router().ServeHTTP(w, req)

			if w.Code != tt.wantStatus {
				t.Errorf("status = %d, want %d", w.Code, tt.wantStatus)
			}
		})
	}
}

func TestAuthMiddlewareNoKeyConfigured(t *testing.T) {
	cfg := &config.Config{
		Server: config.ServerConfig{
			APIPort: 8080,
			APIKey:  "", // No key configured
		},
	}
	sched := newMockScheduler()
	srv := NewServer(cfg, nil, sched, testLogger())

	// Should allow access without auth when no key is configured
	req := httptest.NewRequest("GET", "/api/v1/accounts", nil)
	w := httptest.NewRecorder()

	srv.Router().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want %d when no API key configured", w.Code, http.StatusOK)
	}
}

func TestSchedulerStatusEndpoint(t *testing.T) {
	cfg := &config.Config{
		Server: config.ServerConfig{APIPort: 8080},
	}
	sched := newMockScheduler()
	sched.running = true
	sched.statuses = []AccountStatus{
		{
			Email:    "test@gmail.com",
			Running:  false,
			Schedule: "0 2 * * *",
			NextRun:  time.Now().Add(time.Hour),
		},
	}

	srv := NewServer(cfg, nil, sched, testLogger())

	req := httptest.NewRequest("GET", "/api/v1/scheduler/status", nil)
	w := httptest.NewRecorder()

	srv.Router().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", w.Code, http.StatusOK)
	}

	var resp SchedulerStatusResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if !resp.Running {
		t.Error("expected scheduler to be running")
	}
	if len(resp.Accounts) != 1 {
		t.Errorf("expected 1 account, got %d", len(resp.Accounts))
	}
}

func TestSchedulerStatusNotRunning(t *testing.T) {
	cfg := &config.Config{
		Server: config.ServerConfig{APIPort: 8080},
	}
	sched := newMockScheduler()
	sched.running = false

	srv := NewServer(cfg, nil, sched, testLogger())

	req := httptest.NewRequest("GET", "/api/v1/scheduler/status", nil)
	w := httptest.NewRecorder()

	srv.Router().ServeHTTP(w, req)

	var resp SchedulerStatusResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if resp.Running {
		t.Error("expected scheduler to NOT be running")
	}
}

func TestListAccountsEndpoint(t *testing.T) {
	cfg := &config.Config{
		Server: config.ServerConfig{APIPort: 8080},
		Accounts: []config.AccountSchedule{
			{Email: "user1@gmail.com", Schedule: "0 2 * * *", Enabled: true},
			{Email: "user2@gmail.com", Schedule: "0 3 * * *", Enabled: false},
		},
	}
	sched := newMockScheduler()
	srv := NewServer(cfg, nil, sched, testLogger())

	req := httptest.NewRequest("GET", "/api/v1/accounts", nil)
	w := httptest.NewRecorder()

	srv.Router().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", w.Code, http.StatusOK)
	}

	var resp map[string][]AccountInfo
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	accounts := resp["accounts"]
	if len(accounts) != 2 {
		t.Errorf("expected 2 accounts, got %d", len(accounts))
	}
}

func TestNilStoreReturns503(t *testing.T) {
	cfg := &config.Config{
		Server: config.ServerConfig{APIPort: 8080},
	}
	sched := newMockScheduler()
	srv := NewServer(cfg, nil, sched, testLogger())

	endpoints := []string{
		"/api/v1/stats",
		"/api/v1/messages",
		"/api/v1/messages/1",
		"/api/v1/search?q=test",
	}

	for _, path := range endpoints {
		t.Run(path, func(t *testing.T) {
			req := httptest.NewRequest("GET", path, nil)
			w := httptest.NewRecorder()
			srv.Router().ServeHTTP(w, req)

			if w.Code != http.StatusServiceUnavailable {
				t.Errorf("%s: status = %d, want %d", path, w.Code, http.StatusServiceUnavailable)
			}
		})
	}
}

func TestNilSchedulerReturns503(t *testing.T) {
	cfg := &config.Config{
		Server: config.ServerConfig{APIPort: 8080},
	}
	srv := NewServer(cfg, nil, nil, testLogger())

	endpoints := []struct {
		method string
		path   string
	}{
		{"GET", "/api/v1/accounts"},
		{"POST", "/api/v1/sync/test@gmail.com"},
		{"GET", "/api/v1/scheduler/status"},
	}

	for _, ep := range endpoints {
		t.Run(ep.method+" "+ep.path, func(t *testing.T) {
			req := httptest.NewRequest(ep.method, ep.path, nil)
			w := httptest.NewRecorder()
			srv.Router().ServeHTTP(w, req)

			if w.Code != http.StatusServiceUnavailable {
				t.Errorf("%s %s: status = %d, want %d", ep.method, ep.path, w.Code, http.StatusServiceUnavailable)
			}
		})
	}
}

func TestSecurityValidation(t *testing.T) {
	tests := []struct {
		name      string
		cfg       config.ServerConfig
		wantError bool
	}{
		{"loopback no key", config.ServerConfig{BindAddr: "127.0.0.1"}, false},
		{"loopback 127.0.0.2 no key", config.ServerConfig{BindAddr: "127.0.0.2"}, false},
		{"loopback 127.255.255.254 no key", config.ServerConfig{BindAddr: "127.255.255.254"}, false},
		{"ipv6 loopback no key", config.ServerConfig{BindAddr: "::1"}, false},
		{"localhost no key", config.ServerConfig{BindAddr: "localhost"}, false},
		{"empty addr no key", config.ServerConfig{BindAddr: ""}, false},
		{"non-loopback with key", config.ServerConfig{BindAddr: "0.0.0.0", APIKey: "secret"}, false},
		{"non-loopback no key", config.ServerConfig{BindAddr: "0.0.0.0"}, true},
		{"non-loopback ipv6 no key", config.ServerConfig{BindAddr: "::"}, true},
		{"non-loopback insecure override", config.ServerConfig{BindAddr: "0.0.0.0", AllowInsecure: true}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.ValidateSecure()
			if (err != nil) != tt.wantError {
				t.Errorf("ValidateSecure() error = %v, wantError = %v", err, tt.wantError)
			}
		})
	}
}

func TestCORSFromConfig(t *testing.T) {
	cfg := &config.Config{
		Server: config.ServerConfig{
			APIPort:     8080,
			CORSOrigins: []string{"http://localhost:3000", "http://example.com"},
		},
	}
	sched := newMockScheduler()
	srv := NewServer(cfg, nil, sched, testLogger())

	// Request from allowed origin
	req := httptest.NewRequest("GET", "/health", nil)
	req.Header.Set("Origin", "http://localhost:3000")
	w := httptest.NewRecorder()
	srv.Router().ServeHTTP(w, req)

	if w.Header().Get("Access-Control-Allow-Origin") != "http://localhost:3000" {
		t.Errorf("expected CORS header for allowed origin, got %q", w.Header().Get("Access-Control-Allow-Origin"))
	}

	// Request from disallowed origin
	req2 := httptest.NewRequest("GET", "/health", nil)
	req2.Header.Set("Origin", "http://evil.com")
	w2 := httptest.NewRecorder()
	srv.Router().ServeHTTP(w2, req2)

	if w2.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Errorf("expected no CORS header for disallowed origin, got %q", w2.Header().Get("Access-Control-Allow-Origin"))
	}
}

func TestCORSDisabledByDefault(t *testing.T) {
	cfg := &config.Config{
		Server: config.ServerConfig{APIPort: 8080},
	}
	sched := newMockScheduler()
	srv := NewServer(cfg, nil, sched, testLogger())

	req := httptest.NewRequest("GET", "/health", nil)
	req.Header.Set("Origin", "http://localhost:3000")
	w := httptest.NewRecorder()
	srv.Router().ServeHTTP(w, req)

	if w.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Errorf("expected no CORS header when no origins configured, got %q", w.Header().Get("Access-Control-Allow-Origin"))
	}
}
