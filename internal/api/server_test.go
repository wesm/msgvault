package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/wesm/msgvault/internal/config"
	"github.com/wesm/msgvault/internal/scheduler"
)

// testLogger returns a logger for tests that discards output
func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func TestHealthEndpoint(t *testing.T) {
	cfg := &config.Config{
		Server: config.ServerConfig{APIPort: 8080},
	}
	sched := scheduler.New(func(ctx context.Context, email string) error { return nil })

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
	sched := scheduler.New(func(ctx context.Context, email string) error { return nil })

	srv := NewServer(cfg, nil, sched, testLogger())

	tests := []struct {
		name       string
		authHeader string
		wantStatus int
	}{
		{"no auth", "", http.StatusUnauthorized},
		{"wrong key", "wrong-key", http.StatusUnauthorized},
		{"correct key", "secret-key", http.StatusOK},
		{"bearer prefix", "Bearer secret-key", http.StatusOK},
		{"x-api-key header", "secret-key", http.StatusOK}, // will test with X-API-Key header
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/api/v1/accounts", nil)
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
	sched := scheduler.New(func(ctx context.Context, email string) error { return nil })

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
	sched := scheduler.New(func(ctx context.Context, email string) error { return nil })
	if err := sched.AddAccount("test@gmail.com", "0 2 * * *"); err != nil {
		t.Fatalf("AddAccount: %v", err)
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

func TestListAccountsEndpoint(t *testing.T) {
	cfg := &config.Config{
		Server: config.ServerConfig{APIPort: 8080},
		Accounts: []config.AccountSchedule{
			{Email: "user1@gmail.com", Schedule: "0 2 * * *", Enabled: true},
			{Email: "user2@gmail.com", Schedule: "0 3 * * *", Enabled: false},
		},
	}
	sched := scheduler.New(func(ctx context.Context, email string) error { return nil })

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
