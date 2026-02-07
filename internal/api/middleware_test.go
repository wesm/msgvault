package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCORSMiddleware(t *testing.T) {
	cfg := DefaultCORSConfig()
	middleware := CORSMiddleware(cfg)

	handler := middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	tests := []struct {
		name           string
		method         string
		origin         string
		wantStatus     int
		wantCORSHeader bool
	}{
		{
			name:           "no origin",
			method:         "GET",
			origin:         "",
			wantStatus:     http.StatusOK,
			wantCORSHeader: false,
		},
		{
			name:           "with origin",
			method:         "GET",
			origin:         "http://localhost:3000",
			wantStatus:     http.StatusOK,
			wantCORSHeader: true,
		},
		{
			name:           "preflight request",
			method:         "OPTIONS",
			origin:         "http://localhost:3000",
			wantStatus:     http.StatusNoContent,
			wantCORSHeader: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, "/api/v1/stats", nil)
			if tt.origin != "" {
				req.Header.Set("Origin", tt.origin)
			}
			w := httptest.NewRecorder()

			handler.ServeHTTP(w, req)

			if w.Code != tt.wantStatus {
				t.Errorf("status = %d, want %d", w.Code, tt.wantStatus)
			}

			corsHeader := w.Header().Get("Access-Control-Allow-Origin")
			if tt.wantCORSHeader && corsHeader == "" {
				t.Error("expected CORS header to be set")
			}
			if !tt.wantCORSHeader && corsHeader != "" {
				t.Errorf("unexpected CORS header: %s", corsHeader)
			}
		})
	}
}

func TestCORSPreflightHeaders(t *testing.T) {
	cfg := DefaultCORSConfig()
	middleware := CORSMiddleware(cfg)

	handler := middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest("OPTIONS", "/api/v1/stats", nil)
	req.Header.Set("Origin", "http://localhost:3000")
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	// Check all preflight headers
	if w.Header().Get("Access-Control-Allow-Origin") == "" {
		t.Error("missing Access-Control-Allow-Origin")
	}
	if w.Header().Get("Access-Control-Allow-Methods") == "" {
		t.Error("missing Access-Control-Allow-Methods")
	}
	if w.Header().Get("Access-Control-Allow-Headers") == "" {
		t.Error("missing Access-Control-Allow-Headers")
	}
	if w.Header().Get("Access-Control-Max-Age") == "" {
		t.Error("missing Access-Control-Max-Age")
	}
}

func TestRateLimiter(t *testing.T) {
	rl := NewRateLimiter(2, 2) // 2 req/sec with burst of 2

	// First two requests should succeed (burst)
	if !rl.Allow("127.0.0.1") {
		t.Error("first request should be allowed")
	}
	if !rl.Allow("127.0.0.1") {
		t.Error("second request should be allowed (burst)")
	}

	// Third request should be rate limited
	if rl.Allow("127.0.0.1") {
		t.Error("third request should be rate limited")
	}

	// Different IP should still be allowed
	if !rl.Allow("192.168.1.1") {
		t.Error("different IP should be allowed")
	}
}

func TestRateLimiterCloseConcurrent(t *testing.T) {
	rl := NewRateLimiter(10, 10)

	// Spawn many goroutines calling Close() concurrently — must not panic.
	const n = 50
	start := make(chan struct{})
	done := make(chan struct{}, n)
	for i := 0; i < n; i++ {
		go func() {
			<-start
			rl.Close()
			done <- struct{}{}
		}()
	}
	close(start) // release all at once
	for i := 0; i < n; i++ {
		<-done
	}
	// If we get here without a panic, the test passes.
}

func TestRateLimitMiddleware(t *testing.T) {
	rl := NewRateLimiter(1, 1) // Very restrictive for testing
	middleware := RateLimitMiddleware(rl)

	handler := middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// First request should succeed
	req1 := httptest.NewRequest("GET", "/test", nil)
	req1.RemoteAddr = "127.0.0.1:1234"
	w1 := httptest.NewRecorder()
	handler.ServeHTTP(w1, req1)

	if w1.Code != http.StatusOK {
		t.Errorf("first request status = %d, want %d", w1.Code, http.StatusOK)
	}

	// Second immediate request should be rate limited
	req2 := httptest.NewRequest("GET", "/test", nil)
	req2.RemoteAddr = "127.0.0.1:1234"
	w2 := httptest.NewRecorder()
	handler.ServeHTTP(w2, req2)

	if w2.Code != http.StatusTooManyRequests {
		t.Errorf("second request status = %d, want %d", w2.Code, http.StatusTooManyRequests)
	}

	// Check Retry-After header
	if w2.Header().Get("Retry-After") == "" {
		t.Error("missing Retry-After header on rate limited response")
	}
}
