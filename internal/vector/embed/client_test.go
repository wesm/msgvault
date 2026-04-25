package embed

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// writeEmbeddings writes an OpenAI-compatible embeddings response using the
// provided vectors. It panics on encoding failure; that never happens for
// fixed test payloads.
func writeEmbeddings(t *testing.T, w http.ResponseWriter, vecs [][]float32) {
	t.Helper()
	type item struct {
		Embedding []float32 `json:"embedding"`
		Index     int       `json:"index"`
	}
	payload := struct {
		Data  []item `json:"data"`
		Model string `json:"model"`
	}{Model: "test-model"}
	for i, v := range vecs {
		payload.Data = append(payload.Data, item{Embedding: v, Index: i})
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		t.Fatalf("encode response: %v", err)
	}
}

func decodeRequest(t *testing.T, r *http.Request) embeddingRequest {
	t.Helper()
	var req embeddingRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		t.Fatalf("decode request: %v", err)
	}
	return req
}

func TestClient_Embed_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/embeddings" {
			t.Errorf("path=%q want /embeddings", r.URL.Path)
		}
		if r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("Content-Type=%q want application/json", r.Header.Get("Content-Type"))
		}
		req := decodeRequest(t, r)
		if len(req.Input) != 2 {
			t.Errorf("len(Input)=%d want 2", len(req.Input))
		}
		if req.Model != "test-model" {
			t.Errorf("Model=%q want test-model", req.Model)
		}
		writeEmbeddings(t, w, [][]float32{
			{0.1, 0.2, 0.3},
			{0.4, 0.5, 0.6},
		})
	}))
	defer srv.Close()

	c := NewClient(Config{
		Endpoint:  srv.URL,
		Model:     "test-model",
		Dimension: 3,
	})
	vecs, err := c.Embed(context.Background(), []string{"hello", "world"})
	if err != nil {
		t.Fatalf("Embed: %v", err)
	}
	if len(vecs) != 2 {
		t.Fatalf("len(vecs)=%d want 2", len(vecs))
	}
	for i, v := range vecs {
		if len(v) != 3 {
			t.Errorf("vecs[%d] len=%d want 3", i, len(v))
		}
	}
	if vecs[0][0] != 0.1 || vecs[1][2] != 0.6 {
		t.Errorf("wrong embedding values: %v", vecs)
	}
}

func TestClient_Embed_DimensionMismatch(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeEmbeddings(t, w, [][]float32{{0.1, 0.2}})
	}))
	defer srv.Close()

	c := NewClient(Config{Endpoint: srv.URL, Model: "m", Dimension: 3})
	_, err := c.Embed(context.Background(), []string{"a"})
	if err == nil {
		t.Fatal("expected dimension mismatch error")
	}
	if !strings.Contains(err.Error(), "dimension mismatch") {
		t.Errorf("error %q missing 'dimension mismatch'", err.Error())
	}
}

func TestClient_Embed_Retries5xx(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := attempts.Add(1)
		if n < 3 {
			http.Error(w, "boom", http.StatusInternalServerError)
			return
		}
		writeEmbeddings(t, w, [][]float32{{0.1, 0.2, 0.3}})
	}))
	defer srv.Close()

	c := NewClient(Config{Endpoint: srv.URL, Model: "m", Dimension: 3, MaxRetries: 3})
	vecs, err := c.Embed(context.Background(), []string{"a"})
	if err != nil {
		t.Fatalf("Embed: %v", err)
	}
	if got := attempts.Load(); got != 3 {
		t.Errorf("attempts=%d want 3", got)
	}
	if len(vecs) != 1 || len(vecs[0]) != 3 {
		t.Errorf("unexpected vecs: %v", vecs)
	}
}

func TestClient_Embed_Does_Not_Retry_4xx(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"No models loaded"}`))
	}))
	defer srv.Close()

	c := NewClient(Config{Endpoint: srv.URL, Model: "m", Dimension: 3, MaxRetries: 5})
	_, err := c.Embed(context.Background(), []string{"a"})
	if err == nil {
		t.Fatal("expected error for 4xx")
	}
	if got := attempts.Load(); got != 1 {
		t.Errorf("attempts=%d want 1 (no retry on 4xx)", got)
	}
	if !strings.Contains(err.Error(), "400") {
		t.Errorf("error %q should include status 400", err.Error())
	}
	if !strings.Contains(err.Error(), "No models loaded") {
		t.Errorf("error %q should include response body", err.Error())
	}
}

func TestClient_Embed_AuthHeader(t *testing.T) {
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		writeEmbeddings(t, w, [][]float32{{1, 2, 3}})
	}))
	defer srv.Close()

	c := NewClient(Config{Endpoint: srv.URL, APIKey: "secret-token", Model: "m", Dimension: 3})
	if _, err := c.Embed(context.Background(), []string{"a"}); err != nil {
		t.Fatalf("Embed: %v", err)
	}
	if gotAuth != "Bearer secret-token" {
		t.Errorf("Authorization=%q want 'Bearer secret-token'", gotAuth)
	}
}

func TestClient_Embed_EmptyInput(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		writeEmbeddings(t, w, nil)
	}))
	defer srv.Close()

	c := NewClient(Config{Endpoint: srv.URL, Model: "m", Dimension: 3})

	vecs, err := c.Embed(context.Background(), nil)
	if err != nil {
		t.Fatalf("nil input: %v", err)
	}
	if vecs != nil {
		t.Errorf("nil input returned %v, want nil", vecs)
	}

	vecs, err = c.Embed(context.Background(), []string{})
	if err != nil {
		t.Fatalf("empty input: %v", err)
	}
	if vecs != nil {
		t.Errorf("empty input returned %v, want nil", vecs)
	}

	if got := attempts.Load(); got != 0 {
		t.Errorf("attempts=%d want 0 (no HTTP call for empty input)", got)
	}
}

func TestClient_Embed_GivesUpAfterMaxRetries(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		http.Error(w, "down", http.StatusInternalServerError)
	}))
	defer srv.Close()

	c := NewClient(Config{Endpoint: srv.URL, Model: "m", Dimension: 3, MaxRetries: 2})
	_, err := c.Embed(context.Background(), []string{"a"})
	if err == nil {
		t.Fatal("expected error after exhausting retries")
	}
	if got := attempts.Load(); got != 2 {
		t.Errorf("attempts=%d want 2", got)
	}
	if !strings.Contains(err.Error(), "giving up") {
		t.Errorf("error %q should include 'giving up'", err.Error())
	}
}

func TestClient_Embed_ContextCanceledDuringBackoff(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()

	c := NewClient(Config{Endpoint: srv.URL, Model: "m", Dimension: 3, MaxRetries: 10})

	ctx, cancel := context.WithCancel(context.Background())
	// Cancel shortly after start so we hit the backoff wait.
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	_, err := c.Embed(ctx, []string{"a"})
	if err == nil {
		t.Fatal("expected error from canceled context")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("error %q should wrap context.Canceled", err.Error())
	}
}

func TestClient_Embed_MissingIndex(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Only index 0 returned for 2-input request.
		writeEmbeddings(t, w, [][]float32{{0.1, 0.2, 0.3}})
	}))
	defer srv.Close()

	c := NewClient(Config{Endpoint: srv.URL, Model: "m", Dimension: 3})
	_, err := c.Embed(context.Background(), []string{"a", "b"})
	if err == nil {
		t.Fatal("expected missing embedding error")
	}
	if !strings.Contains(err.Error(), "missing embedding at index 1") {
		t.Errorf("error %q missing 'missing embedding at index 1'", err.Error())
	}
}

// TestClient_Embed_Retries429 verifies 429 Too Many Requests is
// treated as transient and retried rather than failing immediately.
func TestClient_Embed_Retries429(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := attempts.Add(1)
		if n < 2 {
			http.Error(w, "slow down", http.StatusTooManyRequests)
			return
		}
		writeEmbeddings(t, w, [][]float32{{0.1, 0.2, 0.3}})
	}))
	defer srv.Close()

	c := NewClient(Config{Endpoint: srv.URL, Model: "m", Dimension: 3, MaxRetries: 3})
	vecs, err := c.Embed(context.Background(), []string{"a"})
	if err != nil {
		t.Fatalf("Embed: %v", err)
	}
	if got := attempts.Load(); got != 2 {
		t.Errorf("attempts=%d want 2 (retry after 429)", got)
	}
	if len(vecs) != 1 || len(vecs[0]) != 3 {
		t.Errorf("unexpected vecs: %v", vecs)
	}
}

// TestClient_Embed_HonorsRetryAfterOverridesBackoff verifies that a
// long Retry-After value stretches the retry wait past the default
// exponential backoff. Cancelling the context mid-wait must return
// a context-cancel error rather than racing the default-backoff
// deadline.
func TestClient_Embed_HonorsRetryAfterOverridesBackoff(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.Header().Set("Retry-After", "30") // much longer than default 200ms
		http.Error(w, "rl", http.StatusTooManyRequests)
	}))
	defer srv.Close()

	c := NewClient(Config{Endpoint: srv.URL, Model: "m", Dimension: 3, MaxRetries: 3})
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()
	start := time.Now()
	_, err := c.Embed(ctx, []string{"a"})
	elapsed := time.Since(start)
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
	// Should be interrupted at ~100ms by the cancel, well before
	// 30s. A test failure here would mean Retry-After wasn't
	// honored and the default backoff completed first.
	if elapsed > 500*time.Millisecond {
		t.Errorf("elapsed=%v, want <500ms (cancel during Retry-After wait)", elapsed)
	}
	// One attempt plus possibly a second before cancel; never
	// enough to finish the Retry-After window.
	if got := attempts.Load(); got > 2 {
		t.Errorf("attempts=%d, want <=2 (Retry-After should extend the wait)", got)
	}
}

// TestClient_Embed_RetriesTruncatedBody verifies a truncated JSON
// response is treated as transient. Mid-stream cutoffs are common
// when the server hits a deadline, and the old code failed them
// outright.
func TestClient_Embed_RetriesTruncatedBody(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := attempts.Add(1)
		w.Header().Set("Content-Type", "application/json")
		if n < 2 {
			// Write a prefix then cut the connection mid-JSON.
			_, _ = w.Write([]byte(`{"data": [{"embedd`))
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			if h, ok := w.(http.Hijacker); ok {
				conn, _, err := h.Hijack()
				if err == nil {
					_ = conn.Close()
				}
			}
			return
		}
		writeEmbeddings(t, w, [][]float32{{0.1, 0.2, 0.3}})
	}))
	defer srv.Close()

	c := NewClient(Config{Endpoint: srv.URL, Model: "m", Dimension: 3, MaxRetries: 3})
	vecs, err := c.Embed(context.Background(), []string{"a"})
	if err != nil {
		t.Fatalf("Embed: %v", err)
	}
	if got := attempts.Load(); got != 2 {
		t.Errorf("attempts=%d want 2 (retry after truncated body)", got)
	}
	if len(vecs) != 1 {
		t.Fatalf("len(vecs)=%d want 1", len(vecs))
	}
}

// TestClient_parseRetryAfter covers the Retry-After formats (seconds,
// HTTP-date, unparseable) and the cap that protects against absurd
// server-supplied values. The (Duration, bool) return distinguishes
// "Retry-After: 0" (parsed = true, immediate retry) from "missing or
// unparseable" (parsed = false, use default backoff).
func TestClient_parseRetryAfter(t *testing.T) {
	cases := []struct {
		in      string
		wantDur time.Duration
		wantOk  bool
	}{
		{"", 0, false},
		{"   ", 0, false},
		{"abc", 0, false},
		{"-5", 0, false},
		{"0", 0, true}, // explicit immediate retry
		{"2", 2 * time.Second, true},
	}
	for _, c := range cases {
		gotDur, gotOk := parseRetryAfter(c.in)
		if gotDur != c.wantDur || gotOk != c.wantOk {
			t.Errorf("parseRetryAfter(%q) = (%v, %v), want (%v, %v)",
				c.in, gotDur, gotOk, c.wantDur, c.wantOk)
		}
	}
	// Cap: 7200 seconds is capped to 1 hour.
	if got, ok := parseRetryAfter("7200"); got != time.Hour || !ok {
		t.Errorf("parseRetryAfter(7200) = (%v, %v), want (1h, true)", got, ok)
	}
	// HTTP-date: one second in the future is a non-zero positive
	// duration well under the cap.
	future := time.Now().Add(5 * time.Second).UTC().Format(http.TimeFormat)
	if got, ok := parseRetryAfter(future); !ok || got <= 0 || got > time.Hour {
		t.Errorf("parseRetryAfter(%q) = (%v, %v), want (>0 and <=1h, true)", future, got, ok)
	}
}

// TestClient_Embed_RetryAfterZero_RetriesImmediately regresses the
// bug where Retry-After: 0 was indistinguishable from "no override"
// and fell back to exponential backoff. With the (Duration, bool)
// return, an explicit zero must take precedence and retry without
// waiting. We assert by measuring elapsed time across two attempts:
// the second attempt must start far sooner than the default
// 200ms backoff for attempt #1.
func TestClient_Embed_RetryAfterZero_RetriesImmediately(t *testing.T) {
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		writeEmbeddings(t, w, [][]float32{{1, 0, 0, 0}})
	}))
	defer srv.Close()

	c := NewClient(Config{Endpoint: srv.URL, Model: "m", Dimension: 4, MaxRetries: 3})
	start := time.Now()
	vecs, err := c.Embed(context.Background(), []string{"hello"})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("Embed: %v", err)
	}
	if len(vecs) != 1 {
		t.Fatalf("len(vecs)=%d, want 1", len(vecs))
	}
	// Default backoff for attempt #1 is 1<<1 * 100ms = 200ms.
	// Retry-After: 0 should drop that to ~0. Allow generous slack
	// (50ms) for HTTP roundtrips on slow CI.
	if elapsed > 100*time.Millisecond {
		t.Errorf("elapsed=%v, want < 100ms (Retry-After: 0 should bypass exponential backoff)", elapsed)
	}
	if calls != 2 {
		t.Errorf("calls=%d, want 2", calls)
	}
}

func TestClient_Embed_4xxIsPermanent(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"error":{"message":"Invalid input"}}`, http.StatusBadRequest)
	}))
	defer srv.Close()

	c := NewClient(Config{
		Endpoint: srv.URL, Model: "m", Dimension: 4, MaxRetries: 3,
	})
	_, err := c.Embed(context.Background(), []string{"hello"})
	if err == nil {
		t.Fatalf("expected error on 400")
	}
	if !errors.Is(err, ErrPermanent4xx) {
		t.Fatalf("expected errors.Is(err, ErrPermanent4xx), got %v", err)
	}
	// Existing contract: body must still be in the message.
	if !strings.Contains(err.Error(), "Invalid input") {
		t.Errorf("expected body in error, got %v", err)
	}
}

func TestClient_Embed_5xxNotPermanent(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()

	c := NewClient(Config{
		Endpoint: srv.URL, Model: "m", Dimension: 4, MaxRetries: 2,
	})
	_, err := c.Embed(context.Background(), []string{"hello"})
	if err == nil {
		t.Fatalf("expected error after retries exhausted")
	}
	if errors.Is(err, ErrPermanent4xx) {
		t.Fatalf("5xx should NOT match ErrPermanent4xx, got %v", err)
	}
}

func TestClient_Embed_429NotPermanent(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "0")
		http.Error(w, "slow down", http.StatusTooManyRequests)
	}))
	defer srv.Close()

	c := NewClient(Config{
		Endpoint: srv.URL, Model: "m", Dimension: 4, MaxRetries: 2,
	})
	_, err := c.Embed(context.Background(), []string{"hello"})
	if err == nil {
		t.Fatalf("expected error after retries exhausted")
	}
	if errors.Is(err, ErrPermanent4xx) {
		t.Fatalf("429 should NOT match ErrPermanent4xx, got %v", err)
	}
}

func TestClient_Embed_InvalidIndex(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Return index 5 for a 1-input request.
		type item struct {
			Embedding []float32 `json:"embedding"`
			Index     int       `json:"index"`
		}
		payload := struct {
			Data  []item `json:"data"`
			Model string `json:"model"`
		}{
			Data:  []item{{Embedding: []float32{0.1, 0.2, 0.3}, Index: 5}},
			Model: "m",
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(payload); err != nil {
			t.Fatalf("encode: %v", err)
		}
	}))
	defer srv.Close()

	c := NewClient(Config{Endpoint: srv.URL, Model: "m", Dimension: 3})
	_, err := c.Embed(context.Background(), []string{"a"})
	if err == nil {
		t.Fatal("expected invalid index error")
	}
	if !strings.Contains(err.Error(), "invalid index") {
		t.Errorf("error %q missing 'invalid index'", err.Error())
	}
}
