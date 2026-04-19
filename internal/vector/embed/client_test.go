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
		http.Error(w, "bad request", http.StatusBadRequest)
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
