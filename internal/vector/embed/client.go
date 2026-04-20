// Package embed implements an OpenAI-compatible /v1/embeddings HTTP client.
// It is used by the vector search pipeline to convert email text into
// embedding vectors suitable for ANN search.
package embed

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Config controls an embeddings Client. The zero value is not usable; callers
// must set Endpoint, Model, and Dimension at a minimum.
type Config struct {
	// Endpoint is the base URL including /v1 (e.g. "http://host:8080/v1").
	// The request path "/embeddings" is appended.
	Endpoint string
	// APIKey is an optional bearer token sent as Authorization: Bearer <key>.
	APIKey string
	// Model is the model name passed in the request body.
	Model string
	// Dimension is the expected vector dimension. Responses whose vectors
	// differ are rejected with an error.
	Dimension int
	// Timeout is the per-request HTTP timeout. Defaults to 30s when zero.
	Timeout time.Duration
	// MaxRetries is the maximum number of HTTP attempts for a single Embed
	// call. Defaults to 3 when zero. Only transient errors (5xx, network)
	// are retried; 4xx responses fail immediately.
	MaxRetries int
}

// Client calls an OpenAI-compatible /v1/embeddings endpoint.
type Client struct {
	cfg  Config
	http *http.Client
}

// NewClient constructs a Client, applying defaults for Timeout and MaxRetries.
func NewClient(cfg Config) *Client {
	if cfg.Timeout == 0 {
		cfg.Timeout = 30 * time.Second
	}
	if cfg.MaxRetries == 0 {
		cfg.MaxRetries = 3
	}
	return &Client{cfg: cfg, http: &http.Client{Timeout: cfg.Timeout}}
}

// embeddingRequest is the JSON body sent to the server.
type embeddingRequest struct {
	Input []string `json:"input"`
	Model string   `json:"model"`
}

// embeddingResponse is the JSON response body from the server.
type embeddingResponse struct {
	Data []struct {
		Embedding []float32 `json:"embedding"`
		Index     int       `json:"index"`
	} `json:"data"`
	Model string `json:"model"`
}

// Embed returns one vector per input, in input order. Empty input is a no-op
// and returns (nil, nil) without making an HTTP call. Every returned vector
// is verified to match cfg.Dimension. Transient errors — 5xx responses, 429
// Too Many Requests, network failures, and body-read / decode hiccups — are
// retried with exponential backoff up to cfg.MaxRetries total attempts. A
// 429 response's Retry-After header (when present and parseable) overrides
// the backoff for that attempt. Other 4xx responses fail immediately.
func (c *Client) Embed(ctx context.Context, inputs []string) ([][]float32, error) {
	if len(inputs) == 0 {
		return nil, nil
	}
	body, err := json.Marshal(embeddingRequest{Input: inputs, Model: c.cfg.Model})
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	var lastErr error
	for attempt := 1; attempt <= c.cfg.MaxRetries; attempt++ {
		vecs, err := c.doOnce(ctx, body, len(inputs))
		if err == nil {
			return vecs, nil
		}
		lastErr = err
		var re *retryError
		if !errors.As(err, &re) {
			return nil, err
		}
		if attempt == c.cfg.MaxRetries {
			break
		}
		backoff := time.Duration(1<<attempt) * 100 * time.Millisecond
		if re.retryAfter > 0 {
			backoff = re.retryAfter
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("embed: context canceled during backoff: %w", ctx.Err())
		case <-time.After(backoff):
		}
	}
	return nil, fmt.Errorf("embed: giving up after %d attempts: %w", c.cfg.MaxRetries, lastErr)
}

// doOnce performs a single HTTP request. A returned *retryError signals the
// caller that the error is transient and the call should be retried.
// The want parameter is the expected number of vectors (= number of inputs).
func (c *Client) doOnce(ctx context.Context, body []byte, want int) ([][]float32, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.cfg.Endpoint+"/embeddings", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if c.cfg.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.cfg.APIKey)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, &retryError{err: fmt.Errorf("http do: %w", err)}
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusTooManyRequests {
		// 429 is a transient rate-limit signal. Honor Retry-After
		// when the server provides it so we don't thrash.
		return nil, &retryError{
			err:        fmt.Errorf("embed: HTTP 429 (rate limited)"),
			retryAfter: parseRetryAfter(resp.Header.Get("Retry-After")),
		}
	}
	if resp.StatusCode >= 500 {
		return nil, &retryError{err: fmt.Errorf("embed: HTTP %d", resp.StatusCode)}
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("embed: HTTP %d", resp.StatusCode)
	}

	var r embeddingResponse
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		// Body read/decode failures usually mean the connection
		// dropped mid-stream (unexpected EOF, deadline hit while
		// reading). Treat as transient so a healthy retry can
		// succeed rather than failing the whole batch.
		return nil, &retryError{err: fmt.Errorf("decode response: %w", err)}
	}
	vecs := make([][]float32, want)
	for _, d := range r.Data {
		if d.Index < 0 || d.Index >= want {
			return nil, fmt.Errorf("embed: invalid index %d (len=%d)", d.Index, want)
		}
		if len(d.Embedding) != c.cfg.Dimension {
			return nil, fmt.Errorf("embed: dimension mismatch: got %d, configured %d",
				len(d.Embedding), c.cfg.Dimension)
		}
		vecs[d.Index] = d.Embedding
	}
	for i, v := range vecs {
		if v == nil {
			return nil, fmt.Errorf("embed: missing embedding at index %d", i)
		}
	}
	return vecs, nil
}

// retryError wraps a transient error. Callers use errors.As to detect it.
// retryAfter is an optional server-specified delay (from a 429 Retry-After
// header). Zero means "use the default backoff".
type retryError struct {
	err        error
	retryAfter time.Duration
}

func (e *retryError) Error() string { return e.err.Error() }
func (e *retryError) Unwrap() error { return e.err }

// parseRetryAfter parses an HTTP Retry-After header (RFC 7231 §7.1.3),
// which may be either a non-negative delta-seconds integer or an
// HTTP-date. Unparseable values and values in the past return 0, which
// tells the caller to fall back to its default backoff. A very large
// delta is capped to one hour so a misbehaving server can't stall a
// worker indefinitely.
func parseRetryAfter(v string) time.Duration {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0
	}
	const maxWait = time.Hour
	if secs, err := strconv.Atoi(v); err == nil && secs >= 0 {
		d := time.Duration(secs) * time.Second
		if d > maxWait {
			return maxWait
		}
		return d
	}
	if t, err := http.ParseTime(v); err == nil {
		d := time.Until(t)
		if d <= 0 {
			return 0
		}
		if d > maxWait {
			return maxWait
		}
		return d
	}
	return 0
}
