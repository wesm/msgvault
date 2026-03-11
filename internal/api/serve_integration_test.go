package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/wesm/msgvault/internal/config"
	"github.com/wesm/msgvault/internal/query"
	"github.com/wesm/msgvault/internal/testutil/dbtest"
)

// TestServeEngineWired verifies that the production wiring pattern (open DB →
// NewSQLiteEngine → WithEngine) results in a server where the engine-powered
// endpoints return 200 instead of 503.
//
// This catches the class of bug where runServe constructs the API server but
// forgets to call WithEngine, leaving s.engine == nil in production.
func TestServeEngineWired(t *testing.T) {
	// Replicate the production wiring from runServe:
	//   store.Open(dbPath) → s.DB() → query.NewSQLiteEngine → server.WithEngine
	tdb := dbtest.NewTestDB(t, "../store/schema.sql")
	engine := query.NewSQLiteEngine(tdb.DB)

	cfg := &config.Config{
		Server: config.ServerConfig{APIPort: 8080}, // empty APIKey → auth disabled
	}
	srv := NewServer(cfg, &mockStore{}, newMockScheduler(), testLogger()).
		WithEngine(engine)

	tests := []struct {
		name string
		url  string
	}{
		{"aggregate", "/api/v1/aggregate?group_by=sender"},
		{"engine/messages", "/api/v1/engine/messages"},
		{"engine/search", "/api/v1/engine/search?q=hello"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tt.url, nil)
			w := httptest.NewRecorder()
			srv.Router().ServeHTTP(w, req)

			if w.Code == http.StatusServiceUnavailable {
				t.Errorf("%s returned 503 — engine not wired up", tt.url)
			}
		})
	}
}
