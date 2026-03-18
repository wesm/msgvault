package remote

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/wesm/msgvault/internal/query"
)

func TestEngineListMessagesPreservesDeletedAt(t *testing.T) {
	deletedAt := "2026-03-18T15:00:00Z"
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/messages/filter" {
			t.Fatalf("path = %q, want %q", r.URL.Path, "/api/v1/messages/filter")
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"count":    1,
			"has_more": false,
			"offset":   0,
			"limit":    100,
			"messages": []map[string]any{
				{
					"id":         1,
					"subject":    "Deleted message",
					"from":       "sender@example.com",
					"to":         []string{"receiver@example.com"},
					"sent_at":    "2024-01-15T10:30:00Z",
					"snippet":    "preview",
					"labels":     []string{"INBOX"},
					"size_bytes": 1234,
					"deleted_at": deletedAt,
				},
			},
		})
	}))
	defer srv.Close()

	store := &Store{
		baseURL:    srv.URL,
		httpClient: srv.Client(),
	}

	engine := NewEngineFromStore(store)

	msgs, err := engine.ListMessages(context.Background(), query.MessageFilter{})
	if err != nil {
		t.Fatalf("ListMessages() error = %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("len(msgs) = %d, want 1", len(msgs))
	}
	if msgs[0].DeletedAt == nil {
		t.Fatal("DeletedAt = nil, want parsed timestamp")
	}
	if got := msgs[0].DeletedAt.UTC().Format(time.RFC3339); got != deletedAt {
		t.Fatalf("DeletedAt = %q, want %q", got, deletedAt)
	}
}
