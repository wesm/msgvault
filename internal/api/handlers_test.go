package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/wesm/msgvault/internal/config"
	"github.com/wesm/msgvault/internal/scheduler"
	"github.com/wesm/msgvault/internal/store"
)

func toNullString(s string) sql.NullString {
	return sql.NullString{String: s, Valid: s != ""}
}

func setupTestStore(t *testing.T) *store.Store {
	t.Helper()

	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open store: %v", err)
	}

	if err := s.InitSchema(); err != nil {
		t.Fatalf("failed to init schema: %v", err)
	}

	// Create a test source
	source, err := s.GetOrCreateSource("gmail", "test@gmail.com")
	if err != nil {
		t.Fatalf("failed to create source: %v", err)
	}

	// Use store methods to insert test data
	convID, err := s.EnsureConversation(source.ID, "conv-001", "Test Conversation")
	if err != nil {
		t.Fatalf("failed to create conversation: %v", err)
	}

	participantID, err := s.EnsureParticipant("sender@example.com", "Test Sender", "example.com")
	if err != nil {
		t.Fatalf("failed to create participant: %v", err)
	}

	// Use store Message struct
	msg := &store.Message{
		ConversationID:  convID,
		SourceID:        source.ID,
		SourceMessageID: "msg-001",
		Subject:         toNullString("Test Subject"),
		Snippet:         toNullString("This is a test message snippet"),
		SizeEstimate:    1024,
		HasAttachments:  false,
	}

	msgID, err := s.UpsertMessage(msg)
	if err != nil {
		t.Fatalf("failed to insert message: %v", err)
	}

	// Link sender
	err = s.ReplaceMessageRecipients(msgID, "from", []int64{participantID}, []string{"Test Sender"})
	if err != nil {
		t.Fatalf("failed to link sender: %v", err)
	}

	// Create and link label
	labelID, err := s.EnsureLabel(source.ID, "INBOX", "INBOX", "system")
	if err != nil {
		t.Fatalf("failed to create label: %v", err)
	}
	err = s.ReplaceMessageLabels(msgID, []int64{labelID})
	if err != nil {
		t.Fatalf("failed to link label: %v", err)
	}

	// Create message body
	err = s.UpsertMessageBody(msgID, toNullString("This is the full message body text."), toNullString(""))
	if err != nil {
		t.Fatalf("failed to insert body: %v", err)
	}

	t.Cleanup(func() {
		s.Close()
		os.RemoveAll(tmpDir)
	})

	return s
}

func newTestServerWithStore(t *testing.T, s *store.Store) *Server {
	t.Helper()

	cfg := &config.Config{
		Server: config.ServerConfig{
			APIPort: 8080,
		},
		Accounts: []config.AccountSchedule{
			{Email: "test@gmail.com", Schedule: "0 2 * * *", Enabled: true},
		},
	}

	sched := scheduler.New(func(ctx context.Context, email string) error {
		return nil
	})

	return NewServer(cfg, s, sched, testLogger())
}

func TestHandleStats(t *testing.T) {
	s := setupTestStore(t)
	srv := newTestServerWithStore(t, s)

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

	if resp.TotalMessages < 1 {
		t.Error("expected at least 1 message")
	}
	if resp.TotalAccounts < 1 {
		t.Error("expected at least 1 account")
	}
}

func TestHandleListMessages(t *testing.T) {
	s := setupTestStore(t)
	srv := newTestServerWithStore(t, s)

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
}

func TestHandleListMessagesPagination(t *testing.T) {
	s := setupTestStore(t)
	srv := newTestServerWithStore(t, s)

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
	s := setupTestStore(t)
	srv := newTestServerWithStore(t, s)

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
	s := setupTestStore(t)
	srv := newTestServerWithStore(t, s)

	req := httptest.NewRequest("GET", "/api/v1/messages/99999", nil)
	w := httptest.NewRecorder()

	srv.Router().ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want %d", w.Code, http.StatusNotFound)
	}
}

func TestHandleGetMessageInvalidID(t *testing.T) {
	s := setupTestStore(t)
	srv := newTestServerWithStore(t, s)

	req := httptest.NewRequest("GET", "/api/v1/messages/invalid", nil)
	w := httptest.NewRecorder()

	srv.Router().ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", w.Code, http.StatusBadRequest)
	}
}

func TestHandleSearchMissingQuery(t *testing.T) {
	s := setupTestStore(t)
	srv := newTestServerWithStore(t, s)

	req := httptest.NewRequest("GET", "/api/v1/search", nil)
	w := httptest.NewRecorder()

	srv.Router().ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", w.Code, http.StatusBadRequest)
	}
}

func TestHandleSearch(t *testing.T) {
	s := setupTestStore(t)
	srv := newTestServerWithStore(t, s)

	// Search by subject (will use LIKE fallback since FTS5 table may not have data)
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
	s := setupTestStore(t)

	cfg := &config.Config{
		Server: config.ServerConfig{APIPort: 8080},
		Accounts: []config.AccountSchedule{
			{Email: "test@gmail.com", Schedule: "0 2 * * *", Enabled: true},
		},
	}

	sched := scheduler.New(func(ctx context.Context, email string) error {
		return nil
	})
	sched.AddAccount("test@gmail.com", "0 2 * * *")

	srv := NewServer(cfg, s, sched, testLogger())

	req := httptest.NewRequest("POST", "/api/v1/sync/test@gmail.com", nil)
	w := httptest.NewRecorder()

	srv.Router().ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Errorf("status = %d, want %d, body: %s", w.Code, http.StatusAccepted, w.Body.String())
	}
}
