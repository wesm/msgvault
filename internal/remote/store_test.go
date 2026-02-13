package remote

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestNew_RejectsHTTPWithoutAllowInsecure(t *testing.T) {
	_, err := New(Config{
		URL:    "http://nas:8080",
		APIKey: "key",
	})
	if err == nil {
		t.Fatal("New() should reject http:// without AllowInsecure")
	}
}

func TestNew_AllowsHTTPWithAllowInsecure(t *testing.T) {
	s, err := New(Config{
		URL:           "http://nas:8080",
		APIKey:        "key",
		AllowInsecure: true,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if s == nil {
		t.Fatal("New() returned nil store")
	}
}

func TestNew_AllowsHTTPS(t *testing.T) {
	s, err := New(Config{
		URL:    "https://nas:8080",
		APIKey: "key",
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if s == nil {
		t.Fatal("New() returned nil store")
	}
}

func TestNew_RejectsEmptyURL(t *testing.T) {
	_, err := New(Config{APIKey: "key"})
	if err == nil {
		t.Fatal("New() should reject empty URL")
	}
}

func TestListMessages_ZeroLimit(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify we get a valid page_size (default 20), not 0
		ps := r.URL.Query().Get("page_size")
		if ps == "0" {
			t.Error("page_size should not be 0")
		}
		resp := listMessagesResponse{
			Total:    0,
			Page:     1,
			PageSize: 20,
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer srv.Close()

	s := &Store{
		baseURL:    srv.URL,
		apiKey:     "test",
		httpClient: srv.Client(),
	}

	// This previously panicked with divide-by-zero
	msgs, total, err := s.ListMessages(0, 0)
	if err != nil {
		t.Fatalf("ListMessages(0, 0) error = %v", err)
	}
	if total != 0 {
		t.Errorf("total = %d, want 0", total)
	}
	if len(msgs) != 0 {
		t.Errorf("len(msgs) = %d, want 0", len(msgs))
	}
}

func TestListMessages_NegativeLimit(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ps := r.URL.Query().Get("page_size")
		if ps != "20" {
			t.Errorf("page_size = %q, want 20 (default)", ps)
		}
		resp := listMessagesResponse{Total: 0, Page: 1, PageSize: 20}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer srv.Close()

	s := &Store{
		baseURL:    srv.URL,
		apiKey:     "test",
		httpClient: srv.Client(),
	}

	_, _, err := s.ListMessages(0, -5)
	if err != nil {
		t.Fatalf("ListMessages(0, -5) error = %v", err)
	}
}

func TestSearchMessages_ZeroLimit(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ps := r.URL.Query().Get("page_size")
		if ps == "0" {
			t.Error("page_size should not be 0")
		}
		resp := searchResponse{
			Query:    "test",
			Total:    0,
			Page:     1,
			PageSize: 20,
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer srv.Close()

	s := &Store{
		baseURL:    srv.URL,
		apiKey:     "test",
		httpClient: srv.Client(),
	}

	// This previously panicked with divide-by-zero
	msgs, total, err := s.SearchMessages("test", 0, 0)
	if err != nil {
		t.Fatalf("SearchMessages(test, 0, 0) error = %v", err)
	}
	if total != 0 {
		t.Errorf("total = %d, want 0", total)
	}
	if len(msgs) != 0 {
		t.Errorf("len(msgs) = %d, want 0", len(msgs))
	}
}

func TestListMessages_PageCalculation(t *testing.T) {
	tests := []struct {
		name     string
		offset   int
		limit    int
		wantPage string
		wantSize string
	}{
		{"first page", 0, 20, "1", "20"},
		{"second page", 20, 20, "2", "20"},
		{"third page", 40, 20, "3", "20"},
		{"small pages", 10, 10, "2", "10"},
		{"zero limit defaults", 0, 0, "1", "20"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				page := r.URL.Query().Get("page")
				ps := r.URL.Query().Get("page_size")
				if page != tt.wantPage {
					t.Errorf("page = %q, want %q", page, tt.wantPage)
				}
				if ps != tt.wantSize {
					t.Errorf("page_size = %q, want %q", ps, tt.wantSize)
				}
				resp := listMessagesResponse{Total: 0, Page: 1, PageSize: 20}
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(resp)
			}))
			defer srv.Close()

			s := &Store{
				baseURL:    srv.URL,
				apiKey:     "test",
				httpClient: srv.Client(),
			}

			_, _, err := s.ListMessages(tt.offset, tt.limit)
			if err != nil {
				t.Fatalf("ListMessages(%d, %d) error = %v", tt.offset, tt.limit, err)
			}
		})
	}
}
