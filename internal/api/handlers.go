package api

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/wesm/msgvault/internal/scheduler"
)

// StatsResponse represents the archive statistics.
type StatsResponse struct {
	TotalMessages   int64 `json:"total_messages"`
	TotalThreads    int64 `json:"total_threads"`
	TotalAccounts   int64 `json:"total_accounts"`
	TotalLabels     int64 `json:"total_labels"`
	TotalAttach     int64 `json:"total_attachments"`
	DatabaseSize    int64 `json:"database_size_bytes"`
}

// AccountInfo represents an account in list responses.
type AccountInfo struct {
	Email       string `json:"email"`
	DisplayName string `json:"display_name,omitempty"`
	LastSyncAt  string `json:"last_sync_at,omitempty"`
	NextSyncAt  string `json:"next_sync_at,omitempty"`
	Schedule    string `json:"schedule,omitempty"`
	Enabled     bool   `json:"enabled"`
}

// SchedulerStatusResponse represents scheduler status.
type SchedulerStatusResponse struct {
	Running  bool                      `json:"running"`
	Accounts []scheduler.AccountStatus `json:"accounts"`
}

// ErrorResponse represents an API error.
type ErrorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message,omitempty"`
}

// writeJSON writes a JSON response.
func writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

// writeError writes an error response.
func writeError(w http.ResponseWriter, status int, err string, message string) {
	writeJSON(w, status, ErrorResponse{Error: err, Message: message})
}

// handleStats returns archive statistics.
func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	stats, err := s.store.GetStats()
	if err != nil {
		s.logger.Error("failed to get stats", "error", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "Failed to retrieve statistics")
		return
	}

	resp := StatsResponse{
		TotalMessages: stats.MessageCount,
		TotalThreads:  stats.ThreadCount,
		TotalAccounts: stats.SourceCount,
		TotalLabels:   stats.LabelCount,
		TotalAttach:   stats.AttachmentCount,
		DatabaseSize:  stats.DatabaseSize,
	}

	writeJSON(w, http.StatusOK, resp)
}

// handleListMessages returns a paginated list of messages.
// TODO(T-11): Implement with proper pagination and message listing
func (s *Server) handleListMessages(w http.ResponseWriter, r *http.Request) {
	writeError(w, http.StatusNotImplemented, "not_implemented", "Message listing not yet implemented")
}

// handleGetMessage returns a single message by ID.
// TODO(T-11): Implement with full message details
func (s *Server) handleGetMessage(w http.ResponseWriter, r *http.Request) {
	writeError(w, http.StatusNotImplemented, "not_implemented", "Message retrieval not yet implemented")
}

// handleSearch searches messages.
// TODO(T-11): Implement with FTS5 search
func (s *Server) handleSearch(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query().Get("q")
	if query == "" {
		writeError(w, http.StatusBadRequest, "missing_query", "Query parameter 'q' is required")
		return
	}

	writeError(w, http.StatusNotImplemented, "not_implemented", "Search not yet implemented")
}

// handleListAccounts returns all configured accounts.
func (s *Server) handleListAccounts(w http.ResponseWriter, r *http.Request) {
	var accounts []AccountInfo

	// Get schedule info from config
	for _, acc := range s.cfg.Accounts {
		info := AccountInfo{
			Email:    acc.Email,
			Schedule: acc.Schedule,
			Enabled:  acc.Enabled,
		}

		// Add scheduler status
		for _, status := range s.scheduler.Status() {
			if status.Email == acc.Email {
				if !status.LastRun.IsZero() {
					info.LastSyncAt = status.LastRun.Format("2006-01-02T15:04:05Z")
				}
				if !status.NextRun.IsZero() {
					info.NextSyncAt = status.NextRun.Format("2006-01-02T15:04:05Z")
				}
				break
			}
		}

		accounts = append(accounts, info)
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"accounts": accounts,
	})
}

// handleTriggerSync manually triggers a sync for an account.
func (s *Server) handleTriggerSync(w http.ResponseWriter, r *http.Request) {
	account := chi.URLParam(r, "account")
	if account == "" {
		writeError(w, http.StatusBadRequest, "missing_account", "Account email is required")
		return
	}

	err := s.scheduler.TriggerSync(account)
	if err != nil {
		s.logger.Error("failed to trigger sync", "account", account, "error", err)
		writeError(w, http.StatusConflict, "sync_error", err.Error())
		return
	}

	s.logger.Info("sync triggered via API", "account", account)
	writeJSON(w, http.StatusAccepted, map[string]string{
		"status":  "accepted",
		"message": "Sync started for " + account,
	})
}

// handleSchedulerStatus returns the scheduler status.
func (s *Server) handleSchedulerStatus(w http.ResponseWriter, r *http.Request) {
	statuses := s.scheduler.Status()
	writeJSON(w, http.StatusOK, SchedulerStatusResponse{
		Running:  true,
		Accounts: statuses,
	})
}
