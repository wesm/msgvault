package api

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/wesm/msgvault/internal/scheduler"
)

// StatsResponse represents the archive statistics.
type StatsResponse struct {
	TotalMessages int64 `json:"total_messages"`
	TotalThreads  int64 `json:"total_threads"`
	TotalAccounts int64 `json:"total_accounts"`
	TotalLabels   int64 `json:"total_labels"`
	TotalAttach   int64 `json:"total_attachments"`
	DatabaseSize  int64 `json:"database_size_bytes"`
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
	_ = json.NewEncoder(w).Encode(data)
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

// MessageSummary represents a message in list responses.
type MessageSummary struct {
	ID        int64    `json:"id"`
	Subject   string   `json:"subject"`
	From      string   `json:"from"`
	To        []string `json:"to"`
	SentAt    string   `json:"sent_at"`
	Snippet   string   `json:"snippet"`
	Labels    []string `json:"labels"`
	HasAttach bool     `json:"has_attachments"`
	SizeBytes int64    `json:"size_bytes"`
}

// MessageDetail represents a full message response.
type MessageDetail struct {
	MessageSummary
	Body        string           `json:"body"`
	Attachments []AttachmentInfo `json:"attachments"`
}

// AttachmentInfo represents attachment metadata.
type AttachmentInfo struct {
	Filename string `json:"filename"`
	MimeType string `json:"mime_type"`
	Size     int64  `json:"size_bytes"`
}

// SearchResult represents search results.
type SearchResult struct {
	Query    string           `json:"query"`
	Total    int64            `json:"total"`
	Page     int              `json:"page"`
	PageSize int              `json:"page_size"`
	Messages []MessageSummary `json:"messages"`
}

// handleListMessages returns a paginated list of messages.
func (s *Server) handleListMessages(w http.ResponseWriter, r *http.Request) {
	if s.store == nil {
		writeError(w, http.StatusServiceUnavailable, "store_unavailable", "Database not available")
		return
	}

	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	if page < 1 {
		page = 1
	}
	pageSize, _ := strconv.Atoi(r.URL.Query().Get("page_size"))
	if pageSize < 1 || pageSize > 100 {
		pageSize = 20
	}

	offset := (page - 1) * pageSize

	messages, total, err := s.store.ListMessages(offset, pageSize)
	if err != nil {
		s.logger.Error("failed to list messages", "error", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "Failed to retrieve messages")
		return
	}

	summaries := make([]MessageSummary, len(messages))
	for i, m := range messages {
		summaries[i] = MessageSummary{
			ID:        m.ID,
			Subject:   m.Subject,
			From:      m.From,
			To:        m.To,
			SentAt:    m.SentAt.Format("2006-01-02T15:04:05Z"),
			Snippet:   m.Snippet,
			Labels:    m.Labels,
			HasAttach: m.HasAttachments,
			SizeBytes: m.SizeEstimate,
		}
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"total":     total,
		"page":      page,
		"page_size": pageSize,
		"messages":  summaries,
	})
}

// handleGetMessage returns a single message by ID.
func (s *Server) handleGetMessage(w http.ResponseWriter, r *http.Request) {
	if s.store == nil {
		writeError(w, http.StatusServiceUnavailable, "store_unavailable", "Database not available")
		return
	}

	idStr := chi.URLParam(r, "id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_id", "Message ID must be a number")
		return
	}

	msg, err := s.store.GetMessage(id)
	if err != nil {
		s.logger.Error("failed to get message", "id", id, "error", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "Failed to retrieve message")
		return
	}
	if msg == nil {
		writeError(w, http.StatusNotFound, "not_found", "Message not found")
		return
	}

	detail := MessageDetail{
		MessageSummary: MessageSummary{
			ID:        msg.ID,
			Subject:   msg.Subject,
			From:      msg.From,
			To:        msg.To,
			SentAt:    msg.SentAt.Format("2006-01-02T15:04:05Z"),
			Snippet:   msg.Snippet,
			Labels:    msg.Labels,
			HasAttach: msg.HasAttachments,
			SizeBytes: msg.SizeEstimate,
		},
		Body: msg.Body,
	}

	for _, att := range msg.Attachments {
		detail.Attachments = append(detail.Attachments, AttachmentInfo{
			Filename: att.Filename,
			MimeType: att.MimeType,
			Size:     att.Size,
		})
	}

	writeJSON(w, http.StatusOK, detail)
}

// handleSearch searches messages.
func (s *Server) handleSearch(w http.ResponseWriter, r *http.Request) {
	if s.store == nil {
		writeError(w, http.StatusServiceUnavailable, "store_unavailable", "Database not available")
		return
	}

	query := r.URL.Query().Get("q")
	if query == "" {
		writeError(w, http.StatusBadRequest, "missing_query", "Query parameter 'q' is required")
		return
	}

	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	if page < 1 {
		page = 1
	}
	pageSize, _ := strconv.Atoi(r.URL.Query().Get("page_size"))
	if pageSize < 1 || pageSize > 100 {
		pageSize = 20
	}

	offset := (page - 1) * pageSize

	messages, total, err := s.store.SearchMessages(query, offset, pageSize)
	if err != nil {
		s.logger.Error("search failed", "query", query, "error", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "Search failed")
		return
	}

	summaries := make([]MessageSummary, len(messages))
	for i, m := range messages {
		summaries[i] = MessageSummary{
			ID:        m.ID,
			Subject:   m.Subject,
			From:      m.From,
			To:        m.To,
			SentAt:    m.SentAt.Format("2006-01-02T15:04:05Z"),
			Snippet:   m.Snippet,
			Labels:    m.Labels,
			HasAttach: m.HasAttachments,
			SizeBytes: m.SizeEstimate,
		}
	}

	writeJSON(w, http.StatusOK, SearchResult{
		Query:    query,
		Total:    total,
		Page:     page,
		PageSize: pageSize,
		Messages: summaries,
	})
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
