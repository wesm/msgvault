package api

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/wesm/msgvault/internal/fileutil"
	"github.com/wesm/msgvault/internal/store"
	"golang.org/x/oauth2"
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

// APIMessage is an alias for store.APIMessage — single source of truth for
// the message DTO shared between the store and API layers.
type APIMessage = store.APIMessage

// APIAttachment is an alias for store.APIAttachment.
type APIAttachment = store.APIAttachment

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
	Running  bool            `json:"running"`
	Accounts []AccountStatus `json:"accounts"`
}

// ErrorResponse represents an API error.
type ErrorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message,omitempty"`
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

// AttachmentInfo represents attachment metadata in API responses.
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

// toMessageSummary converts an APIMessage to a MessageSummary for API responses.
func toMessageSummary(m APIMessage) MessageSummary {
	to := m.To
	if to == nil {
		to = []string{}
	}
	labels := m.Labels
	if labels == nil {
		labels = []string{}
	}
	return MessageSummary{
		ID:        m.ID,
		Subject:   m.Subject,
		From:      m.From,
		To:        to,
		SentAt:    m.SentAt.UTC().Format(time.RFC3339),
		Snippet:   m.Snippet,
		Labels:    labels,
		HasAttach: m.HasAttachments,
		SizeBytes: m.SizeEstimate,
	}
}

// handleStats returns archive statistics.
func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	if s.store == nil {
		writeError(w, http.StatusServiceUnavailable, "store_unavailable", "Database not available")
		return
	}

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
		summaries[i] = toMessageSummary(m)
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
		MessageSummary: toMessageSummary(*msg),
		Body:           msg.Body,
	}

	attachments := make([]AttachmentInfo, 0, len(msg.Attachments))
	for _, att := range msg.Attachments {
		attachments = append(attachments, AttachmentInfo(att))
	}
	detail.Attachments = attachments

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
		summaries[i] = toMessageSummary(m)
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
	if s.scheduler == nil {
		writeError(w, http.StatusServiceUnavailable, "scheduler_unavailable", "Scheduler not available")
		return
	}

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
					info.LastSyncAt = status.LastRun.UTC().Format(time.RFC3339)
				}
				if !status.NextRun.IsZero() {
					info.NextSyncAt = status.NextRun.UTC().Format(time.RFC3339)
				}
				break
			}
		}

		accounts = append(accounts, info)
	}

	if accounts == nil {
		accounts = []AccountInfo{}
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"accounts": accounts,
	})
}

// handleTriggerSync manually triggers a sync for an account.
func (s *Server) handleTriggerSync(w http.ResponseWriter, r *http.Request) {
	if s.scheduler == nil {
		writeError(w, http.StatusServiceUnavailable, "scheduler_unavailable", "Scheduler not available")
		return
	}

	account := chi.URLParam(r, "account")
	if account == "" {
		writeError(w, http.StatusBadRequest, "missing_account", "Account email is required")
		return
	}

	if !s.scheduler.IsScheduled(account) {
		writeError(w, http.StatusNotFound, "not_found", "Account is not scheduled: "+account)
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
	if s.scheduler == nil {
		writeError(w, http.StatusServiceUnavailable, "scheduler_unavailable", "Scheduler not available")
		return
	}

	statuses := s.scheduler.Status()
	if statuses == nil {
		statuses = []AccountStatus{}
	}

	writeJSON(w, http.StatusOK, SchedulerStatusResponse{
		Running:  s.scheduler.IsRunning(),
		Accounts: statuses,
	})
}

// tokenFile represents the on-disk token format (matches oauth package).
type tokenFile struct {
	oauth2.Token
	Scopes []string `json:"scopes,omitempty"`
}

// handleUploadToken accepts a token from a remote client and saves it.
// POST /api/v1/auth/token/{email}
func (s *Server) handleUploadToken(w http.ResponseWriter, r *http.Request) {
	email := chi.URLParam(r, "email")
	if email == "" {
		writeError(w, http.StatusBadRequest, "missing_email", "Email address is required")
		return
	}

	// Validate email format (basic check)
	if !strings.Contains(email, "@") || !strings.Contains(email, ".") {
		writeError(w, http.StatusBadRequest, "invalid_email", "Invalid email format")
		return
	}

	// Read and validate token JSON
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20)) // 1MB limit
	if err != nil {
		writeError(w, http.StatusBadRequest, "read_error", "Failed to read request body")
		return
	}

	var tf tokenFile
	if err := json.Unmarshal(body, &tf); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json", "Invalid token JSON: "+err.Error())
		return
	}

	// Validate token has required fields
	if tf.RefreshToken == "" {
		writeError(w, http.StatusBadRequest, "invalid_token", "Token must include refresh_token")
		return
	}

	// Get tokens directory from config
	tokensDir := s.cfg.TokensDir()

	// Create tokens directory if needed
	if err := fileutil.SecureMkdirAll(tokensDir, 0700); err != nil {
		s.logger.Error("failed to create tokens directory", "error", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "Failed to create tokens directory")
		return
	}

	// Sanitize email for filename
	tokenPath := sanitizeTokenPath(tokensDir, email)

	// Marshal token back to JSON (normalized)
	data, err := json.MarshalIndent(tf, "", "  ")
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "Failed to serialize token")
		return
	}

	// Atomic write via temp file
	tmpFile, err := os.CreateTemp(tokensDir, ".token-*.tmp")
	if err != nil {
		s.logger.Error("failed to create temp file", "error", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "Failed to save token")
		return
	}
	tmpPath := tmpFile.Name()

	if _, err := tmpFile.Write(data); err != nil {
		tmpFile.Close()
		os.Remove(tmpPath)
		writeError(w, http.StatusInternalServerError, "internal_error", "Failed to write token")
		return
	}
	if err := tmpFile.Close(); err != nil {
		os.Remove(tmpPath)
		writeError(w, http.StatusInternalServerError, "internal_error", "Failed to close token file")
		return
	}
	if err := fileutil.SecureChmod(tmpPath, 0600); err != nil {
		os.Remove(tmpPath)
		writeError(w, http.StatusInternalServerError, "internal_error", "Failed to set token permissions")
		return
	}
	if err := os.Rename(tmpPath, tokenPath); err != nil {
		os.Remove(tmpPath)
		writeError(w, http.StatusInternalServerError, "internal_error", "Failed to save token")
		return
	}

	s.logger.Info("token uploaded via API", "email", email)
	writeJSON(w, http.StatusCreated, map[string]string{
		"status":  "created",
		"message": "Token saved for " + email,
	})
}

// sanitizeTokenPath returns a safe file path for the token.
func sanitizeTokenPath(tokensDir, email string) string {
	// Remove dangerous characters
	safe := strings.Map(func(r rune) rune {
		if r == '/' || r == '\\' || r == '\x00' {
			return -1
		}
		return r
	}, email)

	// Build path and verify it's within tokensDir
	path := filepath.Join(tokensDir, safe+".json")
	cleanPath := filepath.Clean(path)
	cleanTokensDir := filepath.Clean(tokensDir)

	// If path escapes tokensDir, use hash-based fallback
	if !strings.HasPrefix(cleanPath, cleanTokensDir+string(os.PathSeparator)) {
		return filepath.Join(tokensDir, fmt.Sprintf("%x.json", sha256.Sum256([]byte(email))))
	}

	return cleanPath
}
