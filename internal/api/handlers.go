package api

import (
	"context"
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
	"github.com/wesm/msgvault/internal/config"
	"github.com/wesm/msgvault/internal/fileutil"
	"github.com/wesm/msgvault/internal/query"
	"github.com/wesm/msgvault/internal/scheduler"
	"github.com/wesm/msgvault/internal/search"
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

	s.cfgMu.RLock()
	cfgAccounts := make([]config.AccountSchedule, len(s.cfg.Accounts))
	copy(cfgAccounts, s.cfg.Accounts)
	s.cfgMu.RUnlock()

	var accounts []AccountInfo

	// Get schedule info from config
	for _, acc := range cfgAccounts {
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
		s.logger.Warn("invalid token JSON", "error", err)
		writeError(w, http.StatusBadRequest, "invalid_json", "Invalid token JSON format")
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

// AddAccountRequest represents a request to add an account to the config.
type AddAccountRequest struct {
	Email    string `json:"email"`
	Schedule string `json:"schedule"` // Cron expression, defaults to "0 2 * * *"
	Enabled  bool   `json:"enabled"`  // Defaults to true
}

// handleAddAccount adds an account to the config file.
// POST /api/v1/accounts
func (s *Server) handleAddAccount(w http.ResponseWriter, r *http.Request) {
	var req AddAccountRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.logger.Warn("invalid account request JSON", "error", err)
		writeError(w, http.StatusBadRequest, "invalid_json", "Invalid request JSON format")
		return
	}

	// Validate email
	if req.Email == "" {
		writeError(w, http.StatusBadRequest, "missing_email", "Email is required")
		return
	}
	if !strings.Contains(req.Email, "@") || !strings.Contains(req.Email, ".") {
		writeError(w, http.StatusBadRequest, "invalid_email", "Invalid email format")
		return
	}

	// Set defaults
	if req.Schedule == "" {
		req.Schedule = "0 2 * * *" // Default: 2am daily
	}
	req.Enabled = true // Always enable — caller is export-token registering for sync

	// Validate cron expression before persisting
	if err := scheduler.ValidateCronExpr(req.Schedule); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_schedule", err.Error())
		return
	}

	s.cfgMu.Lock()

	// Check if account already exists
	for _, acc := range s.cfg.Accounts {
		if acc.Email == req.Email {
			s.cfgMu.Unlock()
			writeJSON(w, http.StatusOK, map[string]string{
				"status":  "exists",
				"message": "Account already configured for " + req.Email,
			})
			return
		}
	}

	// Add account to config
	newAccount := config.AccountSchedule{
		Email:    req.Email,
		Schedule: req.Schedule,
		Enabled:  req.Enabled,
	}
	s.cfg.Accounts = append(s.cfg.Accounts, newAccount)

	// Save config; rollback in-memory state on failure
	if err := s.cfg.Save(); err != nil {
		s.cfg.Accounts = s.cfg.Accounts[:len(s.cfg.Accounts)-1]
		s.cfgMu.Unlock()
		s.logger.Error("failed to save config", "error", err)
		writeError(w, http.StatusInternalServerError, "save_error", "Failed to save configuration")
		return
	}

	s.cfgMu.Unlock()

	// Register with live scheduler (best-effort — config is already saved)
	if s.scheduler != nil {
		if err := s.scheduler.AddAccount(req.Email, req.Schedule); err != nil {
			s.logger.Warn("account saved but scheduler registration failed",
				"email", req.Email, "error", err)
		}
	}

	s.logger.Info("account added via API", "email", req.Email, "schedule", req.Schedule)
	writeJSON(w, http.StatusCreated, map[string]string{
		"status":  "created",
		"message": "Account added for " + req.Email,
	})
}

// ============================================================================
// TUI Aggregate Endpoints
// ============================================================================

// AggregateResponse represents aggregate query results.
type AggregateResponse struct {
	ViewType string             `json:"view_type"`
	Rows     []AggregateRowJSON `json:"rows"`
}

// AggregateRowJSON represents a single aggregate row in JSON format.
type AggregateRowJSON struct {
	Key             string `json:"key"`
	Count           int64  `json:"count"`
	TotalSize       int64  `json:"total_size"`
	AttachmentSize  int64  `json:"attachment_size"`
	AttachmentCount int64  `json:"attachment_count"`
	TotalUnique     int64  `json:"total_unique"`
}

// TotalStatsResponse represents detailed stats with filters.
type TotalStatsResponse struct {
	MessageCount    int64 `json:"message_count"`
	TotalSize       int64 `json:"total_size"`
	AttachmentCount int64 `json:"attachment_count"`
	AttachmentSize  int64 `json:"attachment_size"`
	LabelCount      int64 `json:"label_count"`
	AccountCount    int64 `json:"account_count"`
}

// SearchFastResponse represents fast search results with stats.
type SearchFastResponse struct {
	Query      string              `json:"query"`
	Messages   []MessageSummary    `json:"messages"`
	TotalCount int64               `json:"total_count"`
	Stats      *TotalStatsResponse `json:"stats,omitempty"`
}

// parseViewType parses a view type string into query.ViewType.
func parseViewType(s string) (query.ViewType, bool) {
	switch strings.ToLower(s) {
	case "senders":
		return query.ViewSenders, true
	case "sender_names":
		return query.ViewSenderNames, true
	case "recipients":
		return query.ViewRecipients, true
	case "recipient_names":
		return query.ViewRecipientNames, true
	case "domains":
		return query.ViewDomains, true
	case "labels":
		return query.ViewLabels, true
	case "time":
		return query.ViewTime, true
	default:
		return query.ViewSenders, false
	}
}

// viewTypeString converts a query.ViewType to its API string representation.
func viewTypeString(v query.ViewType) string {
	switch v {
	case query.ViewSenders:
		return "senders"
	case query.ViewSenderNames:
		return "sender_names"
	case query.ViewRecipients:
		return "recipients"
	case query.ViewRecipientNames:
		return "recipient_names"
	case query.ViewDomains:
		return "domains"
	case query.ViewLabels:
		return "labels"
	case query.ViewTime:
		return "time"
	default:
		return "unknown"
	}
}

// parseSortField parses a sort field string into query.SortField.
func parseSortField(s string) query.SortField {
	switch strings.ToLower(s) {
	case "count":
		return query.SortByCount
	case "size":
		return query.SortBySize
	case "attachment_size":
		return query.SortByAttachmentSize
	case "name":
		return query.SortByName
	default:
		return query.SortByCount
	}
}

// parseSortDirection parses a direction string into query.SortDirection.
func parseSortDirection(s string) query.SortDirection {
	if strings.ToLower(s) == "asc" {
		return query.SortAsc
	}
	return query.SortDesc
}

// parseTimeGranularity parses a granularity string into query.TimeGranularity.
func parseTimeGranularity(s string) query.TimeGranularity {
	switch strings.ToLower(s) {
	case "year":
		return query.TimeYear
	case "day":
		return query.TimeDay
	default:
		return query.TimeMonth
	}
}

// parseAggregateOptions extracts common aggregate options from query parameters.
func parseAggregateOptions(r *http.Request) query.AggregateOptions {
	opts := query.DefaultAggregateOptions()

	if v := r.URL.Query().Get("sort"); v != "" {
		opts.SortField = parseSortField(v)
	}
	if v := r.URL.Query().Get("direction"); v != "" {
		opts.SortDirection = parseSortDirection(v)
	}
	if v := r.URL.Query().Get("limit"); v != "" {
		if limit, err := strconv.Atoi(v); err == nil && limit > 0 {
			opts.Limit = limit
		}
	}
	if v := r.URL.Query().Get("time_granularity"); v != "" {
		opts.TimeGranularity = parseTimeGranularity(v)
	}
	if v := r.URL.Query().Get("source_id"); v != "" {
		if id, err := strconv.ParseInt(v, 10, 64); err == nil {
			opts.SourceID = &id
		}
	}
	if r.URL.Query().Get("attachments_only") == "true" {
		opts.WithAttachmentsOnly = true
	}
	if r.URL.Query().Get("hide_deleted") == "true" {
		opts.HideDeletedFromSource = true
	}
	if v := r.URL.Query().Get("search_query"); v != "" {
		opts.SearchQuery = v
	}

	return opts
}

// parseMessageFilter extracts filter parameters from query parameters.
func parseMessageFilter(r *http.Request) query.MessageFilter {
	var filter query.MessageFilter

	filter.Sender = r.URL.Query().Get("sender")
	filter.SenderName = r.URL.Query().Get("sender_name")
	filter.Recipient = r.URL.Query().Get("recipient")
	filter.RecipientName = r.URL.Query().Get("recipient_name")
	filter.Domain = r.URL.Query().Get("domain")
	filter.Label = r.URL.Query().Get("label")

	if v := r.URL.Query().Get("time_period"); v != "" {
		filter.TimeRange.Period = v
	}
	if v := r.URL.Query().Get("time_granularity"); v != "" {
		filter.TimeRange.Granularity = parseTimeGranularity(v)
	}
	if v := r.URL.Query().Get("conversation_id"); v != "" {
		if id, err := strconv.ParseInt(v, 10, 64); err == nil {
			filter.ConversationID = &id
		}
	}
	if v := r.URL.Query().Get("source_id"); v != "" {
		if id, err := strconv.ParseInt(v, 10, 64); err == nil {
			filter.SourceID = &id
		}
	}
	if r.URL.Query().Get("attachments_only") == "true" {
		filter.WithAttachmentsOnly = true
	}
	if r.URL.Query().Get("hide_deleted") == "true" {
		filter.HideDeletedFromSource = true
	}

	// Pagination
	if v := r.URL.Query().Get("offset"); v != "" {
		if offset, err := strconv.Atoi(v); err == nil && offset >= 0 {
			filter.Pagination.Offset = offset
		}
	}
	if v := r.URL.Query().Get("limit"); v != "" {
		if limit, err := strconv.Atoi(v); err == nil && limit > 0 {
			filter.Pagination.Limit = limit
		}
	}
	if filter.Pagination.Limit == 0 {
		filter.Pagination.Limit = 500 // Default limit for message lists
	}

	// Sorting
	if v := r.URL.Query().Get("sort"); v != "" {
		switch strings.ToLower(v) {
		case "date":
			filter.Sorting.Field = query.MessageSortByDate
		case "size":
			filter.Sorting.Field = query.MessageSortBySize
		case "subject":
			filter.Sorting.Field = query.MessageSortBySubject
		}
	}
	if v := r.URL.Query().Get("direction"); v != "" {
		filter.Sorting.Direction = parseSortDirection(v)
	}

	return filter
}

// toAggregateRowJSON converts query.AggregateRow to JSON format.
func toAggregateRowJSON(row query.AggregateRow) AggregateRowJSON {
	return AggregateRowJSON{
		Key:             row.Key,
		Count:           row.Count,
		TotalSize:       row.TotalSize,
		AttachmentSize:  row.AttachmentSize,
		AttachmentCount: row.AttachmentCount,
		TotalUnique:     row.TotalUnique,
	}
}

// toTotalStatsResponse converts query.TotalStats to JSON format.
func toTotalStatsResponse(stats *query.TotalStats) *TotalStatsResponse {
	if stats == nil {
		return nil
	}
	return &TotalStatsResponse{
		MessageCount:    stats.MessageCount,
		TotalSize:       stats.TotalSize,
		AttachmentCount: stats.AttachmentCount,
		AttachmentSize:  stats.AttachmentSize,
		LabelCount:      stats.LabelCount,
		AccountCount:    stats.AccountCount,
	}
}

// toMessageSummaryFromQuery converts query.MessageSummary to API MessageSummary.
func toMessageSummaryFromQuery(m query.MessageSummary) MessageSummary {
	labels := m.Labels
	if labels == nil {
		labels = []string{}
	}
	return MessageSummary{
		ID:        m.ID,
		Subject:   m.Subject,
		From:      m.FromEmail,
		To:        []string{}, // Query summary doesn't include recipients
		SentAt:    m.SentAt.UTC().Format(time.RFC3339),
		Snippet:   m.Snippet,
		Labels:    labels,
		HasAttach: m.HasAttachments,
		SizeBytes: m.SizeEstimate,
	}
}

// handleAggregates returns aggregate data for a view type.
// GET /api/v1/aggregates?view_type=senders&sort=count&direction=desc&limit=100
func (s *Server) handleAggregates(w http.ResponseWriter, r *http.Request) {
	if s.engine == nil {
		writeError(w, http.StatusServiceUnavailable, "engine_unavailable", "Query engine not available")
		return
	}

	viewTypeStr := r.URL.Query().Get("view_type")
	if viewTypeStr == "" {
		viewTypeStr = "senders" // Default
	}
	viewType, ok := parseViewType(viewTypeStr)
	if !ok {
		writeError(w, http.StatusBadRequest, "invalid_view_type",
			"Invalid view_type. Must be one of: senders, sender_names, recipients, recipient_names, domains, labels, time")
		return
	}

	opts := parseAggregateOptions(r)

	ctx := context.Background()
	rows, err := s.engine.Aggregate(ctx, viewType, opts)
	if err != nil {
		s.logger.Error("aggregate query failed", "view_type", viewTypeStr, "error", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "Aggregate query failed")
		return
	}

	jsonRows := make([]AggregateRowJSON, len(rows))
	for i, row := range rows {
		jsonRows[i] = toAggregateRowJSON(row)
	}

	writeJSON(w, http.StatusOK, AggregateResponse{
		ViewType: viewTypeString(viewType),
		Rows:     jsonRows,
	})
}

// handleSubAggregates returns sub-aggregate data after drill-down.
// GET /api/v1/aggregates/sub?view_type=labels&sender=foo@example.com
func (s *Server) handleSubAggregates(w http.ResponseWriter, r *http.Request) {
	if s.engine == nil {
		writeError(w, http.StatusServiceUnavailable, "engine_unavailable", "Query engine not available")
		return
	}

	viewTypeStr := r.URL.Query().Get("view_type")
	if viewTypeStr == "" {
		writeError(w, http.StatusBadRequest, "missing_view_type", "view_type parameter is required")
		return
	}
	viewType, ok := parseViewType(viewTypeStr)
	if !ok {
		writeError(w, http.StatusBadRequest, "invalid_view_type",
			"Invalid view_type. Must be one of: senders, sender_names, recipients, recipient_names, domains, labels, time")
		return
	}

	filter := parseMessageFilter(r)
	opts := parseAggregateOptions(r)

	ctx := context.Background()
	rows, err := s.engine.SubAggregate(ctx, filter, viewType, opts)
	if err != nil {
		s.logger.Error("sub-aggregate query failed", "view_type", viewTypeStr, "error", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "Sub-aggregate query failed")
		return
	}

	jsonRows := make([]AggregateRowJSON, len(rows))
	for i, row := range rows {
		jsonRows[i] = toAggregateRowJSON(row)
	}

	writeJSON(w, http.StatusOK, AggregateResponse{
		ViewType: viewTypeString(viewType),
		Rows:     jsonRows,
	})
}

// handleFilteredMessages returns a filtered list of messages.
// GET /api/v1/messages/filter?sender=foo@example.com&offset=0&limit=500
func (s *Server) handleFilteredMessages(w http.ResponseWriter, r *http.Request) {
	if s.engine == nil {
		writeError(w, http.StatusServiceUnavailable, "engine_unavailable", "Query engine not available")
		return
	}

	filter := parseMessageFilter(r)

	ctx := context.Background()
	messages, err := s.engine.ListMessages(ctx, filter)
	if err != nil {
		s.logger.Error("filtered messages query failed", "error", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "Message query failed")
		return
	}

	summaries := make([]MessageSummary, len(messages))
	for i, m := range messages {
		summaries[i] = toMessageSummaryFromQuery(m)
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"total":    len(summaries), // Note: This is the returned count, not total matching
		"offset":   filter.Pagination.Offset,
		"limit":    filter.Pagination.Limit,
		"messages": summaries,
	})
}

// handleTotalStats returns detailed stats with optional filters.
// GET /api/v1/stats/total?source_id=1&attachments_only=true
func (s *Server) handleTotalStats(w http.ResponseWriter, r *http.Request) {
	if s.engine == nil {
		writeError(w, http.StatusServiceUnavailable, "engine_unavailable", "Query engine not available")
		return
	}

	var opts query.StatsOptions

	if v := r.URL.Query().Get("source_id"); v != "" {
		if id, err := strconv.ParseInt(v, 10, 64); err == nil {
			opts.SourceID = &id
		}
	}
	if r.URL.Query().Get("attachments_only") == "true" {
		opts.WithAttachmentsOnly = true
	}
	if r.URL.Query().Get("hide_deleted") == "true" {
		opts.HideDeletedFromSource = true
	}
	if v := r.URL.Query().Get("search_query"); v != "" {
		opts.SearchQuery = v
	}
	if v := r.URL.Query().Get("group_by"); v != "" {
		if viewType, ok := parseViewType(v); ok {
			opts.GroupBy = viewType
		}
	}

	ctx := context.Background()
	stats, err := s.engine.GetTotalStats(ctx, opts)
	if err != nil {
		s.logger.Error("total stats query failed", "error", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "Stats query failed")
		return
	}

	writeJSON(w, http.StatusOK, toTotalStatsResponse(stats))
}

// handleFastSearch performs fast metadata search (subject, sender, recipient).
// GET /api/v1/search/fast?q=invoice&offset=0&limit=100
func (s *Server) handleFastSearch(w http.ResponseWriter, r *http.Request) {
	if s.engine == nil {
		writeError(w, http.StatusServiceUnavailable, "engine_unavailable", "Query engine not available")
		return
	}

	queryStr := r.URL.Query().Get("q")
	if queryStr == "" {
		writeError(w, http.StatusBadRequest, "missing_query", "Query parameter 'q' is required")
		return
	}

	filter := parseMessageFilter(r)

	// Get view type for stats grouping
	var statsGroupBy query.ViewType
	if v := r.URL.Query().Get("view_type"); v != "" {
		statsGroupBy, _ = parseViewType(v)
	}

	offset := filter.Pagination.Offset
	limit := filter.Pagination.Limit
	if limit == 0 || limit > 500 {
		limit = 100
	}

	ctx := context.Background()
	q := search.Parse(queryStr)

	result, err := s.engine.SearchFastWithStats(ctx, q, queryStr, filter, statsGroupBy, limit, offset)
	if err != nil {
		s.logger.Error("fast search failed", "query", queryStr, "error", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "Search failed")
		return
	}

	summaries := make([]MessageSummary, len(result.Messages))
	for i, m := range result.Messages {
		summaries[i] = toMessageSummaryFromQuery(m)
	}

	writeJSON(w, http.StatusOK, SearchFastResponse{
		Query:      queryStr,
		Messages:   summaries,
		TotalCount: result.TotalCount,
		Stats:      toTotalStatsResponse(result.Stats),
	})
}

// handleDeepSearch performs full-text body search via FTS5.
// GET /api/v1/search/deep?q=invoice&offset=0&limit=100
func (s *Server) handleDeepSearch(w http.ResponseWriter, r *http.Request) {
	if s.engine == nil {
		writeError(w, http.StatusServiceUnavailable, "engine_unavailable", "Query engine not available")
		return
	}

	queryStr := r.URL.Query().Get("q")
	if queryStr == "" {
		writeError(w, http.StatusBadRequest, "missing_query", "Query parameter 'q' is required")
		return
	}

	offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
	if offset < 0 {
		offset = 0
	}
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 || limit > 500 {
		limit = 100
	}

	ctx := context.Background()
	q := search.Parse(queryStr)

	messages, err := s.engine.Search(ctx, q, limit, offset)
	if err != nil {
		s.logger.Error("deep search failed", "query", queryStr, "error", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "Search failed")
		return
	}

	summaries := make([]MessageSummary, len(messages))
	for i, m := range messages {
		summaries[i] = toMessageSummaryFromQuery(m)
	}

	// For deep search, we don't have a fast count, so use -1 to indicate unknown
	totalCount := int64(len(summaries))
	if len(summaries) == limit {
		totalCount = -1 // Indicates more results may exist
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"query":       queryStr,
		"messages":    summaries,
		"total_count": totalCount,
		"offset":      offset,
		"limit":       limit,
	})
}
