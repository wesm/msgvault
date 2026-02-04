package mcp

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/wesm/msgvault/internal/deletion"
	"github.com/wesm/msgvault/internal/query"
	"github.com/wesm/msgvault/internal/search"
)

const maxLimit = 1000

type handlers struct {
	engine         query.Engine
	attachmentsDir string
	dataDir        string
}

// getIDArg extracts a required positive integer ID from the arguments map.
func getIDArg(args map[string]any, key string) (int64, error) {
	v, ok := args[key].(float64)
	if !ok {
		return 0, fmt.Errorf("%s parameter is required", key)
	}
	if v != math.Trunc(v) || v < 1 || v > math.MaxInt64 {
		return 0, fmt.Errorf("%s must be a positive integer", key)
	}
	return int64(v), nil
}

// getDateArg extracts an optional date (YYYY-MM-DD) from the arguments map.
func getDateArg(args map[string]any, key string) (*time.Time, error) {
	v, ok := args[key].(string)
	if !ok || v == "" {
		return nil, nil
	}
	t, err := time.Parse("2006-01-02", v)
	if err != nil {
		return nil, fmt.Errorf("invalid %s date %q: expected YYYY-MM-DD", key, v)
	}
	return &t, nil
}

// readAttachmentFile reads the content-addressed attachment file after
// validating the hash and checking size limits.
func (h *handlers) readAttachmentFile(contentHash string) ([]byte, error) {
	if contentHash == "" || len(contentHash) < 2 {
		return nil, fmt.Errorf("attachment has no stored content")
	}
	if _, err := hex.DecodeString(contentHash); err != nil {
		return nil, fmt.Errorf("attachment has invalid content hash")
	}

	filePath := filepath.Join(h.attachmentsDir, contentHash[:2], contentHash)

	f, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("attachment file not available: %v", err)
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("attachment file not available: %v", err)
	}
	if info.Size() > maxAttachmentSize {
		return nil, fmt.Errorf("attachment too large: %d bytes (max %d)", info.Size(), maxAttachmentSize)
	}

	data, err := io.ReadAll(io.LimitReader(f, maxAttachmentSize+1))
	if err != nil {
		return nil, fmt.Errorf("attachment file not available: %v", err)
	}
	if int64(len(data)) > maxAttachmentSize {
		return nil, fmt.Errorf("attachment too large: %d bytes (max %d)", len(data), maxAttachmentSize)
	}

	return data, nil
}

func (h *handlers) searchMessages(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	args := req.GetArguments()

	queryStr, _ := args["query"].(string)
	if queryStr == "" {
		return mcp.NewToolResultError("query parameter is required"), nil
	}

	limit := limitArg(args, "limit", 20)
	offset := limitArg(args, "offset", 0)

	q := search.Parse(queryStr)

	// Try fast search first (metadata only), fall back to full FTS.
	results, err := h.engine.SearchFast(ctx, q, query.MessageFilter{}, limit, offset)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("search failed: %v", err)), nil
	}

	// If fast search returns nothing and query has free text, try full FTS.
	if len(results) == 0 && len(q.TextTerms) > 0 {
		results, err = h.engine.Search(ctx, q, limit, offset)
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("search failed: %v", err)), nil
		}
	}

	return jsonResult(results)
}

func (h *handlers) getMessage(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	args := req.GetArguments()

	id, err := getIDArg(args, "id")
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

	msg, err := h.engine.GetMessage(ctx, id)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("message not found: %v", err)), nil
	}

	return jsonResult(msg)
}

const maxAttachmentSize = 50 * 1024 * 1024 // 50MB

func (h *handlers) getAttachment(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	args := req.GetArguments()

	id, err := getIDArg(args, "attachment_id")
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

	att, err := h.engine.GetAttachment(ctx, id)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("get attachment failed: %v", err)), nil
	}
	if att == nil {
		return mcp.NewToolResultError("attachment not found"), nil
	}

	if h.attachmentsDir == "" {
		return mcp.NewToolResultError("attachments directory not configured"), nil
	}

	data, err := h.readAttachmentFile(att.ContentHash)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

	resp := struct {
		Filename      string `json:"filename"`
		MimeType      string `json:"mime_type"`
		Size          int64  `json:"size"`
		ContentBase64 string `json:"content_base64"`
	}{
		Filename:      att.Filename,
		MimeType:      att.MimeType,
		Size:          att.Size,
		ContentBase64: base64.StdEncoding.EncodeToString(data),
	}

	return jsonResult(resp)
}

func (h *handlers) listMessages(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	args := req.GetArguments()

	filter := query.MessageFilter{
		Pagination: query.Pagination{
			Limit:  limitArg(args, "limit", 20),
			Offset: limitArg(args, "offset", 0),
		},
	}

	if v, ok := args["from"].(string); ok && v != "" {
		filter.Sender = v
	}
	if v, ok := args["to"].(string); ok && v != "" {
		filter.Recipient = v
	}
	if v, ok := args["label"].(string); ok && v != "" {
		filter.Label = v
	}
	if v, ok := args["has_attachment"].(bool); ok && v {
		filter.WithAttachmentsOnly = true
	}
	var err error
	if filter.After, err = getDateArg(args, "after"); err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	if filter.Before, err = getDateArg(args, "before"); err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

	results, err := h.engine.ListMessages(ctx, filter)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("list failed: %v", err)), nil
	}

	return jsonResult(results)
}

func (h *handlers) getStats(ctx context.Context, _ mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	stats, err := h.engine.GetTotalStats(ctx, query.StatsOptions{})
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("stats failed: %v", err)), nil
	}

	accounts, err := h.engine.ListAccounts(ctx)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("accounts failed: %v", err)), nil
	}

	resp := struct {
		Stats    *query.TotalStats   `json:"stats"`
		Accounts []query.AccountInfo `json:"accounts"`
	}{
		Stats:    stats,
		Accounts: accounts,
	}

	return jsonResult(resp)
}

func (h *handlers) aggregate(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	args := req.GetArguments()

	groupBy, _ := args["group_by"].(string)
	if groupBy == "" {
		return mcp.NewToolResultError("group_by parameter is required"), nil
	}

	opts := query.AggregateOptions{
		Limit: limitArg(args, "limit", 50),
	}

	var err error
	if opts.After, err = getDateArg(args, "after"); err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	if opts.Before, err = getDateArg(args, "before"); err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

	viewTypeMap := map[string]query.ViewType{
		"sender":    query.ViewSenders,
		"recipient": query.ViewRecipients,
		"domain":    query.ViewDomains,
		"label":     query.ViewLabels,
		"time":      query.ViewTime,
	}

	viewType, ok := viewTypeMap[groupBy]
	if !ok {
		return mcp.NewToolResultError(fmt.Sprintf("invalid group_by: %s", groupBy)), nil
	}

	rows, err := h.engine.Aggregate(ctx, viewType, opts)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("aggregate failed: %v", err)), nil
	}

	return jsonResult(rows)
}

// limitArg extracts a non-negative integer limit from a map, with a default.
// JSON numbers arrive as float64. Clamps to maxLimit to prevent excessive
// result sets.
func limitArg(args map[string]any, key string, def int) int {
	v, ok := args[key].(float64)
	if !ok {
		return def
	}
	if math.IsNaN(v) || v < 0 {
		return 0
	}
	if math.IsInf(v, 1) || v > float64(maxLimit) {
		return maxLimit
	}
	return int(v)
}

func jsonResult(v any) (*mcp.CallToolResult, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("marshal error: %v", err)), nil
	}
	return mcp.NewToolResultText(string(data)), nil
}

// maxStageDeletionResults limits how many messages can be staged in one call.
const maxStageDeletionResults = 100000

func (h *handlers) stageDeletion(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	args := req.GetArguments()

	// Check for query vs structured filters
	queryStr, hasQuery := args["query"].(string)
	hasQuery = hasQuery && queryStr != ""

	// Check for any structured filter
	fromStr, _ := args["from"].(string)
	domainStr, _ := args["domain"].(string)
	labelStr, _ := args["label"].(string)
	hasAttachment, _ := args["has_attachment"].(bool)
	afterDate, err := getDateArg(args, "after")
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	beforeDate, err := getDateArg(args, "before")
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

	hasStructuredFilter := fromStr != "" || domainStr != "" || labelStr != "" ||
		hasAttachment || afterDate != nil || beforeDate != nil

	// Validate: must have either query or structured filters, but not both
	if hasQuery && hasStructuredFilter {
		return mcp.NewToolResultError("use either 'query' or structured filters (from, domain, label, etc.), not both"), nil
	}
	if !hasQuery && !hasStructuredFilter {
		return mcp.NewToolResultError("must provide either 'query' or at least one filter (from, domain, label, after, before, has_attachment)"), nil
	}

	var gmailIDs []string
	var description string

	if hasQuery {
		// Query-based search
		q := search.Parse(queryStr)

		// Try fast search first
		results, err := h.engine.SearchFast(ctx, q, query.MessageFilter{}, maxStageDeletionResults, 0)
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("search failed: %v", err)), nil
		}

		// Fall back to FTS if no results and query has text terms
		if len(results) == 0 && len(q.TextTerms) > 0 {
			results, err = h.engine.Search(ctx, q, maxStageDeletionResults, 0)
			if err != nil {
				return mcp.NewToolResultError(fmt.Sprintf("search failed: %v", err)), nil
			}
		}

		for _, msg := range results {
			gmailIDs = append(gmailIDs, msg.SourceMessageID)
		}
		description = fmt.Sprintf("query: %s", queryStr)
		if len(description) > 50 {
			description = description[:50]
		}
	} else {
		// Structured filter
		filter := query.MessageFilter{
			Sender:              fromStr,
			Domain:              domainStr,
			Label:               labelStr,
			WithAttachmentsOnly: hasAttachment,
			After:               afterDate,
			Before:              beforeDate,
		}

		var err error
		gmailIDs, err = h.engine.GetGmailIDsByFilter(ctx, filter)
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("filter failed: %v", err)), nil
		}

		// Build description from filters
		var parts []string
		if fromStr != "" {
			parts = append(parts, fmt.Sprintf("from:%s", fromStr))
		}
		if domainStr != "" {
			parts = append(parts, fmt.Sprintf("domain:%s", domainStr))
		}
		if labelStr != "" {
			parts = append(parts, fmt.Sprintf("label:%s", labelStr))
		}
		if hasAttachment {
			parts = append(parts, "has:attachment")
		}
		if afterDate != nil {
			parts = append(parts, fmt.Sprintf("after:%s", afterDate.Format("2006-01-02")))
		}
		if beforeDate != nil {
			parts = append(parts, fmt.Sprintf("before:%s", beforeDate.Format("2006-01-02")))
		}
		description = fmt.Sprintf("filter: %s", joinParts(parts, " "))
		if len(description) > 50 {
			description = description[:50]
		}
	}

	if len(gmailIDs) == 0 {
		return mcp.NewToolResultError("no messages match the specified criteria"), nil
	}

	// Create deletion manager and manifest
	deletionsDir := filepath.Join(h.dataDir, "deletions")
	manager, err := deletion.NewManager(deletionsDir)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("create deletion manager: %v", err)), nil
	}

	manifest := deletion.NewManifest(description, gmailIDs)
	manifest.CreatedBy = "mcp"

	// Set filter metadata for execution
	if fromStr != "" {
		manifest.Filters.Senders = []string{fromStr}
	}
	if domainStr != "" {
		manifest.Filters.SenderDomains = []string{domainStr}
	}
	if labelStr != "" {
		manifest.Filters.Labels = []string{labelStr}
	}
	if afterDate != nil {
		manifest.Filters.After = afterDate.Format("2006-01-02")
	}
	if beforeDate != nil {
		manifest.Filters.Before = beforeDate.Format("2006-01-02")
	}

	if err := manager.SaveManifest(manifest); err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("save manifest: %v", err)), nil
	}

	resp := struct {
		BatchID      string `json:"batch_id"`
		MessageCount int    `json:"message_count"`
		Status       string `json:"status"`
		NextStep     string `json:"next_step"`
	}{
		BatchID:      manifest.ID,
		MessageCount: len(gmailIDs),
		Status:       string(manifest.Status),
		NextStep:     "Run 'msgvault delete-staged' to execute deletion, or 'msgvault cancel-deletion " + manifest.ID + "' to cancel",
	}

	return jsonResult(resp)
}

// joinParts joins strings with a separator.
func joinParts(parts []string, sep string) string {
	if len(parts) == 0 {
		return ""
	}
	result := parts[0]
	for _, p := range parts[1:] {
		result += sep + p
	}
	return result
}
