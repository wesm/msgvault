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
	"github.com/wesm/msgvault/internal/query"
	"github.com/wesm/msgvault/internal/search"
)

const maxLimit = 1000

type handlers struct {
	engine         query.Engine
	attachmentsDir string
}

func (h *handlers) searchMessages(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	args := req.GetArguments()

	queryStr, _ := args["query"].(string)
	if queryStr == "" {
		return mcp.NewToolResultError("query parameter is required"), nil
	}

	limit := intArg(args, "limit", 20)
	offset := intArg(args, "offset", 0)

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

	idFloat, ok := args["id"].(float64)
	if !ok {
		return mcp.NewToolResultError("id parameter is required"), nil
	}
	if idFloat != math.Trunc(idFloat) || idFloat < 1 || idFloat > math.MaxInt64 {
		return mcp.NewToolResultError("id must be a positive integer"), nil
	}

	msg, err := h.engine.GetMessage(ctx, int64(idFloat))
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("message not found: %v", err)), nil
	}

	return jsonResult(msg)
}

const maxAttachmentSize = 50 * 1024 * 1024 // 50MB

func (h *handlers) getAttachment(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	args := req.GetArguments()

	idFloat, ok := args["attachment_id"].(float64)
	if !ok {
		return mcp.NewToolResultError("attachment_id parameter is required"), nil
	}
	if idFloat != math.Trunc(idFloat) || idFloat < 1 || idFloat > math.MaxInt64 {
		return mcp.NewToolResultError("attachment_id must be a positive integer"), nil
	}

	att, err := h.engine.GetAttachment(ctx, int64(idFloat))
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("get attachment failed: %v", err)), nil
	}
	if att == nil {
		return mcp.NewToolResultError("attachment not found"), nil
	}

	if h.attachmentsDir == "" {
		return mcp.NewToolResultError("attachments directory not configured"), nil
	}

	if att.ContentHash == "" || len(att.ContentHash) < 2 {
		return mcp.NewToolResultError("attachment has no stored content"), nil
	}

	// Validate content_hash is strictly hex to prevent path traversal.
	if _, err := hex.DecodeString(att.ContentHash); err != nil {
		return mcp.NewToolResultError("attachment has invalid content hash"), nil
	}

	filePath := filepath.Join(h.attachmentsDir, att.ContentHash[:2], att.ContentHash)

	// Open file and check size on the open fd to avoid TOCTOU races.
	f, err := os.Open(filePath)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("attachment file not available: %v", err)), nil
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("attachment file not available: %v", err)), nil
	}
	if info.Size() > maxAttachmentSize {
		return mcp.NewToolResultError(fmt.Sprintf("attachment too large: %d bytes (max %d)", info.Size(), maxAttachmentSize)), nil
	}

	data, err := io.ReadAll(io.LimitReader(f, maxAttachmentSize+1))
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("attachment file not available: %v", err)), nil
	}
	if int64(len(data)) > maxAttachmentSize {
		return mcp.NewToolResultError(fmt.Sprintf("attachment too large: %d bytes (max %d)", len(data), maxAttachmentSize)), nil
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
		Limit:  intArg(args, "limit", 20),
		Offset: intArg(args, "offset", 0),
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
	if v, ok := args["after"].(string); ok && v != "" {
		t, err := time.Parse("2006-01-02", v)
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("invalid after date %q: expected YYYY-MM-DD", v)), nil
		}
		filter.After = &t
	}
	if v, ok := args["before"].(string); ok && v != "" {
		t, err := time.Parse("2006-01-02", v)
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("invalid before date %q: expected YYYY-MM-DD", v)), nil
		}
		filter.Before = &t
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
		Limit: intArg(args, "limit", 50),
	}

	if v, ok := args["after"].(string); ok && v != "" {
		t, err := time.Parse("2006-01-02", v)
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("invalid after date %q: expected YYYY-MM-DD", v)), nil
		}
		opts.After = &t
	}
	if v, ok := args["before"].(string); ok && v != "" {
		t, err := time.Parse("2006-01-02", v)
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("invalid before date %q: expected YYYY-MM-DD", v)), nil
		}
		opts.Before = &t
	}

	var (
		rows []query.AggregateRow
		err  error
	)

	switch groupBy {
	case "sender":
		rows, err = h.engine.AggregateBySender(ctx, opts)
	case "recipient":
		rows, err = h.engine.AggregateByRecipient(ctx, opts)
	case "domain":
		rows, err = h.engine.AggregateByDomain(ctx, opts)
	case "label":
		rows, err = h.engine.AggregateByLabel(ctx, opts)
	case "time":
		rows, err = h.engine.AggregateByTime(ctx, opts)
	default:
		return mcp.NewToolResultError(fmt.Sprintf("invalid group_by: %s", groupBy)), nil
	}

	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("aggregate failed: %v", err)), nil
	}

	return jsonResult(rows)
}

// intArg extracts a non-negative integer from a map, with a default value.
// JSON numbers arrive as float64. Clamps on the float64 value before
// converting to int to avoid overflow on very large or special values.
func intArg(args map[string]any, key string, def int) int {
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
