package mcp

import (
	"context"
	"os"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"github.com/wesm/msgvault/internal/query"
)

// Tool name constants.
const (
	ToolSearchMessages = "search_messages"
	ToolGetMessage     = "get_message"
	ToolGetAttachment  = "get_attachment"
	ToolListMessages   = "list_messages"
	ToolGetStats       = "get_stats"
	ToolAggregate      = "aggregate"
)

// Common argument helpers for recurring tool option definitions.

func withLimit(defaultDesc string) mcp.ToolOption {
	return mcp.WithNumber("limit",
		mcp.Description("Maximum results to return (default "+defaultDesc+")"),
	)
}

func withOffset() mcp.ToolOption {
	return mcp.WithNumber("offset",
		mcp.Description("Number of results to skip for pagination (default 0)"),
	)
}

func withAfter() mcp.ToolOption {
	return mcp.WithString("after",
		mcp.Description("Only messages after this date (YYYY-MM-DD)"),
	)
}

func withBefore() mcp.ToolOption {
	return mcp.WithString("before",
		mcp.Description("Only messages before this date (YYYY-MM-DD)"),
	)
}

// Serve creates an MCP server with email archive tools and serves over stdio.
// It blocks until stdin is closed or the context is cancelled.
func Serve(ctx context.Context, engine query.Engine, attachmentsDir string) error {
	s := server.NewMCPServer(
		"msgvault",
		"1.0.0",
		server.WithToolCapabilities(false),
	)

	h := &handlers{engine: engine, attachmentsDir: attachmentsDir}

	s.AddTool(searchMessagesTool(), h.searchMessages)
	s.AddTool(getMessageTool(), h.getMessage)
	s.AddTool(getAttachmentTool(), h.getAttachment)
	s.AddTool(listMessagesTool(), h.listMessages)
	s.AddTool(getStatsTool(), h.getStats)
	s.AddTool(aggregateTool(), h.aggregate)

	stdio := server.NewStdioServer(s)
	return stdio.Listen(ctx, os.Stdin, os.Stdout)
}

func searchMessagesTool() mcp.Tool {
	return mcp.NewTool(ToolSearchMessages,
		mcp.WithDescription("Search emails using Gmail-like query syntax. Supports from:, to:, subject:, label:, has:attachment, before:, after:, and free text."),
		mcp.WithReadOnlyHintAnnotation(true),
		mcp.WithString("query",
			mcp.Required(),
			mcp.Description("Gmail-style search query (e.g. 'from:alice subject:meeting after:2024-01-01')"),
		),
		withLimit("20"),
		withOffset(),
	)
}

func getMessageTool() mcp.Tool {
	return mcp.NewTool(ToolGetMessage,
		mcp.WithDescription("Get full message details including body text, recipients, labels, and attachments by message ID."),
		mcp.WithReadOnlyHintAnnotation(true),
		mcp.WithNumber("id",
			mcp.Required(),
			mcp.Description("Message ID"),
		),
	)
}

func getAttachmentTool() mcp.Tool {
	return mcp.NewTool(ToolGetAttachment,
		mcp.WithDescription("Get attachment content by attachment ID. Returns base64-encoded content with metadata. Use get_message first to find attachment IDs."),
		mcp.WithReadOnlyHintAnnotation(true),
		mcp.WithNumber("attachment_id",
			mcp.Required(),
			mcp.Description("Attachment ID (from get_message response)"),
		),
	)
}

func listMessagesTool() mcp.Tool {
	return mcp.NewTool(ToolListMessages,
		mcp.WithDescription("List messages with optional filters. Returns message summaries sorted by date."),
		mcp.WithReadOnlyHintAnnotation(true),
		mcp.WithString("from",
			mcp.Description("Filter by sender email address"),
		),
		mcp.WithString("to",
			mcp.Description("Filter by recipient email address"),
		),
		mcp.WithString("label",
			mcp.Description("Filter by Gmail label"),
		),
		withAfter(),
		withBefore(),
		mcp.WithBoolean("has_attachment",
			mcp.Description("Only messages with attachments"),
		),
		withLimit("20"),
		withOffset(),
	)
}

func getStatsTool() mcp.Tool {
	return mcp.NewTool(ToolGetStats,
		mcp.WithDescription("Get archive overview: total messages, size, attachment count, and accounts."),
		mcp.WithReadOnlyHintAnnotation(true),
	)
}

func aggregateTool() mcp.Tool {
	return mcp.NewTool(ToolAggregate,
		mcp.WithDescription("Get grouped statistics (e.g. top senders, domains, labels, or message volume over time)."),
		mcp.WithReadOnlyHintAnnotation(true),
		mcp.WithString("group_by",
			mcp.Required(),
			mcp.Description("Dimension to group by"),
			mcp.Enum("sender", "recipient", "domain", "label", "time"),
		),
		withLimit("50"),
		withAfter(),
		withBefore(),
	)
}
