package cmd

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	mcpserver "github.com/wesm/msgvault/internal/mcp"
	"github.com/wesm/msgvault/internal/query"
	"github.com/wesm/msgvault/internal/store"
)

var mcpForceSQL bool
var mcpNoSQLiteScanner bool

var mcpCmd = &cobra.Command{
	Use:   "mcp",
	Short: "Run MCP server for Claude Desktop integration",
	Long: `Start an MCP (Model Context Protocol) server over stdio.

This allows Claude Desktop (or any MCP client) to query your email archive
using tools like search_messages, get_message, list_messages, get_stats,
aggregate, and stage_deletion.

Add to Claude Desktop config:
  {
    "mcpServers": {
      "msgvault": {
        "command": "msgvault",
        "args": ["mcp"]
      }
    }
  }`,
	RunE: func(cmd *cobra.Command, args []string) error {
		dbPath := cfg.DatabaseDSN()

		// Open read-only: MCP is a query-only workload. This avoids
		// SQLite write-lock contention when multiple MCP processes
		// (one per Claude Code session) access the same database.
		// Schema migrations and FTS backfill are write operations
		// handled by init-db / sync / tui — not by MCP.
		s, err := store.OpenReadOnly(dbPath)
		if err != nil {
			return fmt.Errorf("open database: %w", err)
		}
		defer func() { _ = s.Close() }()

		if stale, col, err := s.SchemaStale(); err != nil {
			return fmt.Errorf("check schema: %w", err)
		} else if stale {
			return fmt.Errorf(
				"database schema is outdated (missing %s); "+
					"run 'msgvault init-db' to update", col)
		}

		if s.FTS5Available() && s.NeedsFTSBackfill() {
			fmt.Fprintf(os.Stderr,
				"Warning: full-text search index needs populating; "+
					"body-text search will return incomplete results "+
					"until 'msgvault tui' or 'msgvault search' is run\n")
		}

		var engine query.Engine
		analyticsDir := cfg.AnalyticsDir()

		if !mcpForceSQL && query.HasCompleteParquetData(analyticsDir) {
			var duckOpts query.DuckDBOptions
			if mcpNoSQLiteScanner {
				duckOpts.DisableSQLiteScanner = true
			}
			duckEngine, err := query.NewDuckDBEngine(analyticsDir, dbPath, s.DB(), duckOpts)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Warning: Failed to open Parquet engine: %v\n", err)
				fmt.Fprintf(os.Stderr, "Falling back to SQLite\n")
				engine = query.NewSQLiteEngine(s.DB())
			} else {
				engine = duckEngine
				defer func() { _ = duckEngine.Close() }()
			}
		} else {
			engine = query.NewSQLiteEngine(s.DB())
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		return mcpserver.Serve(ctx, engine, cfg.AttachmentsDir(), cfg.Data.DataDir)
	},
}

func init() {
	rootCmd.AddCommand(mcpCmd)
	mcpCmd.Flags().BoolVar(&mcpForceSQL, "force-sql", false, "Force SQLite queries instead of Parquet")
	mcpCmd.Flags().BoolVar(&mcpNoSQLiteScanner, "no-sqlite-scanner", false, "Disable DuckDB sqlite_scanner extension (use direct SQLite fallback)")
	_ = mcpCmd.Flags().MarkHidden("no-sqlite-scanner")
}
