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
and aggregate.

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
		s, err := store.Open(dbPath)
		if err != nil {
			return fmt.Errorf("open database: %w", err)
		}
		defer s.Close()

		// Ensure schema is up to date
		if err := s.InitSchema(); err != nil {
			return fmt.Errorf("init schema: %w", err)
		}

		// Build FTS index in background — MCP should start serving immediately
		if s.NeedsFTSBackfill() {
			go func() {
				_, _ = s.BackfillFTS(nil)
			}()
		}

		var engine query.Engine
		analyticsDir := cfg.AnalyticsDir()

		if !mcpForceSQL && query.HasParquetData(analyticsDir) {
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
				defer duckEngine.Close()
			}
		} else {
			engine = query.NewSQLiteEngine(s.DB())
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		return mcpserver.Serve(ctx, engine, cfg.AttachmentsDir())
	},
}

func init() {
	rootCmd.AddCommand(mcpCmd)
	mcpCmd.Flags().BoolVar(&mcpForceSQL, "force-sql", false, "Force SQLite queries instead of Parquet")
	mcpCmd.Flags().BoolVar(&mcpNoSQLiteScanner, "no-sqlite-scanner", false, "Disable DuckDB sqlite_scanner extension (use direct SQLite fallback)")
	_ = mcpCmd.Flags().MarkHidden("no-sqlite-scanner")
}
