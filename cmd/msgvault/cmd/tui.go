package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/spf13/cobra"
	"github.com/wesm/msgvault/internal/query"
	"github.com/wesm/msgvault/internal/store"
	"github.com/wesm/msgvault/internal/tui"
)

var forceSQL bool
var skipCacheBuild bool
var noSQLiteScanner bool

var tuiCmd = &cobra.Command{
	Use:   "tui",
	Short: "Open the interactive terminal UI",
	Long: `Open an interactive terminal UI for browsing your email archive.

The TUI provides aggregate views of your messages by:
  - Senders: Who sends you the most email
  - Recipients: Who you email most frequently
  - Domains: Which domains you interact with
  - Labels: Gmail label distribution
  - Time: Message volume over time

Navigation:
  ↑/k, ↓/j    Move up/down
  PgUp/PgDn   Page up/down
  Enter       Drill down / view message
  Esc         Go back
  Tab         Switch view (aggregates only)
  s           Cycle sort field
  r           Reverse sort direction
  t           Toggle time granularity (Time view only)

Selection & Deletion:
  Space       Toggle selection
  A           Select all visible
  x           Clear selection
  D           Stage selected for deletion
  q           Quit

Performance:
  For large archives (100k+ messages), the TUI uses Parquet files for fast
  aggregation queries. Run 'msgvault-sync build-parquet' to generate them.
  Use --force-sql to bypass Parquet and query SQLite directly (slow).`,
	RunE: func(cmd *cobra.Command, args []string) error {
		// Open database
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

		// Build FTS index in background — TUI uses DuckDB/Parquet for
		// aggregates and only needs FTS for deep search (Tab to switch).
		if s.NeedsFTSBackfill() {
			go func() {
				_, _ = s.BackfillFTS(nil)
			}()
		}

		analyticsDir := cfg.AnalyticsDir()

		// Check if cache needs to be built/updated (unless forcing SQL or skipping)
		if !forceSQL && !skipCacheBuild {
			needsBuild, reason := cacheNeedsBuild(dbPath, analyticsDir)
			if needsBuild {
				fmt.Printf("Building analytics cache (%s)...\n", reason)
				result, err := buildCache(dbPath, analyticsDir, true)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Warning: Failed to build cache: %v\n", err)
					fmt.Fprintf(os.Stderr, "Falling back to SQLite (may be slow for large archives)\n")
				} else if !result.Skipped {
					fmt.Printf("Cached %d messages for fast queries.\n", result.ExportedCount)
				}
			}
		}

		// Determine query engine to use
		var engine query.Engine

		if !forceSQL && query.HasParquetData(analyticsDir) {
			// Use DuckDB for fast Parquet queries
			var duckOpts query.DuckDBOptions
			if noSQLiteScanner {
				duckOpts.DisableSQLiteScanner = true
			}
			duckEngine, err := query.NewDuckDBEngine(analyticsDir, dbPath, s.DB(), duckOpts)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Warning: Failed to open Parquet engine: %v\n", err)
				fmt.Fprintf(os.Stderr, "Falling back to SQLite (may be slow for large archives)\n")
				engine = query.NewSQLiteEngine(s.DB())
			} else {
				engine = duckEngine
				defer duckEngine.Close()
			}
		} else {
			// Use SQLite directly
			if !forceSQL {
				fmt.Fprintf(os.Stderr, "Note: No cache data available, using SQLite (slow for large archives)\n")
				fmt.Fprintf(os.Stderr, "Run 'msgvault build-cache' to enable fast queries.\n")
			}
			engine = query.NewSQLiteEngine(s.DB())
		}

		// Create and run TUI
		model := tui.New(engine, tui.Options{DataDir: cfg.Data.DataDir, Version: Version})
		p := tea.NewProgram(model, tea.WithAltScreen())

		if _, err := p.Run(); err != nil {
			return fmt.Errorf("run tui: %w", err)
		}

		return nil
	},
}

// cacheNeedsBuild checks if the analytics cache needs to be built or updated.
// Returns (needsBuild, reason) where reason describes why.
func cacheNeedsBuild(dbPath, analyticsDir string) (bool, string) {
	messagesDir := filepath.Join(analyticsDir, "messages")
	stateFile := filepath.Join(analyticsDir, "_last_sync.json")

	// Check if cache directory exists with parquet files
	if !query.HasParquetData(analyticsDir) {
		return true, "no cache exists"
	}

	// Load last sync state
	data, err := os.ReadFile(stateFile)
	if err != nil {
		return true, "no sync state found"
	}

	var state syncState
	if err := json.Unmarshal(data, &state); err != nil {
		return true, "invalid sync state"
	}

	// Check if SQLite has newer messages
	// We need to query SQLite directly to check max message ID
	db, err := store.Open(dbPath)
	if err != nil {
		// Can't open DB to check - force rebuild to be safe
		return true, "cannot verify cache status"
	}
	defer db.Close()

	var maxID int64
	err = db.DB().QueryRow(`
		SELECT COALESCE(MAX(id), 0) FROM messages
		WHERE deleted_from_source_at IS NULL AND sent_at IS NOT NULL
	`).Scan(&maxID)
	if err != nil {
		// Can't query - force rebuild to be safe
		return true, "cannot verify cache status"
	}

	if maxID > state.LastMessageID {
		newCount := maxID - state.LastMessageID
		return true, fmt.Sprintf("%d new messages", newCount)
	}

	// Check if parquet files actually exist (directory might be empty)
	files, _ := filepath.Glob(filepath.Join(messagesDir, "*", "*.parquet"))
	if len(files) == 0 {
		return true, "cache directory empty"
	}

	return false, ""
}

func init() {
	rootCmd.AddCommand(tuiCmd)
	tuiCmd.Flags().BoolVar(&forceSQL, "force-sql", false, "Force SQLite queries instead of Parquet (slow for large archives)")
	tuiCmd.Flags().BoolVar(&skipCacheBuild, "no-cache-build", false, "Skip automatic cache build/update")
	tuiCmd.Flags().BoolVar(&noSQLiteScanner, "no-sqlite-scanner", false, "Disable DuckDB sqlite_scanner extension (use direct SQLite fallback)")
	_ = tuiCmd.Flags().MarkHidden("no-sqlite-scanner")
}
