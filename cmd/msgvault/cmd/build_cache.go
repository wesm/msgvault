package cmd

import (
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	_ "github.com/marcboeker/go-duckdb"
	_ "github.com/mattn/go-sqlite3"
	"github.com/spf13/cobra"
	"github.com/wesm/msgvault/internal/config"
	"github.com/wesm/msgvault/internal/query"
)

var fullRebuild bool

// buildCacheMu serializes concurrent buildCache calls. The scheduler may
// trigger syncs for multiple accounts in parallel, each of which calls
// buildCache on completion. Without this lock, concurrent writes to shared
// files (_last_sync.json, parquet directories) can corrupt the cache.
var buildCacheMu sync.Mutex

// syncState tracks the last exported message ID for incremental updates.
type syncState struct {
	LastMessageID int64     `json:"last_message_id"`
	LastSyncAt    time.Time `json:"last_sync_at"`
}

var buildCacheCmd = &cobra.Command{
	Use:     "build-cache",
	Aliases: []string{"build-parquet"}, // Backward compatibility
	Short:   "Build analytics cache for fast TUI queries",
	Long: `Build analytics cache from the SQLite database.

This command exports normalized tables to Parquet files for fast aggregate queries.
DuckDB joins the Parquet files at query time, which is much faster than joining
during export (especially for incremental updates).

The cache files are stored in ~/.msgvault/analytics/:
  - messages/year=*/     Core message data, partitioned by year
  - participants/        Email addresses and domains
  - message_recipients/  Links messages to participants (from/to/cc/bcc)
  - labels/              Label definitions
  - message_labels/      Links messages to labels
  - attachments/         Attachment metadata

By default, this performs an incremental update (only adding new messages).
Use --full-rebuild to recreate all cache files from scratch.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		dbPath := cfg.DatabaseDSN()
		analyticsDir := cfg.AnalyticsDir()

		// Check database exists
		if _, err := os.Stat(dbPath); os.IsNotExist(err) {
			return fmt.Errorf("database not found: %s\nRun 'msgvault init-db' first", dbPath)
		}

		result, err := buildCache(dbPath, analyticsDir, fullRebuild)
		if err != nil {
			return err
		}

		if result.Skipped {
			fmt.Println("No new messages to export.")
		} else {
			fmt.Printf("Exported %d messages to %s\n", result.ExportedCount, result.OutputDir)
		}
		fmt.Println("\nCache build complete! The TUI will now use fast cached queries.")
		return nil
	},
}

type buildResult struct {
	ExportedCount int64
	MaxMessageID  int64
	OutputDir     string
	Skipped       bool
}

func buildCache(dbPath, analyticsDir string, fullRebuild bool) (*buildResult, error) {
	buildCacheMu.Lock()
	defer buildCacheMu.Unlock()

	stateFile := filepath.Join(analyticsDir, "_last_sync.json")

	// Create output directory
	if err := os.MkdirAll(analyticsDir, 0755); err != nil {
		return nil, fmt.Errorf("create analytics dir: %w", err)
	}

	// Load sync state for incremental updates
	var lastMessageID int64
	if !fullRebuild {
		if data, err := os.ReadFile(stateFile); err == nil {
			var state syncState
			if json.Unmarshal(data, &state) == nil {
				lastMessageID = state.LastMessageID
				fmt.Printf("Incremental export from message_id > %d\n", lastMessageID)
			}
		}
	}

	// Use direct SQLite to check for new messages (fast, uses indexes)
	// DuckDB's sqlite extension doesn't use SQLite indexes, so this query
	// would scan the entire table if we used DuckDB.
	sqliteDB, err := sql.Open("sqlite3", dbPath+"?mode=ro")
	if err != nil {
		return nil, fmt.Errorf("open sqlite for max id check: %w", err)
	}

	var maxMessageID sql.NullInt64
	// Use indexed query: id is PRIMARY KEY, sent_at has an index
	maxIDQuery := `SELECT MAX(id) FROM messages WHERE sent_at IS NOT NULL`
	if err := sqliteDB.QueryRow(maxIDQuery).Scan(&maxMessageID); err != nil {
		sqliteDB.Close()
		return nil, fmt.Errorf("get max message id: %w", err)
	}
	sqliteDB.Close()

	maxID := int64(0)
	if maxMessageID.Valid {
		maxID = maxMessageID.Int64
	}

	// Check for missing required parquet tables independently of whether
	// new messages exist. A legacy cache might be missing tables (e.g.
	// conversations) regardless of message count. Force full rebuild to
	// avoid stale incr_*.parquet shards and ensure all tables are populated.
	// Gate on maxID > 0: when the DB has zero messages, missing messages
	// parquet is legitimate, not a sign of a broken cache.
	if !fullRebuild && maxID > 0 && missingRequiredParquet(analyticsDir) {
		fmt.Println("Backfilling missing cache tables (full rebuild)...")
		fullRebuild = true
		lastMessageID = 0
	}

	if maxID <= lastMessageID && !fullRebuild {
		return &buildResult{Skipped: true}, nil
	}

	// Open DuckDB for the actual export
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("open duckdb: %w", err)
	}
	defer db.Close()

	// Set up sqlite_db tables — either via DuckDB's sqlite extension (Linux/macOS)
	// or via CSV intermediate files (Windows, where sqlite_scanner is unavailable).
	cleanup, err := setupSQLiteSource(db, dbPath)
	if err != nil {
		return nil, err
	}
	defer cleanup()

	// On full rebuild, clear existing cache
	if fullRebuild {
		fmt.Println("Full rebuild: clearing existing cache...")
		for _, subdir := range query.RequiredParquetDirs {
			if err := os.RemoveAll(filepath.Join(analyticsDir, subdir)); err != nil {
				return nil, fmt.Errorf("clear existing cache: %w", err)
			}
		}
	}

	// Create subdirectories
	for _, subdir := range query.RequiredParquetDirs {
		if err := os.MkdirAll(filepath.Join(analyticsDir, subdir), 0755); err != nil {
			return nil, fmt.Errorf("create %s dir: %w", subdir, err)
		}
	}

	fmt.Println("Building cache...")
	buildStart := time.Now()

	// Build WHERE clause for incremental exports
	idFilter := ""
	if !fullRebuild && lastMessageID > 0 {
		idFilter = fmt.Sprintf(" AND m.id > %d", lastMessageID)
	}

	// Junction tables (message_recipients, message_labels, attachments) need
	// unique filenames per batch because Parquet files cannot be appended to —
	// DuckDB's COPY with APPEND silently overwrites a single file.
	// Using *.parquet glob in queries reads all batch files together.
	junctionFile := "data.parquet"
	if !fullRebuild && lastMessageID > 0 {
		junctionFile = fmt.Sprintf("incr_%d.parquet", lastMessageID)
	}

	// runExport executes a COPY query and prints timing info.
	runExport := func(label, query string) error {
		start := time.Now()
		fmt.Printf("  %-25s", label+"...")
		if _, err := db.Exec(query); err != nil {
			fmt.Println()
			return err
		}
		fmt.Printf(" done (%s)\n", time.Since(start).Round(time.Millisecond))
		return nil
	}

	// Export each table separately - this is MUCH faster than joining during export
	// because DuckDB can use SQLite indexes efficiently for simple queries

	// 1. Export messages (partitioned by year)
	messagesDir := filepath.Join(analyticsDir, "messages")
	escapedMessagesDir := strings.ReplaceAll(messagesDir, "'", "''")

	writeMode := "OVERWRITE_OR_IGNORE"
	if !fullRebuild && lastMessageID > 0 {
		writeMode = "APPEND"
	}

	if err := runExport("messages", fmt.Sprintf(`
	COPY (
		SELECT
			m.id,
			m.source_id,
			m.source_message_id,
			m.conversation_id,
			CASE WHEN m.subject IS NULL THEN NULL ELSE COALESCE(TRY_CAST(m.subject AS VARCHAR), '') END as subject,
			CASE WHEN m.snippet IS NULL THEN NULL ELSE COALESCE(TRY_CAST(m.snippet AS VARCHAR), '') END as snippet,
			m.sent_at,
			m.size_estimate,
			m.has_attachments,
			m.deleted_from_source_at,
			CAST(EXTRACT(YEAR FROM m.sent_at) AS INTEGER) as year,
			CAST(EXTRACT(MONTH FROM m.sent_at) AS INTEGER) as month
		FROM sqlite_db.messages m
		WHERE m.sent_at IS NOT NULL%s
	) TO '%s' (
		FORMAT PARQUET,
		PARTITION_BY (year),
		%s,
		COMPRESSION 'zstd'
	)
	`, idFilter, escapedMessagesDir, writeMode)); err != nil {
		return nil, fmt.Errorf("export messages: %w", err)
	}

	// 2. Export message_recipients (large junction table)
	recipientsDir := filepath.Join(analyticsDir, "message_recipients")
	escapedRecipientsDir := strings.ReplaceAll(recipientsDir, "'", "''")
	recipientsFilter := ""
	if !fullRebuild && lastMessageID > 0 {
		recipientsFilter = fmt.Sprintf(" WHERE message_id > %d", lastMessageID)
	}
	if err := runExport("message_recipients", fmt.Sprintf(`
	COPY (
		SELECT
			message_id,
			participant_id,
			recipient_type,
			COALESCE(TRY_CAST(display_name AS VARCHAR), '') as display_name
		FROM sqlite_db.message_recipients%s
	) TO '%s/%s' (
		FORMAT PARQUET,
		COMPRESSION 'zstd'
	)
	`, recipientsFilter, escapedRecipientsDir, junctionFile)); err != nil {
		return nil, fmt.Errorf("export message_recipients: %w", err)
	}

	// 3. Export message_labels (large junction table)
	messageLabelsDir := filepath.Join(analyticsDir, "message_labels")
	escapedMessageLabelsDir := strings.ReplaceAll(messageLabelsDir, "'", "''")
	messageLabelsFilter := ""
	if !fullRebuild && lastMessageID > 0 {
		messageLabelsFilter = fmt.Sprintf(" WHERE message_id > %d", lastMessageID)
	}
	if err := runExport("message_labels", fmt.Sprintf(`
	COPY (
		SELECT
			message_id,
			label_id
		FROM sqlite_db.message_labels%s
	) TO '%s/%s' (
		FORMAT PARQUET,
		COMPRESSION 'zstd'
	)
	`, messageLabelsFilter, escapedMessageLabelsDir, junctionFile)); err != nil {
		return nil, fmt.Errorf("export message_labels: %w", err)
	}

	// 4. Export attachments
	attachmentsDir := filepath.Join(analyticsDir, "attachments")
	escapedAttachmentsDir := strings.ReplaceAll(attachmentsDir, "'", "''")
	attachmentsFilter := ""
	if !fullRebuild && lastMessageID > 0 {
		attachmentsFilter = fmt.Sprintf(" WHERE message_id > %d", lastMessageID)
	}
	if err := runExport("attachments", fmt.Sprintf(`
	COPY (
		SELECT
			message_id,
			size,
			COALESCE(TRY_CAST(filename AS VARCHAR), '') as filename
		FROM sqlite_db.attachments%s
	) TO '%s/%s' (
		FORMAT PARQUET,
		COMPRESSION 'zstd'
	)
	`, attachmentsFilter, escapedAttachmentsDir, junctionFile)); err != nil {
		return nil, fmt.Errorf("export attachments: %w", err)
	}

	// 5. Export participants
	participantsDir := filepath.Join(analyticsDir, "participants")
	escapedParticipantsDir := strings.ReplaceAll(participantsDir, "'", "''")
	if err := runExport("participants", fmt.Sprintf(`
	COPY (
		SELECT
			id,
			COALESCE(TRY_CAST(email_address AS VARCHAR), '') as email_address,
			COALESCE(TRY_CAST(domain AS VARCHAR), '') as domain,
			COALESCE(TRY_CAST(display_name AS VARCHAR), '') as display_name
		FROM sqlite_db.participants
	) TO '%s/participants.parquet' (
		FORMAT PARQUET,
		COMPRESSION 'zstd'
	)
	`, escapedParticipantsDir)); err != nil {
		return nil, fmt.Errorf("export participants: %w", err)
	}

	// 6. Export labels
	labelsDir := filepath.Join(analyticsDir, "labels")
	escapedLabelsDir := strings.ReplaceAll(labelsDir, "'", "''")
	if err := runExport("labels", fmt.Sprintf(`
	COPY (
		SELECT
			id,
			COALESCE(TRY_CAST(name AS VARCHAR), '') as name
		FROM sqlite_db.labels
	) TO '%s/labels.parquet' (
		FORMAT PARQUET,
		COMPRESSION 'zstd'
	)
	`, escapedLabelsDir)); err != nil {
		return nil, fmt.Errorf("export labels: %w", err)
	}

	// 7. Export sources
	sourcesDir := filepath.Join(analyticsDir, "sources")
	escapedSourcesDir := strings.ReplaceAll(sourcesDir, "'", "''")
	if err := runExport("sources", fmt.Sprintf(`
	COPY (
		SELECT
			id,
			identifier as account_email
		FROM sqlite_db.sources
	) TO '%s/sources.parquet' (
		FORMAT PARQUET,
		COMPRESSION 'zstd'
	)
	`, escapedSourcesDir)); err != nil {
		return nil, fmt.Errorf("export sources: %w", err)
	}

	// 8. Export conversations (for Gmail thread IDs)
	conversationsDir := filepath.Join(analyticsDir, "conversations")
	escapedConversationsDir := strings.ReplaceAll(conversationsDir, "'", "''")
	if err := runExport("conversations", fmt.Sprintf(`
	COPY (
		SELECT
			id,
			COALESCE(TRY_CAST(source_conversation_id AS VARCHAR), '') as source_conversation_id
		FROM sqlite_db.conversations
	) TO '%s/conversations.parquet' (
		FORMAT PARQUET,
		COMPRESSION 'zstd'
	)
	`, escapedConversationsDir)); err != nil {
		return nil, fmt.Errorf("export conversations: %w", err)
	}

	fmt.Printf("  %-25s %s\n", "Total:", time.Since(buildStart).Round(time.Millisecond))

	// Count exported messages
	var exportedCount int64
	countSQL := fmt.Sprintf("SELECT COUNT(*) FROM read_parquet('%s/**/*.parquet', hive_partitioning=true)", escapedMessagesDir)
	if err := db.QueryRow(countSQL).Scan(&exportedCount); err != nil {
		exportedCount = 0
	}

	// Save sync state
	state := syncState{
		LastMessageID: maxID,
		LastSyncAt:    time.Now(),
	}
	stateData, _ := json.Marshal(state)
	if err := os.WriteFile(stateFile, stateData, 0644); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: failed to save sync state: %v\n", err)
	}

	return &buildResult{
		ExportedCount: exportedCount,
		MaxMessageID:  maxID,
		OutputDir:     analyticsDir,
	}, nil
}

// missingRequiredParquet returns true if some parquet data exists but is
// missing one or more required tables (e.g. upgrading from a cache that
// predates the conversations export). Returns false for a fresh empty cache.
func missingRequiredParquet(analyticsDir string) bool {
	if query.HasCompleteParquetData(analyticsDir) {
		return false
	}
	// Incomplete — check if any table has data (partial/broken cache vs fresh).
	for _, dir := range query.RequiredParquetDirs {
		pattern := filepath.Join(analyticsDir, dir, "*.parquet")
		if matches, _ := filepath.Glob(pattern); len(matches) > 0 {
			return true
		}
		// For messages, also check hive-partitioned layout (messages/year=*/*.parquet)
		if dir == "messages" {
			if deep, _ := filepath.Glob(filepath.Join(analyticsDir, dir, "*", "*.parquet")); len(deep) > 0 {
				return true
			}
		}
	}
	return false
}

var cacheStatsCmd = &cobra.Command{
	Use:     "cache-stats",
	Aliases: []string{"parquet-stats"}, // Backward compatibility
	Short:   "Show statistics about the analytics cache",
	Long:    `Display statistics about the analytics cache, including row counts and file sizes.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		analyticsDir := cfg.AnalyticsDir()
		messagesDir := filepath.Join(analyticsDir, "messages")

		// Check if directory exists and contains parquet files
		if _, err := os.Stat(messagesDir); os.IsNotExist(err) {
			fmt.Println("No cache files found.")
			fmt.Printf("Run 'msgvault build-cache' to create them.\n")
			return nil
		}

		// Check for actual parquet files (directory might exist but be empty)
		parquetFiles, err := filepath.Glob(filepath.Join(messagesDir, "**", "*.parquet"))
		if err != nil {
			return fmt.Errorf("check for cache files: %w", err)
		}
		// Also check one level deep (year=*/data_0.parquet pattern)
		if len(parquetFiles) == 0 {
			parquetFiles, _ = filepath.Glob(filepath.Join(messagesDir, "*", "*.parquet"))
		}
		if len(parquetFiles) == 0 {
			fmt.Println("No cache data found (directory exists but contains no data).")
			fmt.Printf("Run 'msgvault build-cache' to populate it.\n")
			return nil
		}

		// Open DuckDB
		db, err := sql.Open("duckdb", "")
		if err != nil {
			return fmt.Errorf("open duckdb: %w", err)
		}
		defer db.Close()

		// Query stats by joining Parquet files
		escapedDir := strings.ReplaceAll(analyticsDir, "'", "''")
		statsSQL := fmt.Sprintf(`
		WITH msg AS (
			SELECT * FROM read_parquet('%s/messages/**/*.parquet', hive_partitioning=true)
		),
		mr AS (
			SELECT * FROM read_parquet('%s/message_recipients/*.parquet')
		),
		p AS (
			SELECT * FROM read_parquet('%s/participants/*.parquet')
		)
		SELECT
			COUNT(*) as total_messages,
			COUNT(DISTINCT m.source_id) as sources,
			(SELECT COUNT(DISTINCT p2.email_address)
			 FROM mr mr2
			 JOIN p p2 ON p2.id = mr2.participant_id
			 WHERE mr2.recipient_type = 'from') as unique_senders,
			(SELECT COUNT(DISTINCT p2.domain)
			 FROM mr mr2
			 JOIN p p2 ON p2.id = mr2.participant_id
			 WHERE mr2.recipient_type = 'from') as unique_domains,
			MIN(m.year) as min_year,
			MAX(m.year) as max_year,
			COALESCE(SUM(m.size_estimate), 0) as total_size
		FROM msg m
		`, escapedDir, escapedDir, escapedDir)

		var totalMessages, sources, uniqueSenders, uniqueDomains int64
		var minYear, maxYear sql.NullInt64
		var totalSize int64

		err = db.QueryRow(statsSQL).Scan(
			&totalMessages,
			&sources,
			&uniqueSenders,
			&uniqueDomains,
			&minYear,
			&maxYear,
			&totalSize,
		)
		if err != nil {
			return fmt.Errorf("query stats: %w", err)
		}

		// Get attachment stats separately
		attachmentsDir := filepath.Join(analyticsDir, "attachments")
		var attachmentSize int64
		if _, err := os.Stat(attachmentsDir); err == nil {
			attachSQL := fmt.Sprintf(`
			SELECT COALESCE(SUM(size), 0) FROM read_parquet('%s/attachments/*.parquet')
			`, escapedDir)
			_ = db.QueryRow(attachSQL).Scan(&attachmentSize)
		}

		fmt.Println("Cache Statistics:")
		fmt.Printf("  Total messages:    %d\n", totalMessages)
		fmt.Printf("  Accounts:          %d\n", sources)
		fmt.Printf("  Unique senders:    %d\n", uniqueSenders)
		fmt.Printf("  Unique domains:    %d\n", uniqueDomains)
		if minYear.Valid && maxYear.Valid {
			fmt.Printf("  Year range:        %d-%d\n", minYear.Int64, maxYear.Int64)
		}
		fmt.Printf("  Total size:        %.1f MB\n", float64(totalSize)/1024/1024)
		fmt.Printf("  Attachment size:   %.1f MB\n", float64(attachmentSize)/1024/1024)

		// Show sync state
		stateFile := filepath.Join(analyticsDir, "_last_sync.json")
		if data, err := os.ReadFile(stateFile); err == nil {
			var state syncState
			if json.Unmarshal(data, &state) == nil {
				fmt.Printf("  Last sync:         %s\n", state.LastSyncAt.Format("2006-01-02 15:04:05"))
				fmt.Printf("  Last message ID:   %d\n", state.LastMessageID)
			}
		}

		return nil
	},
}

// setupSQLiteSource makes SQLite tables available to DuckDB as sqlite_db.*.
// On Linux/macOS it uses DuckDB's sqlite extension (ATTACH).
// On Windows it exports tables to CSV and creates DuckDB views, since the
// sqlite_scanner extension is not available for MinGW builds.
func setupSQLiteSource(duckDB *sql.DB, dbPath string) (cleanup func(), err error) {
	if runtime.GOOS != "windows" {
		// Try sqlite_scanner extension; fall back to CSV if unavailable
		// (e.g. air-gapped environment with no internet for extension download).
		if _, err := duckDB.Exec("INSTALL sqlite; LOAD sqlite;"); err != nil {
			fmt.Fprintf(os.Stderr, "  sqlite_scanner unavailable, using CSV fallback: %v\n", err)
		} else {
			escapedPath := strings.ReplaceAll(dbPath, "'", "''")
			if _, err := duckDB.Exec(fmt.Sprintf("ATTACH '%s' AS sqlite_db (TYPE sqlite, READ_ONLY)", escapedPath)); err != nil {
				fmt.Fprintf(os.Stderr, "  sqlite attach failed, using CSV fallback: %v\n", err)
			} else {
				return func() {}, nil
			}
		}
	}

	// CSV fallback: export SQLite tables to CSV, create DuckDB views.
	// Prefer the database's parent directory for temp files (avoids
	// cross-device moves), but fall back through system temp and
	// ~/.msgvault/tmp/ for read-only or restricted environments.
	tmpDir, err := config.MkTempDir(".cache-tmp-*", filepath.Dir(dbPath))
	if err != nil {
		return nil, err
	}

	sqliteDB, err := sql.Open("sqlite3", dbPath+"?mode=ro")
	if err != nil {
		os.RemoveAll(tmpDir)
		return nil, fmt.Errorf("open sqlite for CSV export: %w", err)
	}

	// Tables and the SELECT queries to export them.
	// Column lists match what the COPY-to-Parquet queries expect.
	tables := []struct {
		name          string
		query         string
		typeOverrides string // DuckDB types parameter for read_csv_auto (empty = infer all)
	}{
		{"messages", "SELECT id, source_id, source_message_id, conversation_id, subject, snippet, sent_at, size_estimate, has_attachments, deleted_from_source_at FROM messages WHERE sent_at IS NOT NULL",
			"types={'sent_at': 'TIMESTAMP', 'deleted_from_source_at': 'TIMESTAMP'}"},
		{"message_recipients", "SELECT message_id, participant_id, recipient_type, display_name FROM message_recipients", ""},
		{"message_labels", "SELECT message_id, label_id FROM message_labels", ""},
		{"attachments", "SELECT message_id, size, filename FROM attachments", ""},
		{"participants", "SELECT id, email_address, domain, display_name FROM participants", ""},
		{"labels", "SELECT id, name FROM labels", ""},
		{"sources", "SELECT id, identifier FROM sources", ""},
		{"conversations", "SELECT id, source_conversation_id FROM conversations", ""},
	}

	for _, t := range tables {
		csvPath := filepath.Join(tmpDir, t.name+".csv")
		if err := exportToCSV(sqliteDB, t.query, csvPath); err != nil {
			sqliteDB.Close()
			os.RemoveAll(tmpDir)
			return nil, fmt.Errorf("export %s to CSV: %w", t.name, err)
		}
	}
	sqliteDB.Close()

	// Create sqlite_db schema with views pointing to CSV files.
	// This lets the existing COPY queries reference sqlite_db.tablename unchanged.
	if _, err := duckDB.Exec("CREATE SCHEMA sqlite_db"); err != nil {
		os.RemoveAll(tmpDir)
		return nil, fmt.Errorf("create sqlite_db schema: %w", err)
	}
	for _, t := range tables {
		csvPath := filepath.Join(tmpDir, t.name+".csv")
		// DuckDB handles both forward and backslash paths, but normalize to forward.
		escaped := strings.ReplaceAll(csvPath, "\\", "/")
		escaped = strings.ReplaceAll(escaped, "'", "''")
		csvOpts := "header=true, nullstr='\\N'"
		if t.typeOverrides != "" {
			csvOpts += ", " + t.typeOverrides
		}
		viewSQL := fmt.Sprintf(
			`CREATE VIEW sqlite_db."%s" AS SELECT * FROM read_csv_auto('%s', %s)`,
			t.name, escaped, csvOpts,
		)
		if _, err := duckDB.Exec(viewSQL); err != nil {
			os.RemoveAll(tmpDir)
			return nil, fmt.Errorf("create view sqlite_db.%s: %w", t.name, err)
		}
	}

	return func() { os.RemoveAll(tmpDir) }, nil
}

// csvNullStr is written for NULL values in CSV exports so DuckDB can
// distinguish NULL from empty string via the nullstr option.
const csvNullStr = `\N`

// exportToCSV exports the results of a SQL query to a CSV file.
// NULL values are written as \N (PostgreSQL convention).
func exportToCSV(db *sql.DB, query string, dest string) error {
	rows, err := db.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	f, err := os.Create(dest)
	if err != nil {
		return err
	}
	defer f.Close()

	w := csv.NewWriter(f)

	cols, err := rows.Columns()
	if err != nil {
		return err
	}
	if err := w.Write(cols); err != nil {
		return err
	}

	values := make([]sql.NullString, len(cols))
	ptrs := make([]interface{}, len(cols))
	for i := range values {
		ptrs[i] = &values[i]
	}

	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			return err
		}
		record := make([]string, len(cols))
		for i, v := range values {
			if v.Valid {
				record[i] = v.String
			} else {
				record[i] = csvNullStr
			}
		}
		if err := w.Write(record); err != nil {
			return err
		}
	}

	w.Flush()
	if err := w.Error(); err != nil {
		return err
	}
	return rows.Err()
}

func init() {
	rootCmd.AddCommand(buildCacheCmd)
	rootCmd.AddCommand(cacheStatsCmd)
	buildCacheCmd.Flags().BoolVar(&fullRebuild, "full-rebuild", false, "Rebuild all cache files from scratch")
}
