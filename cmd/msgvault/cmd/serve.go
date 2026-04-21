package cmd

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/wesm/msgvault/internal/api"
	"github.com/wesm/msgvault/internal/gmail"
	"github.com/wesm/msgvault/internal/importer"
	"github.com/wesm/msgvault/internal/oauth"
	"github.com/wesm/msgvault/internal/query"
	"github.com/wesm/msgvault/internal/scheduler"
	"github.com/wesm/msgvault/internal/search"
	"github.com/wesm/msgvault/internal/store"
	"github.com/wesm/msgvault/internal/sync"
)

var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "Run msgvault as a daemon with scheduled sync",
	Long: `Run msgvault as a long-running daemon that syncs email accounts on schedule.

The daemon runs in the foreground and performs:
  - HTTP API server on configured port (default: 8080)
  - Scheduled incremental syncs based on account config
  - Automatic cache rebuilds after each sync

Configure schedules in config.toml:
  [[accounts]]
  email = "you@gmail.com"
  schedule = "0 2 * * *"   # 2am daily (cron format)
  enabled = true

Cron format: minute hour day-of-month month day-of-week
  Examples:
    0 2 * * *     = 2:00 AM daily
    */15 * * * *  = Every 15 minutes
    0 0 * * 0     = Midnight on Sundays
    0 8,18 * * *  = 8 AM and 6 PM daily

Use Ctrl+C to stop the daemon gracefully.`,
	RunE: runServe,
}

func init() {
	rootCmd.AddCommand(serveCmd)
}

func runServe(cmd *cobra.Command, args []string) error {
	// Validate security posture before doing any work
	if err := cfg.Server.ValidateSecure(); err != nil {
		return err
	}
	if cfg.Server.APIKey != "" && len(cfg.Server.APIKey) < 16 {
		logger.Warn("api_key is very short — use a randomly generated key of at least 32 characters")
	}

	// Check for scheduled Gmail accounts and imports
	hasGmailAccounts := len(cfg.ScheduledAccounts()) > 0
	hasImports := len(cfg.ScheduledImports()) > 0

	// OAuth is required only when Gmail sync accounts are configured
	if hasGmailAccounts && !cfg.OAuth.HasAnyConfig() {
		return errOAuthNotConfigured()
	}

	if !hasGmailAccounts && !hasImports {
		logger.Warn("no scheduled accounts or imports configured - server will start but no syncs will run",
			"hint", "Add accounts or imports to config.toml")
	}

	// Open database
	dbPath := cfg.DatabaseDSN()
	s, err := store.Open(dbPath)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer func() { _ = s.Close() }()

	if err := s.InitSchema(); err != nil {
		return fmt.Errorf("init schema: %w", err)
	}

	// Set up cancellable context early so vector-backend initialization
	// (which may open files and run migrations) respects Ctrl+C.
	ctx, cancel := context.WithCancel(cmd.Context())
	defer cancel()

	// Build optional vector-search components. Returns (nil, nil) when
	// cfg.Vector.Enabled is false, or an error when enabled but the
	// binary was built without -tags sqlite_vec.
	vf, err := setupVectorFeatures(ctx, s.DB(), dbPath)
	if err != nil {
		return fmt.Errorf("vector features: %w", err)
	}
	defer func() {
		if vf != nil && vf.Close != nil {
			if closeErr := vf.Close(); closeErr != nil {
				logger.Warn("closing vectors.db failed", "error", closeErr)
			}
		}
	}()

	// Create query engine for TUI aggregate support.
	// Prefer DuckDB over Parquet when the cache is complete and fresh;
	// otherwise fall back to SQLite so remote endpoints still work.
	analyticsDir := cfg.AnalyticsDir()
	var engine query.Engine
	staleness := cacheNeedsBuild(dbPath, analyticsDir)
	if !staleness.NeedsBuild && query.HasCompleteParquetData(analyticsDir) {
		duckEngine, engineErr := query.NewDuckDBEngine(
			analyticsDir, dbPath, s.DB(),
		)
		if engineErr != nil {
			logger.Warn("DuckDB engine failed, falling back to SQLite",
				"error", engineErr)
			engine = query.NewSQLiteEngine(s.DB())
		} else {
			engine = duckEngine
		}
	} else {
		if staleness.Reason != "" {
			logger.Info("parquet cache not usable, using SQLite engine",
				"reason", staleness.Reason)
		} else {
			logger.Info("parquet cache not built - using SQLite engine (run 'msgvault build-cache' for faster aggregates)")
		}
		engine = query.NewSQLiteEngine(s.DB())
	}
	defer func() { _ = engine.Close() }()

	getOAuthMgr := oauthManagerCache()

	// Build import lookup table for dispatch
	importConfigs := make(map[string]struct{ path, identifier, attachmentsDir string })
	for _, imp := range cfg.ScheduledImports() {
		key := "import:" + imp.Identifier
		importConfigs[key] = struct{ path, identifier, attachmentsDir string }{
			path: imp.Path, identifier: imp.Identifier, attachmentsDir: cfg.AttachmentsDir(),
		}
	}

	// Create dispatch function: Gmail syncs vs local imports.
	// vf is captured and used inside runScheduledSync to wire the embed
	// enqueuer into each per-run Syncer; it is nil when vector search is disabled.
	dispatchFunc := func(ctx context.Context, key string) error {
		if ic, ok := importConfigs[key]; ok {
			return runScheduledImport(ctx, s, ic.path, ic.identifier, ic.attachmentsDir)
		}
		return runScheduledSync(ctx, key, s, getOAuthMgr, vf)
	}

	// Create and configure scheduler
	sched := scheduler.New(dispatchFunc).WithLogger(logger)

	// Add all scheduled Gmail sync accounts
	count, errs := sched.AddAccountsFromConfig(cfg)
	if len(errs) > 0 {
		for _, err := range errs {
			logger.Error("failed to schedule account", "error", err)
		}
	}

	// Add scheduled imports (emlx, etc.)
	importCount := 0
	for _, imp := range cfg.ScheduledImports() {
		key := "import:" + imp.Identifier
		if err := sched.AddAccount(key, imp.Schedule); err != nil {
			logger.Error("failed to schedule import", "identifier", imp.Identifier, "error", err)
		} else {
			importCount++
			logger.Info("scheduled import", "type", imp.Type, "identifier", imp.Identifier, "path", imp.Path)
		}
	}

	if count == 0 && importCount == 0 {
		logger.Warn("no accounts or imports scheduled - add entries to config.toml")
	}

	// Register the embed job (cron-driven plus optional post-sync hook).
	// Only when vector search is enabled and wired.
	if vf != nil {
		embedJob := &scheduler.EmbedJob{
			Worker:      vf.Worker,
			Backend:     vf.Backend,
			VectorsDB:   vf.VectorsDB,
			Fingerprint: vf.Cfg.Embeddings.Fingerprint(),
			Log:         logger,
		}
		schedule := cfg.Vector.Embed.Schedule.Cron
		if err := sched.SetEmbedJob(
			embedJob, schedule, cfg.Vector.Embed.Schedule.RunAfterSync,
		); err != nil {
			return fmt.Errorf("register embed job: %w", err)
		}
		logger.Info("embed scheduled",
			"cron", schedule,
			"run_after_sync", cfg.Vector.Embed.Schedule.RunAfterSync,
		)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start the scheduler
	sched.Start()

	// Create adapters for the API interfaces
	storeAdapter := &storeAPIAdapter{store: s}
	schedAdapter := &schedulerAdapter{scheduler: sched}

	// Create and start API server
	apiOpts := api.ServerOptions{
		Config:    cfg,
		Store:     storeAdapter,
		Engine:    engine,
		Scheduler: schedAdapter,
		Logger:    logger,
	}
	if vf != nil {
		apiOpts.HybridEngine = vf.HybridEngine
		apiOpts.Backend = vf.Backend
		apiOpts.VectorCfg = vf.Cfg
	}
	apiServer := api.NewServerWithOptions(apiOpts)

	// Start API server in goroutine
	serverErr := make(chan error, 1)
	go func() {
		if err := apiServer.Start(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()

	bindAddr := cfg.Server.BindAddr
	if bindAddr == "" {
		bindAddr = "127.0.0.1"
	}
	serverAddr := net.JoinHostPort(bindAddr, strconv.Itoa(cfg.Server.APIPort))
	fmt.Printf("msgvault daemon started\n")
	fmt.Printf("  Web UI:  http://%s/\n", serverAddr)
	fmt.Printf("  API:     http://%s/api/v1\n", serverAddr)
	fmt.Printf("  Scheduled syncs:    %d\n", count)
	fmt.Printf("  Scheduled imports:  %d\n", importCount)
	fmt.Printf("  Data directory: %s\n", cfg.Data.DataDir)
	fmt.Println()
	fmt.Println("Press Ctrl+C to stop.")
	fmt.Println()

	// Print schedule info
	for _, status := range sched.Status() {
		fmt.Printf("  %s: next sync at %s\n", status.Email, status.NextRun.Local().Format("2006-01-02 15:04:05"))
	}
	fmt.Println()

	// Wait for shutdown signal or server error
	select {
	case sig := <-sigChan:
		logger.Info("received shutdown signal", "signal", sig)
		fmt.Printf("\nReceived %s, shutting down...\n", sig)
	case err := <-serverErr:
		logger.Error("API server error", "error", err)
		fmt.Printf("\nAPI server error: %v\n", err)
	case <-ctx.Done():
		logger.Info("context cancelled")
	}

	// Graceful shutdown
	fmt.Println("Shutting down API server...")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := apiServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("API server shutdown error", "error", err)
	}

	fmt.Println("Waiting for running syncs to complete...")
	schedCtx := sched.Stop()

	// Wait for scheduler to stop (with timeout)
	select {
	case <-schedCtx.Done():
		fmt.Println("Shutdown complete.")
	case <-time.After(30 * time.Second):
		fmt.Println("Shutdown timed out after 30 seconds.")
	}

	return nil
}

// storeAPIAdapter adapts store.Store to api.MessageStore.
// Since api.APIMessage, api.StoreStats, etc. are type aliases for store types,
// the adapter methods are simple pass-throughs with no conversion needed.
type storeAPIAdapter struct {
	store *store.Store
}

func (a *storeAPIAdapter) GetStats() (*api.StoreStats, error) {
	return a.store.GetStats()
}

func (a *storeAPIAdapter) ListMessages(offset, limit int) ([]api.APIMessage, int64, error) {
	return a.store.ListMessages(offset, limit)
}

func (a *storeAPIAdapter) GetMessage(id int64) (*api.APIMessage, error) {
	return a.store.GetMessage(id)
}

func (a *storeAPIAdapter) GetMessagesSummariesByIDs(ids []int64) ([]api.APIMessage, error) {
	return a.store.GetMessagesSummariesByIDs(ids)
}

func (a *storeAPIAdapter) SearchMessages(query string, offset, limit int) ([]api.APIMessage, int64, error) {
	return a.store.SearchMessages(query, offset, limit)
}

func (a *storeAPIAdapter) SearchMessagesQuery(q *search.Query, offset, limit int) ([]api.APIMessage, int64, error) {
	return a.store.SearchMessagesQuery(q, offset, limit)
}

// schedulerAdapter adapts scheduler.Scheduler to api.SyncScheduler.
// Since api.AccountStatus is a type alias for scheduler.AccountStatus,
// the adapter methods are simple pass-throughs.
type schedulerAdapter struct {
	scheduler *scheduler.Scheduler
}

func (a *schedulerAdapter) IsScheduled(email string) bool {
	return a.scheduler.IsScheduled(email)
}

func (a *schedulerAdapter) TriggerSync(email string) error {
	return a.scheduler.TriggerSync(email)
}

func (a *schedulerAdapter) AddAccount(email, schedule string) error {
	return a.scheduler.AddAccount(email, schedule)
}

func (a *schedulerAdapter) IsRunning() bool {
	return a.scheduler.IsRunning()
}

func (a *schedulerAdapter) Status() []api.AccountStatus {
	return a.scheduler.Status()
}

// runScheduledImport performs an emlx import for a scheduled import source.
func runScheduledImport(ctx context.Context, s *store.Store, mailDir, identifier, attachmentsDir string) error {
	logger.Info("starting scheduled import", "identifier", identifier, "path", mailDir)
	startTime := time.Now()

	summary, err := importer.ImportEmlxDir(ctx, s, mailDir, importer.EmlxImportOptions{
		SourceType:         "apple-mail",
		Identifier:         identifier,
		CheckpointInterval: 200,
		AttachmentsDir:     attachmentsDir,
		Logger:             logger,
	})
	if err != nil {
		return fmt.Errorf("emlx import failed: %w", err)
	}

	logger.Info("import completed",
		"identifier", identifier,
		"messages_added", summary.MessagesAdded,
		"messages_skipped", summary.MessagesSkipped,
		"duration", time.Since(startTime),
	)

	// Purge trashed messages after import to keep archive clean
	var purged int64
	source, err := s.GetSourceByIdentifier(identifier)
	if err == nil && source != nil {
		purged, err = s.PurgeTrashMessagesForSource(source.ID)
		if err != nil {
			logger.Error("trash purge failed", "error", err)
		} else if purged > 0 {
			logger.Info("purged trashed messages", "identifier", identifier, "count", purged)
		}
	}

	// Rebuild cache after import if data changed.
	// Full rebuild when trash was purged (deleted rows need re-export).
	if summary.MessagesAdded > 0 || purged > 0 {
		fullRebuild := purged > 0
		logger.Info("building cache after import", "identifier", identifier, "full_rebuild", fullRebuild)
		result, err := buildCache(cfg.DatabaseDSN(), cfg.AnalyticsDir(), fullRebuild)
		if err != nil {
			logger.Error("cache build failed", "error", err)
		} else if !result.Skipped {
			logger.Info("cache build completed", "exported", result.ExportedCount)
		}
	}

	if summary.HardErrors {
		return fmt.Errorf("import completed with %d errors", summary.Errors)
	}

	return nil
}

// runScheduledSync performs an incremental sync for a scheduled
// account. When vf is non-nil (vector search enabled), the Syncer is
// configured to enqueue newly-ingested message IDs into the embedding
// pipeline so subsequent embed runs pick them up.
func runScheduledSync(ctx context.Context, email string, s *store.Store, getOAuthMgr func(string) (*oauth.Manager, error), vf *vectorFeatures) error {
	logger.Info("starting scheduled sync", "email", email)
	startTime := time.Now()

	// Look up source to get OAuth app binding. Fall back to default
	// if no source row exists (token-first workflow).
	appName := ""
	src, srcErr := findGmailSource(s, email)
	if srcErr != nil {
		return fmt.Errorf("look up source for %s: %w", email, srcErr)
	}
	if src != nil {
		appName = sourceOAuthApp(src)
	}

	oauthMgr, err := getOAuthMgr(appName)
	if err != nil {
		return fmt.Errorf("resolve OAuth credentials for %s: %w", email, err)
	}

	// Get token source — intentionally not using getTokenSourceWithReauth here
	// because serve runs as a daemon and cannot open a browser for OAuth.
	tokenSource, err := oauthMgr.TokenSource(ctx, email)
	if err != nil {
		if oauthMgr.HasToken(email) {
			return fmt.Errorf("get token source: %w (token may be expired; run 'sync %s' or 'verify %s' from an interactive terminal to re-authorize)", err, email, email)
		}
		return fmt.Errorf("get token source: %w (run 'add-account %s' first)", err, email)
	}

	// Create Gmail client
	rateLimiter := gmail.NewRateLimiter(float64(cfg.Sync.RateLimitQPS))
	client := gmail.NewClient(tokenSource,
		gmail.WithLogger(logger),
		gmail.WithRateLimiter(rateLimiter),
	)
	defer func() { _ = client.Close() }()

	// Set up sync options
	opts := sync.DefaultOptions()
	opts.AttachmentsDir = cfg.AttachmentsDir()

	// Create syncer (no CLI progress for daemon mode)
	syncer := sync.New(client, s, opts).WithLogger(logger)
	if vf != nil {
		syncer.SetEmbedEnqueuer(vf.Enqueuer)
	}

	// Resolve source — scheduled sync is Gmail-only.
	source, err := s.GetOrCreateSource("gmail", email)
	if err != nil {
		return fmt.Errorf("get source: %w", err)
	}

	// Run incremental sync
	summary, err := syncer.Incremental(ctx, source)
	if err != nil {
		return fmt.Errorf("incremental sync failed: %w", err)
	}

	logger.Info("sync completed",
		"email", email,
		"messages_added", summary.MessagesAdded,
		"duration", time.Since(startTime),
	)

	// Rebuild cache if stale (covers new messages and deletions).
	dbPath := cfg.DatabaseDSN()
	analyticsDir := cfg.AnalyticsDir()
	if staleness := cacheNeedsBuild(dbPath, analyticsDir); staleness.NeedsBuild {
		logger.Info("rebuilding cache after sync",
			"email", email, "reason", staleness.Reason,
			"full_rebuild", staleness.FullRebuild)
		result, err := buildCache(
			dbPath, analyticsDir, staleness.FullRebuild)
		if err != nil {
			logger.Error("cache build failed", "error", err)
			// Don't fail the sync for cache build errors
		} else if !result.Skipped {
			logger.Info("cache build completed",
				"exported", result.ExportedCount,
			)
		}
	}

	return nil
}
