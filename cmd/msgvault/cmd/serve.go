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
	"github.com/wesm/msgvault/internal/oauth"
	"github.com/wesm/msgvault/internal/scheduler"
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

	// Validate config
	if cfg.OAuth.ClientSecrets == "" {
		return errOAuthNotConfigured()
	}

	// Check for scheduled accounts (warn but don't fail - allows token upload first)
	scheduled := cfg.ScheduledAccounts()
	if len(scheduled) == 0 {
		logger.Warn("no scheduled accounts configured - server will start but no syncs will run",
			"hint", "Add accounts to config.toml or upload tokens via API first")
	}

	// Open database
	dbPath := cfg.DatabaseDSN()
	s, err := store.Open(dbPath)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer s.Close()

	if err := s.InitSchema(); err != nil {
		return fmt.Errorf("init schema: %w", err)
	}

	// Create OAuth manager
	oauthMgr, err := oauth.NewManager(cfg.OAuth.ClientSecrets, cfg.TokensDir(), logger)
	if err != nil {
		return wrapOAuthError(fmt.Errorf("create oauth manager: %w", err))
	}

	// Create sync function for the scheduler
	syncFunc := func(ctx context.Context, email string) error {
		return runScheduledSync(ctx, email, s, oauthMgr)
	}

	// Create and configure scheduler
	sched := scheduler.New(syncFunc).WithLogger(logger)

	// Add all scheduled accounts
	count, errs := sched.AddAccountsFromConfig(cfg)
	if len(errs) > 0 {
		for _, err := range errs {
			logger.Error("failed to schedule account", "error", err)
		}
	}
	if count == 0 {
		logger.Warn("no accounts scheduled - upload tokens via API and add accounts to config.toml")
	}

	// Set up signal handling for graceful shutdown
	ctx, cancel := context.WithCancel(cmd.Context())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start the scheduler
	sched.Start()

	// Create adapters for the API interfaces
	storeAdapter := &storeAPIAdapter{store: s}
	schedAdapter := &schedulerAdapter{scheduler: sched}

	// Create and start API server
	apiServer := api.NewServer(cfg, storeAdapter, schedAdapter, logger)

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
	fmt.Printf("msgvault daemon started\n")
	fmt.Printf("  API server: http://%s\n", net.JoinHostPort(bindAddr, strconv.Itoa(cfg.Server.APIPort)))
	fmt.Printf("  Scheduled accounts: %d\n", count)
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

func (a *storeAPIAdapter) SearchMessages(query string, offset, limit int) ([]api.APIMessage, int64, error) {
	return a.store.SearchMessages(query, offset, limit)
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

// runScheduledSync performs an incremental sync for a scheduled account.
func runScheduledSync(ctx context.Context, email string, s *store.Store, oauthMgr *oauth.Manager) error {
	logger.Info("starting scheduled sync", "email", email)
	startTime := time.Now()

	// Get token source — intentionally not using getTokenSourceWithReauth here
	// because serve runs as a daemon and cannot open a browser for OAuth.
	tokenSource, err := oauthMgr.TokenSource(ctx, email)
	if err != nil {
		if oauthMgr.HasToken(email) {
			return fmt.Errorf("get token source: %w (token may be expired; run 'add-account %s --force' to re-authorize)", err, email)
		}
		return fmt.Errorf("get token source: %w (run 'add-account %s' first)", err, email)
	}

	// Create Gmail client
	rateLimiter := gmail.NewRateLimiter(float64(cfg.Sync.RateLimitQPS))
	client := gmail.NewClient(tokenSource,
		gmail.WithLogger(logger),
		gmail.WithRateLimiter(rateLimiter),
	)
	defer client.Close()

	// Set up sync options
	opts := sync.DefaultOptions()
	opts.AttachmentsDir = cfg.AttachmentsDir()

	// Create syncer (no CLI progress for daemon mode)
	syncer := sync.New(client, s, opts).WithLogger(logger)

	// Run incremental sync
	summary, err := syncer.Incremental(ctx, email)
	if err != nil {
		return fmt.Errorf("incremental sync failed: %w", err)
	}

	logger.Info("sync completed",
		"email", email,
		"messages_added", summary.MessagesAdded,
		"duration", time.Since(startTime),
	)

	// Build cache after sync if there were new messages
	if summary.MessagesAdded > 0 {
		logger.Info("building cache after sync", "email", email)
		result, err := buildCache(cfg.DatabaseDSN(), cfg.AnalyticsDir(), false)
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
