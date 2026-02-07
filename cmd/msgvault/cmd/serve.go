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

	// Check for scheduled accounts
	scheduled := cfg.ScheduledAccounts()
	if len(scheduled) == 0 {
		return fmt.Errorf("no scheduled accounts configured\n\nAdd accounts to config.toml:\n\n  [[accounts]]\n  email = \"you@gmail.com\"\n  schedule = \"0 2 * * *\"\n  enabled = true")
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
		return fmt.Errorf("no accounts could be scheduled")
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
type storeAPIAdapter struct {
	store *store.Store
}

func (a *storeAPIAdapter) GetStats() (*api.StoreStats, error) {
	stats, err := a.store.GetStats()
	if err != nil {
		return nil, err
	}
	return &api.StoreStats{
		MessageCount:    stats.MessageCount,
		ThreadCount:     stats.ThreadCount,
		SourceCount:     stats.SourceCount,
		LabelCount:      stats.LabelCount,
		AttachmentCount: stats.AttachmentCount,
		DatabaseSize:    stats.DatabaseSize,
	}, nil
}

func (a *storeAPIAdapter) ListMessages(offset, limit int) ([]api.APIMessage, int64, error) {
	msgs, total, err := a.store.ListMessages(offset, limit)
	if err != nil {
		return nil, 0, err
	}
	return convertMessages(msgs), total, nil
}

func (a *storeAPIAdapter) GetMessage(id int64) (*api.APIMessage, error) {
	msg, err := a.store.GetMessage(id)
	if err != nil {
		return nil, err
	}
	if msg == nil {
		return nil, nil
	}
	result := convertMessage(*msg)
	return &result, nil
}

func (a *storeAPIAdapter) SearchMessages(query string, offset, limit int) ([]api.APIMessage, int64, error) {
	msgs, total, err := a.store.SearchMessages(query, offset, limit)
	if err != nil {
		return nil, 0, err
	}
	return convertMessages(msgs), total, nil
}

func convertMessages(msgs []store.APIMessage) []api.APIMessage {
	result := make([]api.APIMessage, len(msgs))
	for i, m := range msgs {
		result[i] = convertMessage(m)
	}
	return result
}

func convertMessage(m store.APIMessage) api.APIMessage {
	var attachments []api.APIAttachment
	for _, a := range m.Attachments {
		attachments = append(attachments, api.APIAttachment{
			Filename: a.Filename,
			MimeType: a.MimeType,
			Size:     a.Size,
		})
	}
	return api.APIMessage{
		ID:             m.ID,
		Subject:        m.Subject,
		From:           m.From,
		To:             m.To,
		SentAt:         m.SentAt,
		Snippet:        m.Snippet,
		Labels:         m.Labels,
		HasAttachments: m.HasAttachments,
		SizeEstimate:   m.SizeEstimate,
		Body:           m.Body,
		Headers:        m.Headers,
		Attachments:    attachments,
	}
}

// schedulerAdapter adapts scheduler.Scheduler to api.SyncScheduler.
type schedulerAdapter struct {
	scheduler *scheduler.Scheduler
}

func (a *schedulerAdapter) IsScheduled(email string) bool {
	return a.scheduler.IsScheduled(email)
}

func (a *schedulerAdapter) TriggerSync(email string) error {
	return a.scheduler.TriggerSync(email)
}

func (a *schedulerAdapter) IsRunning() bool {
	return a.scheduler.IsRunning()
}

func (a *schedulerAdapter) Status() []api.AccountStatus {
	statuses := a.scheduler.Status()
	result := make([]api.AccountStatus, len(statuses))
	for i, s := range statuses {
		result[i] = api.AccountStatus{
			Email:     s.Email,
			Running:   s.Running,
			LastRun:   s.LastRun,
			NextRun:   s.NextRun,
			Schedule:  s.Schedule,
			LastError: s.LastError,
		}
	}
	return result
}

// runScheduledSync performs an incremental sync for a scheduled account.
func runScheduledSync(ctx context.Context, email string, s *store.Store, oauthMgr *oauth.Manager) error {
	logger.Info("starting scheduled sync", "email", email)
	startTime := time.Now()

	// Get token source
	tokenSource, err := oauthMgr.TokenSource(ctx, email)
	if err != nil {
		return fmt.Errorf("get token source: %w (run 'add-account' first)", err)
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
