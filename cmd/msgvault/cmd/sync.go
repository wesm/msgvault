package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/wesm/msgvault/internal/gmail"
	"github.com/wesm/msgvault/internal/oauth"
	"github.com/wesm/msgvault/internal/store"
	"github.com/wesm/msgvault/internal/sync"
)

var syncIncrementalCmd = &cobra.Command{
	Use:     "sync <email>",
	Aliases: []string{"sync-incremental"},
	Short:   "Sync new and changed messages from a Gmail account",
	Long: `Perform an incremental synchronization using the Gmail History API.

This is faster than a full sync as it only fetches changes since the last sync.
Requires a prior full sync to establish the history ID baseline.

If history is too old (Gmail returns 404), falls back to suggesting a full sync.

Examples:
  msgvault sync you@gmail.com`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		email := args[0]

		// Validate config
		if cfg.OAuth.ClientSecrets == "" {
			return errOAuthNotConfigured()
		}

		// Open database
		dbPath := cfg.DatabasePath()
		s, err := store.Open(dbPath)
		if err != nil {
			return fmt.Errorf("open database: %w", err)
		}
		defer s.Close()

		if err := s.InitSchema(); err != nil {
			return fmt.Errorf("init schema: %w", err)
		}

		// Check if source exists with history ID
		source, err := s.GetSourceByIdentifier(email)
		if err != nil {
			return fmt.Errorf("get source: %w", err)
		}
		if source == nil {
			return fmt.Errorf("no source found for %s - run 'sync-full' first", email)
		}
		if !source.SyncCursor.Valid || source.SyncCursor.String == "" {
			return fmt.Errorf("no history ID for %s - run 'sync-full' first", email)
		}

		// Create OAuth manager and get token source
		oauthMgr, err := oauth.NewManager(cfg.OAuth.ClientSecrets, cfg.TokensDir(), logger)
		if err != nil {
			return wrapOAuthError(fmt.Errorf("create oauth manager: %w", err))
		}

		// Set up context with cancellation
		ctx, cancel := context.WithCancel(cmd.Context())
		defer cancel()

		// Handle Ctrl+C gracefully
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		go func() {
			<-sigChan
			fmt.Println("\nInterrupted. Saving checkpoint...")
			cancel()
		}()

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

		// Create syncer with progress reporter
		syncer := sync.New(client, s, opts).
			WithLogger(logger).
			WithProgress(&CLIProgress{})

		// Run incremental sync
		startTime := time.Now()
		fmt.Printf("Starting incremental sync for %s\n", email)
		fmt.Printf("Last history ID: %s\n\n", source.SyncCursor.String)

		summary, err := syncer.Incremental(ctx, email)
		if err != nil {
			if ctx.Err() != nil {
				fmt.Println("\nSync interrupted. Run again to resume.")
				return nil
			}
			// Check for history expired error
			if errors.Is(err, sync.ErrHistoryExpired) {
				fmt.Println("\nHistory ID has expired. Gmail only keeps ~7 days of history.")
				fmt.Println("Run 'sync-full' to catch up on missed changes.")
				return nil
			}
			return fmt.Errorf("sync failed: %w", err)
		}

		// Print summary
		fmt.Println()
		fmt.Println("Sync complete!")
		fmt.Printf("  Duration:      %s\n", summary.Duration.Round(time.Second))
		fmt.Printf("  Changes:       %d processed, %d added\n",
			summary.MessagesFound, summary.MessagesAdded)
		fmt.Printf("  Downloaded:    %.2f MB\n", float64(summary.BytesDownloaded)/(1024*1024))
		if summary.Errors > 0 {
			fmt.Printf("  Errors:        %d\n", summary.Errors)
		}

		elapsed := time.Since(startTime)
		logger.Info("incremental sync completed",
			"email", email,
			"messages_added", summary.MessagesAdded,
			"elapsed", elapsed,
		)

		return nil
	},
}

func init() {
	rootCmd.AddCommand(syncIncrementalCmd)
}
