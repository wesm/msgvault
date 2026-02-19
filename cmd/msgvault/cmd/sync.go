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
	Use:     "sync [email]",
	Aliases: []string{"sync-incremental"},
	Short:   "Sync new and changed messages from Gmail accounts",
	Long: `Perform an incremental synchronization using the Gmail History API.

This is faster than a full sync as it only fetches changes since the last sync.
Requires a prior full sync to establish the history ID baseline.

If no email is specified, syncs all accounts that have a history ID from a
previous full sync. Accounts without tokens or history IDs are skipped.

If history is too old (Gmail returns 404), falls back to suggesting a full sync.

Examples:
  msgvault sync                 # Sync all accounts
  msgvault sync you@gmail.com   # Sync specific account`,
	Args: cobra.MaximumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		// Validate config
		if cfg.OAuth.ClientSecrets == "" {
			return errOAuthNotConfigured()
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

		// Determine which accounts to sync
		var emails []string
		if len(args) == 1 {
			emails = []string{args[0]}
		} else {
			sources, err := s.ListSources("gmail")
			if err != nil {
				return fmt.Errorf("list sources: %w", err)
			}
			if len(sources) == 0 {
				return fmt.Errorf("no accounts configured - run 'add-account' first")
			}
			for _, src := range sources {
				if !src.SyncCursor.Valid || src.SyncCursor.String == "" {
					fmt.Printf("Skipping %s (no history ID - run 'sync-full' first)\n", src.Identifier)
					continue
				}
				if !oauthMgr.HasToken(src.Identifier) {
					fmt.Printf("Skipping %s (no OAuth token - run 'add-account' first)\n", src.Identifier)
					continue
				}
				emails = append(emails, src.Identifier)
			}
			if len(emails) == 0 {
				return fmt.Errorf("no accounts have been fully synced yet - run 'sync-full' first")
			}
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

		var syncErrors []string
		for _, email := range emails {
			if ctx.Err() != nil {
				break
			}

			if err := runIncrementalSync(ctx, s, oauthMgr, email); err != nil {
				syncErrors = append(syncErrors, fmt.Sprintf("%s: %v", email, err))
				continue
			}
		}

		if len(syncErrors) > 0 {
			fmt.Println()
			fmt.Println("Errors:")
			for _, e := range syncErrors {
				fmt.Printf("  %s\n", e)
			}
			return fmt.Errorf("%d account(s) failed to sync", len(syncErrors))
		}

		return nil
	},
}

func runIncrementalSync(ctx context.Context, s *store.Store, oauthMgr *oauth.Manager, email string) error {
	// Check if source exists with history ID
	source, err := s.GetSourceByIdentifier(email)
	if err != nil {
		return fmt.Errorf("get source: %w", err)
	}
	if source == nil {
		return fmt.Errorf("no source found - run 'sync-full' first")
	}
	if !source.SyncCursor.Valid || source.SyncCursor.String == "" {
		return fmt.Errorf("no history ID - run 'sync-full' first")
	}

	tokenSource, err := getTokenSourceWithReauth(ctx, oauthMgr, email)
	if err != nil {
		return err
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
}

func init() {
	rootCmd.AddCommand(syncIncrementalCmd)
}
