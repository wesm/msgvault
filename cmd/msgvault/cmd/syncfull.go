package cmd

import (
	"context"
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

var (
	syncQuery    string
	syncNoResume bool
	syncBefore   string
	syncAfter    string
	syncLimit    int
)

var syncFullCmd = &cobra.Command{
	Use:   "sync-full [email]",
	Short: "Perform a full sync of Gmail accounts",
	Long: `Perform a full synchronization of a Gmail account.

Downloads all messages matching the query (or all messages if no query).
Supports resumption from interruption - just run again to continue.

If no email is specified, syncs all configured accounts sequentially.

Date filters:
  --after 2024-01-01     Only messages on or after this date
  --before 2024-12-31    Only messages before this date

Examples:
  msgvault sync-full                             # Sync all accounts
  msgvault sync-full you@gmail.com
  msgvault sync-full you@gmail.com --after 2024-01-01
  msgvault sync-full you@gmail.com --query "from:someone@example.com"
  msgvault sync-full you@gmail.com --noresume    # Force fresh sync`,
	Args: cobra.MaximumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		if syncLimit < 0 {
			return fmt.Errorf("--limit must be a non-negative number")
		}

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
				if !oauthMgr.HasToken(src.Identifier) {
					fmt.Printf("Skipping %s (no OAuth token - run 'add-account' first)\n", src.Identifier)
					continue
				}
				emails = append(emails, src.Identifier)
			}
			if len(emails) == 0 {
				return fmt.Errorf("no accounts have valid OAuth tokens - run 'add-account' first")
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

			if err := runFullSync(ctx, s, oauthMgr, email); err != nil {
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

func runFullSync(ctx context.Context, s *store.Store, oauthMgr *oauth.Manager, email string) error {
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

	// Build query from flags
	query := buildSyncQuery()

	// Set up sync options
	opts := sync.DefaultOptions()
	opts.Query = query
	opts.NoResume = syncNoResume
	opts.Limit = syncLimit
	opts.AttachmentsDir = cfg.AttachmentsDir()

	// Create syncer with progress reporter
	syncer := sync.New(client, s, opts).
		WithLogger(logger).
		WithProgress(&CLIProgress{})

	// Run sync
	startTime := time.Now()
	fmt.Printf("Starting full sync for %s\n", email)
	if query != "" {
		fmt.Printf("Query: %s\n", query)
	}
	fmt.Println()

	summary, err := syncer.Full(ctx, email)
	if err != nil {
		if ctx.Err() != nil {
			fmt.Println("\nSync interrupted. Run again to resume.")
			return nil
		}
		return fmt.Errorf("sync failed: %w", err)
	}

	// Print summary
	fmt.Println()
	fmt.Println("Sync complete!")
	fmt.Printf("  Duration:      %s\n", summary.Duration.Round(time.Second))
	fmt.Printf("  Messages:      %d found, %d added, %d skipped\n",
		summary.MessagesFound, summary.MessagesAdded, summary.MessagesSkipped)
	fmt.Printf("  Downloaded:    %.2f MB\n", float64(summary.BytesDownloaded)/(1024*1024))
	if summary.Errors > 0 {
		fmt.Printf("  Errors:        %d\n", summary.Errors)
	}
	if summary.WasResumed {
		fmt.Printf("  (Resumed from checkpoint)\n")
	}

	// Print timing stats
	if summary.MessagesAdded > 0 {
		messagesPerSec := float64(summary.MessagesAdded) / summary.Duration.Seconds()
		fmt.Printf("  Rate:          %.1f messages/sec\n", messagesPerSec)
	}

	elapsed := time.Since(startTime)
	logger.Info("sync completed",
		"email", email,
		"messages_added", summary.MessagesAdded,
		"elapsed", elapsed,
	)

	return nil
}

// buildSyncQuery constructs a Gmail search query from flags.
func buildSyncQuery() string {
	parts := []string{}

	if syncAfter != "" {
		parts = append(parts, fmt.Sprintf("after:%s", syncAfter))
	}
	if syncBefore != "" {
		parts = append(parts, fmt.Sprintf("before:%s", syncBefore))
	}
	if syncQuery != "" {
		parts = append(parts, syncQuery)
	}

	result := ""
	for i, p := range parts {
		if i > 0 {
			result += " "
		}
		result += p
	}
	return result
}

// CLIProgress implements gmail.SyncProgressWithDate for terminal output.
type CLIProgress struct {
	startTime  time.Time
	lastPrint  time.Time
	latestDate time.Time
	// Cache latest stats for combined display
	processed int64
	added     int64
	skipped   int64
}

func (p *CLIProgress) OnStart(total int64) {
	now := time.Now()
	p.startTime = now
	p.lastPrint = now
	// Don't print Gmail's estimate - it's often wildly inaccurate
}

func (p *CLIProgress) OnProgress(processed, added, skipped int64) {
	if p.startTime.IsZero() {
		now := time.Now()
		p.startTime = now
		p.lastPrint = now
	}
	p.processed = processed
	p.added = added
	p.skipped = skipped
	p.printProgress()
}

func (p *CLIProgress) OnLatestDate(date time.Time) {
	if p.startTime.IsZero() {
		now := time.Now()
		p.startTime = now
		p.lastPrint = now
	}
	p.latestDate = date
	p.printProgress()
}

func (p *CLIProgress) printProgress() {
	// Throttle output to every 2 seconds
	if time.Since(p.lastPrint) < 2*time.Second {
		return
	}
	p.lastPrint = time.Now()

	elapsed := time.Since(p.startTime)
	rate := 0.0
	if elapsed.Seconds() >= 1 {
		rate = float64(p.added) / elapsed.Seconds()
	}

	// Format elapsed time nicely
	elapsedStr := formatDuration(elapsed)

	// Format latest message date if available
	dateStr := ""
	if !p.latestDate.IsZero() {
		dateStr = fmt.Sprintf(" | Latest: %s", p.latestDate.Format("Jan 2006"))
	}

	fmt.Printf("\r  Scanned: %d | Added: %d | Skipped: %d | Rate: %.1f/s | Elapsed: %s%s    ",
		p.processed, p.added, p.skipped, rate, elapsedStr, dateStr)
}

func (p *CLIProgress) OnComplete(summary *gmail.SyncSummary) {
	fmt.Println() // Clear the progress line
}

func (p *CLIProgress) OnError(err error) {
	fmt.Printf("\nError: %v\n", err)
}

// formatDuration formats a duration as "Xm Ys" or "Xh Ym" for readability.
func formatDuration(d time.Duration) string {
	d = d.Round(time.Second)
	h := d / time.Hour
	d -= h * time.Hour
	m := d / time.Minute
	d -= m * time.Minute
	s := d / time.Second

	if h > 0 {
		return fmt.Sprintf("%dh %dm", h, m)
	}
	if m > 0 {
		return fmt.Sprintf("%dm %ds", m, s)
	}
	return fmt.Sprintf("%ds", s)
}

func init() {
	syncFullCmd.Flags().StringVar(&syncQuery, "query", "", "Gmail search query")
	syncFullCmd.Flags().BoolVar(&syncNoResume, "noresume", false, "Force fresh sync (don't resume)")
	syncFullCmd.Flags().StringVar(&syncBefore, "before", "", "Only messages before this date (YYYY-MM-DD)")
	syncFullCmd.Flags().StringVar(&syncAfter, "after", "", "Only messages after this date (YYYY-MM-DD)")
	syncFullCmd.Flags().IntVar(&syncLimit, "limit", 0, "Limit number of messages (for testing)")
	rootCmd.AddCommand(syncFullCmd)
}
