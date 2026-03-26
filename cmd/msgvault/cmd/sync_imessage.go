package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/wesm/msgvault/internal/imessage"
	"github.com/wesm/msgvault/internal/store"
	"github.com/wesm/msgvault/internal/sync"
)

var (
	imessageDBPath   string
	imessageBefore   string
	imessageAfter    string
	imessageLimit    int
	imessageMe       string
	imessageNoResume bool
)

var syncImessageCmd = &cobra.Command{
	Use:   "sync-imessage",
	Short: "Import iMessages from local database",
	Long: `Import iMessages from macOS's local Messages database (chat.db).

Reads messages from ~/Library/Messages/chat.db and stores them in the
msgvault archive alongside Gmail messages. This is a read-only operation
that does not modify the iMessage database.

Requires Full Disk Access permission in System Settings > Privacy & Security.

Date filters:
  --after 2024-01-01     Only messages on or after this date
  --before 2024-12-31    Only messages before this date

Examples:
  msgvault sync-imessage
  msgvault sync-imessage --after 2024-01-01
  msgvault sync-imessage --limit 100
  msgvault sync-imessage --me "+15551234567"
  msgvault sync-imessage --db-path /path/to/chat.db`,
	RunE: func(cmd *cobra.Command, args []string) error {
		// Open msgvault database
		dbPath := cfg.DatabaseDSN()
		s, err := store.Open(dbPath)
		if err != nil {
			return fmt.Errorf("open database: %w", err)
		}
		defer s.Close()

		if err := s.InitSchema(); err != nil {
			return fmt.Errorf("init schema: %w", err)
		}

		// Resolve chat.db path
		chatDBPath := imessageDBPath
		if chatDBPath == "" {
			home, err := os.UserHomeDir()
			if err != nil {
				return fmt.Errorf("get home directory: %w", err)
			}
			chatDBPath = filepath.Join(home, "Library", "Messages", "chat.db")
		}

		// Check chat.db exists
		if _, err := os.Stat(chatDBPath); os.IsNotExist(err) {
			return fmt.Errorf("iMessage database not found at %s\n\nMake sure you're running on macOS with Messages enabled", chatDBPath)
		}

		// Build client options
		var clientOpts []imessage.ClientOption
		clientOpts = append(clientOpts, imessage.WithImessageLogger(logger))

		if imessageMe != "" {
			clientOpts = append(clientOpts, imessage.WithMyAddress(imessageMe))
		}

		if imessageAfter != "" {
			t, err := time.Parse("2006-01-02", imessageAfter)
			if err != nil {
				return fmt.Errorf("invalid --after date: %w (use YYYY-MM-DD format)", err)
			}
			clientOpts = append(clientOpts, imessage.WithAfterDate(t))
		}

		if imessageBefore != "" {
			t, err := time.Parse("2006-01-02", imessageBefore)
			if err != nil {
				return fmt.Errorf("invalid --before date: %w (use YYYY-MM-DD format)", err)
			}
			clientOpts = append(clientOpts, imessage.WithBeforeDate(t))
		}

		if imessageLimit > 0 {
			clientOpts = append(clientOpts, imessage.WithLimit(imessageLimit))
		}

		// Determine source identifier
		identifier := "local"
		if imessageMe != "" {
			identifier = imessageMe
		}

		// Create iMessage client
		imsgClient, err := imessage.NewClient(chatDBPath, identifier, clientOpts...)
		if err != nil {
			return fmt.Errorf("open iMessage database: %w", err)
		}
		defer imsgClient.Close()

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

		// Set up sync options
		opts := sync.DefaultOptions()
		opts.NoResume = imessageNoResume
		opts.SourceType = "apple_messages"
		opts.AttachmentsDir = cfg.AttachmentsDir()

		// Create syncer with progress reporter
		syncer := sync.New(imsgClient, s, opts).
			WithLogger(logger).
			WithProgress(&CLIProgress{})

		// Run sync
		startTime := time.Now()
		fmt.Printf("Starting iMessage sync from %s\n", chatDBPath)
		if imessageAfter != "" || imessageBefore != "" {
			parts := []string{}
			if imessageAfter != "" {
				parts = append(parts, "after "+imessageAfter)
			}
			if imessageBefore != "" {
				parts = append(parts, "before "+imessageBefore)
			}
			fmt.Printf("Date filter: %s\n", joinParts(parts))
		}
		if imessageLimit > 0 {
			fmt.Printf("Limit: %d messages\n", imessageLimit)
		}
		fmt.Println()

		summary, err := syncer.Full(ctx, identifier)
		if err != nil {
			if ctx.Err() != nil {
				fmt.Println("\nSync interrupted. Run again to resume.")
				return nil
			}
			return fmt.Errorf("sync failed: %w", err)
		}

		// Print summary
		fmt.Println()
		fmt.Println("iMessage sync complete!")
		fmt.Printf("  Duration:      %s\n", summary.Duration.Round(time.Second))
		fmt.Printf("  Messages:      %d found, %d added, %d skipped\n",
			summary.MessagesFound, summary.MessagesAdded, summary.MessagesSkipped)
		if summary.Errors > 0 {
			fmt.Printf("  Errors:        %d\n", summary.Errors)
		}
		if summary.WasResumed {
			fmt.Printf("  (Resumed from checkpoint)\n")
		}

		if summary.MessagesAdded > 0 {
			elapsed := time.Since(startTime)
			messagesPerSec := float64(summary.MessagesAdded) / elapsed.Seconds()
			fmt.Printf("  Rate:          %.1f messages/sec\n", messagesPerSec)
		}

		return nil
	},
}

func joinParts(parts []string) string {
	result := ""
	for i, p := range parts {
		if i > 0 {
			result += ", "
		}
		result += p
	}
	return result
}

func init() {
	syncImessageCmd.Flags().StringVar(&imessageDBPath, "db-path", "", "path to chat.db (default: ~/Library/Messages/chat.db)")
	syncImessageCmd.Flags().StringVar(&imessageBefore, "before", "", "only messages before this date (YYYY-MM-DD)")
	syncImessageCmd.Flags().StringVar(&imessageAfter, "after", "", "only messages after this date (YYYY-MM-DD)")
	syncImessageCmd.Flags().IntVar(&imessageLimit, "limit", 0, "limit number of messages (for testing)")
	syncImessageCmd.Flags().StringVar(&imessageMe, "me", "", "your phone number or email (e.g., +15551234567)")
	syncImessageCmd.Flags().BoolVar(&imessageNoResume, "noresume", false, "force fresh sync (don't resume)")
	rootCmd.AddCommand(syncImessageCmd)
}
