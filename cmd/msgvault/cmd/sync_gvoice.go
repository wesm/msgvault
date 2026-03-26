package cmd

import (
	"context"
	"fmt"
	"os"
	"strings"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/wesm/msgvault/internal/gvoice"
	"github.com/wesm/msgvault/internal/store"
	"github.com/wesm/msgvault/internal/sync"
)

var (
	gvoiceBefore   string
	gvoiceAfter    string
	gvoiceLimit    int
	gvoiceNoResume bool
)

var syncGvoiceCmd = &cobra.Command{
	Use:   "sync-gvoice <takeout-voice-dir>",
	Short: "Import Google Voice messages from Takeout export",
	Long: `Import Google Voice SMS, MMS, and call records from a Google Takeout export.

Reads HTML files from the Voice/Calls/ directory in a Takeout archive and
stores them in the msgvault archive alongside Gmail and iMessage data.

The takeout-voice-dir argument should point to the "Voice" directory inside
the extracted Takeout archive, which contains "Calls/" and "Phones.vcf".

Date filters:
  --after 2020-01-01     Only messages on or after this date
  --before 2024-12-31    Only messages before this date

Examples:
  msgvault sync-gvoice /path/to/Takeout/Voice
  msgvault sync-gvoice /path/to/Takeout/Voice --after 2020-01-01
  msgvault sync-gvoice /path/to/Takeout/Voice --limit 100`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		takeoutDir := args[0]

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

		// Check takeout directory exists
		if _, err := os.Stat(takeoutDir); os.IsNotExist(err) {
			return fmt.Errorf("takeout directory not found: %s", takeoutDir)
		}

		// Build client options
		var clientOpts []gvoice.ClientOption
		clientOpts = append(clientOpts, gvoice.WithLogger(logger))

		if gvoiceAfter != "" {
			t, err := time.Parse("2006-01-02", gvoiceAfter)
			if err != nil {
				return fmt.Errorf("invalid --after date: %w (use YYYY-MM-DD format)", err)
			}
			clientOpts = append(clientOpts, gvoice.WithAfterDate(t))
		}

		if gvoiceBefore != "" {
			t, err := time.Parse("2006-01-02", gvoiceBefore)
			if err != nil {
				return fmt.Errorf("invalid --before date: %w (use YYYY-MM-DD format)", err)
			}
			clientOpts = append(clientOpts, gvoice.WithBeforeDate(t))
		}

		if gvoiceLimit > 0 {
			clientOpts = append(clientOpts, gvoice.WithLimit(gvoiceLimit))
		}

		// Create Google Voice client
		gvClient, err := gvoice.NewClient(takeoutDir, clientOpts...)
		if err != nil {
			return fmt.Errorf("open Google Voice takeout: %w", err)
		}
		defer gvClient.Close()

		identifier := gvClient.Identifier()

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
		opts.NoResume = gvoiceNoResume
		opts.SourceType = "google_voice"
		opts.AttachmentsDir = cfg.AttachmentsDir()

		// Create syncer with progress reporter
		syncer := sync.New(gvClient, s, opts).
			WithLogger(logger).
			WithProgress(&CLIProgress{})

		// Run sync
		startTime := time.Now()
		fmt.Printf("Starting Google Voice import from %s\n", takeoutDir)
		fmt.Printf("Google Voice number: %s\n", identifier)
		if gvoiceAfter != "" || gvoiceBefore != "" {
			parts := []string{}
			if gvoiceAfter != "" {
				parts = append(parts, "after "+gvoiceAfter)
			}
			if gvoiceBefore != "" {
				parts = append(parts, "before "+gvoiceBefore)
			}
			fmt.Printf("Date filter: %s\n", strings.Join(parts, ", "))
		}
		if gvoiceLimit > 0 {
			fmt.Printf("Limit: %d messages\n", gvoiceLimit)
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
		fmt.Println("Google Voice import complete!")
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

func init() {
	syncGvoiceCmd.Flags().StringVar(&gvoiceBefore, "before", "", "only messages before this date (YYYY-MM-DD)")
	syncGvoiceCmd.Flags().StringVar(&gvoiceAfter, "after", "", "only messages after this date (YYYY-MM-DD)")
	syncGvoiceCmd.Flags().IntVar(&gvoiceLimit, "limit", 0, "limit number of messages (for testing)")
	syncGvoiceCmd.Flags().BoolVar(&gvoiceNoResume, "noresume", false, "force fresh sync (don't resume)")
	rootCmd.AddCommand(syncGvoiceCmd)
}
