package cmd

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/wesm/msgvault/internal/gmail"
	"github.com/wesm/msgvault/internal/mime"
	"github.com/wesm/msgvault/internal/oauth"
	"github.com/wesm/msgvault/internal/store"
)

var verifySampleSize int

var verifyCmd = &cobra.Command{
	Use:   "verify <email>",
	Short: "Verify archive integrity against Gmail",
	Long: `Verify the local archive by comparing message counts with Gmail
and sampling messages to ensure raw MIME data is intact.

This command:
1. Compares local message count with Gmail's reported total
2. Checks how many messages have raw MIME data stored
3. Samples random messages and verifies their MIME can be decompressed

Examples:
  msgvault verify you@gmail.com
  msgvault verify you@gmail.com --sample 200`,
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
			fmt.Println("\nInterrupted.")
			cancel()
		}()

		tokenSource, err := oauthMgr.TokenSource(ctx, email)
		if err != nil {
			return fmt.Errorf("get token source: %w (run 'add-account' first)", err)
		}

		// Create Gmail client (no rate limiter needed for single call)
		client := gmail.NewClient(tokenSource, gmail.WithLogger(logger))
		defer client.Close()

		// Get Gmail profile
		profile, err := client.GetProfile(ctx)
		if err != nil {
			return fmt.Errorf("get Gmail profile: %w", err)
		}

		fmt.Printf("Verifying archive for %s...\n\n", profile.EmailAddress)

		// Get source from database
		source, err := s.GetSourceByIdentifier(profile.EmailAddress)
		if err != nil {
			return fmt.Errorf("get source: %w", err)
		}
		if source == nil {
			fmt.Printf("Account %s not found in database.\n", profile.EmailAddress)
			fmt.Println("Run 'sync-full' first to populate the archive.")
			return nil
		}

		// Count local messages
		archiveCount, err := s.CountMessagesForSource(source.ID)
		if err != nil {
			return fmt.Errorf("count messages: %w", err)
		}

		withRaw, err := s.CountMessagesWithRaw(source.ID)
		if err != nil {
			return fmt.Errorf("count messages with raw: %w", err)
		}

		// Print summary
		gmailTotal := profile.MessagesTotal
		fmt.Printf("Gmail messages:      %10d\n", gmailTotal)
		fmt.Printf("Archived messages:   %10d\n", archiveCount)
		diff := gmailTotal - archiveCount
		if diff > 0 {
			fmt.Printf("Missing:             %10d\n", diff)
		} else if diff < 0 {
			fmt.Printf("Extra in archive:    %10d\n", -diff)
		} else {
			fmt.Printf("Difference:          %10d\n", diff)
		}
		fmt.Println()

		rawPct := float64(0)
		if archiveCount > 0 {
			rawPct = float64(withRaw) / float64(archiveCount) * 100
		}
		fmt.Printf("With raw MIME:       %10d (%.1f%%)\n", withRaw, rawPct)
		fmt.Println()

		// Sample verification
		if archiveCount > 0 && verifySampleSize > 0 {
			actualSampleSize := verifySampleSize
			if int64(actualSampleSize) > archiveCount {
				actualSampleSize = int(archiveCount)
			}

			sampleIDs, err := s.GetRandomMessageIDs(source.ID, actualSampleSize)
			if err != nil {
				return fmt.Errorf("get sample IDs: %w", err)
			}

			fmt.Printf("Sampling %d messages...\n", len(sampleIDs))

			verified := 0
			var errors []string

			for _, msgID := range sampleIDs {
				// Check context cancellation
				if ctx.Err() != nil {
					fmt.Println("\nVerification interrupted.")
					break
				}

				// Get raw MIME
				rawData, err := s.GetMessageRaw(msgID)
				if err != nil {
					if err == sql.ErrNoRows {
						errors = append(errors, fmt.Sprintf("msg %d: missing raw MIME", msgID))
					} else {
						errors = append(errors, fmt.Sprintf("msg %d: db error (%v)", msgID, err))
					}
					continue
				}

				// Verify it can be parsed as MIME
				_, err = mime.Parse(rawData)
				if err != nil {
					errors = append(errors, fmt.Sprintf("msg %d: corrupt MIME (%v)", msgID, err))
					continue
				}

				verified++
			}

			if len(errors) > 0 {
				fmt.Printf("Sample verified:     %10d of %d\n", verified, len(sampleIDs))
				fmt.Printf("Sample errors:       %10d\n", len(errors))
				for i, err := range errors {
					if i >= 5 {
						fmt.Printf("  ... and %d more\n", len(errors)-5)
						break
					}
					fmt.Printf("  - %s\n", err)
				}
			} else {
				fmt.Printf("Sample verified:     %10d (all OK)\n", verified)
			}
		}

		fmt.Println()
		fmt.Println("Verification complete.")

		return nil
	},
}

func init() {
	verifyCmd.Flags().IntVar(&verifySampleSize, "sample", 100, "Number of messages to sample for MIME verification")
	rootCmd.AddCommand(verifyCmd)
}
