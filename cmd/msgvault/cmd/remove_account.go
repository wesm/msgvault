package cmd

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
	"github.com/wesm/msgvault/internal/store"
)

var removeAccountYes bool

var removeAccountCmd = &cobra.Command{
	Use:   "remove-account <email>",
	Short: "Remove an account and all its data",
	Long: `Remove an account and all associated messages, labels, and sync data
from the local database. This is irreversible.

The Parquet analytics cache is deleted because it is shared across accounts
and must be rebuilt. Run 'msgvault build-cache' afterward to rebuild it.

Orphaned participants and attachment files on disk are not cleaned up;
use 'msgvault gc' (when available) to reclaim that space.

Examples:
  msgvault remove-account you@gmail.com
  msgvault remove-account you@gmail.com --yes`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := MustBeLocal("remove-account"); err != nil {
			return err
		}

		email := args[0]

		dbPath := cfg.DatabaseDSN()
		s, err := store.Open(dbPath)
		if err != nil {
			return fmt.Errorf("open database: %w", err)
		}
		defer func() { _ = s.Close() }()

		if err := s.InitSchema(); err != nil {
			return fmt.Errorf("init schema: %w", err)
		}

		source, err := s.GetSourceByIdentifier(email)
		if err != nil {
			return fmt.Errorf("look up account: %w", err)
		}
		if source == nil {
			return fmt.Errorf("account %q not found", email)
		}

		activeSync, err := s.GetActiveSync(source.ID)
		if err != nil {
			return fmt.Errorf("check active sync: %w", err)
		}
		if activeSync != nil && !removeAccountYes {
			return fmt.Errorf(
				"account %s has an active sync in progress\n"+
					"Use --yes to force removal", email,
			)
		}

		msgCount, err := s.CountMessagesForSource(source.ID)
		if err != nil {
			return fmt.Errorf("count messages: %w", err)
		}

		fmt.Printf("Account:  %s\n", email)
		fmt.Printf("Type:     %s\n", source.SourceType)
		fmt.Printf("Messages: %s\n", formatCount(msgCount))

		if !removeAccountYes {
			fmt.Print("\nRemove this account and all its data? [y/N] ")
			scanner := bufio.NewScanner(os.Stdin)
			scanner.Scan()
			answer := strings.TrimSpace(strings.ToLower(scanner.Text()))
			if answer != "y" && answer != "yes" {
				fmt.Println("Aborted.")
				return nil
			}
		}

		if err := s.RemoveSource(source.ID); err != nil {
			return fmt.Errorf("remove account: %w", err)
		}

		// Remove OAuth token file
		tokenPath := filepath.Join(
			cfg.TokensDir(),
			sanitizeEmailForToken(email)+".json",
		)
		if err := os.Remove(tokenPath); err != nil && !os.IsNotExist(err) {
			fmt.Fprintf(os.Stderr,
				"Warning: could not remove token file %s: %v\n",
				tokenPath, err,
			)
		}

		// Remove analytics cache (shared across accounts, needs full rebuild)
		analyticsDir := cfg.AnalyticsDir()
		if err := os.RemoveAll(analyticsDir); err != nil {
			fmt.Fprintf(os.Stderr,
				"Warning: could not remove analytics cache %s: %v\n",
				analyticsDir, err,
			)
		}

		fmt.Printf("\nAccount %s removed.\n", email)
		fmt.Println("Run 'msgvault build-cache' to rebuild the analytics cache.")

		return nil
	},
}

// sanitizeEmailForToken matches the sanitization in internal/oauth/oauth.go.
func sanitizeEmailForToken(email string) string {
	safe := strings.ReplaceAll(email, "/", "_")
	safe = strings.ReplaceAll(safe, "\\", "_")
	safe = strings.ReplaceAll(safe, "..", "_")
	return safe
}

func init() {
	removeAccountCmd.Flags().BoolVarP(
		&removeAccountYes, "yes", "y", false,
		"Skip confirmation prompt",
	)
	rootCmd.AddCommand(removeAccountCmd)
}
