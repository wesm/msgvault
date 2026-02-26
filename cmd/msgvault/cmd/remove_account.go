package cmd

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/wesm/msgvault/internal/oauth"
	"github.com/wesm/msgvault/internal/store"
)

func newRemoveAccountCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "remove-account <email>",
		Short: "Remove an account and all its data",
		Long: `Remove an account and all associated messages, labels, and sync data
from the local database. This is irreversible.

If the same identifier exists for multiple source types (e.g., gmail
and mbox), use --type to specify which one to remove.

The Parquet analytics cache is deleted because it is shared across accounts
and must be rebuilt. Run 'msgvault build-cache' afterward to rebuild it.

Orphaned participants and attachment files on disk are not cleaned up;
use 'msgvault gc' (when available) to reclaim that space.

Examples:
  msgvault remove-account you@gmail.com
  msgvault remove-account you@gmail.com --yes
  msgvault remove-account you@gmail.com --type mbox`,
		Args: cobra.ExactArgs(1),
		RunE: runRemoveAccount,
	}
	cmd.Flags().BoolP("yes", "y", false, "Skip confirmation prompt")
	cmd.Flags().String(
		"type", "",
		"Source type to remove (gmail, mbox, etc.)",
	)
	return cmd
}

func runRemoveAccount(cmd *cobra.Command, args []string) error {
	if err := MustBeLocal("remove-account"); err != nil {
		return err
	}

	yes, err := cmd.Flags().GetBool("yes")
	if err != nil {
		return fmt.Errorf("read --yes flag: %w", err)
	}
	sourceType, err := cmd.Flags().GetString("type")
	if err != nil {
		return fmt.Errorf("read --type flag: %w", err)
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

	source, err := resolveSource(s, email, sourceType)
	if err != nil {
		return err
	}

	activeSync, err := s.GetActiveSync(source.ID)
	if err != nil {
		return fmt.Errorf("check active sync: %w", err)
	}
	if activeSync != nil && !yes {
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

	if !yes {
		fmt.Print("\nRemove this account and all its data? [y/N] ")
		scanner := bufio.NewScanner(os.Stdin)
		if !scanner.Scan() {
			if err := scanner.Err(); err != nil {
				return fmt.Errorf("read confirmation: %w", err)
			}
			return fmt.Errorf(
				"no confirmation input (stdin closed); use --yes",
			)
		}
		answer := strings.TrimSpace(strings.ToLower(scanner.Text()))
		if answer != "y" && answer != "yes" {
			fmt.Println("Aborted.")
			return nil
		}
	}

	if err := s.RemoveSource(source.ID); err != nil {
		return fmt.Errorf("remove account: %w", err)
	}

	// Only remove OAuth token for gmail sources
	if source.SourceType == "gmail" {
		tokenPath := oauth.TokenFilePath(
			cfg.TokensDir(), source.Identifier,
		)
		if err := os.Remove(tokenPath); err != nil && !os.IsNotExist(err) {
			fmt.Fprintf(os.Stderr,
				"Warning: could not remove token file %s: %v\n",
				tokenPath, err,
			)
		}
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
	fmt.Println(
		"Run 'msgvault build-cache' to rebuild the analytics cache.",
	)
	fmt.Println(
		"Note: attachment files on disk were not removed." +
			" Use 'msgvault gc' (when available) to reclaim space.",
	)

	return nil
}

// resolveSource finds the unique source for the given identifier.
// If multiple source types share the identifier, sourceType is
// required to disambiguate.
func resolveSource(
	s *store.Store, identifier, sourceType string,
) (*store.Source, error) {
	sources, err := s.GetSourcesByIdentifier(identifier)
	if err != nil {
		return nil, fmt.Errorf("look up account: %w", err)
	}
	if len(sources) == 0 {
		return nil, fmt.Errorf("account %q not found", identifier)
	}

	if sourceType != "" {
		for _, src := range sources {
			if src.SourceType == sourceType {
				return src, nil
			}
		}
		return nil, fmt.Errorf(
			"account %q with type %q not found",
			identifier, sourceType,
		)
	}

	if len(sources) == 1 {
		return sources[0], nil
	}

	// Multiple matches — require --type to disambiguate
	var types []string
	for _, src := range sources {
		types = append(types, src.SourceType)
	}
	return nil, fmt.Errorf(
		"multiple accounts found for %q (types: %s)\n"+
			"Use --type to specify which one to remove",
		identifier, strings.Join(types, ", "),
	)
}

func init() {
	rootCmd.AddCommand(newRemoveAccountCmd())
}
