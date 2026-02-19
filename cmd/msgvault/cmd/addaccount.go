package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/wesm/msgvault/internal/oauth"
	"github.com/wesm/msgvault/internal/store"
)

var (
	headless           bool
	accountDisplayName string
	forceReauth        bool
)

var addAccountCmd = &cobra.Command{
	Use:   "add-account <email>",
	Short: "Add a Gmail account via OAuth",
	Long: `Add a Gmail account by completing the OAuth2 authorization flow.

By default, opens a browser for authorization. Use --headless to see instructions
for authorizing on headless servers (Google does not support Gmail in device flow).

If a token already exists, the command skips authorization. Use --force to delete
the existing token and re-authorize (useful when a token has expired or been revoked).

Examples:
  msgvault add-account you@gmail.com
  msgvault add-account you@gmail.com --headless
  msgvault add-account you@gmail.com --force
  msgvault add-account you@gmail.com --display-name "Work Account"`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		email := args[0]

		// Reject incompatible flag combination
		if headless && forceReauth {
			return fmt.Errorf("--headless and --force cannot be used together: --force requires browser-based OAuth which is not available in headless mode")
		}

		// For --headless, just show instructions (no OAuth config needed)
		if headless {
			oauth.PrintHeadlessInstructions(email, cfg.TokensDir())
			return nil
		}

		// Validate config
		if cfg.OAuth.ClientSecrets == "" {
			return errOAuthNotConfigured()
		}

		// Initialize database (in case it's new)
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

		// If --force, delete existing token so we re-authorize
		if forceReauth {
			if oauthMgr.HasToken(email) {
				fmt.Printf("Removing existing token for %s...\n", email)
				if err := oauthMgr.DeleteToken(email); err != nil {
					return fmt.Errorf("delete existing token: %w", err)
				}
			} else {
				fmt.Printf("No existing token found for %s, proceeding with authorization.\n", email)
			}
		}

		// Check if already authorized (e.g., token copied from another machine)
		if oauthMgr.HasToken(email) {
			// Still create the source record - needed for headless setup
			// where token was copied but account not yet registered
			source, err := s.GetOrCreateSource("gmail", email)
			if err != nil {
				return fmt.Errorf("create source: %w", err)
			}
			if accountDisplayName != "" {
				if err := s.UpdateSourceDisplayName(source.ID, accountDisplayName); err != nil {
					return fmt.Errorf("set display name: %w", err)
				}
			}
			fmt.Printf("Account %s is already authorized.\n", email)
			fmt.Println("Next step: msgvault sync-full", email)
			fmt.Println("To re-authorize (e.g., expired token), run: msgvault add-account", email, "--force")
			return nil
		}

		// Perform authorization
		fmt.Println("Starting browser authorization...")

		if err := oauthMgr.Authorize(cmd.Context(), email); err != nil {
			return fmt.Errorf("authorization failed: %w", err)
		}

		// Create source record in database
		source, err := s.GetOrCreateSource("gmail", email)
		if err != nil {
			return fmt.Errorf("create source: %w", err)
		}

		// Set display name if provided
		if accountDisplayName != "" {
			if err := s.UpdateSourceDisplayName(source.ID, accountDisplayName); err != nil {
				return fmt.Errorf("set display name: %w", err)
			}
		}

		fmt.Printf("\nAccount %s authorized successfully!\n", email)
		fmt.Println("You can now run: msgvault sync-full", email)

		return nil
	},
}

func init() {
	addAccountCmd.Flags().BoolVar(&headless, "headless", false, "Show instructions for headless server setup")
	addAccountCmd.Flags().BoolVar(&forceReauth, "force", false, "Delete existing token and re-authorize (use when token is expired or revoked)")
	addAccountCmd.Flags().StringVar(&accountDisplayName, "display-name", "", "Display name for the account (e.g., \"Work\", \"Personal\")")
	rootCmd.AddCommand(addAccountCmd)
}
