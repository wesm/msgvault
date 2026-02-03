package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/wesm/msgvault/internal/oauth"
	"github.com/wesm/msgvault/internal/store"
)

var (
	headless bool
)

var addAccountCmd = &cobra.Command{
	Use:   "add-account <email>",
	Short: "Add a Gmail account via OAuth",
	Long: `Add a Gmail account by completing the OAuth2 authorization flow.

By default, opens a browser for authorization. Use --headless to see instructions
for authorizing on headless servers (Google does not support Gmail in device flow).

Example:
  msgvault add-account you@gmail.com
  msgvault add-account you@gmail.com --headless`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		email := args[0]

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

		// Check if already authorized (e.g., token copied from another machine)
		if oauthMgr.HasToken(email) {
			// Still create the source record - needed for headless setup
			// where token was copied but account not yet registered
			_, err = s.GetOrCreateSource("gmail", email)
			if err != nil {
				return fmt.Errorf("create source: %w", err)
			}
			fmt.Printf("Account %s is ready.\n", email)
			fmt.Println("You can now run: msgvault sync-full", email)
			return nil
		}

		// Perform authorization
		fmt.Println("Starting browser authorization...")

		if err := oauthMgr.Authorize(cmd.Context(), email); err != nil {
			return fmt.Errorf("authorization failed: %w", err)
		}

		// Create source record in database
		_, err = s.GetOrCreateSource("gmail", email)
		if err != nil {
			return fmt.Errorf("create source: %w", err)
		}

		fmt.Printf("\nAccount %s authorized successfully!\n", email)
		fmt.Println("You can now run: msgvault sync-full", email)

		return nil
	},
}

func init() {
	addAccountCmd.Flags().BoolVar(&headless, "headless", false, "Show instructions for headless server setup")
	rootCmd.AddCommand(addAccountCmd)
}
