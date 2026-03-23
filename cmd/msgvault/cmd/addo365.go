package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
	imapclient "github.com/wesm/msgvault/internal/imap"
	"github.com/wesm/msgvault/internal/microsoft"
	"github.com/wesm/msgvault/internal/store"
)

var o365TenantID string

var addO365Cmd = &cobra.Command{
	Use:   "add-o365 <email>",
	Short: "Add a Microsoft 365 account via OAuth",
	Long: `Add a Microsoft 365 / Outlook.com email account using OAuth2 authentication.

This opens a browser for Microsoft authorization, then configures IMAP access
to outlook.office365.com automatically using the XOAUTH2 SASL mechanism.

Requires a [microsoft] section in config.toml with your Azure AD app's client_id.
See the docs for Azure AD app registration setup.

Examples:
  msgvault add-o365 user@outlook.com
  msgvault add-o365 user@company.com --tenant my-tenant-id`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		email := args[0]

		if cfg.Microsoft.ClientID == "" {
			return fmt.Errorf("Microsoft OAuth not configured.\n\n" +
				"Add to your config.toml:\n\n" +
				"  [microsoft]\n" +
				"  client_id = \"your-azure-app-client-id\"\n\n" +
				"See docs for Azure AD app registration setup.")
		}

		tenantID := cfg.Microsoft.EffectiveTenantID()
		if o365TenantID != "" {
			tenantID = o365TenantID
		}

		msMgr := microsoft.NewManager(
			cfg.Microsoft.ClientID,
			tenantID,
			cfg.TokensDir(),
			logger,
		)

		fmt.Printf("Authorizing %s with Microsoft...\n", email)
		if err := msMgr.Authorize(cmd.Context(), email); err != nil {
			return fmt.Errorf("authorization failed: %w", err)
		}

		// Auto-configure IMAP for outlook.office365.com
		imapCfg := &imapclient.Config{
			Host:       "outlook.office365.com",
			Port:       993,
			TLS:        true,
			Username:   email,
			AuthMethod: imapclient.AuthXOAuth2,
		}

		dbPath := cfg.DatabaseDSN()
		s, err := store.Open(dbPath)
		if err != nil {
			return fmt.Errorf("open database: %w", err)
		}
		defer s.Close()

		if err := s.InitSchema(); err != nil {
			return fmt.Errorf("init schema: %w", err)
		}

		identifier := imapCfg.Identifier()
		source, err := s.GetOrCreateSource("imap", identifier)
		if err != nil {
			return fmt.Errorf("create source: %w", err)
		}

		cfgJSON, err := imapCfg.ToJSON()
		if err != nil {
			return fmt.Errorf("serialize config: %w", err)
		}
		if err := s.UpdateSourceSyncConfig(source.ID, cfgJSON); err != nil {
			return fmt.Errorf("store config: %w", err)
		}
		if err := s.UpdateSourceDisplayName(source.ID, email); err != nil {
			return fmt.Errorf("set display name: %w", err)
		}

		fmt.Printf("\nMicrosoft 365 account added successfully!\n")
		fmt.Printf("  Email:      %s\n", email)
		fmt.Printf("  Identifier: %s\n", identifier)
		fmt.Println()
		fmt.Println("You can now run:")
		fmt.Printf("  msgvault sync-full %s\n", identifier)

		return nil
	},
}

func init() {
	addO365Cmd.Flags().StringVar(&o365TenantID, "tenant", "",
		"Azure AD tenant ID (default: \"common\" for multi-tenant)")
	rootCmd.AddCommand(addO365Cmd)
}
