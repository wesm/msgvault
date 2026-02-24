package cmd

import (
	"fmt"
	"syscall"

	"github.com/spf13/cobra"
	imapclient "github.com/wesm/msgvault/internal/imap"
	"github.com/wesm/msgvault/internal/store"
	"golang.org/x/term"
)

var (
	imapHost     string
	imapPort     int
	imapUsername string
	imapNoTLS    bool
	imapSTARTTLS bool
)

var addIMAPCmd = &cobra.Command{
	Use:   "add-imap",
	Short: "Add an IMAP account",
	Long: `Add an IMAP email account using username/password authentication.

By default, connects using implicit TLS (IMAPS, port 993).
Use --starttls for STARTTLS upgrade on port 143.
Use --no-tls for a plain unencrypted connection (not recommended).

You will be prompted to enter your password interactively.

Examples:
  msgvault add-imap --host imap.example.com --username user@example.com
  msgvault add-imap --host mail.example.com --port 993 --username user@example.com
  msgvault add-imap --host mail.example.com --username user@example.com --starttls
  msgvault add-imap --host mail.example.com --username user@example.com --no-tls`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if imapHost == "" {
			return fmt.Errorf("--host is required")
		}
		if imapUsername == "" {
			return fmt.Errorf("--username is required")
		}

		// Build IMAP config
		imapCfg := &imapclient.Config{
			Host:     imapHost,
			Port:     imapPort,
			TLS:      !imapNoTLS && !imapSTARTTLS,
			STARTTLS: imapSTARTTLS,
			Username: imapUsername,
		}

		// Get password via secure interactive prompt only (never via flag to
		// avoid exposure in shell history and process listings).
		fmt.Printf("Password for %s@%s: ", imapUsername, imapHost)
		raw, err := term.ReadPassword(int(syscall.Stdin))
		fmt.Println()
		if err != nil {
			return fmt.Errorf("read password: %w", err)
		}
		password := string(raw)
		if password == "" {
			return fmt.Errorf("password is required")
		}

		// Test connection
		fmt.Printf("Testing connection to %s...\n", imapCfg.Addr())
		imapClient := imapclient.NewClient(imapCfg, password, imapclient.WithLogger(logger))
		profile, err := imapClient.GetProfile(cmd.Context())
		_ = imapClient.Close()
		if err != nil {
			return fmt.Errorf("connection test failed: %w", err)
		}
		fmt.Printf("Connected successfully as %s\n", profile.EmailAddress)

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

		// Build identifier and save credentials
		identifier := imapCfg.Identifier()

		if err := imapclient.SaveCredentials(cfg.TokensDir(), identifier, password); err != nil {
			return fmt.Errorf("save credentials: %w", err)
		}

		// Create source record
		source, err := s.GetOrCreateSource("imap", identifier)
		if err != nil {
			return fmt.Errorf("create source: %w", err)
		}

		// Store config JSON
		cfgJSON, err := imapCfg.ToJSON()
		if err != nil {
			return fmt.Errorf("serialize config: %w", err)
		}
		if err := s.UpdateSourceSyncConfig(source.ID, cfgJSON); err != nil {
			return fmt.Errorf("store config: %w", err)
		}

		// Set display name from username
		if err := s.UpdateSourceDisplayName(source.ID, imapUsername); err != nil {
			return fmt.Errorf("set display name: %w", err)
		}

		fmt.Printf("\nIMAP account added successfully!\n")
		fmt.Printf("  Identifier: %s\n", identifier)
		fmt.Println()
		fmt.Println("You can now run:")
		fmt.Printf("  msgvault sync-full %s\n", identifier)

		return nil
	},
}

func init() {
	addIMAPCmd.Flags().StringVar(&imapHost, "host", "", "IMAP server hostname (required)")
	addIMAPCmd.Flags().IntVar(&imapPort, "port", 0, "IMAP server port (default: 993 for TLS, 143 otherwise)")
	addIMAPCmd.Flags().StringVar(&imapUsername, "username", "", "IMAP username / email address (required)")
	addIMAPCmd.Flags().BoolVar(&imapNoTLS, "no-tls", false, "Disable TLS (plain connection, not recommended)")
	addIMAPCmd.Flags().BoolVar(&imapSTARTTLS, "starttls", false, "Use STARTTLS instead of implicit TLS")
	rootCmd.AddCommand(addIMAPCmd)
}
