package cmd

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/charmbracelet/huh"
	"github.com/mattn/go-isatty"
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
You can also pipe a password via stdin for scripting:
  echo "password" | msgvault add-imap --host ... --username ...

Security note: Your password is stored on disk with restricted file
permissions (0600). For stronger security, use an app-specific password
instead of your primary account password.

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
		if imapNoTLS && imapSTARTTLS {
			return fmt.Errorf("--no-tls and --starttls are mutually exclusive")
		}

		// Build IMAP config
		imapCfg := &imapclient.Config{
			Host:     imapHost,
			Port:     imapPort,
			TLS:      !imapNoTLS && !imapSTARTTLS,
			STARTTLS: imapSTARTTLS,
			Username: imapUsername,
		}

		// Read password: prompt renders to stderr (it's UI, not
		// program output) so stdout redirection never captures
		// escape sequences.
		//  - huh masked input when stdin + stderr are terminals
		//  - term.ReadPassword when stdin is a native terminal but
		//    stderr is redirected (doesn't need a TTY for output)
		//  - plain pipe read when stdin is not a terminal
		var (
			password string
			err      error
		)
		prompt := fmt.Sprintf("Password for %s@%s:", imapUsername, imapHost)
		stdinTTY := isatty.IsTerminal(os.Stdin.Fd()) ||
			isatty.IsCygwinTerminal(os.Stdin.Fd())
		stderrTTY := isatty.IsTerminal(os.Stderr.Fd()) ||
			isatty.IsCygwinTerminal(os.Stderr.Fd())

		switch {
		case stdinTTY && stderrTTY:
			password, err = readPasswordInteractive(prompt, os.Stderr)
		case isatty.IsTerminal(os.Stdin.Fd()):
			// Native terminal, stderr redirected: term.ReadPassword
			// suppresses echo without needing a TTY for output.
			fmt.Fprintf(os.Stderr, "%s ", prompt)
			raw, readErr := term.ReadPassword(int(os.Stdin.Fd()))
			fmt.Fprintln(os.Stderr)
			if readErr != nil {
				err = fmt.Errorf("read password: %w", readErr)
			} else if strings.TrimSpace(string(raw)) == "" {
				err = fmt.Errorf("password is required")
			} else {
				password = string(raw)
			}
		case stdinTTY:
			// Cygwin/mintty PTY with stderr redirected:
			// term.ReadPassword doesn't work on Cygwin handles
			// and huh needs a TTY for output.
			return fmt.Errorf("cannot read password: no terminal available for prompt (try piping the password via stdin)")
		default:
			password, err = readPasswordFromPipe(os.Stdin)
		}
		if err != nil {
			return err
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
		fmt.Printf("  Note: Password stored on disk at %s\n", imapclient.CredentialsPath(cfg.TokensDir(), identifier))
		fmt.Println()
		fmt.Println("You can now run:")
		fmt.Printf("  msgvault sync-full %s\n", identifier)

		return nil
	},
}

// readPasswordFromPipe reads a password from a non-terminal reader
// (e.g. piped stdin). Uses only the first line.
func readPasswordFromPipe(r io.Reader) (string, error) {
	scanner := bufio.NewScanner(r)
	if !scanner.Scan() {
		if err := scanner.Err(); err != nil {
			return "", fmt.Errorf("read password: %w", err)
		}
		return "", fmt.Errorf("password is required")
	}
	password := strings.TrimRight(scanner.Text(), "\r\n")
	if strings.TrimSpace(password) == "" {
		return "", fmt.Errorf("password is required")
	}
	return password, nil
}

// readPasswordInteractive prompts for a password using a masked
// input field with asterisk echo. The output writer controls where
// the TUI renders (typically stderr to avoid polluting stdout).
func readPasswordInteractive(prompt string, output io.Writer) (string, error) {
	var password string
	input := huh.NewInput().
		Title(prompt).
		EchoMode(huh.EchoModePassword).
		Value(&password)
	err := huh.NewForm(huh.NewGroup(input)).
		WithOutput(output).
		Run()
	if err != nil {
		return "", fmt.Errorf("read password: %w", err)
	}
	if strings.TrimSpace(password) == "" {
		return "", fmt.Errorf("password is required")
	}
	return password, nil
}

func init() {
	addIMAPCmd.Flags().StringVar(&imapHost, "host", "", "IMAP server hostname (required)")
	addIMAPCmd.Flags().IntVar(&imapPort, "port", 0, "IMAP server port (default: 993 for TLS, 143 otherwise)")
	addIMAPCmd.Flags().StringVar(&imapUsername, "username", "", "IMAP username / email address (required)")
	addIMAPCmd.Flags().BoolVar(&imapNoTLS, "no-tls", false, "Disable TLS (plain connection, not recommended)")
	addIMAPCmd.Flags().BoolVar(&imapSTARTTLS, "starttls", false, "Use STARTTLS instead of implicit TLS")
	rootCmd.AddCommand(addIMAPCmd)
}
