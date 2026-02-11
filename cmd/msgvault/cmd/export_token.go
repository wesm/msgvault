package cmd

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
)

var (
	exportTokenTo     string
	exportTokenAPIKey string
)

var exportTokenCmd = &cobra.Command{
	Use:   "export-token <email>",
	Short: "Export OAuth token to a remote msgvault instance",
	Long: `Export an OAuth token to a remote msgvault server for headless deployment.

This command reads your local token and uploads it to a remote msgvault
instance via the API. Use this to set up msgvault on a NAS or server
without a browser.

Environment variables:
  MSGVAULT_REMOTE_URL      Remote server URL (alternative to --to)
  MSGVAULT_REMOTE_API_KEY  API key (alternative to --api-key)

Examples:
  # Export token to NAS
  msgvault export-token user@gmail.com --to http://nas:8080 --api-key YOUR_KEY

  # Using environment variables
  export MSGVAULT_REMOTE_URL=http://nas:8080
  export MSGVAULT_REMOTE_API_KEY=your-key
  msgvault export-token user@gmail.com

  # With Tailscale
  msgvault export-token user@gmail.com --to http://homebase.tail49367.ts.net:8080 --api-key KEY`,
	Args: cobra.ExactArgs(1),
	RunE: runExportToken,
}

func init() {
	exportTokenCmd.Flags().StringVar(&exportTokenTo, "to", "", "Remote msgvault URL (or MSGVAULT_REMOTE_URL env var)")
	exportTokenCmd.Flags().StringVar(&exportTokenAPIKey, "api-key", "", "API key (or MSGVAULT_REMOTE_API_KEY env var)")
	rootCmd.AddCommand(exportTokenCmd)
}

func runExportToken(cmd *cobra.Command, args []string) error {
	email := args[0]

	// Resolution order: flag > env var > config file
	if exportTokenTo == "" {
		exportTokenTo = os.Getenv("MSGVAULT_REMOTE_URL")
	}
	if exportTokenTo == "" {
		exportTokenTo = cfg.Remote.URL
	}

	if exportTokenAPIKey == "" {
		exportTokenAPIKey = os.Getenv("MSGVAULT_REMOTE_API_KEY")
	}
	if exportTokenAPIKey == "" {
		exportTokenAPIKey = cfg.Remote.APIKey
	}

	// Validate required values
	if exportTokenTo == "" {
		return fmt.Errorf("remote URL required: use --to flag, MSGVAULT_REMOTE_URL env var, or [remote] url in config.toml")
	}
	if exportTokenAPIKey == "" {
		return fmt.Errorf("API key required: use --api-key flag, MSGVAULT_REMOTE_API_KEY env var, or [remote] api_key in config.toml")
	}

	// Validate email format
	if !strings.Contains(email, "@") {
		return fmt.Errorf("invalid email format: %s", email)
	}

	// Find token file
	tokensDir := cfg.TokensDir()
	tokenPath := filepath.Join(tokensDir, email+".json")

	// Check if token exists
	if _, err := os.Stat(tokenPath); os.IsNotExist(err) {
		return fmt.Errorf("no token found for %s\n\nRun 'msgvault add-account %s' first to authenticate", email, email)
	}

	// Read token file
	tokenData, err := os.ReadFile(tokenPath)
	if err != nil {
		return fmt.Errorf("failed to read token: %w", err)
	}

	// Build request URL (escape email for path safety)
	reqURL := strings.TrimSuffix(exportTokenTo, "/") + "/api/v1/auth/token/" + url.PathEscape(email)

	// Create request
	req, err := http.NewRequest("POST", reqURL, bytes.NewReader(tokenData))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-API-Key", exportTokenAPIKey)

	// Send request
	fmt.Printf("Uploading token to %s...\n", exportTokenTo)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to connect to remote server: %w", err)
	}
	defer resp.Body.Close()

	// Read response
	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("upload failed (HTTP %d): %s", resp.StatusCode, string(body))
	}

	fmt.Printf("Token uploaded successfully for %s\n", email)

	// Add account to remote config via API
	fmt.Printf("Adding account to remote config...\n")
	accountURL := strings.TrimSuffix(exportTokenTo, "/") + "/api/v1/accounts"
	accountBody := fmt.Sprintf(`{"email":%q,"schedule":"0 2 * * *","enabled":true}`, email)

	accountReq, err := http.NewRequest("POST", accountURL, strings.NewReader(accountBody))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Warning: Could not create account request: %v\n", err)
	} else {
		accountReq.Header.Set("Content-Type", "application/json")
		accountReq.Header.Set("X-API-Key", exportTokenAPIKey)

		accountResp, err := http.DefaultClient.Do(accountReq)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Warning: Could not add account to remote config: %v\n", err)
		} else {
			accountRespBody, _ := io.ReadAll(accountResp.Body)
			accountResp.Body.Close()

			if accountResp.StatusCode == http.StatusCreated {
				fmt.Printf("Account added to remote config\n")
			} else if accountResp.StatusCode == http.StatusOK {
				fmt.Printf("Account already configured on remote\n")
			} else {
				fmt.Fprintf(os.Stderr, "Warning: Could not add account (HTTP %d): %s\n", accountResp.StatusCode, string(accountRespBody))
			}
		}
	}

	// Save remote config for future use (if not already saved)
	if cfg.Remote.URL != exportTokenTo || cfg.Remote.APIKey != exportTokenAPIKey {
		cfg.Remote.URL = exportTokenTo
		cfg.Remote.APIKey = exportTokenAPIKey
		if err := cfg.Save(); err != nil {
			fmt.Fprintf(os.Stderr, "Note: Could not save remote config: %v\n", err)
		} else {
			fmt.Printf("Remote server saved to %s (future exports won't need --to/--api-key)\n", cfg.ConfigFilePath())
		}
	}

	fmt.Println("\nSetup complete! The remote server will sync daily at 2am.")
	fmt.Printf("To trigger an immediate sync:\n")
	fmt.Printf("  curl -X POST -H 'X-API-Key: ...' %s/api/v1/sync/%s\n", exportTokenTo, email)

	return nil
}
