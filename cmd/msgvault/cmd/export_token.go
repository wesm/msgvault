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

Examples:
  # Export token to NAS
  msgvault export-token user@gmail.com --to http://nas:8080 --api-key YOUR_KEY

  # With Tailscale
  msgvault export-token user@gmail.com --to http://homebase.tail49367.ts.net:8080 --api-key KEY`,
	Args: cobra.ExactArgs(1),
	RunE: runExportToken,
}

func init() {
	exportTokenCmd.Flags().StringVar(&exportTokenTo, "to", "", "Remote msgvault URL (required)")
	exportTokenCmd.Flags().StringVar(&exportTokenAPIKey, "api-key", "", "API key for remote server (required)")
	exportTokenCmd.MarkFlagRequired("to")
	exportTokenCmd.MarkFlagRequired("api-key")
	rootCmd.AddCommand(exportTokenCmd)
}

func runExportToken(cmd *cobra.Command, args []string) error {
	email := args[0]

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
	fmt.Println("\nNext steps on the remote server:")
	fmt.Printf("  1. Add account to config.toml:\n")
	fmt.Printf("     [[accounts]]\n")
	fmt.Printf("     email = %q\n", email)
	fmt.Printf("     schedule = \"0 2 * * *\"\n")
	fmt.Printf("     enabled = true\n")
	fmt.Printf("\n  2. Restart the container or trigger sync:\n")
	fmt.Printf("     curl -X POST -H 'X-API-Key: ...' %s/api/v1/sync/%s\n", exportTokenTo, email)

	return nil
}
