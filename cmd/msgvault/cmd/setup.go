package cmd

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
)

var setupCmd = &cobra.Command{
	Use:   "setup",
	Short: "Interactive setup wizard for first-run configuration",
	Long: `Interactive setup wizard to configure msgvault for first use.

This command helps you:
  1. Locate or configure Google OAuth credentials
  2. Create the config.toml file
  3. Optionally configure a remote NAS server for token export

Run this once after installing msgvault to get started quickly.`,
	Args: cobra.NoArgs,
	RunE: runSetup,
}

func init() {
	rootCmd.AddCommand(setupCmd)
}

func runSetup(cmd *cobra.Command, args []string) error {
	reader := bufio.NewReader(os.Stdin)

	fmt.Println("Welcome to msgvault setup!")
	fmt.Println()

	// Ensure home directory exists
	if err := cfg.EnsureHomeDir(); err != nil {
		return fmt.Errorf("create home directory: %w", err)
	}

	// Step 1: Find or prompt for OAuth credentials
	secretsPath, err := setupOAuthSecrets(reader)
	if err != nil {
		return err
	}

	// Step 2: Optionally configure remote NAS
	remoteURL, remoteAPIKey, err := setupRemoteServer(reader)
	if err != nil {
		return err
	}

	// Step 3: Update config
	if secretsPath != "" {
		cfg.OAuth.ClientSecrets = secretsPath
	}
	if remoteURL != "" {
		cfg.Remote.URL = remoteURL
		cfg.Remote.APIKey = remoteAPIKey
	}

	// Only save if we configured something
	if secretsPath != "" || remoteURL != "" {
		if err := cfg.Save(); err != nil {
			return fmt.Errorf("save config: %w", err)
		}
		fmt.Printf("\nConfiguration saved to %s\n", cfg.ConfigFilePath())
	}

	// Print next steps
	fmt.Println()
	fmt.Println("Setup complete! Next steps:")
	fmt.Println()
	fmt.Println("  1. Add a Gmail account:")
	fmt.Println("     msgvault add-account you@gmail.com")
	fmt.Println()
	fmt.Println("  2. Sync your emails:")
	fmt.Println("     msgvault sync-full you@gmail.com")
	fmt.Println()
	if remoteURL != "" {
		fmt.Println("  3. Export token to your NAS (after add-account):")
		fmt.Println("     msgvault export-token you@gmail.com")
		fmt.Println()
	}
	fmt.Println("For more help: msgvault --help")

	return nil
}

func setupOAuthSecrets(reader *bufio.Reader) (string, error) {
	fmt.Println("Step 1: OAuth Credentials")
	fmt.Println("--------------------------")

	// Check if already configured
	if cfg.OAuth.ClientSecrets != "" {
		fmt.Printf("OAuth credentials already configured: %s\n", cfg.OAuth.ClientSecrets)
		if promptYesNo(reader, "Keep existing configuration?") {
			return "", nil
		}
	}

	// Try to find existing client_secret*.json files
	candidates := findClientSecrets()
	if len(candidates) > 0 {
		fmt.Println("\nFound OAuth credentials:")
		for i, path := range candidates {
			fmt.Printf("  [%d] %s\n", i+1, path)
		}
		fmt.Println("  [0] Enter path manually")
		fmt.Println()

		choice := promptChoice(reader, "Select option", 0, len(candidates))
		if choice > 0 {
			return candidates[choice-1], nil
		}
	} else {
		fmt.Println("\nNo client_secret*.json files found in common locations.")
		fmt.Println()
		fmt.Println("To get OAuth credentials:")
		fmt.Println("  1. Go to https://console.cloud.google.com/apis/credentials")
		fmt.Println("  2. Create OAuth client ID (Desktop app)")
		fmt.Println("  3. Download JSON and save as client_secret.json")
		fmt.Println()
	}

	// Prompt for path
	fmt.Print("Enter path to client_secret.json (or press Enter to skip): ")
	path, _ := reader.ReadString('\n')
	path = strings.TrimSpace(path)

	if path == "" {
		fmt.Println("Skipping OAuth configuration. You can add it later to config.toml.")
		return "", nil
	}

	// Expand ~ in path
	if strings.HasPrefix(path, "~/") {
		home, _ := os.UserHomeDir()
		path = filepath.Join(home, path[2:])
	}

	// Verify file exists
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return "", fmt.Errorf("file not found: %s", path)
	}

	fmt.Printf("Using OAuth credentials: %s\n", path)
	return path, nil
}

func setupRemoteServer(reader *bufio.Reader) (string, string, error) {
	fmt.Println()
	fmt.Println("Step 2: Remote NAS Server (Optional)")
	fmt.Println("-------------------------------------")
	fmt.Println("Configure a remote msgvault server to export tokens for headless deployment.")
	fmt.Println()

	// Check if already configured
	if cfg.Remote.URL != "" {
		fmt.Printf("Remote server already configured: %s\n", cfg.Remote.URL)
		if promptYesNo(reader, "Keep existing configuration?") {
			return "", "", nil
		}
	}

	if !promptYesNo(reader, "Configure remote NAS server?") {
		fmt.Println("Skipping remote server configuration.")
		return "", "", nil
	}

	// Get URL
	fmt.Print("Remote URL (e.g., http://nas:8080): ")
	url, _ := reader.ReadString('\n')
	url = strings.TrimSpace(url)

	if url == "" {
		fmt.Println("Skipping remote server configuration.")
		return "", "", nil
	}

	// Get API key
	fmt.Print("API key: ")
	apiKey, _ := reader.ReadString('\n')
	apiKey = strings.TrimSpace(apiKey)

	if apiKey == "" {
		fmt.Println("Warning: No API key provided. You'll need to specify it with --api-key.")
	}

	return url, apiKey, nil
}

func findClientSecrets() []string {
	var found []string
	home, _ := os.UserHomeDir()

	patterns := []string{
		filepath.Join(home, "Downloads", "client_secret*.json"),
		"client_secret*.json",
		filepath.Join(cfg.HomeDir, "client_secret*.json"),
	}

	seen := make(map[string]bool)
	for _, pattern := range patterns {
		matches, _ := filepath.Glob(pattern)
		for _, m := range matches {
			abs, _ := filepath.Abs(m)
			if !seen[abs] {
				seen[abs] = true
				found = append(found, abs)
			}
		}
	}

	return found
}

func promptYesNo(reader *bufio.Reader, prompt string) bool {
	fmt.Printf("%s [Y/n]: ", prompt)
	response, _ := reader.ReadString('\n')
	response = strings.ToLower(strings.TrimSpace(response))
	return response == "" || response == "y" || response == "yes"
}

func promptChoice(reader *bufio.Reader, prompt string, min, max int) int {
	for {
		fmt.Printf("%s [%d-%d]: ", prompt, min, max)
		response, _ := reader.ReadString('\n')
		response = strings.TrimSpace(response)

		var choice int
		if _, err := fmt.Sscanf(response, "%d", &choice); err == nil {
			if choice >= min && choice <= max {
				return choice
			}
		}
		fmt.Printf("Please enter a number between %d and %d\n", min, max)
	}
}
