package cmd

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/wesm/msgvault/internal/config"
	"github.com/wesm/msgvault/internal/oauth"
	"golang.org/x/oauth2"
)

var (
	cfgFile  string
	homeDir  string
	verbose  bool
	useLocal bool // Force local database even when remote is configured
	cfg      *config.Config
	logger   *slog.Logger
)

var rootCmd = &cobra.Command{
	Use:   "msgvault",
	Short: "Offline email archive tool",
	Long: `msgvault is an offline email archive tool that exports and stores
email data locally with full-text search capabilities.

This is the Go implementation providing sync, search, and TUI functionality
in a single binary.`,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		// Skip config loading for commands that don't need it
		if cmd.Name() == "version" || cmd.Name() == "update" || cmd.Name() == "quickstart" {
			return nil
		}

		// Set up logging
		level := slog.LevelInfo
		if verbose {
			level = slog.LevelDebug
		}
		logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
			Level: level,
		}))

		// Load config (--home is passed through so it influences
		// where config.toml is loaded from, like MSGVAULT_HOME).
		var err error
		cfg, err = config.Load(cfgFile, homeDir)
		if err != nil {
			return fmt.Errorf("load config: %w", err)
		}

		// Ensure home directory exists on first use
		if err := cfg.EnsureHomeDir(); err != nil {
			return fmt.Errorf("create data directory %s: %w", cfg.HomeDir, err)
		}

		return nil
	},
}

// Execute runs the root command with a background context.
// Prefer ExecuteContext for signal-aware execution.
func Execute() error {
	return ExecuteContext(context.Background())
}

// ExecuteContext runs the root command with the given context,
// enabling graceful shutdown when the context is cancelled.
func ExecuteContext(ctx context.Context) error {
	return rootCmd.ExecuteContext(ctx)
}

// oauthSetupHint returns help text for OAuth configuration issues,
// using the actual config file path so it's clear on all platforms.
func oauthSetupHint() string {
	configPath := "<config file>"
	if cfg != nil {
		configPath = cfg.ConfigFilePath()
	}
	return fmt.Sprintf(`
To use msgvault, you need a Google Cloud OAuth credential:
  1. Follow the setup guide: https://msgvault.io/guides/oauth-setup/
  2. Download the client_secret.json file
  3. Create or edit %s:
       [oauth]
       client_secrets = "/path/to/client_secret.json"`, configPath)
}

// errOAuthNotConfigured returns a helpful error when OAuth client secrets are missing.
// It also searches for client_secret*.json files in common locations.
func errOAuthNotConfigured() error {
	// Check common locations for client_secret*.json
	hint := tryFindClientSecrets()
	if hint != "" {
		return fmt.Errorf("OAuth client secrets not configured.%s", hint)
	}
	return fmt.Errorf("OAuth client secrets not configured.%s", oauthSetupHint())
}

// tryFindClientSecrets looks for client_secret*.json in common locations
// and returns a hint if found.
func tryFindClientSecrets() string {
	home, _ := os.UserHomeDir()
	candidates := []string{
		filepath.Join(home, "Downloads", "client_secret*.json"),
		"client_secret*.json",
	}
	if cfg != nil {
		candidates = append(candidates, filepath.Join(cfg.HomeDir, "client_secret*.json"))
	}

	for _, pattern := range candidates {
		matches, _ := filepath.Glob(pattern)
		if len(matches) > 0 {
			configPath := "<config file>"
			if cfg != nil {
				configPath = cfg.ConfigFilePath()
			}
			return fmt.Sprintf(`

Found OAuth credentials at: %s

To use this file, add to %s:
  [oauth]
  client_secrets = %q

Or copy the file to your msgvault home directory:
  cp %q ~/.msgvault/client_secret.json`, matches[0], configPath, matches[0], matches[0])
		}
	}
	return ""
}

// wrapOAuthError wraps an oauth/client-secrets error with setup instructions
// if the root cause is a missing or unreadable secrets file.
func wrapOAuthError(err error) error {
	if errors.Is(err, os.ErrNotExist) || errors.Is(err, os.ErrPermission) {
		return fmt.Errorf("OAuth client secrets file not accessible.%s", oauthSetupHint())
	}
	return err
}

// getTokenSourceWithReauth tries to get a token source for the given email.
// If the token exists but is expired/revoked, it automatically deletes the old
// token and re-initiates the OAuth browser flow.
func getTokenSourceWithReauth(ctx context.Context, oauthMgr *oauth.Manager, email string) (oauth2.TokenSource, error) {
	tokenSource, err := oauthMgr.TokenSource(ctx, email)
	if err == nil {
		return tokenSource, nil
	}

	// No token at all — user needs to run add-account
	if !oauthMgr.HasToken(email) {
		return nil, fmt.Errorf("get token source: %w (run 'add-account %s' first)", err, email)
	}

	// Token exists but failed (expired/revoked) — auto re-authorize
	fmt.Printf("Token for %s is expired or revoked. Re-authorizing...\n", email)

	if delErr := oauthMgr.DeleteToken(email); delErr != nil {
		return nil, fmt.Errorf("delete expired token: %w", delErr)
	}

	if authErr := oauthMgr.Authorize(ctx, email); authErr != nil {
		return nil, fmt.Errorf("re-authorize %s: %w", email, authErr)
	}

	// Retry with the new token
	tokenSource, err = oauthMgr.TokenSource(ctx, email)
	if err != nil {
		return nil, fmt.Errorf("get token source after re-authorization: %w", err)
	}

	return tokenSource, nil
}

func init() {
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default: ~/.msgvault/config.toml)")
	rootCmd.PersistentFlags().StringVar(&homeDir, "home", "", "home directory (overrides MSGVAULT_HOME)")
	rootCmd.PersistentFlags().BoolVarP(&verbose, "verbose", "v", false, "verbose output")
	rootCmd.PersistentFlags().BoolVar(&useLocal, "local", false, "force local database (override remote config)")
}
