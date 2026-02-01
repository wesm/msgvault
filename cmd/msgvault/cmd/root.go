package cmd

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/spf13/cobra"
	"github.com/wesm/msgvault/internal/config"
)

var (
	cfgFile string
	verbose bool
	cfg     *config.Config
	logger  *slog.Logger
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
		if cmd.Name() == "version" || cmd.Name() == "update" {
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

		// Load config
		var err error
		cfg, err = config.Load(cfgFile)
		if err != nil {
			return fmt.Errorf("load config: %w", err)
		}

		return nil
	},
}

func Execute() error {
	return rootCmd.Execute()
}

func init() {
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default: ~/.msgvault/config.toml)")
	rootCmd.PersistentFlags().BoolVarP(&verbose, "verbose", "v", false, "verbose output")
}
