package cmd

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
)

var homeFlag string

var rootCmd = &cobra.Command{
	Use:   "devdata",
	Short: "Manage msgvault datasets",
	Long:  "devdata manages multiple msgvault data directories via symlinks, allowing developers to switch between datasets and create expendable subsets for development.",
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		if envHome := os.Getenv("MSGVAULT_HOME"); envHome != "" && !cmd.Flags().Changed("home") {
			fmt.Fprintf(os.Stderr, "devdata: warning: MSGVAULT_HOME is set to %q; symlink operations on ~/.msgvault will not affect msgvault's data directory.\n", envHome)
		}
		return nil
	},
}

func init() {
	rootCmd.PersistentFlags().StringVar(&homeFlag, "home", "", "override home directory (default: user home)")
}

// Execute runs the root command.
func Execute() error {
	return rootCmd.Execute()
}

// homeDir returns the resolved home directory.
func homeDir() string {
	if homeFlag != "" {
		return homeFlag
	}
	h, err := os.UserHomeDir()
	if err != nil {
		fmt.Fprintf(os.Stderr, "devdata: cannot determine home directory: %v\n", err)
		os.Exit(1)
	}
	return h
}

// msgvaultPath returns the path to ~/.msgvault.
func msgvaultPath() string {
	return filepath.Join(homeDir(), ".msgvault")
}

// datasetPath returns the path to ~/.msgvault-<name>.
func datasetPath(name string) string {
	return filepath.Join(homeDir(), ".msgvault-"+name)
}
