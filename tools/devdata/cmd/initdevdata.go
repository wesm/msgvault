package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/wesm/msgvault/tools/devdata/dataset"
)

var initDevDataCmd = &cobra.Command{
	Use:   "init-dev-data",
	Short: "Initialize dev mode by moving ~/.msgvault to ~/.msgvault-gold",
	Long:  "Safely moves the real ~/.msgvault directory to ~/.msgvault-gold and replaces it with a symlink, so the original data is preserved and msgvault continues to work transparently.",
	RunE:  runInitDevData,
}

func init() {
	rootCmd.AddCommand(initDevDataCmd)
}

func runInitDevData(cmd *cobra.Command, args []string) error {
	path := msgvaultPath()
	goldPath := datasetPath("gold")

	// Check if already a symlink (already initialized)
	isSym, err := dataset.IsSymlink(path)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("%s does not exist", path)
		}
		return fmt.Errorf("check %s: %w", path, err)
	}

	if isSym {
		target, _ := dataset.ReadTarget(path)
		fmt.Fprintf(os.Stderr, "devdata: already in dev mode, linked to %s\n", target)
		return nil
	}

	// Verify gold doesn't already exist
	if dataset.Exists(goldPath) {
		return fmt.Errorf("%s already exists; resolve manually before initializing dev mode", goldPath)
	}

	// Move real directory to gold
	if err := os.Rename(path, goldPath); err != nil {
		return fmt.Errorf("rename %s to %s: %w", path, goldPath, err)
	}

	// Create symlink
	if err := os.Symlink(goldPath, path); err != nil {
		// Try to restore on failure
		_ = os.Rename(goldPath, path)
		return fmt.Errorf("create symlink: %w", err)
	}

	fmt.Fprintf(os.Stderr, "devdata: initialized dev mode: %s -> %s\n", path, goldPath)
	return nil
}
