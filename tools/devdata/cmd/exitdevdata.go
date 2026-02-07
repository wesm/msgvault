package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/wesm/msgvault/tools/devdata/dataset"
)

var exitDevDataCmd = &cobra.Command{
	Use:   "exit-dev-data",
	Short: "Exit dev mode and restore ~/.msgvault from ~/.msgvault-gold",
	Long:  "Undoes init-dev-data: removes the ~/.msgvault symlink and renames ~/.msgvault-gold back to ~/.msgvault.",
	RunE:  runExitDevData,
}

func init() {
	rootCmd.AddCommand(exitDevDataCmd)
}

func runExitDevData(cmd *cobra.Command, args []string) error {
	path := msgvaultPath()
	goldPath := datasetPath("gold")

	// Check if in dev mode
	isSym, err := dataset.IsSymlink(path)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Fprintf(os.Stderr, "devdata: not in dev mode (%s does not exist)\n", path)
			return nil
		}
		return fmt.Errorf("check %s: %w", path, err)
	}

	if !isSym {
		fmt.Fprintf(os.Stderr, "devdata: not in dev mode (no symlink at %s)\n", path)
		return nil
	}

	// Verify gold copy exists
	if !dataset.Exists(goldPath) {
		return fmt.Errorf("%s not found; cannot restore without gold copy", goldPath)
	}

	// Re-verify symlink immediately before removal (Lstat guard)
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("lstat %s: %w", path, err)
	}
	if info.Mode()&os.ModeSymlink == 0 {
		return fmt.Errorf("%s is no longer a symlink; aborting to prevent accidental data deletion", path)
	}

	// Remove symlink
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("remove symlink %s: %w", path, err)
	}

	// Restore gold to original location
	if err := os.Rename(goldPath, path); err != nil {
		// Try to recreate symlink on failure
		_ = os.Symlink(goldPath, path)
		return fmt.Errorf("rename %s to %s: %w", goldPath, path, err)
	}

	fmt.Fprintf(os.Stderr, "devdata: exited dev mode: %s restored\n", path)
	return nil
}
