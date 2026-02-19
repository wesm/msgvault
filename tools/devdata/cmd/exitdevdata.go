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
	path, err := msgvaultPath()
	if err != nil {
		return err
	}
	goldPath, err := datasetPath("gold")
	if err != nil {
		return err
	}

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

	// Save the current symlink target so we can restore it on failure.
	originalTarget, err := os.Readlink(path)
	if err != nil {
		return fmt.Errorf("read symlink target %s: %w", path, err)
	}

	// Lstat guard + remove: re-verify symlink immediately before os.Remove
	// to prevent accidental deletion of a real directory via race condition.
	if info, err := os.Lstat(path); err != nil {
		return fmt.Errorf("lstat %s: %w", path, err)
	} else if info.Mode()&os.ModeSymlink == 0 {
		return fmt.Errorf("%s is no longer a symlink; aborting to prevent accidental data deletion", path)
	}
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("remove symlink %s: %w", path, err)
	}

	// Restore gold to original location
	if err := os.Rename(goldPath, path); err != nil {
		// Try to recreate the symlink with its original target (not
		// necessarily gold) so the user's active dataset is preserved.
		if symlinkErr := os.Symlink(originalTarget, path); symlinkErr != nil {
			return fmt.Errorf("rename %s to %s: %w (recovery also failed: %v)", goldPath, path, err, symlinkErr)
		}
		return fmt.Errorf("rename %s to %s: %w (symlink restored to %s)", goldPath, path, err, originalTarget)
	}

	fmt.Fprintf(os.Stderr, "devdata: exited dev mode: %s restored\n", path)
	return nil
}
