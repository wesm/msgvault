package cmd

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/wesm/msgvault/tools/devdata/dataset"
)

var mountDatasetFlag string

var mountDataCmd = &cobra.Command{
	Use:   "mount-data",
	Short: "Point ~/.msgvault at a named dataset",
	Long:  "Switches the ~/.msgvault symlink to point at ~/.msgvault-<name>.",
	RunE:  runMountData,
}

func init() {
	mountDataCmd.Flags().StringVar(&mountDatasetFlag, "dataset", "", "dataset name to mount (required)")
	_ = mountDataCmd.MarkFlagRequired("dataset")
	rootCmd.AddCommand(mountDataCmd)
}

func runMountData(cmd *cobra.Command, args []string) error {
	path := msgvaultPath()
	target := datasetPath(mountDatasetFlag)

	// Verify ~/.msgvault is a symlink
	isSym, err := dataset.IsSymlink(path)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("%s does not exist; run init-dev-data first", path)
		}
		return fmt.Errorf("check %s: %w", path, err)
	}

	if !isSym {
		return fmt.Errorf("%s is not a symlink; run init-dev-data first", path)
	}

	// Verify target dataset exists
	if !dataset.Exists(target) {
		datasets, _ := dataset.ListDatasets(homeDir())
		var names []string
		for _, d := range datasets {
			if !d.IsDefault {
				names = append(names, d.Name)
			}
		}
		msg := fmt.Sprintf("dataset %q does not exist at %s", mountDatasetFlag, target)
		if len(names) > 0 {
			msg += fmt.Sprintf("; available datasets: %s", strings.Join(names, ", "))
		}
		return fmt.Errorf("%s", msg)
	}

	// Check if already mounted
	currentTarget, _ := dataset.ReadTarget(path)
	if currentTarget == target {
		fmt.Fprintf(os.Stderr, "devdata: already mounted %q\n", mountDatasetFlag)
		return nil
	}

	// Replace symlink
	if err := dataset.ReplaceSymlink(path, target); err != nil {
		return fmt.Errorf("mount dataset: %w", err)
	}

	fmt.Fprintf(os.Stderr, "devdata: mounted dataset %q: %s -> %s\n", mountDatasetFlag, path, target)

	if !dataset.HasDatabase(target) {
		fmt.Fprintf(os.Stderr, "devdata: warning: no msgvault.db found in dataset %q\n", mountDatasetFlag)
	}

	return nil
}
