package cmd

import (
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/spf13/cobra"
	"github.com/wesm/msgvault/tools/devdata/dataset"
)

var listCmd = &cobra.Command{
	Use:   "list",
	Short: "List available datasets",
	Long:  "Shows all available msgvault datasets with their status, size, and which one is currently active.",
	RunE:  runList,
}

func init() {
	rootCmd.AddCommand(listCmd)
}

func runList(cmd *cobra.Command, args []string) error {
	path := msgvaultPath()

	// Show current symlink status
	isSym, err := dataset.IsSymlink(path)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Fprintf(os.Stderr, "devdata: %s does not exist\n", path)
			return nil
		}
		return fmt.Errorf("check %s: %w", path, err)
	}

	if isSym {
		target, _ := dataset.ReadTarget(path)
		fmt.Fprintf(os.Stderr, "devdata: dev mode active (%s -> %s)\n", path, target)
	} else {
		fmt.Fprintf(os.Stderr, "devdata: dev mode not active (%s is a real directory)\n", path)
	}

	datasets, err := dataset.ListDatasets(homeDir())
	if err != nil {
		return fmt.Errorf("list datasets: %w", err)
	}

	if len(datasets) == 0 {
		fmt.Fprintf(os.Stderr, "devdata: no datasets found\n")
		return nil
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "ACTIVE\tNAME\tDB SIZE\tPATH")

	for _, d := range datasets {
		active := " "
		if d.Active {
			active = "*"
		}

		size := "-"
		if d.HasDB {
			size = formatSize(d.DBSize)
		}

		fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", active, d.Name, size, d.Path)
	}

	return w.Flush()
}
