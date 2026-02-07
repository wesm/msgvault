package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/wesm/msgvault/tools/devdata/dataset"
)

var (
	newDataSrcFlag string
	newDataDstFlag string
	newDataRowFlag int
	newDataDryRun  bool
)

var newDataCmd = &cobra.Command{
	Use:   "new-data",
	Short: "Create a new dataset by copying N messages from a source",
	Long:  "Creates a new dataset directory with a subset of messages from the source database, including all referentially-linked data.",
	RunE:  runNewData,
}

func init() {
	newDataCmd.Flags().StringVar(&newDataSrcFlag, "src", "", "source dataset name (default: active dataset)")
	newDataCmd.Flags().StringVar(&newDataDstFlag, "dst", "", "destination dataset name (required)")
	newDataCmd.Flags().IntVar(&newDataRowFlag, "rows", 0, "number of messages to copy (required)")
	newDataCmd.Flags().BoolVar(&newDataDryRun, "dry-run", false, "show what would be copied without writing")
	_ = newDataCmd.MarkFlagRequired("dst")
	_ = newDataCmd.MarkFlagRequired("rows")
	rootCmd.AddCommand(newDataCmd)
}

func runNewData(cmd *cobra.Command, args []string) error {
	home := homeDir()

	// Resolve source path
	var srcDir string
	if newDataSrcFlag != "" {
		srcDir = datasetPath(newDataSrcFlag)
	} else {
		mvPath := msgvaultPath()
		resolved, err := filepath.EvalSymlinks(mvPath)
		if err != nil {
			return fmt.Errorf("resolve %s: %w", mvPath, err)
		}
		srcDir = resolved
	}

	// Resolve destination path
	dstDir := datasetPath(newDataDstFlag)

	// Canonicalize and validate paths
	srcDir, err := filepath.Abs(filepath.Clean(srcDir))
	if err != nil {
		return fmt.Errorf("resolve source path: %w", err)
	}
	dstDir, err = filepath.Abs(filepath.Clean(dstDir))
	if err != nil {
		return fmt.Errorf("resolve destination path: %w", err)
	}

	// Path traversal protection: verify both paths are within home directory
	absHome, err := filepath.Abs(home)
	if err != nil {
		return fmt.Errorf("resolve home directory: %w", err)
	}
	if !strings.HasPrefix(srcDir, absHome+string(filepath.Separator)) && srcDir != absHome {
		return fmt.Errorf("source path %q is outside home directory", srcDir)
	}
	if !strings.HasPrefix(dstDir, absHome+string(filepath.Separator)) && dstDir != absHome {
		return fmt.Errorf("destination path %q is outside home directory", dstDir)
	}

	// Validate source
	srcDBPath := filepath.Join(srcDir, "msgvault.db")
	if !dataset.Exists(srcDBPath) {
		return fmt.Errorf("source database not found: %s", srcDBPath)
	}

	// Validate destination doesn't exist
	if dataset.Exists(dstDir) {
		return fmt.Errorf("destination already exists: %s", dstDir)
	}

	// Dry run: show what would happen
	if newDataDryRun {
		fmt.Fprintf(os.Stdout, "Source:      %s\n", srcDir)
		fmt.Fprintf(os.Stdout, "Destination: %s\n", dstDir)
		fmt.Fprintf(os.Stdout, "Messages:    %d (most recent by sent_at)\n", newDataRowFlag)
		fmt.Fprintf(os.Stderr, "devdata: dry run — no changes made\n")
		return nil
	}

	// Perform the copy
	fmt.Fprintf(os.Stderr, "devdata: copying %d messages from %s to %s...\n", newDataRowFlag, srcDir, dstDir)

	result, err := dataset.CopySubset(srcDBPath, dstDir, newDataRowFlag)
	if err != nil {
		return fmt.Errorf("copy dataset: %w", err)
	}

	// Copy config.toml if present
	srcConfig := filepath.Join(srcDir, "config.toml")
	dstConfig := filepath.Join(dstDir, "config.toml")
	if err := dataset.CopyFileIfExists(srcConfig, dstConfig); err != nil {
		fmt.Fprintf(os.Stderr, "devdata: warning: could not copy config.toml: %v\n", err)
	}

	// Print summary
	fmt.Fprintf(os.Stderr, "devdata: created dataset %q in %s\n", newDataDstFlag, result.Elapsed.Round(time.Millisecond))
	fmt.Fprintf(os.Stdout, "Messages:      %d\n", result.Messages)
	fmt.Fprintf(os.Stdout, "Conversations: %d\n", result.Conversations)
	fmt.Fprintf(os.Stdout, "Participants:  %d\n", result.Participants)
	fmt.Fprintf(os.Stdout, "Labels:        %d\n", result.Labels)
	fmt.Fprintf(os.Stdout, "Database size: %s\n", formatSize(result.DBSize))

	if int64(newDataRowFlag) > result.Messages {
		fmt.Fprintf(os.Stderr, "devdata: warning: requested %d messages but source only had %d\n", newDataRowFlag, result.Messages)
	}

	return nil
}

func formatSize(bytes int64) string {
	const (
		kb = 1024
		mb = 1024 * kb
		gb = 1024 * mb
	)
	switch {
	case bytes >= gb:
		return fmt.Sprintf("%.1f GB", float64(bytes)/float64(gb))
	case bytes >= mb:
		return fmt.Sprintf("%.1f MB", float64(bytes)/float64(mb))
	case bytes >= kb:
		return fmt.Sprintf("%.1f KB", float64(bytes)/float64(kb))
	default:
		return fmt.Sprintf("%d B", bytes)
	}
}
