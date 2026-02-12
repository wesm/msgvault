package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

var statsCmd = &cobra.Command{
	Use:   "stats",
	Short: "Show database statistics",
	Long: `Show statistics about the email archive.

Uses remote server if [remote].url is configured, otherwise uses local database.
Use --local to force local database.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		s, err := OpenStore()
		if err != nil {
			return fmt.Errorf("open store: %w", err)
		}
		defer s.Close()

		stats, err := s.GetStats()
		if err != nil {
			return fmt.Errorf("get stats: %w", err)
		}

		// Show source indicator
		if IsRemoteMode() {
			fmt.Printf("Remote: %s\n", cfg.Remote.URL)
		} else {
			fmt.Printf("Database: %s\n", cfg.DatabaseDSN())
		}

		fmt.Printf("  Messages:    %d\n", stats.MessageCount)
		fmt.Printf("  Threads:     %d\n", stats.ThreadCount)
		fmt.Printf("  Attachments: %d\n", stats.AttachmentCount)
		fmt.Printf("  Labels:      %d\n", stats.LabelCount)
		fmt.Printf("  Accounts:    %d\n", stats.SourceCount)
		fmt.Printf("  Size:        %.2f MB\n", float64(stats.DatabaseSize)/(1024*1024))

		return nil
	},
}

func init() {
	rootCmd.AddCommand(statsCmd)
}
