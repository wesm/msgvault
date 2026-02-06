package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/wesm/msgvault/internal/store"
)

var statsCmd = &cobra.Command{
	Use:   "stats",
	Short: "Show database statistics",
	RunE: func(cmd *cobra.Command, args []string) error {
		dbPath := cfg.DatabaseDSN()

		s, err := store.Open(dbPath)
		if err != nil {
			return fmt.Errorf("open database: %w", err)
		}
		defer s.Close()

		stats, err := s.GetStats()
		if err != nil {
			return fmt.Errorf("get stats: %w", err)
		}

		fmt.Printf("Database: %s\n", dbPath)
		fmt.Printf("  Messages:    %d\n", stats.MessageCount)
		fmt.Printf("  Threads:     %d\n", stats.ThreadCount)
		fmt.Printf("  Attachments: %d\n", stats.AttachmentCount)
		fmt.Printf("  Labels:      %d\n", stats.LabelCount)
		fmt.Printf("  Sources:     %d\n", stats.SourceCount)
		fmt.Printf("  Size:        %.2f MB\n", float64(stats.DatabaseSize)/(1024*1024))

		return nil
	},
}

func init() {
	rootCmd.AddCommand(statsCmd)
}
