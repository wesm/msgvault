package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
	"github.com/wesm/msgvault/internal/store"
)

var listAccountsJSON bool

var listAccountsCmd = &cobra.Command{
	Use:   "list-accounts",
	Short: "List synced email accounts",
	Long: `List all email accounts that have been added to msgvault.

Shows account email, message count, and last sync time.

Examples:
  msgvault list-accounts
  msgvault list-accounts --json`,
	RunE: func(cmd *cobra.Command, args []string) error {
		dbPath := cfg.DatabasePath()
		s, err := store.Open(dbPath)
		if err != nil {
			return fmt.Errorf("open database: %w", err)
		}
		defer s.Close()

		sources, err := s.ListSources()
		if err != nil {
			return fmt.Errorf("list accounts: %w", err)
		}

		if len(sources) == 0 {
			fmt.Println("No accounts found. Use 'msgvault add-account <email>' to add one.")
			return nil
		}

		// Gather stats for each account
		stats := make([]accountStats, len(sources))
		for i, src := range sources {
			count, err := s.CountMessagesForSource(src.ID)
			if err != nil {
				return fmt.Errorf("count messages for %s: %w", src.Identifier, err)
			}

			var lastSync *time.Time
			if src.LastSyncAt.Valid {
				lastSync = &src.LastSyncAt.Time
			}

			displayName := ""
			if src.DisplayName.Valid {
				displayName = src.DisplayName.String
			}

			stats[i] = accountStats{
				ID:           src.ID,
				Email:        src.Identifier,
				Type:         src.SourceType,
				DisplayName:  displayName,
				MessageCount: count,
				LastSync:     lastSync,
			}
		}

		if listAccountsJSON {
			return outputAccountsJSON(stats)
		}
		outputAccountsTable(stats)
		return nil
	},
}

func outputAccountsTable(stats []accountStats) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tACCOUNT\tTYPE\tDISPLAY NAME\tMESSAGES\tLAST SYNC")

	for _, s := range stats {
		displayName := s.DisplayName
		if displayName == "" {
			displayName = "-"
		}
		lastSync := "-"
		if s.LastSync != nil && !s.LastSync.IsZero() {
			lastSync = s.LastSync.Format("2006-01-02 15:04")
		}
		fmt.Fprintf(w, "%d\t%s\t%s\t%s\t%s\t%s\n", s.ID, s.Email, s.Type, displayName, formatCount(s.MessageCount), lastSync)
	}

	w.Flush()
}

func outputAccountsJSON(stats []accountStats) error {
	output := make([]map[string]interface{}, len(stats))
	for i, s := range stats {
		entry := map[string]interface{}{
			"id":            s.ID,
			"email":         s.Email,
			"type":          s.Type,
			"display_name":  s.DisplayName,
			"message_count": s.MessageCount,
		}
		if s.LastSync != nil && !s.LastSync.IsZero() {
			entry["last_sync"] = s.LastSync.Format(time.RFC3339)
		} else {
			entry["last_sync"] = nil
		}
		output[i] = entry
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(output)
}

// formatCount formats a number with thousand separators.
func formatCount(n int64) string {
	if n < 1000 {
		return fmt.Sprintf("%d", n)
	}

	// Format with commas
	s := fmt.Sprintf("%d", n)
	result := make([]byte, 0, len(s)+(len(s)-1)/3)
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			result = append(result, ',')
		}
		result = append(result, byte(c))
	}
	return string(result)
}

type accountStats struct {
	ID           int64
	Email        string
	Type         string
	DisplayName  string
	MessageCount int64
	LastSync     *time.Time
}

func init() {
	rootCmd.AddCommand(listAccountsCmd)
	listAccountsCmd.Flags().BoolVar(&listAccountsJSON, "json", false, "Output as JSON")
}
