package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/spf13/cobra"
	"github.com/wesm/msgvault/internal/query"
	"github.com/wesm/msgvault/internal/store"
)

var listAccountsJSON bool

var listAccountsCmd = &cobra.Command{
	Use:   "list-accounts",
	Short: "List synced email accounts",
	Long: `List all email accounts that have been added to msgvault.

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

		engine := query.NewSQLiteEngine(s.DB())

		accounts, err := engine.ListAccounts(cmd.Context())
		if err != nil {
			return fmt.Errorf("list accounts: %w", err)
		}

		if len(accounts) == 0 {
			fmt.Println("No accounts found. Use 'msgvault add-account <email>' to add one.")
			return nil
		}

		if listAccountsJSON {
			return outputAccountsJSON(accounts)
		}
		outputAccountsTable(accounts)
		return nil
	},
}

func outputAccountsTable(accounts []query.AccountInfo) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tEMAIL\tTYPE\tDISPLAY NAME")
	fmt.Fprintln(w, "──\t─────\t────\t────────────")

	for _, acc := range accounts {
		displayName := acc.DisplayName
		if displayName == "" {
			displayName = "-"
		}
		fmt.Fprintf(w, "%d\t%s\t%s\t%s\n", acc.ID, acc.Identifier, acc.SourceType, displayName)
	}

	w.Flush()
	fmt.Printf("\n%d account(s)\n", len(accounts))
}

func outputAccountsJSON(accounts []query.AccountInfo) error {
	output := make([]map[string]interface{}, len(accounts))
	for i, acc := range accounts {
		output[i] = map[string]interface{}{
			"id":           acc.ID,
			"email":        acc.Identifier,
			"type":         acc.SourceType,
			"display_name": acc.DisplayName,
		}
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(output)
}

func init() {
	rootCmd.AddCommand(listAccountsCmd)
	listAccountsCmd.Flags().BoolVar(&listAccountsJSON, "json", false, "Output as JSON")
}
