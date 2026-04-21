package cmd

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"

	"github.com/spf13/cobra"
	"github.com/wesm/msgvault/internal/store"
)

var collectionsCmd = &cobra.Command{
	Use:   "collections",
	Short: "Manage named groups of accounts",
	Long: `Collections are named groupings of accounts that let you view and
deduplicate across multiple sources as one unified archive.

A default "All" collection is created automatically and includes
every account.`,
}

var collectionsCreateCmd = &cobra.Command{
	Use:   "create <name> --accounts <email1,email2,...>",
	Short: "Create a new collection",
	Args:  cobra.ExactArgs(1),
	RunE:  runCollectionsCreate,
}

var collectionsListCmd = &cobra.Command{
	Use:   "list",
	Short: "List all collections",
	RunE:  runCollectionsList,
}

var collectionsShowCmd = &cobra.Command{
	Use:   "show <name>",
	Short: "Show collection details",
	Args:  cobra.ExactArgs(1),
	RunE:  runCollectionsShow,
}

var collectionsAddCmd = &cobra.Command{
	Use:   "add <name> --accounts <email1,email2,...>",
	Short: "Add accounts to a collection",
	Args:  cobra.ExactArgs(1),
	RunE:  runCollectionsAdd,
}

var collectionsRemoveCmd = &cobra.Command{
	Use:   "remove <name> --accounts <email1,email2,...>",
	Short: "Remove accounts from a collection",
	Args:  cobra.ExactArgs(1),
	RunE:  runCollectionsRemove,
}

var collectionsDeleteCmd = &cobra.Command{
	Use:   "delete <name>",
	Short: "Delete a collection (sources and messages are untouched)",
	Args:  cobra.ExactArgs(1),
	RunE:  runCollectionsDelete,
}

var collectionsAccounts string

func runCollectionsCreate(_ *cobra.Command, args []string) error {
	st, err := openStore()
	if err != nil {
		return err
	}
	defer func() { _ = st.Close() }()

	name := args[0]
	sourceIDs, err := resolveAccountList(st, collectionsAccounts)
	if err != nil {
		return err
	}

	coll, err := st.CreateCollection(name, "", sourceIDs)
	if err != nil {
		return err
	}
	fmt.Printf("Created collection %q with %d source(s).\n",
		coll.Name, len(sourceIDs))
	return nil
}

func runCollectionsList(_ *cobra.Command, _ []string) error {
	st, err := openStore()
	if err != nil {
		return err
	}
	defer func() { _ = st.Close() }()

	collections, err := st.ListCollections()
	if err != nil {
		return err
	}
	if len(collections) == 0 {
		fmt.Println("No collections.")
		return nil
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(w, "NAME\tSOURCES\tMESSAGES")
	for _, c := range collections {
		_, _ = fmt.Fprintf(w, "%s\t%d\t%s\n",
			c.Name, len(c.SourceIDs),
			formatCount(c.MessageCount))
	}
	_ = w.Flush()
	return nil
}

func runCollectionsShow(_ *cobra.Command, args []string) error {
	st, err := openStore()
	if err != nil {
		return err
	}
	defer func() { _ = st.Close() }()

	coll, err := st.GetCollectionByName(args[0])
	if err != nil {
		return err
	}

	fmt.Printf("Collection: %s\n", coll.Name)
	if coll.Description != "" {
		fmt.Printf("Description: %s\n", coll.Description)
	}
	fmt.Printf("Sources: %d\n", len(coll.SourceIDs))
	fmt.Printf("Messages: %s\n", formatCount(coll.MessageCount))
	fmt.Printf("Created: %s\n", coll.CreatedAt.Format("2006-01-02 15:04"))

	if len(coll.SourceIDs) > 0 {
		fmt.Println("\nMember source IDs:", coll.SourceIDs)
	}
	return nil
}

func runCollectionsAdd(_ *cobra.Command, args []string) error {
	st, err := openStore()
	if err != nil {
		return err
	}
	defer func() { _ = st.Close() }()

	sourceIDs, err := resolveAccountList(st, collectionsAccounts)
	if err != nil {
		return err
	}

	if err := st.AddSourcesToCollection(args[0], sourceIDs); err != nil {
		return err
	}
	fmt.Printf("Added %d source(s) to %q.\n", len(sourceIDs), args[0])
	return nil
}

func runCollectionsRemove(_ *cobra.Command, args []string) error {
	st, err := openStore()
	if err != nil {
		return err
	}
	defer func() { _ = st.Close() }()

	sourceIDs, err := resolveAccountList(st, collectionsAccounts)
	if err != nil {
		return err
	}

	if err := st.RemoveSourcesFromCollection(args[0], sourceIDs); err != nil {
		return err
	}
	fmt.Printf("Removed %d source(s) from %q.\n", len(sourceIDs), args[0])
	return nil
}

func runCollectionsDelete(_ *cobra.Command, args []string) error {
	st, err := openStore()
	if err != nil {
		return err
	}
	defer func() { _ = st.Close() }()

	if err := st.DeleteCollection(args[0]); err != nil {
		return err
	}
	fmt.Printf("Deleted collection %q.\n", args[0])
	return nil
}

// openStore opens the database and inits schema.
func openStore() (*store.Store, error) {
	dbPath := cfg.DatabaseDSN()
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		return nil, fmt.Errorf(
			"database not found: %s\nRun 'msgvault init-db' first",
			dbPath,
		)
	}
	st, err := store.Open(dbPath)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}
	if err := st.InitSchema(); err != nil {
		_ = st.Close()
		return nil, fmt.Errorf("init schema: %w", err)
	}
	return st, nil
}

// resolveAccountList resolves a comma-separated list of account
// identifiers to source IDs.
func resolveAccountList(st *store.Store, accounts string) ([]int64, error) {
	if accounts == "" {
		return nil, fmt.Errorf("--accounts is required")
	}
	parts := strings.Split(accounts, ",")
	var ids []int64
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		// Try as numeric ID first
		if id, err := strconv.ParseInt(p, 10, 64); err == nil {
			ids = append(ids, id)
			continue
		}
		// Resolve by identifier
		scope, err := ResolveAccount(st, p)
		if err != nil {
			return nil, err
		}
		ids = append(ids, scope.SourceIDs()...)
	}
	if len(ids) == 0 {
		return nil, fmt.Errorf("no valid accounts in --accounts")
	}
	return ids, nil
}

func init() {
	rootCmd.AddCommand(collectionsCmd)
	collectionsCmd.AddCommand(collectionsCreateCmd)
	collectionsCmd.AddCommand(collectionsListCmd)
	collectionsCmd.AddCommand(collectionsShowCmd)
	collectionsCmd.AddCommand(collectionsAddCmd)
	collectionsCmd.AddCommand(collectionsRemoveCmd)
	collectionsCmd.AddCommand(collectionsDeleteCmd)

	collectionsCreateCmd.Flags().StringVar(&collectionsAccounts,
		"accounts", "", "Comma-separated account emails or source IDs")
	collectionsAddCmd.Flags().StringVar(&collectionsAccounts,
		"accounts", "", "Comma-separated account emails or source IDs")
	collectionsRemoveCmd.Flags().StringVar(&collectionsAccounts,
		"accounts", "", "Comma-separated account emails or source IDs")
}
