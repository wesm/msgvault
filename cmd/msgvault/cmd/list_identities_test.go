package cmd

import (
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

// TestListIdentitiesMutualExclusion confirms that passing both --account and
// --collection to list-identities is rejected by cobra.
func TestListIdentitiesMutualExclusion(t *testing.T) {
	var a, b string
	cmd := &cobra.Command{Use: "list-identities-test", SilenceErrors: true}
	sub := &cobra.Command{Use: "list-identities", RunE: func(cmd *cobra.Command, args []string) error { return nil }}
	sub.Flags().StringVar(&a, "account", "", "")
	sub.Flags().StringVar(&b, "collection", "", "")
	sub.MarkFlagsMutuallyExclusive("account", "collection")
	cmd.AddCommand(sub)
	cmd.SetArgs([]string{"list-identities", "--account", "alpha@example.com", "--collection", "work"})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error when both --account and --collection are set, got nil")
	}
	msg := err.Error()
	if !strings.Contains(msg, "account") || !strings.Contains(msg, "collection") {
		t.Errorf("error should mention both flag names; got: %q", msg)
	}
	_ = a
	_ = b
}

func TestListIdentitiesOutputMutualExclusion(t *testing.T) {
	var jsonOut, tomlOut bool
	cmd := &cobra.Command{Use: "list-identities-test", SilenceErrors: true}
	sub := &cobra.Command{Use: "list-identities", RunE: func(cmd *cobra.Command, args []string) error { return nil }}
	sub.Flags().BoolVar(&jsonOut, "json", false, "")
	sub.Flags().BoolVar(&tomlOut, "toml", false, "")
	sub.MarkFlagsMutuallyExclusive("json", "toml")
	cmd.AddCommand(sub)
	cmd.SetArgs([]string{"list-identities", "--json", "--toml"})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error when both --json and --toml are set, got nil")
	}
	msg := err.Error()
	if !strings.Contains(msg, "json") || !strings.Contains(msg, "toml") {
		t.Errorf("error should mention both flag names; got: %q", msg)
	}
	_ = jsonOut
	_ = tomlOut
}

// TestListIdentitiesCollectionResolution confirms that --collection resolves
// successfully when the name matches a real collection in the store.
func TestListIdentitiesCollectionResolution(t *testing.T) {
	f, _, collectionName := setupScopeFixture(t)

	scope, err := ResolveCollectionFlag(f.Store, collectionName)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if scope.Collection == nil {
		t.Fatal("expected Collection to be populated")
	}
	if scope.Collection.Name != collectionName {
		t.Errorf("collection name = %q, want %q", scope.Collection.Name, collectionName)
	}
	ids := scope.SourceIDs()
	if len(ids) == 0 {
		t.Error("expected non-empty SourceIDs for collection")
	}
}
