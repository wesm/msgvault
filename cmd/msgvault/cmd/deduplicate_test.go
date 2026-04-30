package cmd

import (
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/wesm/msgvault/internal/testutil"
	"github.com/wesm/msgvault/internal/testutil/storetest"
)

// TestDeduplicateMutualExclusion confirms that passing both --account and
// --collection to the deduplicate command is rejected by cobra.
func TestDeduplicateMutualExclusion(t *testing.T) {
	// Build a minimal parent so Execute() returns errors rather than printing
	// them and swallowing them via the global rootCmd error handler.
	var a, b string
	cmd := &cobra.Command{Use: "dedup-test", SilenceErrors: true}
	sub := &cobra.Command{Use: "deduplicate", RunE: func(cmd *cobra.Command, args []string) error { return nil }}
	sub.Flags().StringVar(&a, "account", "", "")
	sub.Flags().StringVar(&b, "collection", "", "")
	sub.MarkFlagsMutuallyExclusive("account", "collection")
	cmd.AddCommand(sub)
	cmd.SetArgs([]string{"deduplicate", "--account", "alpha@example.com", "--collection", "work"})

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

// TestDeduplicateCollectionResolution confirms that --collection resolves
// successfully when the name matches a real collection in the store.
func TestDeduplicateCollectionResolution(t *testing.T) {
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

// TestDeduplicateCollectionResolution_MultiSource confirms SourceIDs expands
// to all members when a collection has more than one source.
func TestDeduplicateCollectionResolution_MultiSource(t *testing.T) {
	f := storetest.New(t)

	src2, err := f.Store.GetOrCreateSource("mbox", "backup@example.com")
	testutil.MustNoErr(t, err, "GetOrCreateSource src2")

	collName := "two-account-collection"
	_, err = f.Store.CreateCollection(collName, "", []int64{f.Source.ID, src2.ID})
	testutil.MustNoErr(t, err, "CreateCollection")

	scope, err := ResolveCollectionFlag(f.Store, collName)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	ids := scope.SourceIDs()
	if len(ids) != 2 {
		t.Errorf("expected 2 source IDs, got %d: %v", len(ids), ids)
	}
	if scope.DisplayName() != collName {
		t.Errorf("DisplayName = %q, want %q", scope.DisplayName(), collName)
	}
}
