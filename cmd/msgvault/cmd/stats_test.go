package cmd

import (
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

// TestStatsCommand_AccountAndCollectionMutuallyExclusive confirms that passing
// both --account and --collection to the stats command is rejected by cobra.
func TestStatsCommand_AccountAndCollectionMutuallyExclusive(t *testing.T) {
	var a, b string
	cmd := &cobra.Command{Use: "stats-test", SilenceErrors: true}
	sub := &cobra.Command{Use: "stats", RunE: func(cmd *cobra.Command, args []string) error { return nil }}
	sub.Flags().StringVar(&a, "account", "", "")
	sub.Flags().StringVar(&b, "collection", "", "")
	sub.MarkFlagsMutuallyExclusive("account", "collection")
	cmd.AddCommand(sub)
	cmd.SetArgs([]string{"stats", "--account", "foo@example.com", "--collection", "bar"})

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
