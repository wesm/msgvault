package cmd

import (
	"testing"
)

func TestExportAttachmentsCmd_Registration(t *testing.T) {
	// Verify the command is registered and has expected configuration
	cmd, _, err := rootCmd.Find([]string{"export-attachments"})
	if err != nil {
		t.Fatalf("export-attachments command not found: %v", err)
	}
	if cmd.Use != "export-attachments <message-id>" {
		t.Errorf("Use = %q, want %q", cmd.Use, "export-attachments <message-id>")
	}

	// Verify -o flag exists
	f := cmd.Flags().Lookup("output")
	if f == nil {
		t.Fatal("expected --output flag")
	}
	if f.Shorthand != "o" {
		t.Errorf("output shorthand = %q, want %q", f.Shorthand, "o")
	}
}
