package export

import (
	"testing"

	"github.com/wesm/msgvault/internal/query"
)

func TestAttachments_ShortContentHash(t *testing.T) {
	// ContentHash shorter than 2 chars should not panic
	result := Attachments(
		t.TempDir()+"/test.zip",
		t.TempDir(),
		[]query.AttachmentInfo{
			{Filename: "file.txt", ContentHash: ""},
			{Filename: "file2.txt", ContentHash: "a"},
		},
	)
	if result.Err != nil {
		t.Fatalf("unexpected error: %v", result.Err)
	}
	// Should report errors for both, not panic
	if result.Result == "" {
		t.Error("expected non-empty result with error details")
	}
}
