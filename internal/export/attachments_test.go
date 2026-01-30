package export

import (
	"testing"

	"github.com/wesm/msgvault/internal/query"
)

// tempPaths returns a zip file path and output directory using t.TempDir().
func tempPaths(t *testing.T) (zipPath, outDir string) {
	t.Helper()
	return t.TempDir() + "/test.zip", t.TempDir()
}

// att constructs a minimal AttachmentInfo for test fixtures.
func att(filename, hash string) query.AttachmentInfo {
	return query.AttachmentInfo{Filename: filename, ContentHash: hash}
}

func TestAttachments_ShortContentHash(t *testing.T) {
	// ContentHash shorter than 2 chars should not panic
	zipPath, outDir := tempPaths(t)
	result := Attachments(zipPath, outDir, []query.AttachmentInfo{
		att("file.txt", ""),
		att("file2.txt", "a"),
	})
	if result.Err != nil {
		t.Fatalf("unexpected error: %v", result.Err)
	}
	// Should report errors for both, not panic
	if result.Result == "" {
		t.Error("expected non-empty result with error details")
	}
}
