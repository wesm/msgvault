package export

import (
	"archive/zip"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/wesm/msgvault/internal/query"
	"github.com/wesm/msgvault/internal/testutil"
)

// createAttachmentFile creates a file in the content-addressed storage layout
// (root/<hash[:2]>/<hash>) and returns the SHA-256 hex hash of the content.
func createAttachmentFile(t *testing.T, root string, content []byte) string {
	t.Helper()
	hash := fmt.Sprintf("%x", sha256.Sum256(content))
	dir := filepath.Join(root, hash[:2])
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, hash), content, 0o644); err != nil {
		t.Fatal(err)
	}
	return hash
}

func TestFormatExportResult_WriteErrorWithCount(t *testing.T) {
	// Test that WriteError flag causes failure message even when Count > 0
	stats := ExportStats{
		Count:      5,
		Size:       1024,
		WriteError: true,
		Errors:     []string{"zip finalization error: disk full"},
		ZipPath:    "", // Empty because zip was removed
	}

	result := FormatExportResult(stats)

	// Should report failure, not success
	if !strings.Contains(result, "Export failed due to write errors") {
		t.Errorf("expected failure message, got: %s", result)
	}
	if !strings.Contains(result, "Zip file removed") {
		t.Errorf("expected 'Zip file removed', got: %s", result)
	}
	if strings.Contains(result, "Exported 5 attachment") {
		t.Errorf("should not report success count when WriteError is true, got: %s", result)
	}
	if strings.Contains(result, "Saved to:") {
		t.Errorf("should not show 'Saved to:' when WriteError is true, got: %s", result)
	}
}

func TestAttachments(t *testing.T) {
	tests := []struct {
		name           string
		setup          func(t *testing.T, attachDir string) []query.AttachmentInfo
		wantCount      int
		wantSubstrings []string
		wantFiles      []string // files expected in the zip
	}{
		{
			name: "valid file is exported",
			setup: func(t *testing.T, attachDir string) []query.AttachmentInfo {
				hash := createAttachmentFile(t, attachDir, []byte("hello world"))
				return []query.AttachmentInfo{{Filename: "greeting.txt", ContentHash: hash}}
			},
			wantCount:      1,
			wantSubstrings: []string{"Exported 1 attachment(s)"},
			wantFiles:      []string{"greeting.txt"},
		},
		{
			name: "multiple valid files exported",
			setup: func(t *testing.T, attachDir string) []query.AttachmentInfo {
				h1 := createAttachmentFile(t, attachDir, []byte("file one"))
				h2 := createAttachmentFile(t, attachDir, []byte("file two"))
				return []query.AttachmentInfo{
					{Filename: "one.txt", ContentHash: h1},
					{Filename: "two.txt", ContentHash: h2},
				}
			},
			wantCount:      2,
			wantSubstrings: []string{"Exported 2 attachment(s)"},
			wantFiles:      []string{"one.txt", "two.txt"},
		},
		{
			name: "empty content hash is skipped",
			setup: func(_ *testing.T, _ string) []query.AttachmentInfo {
				return []query.AttachmentInfo{{Filename: "file.txt", ContentHash: ""}}
			},
			wantSubstrings: []string{"file.txt: missing or invalid content hash"},
		},
		{
			name: "single-char content hash is skipped",
			setup: func(_ *testing.T, _ string) []query.AttachmentInfo {
				return []query.AttachmentInfo{{Filename: "file2.txt", ContentHash: "a"}}
			},
			wantSubstrings: []string{"file2.txt: missing or invalid content hash"},
		},
		{
			name: "mixed short hashes all reported",
			setup: func(_ *testing.T, _ string) []query.AttachmentInfo {
				return []query.AttachmentInfo{
					{Filename: "file.txt", ContentHash: ""},
					{Filename: "file2.txt", ContentHash: "a"},
				}
			},
			wantSubstrings: []string{
				"No attachments exported",
				"file.txt: missing or invalid content hash",
				"file2.txt: missing or invalid content hash",
			},
		},
		{
			name: "nil inputs produces no panic",
			setup: func(_ *testing.T, _ string) []query.AttachmentInfo {
				return nil
			},
			wantSubstrings: []string{"No attachments exported"},
		},
		{
			name: "mix of valid and invalid attachments",
			setup: func(t *testing.T, attachDir string) []query.AttachmentInfo {
				hash := createAttachmentFile(t, attachDir, []byte("good content"))
				return []query.AttachmentInfo{
					{Filename: "bad.txt", ContentHash: ""},
					{Filename: "good.txt", ContentHash: hash},
				}
			},
			wantCount:      1,
			wantSubstrings: []string{"Exported 1 attachment(s)", "bad.txt: missing or invalid content hash"},
			wantFiles:      []string{"good.txt"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			attachDir := t.TempDir()
			zipPath := filepath.Join(t.TempDir(), "test.zip")

			inputs := tt.setup(t, attachDir)
			stats := Attachments(zipPath, attachDir, inputs)

			if stats.Count != tt.wantCount {
				t.Fatalf("Attachments() count = %d, want %d", stats.Count, tt.wantCount)
			}

			formatted := FormatExportResult(stats)
			testutil.AssertContainsAll(t, formatted, tt.wantSubstrings)

			// Verify exported files exist in the zip
			if len(tt.wantFiles) > 0 {
				zr, err := zip.OpenReader(zipPath)
				if err != nil {
					t.Fatalf("failed to open zip: %v", err)
				}
				defer zr.Close()

				zipEntries := make(map[string]bool)
				for _, f := range zr.File {
					zipEntries[f.Name] = true
				}
				for _, want := range tt.wantFiles {
					if !zipEntries[want] {
						t.Errorf("expected file %q in zip, got entries: %v", want, zipEntries)
					}
				}
			}
		})
	}
}
