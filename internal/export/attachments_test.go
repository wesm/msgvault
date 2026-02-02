package export

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/wesm/msgvault/internal/query"
)

func assertContainsSubstrings(t *testing.T, got string, subs []string) {
	t.Helper()
	for _, sub := range subs {
		if !strings.Contains(got, sub) {
			t.Errorf("Result missing expected substring: %q\n  got: %q", sub, got)
		}
	}
}

func TestAttachments(t *testing.T) {
	tests := []struct {
		name           string
		inputs         []query.AttachmentInfo
		wantErr        bool
		wantSubstrings []string
	}{
		{
			name:           "empty content hash is skipped",
			inputs:         []query.AttachmentInfo{{Filename: "file.txt", ContentHash: ""}},
			wantSubstrings: []string{"file.txt: missing or invalid content hash"},
		},
		{
			name:           "single-char content hash is skipped",
			inputs:         []query.AttachmentInfo{{Filename: "file2.txt", ContentHash: "a"}},
			wantSubstrings: []string{"file2.txt: missing or invalid content hash"},
		},
		{
			name: "mixed short hashes all reported",
			inputs: []query.AttachmentInfo{
				{Filename: "file.txt", ContentHash: ""},
				{Filename: "file2.txt", ContentHash: "a"},
			},
			wantSubstrings: []string{
				"No attachments exported",
				"file.txt: missing or invalid content hash",
				"file2.txt: missing or invalid content hash",
			},
		},
		{
			name:           "nil inputs produces no panic",
			inputs:         nil,
			wantSubstrings: []string{"No attachments exported"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			zipPath := filepath.Join(t.TempDir(), "test.zip")
			outDir := t.TempDir()

			result := Attachments(zipPath, outDir, tt.inputs)

			if (result.Err != nil) != tt.wantErr {
				t.Fatalf("Attachments() error = %v, wantErr %v", result.Err, tt.wantErr)
			}

			assertContainsSubstrings(t, result.Result, tt.wantSubstrings)
		})
	}
}
