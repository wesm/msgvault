package export

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/wesm/msgvault/internal/query"
)

func TestAttachments(t *testing.T) {
	tests := []struct {
		name            string
		inputs          []query.AttachmentInfo
		wantErr         bool
		wantInResult    string
		wantAllInResult []string
	}{
		{
			name:         "empty content hash is skipped",
			inputs:       []query.AttachmentInfo{{Filename: "file.txt", ContentHash: ""}},
			wantInResult: "file.txt: missing or invalid content hash",
		},
		{
			name:         "single-char content hash is skipped",
			inputs:       []query.AttachmentInfo{{Filename: "file2.txt", ContentHash: "a"}},
			wantInResult: "file2.txt: missing or invalid content hash",
		},
		{
			name: "mixed short hashes all reported",
			inputs: []query.AttachmentInfo{
				{Filename: "file.txt", ContentHash: ""},
				{Filename: "file2.txt", ContentHash: "a"},
			},
			wantInResult: "No attachments exported",
			wantAllInResult: []string{
				"file.txt: missing or invalid content hash",
				"file2.txt: missing or invalid content hash",
			},
		},
		{
			name:         "nil inputs produces no panic",
			inputs:       nil,
			wantInResult: "No attachments exported",
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

			if tt.wantInResult != "" && !strings.Contains(result.Result, tt.wantInResult) {
				t.Errorf("Attachments() result = %q, want substring %q", result.Result, tt.wantInResult)
			}
			for _, want := range tt.wantAllInResult {
				if !strings.Contains(result.Result, want) {
					t.Errorf("Attachments() result = %q, want substring %q", result.Result, want)
				}
			}
		})
	}
}
