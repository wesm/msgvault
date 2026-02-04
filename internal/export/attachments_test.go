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

func TestPathTraversalInContentHash(t *testing.T) {
	// This test verifies that malicious content hashes with path traversal
	// sequences are rejected and cannot be used to read arbitrary files.

	// Create a "secret" file outside the attachments directory
	secretDir := t.TempDir()
	secretFile := filepath.Join(secretDir, "secret.txt")
	secretContent := []byte("TOP SECRET DATA")
	if err := os.WriteFile(secretFile, secretContent, 0o644); err != nil {
		t.Fatal(err)
	}

	// Create attachments directory (subdirectory to enable traversal)
	attachDir := filepath.Join(t.TempDir(), "attachments")
	if err := os.MkdirAll(attachDir, 0o755); err != nil {
		t.Fatal(err)
	}

	// Craft a malicious content hash that would traverse to the secret file
	// The path construction is: root/<hash[:2]>/<hash>
	// We need to escape from root/../ to reach secretDir
	// Since hash[:2] is used as subdir, we use ".." as the prefix
	maliciousHash := "../" + secretFile

	zipPath := filepath.Join(t.TempDir(), "test.zip")
	inputs := []query.AttachmentInfo{
		{Filename: "stolen.txt", ContentHash: maliciousHash},
	}

	stats := Attachments(zipPath, attachDir, inputs)

	// The export should fail - path traversal should be detected and rejected
	if stats.Count > 0 {
		t.Errorf("path traversal attack succeeded: exported %d files", stats.Count)
	}

	// Should report an error about the invalid content hash
	found := false
	for _, errMsg := range stats.Errors {
		if strings.Contains(errMsg, "invalid content hash") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected 'invalid content hash' error, got errors: %v", stats.Errors)
	}
}

func TestContentHashValidation(t *testing.T) {
	tests := []struct {
		name    string
		hash    string
		wantErr bool
	}{
		{"valid sha256 lowercase", "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2", false},
		{"valid sha256 uppercase", "A1B2C3D4E5F6A1B2C3D4E5F6A1B2C3D4E5F6A1B2C3D4E5F6A1B2C3D4E5F6A1B2", false},
		{"valid sha256 mixed case", "A1b2C3d4E5f6A1b2C3d4E5f6A1b2C3d4E5f6A1b2C3d4E5f6A1b2C3d4E5f6A1b2", false},
		{"path traversal ../", "../../../etc/passwd", true},
		{"path traversal ..\\", "..\\..\\windows\\system32", true},
		{"contains slash", "abc/def", true},
		{"contains backslash", "abc\\def", true},
		{"too short", "abc123", true},
		{"too long", "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2extra", true},
		{"non-hex characters", "g1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2", true},
		{"empty string", "", true},
		{"null bytes", "a1b2c3d4\x00e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateContentHash(tt.hash)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateContentHash(%q) error = %v, wantErr %v", tt.hash, err, tt.wantErr)
			}
		})
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
			wantSubstrings: []string{"file.txt: invalid content hash"},
		},
		{
			name: "single-char content hash is skipped",
			setup: func(_ *testing.T, _ string) []query.AttachmentInfo {
				return []query.AttachmentInfo{{Filename: "file2.txt", ContentHash: "a"}}
			},
			wantSubstrings: []string{"file2.txt: invalid content hash"},
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
				"file.txt: invalid content hash",
				"file2.txt: invalid content hash",
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
			wantSubstrings: []string{"Exported 1 attachment(s)", "bad.txt: invalid content hash"},
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
