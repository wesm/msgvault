package cmd

import (
	"encoding/base64"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"
)

// setupTestAttachment creates a temp dir with an attachment file stored using
// the content-addressed layout (hash[:2]/hash). Returns the attachments dir,
// the content hash, the file data, and a cleanup function.
func setupTestAttachment(t *testing.T) (string, string, []byte, func()) {
	t.Helper()

	tmpDir, err := os.MkdirTemp("", "msgvault-export-att-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}

	contentHash := "61ccf192b5bd358738802dc2676d3ceab856f47d26dd29681ac3d335bfd5bbd0"
	data := []byte("test attachment content")

	subDir := filepath.Join(tmpDir, contentHash[:2])
	if err := os.MkdirAll(subDir, 0755); err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("create subdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(subDir, contentHash), data, 0600); err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("write test file: %v", err)
	}

	return tmpDir, contentHash, data, func() { os.RemoveAll(tmpDir) }
}

func TestExportAttachment_BinaryToFile(t *testing.T) {
	attDir, contentHash, wantData, cleanup := setupTestAttachment(t)
	defer cleanup()

	outFile := filepath.Join(attDir, "output.bin")
	storagePath := filepath.Join(attDir, contentHash[:2], contentHash)

	// Reset global flag state
	exportAttachmentOutput = outFile
	exportAttachmentJSON = false
	exportAttachmentBase64 = false
	defer func() { exportAttachmentOutput = "" }()

	if err := exportAttachmentBinary(storagePath, contentHash); err != nil {
		t.Fatalf("exportAttachmentBinary: %v", err)
	}

	got, err := os.ReadFile(outFile)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	if string(got) != string(wantData) {
		t.Errorf("output = %q, want %q", got, wantData)
	}

	// Verify file permissions
	info, _ := os.Stat(outFile)
	if perm := info.Mode().Perm(); perm != 0600 {
		t.Errorf("file permissions = %o, want 0600", perm)
	}
}

func TestExportAttachment_JSONOutput(t *testing.T) {
	attDir, contentHash, wantData, cleanup := setupTestAttachment(t)
	defer cleanup()

	storagePath := filepath.Join(attDir, contentHash[:2], contentHash)

	// Capture stdout
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	err := exportAttachmentAsJSON(storagePath, contentHash)
	w.Close()
	os.Stdout = oldStdout

	if err != nil {
		t.Fatalf("exportAttachmentAsJSON: %v", err)
	}

	var result map[string]any
	if err := json.NewDecoder(r).Decode(&result); err != nil {
		t.Fatalf("decode JSON: %v", err)
	}

	if result["content_hash"] != contentHash {
		t.Errorf("content_hash = %v, want %s", result["content_hash"], contentHash)
	}
	if int(result["size"].(float64)) != len(wantData) {
		t.Errorf("size = %v, want %d", result["size"], len(wantData))
	}

	decoded, err := base64.StdEncoding.DecodeString(result["data_base64"].(string))
	if err != nil {
		t.Fatalf("decode base64: %v", err)
	}
	if string(decoded) != string(wantData) {
		t.Errorf("decoded data = %q, want %q", decoded, wantData)
	}
}

func TestExportAttachment_Base64Output(t *testing.T) {
	attDir, contentHash, wantData, cleanup := setupTestAttachment(t)
	defer cleanup()

	storagePath := filepath.Join(attDir, contentHash[:2], contentHash)

	// Capture stdout
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	err := exportAttachmentAsBase64(storagePath)
	w.Close()
	os.Stdout = oldStdout

	if err != nil {
		t.Fatalf("exportAttachmentAsBase64: %v", err)
	}

	outputBytes, _ := io.ReadAll(r)
	output := string(outputBytes)

	// Strip trailing newline
	expected := base64.StdEncoding.EncodeToString(wantData) + "\n"
	if output != expected {
		t.Errorf("base64 output = %q, want %q", output, expected)
	}
}

func TestExportAttachment_MissingFile(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "msgvault-export-att-missing-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	hash := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	storagePath := filepath.Join(tmpDir, hash[:2], hash)

	_, err = openAttachmentFile(storagePath)
	if err == nil {
		t.Fatal("expected error for missing file, got nil")
	}
	if !contains(err.Error(), "attachment not found") {
		t.Errorf("error = %q, want 'attachment not found'", err)
	}
}

func TestExportAttachment_FlagMutualExclusivity(t *testing.T) {
	tests := []struct {
		name   string
		output string
		json   bool
		base64 bool
		errMsg string
	}{
		{"json+base64", "", true, true, "--json and --base64 are mutually exclusive"},
		{"json+output", "file.bin", true, false, "--json and --output are mutually exclusive"},
		{"base64+output", "file.bin", false, true, "--base64 and --output are mutually exclusive"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			exportAttachmentOutput = tc.output
			exportAttachmentJSON = tc.json
			exportAttachmentBase64 = tc.base64
			defer func() {
				exportAttachmentOutput = ""
				exportAttachmentJSON = false
				exportAttachmentBase64 = false
			}()

			// Use a valid hash — flag validation happens before file access
			err := runExportAttachment(nil, []string{
				"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			})

			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.errMsg)
			}
			if !contains(err.Error(), tc.errMsg) {
				t.Errorf("error = %q, want containing %q", err, tc.errMsg)
			}
		})
	}
}

func TestExportAttachment_HashValidation(t *testing.T) {
	tests := []struct {
		name string
		hash string
	}{
		{"too short", "61ccf192"},
		{"too long", "61ccf192b5bd358738802dc2676d3ceab856f47d26dd29681ac3d335bfd5bbd0aa"},
		{"invalid hex", "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"},
		{"empty", ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			exportAttachmentOutput = ""
			exportAttachmentJSON = false
			exportAttachmentBase64 = false

			err := runExportAttachment(nil, []string{tc.hash})
			if err == nil {
				t.Fatal("expected error for invalid hash, got nil")
			}
			if !contains(err.Error(), "invalid content hash") {
				t.Errorf("error = %q, want containing 'invalid content hash'", err)
			}
		})
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && searchString(s, substr)
}

func searchString(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
