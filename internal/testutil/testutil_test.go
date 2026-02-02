package testutil

import (
	"os"
	"path/filepath"
	"testing"
)

func TestNewTestStore(t *testing.T) {
	st := NewTestStore(t)

	// Verify store is usable
	stats, err := st.GetStats()
	if err != nil {
		t.Fatalf("get stats: %v", err)
	}

	// Fresh database should have no messages
	if stats.MessageCount != 0 {
		t.Errorf("expected 0 messages, got %d", stats.MessageCount)
	}
}

func TestTempDir(t *testing.T) {
	dir := TempDir(t)

	// Verify directory exists
	info, err := os.Stat(dir)
	if err != nil {
		t.Fatalf("stat temp dir: %v", err)
	}
	if !info.IsDir() {
		t.Errorf("expected directory, got file")
	}
}

// validRelativePaths is a shared fixture of relative paths that should pass
// validation and be writable. Used by TestValidateRelativePath and
// TestWriteFileWithValidPaths.
var validRelativePaths = []string{
	"simple.txt",
	"subdir/file.txt",
	"a/b/c/deep.txt",
	"file-with-dots.test.txt",
	"./current.txt",
}

// writeFileAndAssertExists writes a file and asserts it exists, returning the path.
func writeFileAndAssertExists(t *testing.T, dir, rel string, content []byte) string {
	t.Helper()
	path := WriteFile(t, dir, rel, content)
	MustExist(t, path)
	return path
}

func TestWriteFileAndReadBack(t *testing.T) {
	dir := TempDir(t)
	WriteAndVerifyFile(t, dir, "test.txt", []byte("hello world"))
}

func TestWriteFileSubdir(t *testing.T) {
	dir := TempDir(t)

	writeFileAndAssertExists(t, dir, "subdir/nested/test.txt", []byte("nested content"))
	MustExist(t, filepath.Join(dir, "subdir", "nested"))
}

func TestMustExist(t *testing.T) {
	dir := TempDir(t)
	writeFileAndAssertExists(t, dir, "exists.txt", []byte("data"))
	MustExist(t, dir)
}

func TestMustNotExist(t *testing.T) {
	dir := TempDir(t)

	// Should not panic for non-existent path
	MustNotExist(t, filepath.Join(dir, "does-not-exist.txt"))
}

func TestValidateRelativePath(t *testing.T) {
	dir := TempDir(t)

	// Invalid paths from shared fixture
	for _, tt := range PathTraversalCases {
		t.Run(tt.Name, func(t *testing.T) {
			if err := validateRelativePath(dir, tt.Path); err == nil {
				t.Errorf("validateRelativePath(%q) expected error, got nil", tt.Path)
			}
		})
	}

	// Valid paths from shared fixture
	for _, path := range validRelativePaths {
		t.Run("valid "+path, func(t *testing.T) {
			if err := validateRelativePath(dir, path); err != nil {
				t.Errorf("validateRelativePath(%q) unexpected error: %v", path, err)
			}
		})
	}
}

func TestWriteFileWithValidPaths(t *testing.T) {
	dir := TempDir(t)

	for _, name := range validRelativePaths {
		t.Run(name, func(t *testing.T) {
			writeFileAndAssertExists(t, dir, name, []byte("data"))
		})
	}
}
