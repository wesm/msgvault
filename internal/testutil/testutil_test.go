package testutil

import (
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

// validRelativePaths returns a fresh slice of relative paths that should pass
// validation and be writable. Used by TestValidateRelativePath and
// TestWriteFileWithValidPaths.
func validRelativePaths() []string {
	return []string{
		"simple.txt",
		"subdir/file.txt",
		"a/b/c/deep.txt",
		"file-with-dots.test.txt",
		"./current.txt",
		// Paths that look like ".." but are actually valid filenames
		"....",            // four dots - valid filename, not parent escape
		"..foo",           // starts with dots but is a valid filename
		"subdir/..hidden", // hidden-style name in subdir
	}
}

func TestWriteFileAndReadBack(t *testing.T) {
	dir := t.TempDir()
	WriteAndVerifyFile(t, dir, "test.txt", []byte("hello world"))
}

func TestWriteFileSubdir(t *testing.T) {
	dir := t.TempDir()

	WriteAndVerifyFile(t, dir, "subdir/nested/test.txt", []byte("nested content"))
	MustExist(t, filepath.Join(dir, "subdir", "nested"))
}

func TestMustExist(t *testing.T) {
	dir := t.TempDir()
	WriteAndVerifyFile(t, dir, "exists.txt", []byte("data"))
	MustExist(t, dir)
}

func TestMustNotExist(t *testing.T) {
	dir := t.TempDir()

	// Should not panic for non-existent path
	MustNotExist(t, filepath.Join(dir, "does-not-exist.txt"))
}

func TestValidateRelativePath(t *testing.T) {
	dir := t.TempDir()

	// Invalid paths from shared fixture
	for _, tt := range PathTraversalCases() {
		t.Run(tt.Name, func(t *testing.T) {
			if err := validateRelativePath(dir, tt.Path); err == nil {
				t.Errorf("validateRelativePath(%q) expected error, got nil", tt.Path)
			}
		})
	}

	// Valid paths from shared fixture
	for _, path := range validRelativePaths() {
		t.Run("valid "+path, func(t *testing.T) {
			if err := validateRelativePath(dir, path); err != nil {
				t.Errorf("validateRelativePath(%q) unexpected error: %v", path, err)
			}
		})
	}
}

func TestPathTraversalCasesReturnsFreshSlice(t *testing.T) {
	a := PathTraversalCases()
	b := PathTraversalCases()

	// Mutate the first slice and verify the second is unaffected.
	if len(a) == 0 {
		t.Fatal("PathTraversalCases() returned empty slice")
	}
	if len(b) == 0 {
		t.Fatal("PathTraversalCases() returned empty slice on second call")
	}
	original := b[0].Name
	a[0].Name = "MUTATED"
	if b[0].Name != original {
		t.Errorf("PathTraversalCases() returned shared slice: mutating one affected the other")
	}
}

func TestWriteFileWithValidPaths(t *testing.T) {
	dir := t.TempDir()

	for _, name := range validRelativePaths() {
		t.Run(name, func(t *testing.T) {
			WriteAndVerifyFile(t, dir, name, []byte("data"))
		})
	}
}
