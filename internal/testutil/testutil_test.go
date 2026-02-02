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
}

// writeFileAndAssertExists writes a file and asserts it exists, returning the path.
func writeFileAndAssertExists(t *testing.T, dir, rel string, content []byte) string {
	t.Helper()
	path := WriteFile(t, dir, rel, content)
	MustExist(t, path)
	return path
}

// writeFileAndAssertContent writes a file, asserts it exists, and verifies its content.
func writeFileAndAssertContent(t *testing.T, dir, rel string, content []byte) string {
	t.Helper()
	path := writeFileAndAssertExists(t, dir, rel, content)
	AssertFileContent(t, path, string(content))
	return path
}

func TestWriteAndReadFile(t *testing.T) {
	dir := TempDir(t)
	writeFileAndAssertContent(t, dir, "test.txt", []byte("hello world"))
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

	// Use filepath.Abs to get a platform-appropriate absolute path
	absPath, err := filepath.Abs("/some/path.txt")
	if err != nil {
		t.Fatalf("failed to get absolute path: %v", err)
	}

	// Edge cases and invalid paths
	cases := []struct {
		name    string
		path    string
		wantErr bool
	}{
		{
			name:    "absolute path",
			path:    absPath,
			wantErr: true,
		},
		{
			name:    "rooted path",
			path:    string(filepath.Separator) + "rooted" + string(filepath.Separator) + "path.txt",
			wantErr: true,
		},
		{
			name:    "escape dot dot",
			path:    "../escape.txt",
			wantErr: true,
		},
		{
			name:    "escape dot dot nested",
			path:    "subdir/../../escape.txt",
			wantErr: true,
		},
		{
			name:    "escape just dot dot",
			path:    "..",
			wantErr: true,
		},
		{
			name:    "valid with dots",
			path:    "file-with-dots.test.txt",
			wantErr: false,
		},
		{
			name:    "valid current dir",
			path:    "./current.txt",
			wantErr: false,
		},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			err := validateRelativePath(dir, tt.path)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateRelativePath() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}

	// Shared valid paths fixture
	for _, path := range validRelativePaths {
		t.Run("valid "+path, func(t *testing.T) {
			if err := validateRelativePath(dir, path); err != nil {
				t.Errorf("validateRelativePath() unexpected error: %v", err)
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
