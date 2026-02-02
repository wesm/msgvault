// Package testutil provides test helpers for msgvault tests.
package testutil

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/wesm/msgvault/internal/store"
)

// NewTestStore creates a temporary database for testing.
// The database is automatically cleaned up when the test completes.
func NewTestStore(t *testing.T) *store.Store {
	t.Helper()

	tmpDir, err := os.MkdirTemp("", "msgvault-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}

	// Register cleanup
	t.Cleanup(func() {
		os.RemoveAll(tmpDir)
	})

	dbPath := filepath.Join(tmpDir, "test.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}

	// Register close on cleanup
	t.Cleanup(func() {
		st.Close()
	})

	// Initialize schema
	if err := st.InitSchema(); err != nil {
		t.Fatalf("init schema: %v", err)
	}

	return st
}

// TempDir creates a temporary directory for testing.
// The directory is automatically cleaned up when the test completes.
func TempDir(t *testing.T) string {
	t.Helper()

	tmpDir, err := os.MkdirTemp("", "msgvault-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}

	t.Cleanup(func() {
		os.RemoveAll(tmpDir)
	})

	return tmpDir
}

// validateRelativePath checks that name is a relative path that stays within dir.
// Returns an error if the path is absolute, rooted, or would escape the directory.
func validateRelativePath(dir, name string) error {
	// Reject absolute paths
	if filepath.IsAbs(name) {
		return fmt.Errorf("name must be relative, got absolute path: %s", name)
	}

	// Reject paths with volume names (Windows: C:foo, D:\bar)
	if filepath.VolumeName(name) != "" {
		return fmt.Errorf("name must not contain volume: %s", name)
	}

	// Clean the path and check for rooted paths (Windows: \foo) and .. escapes
	cleaned := filepath.Clean(name)

	// Reject rooted paths that start with separator (e.g., \foo on Windows)
	if len(cleaned) > 0 && cleaned[0] == filepath.Separator {
		return fmt.Errorf("name must not be rooted: %s", name)
	}

	// Reject paths that escape via ..
	if cleaned == ".." || (len(cleaned) >= 3 && cleaned[:3] == ".."+string(filepath.Separator)) {
		return fmt.Errorf("name must not escape directory: %s", name)
	}

	path := filepath.Join(dir, cleaned)

	// Verify the final path is still within dir using filepath.Rel
	// This handles case-insensitivity on Windows and other edge cases
	absDir, err := filepath.Abs(dir)
	if err != nil {
		return fmt.Errorf("cannot resolve directory: %w", err)
	}
	absPath, err := filepath.Abs(path)
	if err != nil {
		return fmt.Errorf("cannot resolve path: %w", err)
	}

	// Use Rel to check containment - if the relative path starts with "..",
	// the target is outside the directory
	rel, err := filepath.Rel(absDir, absPath)
	if err != nil {
		return fmt.Errorf("cannot compute relative path: %w", err)
	}
	if rel == ".." || (len(rel) >= 3 && rel[:3] == ".."+string(filepath.Separator)) {
		return fmt.Errorf("path escapes directory: %s", name)
	}

	return nil
}

// WriteFile writes content to a file in the given directory.
// The name must be a relative path without ".." components to ensure
// test isolation. Absolute paths or paths that escape dir will fail the test.
func WriteFile(t *testing.T, dir, name string, content []byte) string {
	t.Helper()

	if err := validateRelativePath(dir, name); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	path := filepath.Join(dir, filepath.Clean(name))
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatalf("create dir: %v", err)
	}
	if err := os.WriteFile(path, content, 0644); err != nil {
		t.Fatalf("write file: %v", err)
	}
	return path
}

// ReadFile reads a file and fails the test on error.
func ReadFile(t *testing.T, path string) []byte {
	t.Helper()

	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read file %s: %v", path, err)
	}
	return content
}

// AssertFileContent reads the file at path and asserts its content matches expected.
func AssertFileContent(t *testing.T, path string, expected string) {
	t.Helper()

	content := ReadFile(t, path)
	if string(content) != expected {
		t.Errorf("file content mismatch\nexpected: %q\ngot:      %q", expected, content)
	}
}

// MustExist fails the test if the path does not exist or cannot be accessed.
func MustExist(t *testing.T, path string) {
	t.Helper()

	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected %s to exist: %v", path, err)
	}
}

// MustNotExist fails the test if the path exists or if there's an error
// other than "not exist" (e.g., permission denied).
func MustNotExist(t *testing.T, path string) {
	t.Helper()

	_, err := os.Stat(path)
	if err == nil {
		t.Fatalf("expected %s to not exist", path)
	}
	if !os.IsNotExist(err) {
		t.Fatalf("unexpected error checking %s: %v", path, err)
	}
}

// MustNoErr fails the test immediately if err is non-nil.
// Use this for setup operations where failure means the test cannot proceed.
func MustNoErr(t *testing.T, err error, msg string) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: %v", msg, err)
	}
}

// PathTraversalCases is a shared fixture of path traversal attack vectors for
// testing path sanitization logic across packages.
var PathTraversalCases = []struct{ Name, Path string }{
	{"absolute path", "/abs/path"},
	{"rooted path", string(filepath.Separator) + "rooted" + string(filepath.Separator) + "path.txt"},
	{"escape dot dot", "../escape.txt"},
	{"escape dot dot nested", "subdir/../../escape.txt"},
	{"escape just dot dot", ".."},
}

// WriteAndVerifyFile writes content to a file, asserts it exists, and verifies
// its content matches. Returns the full path to the written file.
func WriteAndVerifyFile(t *testing.T, dir, rel string, content []byte) string {
	t.Helper()
	path := WriteFile(t, dir, rel, content)
	MustExist(t, path)
	AssertFileContent(t, path, string(content))
	return path
}

// ArchiveEntry describes a single entry in a tar.gz archive for testing.
type ArchiveEntry struct {
	Name     string
	Content  string
	TypeFlag byte
	LinkName string
	Mode     int64
}

// CreateTarGz creates a tar.gz archive at path containing the given entries.
func CreateTarGz(t *testing.T, path string, entries []ArchiveEntry) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	gzw := gzip.NewWriter(f)
	defer gzw.Close()
	tw := tar.NewWriter(gzw)
	defer tw.Close()

	for _, e := range entries {
		mode := e.Mode
		if mode == 0 {
			mode = 0644
		}
		h := &tar.Header{
			Name:     e.Name,
			Mode:     mode,
			Size:     int64(len(e.Content)),
			Typeflag: e.TypeFlag,
			Linkname: e.LinkName,
		}
		if err := tw.WriteHeader(h); err != nil {
			t.Fatal(err)
		}
		if len(e.Content) > 0 {
			if _, err := tw.Write([]byte(e.Content)); err != nil {
				t.Fatal(err)
			}
		}
	}
}

// CreateTempZip creates a zip file in a temporary directory containing the
// provided entries (filename -> content). Returns the path to the zip file.
func CreateTempZip(t *testing.T, entries map[string]string) string {
	t.Helper()

	zipPath := filepath.Join(t.TempDir(), "test.zip")
	f, err := os.Create(zipPath)
	if err != nil {
		t.Fatalf("create zip file: %v", err)
	}
	defer f.Close()

	w := zip.NewWriter(f)
	keys := make([]string, 0, len(entries))
	for name := range entries {
		keys = append(keys, name)
	}
	sort.Strings(keys)
	for _, name := range keys {
		content := entries[name]
		fw, err := w.Create(name)
		if err != nil {
			t.Fatalf("create zip entry %s: %v", name, err)
		}
		if _, err := fw.Write([]byte(content)); err != nil {
			t.Fatalf("write zip entry %s: %v", name, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close zip writer: %v", err)
	}

	return zipPath
}
