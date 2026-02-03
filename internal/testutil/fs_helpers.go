package testutil

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// validateRelativePath checks that name is a relative path that stays within dir.
// Returns an error if the path is absolute or would escape the directory.
func validateRelativePath(dir, name string) error {
	if filepath.IsAbs(name) {
		return fmt.Errorf("absolute path not allowed: %s", name)
	}

	// Reject drive-relative paths on Windows (e.g., "C:foo").
	// These are not absolute but filepath.Join(dir, "C:foo") ignores dir
	// and resolves relative to the current directory on the C: drive.
	if filepath.VolumeName(name) != "" {
		return fmt.Errorf("path with volume name not allowed: %s", name)
	}

	// Join and Clean handles separators and ".." resolution
	targetPath := filepath.Join(dir, name)

	// Verify the resolved path is still inside dir
	rel, err := filepath.Rel(dir, targetPath)
	if err != nil {
		return fmt.Errorf("cannot compute relative path: %w", err)
	}
	// Check for parent directory escape: exactly ".." or starts with "../" (or "..\")
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
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

// WriteAndVerifyFile writes content to a file, asserts it exists, and verifies
// its content matches. Returns the full path to the written file.
func WriteAndVerifyFile(t *testing.T, dir, rel string, content []byte) string {
	t.Helper()
	path := WriteFile(t, dir, rel, content)
	MustExist(t, path)
	AssertFileContent(t, path, string(content))
	return path
}
