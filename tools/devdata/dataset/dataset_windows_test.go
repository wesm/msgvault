//go:build windows

package dataset

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestReplaceSymlink_Windows verifies the Windows-specific symlink replacement logic.
// This test only runs on Windows where os.Rename behavior differs from POSIX.
func TestReplaceSymlink_Windows(t *testing.T) {
	tmpDir := t.TempDir()

	// Create initial target directories
	target1 := filepath.Join(tmpDir, "target1")
	target2 := filepath.Join(tmpDir, "target2")
	if err := os.Mkdir(target1, 0755); err != nil {
		t.Fatalf("mkdir target1: %v", err)
	}
	if err := os.Mkdir(target2, 0755); err != nil {
		t.Fatalf("mkdir target2: %v", err)
	}

	// Create initial symlink
	linkPath := filepath.Join(tmpDir, "link")
	if err := os.Symlink(target1, linkPath); err != nil {
		t.Fatalf("create initial symlink: %v", err)
	}

	// Verify initial state
	target, err := os.Readlink(linkPath)
	if err != nil {
		t.Fatalf("readlink before replace: %v", err)
	}
	if target != target1 {
		t.Errorf("initial target = %q, want %q", target, target1)
	}

	// Replace symlink to point to target2
	if err := ReplaceSymlink(linkPath, target2); err != nil {
		t.Fatalf("ReplaceSymlink: %v", err)
	}

	// Verify symlink now points to target2
	target, err = os.Readlink(linkPath)
	if err != nil {
		t.Fatalf("readlink after replace: %v", err)
	}
	if target != target2 {
		t.Errorf("replaced target = %q, want %q", target, target2)
	}
}

// TestReplaceSymlink_Windows_ErrorCases verifies error handling on Windows.
func TestReplaceSymlink_Windows_ErrorCases(t *testing.T) {
	tmpDir := t.TempDir()

	t.Run("not_a_symlink", func(t *testing.T) {
		// Create a regular file
		regularFile := filepath.Join(tmpDir, "regular")
		if err := os.WriteFile(regularFile, []byte("data"), 0644); err != nil {
			t.Fatalf("create regular file: %v", err)
		}

		// Attempt to replace should fail with safety check
		err := ReplaceSymlink(regularFile, tmpDir)
		if err == nil {
			t.Fatal("ReplaceSymlink should reject regular file")
		}
		if !strings.Contains(err.Error(), "not a symlink") {
			t.Errorf("error = %q, should mention 'not a symlink'", err)
		}
	})

	t.Run("nonexistent_path", func(t *testing.T) {
		nonexistent := filepath.Join(tmpDir, "nonexistent")
		err := ReplaceSymlink(nonexistent, tmpDir)
		if err == nil {
			t.Fatal("ReplaceSymlink should fail for nonexistent path")
		}
	})
}
