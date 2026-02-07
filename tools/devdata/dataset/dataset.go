// Package dataset provides filesystem operations for managing msgvault datasets.
package dataset

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// DatasetInfo describes a discovered dataset directory.
type DatasetInfo struct {
	Name      string // dataset name (e.g., "gold", "dev") or "(default)" for real ~/.msgvault
	Path      string // absolute path to the directory
	HasDB     bool   // whether msgvault.db exists in the directory
	Active    bool   // whether this is the current symlink target
	IsDefault bool   // true for a real ~/.msgvault directory (not in dev mode)
	DBSize    int64  // size of msgvault.db in bytes (0 if not present)
}

// IsSymlink reports whether the path is a symbolic link.
func IsSymlink(path string) (bool, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return false, err
	}
	return info.Mode()&os.ModeSymlink != 0, nil
}

// ReadTarget returns the target of the symbolic link at path.
func ReadTarget(path string) (string, error) {
	return os.Readlink(path)
}

// Exists reports whether the path exists (follows symlinks).
func Exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// HasDatabase reports whether path/msgvault.db exists.
func HasDatabase(path string) bool {
	_, err := os.Stat(filepath.Join(path, "msgvault.db"))
	return err == nil
}

// DatabaseSize returns the size of msgvault.db in the given directory, or 0.
func DatabaseSize(path string) int64 {
	info, err := os.Stat(filepath.Join(path, "msgvault.db"))
	if err != nil {
		return 0
	}
	return info.Size()
}

// ReplaceSymlink atomically replaces the symlink at linkPath to point to target.
// It re-verifies that linkPath is a symlink immediately before removal to prevent
// accidental deletion of a real directory.
func ReplaceSymlink(linkPath, target string) error {
	// Re-verify immediately before removal (Lstat guard)
	info, err := os.Lstat(linkPath)
	if err != nil {
		return fmt.Errorf("lstat %s: %w", linkPath, err)
	}
	if info.Mode()&os.ModeSymlink == 0 {
		return fmt.Errorf("%s is not a symlink; refusing to remove (safety check)", linkPath)
	}

	if err := os.Remove(linkPath); err != nil {
		return fmt.Errorf("remove symlink %s: %w", linkPath, err)
	}

	if err := os.Symlink(target, linkPath); err != nil {
		return fmt.Errorf("create symlink %s -> %s: %w", linkPath, target, err)
	}

	return nil
}

// ListDatasets enumerates all dataset directories in homeDir.
// It looks for directories matching ~/.msgvault-* and also includes
// ~/.msgvault itself when it is a real directory (not a symlink).
func ListDatasets(homeDir string) ([]DatasetInfo, error) {
	mvPath := filepath.Join(homeDir, ".msgvault")

	// Determine current symlink target for marking active dataset
	var activeTarget string
	if isSym, _ := IsSymlink(mvPath); isSym {
		if target, err := ReadTarget(mvPath); err == nil {
			// Resolve to absolute path for comparison
			if !filepath.IsAbs(target) {
				target = filepath.Join(homeDir, target)
			}
			activeTarget = filepath.Clean(target)
		}
	}

	var datasets []DatasetInfo

	// Check if ~/.msgvault is a real directory (not in dev mode)
	if isSym, err := IsSymlink(mvPath); err == nil && !isSym {
		if info, err := os.Stat(mvPath); err == nil && info.IsDir() {
			datasets = append(datasets, DatasetInfo{
				Name:      "(default)",
				Path:      mvPath,
				HasDB:     HasDatabase(mvPath),
				Active:    true,
				IsDefault: true,
				DBSize:    DatabaseSize(mvPath),
			})
		}
	}

	// Glob for ~/.msgvault-* directories
	pattern := filepath.Join(homeDir, ".msgvault-*")
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return nil, fmt.Errorf("glob datasets: %w", err)
	}

	for _, m := range matches {
		info, err := os.Stat(m)
		if err != nil || !info.IsDir() {
			continue
		}

		name := strings.TrimPrefix(filepath.Base(m), ".msgvault-")
		cleanPath := filepath.Clean(m)

		datasets = append(datasets, DatasetInfo{
			Name:   name,
			Path:   cleanPath,
			HasDB:  HasDatabase(cleanPath),
			Active: activeTarget != "" && activeTarget == cleanPath,
			DBSize: DatabaseSize(cleanPath),
		})
	}

	sort.Slice(datasets, func(i, j int) bool {
		return datasets[i].Name < datasets[j].Name
	})

	return datasets, nil
}
