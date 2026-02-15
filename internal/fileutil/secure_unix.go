//go:build !windows

// Package fileutil provides cross-platform file helpers.
// On Unix, Secure* helpers are best-effort wrappers around os.* and do not
// protect against symlink traversal or TOCTOU races.
// On Windows, owner-only modes (perm & 0077 == 0) additionally set
// a DACL restricting access to the current user.
package fileutil

import "os"

// SecureWriteFile writes data to the named file, creating it if necessary.
// On Unix, this does not add symlink or race protections beyond os.WriteFile.
// On Windows, owner-only modes get a restrictive DACL.
func SecureWriteFile(path string, data []byte, perm os.FileMode) error {
	return os.WriteFile(path, data, perm)
}

// SecureMkdirAll creates a directory path and all parents that do not yet exist.
// On Unix, this does not add symlink or race protections beyond os.MkdirAll.
// On Windows, owner-only modes get a restrictive DACL.
func SecureMkdirAll(path string, perm os.FileMode) error {
	return os.MkdirAll(path, perm)
}

// SecureChmod changes the mode of the named file.
// On Unix, this does not add symlink or race protections beyond os.Chmod.
// On Windows, owner-only modes get a restrictive DACL.
func SecureChmod(path string, perm os.FileMode) error {
	return os.Chmod(path, perm)
}

// SecureOpenFile opens the named file with specified flag and permissions.
// On Unix, this does not add symlink or race protections beyond os.OpenFile.
// On Windows, owner-only modes with O_CREATE get a restrictive DACL.
func SecureOpenFile(path string, flag int, perm os.FileMode) (*os.File, error) {
	return os.OpenFile(path, flag, perm)
}
