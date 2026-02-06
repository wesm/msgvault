//go:build !windows

// Package fileutil provides cross-platform secure file operations.
// On Unix, these are thin wrappers around the standard os functions.
// On Windows, owner-only modes (perm & 0077 == 0) additionally set
// a DACL restricting access to the current user.
package fileutil

import "os"

// SecureWriteFile writes data to the named file, creating it if necessary.
// On Windows, owner-only modes get a restrictive DACL.
func SecureWriteFile(path string, data []byte, perm os.FileMode) error {
	return os.WriteFile(path, data, perm)
}

// SecureMkdirAll creates a directory path and all parents that do not yet exist.
// On Windows, owner-only modes get a restrictive DACL.
func SecureMkdirAll(path string, perm os.FileMode) error {
	return os.MkdirAll(path, perm)
}

// SecureChmod changes the mode of the named file.
// On Windows, owner-only modes get a restrictive DACL.
func SecureChmod(path string, perm os.FileMode) error {
	return os.Chmod(path, perm)
}

// SecureOpenFile opens the named file with specified flag and permissions.
// On Windows, owner-only modes with O_CREATE get a restrictive DACL.
func SecureOpenFile(path string, flag int, perm os.FileMode) (*os.File, error) {
	return os.OpenFile(path, flag, perm)
}
