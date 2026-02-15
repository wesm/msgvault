//go:build windows

package export

import "os"

// openNoFollow is a best-effort equivalent of O_NOFOLLOW.
// Windows does not provide an easy O_NOFOLLOW-style open here; this may follow
// reparse points and symlinks. Callers still validate size and content hash.
func openNoFollow(path string) (*os.File, error) {
	return os.Open(path)
}
