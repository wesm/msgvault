//go:build unix

package export

import (
	"os"

	"golang.org/x/sys/unix"
)

// openNoFollow opens a file read-only without following symlinks.
// Uses O_NOFOLLOW to prevent symlink traversal on the final path component.
func openNoFollow(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_RDONLY|unix.O_NOFOLLOW, 0)
}
