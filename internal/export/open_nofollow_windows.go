//go:build windows

package export

import "os"

// openNoFollow opens a file read-only.
// Windows does not provide O_NOFOLLOW; this may follow reparse points and
// symlinks. Callers validate size and content hash as additional safeguards.
func openNoFollow(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_RDONLY, 0)
}
