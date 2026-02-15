//go:build !windows && !unix

package export

import "os"

func openNoFollow(path string) (*os.File, error) {
	return os.Open(path)
}
