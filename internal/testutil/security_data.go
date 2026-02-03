package testutil

import (
	"path/filepath"
	"runtime"
)

// PathTraversalCase describes a single path traversal test vector.
type PathTraversalCase struct{ Name, Path string }

// PathTraversalCases returns a fresh slice of path traversal attack vectors for
// testing path sanitization logic. The returned cases include OS-appropriate
// absolute path variants so Windows UNC/drive-letter paths are also covered.
func PathTraversalCases() []PathTraversalCase {
	cases := []PathTraversalCase{
		{"rooted path", string(filepath.Separator) + "rooted" + string(filepath.Separator) + "path.txt"},
		{"escape dot dot", "../escape.txt"},
		{"escape dot dot nested", "subdir/../../escape.txt"},
		{"escape just dot dot", ".."},
	}
	// OS-appropriate absolute paths
	if runtime.GOOS == "windows" {
		cases = append(cases,
			PathTraversalCase{"absolute drive path", `C:\Windows\system32`},
			PathTraversalCase{"UNC path", `\\server\share\file.txt`},
			// Drive-relative paths (not absolute, but have a volume name).
			// filepath.Join(dir, "C:foo") ignores dir and resolves relative to
			// the current directory on the C: drive, escaping the sandbox.
			PathTraversalCase{"drive-relative path", `C:foo`},
			PathTraversalCase{"drive-relative nested", `D:subdir\file.txt`},
		)
	} else {
		cases = append(cases, PathTraversalCase{"absolute path", "/abs/path"})
	}
	// Forward-slash absolute paths are accepted by Windows APIs too.
	if runtime.GOOS == "windows" {
		cases = append(cases, PathTraversalCase{"forward-slash absolute path", "/abs/path"})
	}
	return cases
}
