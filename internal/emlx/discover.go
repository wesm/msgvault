package emlx

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Mailbox represents an Apple Mail mailbox directory containing .emlx files.
type Mailbox struct {
	// Path is the absolute path to the .mbox or .imapmbox directory.
	Path string

	// MsgDir is the absolute path to the Messages/ directory
	// containing .emlx files. In legacy layouts this is Path/Messages;
	// in modern V10 layouts it is Path/<GUID>/Data/Messages.
	MsgDir string

	// Label is the derived label for messages in this mailbox.
	Label string

	// Files contains sorted .emlx filenames within MsgDir.
	Files []string
}

// DiscoverMailboxes walks an Apple Mail directory tree and returns all
// mailbox directories that contain .emlx files.
//
// If rootDir itself is a mailbox directory (ends in .mbox or .imapmbox
// and contains a Messages/ subdirectory with .emlx files), only that
// single mailbox is returned.
func DiscoverMailboxes(rootDir string) ([]Mailbox, error) {
	abs, err := filepath.Abs(rootDir)
	if err != nil {
		return nil, fmt.Errorf("emlx discover: abs path: %w", err)
	}

	info, err := os.Stat(abs)
	if err != nil {
		return nil, fmt.Errorf("emlx discover: stat %q: %w", abs, err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf(
			"emlx discover: %q is not a directory", abs,
		)
	}

	// Auto-detect: if the path itself is a mailbox, import just that one.
	if isMailboxDir(abs) {
		mboxes := listAllEmlxFiles(abs)
		if len(mboxes) > 0 {
			label := LabelFromPath(filepath.Dir(abs), abs)
			var mailboxes []Mailbox
			for _, mf := range mboxes {
				mailboxes = append(mailboxes, Mailbox{
					Path: abs, MsgDir: mf.Dir,
					Label: label, Files: mf.Files,
				})
			}
			return mailboxes, nil
		}
	}

	var mailboxes []Mailbox
	err = filepath.WalkDir(abs, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil // skip errors
		}
		if !d.IsDir() {
			return nil
		}

		// Skip Messages/ directories (read directly by findAllMessagesDirs).
		if d.Name() == "Messages" {
			return filepath.SkipDir
		}

		// Skip UUID directories that are direct children of .mbox dirs.
		// These contain Data/ shard subtrees that findAllMessagesDirs
		// reads directly; walking into them wastes I/O.
		if isUUID(d.Name()) {
			parent := filepath.Base(filepath.Dir(path))
			parentLower := strings.ToLower(parent)
			if strings.HasSuffix(parentLower, ".mbox") ||
				strings.HasSuffix(parentLower, ".imapmbox") {
				return filepath.SkipDir
			}
		}

		// Skip non-mailbox directories.
		if !isMailboxDir(path) {
			return nil
		}

		mboxes := listAllEmlxFiles(path)
		if len(mboxes) == 0 {
			return nil
		}

		label := LabelFromPath(abs, path)
		for _, mf := range mboxes {
			mailboxes = append(mailboxes, Mailbox{
				Path: path, MsgDir: mf.Dir,
				Label: label, Files: mf.Files,
			})
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("emlx discover: walk: %w", err)
	}

	sort.Slice(mailboxes, func(i, j int) bool {
		if mailboxes[i].Path != mailboxes[j].Path {
			return mailboxes[i].Path < mailboxes[j].Path
		}
		return mailboxes[i].MsgDir < mailboxes[j].MsgDir
	})
	return mailboxes, nil
}

// LabelFromPath derives a human-readable label from a mailbox path
// relative to the root directory.
//
// Rules:
//   - Strip root prefix
//   - Strip known containers: Mailboxes/, IMAP-*/, POP-*/
//   - Strip .mbox/.imapmbox suffix from all components
//   - Use the remaining path as the label
func LabelFromPath(rootDir, mailboxPath string) string {
	rel, err := filepath.Rel(rootDir, mailboxPath)
	if err != nil {
		// Fallback: use the directory base name.
		return stripMailboxSuffix(filepath.Base(mailboxPath))
	}

	// Split into components.
	parts := strings.Split(filepath.ToSlash(rel), "/")

	// Filter out known container directories.
	var filtered []string
	for _, p := range parts {
		if p == "Mailboxes" {
			continue
		}
		if strings.HasPrefix(p, "IMAP-") || strings.HasPrefix(p, "POP-") {
			continue
		}
		// V10 account GUID directories (e.g. 13C9A646-...-E07FFBDDEED3).
		if isUUID(p) {
			continue
		}
		filtered = append(filtered, p)
	}

	if len(filtered) == 0 {
		return stripMailboxSuffix(filepath.Base(mailboxPath))
	}

	// Strip .mbox/.imapmbox suffix from all components.
	for i := range filtered {
		filtered[i] = stripMailboxSuffix(filtered[i])
	}

	return strings.Join(filtered, "/")
}

func isMailboxDir(path string) bool {
	base := filepath.Base(path)
	lower := strings.ToLower(base)
	if !strings.HasSuffix(lower, ".mbox") &&
		!strings.HasSuffix(lower, ".imapmbox") {
		return false
	}

	return findMessagesDir(path) != ""
}

// findAllMessagesDirs locates all Messages/ directories within a .mbox.
// Checks legacy (Messages/), modern V10 (<GUID>/Data/Messages/), and
// sharded V10 (<GUID>/Data/<num>/<num>/<num>/Messages/) layouts.
func findAllMessagesDirs(mailboxPath string) []string {
	var dirs []string

	// Legacy: direct Messages/ subdirectory.
	legacy := filepath.Join(mailboxPath, "Messages")
	if info, err := os.Stat(legacy); err == nil && info.IsDir() {
		dirs = append(dirs, legacy)
	}

	// Modern V10: walk <subdir>/Data/ looking for Messages/ directories.
	// Handles both flat (<GUID>/Data/Messages/) and sharded
	// (<GUID>/Data/<num>/<num>/<num>/Messages/) layouts.
	entries, err := os.ReadDir(mailboxPath)
	if err != nil {
		return dirs
	}
	for _, e := range entries {
		if !e.IsDir() || e.Name() == "Messages" {
			continue
		}
		dataDir := filepath.Join(mailboxPath, e.Name(), "Data")
		info, statErr := os.Stat(dataDir)
		if statErr != nil || !info.IsDir() {
			continue
		}
		_ = filepath.WalkDir(dataDir, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return nil
			}
			if d.IsDir() && d.Name() == "Messages" {
				dirs = append(dirs, path)
				return filepath.SkipDir
			}
			return nil
		})
	}

	return dirs
}

// findMessagesDir locates the Messages/ directory within a .mbox.
// Returns "" if none found. When multiple directories exist (sharded
// V10 layout), returns the first one that contains .emlx files.
func findMessagesDir(mailboxPath string) string {
	dirs := findAllMessagesDirs(mailboxPath)
	if len(dirs) == 0 {
		return ""
	}

	// Prefer the first candidate that has .emlx files.
	for _, dir := range dirs {
		if hasEmlxFiles(dir) {
			return dir
		}
	}

	// No candidate has files; return first for isMailboxDir.
	return dirs[0]
}

// hasEmlxFiles returns true if dir contains at least one
// non-partial .emlx file.
func hasEmlxFiles(dir string) bool {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		lower := strings.ToLower(e.Name())
		if strings.HasSuffix(lower, ".emlx") &&
			!strings.HasSuffix(lower, ".partial.emlx") {
			return true
		}
	}
	return false
}

// isUUID returns true if s matches UUID format (8-4-4-4-12 hex).
func isUUID(s string) bool {
	if len(s) != 36 {
		return false
	}
	for i, c := range s {
		switch i {
		case 8, 13, 18, 23:
			if c != '-' {
				return false
			}
		default:
			isHex := (c >= '0' && c <= '9') ||
				(c >= 'a' && c <= 'f') ||
				(c >= 'A' && c <= 'F')
			if !isHex {
				return false
			}
		}
	}
	return true
}

func stripMailboxSuffix(name string) string {
	lower := strings.ToLower(name)
	if strings.HasSuffix(lower, ".imapmbox") {
		return name[:len(name)-len(".imapmbox")]
	}
	if strings.HasSuffix(lower, ".mbox") {
		return name[:len(name)-len(".mbox")]
	}
	return name
}

// msgDirFiles holds a Messages directory path and its sorted .emlx filenames.
type msgDirFiles struct {
	Dir   string
	Files []string
}

// listAllEmlxFiles returns all (Messages dir, sorted files) pairs for a
// mailbox, supporting sharded V10 layouts where .emlx files are spread
// across multiple Messages/ directories. Only returns entries with files.
func listAllEmlxFiles(mailboxPath string) []msgDirFiles {
	dirs := findAllMessagesDirs(mailboxPath)
	var result []msgDirFiles
	for _, dir := range dirs {
		files := readEmlxDir(dir)
		if len(files) > 0 {
			result = append(result, msgDirFiles{Dir: dir, Files: files})
		}
	}
	return result
}

// readEmlxDir reads sorted .emlx filenames from a single Messages
// directory, excluding .partial.emlx files.
func readEmlxDir(msgDir string) []string {
	entries, err := os.ReadDir(msgDir)
	if err != nil {
		return nil
	}

	var files []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasSuffix(strings.ToLower(name), ".emlx") {
			continue
		}
		// Skip .partial.emlx files (Apple Mail temp files).
		if strings.HasSuffix(
			strings.ToLower(name), ".partial.emlx",
		) {
			continue
		}
		files = append(files, name)
	}

	sort.Strings(files)
	return files
}
