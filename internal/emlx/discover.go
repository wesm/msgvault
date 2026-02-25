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

	// Label is the derived label for messages in this mailbox.
	Label string

	// Files contains sorted .emlx filenames within Messages/.
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
		files, err := listEmlxFiles(abs)
		if err != nil {
			return nil, err
		}
		if len(files) > 0 {
			label := LabelFromPath(filepath.Dir(abs), abs)
			return []Mailbox{{
				Path: abs, Label: label, Files: files,
			}}, nil
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

		// listEmlxFiles reads Messages/ directly, so skip walking it.
		if d.Name() == "Messages" {
			return filepath.SkipDir
		}

		// Skip non-mailbox directories.
		if !isMailboxDir(path) {
			return nil
		}

		files, listErr := listEmlxFiles(path)
		if listErr != nil || len(files) == 0 {
			return nil
		}

		label := LabelFromPath(abs, path)
		mailboxes = append(mailboxes, Mailbox{
			Path: path, Label: label, Files: files,
		})

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("emlx discover: walk: %w", err)
	}

	sort.Slice(mailboxes, func(i, j int) bool {
		return mailboxes[i].Path < mailboxes[j].Path
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

	// Must have a Messages/ subdirectory.
	msgDir := filepath.Join(path, "Messages")
	info, err := os.Stat(msgDir)
	return err == nil && info.IsDir()
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

// listEmlxFiles returns sorted .emlx filenames (not paths) within
// the Messages/ subdirectory of a mailbox, excluding .partial.emlx.
func listEmlxFiles(mailboxPath string) ([]string, error) {
	msgDir := filepath.Join(mailboxPath, "Messages")
	entries, err := os.ReadDir(msgDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read Messages dir: %w", err)
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
		if strings.HasSuffix(strings.ToLower(name), ".partial.emlx") {
			continue
		}
		files = append(files, name)
	}

	sort.Strings(files)
	return files, nil
}
