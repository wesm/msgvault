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
		msgDir, files, err := listEmlxFiles(abs)
		if err != nil {
			return nil, err
		}
		if len(files) > 0 {
			label := LabelFromPath(filepath.Dir(abs), abs)
			return []Mailbox{{
				Path: abs, MsgDir: msgDir,
				Label: label, Files: files,
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

		msgDir, files, listErr := listEmlxFiles(path)
		if listErr != nil || len(files) == 0 {
			return nil
		}

		label := LabelFromPath(abs, path)
		mailboxes = append(mailboxes, Mailbox{
			Path: path, MsgDir: msgDir,
			Label: label, Files: files,
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

// findMessagesDir locates the Messages/ directory within a .mbox.
// Returns "" if none found. Checks both legacy (Messages/) and
// modern V10 (<GUID>/Data/Messages/) layouts.
func findMessagesDir(mailboxPath string) string {
	// Legacy: direct Messages/ subdirectory.
	legacy := filepath.Join(mailboxPath, "Messages")
	if info, err := os.Stat(legacy); err == nil && info.IsDir() {
		return legacy
	}

	// Modern V10: <subdir>/Data/Messages/ subdirectory.
	entries, err := os.ReadDir(mailboxPath)
	if err != nil {
		return ""
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		modern := filepath.Join(
			mailboxPath, e.Name(), "Data", "Messages",
		)
		info, statErr := os.Stat(modern)
		if statErr == nil && info.IsDir() {
			return modern
		}
	}

	return ""
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

// listEmlxFiles returns the Messages directory path and sorted .emlx
// filenames within it, excluding .partial.emlx. Returns ("", nil, nil)
// if no Messages directory is found.
func listEmlxFiles(
	mailboxPath string,
) (string, []string, error) {
	msgDir := findMessagesDir(mailboxPath)
	if msgDir == "" {
		return "", nil, nil
	}

	entries, err := os.ReadDir(msgDir)
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil, nil
		}
		return "", nil, fmt.Errorf("read Messages dir: %w", err)
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
	return msgDir, files, nil
}
