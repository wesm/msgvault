// Package export handles file export operations such as creating zip archives
// of email attachments.
package export

import (
	"archive/zip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/wesm/msgvault/internal/query"
)

// ExportStats contains structured results of an attachment export operation.
type ExportStats struct {
	Count      int
	Size       int64
	Errors     []string
	ZipPath    string
	WriteError bool // true if a write error occurred and the zip was removed
}

// Attachments exports the given attachments into a zip file.
// It reads attachment content from attachmentsDir using content-hash based paths.
func Attachments(zipFilename, attachmentsDir string, attachments []query.AttachmentInfo) ExportStats {
	zipFile, err := os.Create(zipFilename)
	if err != nil {
		return ExportStats{Errors: []string{fmt.Sprintf("failed to create zip file: %v", err)}}
	}

	zipWriter := zip.NewWriter(zipFile)

	var stats ExportStats
	var writeError bool

	usedNames := make(map[string]int)
	for _, att := range attachments {
		if len(att.ContentHash) < 2 {
			stats.Errors = append(stats.Errors, fmt.Sprintf("%s: missing or invalid content hash", att.Filename))
			continue
		}

		n, err := addAttachmentToZip(zipWriter, attachmentsDir, att, usedNames)
		if err != nil {
			stats.Errors = append(stats.Errors, fmt.Sprintf("%s: %v", att.Filename, err))
			if isWriteError(err) {
				writeError = true
			}
			continue
		}

		stats.Count++
		stats.Size += n
	}

	if err := zipWriter.Close(); err != nil {
		stats.Errors = append(stats.Errors, fmt.Sprintf("zip finalization error: %v", err))
		writeError = true
	}
	if err := zipFile.Close(); err != nil {
		stats.Errors = append(stats.Errors, fmt.Sprintf("file close error: %v", err))
		writeError = true
	}

	if stats.Count == 0 || writeError {
		os.Remove(zipFilename)
		stats.WriteError = writeError
		return stats
	}

	cwd, _ := os.Getwd()
	stats.ZipPath = filepath.Join(cwd, zipFilename)
	return stats
}

// FormatExportResult formats ExportStats into a human-readable string for display.
func FormatExportResult(stats ExportStats) string {
	// Write error is fatal - zip was removed regardless of count
	if stats.WriteError {
		msg := "Export failed due to write errors. Zip file removed."
		if len(stats.Errors) > 0 {
			msg += "\n\nErrors:\n" + strings.Join(stats.Errors, "\n")
		}
		return msg
	}

	if stats.Count == 0 {
		msg := "No attachments exported."
		if len(stats.Errors) > 0 {
			msg += "\n\nErrors:\n" + strings.Join(stats.Errors, "\n")
		}
		return msg
	}

	result := fmt.Sprintf("Exported %d attachment(s) (%s)\n\nSaved to:\n%s",
		stats.Count, FormatBytesLong(stats.Size), stats.ZipPath)
	if len(stats.Errors) > 0 {
		result += "\n\nErrors:\n" + strings.Join(stats.Errors, "\n")
	}
	return result
}

type zipWriteError struct {
	err error
}

func (e *zipWriteError) Error() string { return e.err.Error() }
func (e *zipWriteError) Unwrap() error { return e.err }

func isWriteError(err error) bool {
	_, ok := err.(*zipWriteError)
	return ok
}

func addAttachmentToZip(zw *zip.Writer, root string, att query.AttachmentInfo, usedNames map[string]int) (int64, error) {
	storagePath := filepath.Join(root, att.ContentHash[:2], att.ContentHash)

	srcFile, err := os.Open(storagePath)
	if err != nil {
		return 0, err
	}
	defer srcFile.Close()

	filename := resolveUniqueFilename(att.Filename, att.ContentHash, usedNames)

	w, err := zw.Create(filename)
	if err != nil {
		return 0, &zipWriteError{fmt.Errorf("zip write error: %w", err)}
	}

	n, err := io.Copy(w, srcFile)
	if err != nil {
		return 0, &zipWriteError{fmt.Errorf("zip write error: %w", err)}
	}

	return n, nil
}

func resolveUniqueFilename(original, contentHash string, usedNames map[string]int) string {
	filename := SanitizeFilename(filepath.Base(original))
	if filename == "" || filename == "." {
		filename = contentHash
	}

	baseKey := filename
	if count, exists := usedNames[baseKey]; exists {
		ext := filepath.Ext(filename)
		base := filename[:len(filename)-len(ext)]
		filename = fmt.Sprintf("%s_%d%s", base, count+1, ext)
		usedNames[baseKey] = count + 1
	} else {
		usedNames[baseKey] = 1
	}

	return filename
}

// SanitizeFilename removes or replaces characters that are invalid in filenames.
func SanitizeFilename(s string) string {
	var result []rune
	for _, r := range s {
		switch r {
		case '/', '\\', ':', '*', '?', '"', '<', '>', '|', '\n', '\r', '\t':
			result = append(result, '_')
		default:
			result = append(result, r)
		}
	}
	return string(result)
}

// FormatBytesLong formats bytes with full precision for export results.
func FormatBytesLong(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}
