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

// AttachmentResult contains the outcome of an attachment export operation.
type AttachmentResult struct {
	Result string
	Err    error
}

// Attachments exports the given attachments into a zip file.
// It reads attachment content from attachmentsDir using content-hash based paths.
func Attachments(zipFilename, attachmentsDir string, attachments []query.AttachmentInfo) AttachmentResult {
	zipFile, err := os.Create(zipFilename)
	if err != nil {
		return AttachmentResult{Err: fmt.Errorf("failed to create zip file: %w", err)}
	}
	// Don't defer Close - we need to handle errors and avoid double-close

	zipWriter := zip.NewWriter(zipFile)

	var exportedCount int
	var totalSize int64
	var errors []string
	var writeError bool

	usedNames := make(map[string]int)
	for _, att := range attachments {
		if len(att.ContentHash) < 2 {
			errors = append(errors, fmt.Sprintf("%s: missing or invalid content hash", att.Filename))
			continue
		}

		storagePath := filepath.Join(attachmentsDir, att.ContentHash[:2], att.ContentHash)

		srcFile, err := os.Open(storagePath)
		if err != nil {
			errors = append(errors, fmt.Sprintf("%s: %v", att.Filename, err))
			continue
		}

		// Use filepath.Base to prevent Zip Slip (path traversal) attacks
		filename := filepath.Base(att.Filename)
		if filename == "" || filename == "." {
			filename = att.ContentHash
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

		w, err := zipWriter.Create(filename)
		if err != nil {
			srcFile.Close()
			errors = append(errors, fmt.Sprintf("%s: zip write error: %v", att.Filename, err))
			writeError = true
			continue
		}

		n, err := io.Copy(w, srcFile)
		srcFile.Close()
		if err != nil {
			errors = append(errors, fmt.Sprintf("%s: zip write error: %v", att.Filename, err))
			writeError = true
			continue
		}

		exportedCount++
		totalSize += n
	}

	// Close zip writer first - check for errors as this finalizes the archive
	if err := zipWriter.Close(); err != nil {
		errors = append(errors, fmt.Sprintf("zip finalization error: %v", err))
		writeError = true
	}
	if err := zipFile.Close(); err != nil {
		errors = append(errors, fmt.Sprintf("file close error: %v", err))
		writeError = true
	}

	// Build result message
	if exportedCount == 0 || writeError {
		os.Remove(zipFilename)
		if writeError {
			return AttachmentResult{Result: "Export failed due to write errors. Zip file removed.\n\nErrors:\n" + strings.Join(errors, "\n")}
		}
		return AttachmentResult{Result: "No attachments exported.\n\nErrors:\n" + strings.Join(errors, "\n")}
	}

	cwd, _ := os.Getwd()
	fullPath := filepath.Join(cwd, zipFilename)
	result := fmt.Sprintf("Exported %d attachment(s) (%s)\n\nSaved to:\n%s",
		exportedCount, FormatBytesLong(totalSize), fullPath)
	if len(errors) > 0 {
		result += "\n\nErrors:\n" + strings.Join(errors, "\n")
	}
	return AttachmentResult{Result: result}
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
