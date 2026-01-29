package tui

import (
	"archive/zip"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/wesm/msgvault/internal/deletion"
	"github.com/wesm/msgvault/internal/query"
)

// ExportResultMsg is returned when attachment export completes.
type ExportResultMsg struct {
	Result string
	Err    error
}

// ActionController handles business logic for actions like deletion and export.
type ActionController struct {
	engine      query.Engine
	deletionMgr *deletion.Manager
	dataDir     string
}

// NewActionController creates a new action controller.
func NewActionController(engine query.Engine, dataDir string) *ActionController {
	return &ActionController{
		engine:  engine,
		dataDir: dataDir,
	}
}

// StageForDeletion prepares messages for deletion based on selection.
func (c *ActionController) StageForDeletion(aggregateSelection map[string]bool, messageSelection map[int64]bool, aggregateViewType query.ViewType, accountFilter *int64, accounts []query.AccountInfo, currentViewType query.ViewType, currentFilterKey string, timeGranularity query.TimeGranularity, messages []query.MessageSummary) (*deletion.Manifest, error) {
	// Collect Gmail IDs to delete
	gmailIDSet := make(map[string]bool)
	ctx := context.Background()

	// From selected aggregates - resolve to Gmail IDs via query engine
	if len(aggregateSelection) > 0 {
		for key := range aggregateSelection {
			filter := query.MessageFilter{
				SourceID: accountFilter,
			}

			switch aggregateViewType {
			case query.ViewSenders:
				filter.Sender = key
			case query.ViewRecipients:
				filter.Recipient = key
			case query.ViewDomains:
				filter.Domain = key
			case query.ViewLabels:
				filter.Label = key
			case query.ViewTime:
				filter.TimePeriod = key
				filter.TimeGranularity = timeGranularity
			}

			ids, err := c.engine.GetGmailIDsByFilter(ctx, filter)
			if err != nil {
				return nil, fmt.Errorf("error loading messages: %v", err)
			}
			for _, id := range ids {
				gmailIDSet[id] = true
			}
		}
	}

	// From selected message IDs
	if len(messageSelection) > 0 {
		for _, msg := range messages {
			if messageSelection[msg.ID] {
				gmailIDSet[msg.SourceMessageID] = true
			}
		}
	}

	gmailIDs := make([]string, 0, len(gmailIDSet))
	for id := range gmailIDSet {
		gmailIDs = append(gmailIDs, id)
	}

	if len(gmailIDs) == 0 {
		return nil, fmt.Errorf("no messages selected")
	}

	// Build description
	var description string
	if len(aggregateSelection) == 1 {
		for key := range aggregateSelection {
			description = fmt.Sprintf("%s-%s", aggregateViewType.String(), key)
			break
		}
	} else if len(aggregateSelection) > 1 {
		description = fmt.Sprintf("%s-multiple(%d)", aggregateViewType.String(), len(aggregateSelection))
	} else if len(messageSelection) > 0 {
		// Just a generic description for message list selection
		description = fmt.Sprintf("messages-multiple(%d)", len(messageSelection))
	} else {
		description = "selection"
	}

	if len(description) > 30 {
		description = description[:30]
	}

	manifest := deletion.NewManifest(description, gmailIDs)
	manifest.CreatedBy = "tui"

	// Set filters
	if accountFilter != nil {
		for _, acc := range accounts {
			if acc.ID == *accountFilter {
				manifest.Filters.Account = acc.Identifier
				break
			}
		}
	} else if len(accounts) == 1 {
		manifest.Filters.Account = accounts[0].Identifier
	}

	// Set context filters from first aggregate
	if len(aggregateSelection) > 0 {
		for key := range aggregateSelection {
			switch aggregateViewType {
			case query.ViewSenders:
				manifest.Filters.Sender = key
			case query.ViewRecipients:
				manifest.Filters.Recipient = key
			case query.ViewDomains:
				manifest.Filters.SenderDomain = key
			case query.ViewLabels:
				manifest.Filters.Label = key
			}
			break
		}
	}

	return manifest, nil
}

// ExportAttachments performs the export logic.
func (c *ActionController) ExportAttachments(detail *query.MessageDetail, selection map[int]bool) tea.Cmd {
	if detail == nil || len(detail.Attachments) == 0 {
		return nil
	}

	var selectedAttachments []query.AttachmentInfo
	for i, att := range detail.Attachments {
		if selection[i] {
			selectedAttachments = append(selectedAttachments, att)
		}
	}

	if len(selectedAttachments) == 0 {
		return nil
	}

	attachmentsDir := filepath.Join(c.dataDir, "attachments")
	subject := detail.Subject
	if subject == "" {
		subject = "attachments"
	}
	subject = sanitizeFilename(subject)
	if len(subject) > 50 {
		subject = subject[:50]
	}
	zipFilename := fmt.Sprintf("%s_%d.zip", subject, detail.ID)

	return func() tea.Msg {
		return doExportAttachments(zipFilename, attachmentsDir, selectedAttachments)
	}
}

// doExportAttachments performs the actual export work and returns the result message.
func doExportAttachments(zipFilename, attachmentsDir string, attachments []query.AttachmentInfo) ExportResultMsg {
	zipFile, err := os.Create(zipFilename)
	if err != nil {
		return ExportResultMsg{Err: fmt.Errorf("failed to create zip file: %w", err)}
	}
	// Don't defer Close - we need to handle errors and avoid double-close

	zipWriter := zip.NewWriter(zipFile)

	var exportedCount int
	var totalSize int64
	var errors []string
	var writeError bool

	usedNames := make(map[string]int)
	for _, att := range attachments {
		if att.ContentHash == "" {
			errors = append(errors, fmt.Sprintf("%s: missing content hash", att.Filename))
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
			return ExportResultMsg{Result: "Export failed due to write errors. Zip file removed.\n\nErrors:\n" + strings.Join(errors, "\n")}
		}
		return ExportResultMsg{Result: "No attachments exported.\n\nErrors:\n" + strings.Join(errors, "\n")}
	}

	cwd, _ := os.Getwd()
	fullPath := filepath.Join(cwd, zipFilename)
	result := fmt.Sprintf("Exported %d attachment(s) (%s)\n\nSaved to:\n%s",
		exportedCount, formatBytesLong(totalSize), fullPath)
	if len(errors) > 0 {
		result += "\n\nErrors:\n" + strings.Join(errors, "\n")
	}
	return ExportResultMsg{Result: result}
}

// sanitizeFilename removes or replaces characters that are invalid in filenames.
func sanitizeFilename(s string) string {
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

// formatBytesLong formats bytes with full precision for export results.
func formatBytesLong(b int64) string {
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
