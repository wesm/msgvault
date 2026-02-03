package tui

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/wesm/msgvault/internal/deletion"
	"github.com/wesm/msgvault/internal/export"
	"github.com/wesm/msgvault/internal/query"
)

// ExportResultMsg is returned when attachment export completes.
type ExportResultMsg struct {
	Result string
	Err    error
}

// ActionController handles business logic for actions like deletion and export,
// keeping domain operations out of the TUI Model.
type ActionController struct {
	queries   query.Engine
	deletions *deletion.Manager
	dataDir   string
}

// NewActionController creates a new action controller.
// If deletions is nil, the manager will be lazily initialized on first use.
func NewActionController(queries query.Engine, dataDir string, deletions *deletion.Manager) *ActionController {
	return &ActionController{
		queries:   queries,
		deletions: deletions,
		dataDir:   dataDir,
	}
}

// SaveManifest initializes the deletion manager if needed and saves the manifest.
func (c *ActionController) SaveManifest(manifest *deletion.Manifest) error {
	if c.deletions == nil {
		deletionsDir := filepath.Join(c.dataDir, "deletions")
		mgr, err := deletion.NewManager(deletionsDir)
		if err != nil {
			return err
		}
		c.deletions = mgr
	}
	return c.deletions.SaveManifest(manifest)
}

// StageForDeletion prepares messages for deletion based on selection.
func (c *ActionController) StageForDeletion(aggregateSelection map[string]bool, messageSelection map[int64]bool, aggregateViewType query.ViewType, accountFilter *int64, accounts []query.AccountInfo, currentViewType query.ViewType, currentFilterKey string, timeGranularity query.TimeGranularity, messages []query.MessageSummary, drillFilter *query.MessageFilter) (*deletion.Manifest, error) {
	// Collect Gmail IDs to delete
	gmailIDSet := make(map[string]bool)
	ctx := context.Background()

	// From selected aggregates - resolve to Gmail IDs via query engine
	if len(aggregateSelection) > 0 {
		for key := range aggregateSelection {
			// Start with drill-down filter as base (preserves parent context)
			var filter query.MessageFilter
			if drillFilter != nil {
				filter = *drillFilter
			}
			if accountFilter != nil {
				filter.SourceID = accountFilter
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
				filter.TimeRange.Period = key
				filter.TimeRange.Granularity = timeGranularity
			}

			ids, err := c.queries.GetGmailIDsByFilter(ctx, filter)
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

	// Set context filters from all selected aggregates
	if len(aggregateSelection) > 0 {
		keys := make([]string, 0, len(aggregateSelection))
		for key := range aggregateSelection {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		switch aggregateViewType {
		case query.ViewSenders:
			manifest.Filters.Senders = keys
		case query.ViewRecipients:
			manifest.Filters.Recipients = keys
		case query.ViewDomains:
			manifest.Filters.SenderDomains = keys
		case query.ViewLabels:
			manifest.Filters.Labels = keys
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
	subject = export.SanitizeFilename(subject)
	if len(subject) > 50 {
		subject = subject[:50]
	}
	zipFilename := fmt.Sprintf("%s_%d.zip", subject, detail.ID)

	return func() tea.Msg {
		stats := export.Attachments(zipFilename, attachmentsDir, selectedAttachments)
		return ExportResultMsg{Result: export.FormatExportResult(stats)}
	}
}
