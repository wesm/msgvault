// Package deletion provides safe, staged email deletion from Gmail.
package deletion

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// Status represents the state of a deletion batch.
type Status string

const (
	StatusPending    Status = "pending"
	StatusInProgress Status = "in_progress"
	StatusCompleted  Status = "completed"
	StatusFailed     Status = "failed"
	StatusCancelled  Status = "cancelled"
)

// Method represents how messages are deleted.
type Method string

const (
	MethodTrash  Method = "trash"  // Move to Gmail trash (30-day recovery)
	MethodDelete Method = "delete" // Permanent deletion
)

// Filters specifies criteria for selecting messages.
type Filters struct {
	Senders      []string `json:"senders,omitempty"`
	SenderDomains []string `json:"sender_domains,omitempty"`
	Recipients   []string `json:"recipients,omitempty"`
	Labels       []string `json:"labels,omitempty"`
	After        string   `json:"after,omitempty"`  // ISO date
	Before       string   `json:"before,omitempty"` // ISO date
	Account      string   `json:"account,omitempty"`
}

// Summary contains statistics about messages to be deleted.
type Summary struct {
	MessageCount   int           `json:"message_count"`
	TotalSizeBytes int64         `json:"total_size_bytes"`
	DateRange      [2]string     `json:"date_range"` // [earliest, latest]
	Accounts       []string      `json:"accounts"`
	TopSenders     []SenderCount `json:"top_senders"`
}

// SenderCount represents a sender and their message count.
type SenderCount struct {
	Sender string `json:"sender"`
	Count  int    `json:"count"`
}

// Execution tracks progress of a deletion operation.
type Execution struct {
	StartedAt          time.Time  `json:"started_at"`
	CompletedAt        *time.Time `json:"completed_at,omitempty"`
	Method             Method     `json:"method"`
	Succeeded          int        `json:"succeeded"`
	Failed             int        `json:"failed"`
	FailedIDs          []string   `json:"failed_ids,omitempty"`
	LastProcessedIndex int        `json:"last_processed_index"` // For resumability
}

// Manifest represents a deletion batch.
type Manifest struct {
	Version     int        `json:"version"`
	ID          string     `json:"id"`
	CreatedAt   time.Time  `json:"created_at"`
	CreatedBy   string     `json:"created_by"` // "tui", "cli", "api"
	Description string     `json:"description"`
	Filters     Filters    `json:"filters"`
	Summary     *Summary   `json:"summary,omitempty"`
	GmailIDs    []string   `json:"gmail_ids"`
	Status      Status     `json:"status"`
	Execution   *Execution `json:"execution,omitempty"`
}

// NewManifest creates a new deletion manifest.
func NewManifest(description string, gmailIDs []string) *Manifest {
	return &Manifest{
		Version:     1,
		ID:          generateID(description),
		CreatedAt:   time.Now(),
		CreatedBy:   "cli",
		Description: description,
		GmailIDs:    gmailIDs,
		Status:      StatusPending,
	}
}

// generateID creates a manifest ID from timestamp and description.
func generateID(description string) string {
	ts := time.Now().Format("20060102-150405")
	// Sanitize description for filename
	sanitized := sanitizeForFilename(description)
	if sanitized == "" {
		sanitized = "batch"
	}
	if len(sanitized) > 20 {
		sanitized = sanitized[:20]
	}
	return fmt.Sprintf("%s-%s", ts, sanitized)
}

// sanitizeForFilename removes characters unsafe for filenames.
func sanitizeForFilename(s string) string {
	result := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
			(c >= '0' && c <= '9') || c == '-' || c == '_' {
			result = append(result, c)
		} else if c == ' ' || c == '.' {
			result = append(result, '-')
		}
	}
	return string(result)
}

// LoadManifest reads a manifest from a JSON file.
func LoadManifest(path string) (*Manifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}

	return &m, nil
}

// Save writes the manifest to a JSON file.
func (m *Manifest) Save(path string) error {
	// Ensure parent directory exists
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}

	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(path, data, 0600)
}

// FormatSummary returns a human-readable summary of the deletion.
func (m *Manifest) FormatSummary() string {
	var result string

	result += fmt.Sprintf("Deletion Batch: %s\n", m.ID)
	result += fmt.Sprintf("Status: %s\n", m.Status)
	result += fmt.Sprintf("Created: %s\n", m.CreatedAt.Format(time.RFC3339))
	result += fmt.Sprintf("Description: %s\n", m.Description)
	result += fmt.Sprintf("Messages: %d\n", len(m.GmailIDs))

	if m.Summary != nil {
		result += fmt.Sprintf("Total Size: %.2f MB\n", float64(m.Summary.TotalSizeBytes)/(1024*1024))
		if len(m.Summary.DateRange) == 2 && m.Summary.DateRange[0] != "" {
			result += fmt.Sprintf("Date Range: %s to %s\n", m.Summary.DateRange[0], m.Summary.DateRange[1])
		}
		if len(m.Summary.TopSenders) > 0 {
			result += "\nTop Senders:\n"
			for i, s := range m.Summary.TopSenders {
				if i >= 10 {
					break
				}
				result += fmt.Sprintf("  %s: %d messages\n", s.Sender, s.Count)
			}
		}
	}

	if m.Execution != nil {
		result += "\nExecution:\n"
		result += fmt.Sprintf("  Method: %s\n", m.Execution.Method)
		result += fmt.Sprintf("  Succeeded: %d\n", m.Execution.Succeeded)
		result += fmt.Sprintf("  Failed: %d\n", m.Execution.Failed)
		if m.Execution.CompletedAt != nil {
			result += fmt.Sprintf("  Completed: %s\n", m.Execution.CompletedAt.Format(time.RFC3339))
		}
	}

	return result
}

// Manager handles deletion manifest files.
type Manager struct {
	baseDir string // ~/.msgvault/deletions
}

// NewManager creates a deletion manager.
func NewManager(baseDir string) (*Manager, error) {
	m := &Manager{baseDir: baseDir}

	// Create directory structure
	dirs := []string{
		filepath.Join(baseDir, "pending"),
		filepath.Join(baseDir, "in_progress"),
		filepath.Join(baseDir, "completed"),
		filepath.Join(baseDir, "failed"),
	}
	for _, d := range dirs {
		if err := os.MkdirAll(d, 0755); err != nil {
			return nil, fmt.Errorf("create %s: %w", d, err)
		}
	}

	return m, nil
}

// PendingDir returns the path to the pending directory.
func (m *Manager) PendingDir() string {
	return filepath.Join(m.baseDir, "pending")
}

// InProgressDir returns the path to the in_progress directory.
func (m *Manager) InProgressDir() string {
	return filepath.Join(m.baseDir, "in_progress")
}

// CompletedDir returns the path to the completed directory.
func (m *Manager) CompletedDir() string {
	return filepath.Join(m.baseDir, "completed")
}

// FailedDir returns the path to the failed directory.
func (m *Manager) FailedDir() string {
	return filepath.Join(m.baseDir, "failed")
}

// ListPending returns all pending deletion manifests.
func (m *Manager) ListPending() ([]*Manifest, error) {
	return m.listManifests(m.PendingDir())
}

// ListInProgress returns all in-progress deletion manifests.
func (m *Manager) ListInProgress() ([]*Manifest, error) {
	return m.listManifests(m.InProgressDir())
}

// ListCompleted returns all completed deletion manifests.
func (m *Manager) ListCompleted() ([]*Manifest, error) {
	return m.listManifests(m.CompletedDir())
}

// ListFailed returns all failed deletion manifests.
func (m *Manager) ListFailed() ([]*Manifest, error) {
	return m.listManifests(m.FailedDir())
}

func (m *Manager) listManifests(dir string) ([]*Manifest, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var manifests []*Manifest
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".json" {
			continue
		}

		path := filepath.Join(dir, e.Name())
		manifest, err := LoadManifest(path)
		if err != nil {
			continue // Skip invalid manifests
		}
		manifests = append(manifests, manifest)
	}

	// Sort by created time, newest first
	sort.Slice(manifests, func(i, j int) bool {
		return manifests[i].CreatedAt.After(manifests[j].CreatedAt)
	})

	return manifests, nil
}

// GetManifest loads a manifest by ID from any status directory.
func (m *Manager) GetManifest(id string) (*Manifest, string, error) {
	dirs := []string{
		m.PendingDir(),
		m.InProgressDir(),
		m.CompletedDir(),
		m.FailedDir(),
	}

	filename := id + ".json"
	for _, dir := range dirs {
		path := filepath.Join(dir, filename)
		if manifest, err := LoadManifest(path); err == nil {
			return manifest, path, nil
		}
	}

	return nil, "", fmt.Errorf("manifest %s not found", id)
}

// SaveManifest saves a manifest to the appropriate directory based on status.
func (m *Manager) SaveManifest(manifest *Manifest) error {
	var dir string
	switch manifest.Status {
	case StatusPending:
		dir = m.PendingDir()
	case StatusInProgress:
		dir = m.InProgressDir()
	case StatusCompleted:
		dir = m.CompletedDir()
	case StatusFailed:
		dir = m.FailedDir()
	default:
		dir = m.PendingDir()
	}

	path := filepath.Join(dir, manifest.ID+".json")
	return manifest.Save(path)
}

// MoveManifest moves a manifest from one status directory to another.
func (m *Manager) MoveManifest(id string, fromStatus, toStatus Status) error {
	var fromDir, toDir string

	switch fromStatus {
	case StatusPending:
		fromDir = m.PendingDir()
	case StatusInProgress:
		fromDir = m.InProgressDir()
	default:
		return fmt.Errorf("cannot move from status %s", fromStatus)
	}

	switch toStatus {
	case StatusInProgress:
		toDir = m.InProgressDir()
	case StatusCompleted:
		toDir = m.CompletedDir()
	case StatusFailed:
		toDir = m.FailedDir()
	default:
		return fmt.Errorf("cannot move to status %s", toStatus)
	}

	fromPath := filepath.Join(fromDir, id+".json")
	toPath := filepath.Join(toDir, id+".json")

	return os.Rename(fromPath, toPath)
}

// CancelManifest removes a pending manifest.
func (m *Manager) CancelManifest(id string) error {
	path := filepath.Join(m.PendingDir(), id+".json")
	return os.Remove(path)
}

// CreateManifest creates and saves a new manifest.
func (m *Manager) CreateManifest(description string, gmailIDs []string, filters Filters) (*Manifest, error) {
	manifest := NewManifest(description, gmailIDs)
	manifest.Filters = filters

	if err := m.SaveManifest(manifest); err != nil {
		return nil, err
	}

	return manifest, nil
}
