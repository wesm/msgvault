// Package web provides the server-rendered web UI for msgvault.
package web

import (
	"embed"
	"fmt"
	"io/fs"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/wesm/msgvault/internal/deletion"
	"github.com/wesm/msgvault/internal/query"
	"github.com/wesm/msgvault/internal/scheduler"
)

//go:embed static
var staticFS embed.FS

// SyncScheduler triggers syncs for scheduled accounts.
type SyncScheduler interface {
	IsScheduled(email string) bool
	TriggerSync(email string) error
	Status() []scheduler.AccountStatus
}

// Handler serves the web UI.
type Handler struct {
	engine         query.Engine
	deletions      *deletion.Manager
	scheduler      SyncScheduler
	staticFS       fs.FS
	attachmentsDir string
}

// NewHandler creates a new web UI handler.
func NewHandler(engine query.Engine, deletions *deletion.Manager, attachmentsDir string, scheduler SyncScheduler) *Handler {
	staticSub, err := fs.Sub(staticFS, "static")
	if err != nil {
		panic(fmt.Sprintf("web: failed to sub static FS: %v", err))
	}
	return &Handler{engine: engine, deletions: deletions, scheduler: scheduler, staticFS: staticSub, attachmentsDir: attachmentsDir}
}

// Routes returns a chi.Router with all web UI routes mounted.
func (h *Handler) Routes() chi.Router {
	r := chi.NewRouter()

	// Static assets (no auth needed for CSS/JS)
	fileServer := http.StripPrefix("/static/", http.FileServer(http.FS(h.staticFS)))
	r.Handle("/static/*", fileServer)

	// Pages
	r.Get("/", h.handleDashboard)
	r.Get("/browse", h.handleBrowse)
	r.Get("/browse/drill", h.handleDrill)
	r.Get("/messages", h.handleMessages)
	r.Get("/messages/{id}", h.handleMessageDetail)
	r.Get("/attachments/{id}/download", h.handleAttachmentDownload)
	r.Get("/search", h.handleSearch)

	// Sync trigger and status
	r.Post("/sync/{identifier}", h.handleTriggerSync)
	r.Get("/sync/{identifier}/status", h.handleSyncStatus)

	// Deletion staging
	r.Get("/deletions", h.handleDeletions)
	r.Post("/deletions/stage-batch", h.handleStageBatch)
	r.Post("/deletions/stage/{id}", h.handleStageMessage)
	r.Post("/deletions/{id}/cancel", h.handleCancelDeletion)

	return r
}
