package web

import (
	"bytes"
	"fmt"
	"log/slog"
	"net/http"
	"regexp"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/wesm/msgvault/internal/deletion"
	"github.com/wesm/msgvault/internal/web/templates"
)

// validManifestID matches the format produced by deletion.generateID:
// YYYYMMDD-HHMMSS-<sanitized description up to 20 chars>
var validManifestID = regexp.MustCompile(`^[0-9]{8}-[0-9]{6}-[a-zA-Z0-9_-]{1,20}$`)

func (h *Handler) handleDeletions(w http.ResponseWriter, r *http.Request) {
	if h.deletions == nil {
		http.Error(w, "Deletion staging not available", http.StatusServiceUnavailable)
		return
	}

	pending, err := h.deletions.ListPending()
	if err != nil {
		slog.Error("failed to list pending deletions", "error", err)
	}
	inProgress, err := h.deletions.ListInProgress()
	if err != nil {
		slog.Error("failed to list in-progress deletions", "error", err)
	}
	completed, err := h.deletions.ListCompleted()
	if err != nil {
		slog.Error("failed to list completed deletions", "error", err)
	}
	failed, err := h.deletions.ListFailed()
	if err != nil {
		slog.Error("failed to list failed deletions", "error", err)
	}

	flash := r.URL.Query().Get("flash")
	flashCount, _ := strconv.Atoi(r.URL.Query().Get("count"))

	data := templates.DeletionsData{
		Pending:    pending,
		InProgress: inProgress,
		Completed:  completed,
		Failed:     failed,
		Flash:      flash,
		FlashCount: flashCount,
	}

	var buf bytes.Buffer
	if err := templates.DeletionsPage(data).Render(r.Context(), &buf); err != nil {
		slog.Error("failed to render deletions", "error", err)
		http.Error(w, "Failed to render page", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = buf.WriteTo(w)
}

// handleStageBatch stages multiple messages for deletion from checkbox selection.
// Accepts gmail_id[] form values posted from message list checkboxes.
func (h *Handler) handleStageBatch(w http.ResponseWriter, r *http.Request) {
	if h.deletions == nil {
		http.Error(w, "Deletion staging not available", http.StatusServiceUnavailable)
		return
	}

	// Limit form body size before parsing to prevent memory exhaustion.
	r.Body = http.MaxBytesReader(w, r.Body, 2<<20) // 2 MB
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Invalid form data", http.StatusBadRequest)
		return
	}

	ctx := r.Context()

	gmailIDs := r.Form["gmail_id"]
	if len(gmailIDs) == 0 {
		http.Redirect(w, r, "/messages", http.StatusSeeOther)
		return
	}

	// Filter out empty Gmail IDs (messages without a source ID).
	filtered := gmailIDs[:0]
	for _, id := range gmailIDs {
		if id != "" {
			filtered = append(filtered, id)
		}
	}
	gmailIDs = filtered
	if len(gmailIDs) == 0 {
		http.Redirect(w, r, "/messages", http.StatusSeeOther)
		return
	}

	const maxBatchSize = 10000
	if len(gmailIDs) > maxBatchSize {
		http.Error(w, fmt.Sprintf("Too many messages (max %d)", maxBatchSize), http.StatusBadRequest)
		return
	}

	// Determine account from selected messages. Sample first and last to
	// detect mixed-account selections (which cannot be executed correctly).
	var account string
	msg, err := h.engine.GetMessageBySourceID(ctx, gmailIDs[0])
	if err == nil && msg != nil {
		account = msg.AccountEmail
	}
	if len(gmailIDs) > 1 && account != "" {
		lastMsg, err := h.engine.GetMessageBySourceID(ctx, gmailIDs[len(gmailIDs)-1])
		if err == nil && lastMsg != nil && lastMsg.AccountEmail != account {
			http.Error(w, "Selection contains messages from multiple accounts. Stage each account separately.", http.StatusBadRequest)
			return
		}
	}

	description := fmt.Sprintf("Web selection (%d messages)", len(gmailIDs))

	manifest := deletion.NewManifest(description, gmailIDs)
	manifest.Filters = deletion.Filters{Account: account}
	manifest.CreatedBy = "web"

	if err := h.deletions.SaveManifest(manifest); err != nil {
		slog.Error("failed to save manifest", "error", err)
		http.Error(w, "Failed to save manifest", http.StatusInternalServerError)
		return
	}

	http.Redirect(w, r, fmt.Sprintf("/deletions?flash=staged&count=%d", len(gmailIDs)), http.StatusSeeOther)
}

// handleStageMessage stages a single message for deletion by its database ID.
func (h *Handler) handleStageMessage(w http.ResponseWriter, r *http.Request) {
	if h.deletions == nil {
		http.Error(w, "Deletion staging not available", http.StatusServiceUnavailable)
		return
	}

	ctx := r.Context()

	msgIDStr := chi.URLParam(r, "id")
	msgID, err := strconv.ParseInt(msgIDStr, 10, 64)
	if err != nil {
		http.Error(w, "Invalid message ID", http.StatusBadRequest)
		return
	}

	msg, err := h.engine.GetMessage(ctx, msgID)
	if err != nil {
		slog.Error("failed to load message for deletion", "error", err, "id", msgID)
		http.Error(w, "Failed to load message", http.StatusInternalServerError)
		return
	}
	if msg == nil {
		http.Error(w, "Message not found", http.StatusNotFound)
		return
	}

	if msg.SourceMessageID == "" {
		http.Error(w, "Message has no Gmail ID", http.StatusBadRequest)
		return
	}

	description := fmt.Sprintf("Message: %s", msg.Subject)
	if description == "Message: " {
		description = "Message: (no subject)"
	}

	manifest := deletion.NewManifest(description, []string{msg.SourceMessageID})
	manifest.Filters = deletion.Filters{Account: msg.AccountEmail}
	manifest.CreatedBy = "web"

	if err := h.deletions.SaveManifest(manifest); err != nil {
		slog.Error("failed to save manifest", "error", err)
		http.Error(w, "Failed to save manifest", http.StatusInternalServerError)
		return
	}

	http.Redirect(w, r, "/deletions?flash=staged&count=1", http.StatusSeeOther)
}

func (h *Handler) handleCancelDeletion(w http.ResponseWriter, r *http.Request) {
	if h.deletions == nil {
		http.Error(w, "Deletion staging not available", http.StatusServiceUnavailable)
		return
	}

	id := chi.URLParam(r, "id")
	if !validManifestID.MatchString(id) {
		http.Error(w, "Invalid batch ID", http.StatusBadRequest)
		return
	}

	if err := h.deletions.CancelManifest(id); err != nil {
		slog.Error("failed to cancel manifest", "error", err, "id", id)
		http.Error(w, fmt.Sprintf("Failed to cancel: %v", err), http.StatusInternalServerError)
		return
	}

	http.Redirect(w, r, "/deletions", http.StatusSeeOther)
}
