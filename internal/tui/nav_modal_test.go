package tui

import (
	"testing"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/wesm/msgvault/internal/query"
)

// =============================================================================
// Quit Confirmation Modal Tests
// =============================================================================

func TestQuitConfirmationModal(t *testing.T) {
	model := NewBuilder().Build()

	// Press 'q' should open quit confirmation, not quit immediately
	var cmd tea.Cmd
	model, cmd = sendKey(t, model, key('q'))

	assertModal(t, model, modalQuitConfirm)

	if model.quitting {
		t.Error("should not be quitting yet")
	}
	if cmd != nil {
		t.Error("should not have quit command yet")
	}

	// Press 'n' to cancel
	model, _ = sendKey(t, model, key('n'))

	assertModal(t, model, modalNone)
}

func TestQuitConfirmationConfirm(t *testing.T) {
	model := NewBuilder().WithModal(modalQuitConfirm).WithPageSize(10).WithSize(100, 20).Build()

	// Press 'y' to confirm quit
	m, cmd := applyModalKey(t, model, key('y'))

	if !m.quitting {
		t.Error("expected quitting = true")
	}
	if cmd == nil {
		t.Error("expected quit command")
	}
}

// =============================================================================
// Account Selector Modal Tests
// =============================================================================

func TestAccountSelectorModal(t *testing.T) {
	model := NewBuilder().
		WithAccounts(
			query.AccountInfo{ID: 1, Identifier: "alice@example.com"},
			query.AccountInfo{ID: 2, Identifier: "bob@example.com"},
		).
		WithPageSize(10).WithSize(100, 20).
		Build()

	// Press 'A' to open account selector
	m := applyAggregateKey(t, model, key('A'))

	if m.modal != modalAccountSelector {
		t.Errorf("expected modalAccountSelector, got %v", m.modal)
	}
	if m.modalCursor != 0 {
		t.Errorf("expected modalCursor = 0 (All Accounts), got %d", m.modalCursor)
	}

	// Navigate down
	m, _ = applyModalKey(t, m, key('j'))
	if m.modalCursor != 1 {
		t.Errorf("expected modalCursor = 1, got %d", m.modalCursor)
	}

	// Select account
	var cmd tea.Cmd
	m, cmd = applyModalKey(t, m, keyEnter())

	if m.modal != modalNone {
		t.Errorf("expected modalNone after selection, got %v", m.modal)
	}
	if m.accountFilter == nil || *m.accountFilter != 1 {
		t.Errorf("expected accountFilter = 1, got %v", m.accountFilter)
	}
	if cmd == nil {
		t.Error("expected command to reload data")
	}
}

func TestOpenAccountSelector(t *testing.T) {
	t.Run("no accounts", func(t *testing.T) {
		m := NewBuilder().Build()
		m.openAccountSelector()
		assertModal(t, m, modalAccountSelector)
		if m.modalCursor != 0 {
			t.Errorf("expected modalCursor 0, got %d", m.modalCursor)
		}
	})

	t.Run("with matching filter", func(t *testing.T) {
		acctID := int64(42)
		m := NewBuilder().WithAccounts(
			query.AccountInfo{ID: 10, Identifier: "a@example.com"},
			query.AccountInfo{ID: 42, Identifier: "b@example.com"},
		).Build()
		m.accountFilter = &acctID
		m.openAccountSelector()
		assertModal(t, m, modalAccountSelector)
		if m.modalCursor != 2 { // index 1 + 1 for "All Accounts"
			t.Errorf("expected modalCursor 2, got %d", m.modalCursor)
		}
	})
}

// =============================================================================
// Attachment Filter Modal Tests
// =============================================================================

func TestAttachmentFilterModal(t *testing.T) {
	model := NewBuilder().WithPageSize(10).WithSize(100, 20).Build()

	// Press 'f' to open filter modal
	m := applyAggregateKey(t, model, key('f'))

	if m.modal != modalAttachmentFilter {
		t.Errorf("expected modalAttachmentFilter, got %v", m.modal)
	}
	if m.modalCursor != 0 {
		t.Errorf("expected modalCursor = 0 (All Messages), got %d", m.modalCursor)
	}

	// Navigate down to "With Attachments"
	m, _ = applyModalKey(t, m, key('j'))
	if m.modalCursor != 1 {
		t.Errorf("expected modalCursor = 1, got %d", m.modalCursor)
	}

	// Select "With Attachments"
	m, _ = applyModalKey(t, m, keyEnter())

	if m.modal != modalNone {
		t.Errorf("expected modalNone after selection, got %v", m.modal)
	}
	if !m.attachmentFilter {
		t.Error("expected attachmentFilter = true")
	}
}

func TestAttachmentFilterInMessageList(t *testing.T) {
	model := NewBuilder().WithLevel(levelMessageList).WithPageSize(10).WithSize(100, 20).Build()

	// Press 'f' to open filter modal in message list
	m := applyMessageListKey(t, model, key('f'))

	if m.modal != modalAttachmentFilter {
		t.Errorf("expected modalAttachmentFilter, got %v", m.modal)
	}

	// Select "With Attachments" and verify reload is triggered
	m.modalCursor = 1
	var cmd tea.Cmd
	m, cmd = applyModalKey(t, m, keyEnter())

	if !m.attachmentFilter {
		t.Error("expected attachmentFilter = true")
	}
	if cmd == nil {
		t.Error("expected command to reload messages")
	}
}

func TestOpenAttachmentFilter(t *testing.T) {
	m := NewBuilder().Build()

	m.attachmentFilter = false
	m.openAttachmentFilter()
	if m.modalCursor != 0 {
		t.Errorf("expected modalCursor 0 for no filter, got %d", m.modalCursor)
	}

	m.attachmentFilter = true
	m.openAttachmentFilter()
	if m.modalCursor != 1 {
		t.Errorf("expected modalCursor 1 for attachment filter, got %d", m.modalCursor)
	}
}
