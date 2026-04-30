package store_test

import (
	"testing"

	"github.com/wesm/msgvault/internal/store"
)

func TestLiveMessagesWhere_NoAlias(t *testing.T) {
	got := store.LiveMessagesWhere("")
	want := "deleted_at IS NULL AND deleted_from_source_at IS NULL"
	if got != want {
		t.Errorf("LiveMessagesWhere(%q) = %q, want %q", "", got, want)
	}
}

func TestLiveMessagesWhere_WithAlias(t *testing.T) {
	got := store.LiveMessagesWhere("m")
	want := "m.deleted_at IS NULL AND m.deleted_from_source_at IS NULL"
	if got != want {
		t.Errorf("LiveMessagesWhere(%q) = %q, want %q", "m", got, want)
	}
}
