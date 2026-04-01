package store_test

import (
	"testing"

	"github.com/wesm/msgvault/internal/testutil"
)

func TestEnsureParticipantByPhone_IdentifierType(t *testing.T) {
	st := testutil.NewTestStore(t)

	// Create participant via WhatsApp
	id1, err := st.EnsureParticipantByPhone("+15551234567", "Alice", "whatsapp")
	if err != nil {
		t.Fatalf("EnsureParticipantByPhone(whatsapp): %v", err)
	}
	if id1 == 0 {
		t.Fatal("expected non-zero participant ID")
	}

	// Same phone via iMessage — should return the same participant ID
	id2, err := st.EnsureParticipantByPhone("+15551234567", "Alice", "imessage")
	if err != nil {
		t.Fatalf("EnsureParticipantByPhone(imessage): %v", err)
	}
	if id2 != id1 {
		t.Errorf("imessage call returned participant ID %d, want %d (same as whatsapp)", id2, id1)
	}

	// Both participant_identifiers rows should exist
	var count int
	err = st.DB().QueryRow(
		`SELECT COUNT(*) FROM participant_identifiers WHERE participant_id = ?`,
		id1,
	).Scan(&count)
	if err != nil {
		t.Fatalf("count participant_identifiers: %v", err)
	}
	if count != 2 {
		t.Errorf("participant_identifiers count = %d, want 2", count)
	}

	// Verify each identifier type is present
	for _, identType := range []string{"whatsapp", "imessage"} {
		var exists int
		err = st.DB().QueryRow(
			`SELECT COUNT(*) FROM participant_identifiers
			 WHERE participant_id = ? AND identifier_type = ?`,
			id1, identType,
		).Scan(&exists)
		if err != nil {
			t.Fatalf("check identifier_type %q: %v", identType, err)
		}
		if exists != 1 {
			t.Errorf("identifier_type %q count = %d, want 1", identType, exists)
		}
	}
}
