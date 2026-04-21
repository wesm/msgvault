package store_test

import (
	"database/sql"
	"testing"

	"github.com/wesm/msgvault/internal/store"
	"github.com/wesm/msgvault/internal/testutil"
	"github.com/wesm/msgvault/internal/testutil/storetest"
)

func addMessageFromParticipant(
	t *testing.T, f *storetest.Fixture,
	source *store.Source,
	srcMessageID, fromEmail string,
	isFromMe bool,
) int64 {
	t.Helper()
	pid, err := f.Store.EnsureParticipant(fromEmail, "", "")
	testutil.MustNoErr(t, err, "EnsureParticipant "+fromEmail)

	convID, err := f.Store.EnsureConversation(
		source.ID, "conv-"+srcMessageID, "Subject",
	)
	testutil.MustNoErr(t, err, "EnsureConversation")

	mid, err := f.Store.UpsertMessage(&store.Message{
		ConversationID:  convID,
		SourceID:        source.ID,
		SourceMessageID: srcMessageID,
		MessageType:     "email",
		IsFromMe:        isFromMe,
		SizeEstimate:    1000,
	})
	testutil.MustNoErr(t, err, "UpsertMessage")

	testutil.MustNoErr(t,
		f.Store.ReplaceMessageRecipients(
			mid, "from", []int64{pid}, []string{""},
		),
		"ReplaceMessageRecipients",
	)
	return mid
}

func TestListLikelyIdentities_SignalsFromMe(t *testing.T) {
	f := storetest.New(t)
	for i := 1; i <= 3; i++ {
		addMessageFromParticipant(
			t, f, f.Source,
			"m"+string(rune('0'+i)),
			"alice@example.com",
			true,
		)
	}

	ids, err := f.Store.ListLikelyIdentities()
	testutil.MustNoErr(t, err, "ListLikelyIdentities")

	if len(ids) != 1 {
		t.Fatalf("got %d candidates, want 1", len(ids))
	}
	got := ids[0]
	if got.Email != "alice@example.com" {
		t.Errorf("email = %q, want alice@example.com", got.Email)
	}
	if got.MessageCount != 3 {
		t.Errorf("count = %d, want 3", got.MessageCount)
	}
	if got.Signals&store.SignalFromMe == 0 {
		t.Errorf("SignalFromMe not set: %v", got.Signals)
	}
}

func TestListLikelyIdentities_SentLabelWithoutIsFromMe(t *testing.T) {
	f := storetest.New(t)
	mid := addMessageFromParticipant(
		t, f, f.Source, "m1", "alice@example.com", false,
	)

	lid, err := f.Store.EnsureLabel(
		f.Source.ID, "SENT", "Sent", "system",
	)
	testutil.MustNoErr(t, err, "EnsureLabel SENT")
	testutil.MustNoErr(t,
		f.Store.LinkMessageLabel(mid, lid),
		"LinkMessageLabel",
	)

	ids, err := f.Store.ListLikelyIdentities()
	testutil.MustNoErr(t, err, "ListLikelyIdentities")

	if len(ids) != 1 {
		t.Fatalf("got %d candidates, want 1", len(ids))
	}
	got := ids[0]
	if got.Signals&store.SignalSentLabel == 0 {
		t.Errorf("SignalSentLabel not set: %v", got.Signals)
	}
	if got.Signals&store.SignalFromMe != 0 {
		t.Errorf("SignalFromMe should not fire: %v", got.Signals)
	}
}

func TestListLikelyIdentities_AccountIdentifierMatch(t *testing.T) {
	f := storetest.New(t)
	addMessageFromParticipant(
		t, f, f.Source, "m1", "test@example.com", false,
	)

	ids, err := f.Store.ListLikelyIdentities()
	testutil.MustNoErr(t, err, "ListLikelyIdentities")

	if len(ids) != 1 {
		t.Fatalf("got %d candidates, want 1", len(ids))
	}
	got := ids[0]
	if got.Signals != store.SignalAccountMatch {
		t.Errorf("signals = %v, want SignalAccountMatch only", got.Signals)
	}
	if got.Email != "test@example.com" {
		t.Errorf("email = %q, want test@example.com", got.Email)
	}
}

func TestListLikelyIdentities_ExcludesOtherPeople(t *testing.T) {
	f := storetest.New(t)
	addMessageFromParticipant(
		t, f, f.Source, "m1", "stranger@elsewhere.org", false,
	)

	ids, err := f.Store.ListLikelyIdentities()
	testutil.MustNoErr(t, err, "ListLikelyIdentities")

	if len(ids) != 0 {
		t.Errorf("got %d candidates, want 0: %v", len(ids), ids)
	}
}

func TestListLikelyIdentities_RankedByCount(t *testing.T) {
	f := storetest.New(t)

	for i := 1; i <= 3; i++ {
		addMessageFromParticipant(
			t, f, f.Source,
			"a"+string(rune('0'+i)),
			"alice@example.com",
			true,
		)
	}
	addMessageFromParticipant(
		t, f, f.Source, "b1", "bob@example.com", true,
	)

	ids, err := f.Store.ListLikelyIdentities()
	testutil.MustNoErr(t, err, "ListLikelyIdentities")
	if len(ids) != 2 {
		t.Fatalf("got %d candidates, want 2", len(ids))
	}
	if ids[0].Email != "alice@example.com" {
		t.Errorf("first = %q, want alice@example.com", ids[0].Email)
	}
	if ids[0].MessageCount != 3 {
		t.Errorf("alice count = %d, want 3", ids[0].MessageCount)
	}
	if ids[1].Email != "bob@example.com" {
		t.Errorf("second = %q, want bob@example.com", ids[1].Email)
	}
}

func TestListLikelyIdentities_ScopedToSources(t *testing.T) {
	f := storetest.New(t)

	addMessageFromParticipant(
		t, f, f.Source, "m1", "alice@example.com", true,
	)

	src2, err := f.Store.GetOrCreateSource("gmail", "bob@other.com")
	testutil.MustNoErr(t, err, "GetOrCreateSource bob")
	addMessageFromParticipant(
		t, f, src2, "m2", "bob@other.com", true,
	)

	ids, err := f.Store.ListLikelyIdentities(f.Source.ID)
	testutil.MustNoErr(t, err, "ListLikelyIdentities scoped")
	if len(ids) != 1 || ids[0].Email != "alice@example.com" {
		t.Errorf("scoped result = %v, want [alice@example.com]", ids)
	}

	ids, err = f.Store.ListLikelyIdentities()
	testutil.MustNoErr(t, err, "ListLikelyIdentities all")
	if len(ids) != 2 {
		t.Errorf("unscoped count = %d, want 2", len(ids))
	}
}

func TestListLikelyIdentities_ExcludesSoftDeleted(t *testing.T) {
	f := storetest.New(t)
	mid := addMessageFromParticipant(
		t, f, f.Source, "m1", "alice@example.com", true,
	)
	_, err := f.Store.DB().Exec(
		"UPDATE messages SET deleted_at = datetime('now') WHERE id = ?",
		mid,
	)
	testutil.MustNoErr(t, err, "soft-delete")

	ids, err := f.Store.ListLikelyIdentities()
	testutil.MustNoErr(t, err, "ListLikelyIdentities")
	if len(ids) != 0 {
		t.Errorf("soft-deleted message should be excluded: %v", ids)
	}

	var deletedAt sql.NullTime
	err = f.Store.DB().QueryRow(
		"SELECT deleted_at FROM messages WHERE id = ?", mid,
	).Scan(&deletedAt)
	testutil.MustNoErr(t, err, "scan deleted_at")
	if !deletedAt.Valid {
		t.Error("deleted_at should be set")
	}
}

func TestListLikelyIdentities_AllThreeSignals(t *testing.T) {
	f := storetest.New(t)
	mid := addMessageFromParticipant(
		t, f, f.Source, "m1", "test@example.com", true,
	)
	lid, err := f.Store.EnsureLabel(f.Source.ID, "SENT", "Sent", "system")
	testutil.MustNoErr(t, err, "EnsureLabel")
	testutil.MustNoErr(t, f.Store.LinkMessageLabel(mid, lid), "LinkMessageLabel")

	ids, err := f.Store.ListLikelyIdentities()
	testutil.MustNoErr(t, err, "ListLikelyIdentities")
	if len(ids) != 1 {
		t.Fatalf("got %d candidates, want 1", len(ids))
	}

	got := ids[0]
	want := store.SignalFromMe | store.SignalSentLabel | store.SignalAccountMatch
	if got.Signals != want {
		t.Errorf("signals = %v, want all three: %v", got.Signals, want)
	}
}

func TestListLikelyIdentities_CaseInsensitive(t *testing.T) {
	f := storetest.New(t)
	addMessageFromParticipant(
		t, f, f.Source, "m1", "Alice@Example.COM", true,
	)

	ids, err := f.Store.ListLikelyIdentities()
	testutil.MustNoErr(t, err, "ListLikelyIdentities")
	if len(ids) != 1 {
		t.Fatalf("got %d candidates, want 1", len(ids))
	}
	if ids[0].Email != "alice@example.com" {
		t.Errorf("email = %q, want lower-cased alice@example.com", ids[0].Email)
	}
}
