package store_test

import (
	"database/sql"
	"testing"

	"github.com/wesm/msgvault/internal/testutil"
	"github.com/wesm/msgvault/internal/testutil/storetest"
)

func TestStore_GetSourcesByIdentifier(t *testing.T) {
	st := testutil.NewTestStore(t)

	// Create two sources with same identifier, different types
	_, err := st.GetOrCreateSource("gmail", "user@example.com")
	testutil.MustNoErr(t, err, "create gmail source")
	_, err = st.GetOrCreateSource("mbox", "user@example.com")
	testutil.MustNoErr(t, err, "create mbox source")

	sources, err := st.GetSourcesByIdentifier("user@example.com")
	testutil.MustNoErr(t, err, "GetSourcesByIdentifier")
	if len(sources) != 2 {
		t.Fatalf("got %d sources, want 2", len(sources))
	}

	// Verify ordering by source_type
	if sources[0].SourceType != "gmail" {
		t.Errorf("sources[0].SourceType = %q, want gmail", sources[0].SourceType)
	}
	if sources[1].SourceType != "mbox" {
		t.Errorf("sources[1].SourceType = %q, want mbox", sources[1].SourceType)
	}
}

func TestStore_GetSourcesByIdentifier_NotFound(t *testing.T) {
	st := testutil.NewTestStore(t)

	sources, err := st.GetSourcesByIdentifier("nobody@example.com")
	testutil.MustNoErr(t, err, "GetSourcesByIdentifier")
	if len(sources) != 0 {
		t.Errorf("got %d sources, want 0", len(sources))
	}
}

func TestStore_RemoveSource(t *testing.T) {
	f := storetest.New(t)

	// Create messages, labels, and FTS data
	msgID := f.CreateMessage("msg-remove-1")
	f.CreateMessage("msg-remove-2")

	labels := f.EnsureLabels(map[string]string{
		"INBOX": "Inbox",
	}, "system")
	err := f.Store.ReplaceMessageLabels(msgID, []int64{labels["INBOX"]})
	testutil.MustNoErr(t, err, "ReplaceMessageLabels")

	if f.Store.FTS5Available() {
		err = f.Store.UpsertFTS(msgID, "Test", "body", "a@b.com", "", "")
		testutil.MustNoErr(t, err, "UpsertFTS")
	}

	// Remove source
	err = f.Store.RemoveSource(f.Source.ID)
	testutil.MustNoErr(t, err, "RemoveSource")

	// Verify source is gone
	src, err := f.Store.GetSourceByIdentifier("test@example.com")
	testutil.MustNoErr(t, err, "GetSourceByIdentifier")
	if src != nil {
		t.Error("source should be nil after removal")
	}

	// Verify messages are gone
	count, err := f.Store.CountMessagesForSource(f.Source.ID)
	testutil.MustNoErr(t, err, "CountMessagesForSource")
	if count != 0 {
		t.Errorf("message count = %d, want 0", count)
	}

	// Verify labels are gone
	var labelCount int
	err = f.Store.DB().QueryRow(
		`SELECT COUNT(*) FROM labels WHERE source_id = ?`, f.Source.ID,
	).Scan(&labelCount)
	testutil.MustNoErr(t, err, "count labels")
	if labelCount != 0 {
		t.Errorf("label count = %d, want 0", labelCount)
	}

	// Verify FTS rows are gone
	if f.Store.FTS5Available() {
		var ftsCount int
		err = f.Store.DB().QueryRow(
			`SELECT COUNT(*) FROM messages_fts`,
		).Scan(&ftsCount)
		testutil.MustNoErr(t, err, "count FTS")
		if ftsCount != 0 {
			t.Errorf("FTS count = %d, want 0", ftsCount)
		}
	}
}

func TestStore_RemoveSource_NotFound(t *testing.T) {
	st := testutil.NewTestStore(t)

	err := st.RemoveSource(99999)
	if err == nil {
		t.Fatal("RemoveSource should error for nonexistent ID")
	}
}

func TestStore_RemoveSource_CascadesConversations(t *testing.T) {
	f := storetest.New(t)

	// Create message with body, raw, and recipients
	msgID := f.CreateMessage("msg-cascade-1")

	err := f.Store.UpsertMessageBody(msgID,
		sql.NullString{String: "body text", Valid: true},
		sql.NullString{},
	)
	testutil.MustNoErr(t, err, "UpsertMessageBody")

	err = f.Store.UpsertMessageRaw(msgID, []byte("raw MIME data"))
	testutil.MustNoErr(t, err, "UpsertMessageRaw")

	pid := f.EnsureParticipant("sender@example.com", "Sender", "example.com")
	err = f.Store.ReplaceMessageRecipients(
		msgID, "from", []int64{pid}, []string{"Sender"},
	)
	testutil.MustNoErr(t, err, "ReplaceMessageRecipients")

	// Remove source
	err = f.Store.RemoveSource(f.Source.ID)
	testutil.MustNoErr(t, err, "RemoveSource")

	// Verify conversations are gone
	var convCount int
	err = f.Store.DB().QueryRow(
		`SELECT COUNT(*) FROM conversations WHERE source_id = ?`,
		f.Source.ID,
	).Scan(&convCount)
	testutil.MustNoErr(t, err, "count conversations")
	if convCount != 0 {
		t.Errorf("conversation count = %d, want 0", convCount)
	}

	// Verify message_bodies are gone (cascaded via messages)
	var bodyCount int
	err = f.Store.DB().QueryRow(
		`SELECT COUNT(*) FROM message_bodies WHERE message_id = ?`, msgID,
	).Scan(&bodyCount)
	testutil.MustNoErr(t, err, "count message_bodies")
	if bodyCount != 0 {
		t.Errorf("message_bodies count = %d, want 0", bodyCount)
	}

	// Verify message_raw is gone (cascaded via messages)
	var rawCount int
	err = f.Store.DB().QueryRow(
		`SELECT COUNT(*) FROM message_raw WHERE message_id = ?`, msgID,
	).Scan(&rawCount)
	testutil.MustNoErr(t, err, "count message_raw")
	if rawCount != 0 {
		t.Errorf("message_raw count = %d, want 0", rawCount)
	}

	// Verify message_recipients are gone (cascaded via messages)
	var recipCount int
	err = f.Store.DB().QueryRow(
		`SELECT COUNT(*) FROM message_recipients WHERE message_id = ?`, msgID,
	).Scan(&recipCount)
	testutil.MustNoErr(t, err, "count message_recipients")
	if recipCount != 0 {
		t.Errorf("message_recipients count = %d, want 0", recipCount)
	}
}
