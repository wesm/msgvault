package store_test

import (
	"database/sql"
	"testing"

	"github.com/wesm/msgvault/internal/store"
	"github.com/wesm/msgvault/internal/testutil"
	"github.com/wesm/msgvault/internal/testutil/storetest"
)

func newRFC822Message(
	t *testing.T, f *storetest.Fixture, sourceMessageID, rfc822ID string,
) int64 {
	t.Helper()
	id, err := f.Store.UpsertMessage(&store.Message{
		ConversationID:  f.ConvID,
		SourceID:        f.Source.ID,
		SourceMessageID: sourceMessageID,
		RFC822MessageID: sql.NullString{
			String: rfc822ID, Valid: rfc822ID != "",
		},
		MessageType:  "email",
		SizeEstimate: 1000,
	})
	testutil.MustNoErr(t, err, "UpsertMessage")
	return id
}

func TestStore_FindDuplicatesByRFC822ID(t *testing.T) {
	f := storetest.New(t)
	idA := newRFC822Message(t, f, "src-a", "rfc822-shared")
	idB := newRFC822Message(t, f, "src-b", "rfc822-shared")
	_ = newRFC822Message(t, f, "src-c", "rfc822-unique")

	groups, err := f.Store.FindDuplicatesByRFC822ID()
	testutil.MustNoErr(t, err, "FindDuplicatesByRFC822ID")
	if len(groups) != 1 {
		t.Fatalf("groups = %d, want 1", len(groups))
	}
	if groups[0].RFC822MessageID != "rfc822-shared" {
		t.Errorf("key = %q, want rfc822-shared", groups[0].RFC822MessageID)
	}
	if groups[0].Count != 2 {
		t.Errorf("count = %d, want 2", groups[0].Count)
	}

	_, err = f.Store.MergeDuplicates(idA, []int64{idB}, "batch-test")
	testutil.MustNoErr(t, err, "MergeDuplicates")

	groups, err = f.Store.FindDuplicatesByRFC822ID()
	testutil.MustNoErr(t, err, "FindDuplicatesByRFC822ID after merge")
	if len(groups) != 0 {
		t.Errorf("groups after merge = %d, want 0", len(groups))
	}
}

func TestStore_GetDuplicateGroupMessages_SentLabel(t *testing.T) {
	f := storetest.New(t)
	idInbox := newRFC822Message(t, f, "inbox-copy", "rfc822-sent")
	idSent := newRFC822Message(t, f, "sent-copy", "rfc822-sent")

	labels := f.EnsureLabels(
		map[string]string{"SENT": "Sent", "INBOX": "Inbox"}, "system",
	)
	testutil.MustNoErr(t, f.Store.LinkMessageLabel(idInbox, labels["INBOX"]), "link INBOX")
	testutil.MustNoErr(t, f.Store.LinkMessageLabel(idSent, labels["SENT"]), "link SENT")

	rows, err := f.Store.GetDuplicateGroupMessages("rfc822-sent")
	testutil.MustNoErr(t, err, "GetDuplicateGroupMessages")
	if len(rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(rows))
	}

	var sentRow, inboxRow *store.DuplicateMessageRow
	for i := range rows {
		switch rows[i].ID {
		case idSent:
			sentRow = &rows[i]
		case idInbox:
			inboxRow = &rows[i]
		}
	}
	if sentRow == nil || inboxRow == nil {
		t.Fatalf("missing rows: sent=%v inbox=%v", sentRow, inboxRow)
	}
	if !sentRow.HasSentLabel {
		t.Errorf("sent row: HasSentLabel = false, want true")
	}
	if inboxRow.HasSentLabel {
		t.Errorf("inbox row: HasSentLabel = true, want false")
	}
}

func TestStore_MergeDuplicates_UnionsLabels(t *testing.T) {
	f := storetest.New(t)
	idKeep := newRFC822Message(t, f, "keep", "rfc822-merge")
	idDrop := newRFC822Message(t, f, "drop", "rfc822-merge")

	labels := f.EnsureLabels(
		map[string]string{"INBOX": "Inbox", "IMPORTANT": "Important", "WORK": "Work"}, "user",
	)
	testutil.MustNoErr(t, f.Store.LinkMessageLabel(idKeep, labels["INBOX"]), "link INBOX to keep")
	testutil.MustNoErr(t, f.Store.LinkMessageLabel(idDrop, labels["IMPORTANT"]), "link IMPORTANT to drop")
	testutil.MustNoErr(t, f.Store.LinkMessageLabel(idDrop, labels["WORK"]), "link WORK to drop")

	result, err := f.Store.MergeDuplicates(idKeep, []int64{idDrop}, "batch-labels")
	testutil.MustNoErr(t, err, "MergeDuplicates")
	if result.LabelsTransferred != 2 {
		t.Errorf("labelsTransferred = %d, want 2", result.LabelsTransferred)
	}

	f.AssertLabelCount(idKeep, 3)
	assertDedupDeleted(t, f.Store, idDrop, true)

	restored, err := f.Store.UndoDedup("batch-labels")
	testutil.MustNoErr(t, err, "UndoDedup")
	if restored != 1 {
		t.Errorf("restored = %d, want 1", restored)
	}
	assertDedupDeleted(t, f.Store, idDrop, false)
}

func assertDedupDeleted(
	t *testing.T, st *store.Store, msgID int64, wantDeleted bool,
) {
	t.Helper()
	var deletedAt sql.NullTime
	err := st.DB().QueryRow(
		"SELECT deleted_at FROM messages WHERE id = ?", msgID,
	).Scan(&deletedAt)
	testutil.MustNoErr(t, err, "query deleted_at")
	if wantDeleted && !deletedAt.Valid {
		t.Errorf("message %d: deleted_at should be set", msgID)
	}
	if !wantDeleted && deletedAt.Valid {
		t.Errorf("message %d: deleted_at should be NULL", msgID)
	}
}

func TestStore_BackfillRFC822IDs_EmptyTable(t *testing.T) {
	f := storetest.New(t)
	count, err := f.Store.CountMessagesWithoutRFC822ID()
	testutil.MustNoErr(t, err, "CountMessagesWithoutRFC822ID")
	if count != 0 {
		t.Errorf("empty-table count = %d, want 0", count)
	}

	updated, err := f.Store.BackfillRFC822IDs(nil)
	testutil.MustNoErr(t, err, "BackfillRFC822IDs")
	if updated != 0 {
		t.Errorf("updated = %d, want 0", updated)
	}
}

func TestStore_CountActiveMessages(t *testing.T) {
	f := storetest.New(t)
	_ = newRFC822Message(t, f, "a", "id-a")
	idB := newRFC822Message(t, f, "b", "id-b")

	total, err := f.Store.CountActiveMessages()
	testutil.MustNoErr(t, err, "CountActiveMessages")
	if total != 2 {
		t.Errorf("active = %d, want 2", total)
	}

	_, err = f.Store.MergeDuplicates(
		newRFC822Message(t, f, "c", "id-c"),
		[]int64{idB},
		"batch-count",
	)
	testutil.MustNoErr(t, err, "MergeDuplicates")

	total, err = f.Store.CountActiveMessages()
	testutil.MustNoErr(t, err, "CountActiveMessages after merge")
	if total != 2 {
		t.Errorf("active after merge = %d, want 2", total)
	}
}
