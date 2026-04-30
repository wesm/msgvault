package store_test

import (
	"testing"

	"github.com/wesm/msgvault/internal/testutil"
	"github.com/wesm/msgvault/internal/testutil/storetest"
)

// TestPurgeBatch_DeletesHiddenRows verifies that PurgeBatch removes only the
// rows associated with the given batch ID and that ON DELETE CASCADE removes
// child rows (message_labels).
func TestPurgeBatch_DeletesHiddenRows(t *testing.T) {
	f := storetest.New(t)
	idKeep := newRFC822Message(t, f, "keep", "rfc822-purge-a")
	idDrop := newRFC822Message(t, f, "drop", "rfc822-purge-a")

	labels := f.EnsureLabels(
		map[string]string{"INBOX": "Inbox", "SENT": "Sent"}, "system",
	)
	testutil.MustNoErr(t, f.Store.LinkMessageLabel(idDrop, labels["INBOX"]), "link INBOX")
	testutil.MustNoErr(t, f.Store.LinkMessageLabel(idDrop, labels["SENT"]), "link SENT")

	_, err := f.Store.MergeDuplicates(idKeep, []int64{idDrop}, "batch-purge")
	testutil.MustNoErr(t, err, "MergeDuplicates")

	// idDrop should be hidden before purge.
	assertDedupDeleted(t, f.Store, idDrop, true)

	deleted, err := f.Store.PurgeBatch("batch-purge")
	testutil.MustNoErr(t, err, "PurgeBatch")
	if deleted != 1 {
		t.Errorf("PurgeBatch deleted = %d, want 1", deleted)
	}

	// Row should be gone.
	var count int
	err = f.Store.DB().QueryRow(
		"SELECT COUNT(*) FROM messages WHERE id = ?", idDrop,
	).Scan(&count)
	testutil.MustNoErr(t, err, "query messages after purge")
	if count != 0 {
		t.Errorf("message %d still present after purge", idDrop)
	}

	// Child message_labels rows should cascade-delete.
	err = f.Store.DB().QueryRow(
		"SELECT COUNT(*) FROM message_labels WHERE message_id = ?", idDrop,
	).Scan(&count)
	testutil.MustNoErr(t, err, "query message_labels after purge")
	if count != 0 {
		t.Errorf("message_labels for %d still present after purge (%d rows)", idDrop, count)
	}

	// Survivor should be untouched.
	assertDedupDeleted(t, f.Store, idKeep, false)
}

// TestPurgeBatch_UnknownBatch verifies that PurgeBatch with a non-existent
// batch ID returns 0 without error.
func TestPurgeBatch_UnknownBatch(t *testing.T) {
	f := storetest.New(t)
	_ = newRFC822Message(t, f, "msg-a", "rfc822-only")

	deleted, err := f.Store.PurgeBatch("no-such-batch")
	testutil.MustNoErr(t, err, "PurgeBatch unknown batch")
	if deleted != 0 {
		t.Errorf("PurgeBatch deleted = %d, want 0", deleted)
	}
}

// TestPurgeAllHidden_MultiplesBatches verifies that PurgeAllHidden removes
// rows from all batches and reports the correct counts.
func TestPurgeAllHidden_MultipleBatches(t *testing.T) {
	f := storetest.New(t)

	// batch-alpha hides one message
	idKeepA := newRFC822Message(t, f, "keep-a", "rfc822-multi-a")
	idDropA := newRFC822Message(t, f, "drop-a", "rfc822-multi-a")
	_, err := f.Store.MergeDuplicates(idKeepA, []int64{idDropA}, "batch-alpha")
	testutil.MustNoErr(t, err, "MergeDuplicates alpha")

	// batch-beta hides one message
	idKeepB := newRFC822Message(t, f, "keep-b", "rfc822-multi-b")
	idDropB := newRFC822Message(t, f, "drop-b", "rfc822-multi-b")
	_, err = f.Store.MergeDuplicates(idKeepB, []int64{idDropB}, "batch-beta")
	testutil.MustNoErr(t, err, "MergeDuplicates beta")

	deleted, batches, err := f.Store.PurgeAllHidden()
	testutil.MustNoErr(t, err, "PurgeAllHidden")
	if deleted != 2 {
		t.Errorf("PurgeAllHidden deleted = %d, want 2", deleted)
	}
	if batches != 2 {
		t.Errorf("PurgeAllHidden distinctBatches = %d, want 2", batches)
	}

	// All four messages should still exist (survivors untouched).
	var count int
	err = f.Store.DB().QueryRow("SELECT COUNT(*) FROM messages").Scan(&count)
	testutil.MustNoErr(t, err, "count messages after PurgeAllHidden")
	if count != 2 {
		t.Errorf("messages count = %d, want 2 (survivors only)", count)
	}
}

// TestPurgeAllHidden_Empty verifies that PurgeAllHidden with no hidden rows
// returns 0/0 without error.
func TestPurgeAllHidden_Empty(t *testing.T) {
	f := storetest.New(t)
	_ = newRFC822Message(t, f, "visible", "rfc822-vis")

	deleted, batches, err := f.Store.PurgeAllHidden()
	testutil.MustNoErr(t, err, "PurgeAllHidden empty")
	if deleted != 0 {
		t.Errorf("deleted = %d, want 0", deleted)
	}
	if batches != 0 {
		t.Errorf("distinctBatches = %d, want 0", batches)
	}
}

// TestPurgeBatch_ThenUndoNoOps verifies that calling UndoDedup after PurgeBatch
// returns 0 (the rows no longer exist) without error.
func TestPurgeBatch_ThenUndoNoOps(t *testing.T) {
	f := storetest.New(t)
	idKeep := newRFC822Message(t, f, "keep", "rfc822-undo-noop")
	idDrop := newRFC822Message(t, f, "drop", "rfc822-undo-noop")

	_, err := f.Store.MergeDuplicates(idKeep, []int64{idDrop}, "batch-noop")
	testutil.MustNoErr(t, err, "MergeDuplicates")

	_, err = f.Store.PurgeBatch("batch-noop")
	testutil.MustNoErr(t, err, "PurgeBatch")

	restored, err := f.Store.UndoDedup("batch-noop")
	testutil.MustNoErr(t, err, "UndoDedup after purge")
	if restored != 0 {
		t.Errorf("UndoDedup after purge restored = %d, want 0", restored)
	}
}
