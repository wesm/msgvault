package dbtest

import (
	"fmt"
	"testing"
)

const schemaPath = "../../store/schema.sql"

// fakeT implements testing.TB and captures Fatalf calls instead of aborting.
type fakeT struct {
	testing.TB
	fatalMsg string
}

func (f *fakeT) Fatalf(format string, args ...interface{}) {
	f.fatalMsg = fmt.Sprintf(format, args...)
	panic("fatalf") // stop execution in the caller
}

func (f *fakeT) Helper() {}

func TestAddMessage_SourceIDMatchesConversation(t *testing.T) {
	tdb := NewTestDB(t, schemaPath)
	tdb.SeedStandardDataSet()

	// Happy path: SourceID 1 matches conversation 1's source_id.
	id := tdb.AddMessage(MessageOpts{
		SourceID:       1,
		ConversationID: 1,
		Subject:        "match",
		SentAt:         "2024-06-01 10:00:00",
	})
	if id == 0 {
		t.Fatal("expected non-zero message ID")
	}
}

func TestAddMessage_MismatchedSourceID(t *testing.T) {
	tdb := NewTestDB(t, schemaPath)
	tdb.SeedStandardDataSet()

	src2 := tdb.AddSource(SourceOpts{Identifier: "other@gmail.com"})

	ft := &fakeT{TB: t}
	fakeTDB := &TestDB{
		DB:            tdb.DB,
		T:             ft,
		nextMessageID: tdb.nextMessageID,
	}

	var caught bool
	func() {
		defer func() {
			if r := recover(); r != nil {
				caught = true
			}
		}()
		fakeTDB.AddMessage(MessageOpts{
			SourceID:       src2,
			ConversationID: 1,
			Subject:        "mismatch",
			SentAt:         "2024-06-01 11:00:00",
		})
	}()

	if !caught || ft.fatalMsg == "" {
		t.Fatal("expected fatal for mismatched SourceID")
	}
	t.Logf("got expected fatal: %s", ft.fatalMsg)
}

func TestAddMessage_DBErrorFailsTest(t *testing.T) {
	tdb := NewTestDB(t, schemaPath)
	tdb.SeedStandardDataSet()

	ft := &fakeT{TB: t}
	fakeTDB := &TestDB{
		DB:            tdb.DB,
		T:             ft,
		nextMessageID: tdb.nextMessageID,
	}

	// Close the DB to force a non-ErrNoRows error on the source_id lookup.
	tdb.DB.Close()

	func() {
		defer func() { _ = recover() }()
		fakeTDB.AddMessage(MessageOpts{
			ConversationID: 1,
			Subject:        "db error",
			SentAt:         "2024-06-01 12:00:00",
		})
	}()

	if ft.fatalMsg == "" {
		t.Fatal("expected fatal for DB error on source_id lookup")
	}
	t.Logf("got expected fatal: %s", ft.fatalMsg)
}

func TestAddMessage_MissingConversation(t *testing.T) {
	tdb := NewTestDB(t, schemaPath)
	tdb.SeedStandardDataSet()

	ft := &fakeT{TB: t}
	fakeTDB := &TestDB{
		DB:            tdb.DB,
		T:             ft,
		nextMessageID: tdb.nextMessageID,
	}

	var caught bool
	func() {
		defer func() {
			if r := recover(); r != nil {
				caught = true
			}
		}()
		fakeTDB.AddMessage(MessageOpts{
			SourceID:       1,
			ConversationID: 999,
			Subject:        "missing conv",
			SentAt:         "2024-06-01 12:00:00",
		})
	}()

	if !caught || ft.fatalMsg == "" {
		t.Fatal("expected fatal for missing conversation")
	}
	t.Logf("got expected fatal: %s", ft.fatalMsg)
}
