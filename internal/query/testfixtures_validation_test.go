package query

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// fatalSentinel is panicked by fakeT to halt execution, mimicking
// testing.TB methods that call runtime.Goexit (Fatal, Fatalf, FailNow,
// Skip, Skipf, SkipNow). Recovered by expectFatal.
type fatalSentinel struct{ msg string }

// fakeT wraps a real testing.TB so that un-overridden methods delegate safely,
// while intercepting all fail/skip methods via a panic sentinel.
type fakeT struct {
	testing.TB // delegate to a real TB for methods we don't override
	failed     bool
	fatalMsg   string
}

func newFakeT(t testing.TB) *fakeT {
	return &fakeT{TB: t}
}

func (f *fakeT) Helper()                          {}
func (f *fakeT) Errorf(format string, args ...any) {}
func (f *fakeT) Cleanup(fn func())                 {}
func (f *fakeT) Fatalf(format string, args ...any) {
	f.failed = true
	f.fatalMsg = fmt.Sprintf(format, args...)
	panic(fatalSentinel{f.fatalMsg})
}
func (f *fakeT) Fatal(args ...any) {
	f.failed = true
	f.fatalMsg = fmt.Sprint(args...)
	panic(fatalSentinel{f.fatalMsg})
}
func (f *fakeT) FailNow() {
	f.failed = true
	f.fatalMsg = ""
	panic(fatalSentinel{})
}
func (f *fakeT) Skip(args ...any) {
	f.failed = true
	f.fatalMsg = fmt.Sprint(args...)
	panic(fatalSentinel{f.fatalMsg})
}
func (f *fakeT) Skipf(format string, args ...any) {
	f.failed = true
	f.fatalMsg = fmt.Sprintf(format, args...)
	panic(fatalSentinel{f.fatalMsg})
}
func (f *fakeT) SkipNow() {
	f.failed = true
	f.fatalMsg = ""
	panic(fatalSentinel{})
}

// expectFatal calls fn and recovers if it triggered a fakeT fatal/skip.
func expectFatal(ft *fakeT, fn func()) {
	defer func() {
		if r := recover(); r != nil {
			if _, ok := r.(fatalSentinel); !ok {
				panic(r) // re-panic non-sentinel
			}
		}
	}()
	fn()
}

func TestAddLabel_ValidName(t *testing.T) {
	b := NewTestDataBuilder(t)
	id := b.AddLabel("INBOX")
	if id != 1 {
		t.Errorf("expected label ID 1, got %d", id)
	}
	id2 := b.AddLabel("SENT")
	if id2 != 2 {
		t.Errorf("expected label ID 2, got %d", id2)
	}
}

func TestAddMessage_ExplicitSourceID_BypassesCheck(t *testing.T) {
	// Explicit SourceID bypasses the "no sources" check.
	b := NewTestDataBuilder(t)
	id := b.AddMessage(MessageOpt{
		Subject:  "test",
		SourceID: 99, // explicit, so no sources needed
	})
	if id != 1 {
		t.Errorf("expected message ID 1, got %d", id)
	}
}

func TestAddMessage_FailsWithoutSources(t *testing.T) {
	// When no sources exist and SourceID is 0, AddMessage should fatal.
	ft := newFakeT(t)
	expectFatal(ft, func() {
		b := NewTestDataBuilder(ft)
		b.AddMessage(MessageOpt{Subject: "test"}) // SourceID defaults to 0
	})
	if !ft.failed {
		t.Error("expected Fatalf when adding message without sources")
	}
}

func TestAddAttachment_FailsWithMissingMessage(t *testing.T) {
	// AddAttachment should fatal when the message ID doesn't exist.
	ft := newFakeT(t)
	expectFatal(ft, func() {
		b := NewTestDataBuilder(ft)
		b.AddSource("a@test.com")
		b.AddMessage(MessageOpt{Subject: "exists"}) // ID = 1
		b.AddAttachment(999, 1024, "missing.txt")   // message 999 doesn't exist
	})
	if !ft.failed {
		t.Error("expected Fatalf when attaching to nonexistent message")
	}
}

func TestAddMessage_UsesFirstSource(t *testing.T) {
	b := NewTestDataBuilder(t)
	b.AddSource("a@test.com")
	id := b.AddMessage(MessageOpt{Subject: "test"})
	if id != 1 {
		t.Errorf("expected message ID 1, got %d", id)
	}
	if b.messages[0].SourceID != 1 {
		t.Errorf("expected source ID 1, got %d", b.messages[0].SourceID)
	}
}

func TestAddAttachment_SetsHasAttachments(t *testing.T) {
	b := NewTestDataBuilder(t)
	b.AddSource("a@test.com")
	msgID := b.AddMessage(MessageOpt{Subject: "with attachment"})

	if b.messages[0].HasAttachments {
		t.Error("HasAttachments should be false before AddAttachment")
	}

	b.AddAttachment(msgID, 1024, "file.txt")

	if !b.messages[0].HasAttachments {
		t.Error("HasAttachments should be true after AddAttachment")
	}
}

func TestBuild_EmptyAuxiliaryTables(t *testing.T) {
	// Build should succeed with messages but no participants, labels, etc.
	b := NewTestDataBuilder(t)
	b.AddSource("a@test.com")
	b.AddMessage(MessageOpt{
		Subject: "solo message",
		SentAt:  time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC),
	})

	engine := b.BuildEngine()
	defer engine.Close()

	// Should be able to query without errors.
	stats, err := engine.GetTotalStats(context.Background(), StatsOptions{})
	if err != nil {
		t.Fatalf("GetTotalStats: %v", err)
	}
	if stats.MessageCount != 1 {
		t.Errorf("expected 1 message, got %d", stats.MessageCount)
	}
}
