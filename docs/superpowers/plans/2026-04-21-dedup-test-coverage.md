# Dedup Test Coverage Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fill the critical test gaps in the dedup/identities/collections features to protect against silent data loss and incorrect survivor selection.

**Architecture:** Pure test additions — no production code changes. Tests are organized by package: `internal/dedup/`, `internal/store/`, `internal/query/`. Each task adds tests to existing `_test.go` files or creates new ones.

**Tech Stack:** Go, table-driven tests, `storetest.Fixture`, `testutil.MustNoErr`

**Test run command:** `go test ./internal/dedup/ ./internal/store/ ./internal/query/ -v -count=1`

---

## Task 1: normalizeRawMIME Unit Tests

The most dangerous untested function. It determines which headers are stripped before hashing. A bug causes false-positive merges (data loss) or missed duplicates.

**Files:**
- Create: `internal/dedup/normalize_test.go`

Since `normalizeRawMIME` is unexported, tests go in `package dedup` (internal test package).

- [ ] **Step 1: Write table-driven tests**

```go
// internal/dedup/normalize_test.go
package dedup

import (
	"bytes"
	"testing"
)

func TestNormalizeRawMIME(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		wantSame bool   // true if output should equal input
		contains string // substring the output must contain (empty = skip)
		excludes string // substring the output must NOT contain (empty = skip)
	}{
		{
			name:     "strips Received header (CRLF)",
			input:    []byte("Received: from mx1.google.com\r\nFrom: alice@example.com\r\nSubject: Hi\r\n\r\nBody"),
			contains: "From: alice@example.com",
			excludes: "Received",
		},
		{
			name:     "strips multiple transport headers",
			input:    []byte("Delivered-To: bob@example.com\r\nX-Gmail-Labels: INBOX\r\nAuthentication-Results: spf=pass\r\nFrom: alice@example.com\r\nSubject: Test\r\n\r\nBody"),
			contains: "From: alice@example.com",
			excludes: "Delivered-To",
		},
		{
			name:     "preserves non-transport headers",
			input:    []byte("From: alice@example.com\r\nTo: bob@example.com\r\nSubject: Meeting\r\nDate: Mon, 1 Jan 2024 12:00:00 +0000\r\n\r\nBody text"),
			contains: "Subject: Meeting",
		},
		{
			name:     "handles LF-only line endings",
			input:    []byte("Received: from mx1\nFrom: alice@example.com\nSubject: Test\n\nBody with LF"),
			contains: "From: alice@example.com",
			excludes: "Received",
		},
		{
			name:     "no header/body separator returns raw unchanged",
			input:    []byte("This is just a blob of text with no headers"),
			wantSame: true,
		},
		{
			name:     "malformed headers return raw unchanged",
			input:    []byte("Not a header at all\r\n\r\nBody"),
			wantSame: true,
		},
		{
			name:     "empty body preserved",
			input:    []byte("From: alice@example.com\r\nSubject: Empty\r\n\r\n"),
			contains: "Subject: Empty",
		},
		{
			name:     "preserves body content exactly",
			input:    []byte("Received: from mx1\r\nFrom: a@b.com\r\n\r\nExact body content here."),
			contains: "Exact body content here.",
		},
		{
			name:     "does not mutate input buffer",
			input:    []byte("Received: from mx1\r\nFrom: a@b.com\r\nSubject: Test\r\n\r\nBody"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Copy input to detect mutation
			inputCopy := make([]byte, len(tt.input))
			copy(inputCopy, tt.input)

			result := normalizeRawMIME(tt.input)

			// Verify input was not mutated
			if !bytes.Equal(tt.input, inputCopy) {
				t.Error("normalizeRawMIME mutated its input buffer")
			}

			if tt.wantSame {
				if !bytes.Equal(result, tt.input) {
					t.Errorf("expected unchanged output, got:\n%s", result)
				}
				return
			}
			if tt.contains != "" && !bytes.Contains(result, []byte(tt.contains)) {
				t.Errorf("output missing %q:\n%s", tt.contains, result)
			}
			if tt.excludes != "" && bytes.Contains(result, []byte(tt.excludes)) {
				t.Errorf("output should not contain %q:\n%s", tt.excludes, result)
			}
		})
	}
}

func TestNormalizeRawMIME_DeterministicOutput(t *testing.T) {
	// Same logical message with different transport headers must hash identically
	raw1 := []byte("Received: from mx1.google.com\r\nFrom: sender@example.com\r\nSubject: Meeting\r\nDate: Mon, 1 Jan 2024 12:00:00 +0000\r\n\r\nLet's meet at 3pm.")
	raw2 := []byte("Received: from mx2.google.com\r\nDelivered-To: other@example.com\r\nFrom: sender@example.com\r\nSubject: Meeting\r\nDate: Mon, 1 Jan 2024 12:00:00 +0000\r\n\r\nLet's meet at 3pm.")

	norm1 := normalizeRawMIME(raw1)
	norm2 := normalizeRawMIME(raw2)

	hash1 := sha256Hex(norm1)
	hash2 := sha256Hex(norm2)
	if hash1 != hash2 {
		t.Errorf("same message with different transport headers produced different hashes:\n  raw1 normalized:\n%s\n  raw2 normalized:\n%s", norm1, norm2)
	}
}
```

- [ ] **Step 2: Run tests**

Run: `go test ./internal/dedup/ -run TestNormalizeRawMIME -v`

- [ ] **Step 3: Format, vet, commit**

---

## Task 2: selectSurvivor Tiebreaker Chain Tests

Only source-type preference and sent-copy are tested. The HasRawMIME > LabelCount > ArchivedAt > ID tiebreaker chain has no coverage. If someone reorders conditions, nothing catches it.

**Files:**
- Modify: `internal/dedup/dedup_test.go`

- [ ] **Step 1: Write tiebreaker tests**

Append to `internal/dedup/dedup_test.go`:

```go
func TestSelectSurvivor_Tiebreakers(t *testing.T) {
	st := testutil.NewTestStore(t)
	eng := dedup.NewEngine(st, dedup.Config{
		AccountSourceIDs: []int64{1},
		Account:          "test",
	}, nil)

	// Helper to build a group and call Scan-less selectSurvivor
	// via a Scan that creates a synthetic group. Since selectSurvivor
	// is unexported, we test it indirectly through Scan. Instead,
	// we build the scenario in the database and let the engine pick.

	// For these tests we use a simpler approach: create two messages
	// with the same rfc822_message_id in the same source, and vary
	// the tiebreaker attribute.

	tests := []struct {
		name     string
		messages []struct {
			srcMsgID   string
			hasRaw     bool
			labelCount int
			isFromMe   bool
		}
		wantSurvivorIdx int // index into messages (0 or 1)
	}{
		{
			name: "raw MIME wins over no raw MIME",
			messages: []struct {
				srcMsgID   string
				hasRaw     bool
				labelCount int
				isFromMe   bool
			}{
				{"no-raw", false, 1, false},
				{"has-raw", true, 1, false},
			},
			wantSurvivorIdx: 1,
		},
		{
			name: "more labels wins when raw MIME is equal",
			messages: []struct {
				srcMsgID   string
				hasRaw     bool
				labelCount int
				isFromMe   bool
			}{
				{"few-labels", false, 1, false},
				{"many-labels", false, 3, false},
			},
			wantSurvivorIdx: 1,
		},
		{
			name: "lower ID wins as final tiebreaker",
			messages: []struct {
				srcMsgID   string
				hasRaw     bool
				labelCount int
				isFromMe   bool
			}{
				{"first", false, 1, false},
				{"second", false, 1, false},
			},
			wantSurvivorIdx: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := storetest.New(t)
			rfc822ID := "rfc-" + tt.name

			var msgIDs []int64
			for _, m := range tt.messages {
				id := addMessage(t, f.Store, f.Source, m.srcMsgID+"-"+tt.name, rfc822ID, m.isFromMe)
				if m.hasRaw {
					testutil.MustNoErr(t,
						f.Store.UpsertMessageRaw(id, []byte("Subject: test\r\n\r\nBody")),
						"UpsertMessageRaw",
					)
				}
				for i := 0; i < m.labelCount; i++ {
					lid, err := f.Store.EnsureLabel(
						f.Source.ID,
						fmt.Sprintf("LBL-%s-%d", m.srcMsgID, i),
						fmt.Sprintf("Label %d", i),
						"user",
					)
					testutil.MustNoErr(t, err, "EnsureLabel")
					testutil.MustNoErr(t,
						f.Store.LinkMessageLabel(id, lid),
						"LinkMessageLabel",
					)
				}
				msgIDs = append(msgIDs, id)
			}

			eng := dedup.NewEngine(f.Store, dedup.Config{
				AccountSourceIDs: []int64{f.Source.ID},
				Account:          "test",
			}, nil)

			report, err := eng.Scan(context.Background())
			testutil.MustNoErr(t, err, "Scan")
			if report.DuplicateGroups != 1 {
				t.Fatalf("groups = %d, want 1", report.DuplicateGroups)
			}

			group := report.Groups[0]
			survivor := group.Messages[group.Survivor]
			wantID := msgIDs[tt.wantSurvivorIdx]
			if survivor.ID != wantID {
				t.Errorf("survivor = %d, want %d (index %d)",
					survivor.ID, wantID, tt.wantSurvivorIdx)
			}
		})
	}
}
```

- [ ] **Step 2: Run tests**

Run: `go test ./internal/dedup/ -run TestSelectSurvivor_Tiebreakers -v`

- [ ] **Step 3: Format, vet, commit**

---

## Task 3: BackfillRFC822IDs and MergeDuplicates Raw MIME Tests

Two store-layer write paths with zero coverage on their non-trivial branches.

**Files:**
- Modify: `internal/store/dedup_test.go`

- [ ] **Step 1: Write BackfillRFC822IDs test with real MIME data**

Append to `internal/store/dedup_test.go`:

```go
func TestStore_BackfillRFC822IDs_ParsesFromRawMIME(t *testing.T) {
	f := storetest.New(t)

	// Insert a message WITHOUT an rfc822_message_id but WITH raw MIME
	// that contains a Message-ID header.
	id := newRFC822Message(t, f, "needs-backfill", "") // empty rfc822 ID

	rawMIME := []byte("From: alice@example.com\r\nTo: bob@example.com\r\nMessage-ID: <unique-123@example.com>\r\nSubject: Backfill test\r\n\r\nBody text")
	testutil.MustNoErr(t,
		f.Store.UpsertMessageRaw(id, rawMIME),
		"UpsertMessageRaw",
	)

	// Verify it needs backfill.
	count, err := f.Store.CountMessagesWithoutRFC822ID()
	testutil.MustNoErr(t, err, "CountMessagesWithoutRFC822ID")
	if count != 1 {
		t.Fatalf("count without rfc822 = %d, want 1", count)
	}

	// Run backfill.
	updated, err := f.Store.BackfillRFC822IDs(nil)
	testutil.MustNoErr(t, err, "BackfillRFC822IDs")
	if updated != 1 {
		t.Fatalf("updated = %d, want 1", updated)
	}

	// Verify the rfc822_message_id was set correctly (angle brackets stripped).
	var rfc822ID string
	err = f.Store.DB().QueryRow(
		"SELECT rfc822_message_id FROM messages WHERE id = ?", id,
	).Scan(&rfc822ID)
	testutil.MustNoErr(t, err, "scan rfc822_message_id")
	if rfc822ID != "unique-123@example.com" {
		t.Errorf("rfc822_message_id = %q, want unique-123@example.com", rfc822ID)
	}

	// Verify count is now zero.
	count, err = f.Store.CountMessagesWithoutRFC822ID()
	testutil.MustNoErr(t, err, "CountMessagesWithoutRFC822ID after backfill")
	if count != 0 {
		t.Errorf("count after backfill = %d, want 0", count)
	}
}
```

- [ ] **Step 2: Write MergeDuplicates raw MIME backfill test**

```go
func TestStore_MergeDuplicates_BackfillsRawMIME(t *testing.T) {
	f := storetest.New(t)

	// Survivor has NO raw MIME, duplicate HAS raw MIME.
	idSurvivor := newRFC822Message(t, f, "survivor", "rfc822-mime-backfill")
	idDuplicate := newRFC822Message(t, f, "duplicate", "rfc822-mime-backfill")

	rawData := []byte("From: alice@example.com\r\nSubject: Test\r\n\r\nBody")
	testutil.MustNoErr(t,
		f.Store.UpsertMessageRaw(idDuplicate, rawData),
		"UpsertMessageRaw on duplicate",
	)

	// Verify survivor has no raw MIME before merge.
	_, err := f.Store.GetMessageRaw(idSurvivor)
	if err == nil {
		t.Fatal("survivor should not have raw MIME before merge")
	}

	result, err := f.Store.MergeDuplicates(
		idSurvivor, []int64{idDuplicate}, "batch-mime",
	)
	testutil.MustNoErr(t, err, "MergeDuplicates")
	if result.RawMIMEBackfilled != 1 {
		t.Errorf("RawMIMEBackfilled = %d, want 1", result.RawMIMEBackfilled)
	}

	// Verify survivor now has raw MIME.
	got, err := f.Store.GetMessageRaw(idSurvivor)
	testutil.MustNoErr(t, err, "GetMessageRaw survivor after merge")
	if len(got) == 0 {
		t.Error("survivor raw MIME should not be empty after backfill")
	}
}
```

- [ ] **Step 3: Run tests**

Run: `go test ./internal/store/ -run "TestStore_(BackfillRFC822IDs_Parses|MergeDuplicates_Backfills)" -v`

- [ ] **Step 4: Format, vet, commit**

---

## Task 4: appendSourceFilter Unit Tests

Pure function used by every scoped query. No test file exists.

**Files:**
- Create: `internal/query/source_filter_test.go`

- [ ] **Step 1: Write table-driven tests**

```go
package query

import (
	"testing"
)

func TestAppendSourceFilter(t *testing.T) {
	id42 := int64(42)

	tests := []struct {
		name           string
		singleID       *int64
		multiIDs       []int64
		prefix         string
		wantConditions int // number of conditions added (0 or 1)
		wantArgs       int // number of args added
		wantCondition  string
	}{
		{
			name:           "neither single nor multi",
			singleID:       nil,
			multiIDs:       nil,
			prefix:         "m.",
			wantConditions: 0,
			wantArgs:       0,
		},
		{
			name:           "single ID",
			singleID:       &id42,
			multiIDs:       nil,
			prefix:         "m.",
			wantConditions: 1,
			wantArgs:       1,
			wantCondition:  "m.source_id = ?",
		},
		{
			name:           "multi IDs",
			singleID:       nil,
			multiIDs:       []int64{1, 2, 3},
			prefix:         "msg.",
			wantConditions: 1,
			wantArgs:       3,
			wantCondition:  "msg.source_id IN (?,?,?)",
		},
		{
			name:           "multi IDs take precedence over single",
			singleID:       &id42,
			multiIDs:       []int64{10, 20},
			prefix:         "",
			wantConditions: 1,
			wantArgs:       2,
			wantCondition:  "source_id IN (?,?)",
		},
		{
			name:           "empty prefix works",
			singleID:       &id42,
			multiIDs:       nil,
			prefix:         "",
			wantConditions: 1,
			wantArgs:       1,
			wantCondition:  "source_id = ?",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conditions, args := appendSourceFilter(
				nil, nil, tt.prefix, tt.singleID, tt.multiIDs,
			)
			if len(conditions) != tt.wantConditions {
				t.Errorf("conditions = %d, want %d: %v",
					len(conditions), tt.wantConditions, conditions)
			}
			if len(args) != tt.wantArgs {
				t.Errorf("args = %d, want %d", len(args), tt.wantArgs)
			}
			if tt.wantCondition != "" && len(conditions) > 0 {
				if conditions[0] != tt.wantCondition {
					t.Errorf("condition = %q, want %q",
						conditions[0], tt.wantCondition)
				}
			}
		})
	}
}
```

- [ ] **Step 2: Run tests**

Run: `go test ./internal/query/ -run TestAppendSourceFilter -v`

- [ ] **Step 3: Format, vet, commit**

---

## Task 5: Collections Edge Case Tests

Missing validation paths: bad input, idempotent operations, default collection incremental behavior.

**Files:**
- Modify: `internal/store/collections_test.go`

- [ ] **Step 1: Write validation and edge case tests**

Append to `internal/store/collections_test.go`:

```go
func TestCollections_Validation(t *testing.T) {
	f := storetest.New(t)
	st := f.Store

	t.Run("empty name rejected", func(t *testing.T) {
		_, err := st.CreateCollection("", "", []int64{f.Source.ID})
		if err == nil {
			t.Fatal("expected error for empty name")
		}
	})

	t.Run("zero sources rejected", func(t *testing.T) {
		_, err := st.CreateCollection("empty", "", nil)
		if err == nil {
			t.Fatal("expected error for zero sources")
		}
	})

	t.Run("nonexistent source rejected", func(t *testing.T) {
		_, err := st.CreateCollection("bad", "", []int64{99999})
		if err == nil {
			t.Fatal("expected error for nonexistent source")
		}
	})

	t.Run("delete nonexistent returns error", func(t *testing.T) {
		err := st.DeleteCollection("nonexistent")
		if err != store.ErrCollectionNotFound {
			t.Fatalf("expected ErrCollectionNotFound, got %v", err)
		}
	})
}

func TestCollections_Idempotent(t *testing.T) {
	f := storetest.New(t)
	st := f.Store

	_, err := st.CreateCollection("idem", "", []int64{f.Source.ID})
	testutil.MustNoErr(t, err, "CreateCollection")

	t.Run("add same source twice is no-op", func(t *testing.T) {
		err := st.AddSourcesToCollection("idem", []int64{f.Source.ID})
		testutil.MustNoErr(t, err, "AddSourcesToCollection (dupe)")
		coll, err := st.GetCollectionByName("idem")
		testutil.MustNoErr(t, err, "GetCollectionByName")
		if len(coll.SourceIDs) != 1 {
			t.Fatalf("sourceIDs = %d, want 1", len(coll.SourceIDs))
		}
	})

	t.Run("remove absent source is no-op", func(t *testing.T) {
		src2, err := st.GetOrCreateSource("mbox", "other@example.com")
		testutil.MustNoErr(t, err, "GetOrCreateSource")
		err = st.RemoveSourcesFromCollection("idem", []int64{src2.ID})
		testutil.MustNoErr(t, err, "RemoveSourcesFromCollection (absent)")
	})
}

func TestCollections_DefaultAllIncremental(t *testing.T) {
	f := storetest.New(t)
	st := f.Store

	// First call creates "All" with the existing source.
	testutil.MustNoErr(t, st.EnsureDefaultCollection(), "EnsureDefaultCollection 1")
	coll, err := st.GetCollectionByName("All")
	testutil.MustNoErr(t, err, "GetCollectionByName")
	initialCount := len(coll.SourceIDs)

	// Add a new source.
	_, err = st.GetOrCreateSource("mbox", "new@example.com")
	testutil.MustNoErr(t, err, "GetOrCreateSource")

	// Second call should add the new source to "All".
	testutil.MustNoErr(t, st.EnsureDefaultCollection(), "EnsureDefaultCollection 2")
	coll, err = st.GetCollectionByName("All")
	testutil.MustNoErr(t, err, "GetCollectionByName after add")
	if len(coll.SourceIDs) != initialCount+1 {
		t.Errorf("sourceIDs = %d, want %d", len(coll.SourceIDs), initialCount+1)
	}
}
```

- [ ] **Step 2: Run tests**

Run: `go test ./internal/store/ -run "TestCollections_(Validation|Idempotent|DefaultAllIncremental)" -v`

- [ ] **Step 3: Format, vet, commit**

---

## Task 6: Identity Signal Combination and Case Sensitivity Tests

No test verifies all three signals firing simultaneously, and no test checks case-insensitive address matching.

**Files:**
- Modify: `internal/store/identities_test.go`

- [ ] **Step 1: Write combined signal and case tests**

Append to `internal/store/identities_test.go`:

```go
func TestListLikelyIdentities_AllThreeSignals(t *testing.T) {
	f := storetest.New(t)
	// Source identifier is "test@example.com" (matches the fixture).
	// Insert a message: From: test@example.com, is_from_me=true, has SENT label.
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
	// Insert with mixed-case From address, is_from_me=true.
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
```

- [ ] **Step 2: Run tests**

Run: `go test ./internal/store/ -run "TestListLikelyIdentities_(AllThreeSignals|CaseInsensitive)" -v`

- [ ] **Step 3: Format, vet, commit**
