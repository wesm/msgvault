//go:build sqlite_vec

package sqlitevec

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"github.com/wesm/msgvault/internal/vector"
)

func TestFusedSearch_BothSignalsContribute(t *testing.T) {
	b, ctx, _ := newFusedBackendForTest(t)
	gid := seedAndEmbed(t, b, map[int64][]float32{
		1: unitVec(768, 0),
		2: unitVec(768, 1),
		3: unitVec(768, 2),
	})
	if err := b.ActivateGeneration(ctx, gid); err != nil {
		t.Fatalf("ActivateGeneration: %v", err)
	}

	req := vector.FusedRequest{
		FTSQuery:   "meeting",
		QueryVec:   unitVec(768, 1),
		Generation: gid,
		KPerSignal: 10,
		Limit:      5,
		RRFK:       60,
	}
	hits, _, err := b.FusedSearch(ctx, req)
	if err != nil {
		t.Fatalf("FusedSearch: %v", err)
	}
	if len(hits) == 0 || hits[0].MessageID != 2 {
		t.Fatalf("expected msg 2 at top, got %+v", hits)
	}
	if math.IsNaN(hits[0].BM25Score) || math.IsNaN(hits[0].VectorScore) {
		t.Errorf("top hit should have both scores, got %+v", hits[0])
	}
}

func TestFusedSearch_FTSOnly_VectorScoreIsNaN(t *testing.T) {
	b, ctx, _ := newFusedBackendForTest(t)
	gid := seedAndEmbed(t, b, map[int64][]float32{
		1: unitVec(768, 0),
		2: unitVec(768, 1),
		3: unitVec(768, 2),
	})
	if err := b.ActivateGeneration(ctx, gid); err != nil {
		t.Fatalf("ActivateGeneration: %v", err)
	}

	req := vector.FusedRequest{
		FTSQuery:   "meeting",
		QueryVec:   nil, // FTS-only
		Generation: gid,
		KPerSignal: 10,
		Limit:      5,
		RRFK:       60,
	}
	hits, _, err := b.FusedSearch(ctx, req)
	if err != nil {
		t.Fatalf("FusedSearch: %v", err)
	}
	if len(hits) == 0 || hits[0].MessageID != 2 {
		t.Fatalf("expected msg 2 at top, got %+v", hits)
	}
	if math.IsNaN(hits[0].BM25Score) {
		t.Errorf("BM25Score should be present, got NaN")
	}
	if !math.IsNaN(hits[0].VectorScore) {
		t.Errorf("VectorScore should be NaN for FTS-only, got %v", hits[0].VectorScore)
	}
}

func TestFusedSearch_VectorOnly_BM25ScoreIsNaN(t *testing.T) {
	b, ctx, _ := newFusedBackendForTest(t)
	gid := seedAndEmbed(t, b, map[int64][]float32{
		1: unitVec(768, 0),
		2: unitVec(768, 1),
		3: unitVec(768, 2),
	})
	if err := b.ActivateGeneration(ctx, gid); err != nil {
		t.Fatalf("ActivateGeneration: %v", err)
	}

	req := vector.FusedRequest{
		FTSQuery:   "",
		QueryVec:   unitVec(768, 1),
		Generation: gid,
		KPerSignal: 10,
		Limit:      5,
		RRFK:       60,
	}
	hits, _, err := b.FusedSearch(ctx, req)
	if err != nil {
		t.Fatalf("FusedSearch: %v", err)
	}
	if len(hits) == 0 || hits[0].MessageID != 2 {
		t.Fatalf("expected msg 2 at top, got %+v", hits)
	}
	if !math.IsNaN(hits[0].BM25Score) {
		t.Errorf("BM25Score should be NaN for vector-only, got %v", hits[0].BM25Score)
	}
	if math.IsNaN(hits[0].VectorScore) {
		t.Errorf("VectorScore should be present, got NaN")
	}
}

// TestFusedSearch_AnnSaturation_VectorOnly proves ANN-side saturation
// is reported even when BM25 is silent. Regression for the bug where
// the returned saturated flag was derived solely from the BM25 pool
// — a vector-only query that maxed out KPerSignal would falsely
// report not-saturated.
func TestFusedSearch_AnnSaturation_VectorOnly(t *testing.T) {
	b, ctx, _ := newFusedBackendForTest(t)
	// Seed 5 vectors all close to axis 0, then query along axis 0.
	// With KPerSignal=2 the ANN CTE probes for 3 (= 2+1) — when the
	// extra slot is filled, saturation must be reported.
	vecs := map[int64][]float32{}
	for i := int64(1); i <= 5; i++ {
		vecs[i] = unitVec(768, 0)
	}
	gid := seedAndEmbed(t, b, vecs)
	if err := b.ActivateGeneration(ctx, gid); err != nil {
		t.Fatalf("ActivateGeneration: %v", err)
	}

	req := vector.FusedRequest{
		QueryVec:   unitVec(768, 0),
		Generation: gid,
		KPerSignal: 2,
		Limit:      10,
		RRFK:       60,
	}
	hits, saturated, err := b.FusedSearch(ctx, req)
	if err != nil {
		t.Fatalf("FusedSearch: %v", err)
	}
	if !saturated {
		t.Errorf("saturated=false for ANN pool of 5 with KPerSignal=2; want true (hits=%d)", len(hits))
	}
}

// TestFusedSearch_AnnSaturation_BelowCap is the counter-test for
// TestFusedSearch_AnnSaturation_VectorOnly: with fewer matches than
// KPerSignal, saturation must NOT be reported.
func TestFusedSearch_AnnSaturation_BelowCap(t *testing.T) {
	b, ctx, _ := newFusedBackendForTest(t)
	gid := seedAndEmbed(t, b, map[int64][]float32{
		1: unitVec(768, 0),
		2: unitVec(768, 0),
	})
	if err := b.ActivateGeneration(ctx, gid); err != nil {
		t.Fatalf("ActivateGeneration: %v", err)
	}

	req := vector.FusedRequest{
		QueryVec:   unitVec(768, 0),
		Generation: gid,
		KPerSignal: 5,
		Limit:      10,
		RRFK:       60,
	}
	_, saturated, err := b.FusedSearch(ctx, req)
	if err != nil {
		t.Fatalf("FusedSearch: %v", err)
	}
	if saturated {
		t.Errorf("saturated=true for ANN pool of 2 with KPerSignal=5; want false")
	}
}

func TestFusedSearch_NoSignals_Errors(t *testing.T) {
	b, ctx, _ := newFusedBackendForTest(t)
	gid := seedAndEmbed(t, b, map[int64][]float32{
		1: unitVec(768, 0),
	})
	_, _, err := b.FusedSearch(ctx, vector.FusedRequest{
		Generation: gid,
		KPerSignal: 10, Limit: 5, RRFK: 60,
	})
	if err == nil {
		t.Error("FusedSearch with no signals should error")
	}
}

func TestFusedSearch_UnknownGeneration(t *testing.T) {
	b, ctx, _ := newFusedBackendForTest(t)
	_, _, err := b.FusedSearch(ctx, vector.FusedRequest{
		FTSQuery:   "meeting",
		QueryVec:   unitVec(768, 0),
		Generation: vector.GenerationID(9999),
		KPerSignal: 10, Limit: 5, RRFK: 60,
	})
	if !errors.Is(err, vector.ErrUnknownGeneration) {
		t.Errorf("err = %v, want ErrUnknownGeneration", err)
	}
}

// TestFusedSearch_BM25TopKRespectsRank seeds messages with varying
// BM25 rank for "meeting" and forces KPerSignal below the match
// count. The BM25 CTE must pick the top-ranked rows, not an
// arbitrary prefix. Without ORDER BY fts.rank before LIMIT, SQLite
// may return any K rows from the match set and silently lose the
// most-relevant messages from the fused signal.
func TestFusedSearch_BM25TopKRespectsRank(t *testing.T) {
	ctx := context.Background()
	// Fresh main DB where we control the FTS content density so BM25
	// produces a known ranking. Message 5 gets the highest rank by
	// having "meeting" dense in a short doc; 4 is next; 1 is barely
	// a match. The rest are easy filler.
	dir := t.TempDir()
	mainPath := filepath.Join(dir, "main.db")
	main := openFusedMainWithSchema(t, mainPath)

	docs := map[int64]string{
		1: "meeting",                                          // 1 occurrence, short doc
		2: "the agenda is ready for the meeting next week",    // 1 occurrence, longer
		3: "planning the meeting agenda for the next meeting", // 2 occurrences
		4: "meeting meeting meeting agenda",                   // 3 occurrences, short
		5: "meeting meeting meeting meeting",                  // 4 occurrences, shortest — top BM25
		6: "lunch and dinner plans",                           // no match
	}
	for id, body := range docs {
		if _, err := main.ExecContext(ctx, `INSERT INTO messages (id) VALUES (?)`, id); err != nil {
			t.Fatalf("insert msg %d: %v", id, err)
		}
		if _, err := main.ExecContext(ctx,
			`INSERT INTO messages_fts (rowid, subject, body) VALUES (?, ?, ?)`,
			id, "", body); err != nil {
			t.Fatalf("insert fts %d: %v", id, err)
		}
	}

	vecPath := filepath.Join(dir, "vectors.db")
	b, err := Open(ctx, Options{
		Path:      vecPath,
		MainPath:  mainPath,
		Dimension: 768,
		MainDB:    main,
	})
	if err != nil {
		t.Fatalf("Open backend: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })

	gid, err := b.CreateGeneration(ctx, "m", 768)
	if err != nil {
		t.Fatalf("CreateGeneration: %v", err)
	}
	if err := b.ActivateGeneration(ctx, gid); err != nil {
		t.Fatalf("ActivateGeneration: %v", err)
	}

	// Confirm the ground-truth BM25 ordering in the attached DB so
	// the assertion below reflects what FTS5 would produce without
	// any LIMIT. Rank is negated BM25 — more-negative is better.
	type ranked struct {
		id   int64
		rank float64
	}
	rows, err := main.QueryContext(ctx,
		`SELECT rowid, rank FROM messages_fts WHERE messages_fts MATCH 'meeting' ORDER BY rank`)
	if err != nil {
		t.Fatalf("ground-truth rank query: %v", err)
	}
	var expected []ranked
	for rows.Next() {
		var r ranked
		if err := rows.Scan(&r.id, &r.rank); err != nil {
			t.Fatalf("scan: %v", err)
		}
		expected = append(expected, r)
	}
	_ = rows.Close()
	if len(expected) < 4 {
		t.Fatalf("ground-truth matches = %d, want >= 4", len(expected))
	}

	// Request only the top 3 BM25 matches via the fused CTE. The
	// BM25 branch without ORDER BY would be free to pick any 3
	// matching rows, dropping higher-ranked ones.
	req := vector.FusedRequest{
		FTSQuery:   "meeting",
		Generation: gid,
		KPerSignal: 3,
		Limit:      10,
		RRFK:       60,
	}
	hits, _, err := b.FusedSearch(ctx, req)
	if err != nil {
		t.Fatalf("FusedSearch: %v", err)
	}
	if len(hits) != 3 {
		t.Fatalf("hits=%d, want 3", len(hits))
	}
	got := map[int64]bool{}
	for _, h := range hits {
		got[h.MessageID] = true
	}
	wantTop := expected[:3]
	for _, w := range wantTop {
		if !got[w.id] {
			ids := []string{}
			for _, h := range hits {
				ids = append(ids, formatInt(h.MessageID))
			}
			t.Errorf("missing top-rank id %d from fused hits=[%s]; ground-truth top3 by BM25: %v",
				w.id, strings.Join(ids, ","), wantTop)
		}
	}
}

// openFusedMainWithSchema creates a main DB at path with the FTS
// schema used by FusedSearch. Cleanup is registered via t.Cleanup.
func openFusedMainWithSchema(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("open main: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	// sent_at is DATETIME (text) to match the production schema.
	schema := `
CREATE TABLE messages (
    id INTEGER PRIMARY KEY,
    subject TEXT,
    source_id INTEGER,
    sender_id INTEGER,
    has_attachments INTEGER DEFAULT 0,
    size_estimate INTEGER,
    sent_at DATETIME,
    deleted_from_source_at DATETIME
);
CREATE VIRTUAL TABLE messages_fts USING fts5(subject, body, content='', contentless_delete=1);
CREATE TABLE message_labels (
    message_id INTEGER NOT NULL,
    label_id INTEGER NOT NULL,
    PRIMARY KEY (message_id, label_id)
);
CREATE TABLE message_recipients (
    id INTEGER PRIMARY KEY,
    message_id INTEGER NOT NULL,
    recipient_type TEXT NOT NULL,
    participant_id INTEGER NOT NULL
);`
	if _, err := db.Exec(schema); err != nil {
		t.Fatalf("schema: %v", err)
	}
	return db
}

func formatInt(n int64) string { return fmt.Sprintf("%d", n) }

// TestFusedSearch_PinnedPoolKeepsAttach regression-guards the pool
// pinning in openFusedConn. SQLite's ATTACH DATABASE is per-connection,
// so if the pool is allowed to open a second connection, any query
// that references vec.* tables on the unattached connection fails
// with "no such table". Two checks here:
//
//  1. The pool reports MaxOpenConnections == 1, which is the
//     compile-time-adjacent guarantee that the pin is still in
//     place.
//  2. A second query sent via the same *sql.DB still sees the
//     attached vec table — catching any future refactor that
//     unsets the pin in a way the stats check might miss.
func TestFusedSearch_PinnedPoolKeepsAttach(t *testing.T) {
	b, ctx, _ := newFusedBackendForTest(t)
	gid := seedAndEmbed(t, b, map[int64][]float32{1: unitVec(768, 0)})
	if err := b.ActivateGeneration(ctx, gid); err != nil {
		t.Fatalf("ActivateGeneration: %v", err)
	}

	conn, err := b.openFusedConn(ctx)
	if err != nil {
		t.Fatalf("openFusedConn: %v", err)
	}
	defer func() { _ = conn.Close() }()

	if got := conn.Stats().MaxOpenConnections; got != 1 {
		t.Errorf("MaxOpenConnections=%d, want 1 (ATTACH is per-connection; pool must be pinned)", got)
	}

	// Hit vec.* repeatedly. If the pool ever hands out a fresh
	// connection mid-test, the ATTACH is gone and the query errors.
	for i := 0; i < 3; i++ {
		var n int
		if err := conn.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM vec.embeddings WHERE generation_id = ?`,
			int64(gid)).Scan(&n); err != nil {
			t.Fatalf("query #%d: %v (attach likely dropped)", i+1, err)
		}
		if n != 1 {
			t.Errorf("query #%d: count=%d, want 1", i+1, n)
		}
	}

	// Force a simulated "busy first connection" scenario: open a
	// transaction on the pool and then issue a second query. Under
	// the pin, the second query waits for the tx's connection. Under
	// an unpinned pool, the second query would get a fresh
	// connection without ATTACH and fail.
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	// Send the second query with a short deadline so if the pool
	// were unpinned AND vec.* actually missing, we wouldn't hang.
	// With the pin it will timeout waiting for the tx's conn — which
	// is exactly the intended serialisation, not a failure.
	queryCtx, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
	defer cancel()
	rows, secondErr := conn.QueryContext(queryCtx,
		`SELECT COUNT(*) FROM vec.embeddings`)
	if rows != nil {
		_ = rows.Close()
	}
	// Finish the tx so the connection is released.
	_ = tx.Rollback()

	// Under the pin, we expect a context deadline error (the query
	// was queued waiting for the single connection). We do NOT
	// expect a "no such table: vec.embeddings" error.
	if secondErr != nil && strings.Contains(secondErr.Error(), "no such table") {
		t.Errorf("second query saw 'no such table' — pool is not pinned: %v", secondErr)
	}
}

// TestFusedSearch_AfterBeforeBoundaries_TextDate covers the regression
// where After/Before bounds were bound as integers but compared against
// the production text DATETIME column. Boundary semantics: After is
// inclusive (>=), Before is exclusive (<). We seed three messages with
// distinct text sent_at values and assert the bounds carve out exactly
// the expected subset.
func TestFusedSearch_AfterBeforeBoundaries_TextDate(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	mainPath := filepath.Join(dir, "main.db")
	main := openFusedMainWithSchema(t, mainPath)

	// Three messages spaced one day apart, indexed in messages_fts so
	// the BM25 branch has matches and the date filter is the
	// discriminator.
	type row struct {
		id     int64
		sentAt string
	}
	rows := []row{
		{1, "2026-01-01 00:00:00"},
		{2, "2026-01-15 12:00:00"},
		{3, "2026-02-01 00:00:00"},
	}
	for _, r := range rows {
		if _, err := main.ExecContext(ctx,
			`INSERT INTO messages (id, sent_at) VALUES (?, ?)`,
			r.id, r.sentAt); err != nil {
			t.Fatalf("insert msg %d: %v", r.id, err)
		}
		if _, err := main.ExecContext(ctx,
			`INSERT INTO messages_fts (rowid, subject, body) VALUES (?, ?, ?)`,
			r.id, "", "topic"); err != nil {
			t.Fatalf("insert fts %d: %v", r.id, err)
		}
	}

	b, err := Open(ctx, Options{
		Path:      filepath.Join(dir, "vectors.db"),
		MainPath:  mainPath,
		Dimension: 768,
		MainDB:    main,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })

	gid, err := b.CreateGeneration(ctx, "m", 768)
	if err != nil {
		t.Fatalf("CreateGeneration: %v", err)
	}
	if err := b.ActivateGeneration(ctx, gid); err != nil {
		t.Fatalf("Activate: %v", err)
	}

	mid := time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC)
	end := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)

	cases := []struct {
		name   string
		filter vector.Filter
		want   []int64
	}{
		{
			name:   "after_inclusive_picks_boundary_and_later",
			filter: vector.Filter{After: &mid},
			want:   []int64{2, 3},
		},
		{
			name:   "before_exclusive_drops_boundary",
			filter: vector.Filter{Before: &end},
			want:   []int64{1, 2},
		},
		{
			name:   "after_and_before_carve_out_window",
			filter: vector.Filter{After: &mid, Before: &end},
			want:   []int64{2},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			req := vector.FusedRequest{
				FTSQuery:   "topic",
				Generation: gid,
				Filter:     c.filter,
				KPerSignal: 10,
				Limit:      10,
				RRFK:       60,
			}
			hits, _, err := b.FusedSearch(ctx, req)
			if err != nil {
				t.Fatalf("FusedSearch: %v", err)
			}
			got := make(map[int64]bool, len(hits))
			for _, h := range hits {
				got[h.MessageID] = true
			}
			for _, id := range c.want {
				if !got[id] {
					t.Errorf("missing expected id %d (got %v)", id, got)
				}
			}
			if len(got) != len(c.want) {
				t.Errorf("got %d hits, want %d (got=%v want=%v)", len(got), len(c.want), got, c.want)
			}
		})
	}
}

// TestFusedSearch_SenderFallback_ToMessageRecipients confirms the
// sender filter matches messages whose only record of the sender is a
// message_recipients row with recipient_type='from' — i.e. legacy rows
// where messages.sender_id is NULL. This mirrors the same fallback in
// Backend.Search; without coverage on the fused path, a future
// refactor of the CTE could quietly drop legacy rows from hybrid
// search.
func TestFusedSearch_SenderFallback_ToMessageRecipients(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	mainPath := filepath.Join(dir, "main.db")
	main := openFusedMainWithSchema(t, mainPath)

	// msg 1: direct sender_id=100. msg 2: sender_id NULL but a 'from'
	// recipient row points at participant 100. msg 3: different sender.
	if _, err := main.ExecContext(ctx,
		`INSERT INTO messages (id, sender_id) VALUES (1, 100)`); err != nil {
		t.Fatalf("insert msg 1: %v", err)
	}
	if _, err := main.ExecContext(ctx,
		`INSERT INTO messages (id) VALUES (2)`); err != nil {
		t.Fatalf("insert msg 2: %v", err)
	}
	if _, err := main.ExecContext(ctx,
		`INSERT INTO message_recipients (message_id, recipient_type, participant_id)
		 VALUES (2, 'from', 100)`); err != nil {
		t.Fatalf("insert mr: %v", err)
	}
	if _, err := main.ExecContext(ctx,
		`INSERT INTO messages (id, sender_id) VALUES (3, 999)`); err != nil {
		t.Fatalf("insert msg 3: %v", err)
	}
	for _, id := range []int64{1, 2, 3} {
		if _, err := main.ExecContext(ctx,
			`INSERT INTO messages_fts (rowid, subject, body) VALUES (?, ?, ?)`,
			id, "", "topic"); err != nil {
			t.Fatalf("insert fts %d: %v", id, err)
		}
	}

	b, err := Open(ctx, Options{
		Path:      filepath.Join(dir, "vectors.db"),
		MainPath:  mainPath,
		Dimension: 768,
		MainDB:    main,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })

	gid, err := b.CreateGeneration(ctx, "m", 768)
	if err != nil {
		t.Fatalf("CreateGeneration: %v", err)
	}
	if err := b.ActivateGeneration(ctx, gid); err != nil {
		t.Fatalf("Activate: %v", err)
	}

	req := vector.FusedRequest{
		FTSQuery:   "topic",
		Generation: gid,
		Filter:     vector.Filter{SenderIDs: []int64{100}},
		KPerSignal: 10,
		Limit:      10,
		RRFK:       60,
	}
	hits, _, err := b.FusedSearch(ctx, req)
	if err != nil {
		t.Fatalf("FusedSearch: %v", err)
	}
	got := make(map[int64]bool, len(hits))
	for _, h := range hits {
		got[h.MessageID] = true
	}
	if !got[1] {
		t.Errorf("missing msg 1 (direct sender_id=100)")
	}
	if !got[2] {
		t.Errorf("missing msg 2 (sender_id NULL, recipient_type='from' fallback)")
	}
	if got[3] {
		t.Errorf("unexpected msg 3 (different sender, should be filtered out)")
	}
}

// TestFusedSearch_RecipientFiltersMatchNoneSentinel guards the
// "operator present, resolved to zero IDs" semantics for to/cc/bcc.
// BuildFilter substitutes a negative sentinel id when a recipient
// token resolves to nothing; the fused query's IN clause must produce
// zero hits, NOT degrade to an unfiltered search. Without this guard,
// a typo'd to:nonexistent would broaden results instead of returning
// none.
func TestFusedSearch_RecipientFiltersMatchNoneSentinel(t *testing.T) {
	b, ctx, _ := newFusedBackendForTest(t)
	gid := seedAndEmbed(t, b, map[int64][]float32{
		1: unitVec(768, 0),
		2: unitVec(768, 1),
		3: unitVec(768, 2),
	})
	if err := b.ActivateGeneration(ctx, gid); err != nil {
		t.Fatalf("Activate: %v", err)
	}

	const sentinel int64 = -1 // mirrors hybrid.noMatchSentinel
	cases := []struct {
		name   string
		filter vector.Filter
	}{
		{"ToIDs_sentinel", vector.Filter{ToIDs: []int64{sentinel}}},
		{"CcIDs_sentinel", vector.Filter{CcIDs: []int64{sentinel}}},
		{"BccIDs_sentinel", vector.Filter{BccIDs: []int64{sentinel}}},
		{"SenderIDs_sentinel", vector.Filter{SenderIDs: []int64{sentinel}}},
		{"LabelIDs_sentinel", vector.Filter{LabelIDs: []int64{sentinel}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			req := vector.FusedRequest{
				FTSQuery:   "meeting",
				QueryVec:   unitVec(768, 1),
				Generation: gid,
				Filter:     c.filter,
				KPerSignal: 10,
				Limit:      10,
				RRFK:       60,
			}
			hits, _, err := b.FusedSearch(ctx, req)
			if err != nil {
				t.Fatalf("FusedSearch: %v", err)
			}
			if len(hits) != 0 {
				t.Errorf("got %d hits with sentinel filter, want 0 (operator present + no match must return nothing)", len(hits))
			}
		})
	}
}

func TestFusedSearch_DimensionMismatch(t *testing.T) {
	b, ctx, _ := newFusedBackendForTest(t)
	gid := seedAndEmbed(t, b, map[int64][]float32{
		1: unitVec(768, 0),
	})
	_, _, err := b.FusedSearch(ctx, vector.FusedRequest{
		QueryVec:   unitVec(64, 0), // wrong dim
		Generation: gid,
		KPerSignal: 10, Limit: 5, RRFK: 60,
	})
	if !errors.Is(err, vector.ErrDimensionMismatch) {
		t.Errorf("err = %v, want ErrDimensionMismatch", err)
	}
}
