package query

import (
	"context"
	"database/sql"
	"testing"

	"github.com/wesm/msgvault/internal/search"
	"github.com/wesm/msgvault/internal/testutil/dbtest"
)

// testEnv encapsulates the DB, Engine, and Context setup for tests.
type testEnv struct {
	*dbtest.TestDB
	Engine *SQLiteEngine
	Ctx    context.Context
}

// setupTestDB creates an in-memory SQLite database with the production schema and
// standard test data. Use newTestEnv for tests that need builder helpers.
func setupTestDB(t *testing.T) *sql.DB {
	t.Helper()
	tdb := dbtest.NewTestDB(t, "../store/schema.sql")
	tdb.SeedStandardDataSet()
	return tdb.DB
}

// newTestEnv creates a test environment with an in-memory SQLite database and test data.
func newTestEnv(t *testing.T) *testEnv {
	t.Helper()
	tdb := dbtest.NewTestDB(t, "../store/schema.sql")
	tdb.SeedStandardDataSet()
	return &testEnv{
		TestDB: tdb,
		Engine: NewSQLiteEngine(tdb.DB),
		Ctx:    context.Background(),
	}
}

// MustListMessages calls ListMessages and fails the test on error.
func (e *testEnv) MustListMessages(filter MessageFilter) []MessageSummary {
	e.T.Helper()
	messages, err := e.Engine.ListMessages(e.Ctx, filter)
	if err != nil {
		e.T.Fatalf("ListMessages: %v", err)
	}
	return messages
}

// MustSearch calls Search and fails the test on error.
func (e *testEnv) MustSearch(q *search.Query, limit, offset int) []MessageSummary {
	e.T.Helper()
	results, err := e.Engine.Search(e.Ctx, q, limit, offset)
	if err != nil {
		e.T.Fatalf("Search: %v", err)
	}
	return results
}

// MustGetTotalStats calls GetTotalStats and fails the test on error.
func (e *testEnv) MustGetTotalStats(opts StatsOptions) *TotalStats {
	e.T.Helper()
	stats, err := e.Engine.GetTotalStats(e.Ctx, opts)
	if err != nil {
		e.T.Fatalf("GetTotalStats: %v", err)
	}
	return stats
}

// EnableFTS creates the FTS5 table, populates it, and re-creates the engine
// to clear cached FTS state.
func (e *testEnv) EnableFTS() {
	e.T.Helper()
	e.TestDB.EnableFTS()
	// Re-create engine to clear cached FTS state.
	e.Engine = NewSQLiteEngine(e.DB)
}

// aggExpectation describes an expected key/count pair in aggregate results.
type aggExpectation struct {
	Key   string
	Count int64
}

// assertRow finds a single key in the aggregate rows and asserts its count.
// It also fails if the key appears more than once (duplicate detection).
func assertRow(t *testing.T, rows []AggregateRow, key string, count int64) {
	t.Helper()
	found := 0
	for _, r := range rows {
		if r.Key == key {
			found++
			if found > 1 {
				t.Errorf("key %q appears multiple times in results", key)
			}
			if r.Count != count {
				t.Errorf("key %q: expected count %d, got %d", key, count, r.Count)
			}
		}
	}
	if found == 0 {
		t.Errorf("key %q not found in results", key)
	}
}

// assertNoDuplicateKeys fails if any key appears more than once in the rows.
func assertNoDuplicateKeys(t *testing.T, rows []AggregateRow) {
	t.Helper()
	seen := make(map[string]int)
	for _, r := range rows {
		seen[r.Key]++
	}
	for key, n := range seen {
		if n > 1 {
			t.Errorf("duplicate key %q appears %d times in results", key, n)
		}
	}
}

// assertRowsContain verifies that a subset of expected key/count pairs exist
// in the aggregate rows (order-independent). Also checks for duplicate keys.
func assertRowsContain(t *testing.T, rows []AggregateRow, want []aggExpectation) {
	t.Helper()
	assertNoDuplicateKeys(t, rows)
	for _, w := range want {
		assertRow(t, rows, w.Key, w.Count)
	}
}

// assertAggRows verifies that aggregate rows contain the expected key/count pairs
// in the exact order given. This ensures both correctness and default sort behavior.
// Also checks for duplicate keys.
func assertAggRows(t *testing.T, rows []AggregateRow, want []aggExpectation) {
	t.Helper()
	assertNoDuplicateKeys(t, rows)
	if len(rows) != len(want) {
		t.Errorf("expected %d aggregate rows, got %d", len(want), len(rows))
	}
	for i := range want {
		if i >= len(rows) {
			break
		}
		if rows[i].Key != want[i].Key {
			t.Errorf("row[%d]: expected key %q, got %q", i, want[i].Key, rows[i].Key)
		}
		if rows[i].Count != want[i].Count {
			t.Errorf("row[%d] (key %q): expected count %d, got %d", i, rows[i].Key, want[i].Count, rows[i].Count)
		}
	}
}

// assertSearchCount runs a search and verifies the number of results.
func assertSearchCount(t *testing.T, env *testEnv, q *search.Query, want int) []MessageSummary {
	t.Helper()
	results := env.MustSearch(q, 100, 0)
	if len(results) != want {
		t.Errorf("Search(%+v): got %d results, want %d", q, len(results), want)
	}
	return results
}

// assertAllResults verifies that every result satisfies the given predicate.
func assertAllResults(t *testing.T, results []MessageSummary, desc string, pred func(MessageSummary) bool) {
	t.Helper()
	for _, r := range results {
		if !pred(r) {
			t.Errorf("result id=%d did not satisfy %s", r.ID, desc)
		}
	}
}

// newTestEnvWithEmptyBuckets creates a test DB with messages that have
// empty senders, recipients, domains, and labels for testing MatchEmpty* filters.
func newTestEnvWithEmptyBuckets(t *testing.T) *testEnv {
	t.Helper()

	env := newTestEnv(t)

	// Participant with empty domain
	emptyDomainID := env.AddParticipant(dbtest.ParticipantOpts{
		Email:       dbtest.StrPtr("nodomain@"),
		DisplayName: dbtest.StrPtr("No Domain User"),
		Domain:      "",
	})

	// Message with no sender (msg6)
	env.AddMessage(dbtest.MessageOpts{
		Subject: "No Sender",
		SentAt:  "2024-04-01 10:00:00",
	})

	// Message with no recipients (msg7)
	env.AddMessage(dbtest.MessageOpts{
		Subject: "No Recipients",
		SentAt:  "2024-04-02 10:00:00",
		FromID:  1, // Alice
	})

	// Message with empty domain sender (msg8)
	env.AddMessage(dbtest.MessageOpts{
		Subject: "Empty Domain",
		SentAt:  "2024-04-03 10:00:00",
		FromID:  emptyDomainID,
		ToIDs:   []int64{1}, // Alice
	})

	// Message with no labels (msg9)
	env.AddMessage(dbtest.MessageOpts{
		Subject: "No Labels",
		SentAt:  "2024-04-04 10:00:00",
		FromID:  1,          // Alice
		ToIDs:   []int64{2}, // Bob
	})

	return env
}
