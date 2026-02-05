package query

import (
	"strings"
	"testing"
)

// TestSQLInjection_InvalidViewType tests that invalid ViewType values are rejected.
// This ensures that even if a malicious or buggy caller passes an out-of-range
// ViewType, it won't result in undefined behavior or SQL injection.
func TestSQLInjection_InvalidViewType(t *testing.T) {
	env := newTestEnv(t)

	// Attempt to aggregate with an invalid ViewType value
	// This simulates a potential attack vector if enum validation is missing
	invalidViewType := ViewType(999)

	_, err := env.Engine.Aggregate(env.Ctx, invalidViewType, DefaultAggregateOptions())
	if err == nil {
		t.Error("expected error for invalid ViewType, got nil")
	}
	if !strings.Contains(err.Error(), "unsupported view type") {
		t.Errorf("expected error message to contain 'unsupported view type', got: %v", err)
	}
}

// TestSQLInjection_InvalidSortField tests that invalid SortField values are handled safely.
// The sortClause function should validate the SortField is within expected range.
func TestSQLInjection_InvalidSortField(t *testing.T) {
	env := newTestEnv(t)

	// Create options with an invalid SortField value
	opts := DefaultAggregateOptions()
	opts.SortField = SortField(999) // Invalid sort field

	// The current implementation falls through to default, which is unsafe
	// because it allows arbitrary sort field values without explicit validation.
	// After the fix, this should return an error.
	_, err := env.Engine.Aggregate(env.Ctx, ViewSenders, opts)
	if err == nil {
		t.Fatal("expected error for invalid SortField, got nil")
	}
	if !strings.Contains(err.Error(), "unsupported sort field") {
		t.Errorf("expected error message to contain 'unsupported sort field', got: %v", err)
	}
}

// TestSQLInjection_InvalidMessageSortField tests that invalid MessageSortField values
// are handled safely in ListMessages.
func TestSQLInjection_InvalidMessageSortField(t *testing.T) {
	env := newTestEnv(t)

	// Create filter with an invalid MessageSortField value
	filter := MessageFilter{
		Sorting: MessageSorting{
			Field:     MessageSortField(999), // Invalid sort field
			Direction: SortAsc,
		},
	}

	// The current implementation falls through to default, which is unsafe.
	// After the fix, this should return an error.
	_, err := env.Engine.ListMessages(env.Ctx, filter)
	if err == nil {
		t.Fatal("expected error for invalid MessageSortField, got nil")
	}
	if !strings.Contains(err.Error(), "unsupported message sort field") {
		t.Errorf("expected error message to contain 'unsupported message sort field', got: %v", err)
	}
}

// TestSQLInjection_InvalidTimeGranularity tests that invalid TimeGranularity values
// are handled safely when used in queries.
func TestSQLInjection_InvalidTimeGranularity(t *testing.T) {
	env := newTestEnv(t)

	opts := DefaultAggregateOptions()
	opts.TimeGranularity = TimeGranularity(999) // Invalid granularity

	// When aggregating by time with an invalid granularity, should return error
	_, err := env.Engine.Aggregate(env.Ctx, ViewTime, opts)
	if err == nil {
		t.Fatal("expected error for invalid TimeGranularity, got nil")
	}
	if !strings.Contains(err.Error(), "unsupported time granularity") {
		t.Errorf("expected error message to contain 'unsupported time granularity', got: %v", err)
	}
}

// TestSQLInjection_FilterStringsAreSafelyParameterized verifies that filter
// string fields like Sender, Label, etc. are properly parameterized and
// cannot be used for SQL injection.
func TestSQLInjection_FilterStringsAreSafelyParameterized(t *testing.T) {
	env := newTestEnv(t)

	// These SQL injection payloads should be treated as literal strings,
	// not executed as SQL.
	injectionPayloads := []string{
		"'; DROP TABLE messages; --",
		"alice@example.com' OR '1'='1",
		"alice@example.com\" OR \"1\"=\"1",
		"alice@example.com; DELETE FROM messages WHERE '1'='1",
		"alice@example.com UNION SELECT * FROM messages--",
	}

	for _, payload := range injectionPayloads {
		t.Run("Sender_"+payload[:min(20, len(payload))], func(t *testing.T) {
			filter := MessageFilter{Sender: payload}
			// Should not panic or cause SQL error - just return empty results
			msgs, err := env.Engine.ListMessages(env.Ctx, filter)
			if err != nil {
				t.Errorf("unexpected error with payload %q: %v", payload, err)
			}
			// Should return 0 results (no match), not all messages
			if len(msgs) != 0 {
				t.Errorf("expected 0 results for SQL injection payload, got %d", len(msgs))
			}
		})

		t.Run("Label_"+payload[:min(20, len(payload))], func(t *testing.T) {
			filter := MessageFilter{Label: payload}
			msgs, err := env.Engine.ListMessages(env.Ctx, filter)
			if err != nil {
				t.Errorf("unexpected error with payload %q: %v", payload, err)
			}
			if len(msgs) != 0 {
				t.Errorf("expected 0 results for SQL injection payload, got %d", len(msgs))
			}
		})
	}

	// Verify the database is still intact after all injection attempts
	var count int
	err := env.DB.QueryRow("SELECT COUNT(*) FROM messages").Scan(&count)
	if err != nil {
		t.Fatalf("failed to count messages after injection tests: %v", err)
	}
	if count != 5 { // Standard seed data has 5 messages
		t.Errorf("expected 5 messages in database after injection tests, got %d", count)
	}
}
