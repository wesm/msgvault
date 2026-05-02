package store

import "testing"

// TestIdentifierMatch_TableDriven asserts the SQL-composition contract
// of newIdentifierMatch for representative inputs. Email-shaped tokens
// produce a LOWER()-wrapped predicate; everything else produces a
// case-sensitive predicate. BindValue is always the raw input.
//
// The classification rule is "@ not at index 0 AND right side contains
// a dot" — see looksLikeEmail. This test treats that rule as the
// contract; TestLooksLikeEmail tests the predicate directly.
func TestIdentifierMatch_TableDriven(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		wantWhere string
	}{
		{"email", "foo@x.com", "LOWER(address) = LOWER(?)"},
		{"email_mixed_case", "Foo@X.COM", "LOWER(address) = LOWER(?)"},
		{"matrix_mxid", "@alice:matrix.org", "address = ?"},
		{"bare_handle", "AliceHandle", "address = ?"},
		{"phone", "+15551234567", "address = ?"},
		{"email_no_dot", "alice@localhost", "address = ?"},
		{"empty", "", "address = ?"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := newIdentifierMatch(tc.input)
			if got := m.WhereClause("address"); got != tc.wantWhere {
				t.Errorf("WhereClause(%q) = %q, want %q", tc.input, got, tc.wantWhere)
			}
			if got := m.BindValue(); got != tc.input {
				t.Errorf("BindValue() = %q, want %q (raw)", got, tc.input)
			}
		})
	}
}

// TestIdentifierMatch_WhereClauseAcceptsCustomColumn asserts the helper
// is column-name-driven so call sites can specify their own column
// (today every site uses "address", but the contract supports more).
func TestIdentifierMatch_WhereClauseAcceptsCustomColumn(t *testing.T) {
	m := newIdentifierMatch("foo@x.com")
	if got := m.WhereClause("normalized"); got != "LOWER(normalized) = LOWER(?)" {
		t.Errorf("WhereClause(\"normalized\") = %q", got)
	}
	m2 := newIdentifierMatch("AliceHandle")
	if got := m2.WhereClause("col"); got != "col = ?" {
		t.Errorf("WhereClause(\"col\") = %q", got)
	}
}

// TestLooksLikeEmail asserts the email-shape predicate directly. The
// regression cases (iter2→iter3 Matrix MXID misclassification) are
// the load-bearing rows here: a future refactor that loosens the
// predicate to "@ contains" must fail this test.
func TestLooksLikeEmail(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  bool
	}{
		{"plain_email", "foo@x.com", true},
		{"mixed_case", "Foo@X.COM", true},
		{"subdomain", "foo@mail.x.com", true},
		{"matrix_mxid", "@alice:matrix.org", false},
		{"matrix_mxid_with_subdomain", "@alice:server.matrix.org", false},
		{"bare_handle", "AliceHandle", false},
		{"phone_e164", "+15551234567", false},
		{"empty", "", false},
		{"email_no_dot", "alice@localhost", false},
		{"trailing_at", "alice@", false},
		{"leading_at_only", "@", false},
		{"single_char", "a", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := looksLikeEmail(tc.input); got != tc.want {
				t.Errorf("looksLikeEmail(%q) = %v, want %v", tc.input, got, tc.want)
			}
		})
	}
}
