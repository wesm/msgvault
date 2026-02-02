package search

import (
	"reflect"
	"testing"
	"time"
)

func utcDate(y int, m time.Month, d int) time.Time {
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}

func boolPtr(v bool) *bool    { return &v }
func i64Ptr(v int64) *int64   { return &v }
func timePtr(v time.Time) *time.Time { return &v }

// assertQueryEqual compares two Query structs field by field, treating nil
// slices and empty slices as equivalent.
func assertQueryEqual(t *testing.T, got, want Query) {
	t.Helper()

	stringsEqual := func(field string, g, w []string) {
		if len(g) == 0 && len(w) == 0 {
			return
		}
		if !reflect.DeepEqual(g, w) {
			t.Errorf("%s: got %v, want %v", field, g, w)
		}
	}

	stringsEqual("TextTerms", got.TextTerms, want.TextTerms)
	stringsEqual("FromAddrs", got.FromAddrs, want.FromAddrs)
	stringsEqual("ToAddrs", got.ToAddrs, want.ToAddrs)
	stringsEqual("CcAddrs", got.CcAddrs, want.CcAddrs)
	stringsEqual("BccAddrs", got.BccAddrs, want.BccAddrs)
	stringsEqual("SubjectTerms", got.SubjectTerms, want.SubjectTerms)
	stringsEqual("Labels", got.Labels, want.Labels)

	if !reflect.DeepEqual(got.HasAttachment, want.HasAttachment) {
		t.Errorf("HasAttachment: got %v, want %v", got.HasAttachment, want.HasAttachment)
	}
	if !reflect.DeepEqual(got.BeforeDate, want.BeforeDate) {
		t.Errorf("BeforeDate: got %v, want %v", got.BeforeDate, want.BeforeDate)
	}
	if !reflect.DeepEqual(got.AfterDate, want.AfterDate) {
		t.Errorf("AfterDate: got %v, want %v", got.AfterDate, want.AfterDate)
	}
	if !reflect.DeepEqual(got.LargerThan, want.LargerThan) {
		t.Errorf("LargerThan: got %v, want %v", got.LargerThan, want.LargerThan)
	}
	if !reflect.DeepEqual(got.SmallerThan, want.SmallerThan) {
		t.Errorf("SmallerThan: got %v, want %v", got.SmallerThan, want.SmallerThan)
	}
}

func TestParse(t *testing.T) {
	tests := []struct {
		name  string
		query string
		want  Query
	}{
		// Basic Operators
		{
			name:  "from operator",
			query: "from:alice@example.com",
			want:  Query{FromAddrs: []string{"alice@example.com"}},
		},
		{
			name:  "to operator",
			query: "to:bob@example.com",
			want:  Query{ToAddrs: []string{"bob@example.com"}},
		},
		{
			name:  "multiple from",
			query: "from:alice@example.com from:bob@example.com",
			want:  Query{FromAddrs: []string{"alice@example.com", "bob@example.com"}},
		},
		{
			name:  "bare text",
			query: "hello world",
			want:  Query{TextTerms: []string{"hello", "world"}},
		},
		{
			name:  "quoted phrase",
			query: `"hello world"`,
			want:  Query{TextTerms: []string{"hello world"}},
		},
		{
			name:  "mixed operators and text",
			query: "from:alice@example.com meeting notes",
			want: Query{
				FromAddrs: []string{"alice@example.com"},
				TextTerms: []string{"meeting", "notes"},
			},
		},

		// Quoted Operator Values
		{
			name:  "subject with quoted phrase",
			query: `subject:"meeting notes"`,
			want:  Query{SubjectTerms: []string{"meeting notes"}},
		},
		{
			name:  "subject with quoted phrase and other terms",
			query: `subject:"project update" from:alice@example.com`,
			want: Query{
				SubjectTerms: []string{"project update"},
				FromAddrs:    []string{"alice@example.com"},
			},
		},
		{
			name:  "label with quoted value containing spaces",
			query: `label:"My Important Label"`,
			want:  Query{Labels: []string{"My Important Label"}},
		},
		{
			name:  "mixed quoted and unquoted",
			query: `subject:urgent subject:"very important" search term`,
			want: Query{
				SubjectTerms: []string{"urgent", "very important"},
				TextTerms:    []string{"search", "term"},
			},
		},
		{
			name:  "from with quoted display name style (edge case)",
			query: `from:"alice@example.com"`,
			want:  Query{FromAddrs: []string{"alice@example.com"}},
		},

		// Quoted Phrases With Colons
		{
			name:  "quoted phrase with colon",
			query: `"foo:bar"`,
			want:  Query{TextTerms: []string{"foo:bar"}},
		},
		{
			name:  "quoted phrase with time",
			query: `"meeting at 10:30"`,
			want:  Query{TextTerms: []string{"meeting at 10:30"}},
		},
		{
			name:  "quoted phrase with URL-like content",
			query: `"check http://example.com"`,
			want:  Query{TextTerms: []string{"check http://example.com"}},
		},
		{
			name:  "quoted phrase with multiple colons",
			query: `"a:b:c:d"`,
			want:  Query{TextTerms: []string{"a:b:c:d"}},
		},
		{
			name:  "quoted colon phrase mixed with real operator",
			query: `from:alice@example.com "subject:not an operator"`,
			want: Query{
				FromAddrs: []string{"alice@example.com"},
				TextTerms: []string{"subject:not an operator"},
			},
		},
		{
			name:  "operator followed by quoted colon phrase",
			query: `"re: meeting notes" from:bob@example.com`,
			want: Query{
				TextTerms: []string{"re: meeting notes"},
				FromAddrs: []string{"bob@example.com"},
			},
		},

		// Labels
		{
			name:  "multiple labels",
			query: "label:INBOX l:work",
			want:  Query{Labels: []string{"INBOX", "work"}},
		},

		// Subject
		{
			name:  "simple subject",
			query: "subject:urgent",
			want:  Query{SubjectTerms: []string{"urgent"}},
		},

		// Has Attachment
		{
			name:  "has attachment",
			query: "has:attachment",
			want:  Query{HasAttachment: boolPtr(true)},
		},

		// Dates
		{
			name:  "after and before dates",
			query: "after:2024-01-15 before:2024-06-30",
			want: Query{
				AfterDate:  timePtr(utcDate(2024, 1, 15)),
				BeforeDate: timePtr(utcDate(2024, 6, 30)),
			},
		},

		// Sizes
		{
			name:  "larger than 5M",
			query: "larger:5M",
			want:  Query{LargerThan: i64Ptr(5 * 1024 * 1024)},
		},
		{
			name:  "smaller than 100K",
			query: "smaller:100K",
			want:  Query{SmallerThan: i64Ptr(100 * 1024)},
		},
		{
			name:  "larger than 1G",
			query: "larger:1G",
			want:  Query{LargerThan: i64Ptr(1024 * 1024 * 1024)},
		},

		// Complex Query
		{
			name:  "complex query",
			query: `from:alice@example.com to:bob@example.com subject:meeting has:attachment after:2024-01-01 "project report"`,
			want: Query{
				FromAddrs:     []string{"alice@example.com"},
				ToAddrs:       []string{"bob@example.com"},
				SubjectTerms:  []string{"meeting"},
				TextTerms:     []string{"project report"},
				HasAttachment: boolPtr(true),
				AfterDate:     timePtr(utcDate(2024, 1, 1)),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Parse(tt.query)
			assertQueryEqual(t, *got, tt.want)
		})
	}
}

func TestParse_RelativeDates(t *testing.T) {
	q := Parse("newer_than:7d")
	expected := time.Now().UTC().AddDate(0, 0, -7)
	if q.AfterDate == nil {
		t.Fatal("AfterDate: expected not nil")
	}
	diff := q.AfterDate.Sub(expected)
	if diff < -time.Second || diff > time.Second {
		t.Errorf("AfterDate: got %v, expected within 1s of %v", *q.AfterDate, expected)
	}
}

func TestQuery_IsEmpty(t *testing.T) {
	tests := []struct {
		query   string
		isEmpty bool
	}{
		{"", true},
		{"from:alice@example.com", false},
		{"hello", false},
		{"has:attachment", false},
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			q := Parse(tt.query)
			if q.IsEmpty() != tt.isEmpty {
				t.Errorf("IsEmpty(%q): got %v, want %v", tt.query, q.IsEmpty(), tt.isEmpty)
			}
		})
	}
}
