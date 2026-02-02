package search

import (
	"reflect"
	"testing"
)

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
