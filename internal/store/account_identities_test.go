package store_test

import (
	"testing"

	"github.com/wesm/msgvault/internal/testutil"
	"github.com/wesm/msgvault/internal/testutil/storetest"
)

func TestAddAndListAccountIdentities(t *testing.T) {
	f := storetest.New(t)
	st := f.Store

	if err := st.AddAccountIdentity(f.Source.ID, "me@example.com", "manual"); err != nil {
		t.Fatalf("AddAccountIdentity: %v", err)
	}

	ids, err := st.ListAccountIdentities(f.Source.ID)
	testutil.MustNoErr(t, err, "ListAccountIdentities")
	if len(ids) != 1 {
		t.Fatalf("got %d identities, want 1", len(ids))
	}
	got := ids[0]
	if got.Address != "me@example.com" {
		t.Errorf("address = %q, want me@example.com", got.Address)
	}
	if got.SourceSignal != "manual" {
		t.Errorf("source_signal = %q, want manual", got.SourceSignal)
	}
	if got.SourceID != f.Source.ID {
		t.Errorf("source_id = %d, want %d", got.SourceID, f.Source.ID)
	}
}

func TestAddAccountIdentity_Idempotent(t *testing.T) {
	f := storetest.New(t)
	st := f.Store

	for range 2 {
		if err := st.AddAccountIdentity(f.Source.ID, "me@example.com", "manual"); err != nil {
			t.Fatalf("AddAccountIdentity: %v", err)
		}
	}

	ids, err := st.ListAccountIdentities(f.Source.ID)
	testutil.MustNoErr(t, err, "ListAccountIdentities")
	if len(ids) != 1 {
		t.Errorf("got %d rows, want 1 (insert should be idempotent)", len(ids))
	}
}

func TestAddAccountIdentity_Lowercases(t *testing.T) {
	f := storetest.New(t)
	st := f.Store

	if err := st.AddAccountIdentity(f.Source.ID, "ME@Example.COM", "manual"); err != nil {
		t.Fatalf("AddAccountIdentity: %v", err)
	}

	ids, err := st.ListAccountIdentities(f.Source.ID)
	testutil.MustNoErr(t, err, "ListAccountIdentities")
	if len(ids) != 1 {
		t.Fatalf("got %d identities, want 1", len(ids))
	}
	if ids[0].Address != "me@example.com" {
		t.Errorf("address = %q, want lowercased me@example.com", ids[0].Address)
	}
}

func TestAddAccountIdentity_SkipsEmpty(t *testing.T) {
	f := storetest.New(t)
	st := f.Store

	if err := st.AddAccountIdentity(f.Source.ID, "   ", "manual"); err != nil {
		t.Fatalf("AddAccountIdentity empty: %v", err)
	}

	ids, err := st.ListAccountIdentities(f.Source.ID)
	testutil.MustNoErr(t, err, "ListAccountIdentities")
	if len(ids) != 0 {
		t.Errorf("got %d identities, want 0 for empty address", len(ids))
	}
}

func TestGetIdentitiesForScope_MultiSource(t *testing.T) {
	f := storetest.New(t)
	st := f.Store

	src2, err := st.GetOrCreateSource("gmail", "other@example.com")
	testutil.MustNoErr(t, err, "GetOrCreateSource")

	testutil.MustNoErr(t, st.AddAccountIdentity(f.Source.ID, "alice@example.com", "manual"), "add alice")
	testutil.MustNoErr(t, st.AddAccountIdentity(src2.ID, "bob@example.com", "manual"), "add bob")

	scope, err := st.GetIdentitiesForScope([]int64{f.Source.ID, src2.ID})
	testutil.MustNoErr(t, err, "GetIdentitiesForScope")

	if len(scope) != 2 {
		t.Fatalf("got %d addresses, want 2", len(scope))
	}
	if _, ok := scope["alice@example.com"]; !ok {
		t.Error("alice@example.com missing from scope")
	}
	if _, ok := scope["bob@example.com"]; !ok {
		t.Error("bob@example.com missing from scope")
	}
}

func TestGetIdentitiesForScope_EmptyInput(t *testing.T) {
	f := storetest.New(t)
	st := f.Store

	testutil.MustNoErr(t, st.AddAccountIdentity(f.Source.ID, "me@example.com", "manual"), "add identity")

	scope, err := st.GetIdentitiesForScope([]int64{})
	testutil.MustNoErr(t, err, "GetIdentitiesForScope empty")
	if scope == nil {
		t.Error("expected non-nil map for empty scope")
	}
	if len(scope) != 0 {
		t.Errorf("got %d entries, want 0 for empty scope", len(scope))
	}
}
