package store_test

import (
	"testing"

	"github.com/wesm/msgvault/internal/testutil"
	"github.com/wesm/msgvault/internal/testutil/storetest"
)

func TestMigrateLegacyIdentityConfig_Basic(t *testing.T) {
	f := storetest.New(t)
	st := f.Store

	src2, err := st.GetOrCreateSource("gmail", "second@example.com")
	testutil.MustNoErr(t, err, "GetOrCreateSource")

	addresses := []string{"alice@example.com", "alice@work.com", "shared@example.com"}

	applied, deferred, sources, addrs, err := st.MigrateLegacyIdentityConfig(addresses)
	testutil.MustNoErr(t, err, "MigrateLegacyIdentityConfig")

	if !applied {
		t.Error("applied should be true on first run")
	}
	if deferred {
		t.Error("deferred should be false when sources exist")
	}
	if sources != 2 {
		t.Errorf("sources = %d, want 2", sources)
	}
	if addrs != 3 {
		t.Errorf("addrs = %d, want 3", addrs)
	}

	// Verify rows: 2 sources × 3 addresses = 6 rows total.
	for _, srcID := range []int64{f.Source.ID, src2.ID} {
		ids, listErr := st.ListAccountIdentities(srcID)
		testutil.MustNoErr(t, listErr, "ListAccountIdentities")
		if len(ids) != 3 {
			t.Errorf("source %d: got %d identities, want 3", srcID, len(ids))
		}
		for _, id := range ids {
			if id.SourceSignal != "config_migration" {
				t.Errorf("source_signal = %q, want config_migration", id.SourceSignal)
			}
		}
	}
}

func TestMigrateLegacyIdentityConfig_MergesExistingSignal(t *testing.T) {
	f := storetest.New(t)
	st := f.Store

	testutil.MustNoErr(t, st.AddAccountIdentity(f.Source.ID, "alice@example.com", "account-identifier"), "AddAccountIdentity")

	applied, _, _, _, err := st.MigrateLegacyIdentityConfig([]string{"alice@example.com"})
	testutil.MustNoErr(t, err, "MigrateLegacyIdentityConfig")
	if !applied {
		t.Fatal("applied should be true on first run")
	}

	ids, err := st.ListAccountIdentities(f.Source.ID)
	testutil.MustNoErr(t, err, "ListAccountIdentities")
	if len(ids) != 1 {
		t.Fatalf("got %d identities, want 1", len(ids))
	}
	if ids[0].SourceSignal != "account-identifier,config_migration" {
		t.Errorf("source_signal = %q, want account-identifier,config_migration", ids[0].SourceSignal)
	}
}

func TestMigrateLegacyIdentityConfig_SecondCallNoOp(t *testing.T) {
	f := storetest.New(t)
	st := f.Store

	addresses := []string{"alice@example.com"}

	_, _, _, _, err := st.MigrateLegacyIdentityConfig(addresses)
	testutil.MustNoErr(t, err, "first migration")

	applied, _, sources, addrs, err := st.MigrateLegacyIdentityConfig(addresses)
	testutil.MustNoErr(t, err, "second migration")

	if applied {
		t.Error("applied should be false on second call")
	}
	if sources != 0 || addrs != 0 {
		t.Errorf("second call counts = (%d, %d), want (0, 0)", sources, addrs)
	}
}

func TestMigrateLegacyIdentityConfig_DeferredUntilSourceExists(t *testing.T) {
	st := testutil.NewTestStore(t)

	applied, deferred, sources, addrs, err := st.MigrateLegacyIdentityConfig([]string{"alice@example.com"})
	testutil.MustNoErr(t, err, "first migration")
	if applied {
		t.Fatal("applied should be false before any sources exist")
	}
	if !deferred {
		t.Fatal("deferred should be true when addresses exist but no sources")
	}
	if sources != 0 || addrs != 0 {
		t.Fatalf("counts = (%d, %d), want (0, 0)", sources, addrs)
	}

	_, err = st.GetOrCreateSource("gmail", "alice@example.com")
	testutil.MustNoErr(t, err, "GetOrCreateSource")

	applied, deferred, sources, addrs, err = st.MigrateLegacyIdentityConfig([]string{"alice@example.com"})
	testutil.MustNoErr(t, err, "second migration")
	if !applied {
		t.Fatal("applied should be true after a source exists")
	}
	if deferred {
		t.Fatal("deferred should be false once a source exists")
	}
	if sources != 1 || addrs != 1 {
		t.Fatalf("counts = (%d, %d), want (1, 1)", sources, addrs)
	}
}

func TestMigrateLegacyIdentityConfig_EmptyAddresses(t *testing.T) {
	f := storetest.New(t)
	st := f.Store

	applied, _, sources, addrs, err := st.MigrateLegacyIdentityConfig(nil)
	testutil.MustNoErr(t, err, "MigrateLegacyIdentityConfig empty")

	if applied {
		t.Error("applied should be false for empty address list")
	}
	if sources != 0 || addrs != 0 {
		t.Errorf("counts = (%d, %d), want (0, 0)", sources, addrs)
	}

	// Migration should be marked so it won't re-run.
	wasMigrated, err := st.IsMigrationApplied("legacy_identity_to_per_account")
	testutil.MustNoErr(t, err, "IsMigrationApplied")
	if !wasMigrated {
		t.Error("migration sentinel should be set even for empty address list")
	}
}

func TestMigrateLegacyIdentityConfig_TrimsWhitespace(t *testing.T) {
	f := storetest.New(t)
	st := f.Store

	_, _, _, _, err := st.MigrateLegacyIdentityConfig([]string{"  ME@Example.COM  "})
	testutil.MustNoErr(t, err, "MigrateLegacyIdentityConfig")

	ids, err := st.ListAccountIdentities(f.Source.ID)
	testutil.MustNoErr(t, err, "ListAccountIdentities")
	if len(ids) != 1 {
		t.Fatalf("got %d identities, want 1", len(ids))
	}
	if ids[0].Address != "ME@Example.COM" {
		t.Errorf("address = %q, want ME@Example.COM", ids[0].Address)
	}
}

func TestMigrateLegacyIdentityConfig_PreservesCase(t *testing.T) {
	f := storetest.New(t)
	st := f.Store

	applied, _, _, _, err := st.MigrateLegacyIdentityConfig([]string{"Alice@Example.com"})
	testutil.MustNoErr(t, err, "MigrateLegacyIdentityConfig")
	if !applied {
		t.Fatal("expected applied=true on first run")
	}

	rows, err := st.ListAccountIdentities(f.Source.ID)
	testutil.MustNoErr(t, err, "ListAccountIdentities")
	if len(rows) != 1 {
		t.Fatalf("got %d identities, want 1", len(rows))
	}
	if rows[0].Address != "Alice@Example.com" {
		t.Errorf("address = %q, want Alice@Example.com", rows[0].Address)
	}
}
