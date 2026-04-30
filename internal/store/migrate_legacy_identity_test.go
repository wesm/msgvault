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

	applied, sources, addrs, err := st.MigrateLegacyIdentityConfig(addresses)
	testutil.MustNoErr(t, err, "MigrateLegacyIdentityConfig")

	if !applied {
		t.Error("applied should be true on first run")
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

func TestMigrateLegacyIdentityConfig_SecondCallNoOp(t *testing.T) {
	f := storetest.New(t)
	st := f.Store

	addresses := []string{"alice@example.com"}

	_, _, _, err := st.MigrateLegacyIdentityConfig(addresses)
	testutil.MustNoErr(t, err, "first migration")

	applied, sources, addrs, err := st.MigrateLegacyIdentityConfig(addresses)
	testutil.MustNoErr(t, err, "second migration")

	if applied {
		t.Error("applied should be false on second call")
	}
	if sources != 0 || addrs != 0 {
		t.Errorf("second call counts = (%d, %d), want (0, 0)", sources, addrs)
	}
}

func TestMigrateLegacyIdentityConfig_EmptyAddresses(t *testing.T) {
	f := storetest.New(t)
	st := f.Store

	applied, sources, addrs, err := st.MigrateLegacyIdentityConfig(nil)
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

func TestMigrateLegacyIdentityConfig_Lowercases(t *testing.T) {
	f := storetest.New(t)
	st := f.Store

	_, _, _, err := st.MigrateLegacyIdentityConfig([]string{"ME@Example.COM"})
	testutil.MustNoErr(t, err, "MigrateLegacyIdentityConfig")

	ids, err := st.ListAccountIdentities(f.Source.ID)
	testutil.MustNoErr(t, err, "ListAccountIdentities")
	if len(ids) != 1 {
		t.Fatalf("got %d identities, want 1", len(ids))
	}
	if ids[0].Address != "me@example.com" {
		t.Errorf("address = %q, want me@example.com", ids[0].Address)
	}
}
