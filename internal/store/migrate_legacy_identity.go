package store

import (
	"fmt"
	"strings"
)

const migrationLegacyIdentity = "legacy_identity_to_per_account"

// MigrateLegacyIdentityConfig migrates a list of legacy global identity
// addresses into per-account confirmed records. It runs at most once:
// subsequent calls are no-ops, marked by the
// "legacy_identity_to_per_account" entry in applied_migrations.
//
// Returns (applied bool, sourceCount int, addressCount int, err error).
//
//	applied:      true if this call performed the migration; false if
//	              already applied or no addresses to migrate.
//	sourceCount:  number of accounts that received identity records.
//	addressCount: number of distinct addresses migrated (per source).
//
// Migration semantics: every existing source receives a copy of every
// legacy address. After this call, the legacy [identity] config block
// is no longer load-bearing; the dedup engine should read from
// account_identities instead.
func (s *Store) MigrateLegacyIdentityConfig(addresses []string) (applied bool, sourceCount int, addressCount int, err error) {
	already, err := s.IsMigrationApplied(migrationLegacyIdentity)
	if err != nil {
		return false, 0, 0, err
	}
	if already {
		return false, 0, 0, nil
	}

	// Normalize addresses: lowercase, trim, deduplicate, drop empties.
	seen := make(map[string]struct{}, len(addresses))
	var normalized []string
	for _, addr := range addresses {
		a := strings.ToLower(strings.TrimSpace(addr))
		if a == "" {
			continue
		}
		if _, dup := seen[a]; dup {
			continue
		}
		seen[a] = struct{}{}
		normalized = append(normalized, a)
	}

	if len(normalized) == 0 {
		if err := s.MarkMigrationApplied(migrationLegacyIdentity); err != nil {
			return false, 0, 0, err
		}
		return false, 0, 0, nil
	}

	sources, err := s.ListSources("")
	if err != nil {
		return false, 0, 0, fmt.Errorf("list sources for identity migration: %w", err)
	}

	// If the user has legacy [identity] addresses configured but no
	// sources exist yet (typical at init-db time, or before the first
	// `add-account`), defer the migration. Marking it applied now would
	// permanently drop the addresses on the floor: the next account the
	// user adds would never receive them. Leave the sentinel unmarked
	// and let the next command run after a source exists pick it up.
	if len(sources) == 0 {
		return false, 0, 0, nil
	}

	if err := s.withTx(func(tx *loggedTx) error {
		for _, src := range sources {
			for _, addr := range normalized {
				_, txErr := tx.Exec(
					s.dialect.InsertOrIgnore(`INSERT OR IGNORE INTO account_identities (source_id, address, source_signal) VALUES (?, ?, ?)`),
					src.ID, addr, "config_migration",
				)
				if txErr != nil {
					return fmt.Errorf("insert identity (source=%d, addr=%s): %w", src.ID, addr, txErr)
				}
			}
		}

		_, txErr := tx.Exec(
			s.dialect.InsertOrIgnore(`INSERT OR IGNORE INTO applied_migrations (name) VALUES (?)`),
			migrationLegacyIdentity,
		)
		return txErr
	}); err != nil {
		return false, 0, 0, fmt.Errorf("migrate legacy identity config: %w", err)
	}

	return true, len(sources), len(normalized), nil
}

// RunStartupMigrations runs all one-time data migrations that should execute
// on every command launch. It is idempotent: already-applied migrations are
// skipped. legacyIdentityAddresses comes from cfg.Identity.Addresses.
//
// Returns a non-empty notice string when migration was actually performed
// (caller should print it to stderr). Returns empty string when migration was
// a no-op or already applied.
func (s *Store) RunStartupMigrations(legacyIdentityAddresses []string) (string, error) {
	applied, sources, addrs, err := s.MigrateLegacyIdentityConfig(legacyIdentityAddresses)
	if err != nil {
		return "", err
	}
	if !applied {
		return "", nil
	}
	notice := fmt.Sprintf(
		"Migrated legacy [identity] config to per-account identities (%d addresses across %d accounts).\n"+
			"Run 'msgvault list-identities' to review per-account identities;\n"+
			"the [identity] block in config.toml is no longer used.",
		addrs, sources,
	)
	return notice, nil
}
