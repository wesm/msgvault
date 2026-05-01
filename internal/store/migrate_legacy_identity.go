package store

import (
	"database/sql"
	"fmt"
	"strings"
)

const migrationLegacyIdentity = "legacy_identity_to_per_account"

// MigrateLegacyIdentityConfig migrates a list of legacy global identity
// addresses into per-account confirmed records. It runs at most once:
// subsequent calls are no-ops, marked by the
// "legacy_identity_to_per_account" entry in applied_migrations. An
// empty or blank-only address list still marks the migration applied so
// a later config change does not re-run the migration unexpectedly.
//
// Returns (applied bool, deferred bool, sourceCount int, addressCount int, err error).
//
//	applied:      true if this call performed the migration; false if
//	              already applied or no addresses to migrate.
//	deferred:     true when legacy addresses are configured but no
//	              sources exist yet, so the migration is parked until
//	              the user adds an account. Distinguishable from the
//	              "already applied" / "no addresses" no-ops.
//	sourceCount:  number of accounts that received identity records.
//	addressCount: number of distinct addresses migrated (per source).
//
// Migration semantics: every existing source receives a copy of every
// legacy address. After this call, the legacy [identity] config block
// is no longer load-bearing; the dedup engine should read from
// account_identities instead.
func (s *Store) MigrateLegacyIdentityConfig(addresses []string) (applied, deferred bool, sourceCount, addressCount int, err error) {
	already, err := s.IsMigrationApplied(migrationLegacyIdentity)
	if err != nil {
		return false, false, 0, 0, err
	}
	if already {
		return false, false, 0, 0, nil
	}

	// Normalize addresses: trim whitespace, deduplicate, drop empties.
	seen := make(map[string]struct{}, len(addresses))
	var normalized []string
	for _, addr := range addresses {
		a := strings.TrimSpace(addr)
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
			return false, false, 0, 0, err
		}
		return false, false, 0, 0, nil
	}

	sources, err := s.ListSources("")
	if err != nil {
		return false, false, 0, 0, fmt.Errorf("list sources for identity migration: %w", err)
	}

	// If the user has legacy [identity] addresses configured but no
	// sources exist yet (typical at init-db time, or before the first
	// `add-account`), defer the migration. Marking it applied now would
	// permanently drop the addresses on the floor: the next account the
	// user adds would never receive them. Leave the sentinel unmarked
	// and let the next command run after a source exists pick it up.
	//
	// Report the post-normalization address count so the deferred
	// notice doesn't overstate (raw input may include blanks/dupes).
	if len(sources) == 0 {
		return false, true, 0, len(normalized), nil
	}

	if err := s.withTx(func(tx *loggedTx) error {
		var appliedMarker string
		err := tx.QueryRow(
			`SELECT name FROM applied_migrations WHERE name = ?`,
			migrationLegacyIdentity,
		).Scan(&appliedMarker)
		switch {
		case err == nil:
			return nil
		case err != sql.ErrNoRows:
			return fmt.Errorf("check migration %q in tx: %w", migrationLegacyIdentity, err)
		}

		for _, src := range sources {
			for _, addr := range normalized {
				var existing string
				err := tx.QueryRow(
					`SELECT source_signal FROM account_identities
					 WHERE source_id = ? AND address = ?`,
					src.ID, addr,
				).Scan(&existing)
				switch {
				case err == sql.ErrNoRows:
					_, txErr := tx.Exec(
						`INSERT INTO account_identities (source_id, address, source_signal)
						 VALUES (?, ?, ?)`,
						src.ID, addr, "config_migration",
					)
					if txErr != nil {
						return fmt.Errorf("insert identity (source=%d, addr=%s): %w", src.ID, addr, txErr)
					}
				case err != nil:
					return fmt.Errorf("read existing identity (source=%d, addr=%s): %w", src.ID, addr, err)
				default:
					merged := mergeSignalSet(existing, "config_migration")
					if merged != existing {
						_, txErr := tx.Exec(
							`UPDATE account_identities
							 SET source_signal = ?
							 WHERE source_id = ? AND address = ?`,
							merged, src.ID, addr,
						)
						if txErr != nil {
							return fmt.Errorf("update identity (source=%d, addr=%s): %w", src.ID, addr, txErr)
						}
					}
				}
			}
		}

		_, txErr := tx.Exec(
			s.dialect.InsertOrIgnore(`INSERT OR IGNORE INTO applied_migrations (name) VALUES (?)`),
			migrationLegacyIdentity,
		)
		return txErr
	}); err != nil {
		return false, false, 0, 0, fmt.Errorf("migrate legacy identity config: %w", err)
	}

	return true, false, len(sources), len(normalized), nil
}

// RunStartupMigrations runs all one-time data migrations that should execute
// on every command launch. It is idempotent: already-applied migrations are
// skipped. legacyIdentityAddresses comes from cfg.Identity.Addresses.
//
// Returns a non-empty notice string when migration was actually performed,
// or when legacy [identity] addresses are configured but no source exists
// yet (deferred path) so the user is told their config is parked rather
// than silently dropped. Caller should print the notice to stderr.
// Returns empty string when migration was already applied or had nothing
// to migrate.
func (s *Store) RunStartupMigrations(legacyIdentityAddresses []string) (string, error) {
	applied, deferred, sources, addrs, err := s.MigrateLegacyIdentityConfig(legacyIdentityAddresses)
	if err != nil {
		return "", err
	}
	if deferred {
		return fmt.Sprintf(
			"Notice: legacy [identity] config has %d address(es) but no accounts exist yet.\n"+
				"The migration will run on the next command after you add an account\n"+
				"(e.g. 'msgvault add-account ...').",
			addrs,
		), nil
	}
	if !applied {
		return "", nil
	}
	return fmt.Sprintf(
		"Migrated legacy [identity] config to per-account identities (%d addresses across %d accounts).\n"+
			"Run 'msgvault identity list' to review per-account identities;\n"+
			"the [identity] block in config.toml is no longer used.",
		addrs, sources,
	), nil
}
