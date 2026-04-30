package cmd

import (
	"fmt"
	"strings"

	"github.com/wesm/msgvault/internal/store"
)

// noDefaultIdentityHelp is the flag help text for --no-default-identity.
// Each ingest command registers its own bool variable and reuses this constant.
const noDefaultIdentityHelp = "Suppress auto-default-identity at account creation. " +
	"Note: a one-time legacy [identity] config migration may still write confirmed " +
	"identifiers to the account on first post-upgrade startup."

// confirmDefaultIdentity writes one confirmed identifier to a freshly
// created source's identity. Best-effort: any error is logged and swallowed
// so a partially failed identity write never breaks ingest. Empty identifiers
// are a silent no-op.
//
// account is the user-facing account name shown in the confirmation message.
// Callers should gate this behind the per-command --no-default-identity flag.
func confirmDefaultIdentity(s *store.Store, sourceID int64, account, identifier, signal string) {
	id := strings.TrimSpace(identifier)
	if id == "" {
		return
	}
	if err := s.AddAccountIdentity(sourceID, id, signal); err != nil {
		logger.Warn("auto-default-identity write failed",
			"source_id", sourceID,
			"account", account,
			"identifier", id,
			"signal", signal,
			"error", err.Error())
		return
	}
	fmt.Printf("Confirmed identity %s on %s (signal: %s).\n", id, account, signal)
}
