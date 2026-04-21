package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
	"github.com/wesm/msgvault/internal/store"
)

var (
	identitiesAccount  string
	identitiesJSON     bool
	identitiesTOML     bool
	identitiesMinCount int64
	identitiesMatch    string
)

var listIdentitiesCmd = &cobra.Command{
	Use:   "list-identities",
	Short: "List every email address you've likely sent from",
	Long: `List every email address that the archive considers a likely "me"
identity, ranked by the number of sent messages attributable to each address.

Three independent signals are used for detection:
  is_from_me      messages.is_from_me set at ingest time
  sent-label      message carries a SENT label
  account-match   From: address matches the source identifier

Use --toml to generate a ready-to-paste [identity] config block for
deduplicate's sent-copy detection.`,
	RunE: runListIdentities,
}

func runListIdentities(_ *cobra.Command, _ []string) error {
	st, err := openStore()
	if err != nil {
		return err
	}
	defer func() { _ = st.Close() }()

	var scopeIDs []int64
	scopeLabel := "all"
	if identitiesAccount != "" {
		scope, err := ResolveAccount(st, identitiesAccount)
		if err != nil {
			return err
		}
		scopeIDs = scope.SourceIDs()
		scopeLabel = scope.DisplayName()
	}

	var matcher *regexp.Regexp
	if identitiesMatch != "" {
		pattern := identitiesMatch
		if len(pattern) < 4 || pattern[:4] != "(?i)" {
			pattern = "(?i)" + pattern
		}
		matcher, err = regexp.Compile(pattern)
		if err != nil {
			return fmt.Errorf(
				"invalid --match regex %q: %w",
				identitiesMatch, err,
			)
		}
	}

	started := time.Now()
	candidates, err := st.ListLikelyIdentities(scopeIDs...)
	if err != nil {
		return err
	}

	if identitiesMinCount > 0 || matcher != nil {
		trimmed := make(
			[]store.IdentityCandidate, 0, len(candidates),
		)
		for _, c := range candidates {
			if c.MessageCount < identitiesMinCount {
				continue
			}
			if matcher != nil && !matcher.MatchString(c.Email) {
				continue
			}
			trimmed = append(trimmed, c)
		}
		candidates = trimmed
	}

	logger.Info("list-identities",
		"scope", scopeLabel,
		"count", len(candidates),
		"duration_ms", time.Since(started).Milliseconds())

	if identitiesJSON && identitiesTOML {
		return fmt.Errorf("--json and --toml are mutually exclusive")
	}
	switch {
	case identitiesJSON:
		return writeIdentitiesJSON(candidates)
	case identitiesTOML:
		return writeIdentitiesTOML(candidates)
	default:
		return writeIdentitiesTable(candidates)
	}
}

func writeIdentitiesTOML(candidates []store.IdentityCandidate) error {
	if len(candidates) == 0 {
		fmt.Println("# no candidates — nothing to paste")
		return nil
	}
	fmt.Println("[identity]")
	fmt.Println("addresses = [")
	for _, c := range candidates {
		safe := strings.ReplaceAll(c.Email, `"`, `\"`)
		fmt.Printf("  \"%s\",  # %s msgs, %s\n",
			safe,
			formatCount(c.MessageCount),
			c.Signals.String(),
		)
	}
	fmt.Println("]")
	return nil
}

func writeIdentitiesTable(candidates []store.IdentityCandidate) error {
	if len(candidates) == 0 {
		fmt.Println("No likely sent-from addresses found.")
		return nil
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(w, "ADDRESS\tMESSAGES\tSOURCES\tSIGNALS")
	for _, c := range candidates {
		_, _ = fmt.Fprintf(w, "%s\t%s\t%d\t%s\n",
			c.Email,
			formatCount(c.MessageCount),
			len(c.SourceIDs),
			c.Signals.String(),
		)
	}
	_ = w.Flush()
	fmt.Printf("\n%d candidate address(es)\n", len(candidates))
	return nil
}

func writeIdentitiesJSON(candidates []store.IdentityCandidate) error {
	payload := make([]map[string]any, 0, len(candidates))
	for _, c := range candidates {
		entry := map[string]any{
			"email":         c.Email,
			"message_count": c.MessageCount,
			"source_ids":    c.SourceIDs,
			"signals":       splitSignals(c.Signals),
		}
		payload = append(payload, entry)
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(payload)
}

func splitSignals(s store.IdentitySignal) []string {
	var out []string
	if s&store.SignalFromMe != 0 {
		out = append(out, "is_from_me")
	}
	if s&store.SignalSentLabel != 0 {
		out = append(out, "sent_label")
	}
	if s&store.SignalAccountMatch != 0 {
		out = append(out, "account_match")
	}
	return out
}

func init() {
	listIdentitiesCmd.Flags().StringVar(&identitiesAccount,
		"account", "", "Restrict to a single account")
	listIdentitiesCmd.Flags().BoolVar(&identitiesJSON,
		"json", false, "Output as JSON")
	listIdentitiesCmd.Flags().BoolVar(&identitiesTOML,
		"toml", false, "Output a ready-to-paste [identity] config block")
	listIdentitiesCmd.Flags().Int64Var(&identitiesMinCount,
		"min-count", 0, "Drop addresses with fewer than N messages")
	listIdentitiesCmd.Flags().StringVar(&identitiesMatch,
		"match", "", "Filter by Go regex (case-insensitive)")
	rootCmd.AddCommand(listIdentitiesCmd)
}
