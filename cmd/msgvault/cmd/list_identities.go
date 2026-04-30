package cmd

import (
	"bytes"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/spf13/cobra"
	"github.com/wesm/msgvault/internal/store"
)

var (
	identitiesAccount    string
	identitiesCollection string
	identitiesJSON       bool
	identitiesTOML       bool
	identitiesMinCount   int64
	identitiesMatch      string
	identitiesConfirmed  bool
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

Use --account <email> or --collection <name> to restrict to a single account
or all member accounts of a collection.

Use --confirmed to show the persisted confirmed identities for the scope
instead of the discovery output. For --collection scope, this shows the union
of confirmed identities across all member accounts.

Use --toml to generate a ready-to-paste [identity] config block for
deduplicate's sent-copy detection.`,
	RunE: runListIdentities,
}

func runListIdentities(_ *cobra.Command, _ []string) error {
	if identitiesJSON && identitiesTOML {
		return fmt.Errorf("--json and --toml are mutually exclusive")
	}

	st, err := openStoreAndInit()
	if err != nil {
		return err
	}
	defer func() { _ = st.Close() }()

	var scopeIDs []int64
	scopeLabel := "all"
	switch {
	case identitiesAccount != "":
		scope, err := ResolveAccountFlag(st, identitiesAccount)
		if err != nil {
			return err
		}
		scopeIDs = scope.SourceIDs()
		if len(scopeIDs) == 0 {
			return fmt.Errorf("--account %q resolved to zero sources", identitiesAccount)
		}
		scopeLabel = scope.DisplayName()
	case identitiesCollection != "":
		scope, err := ResolveCollectionFlag(st, identitiesCollection)
		if err != nil {
			return err
		}
		scopeIDs = scope.SourceIDs()
		if len(scopeIDs) == 0 {
			return fmt.Errorf("--collection %q has no member accounts", identitiesCollection)
		}
		scopeLabel = scope.DisplayName()
	}

	if identitiesConfirmed {
		// Default to the union across all sources when no scope is set.
		// GetIdentitiesForScope deliberately returns an empty map for an
		// empty input slice (an "explicit empty scope" means no match);
		// here the user is asking for "all confirmed identities," so
		// expand to every source.
		if len(scopeIDs) == 0 {
			sources, err := st.ListSources("")
			if err != nil {
				return fmt.Errorf("list sources: %w", err)
			}
			scopeIDs = make([]int64, len(sources))
			for i, src := range sources {
				scopeIDs[i] = src.ID
			}
		}
		return runListConfirmedIdentities(st, scopeIDs, scopeLabel)
	}

	var matcher *regexp.Regexp
	if identitiesMatch != "" {
		pattern := identitiesMatch
		if !strings.HasPrefix(pattern, "(?i)") {
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

	switch {
	case identitiesJSON:
		return writeIdentitiesJSON(candidates)
	case identitiesTOML:
		return writeIdentitiesTOML(candidates)
	default:
		return writeIdentitiesTable(candidates)
	}
}

func runListConfirmedIdentities(st *store.Store, scopeIDs []int64, scopeLabel string) error {
	addrs, err := st.GetIdentitiesForScope(scopeIDs)
	if err != nil {
		return fmt.Errorf("get confirmed identities: %w", err)
	}
	if len(addrs) == 0 {
		fmt.Printf("No confirmed identities for scope %q.\n", scopeLabel)
		fmt.Println("Run 'msgvault list-identities' to discover candidates, then confirm them.")
		return nil
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(w, "ADDRESS")
	sorted := make([]string, 0, len(addrs))
	for addr := range addrs {
		sorted = append(sorted, addr)
	}
	sort.Strings(sorted)
	for _, addr := range sorted {
		_, _ = fmt.Fprintln(w, addr)
	}
	_ = w.Flush()
	fmt.Printf("\n%d confirmed address(es) for scope %q\n", len(sorted), scopeLabel)
	return nil
}

func writeIdentitiesTOML(candidates []store.IdentityCandidate) error {
	if len(candidates) == 0 {
		fmt.Println("# no candidates — nothing to paste")
		return nil
	}

	addrs := make([]string, 0, len(candidates))
	for _, c := range candidates {
		addrs = append(addrs, c.Email)
	}

	cfg := struct {
		Identity struct {
			Addresses []string `toml:"addresses"`
		} `toml:"identity"`
	}{}
	cfg.Identity.Addresses = addrs

	var buf bytes.Buffer
	if err := toml.NewEncoder(&buf).Encode(cfg); err != nil {
		return fmt.Errorf("encode TOML: %w", err)
	}
	fmt.Print(buf.String())
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
	return printJSON(payload)
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
	listIdentitiesCmd.Flags().StringVar(&identitiesCollection,
		"collection", "", "Restrict to all member accounts of one collection")
	listIdentitiesCmd.MarkFlagsMutuallyExclusive("account", "collection")
	listIdentitiesCmd.Flags().BoolVar(&identitiesJSON,
		"json", false, "Output as JSON")
	listIdentitiesCmd.Flags().BoolVar(&identitiesTOML,
		"toml", false, "Output a ready-to-paste [identity] config block")
	listIdentitiesCmd.MarkFlagsMutuallyExclusive("json", "toml")
	listIdentitiesCmd.Flags().Int64Var(&identitiesMinCount,
		"min-count", 0, "Drop addresses with fewer than N messages")
	listIdentitiesCmd.Flags().StringVar(&identitiesMatch,
		"match", "", "Filter by Go regex (case-insensitive)")
	listIdentitiesCmd.Flags().BoolVar(&identitiesConfirmed,
		"confirmed", false, "Show persisted confirmed identities instead of discovery output")
	rootCmd.AddCommand(listIdentitiesCmd)
}
