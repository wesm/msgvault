package cmd

import (
	"bufio"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/wesm/msgvault/internal/dedup"
	"github.com/wesm/msgvault/internal/store"
)

var deduplicateCmd = &cobra.Command{
	Use:   "deduplicate",
	Short: "Find and merge duplicate messages within an account",
	Long: `Find and merge duplicate messages that were ingested through multiple paths
for the same account (for example, Gmail API sync plus an mbox export of the
same mailbox, or an IMAP sync plus an emlx import).

Duplicates are grouped by the RFC822 Message-ID header. For each group the
engine selects a survivor, unions the labels from every copy onto the
survivor, and hides the pruned copies in the msgvault database.

By default, deduplicate ONLY modifies the msgvault database. Your original
source files and remote servers are never modified. Hidden rows can be
restored with --undo, so a dedup run is fully reversible.

Use --account <email> to scope dedup to one account.
Use --collection <name> to dedup across all member accounts of a collection.
Without either flag, dedup runs per-account independently for every source.

Use --dry-run to scan and report without writing anything.
Use --content-hash to also group messages by normalized raw MIME when
Message-ID matching is insufficient.
Use --undo <batch-id> to reverse a previous dedup run.`,
	RunE: runDeduplicate,
}

var (
	dedupDryRun               bool
	dedupNoBackup             bool
	dedupPrefer               string
	dedupContentHash          bool
	dedupUndo                 []string
	dedupAccount              string
	dedupCollection           string
	dedupDeleteFromSourceSrvr bool
	dedupYes                  bool
)

func runDeduplicate(cmd *cobra.Command, _ []string) error {
	st, err := openStoreAndInit()
	if err != nil {
		return err
	}
	defer func() { _ = st.Close() }()

	dbPath := cfg.DatabaseDSN()

	deletionsDir := filepath.Join(cfg.Data.DataDir, "deletions")

	preference := dedup.DefaultSourcePreference
	if dedupPrefer != "" {
		preference = strings.Split(dedupPrefer, ",")
		known := make(map[string]bool, len(dedup.DefaultSourcePreference))
		for _, t := range dedup.DefaultSourcePreference {
			known[t] = true
		}
		for i := range preference {
			preference[i] = strings.TrimSpace(preference[i])
			if !known[preference[i]] {
				fmt.Fprintf(os.Stderr, "Warning: unknown source type in --prefer: %q\n", preference[i])
			}
		}
	}

	var (
		accountSourceIDs  []int64
		canonicalAccount  string
		scopeIsCollection bool
	)
	switch {
	case dedupAccount != "":
		scope, err := ResolveAccountFlag(st, dedupAccount)
		if err != nil {
			return err
		}
		accountSourceIDs = scope.SourceIDs()
		if len(accountSourceIDs) == 0 {
			return fmt.Errorf("--account %q resolved to zero sources", dedupAccount)
		}
		canonicalAccount = scope.DisplayName()
	case dedupCollection != "":
		scope, err := ResolveCollectionFlag(st, dedupCollection)
		if err != nil {
			return err
		}
		accountSourceIDs = scope.SourceIDs()
		if len(accountSourceIDs) == 0 {
			return fmt.Errorf("--collection %q has no member accounts", dedupCollection)
		}
		canonicalAccount = scope.DisplayName()
		scopeIsCollection = true
	}

	config := dedup.Config{
		SourcePreference:           preference,
		ContentHashFallback:        dedupContentHash,
		DryRun:                     dedupDryRun,
		AccountSourceIDs:           accountSourceIDs,
		Account:                    canonicalAccount,
		DeleteDupsFromSourceServer: dedupDeleteFromSourceSrvr,
		DeletionsDir:               deletionsDir,
	}

	if len(accountSourceIDs) > 0 {
		bySource, err := loadPerSourceIdentities(st, accountSourceIDs)
		if err != nil {
			return fmt.Errorf("load per-source identities: %w", err)
		}
		config.IdentityAddressesBySource = bySource
		if len(bySource) > 0 {
			logger.Info("dedup per-source identities loaded",
				"sources", len(bySource))
		}
	}

	if scopeIsCollection {
		allSources, err := st.ListSources("")
		if err != nil {
			return fmt.Errorf("list sources: %w", err)
		}
		idSet := make(map[int64]struct{}, len(accountSourceIDs))
		for _, id := range accountSourceIDs {
			idSet[id] = struct{}{}
		}
		var memberNames []string
		for _, src := range allSources {
			if _, ok := idSet[src.ID]; ok {
				memberNames = append(memberNames, src.Identifier)
			}
		}
		fmt.Printf("Deduping across collection %q (%d accounts: %s)\n",
			canonicalAccount, len(memberNames), strings.Join(memberNames, ", "))
	}

	engine := dedup.NewEngine(st, config, logger)

	if len(dedupUndo) > 0 {
		var allStillRunning []string
		for _, batchID := range dedupUndo {
			restored, stillRunning, err := engine.Undo(batchID)
			// Undo is best-effort: database rows may have been restored
			// even if cancelling pending manifests failed. Always report
			// the restored count and any still-running manifests before
			// returning the error so the user isn't left thinking the
			// undo did nothing.
			fmt.Printf("Restored %d messages from batch %q.\n",
				restored, batchID)
			allStillRunning = append(allStillRunning, stillRunning...)
			if err != nil {
				printStillRunningWarning(allStillRunning)
				fmt.Fprintf(os.Stderr,
					"\nError cancelling one or more pending manifests "+
						"for batch %q:\n  %v\n", batchID, err)
				return fmt.Errorf("undo dedup %q: %w", batchID, err)
			}
		}
		printStillRunningWarning(allStillRunning)
		return nil
	}

	if len(accountSourceIDs) == 0 {
		return runDeduplicatePerSource(cmd, st, dbPath, config)
	}

	return runDeduplicateOnce(cmd, st, dbPath, config, engine)
}

func runDeduplicatePerSource(
	cmd *cobra.Command,
	st *store.Store,
	dbPath string,
	cfgBase dedup.Config,
) error {
	sources, err := st.ListSources("")
	if err != nil {
		return fmt.Errorf("list sources: %w", err)
	}
	if len(sources) == 0 {
		fmt.Println("No sources found.")
		return nil
	}

	fmt.Println(
		"No --account specified; deduping each source independently.",
	)
	fmt.Println()

	backedUp := false
	anyRan := false
	var executedBatches []string
	for _, src := range sources {
		cfgScoped := cfgBase
		cfgScoped.AccountSourceIDs = []int64{src.ID}
		cfgScoped.Account = src.Identifier
		bySource, err := loadPerSourceIdentities(st, []int64{src.ID})
		if err != nil {
			return fmt.Errorf("load identities for %s: %w", src.Identifier, err)
		}
		cfgScoped.IdentityAddressesBySource = bySource
		engineScoped := dedup.NewEngine(st, cfgScoped, logger)

		fmt.Printf("--- %s (%s) ---\n", src.Identifier, src.SourceType)
		report, err := engineScoped.Scan(cmd.Context())
		if err != nil {
			return fmt.Errorf("scan %s: %w", src.Identifier, err)
		}
		if report.DuplicateGroups == 0 {
			fmt.Println("  No duplicates.")
			fmt.Println()
			continue
		}

		anyRan = true
		fmt.Print(engineScoped.FormatReport(report))
		if cfgScoped.DryRun {
			fmt.Println()
			continue
		}

		if !dedupYes {
			fmt.Printf(
				"\nProceed with deduplication for %s? "+
					"This will hide %d duplicates "+
					"(reversible with --undo). [y/N]: ",
				src.Identifier, report.DuplicateMessages,
			)
			ok, err := readDedupYesNo(cmd)
			if err != nil {
				return err
			}
			if !ok {
				fmt.Println("Skipped.")
				continue
			}
		}

		if !backedUp && !dedupNoBackup {
			backedUp = true
			backupPath := fmt.Sprintf(
				"%s.dedup-backup-%s", dbPath,
				time.Now().Format("20060102-150405"),
			)
			fmt.Printf("Backing up database to %s...\n",
				filepath.Base(backupPath))
			if err := backupDatabase(st, backupPath); err != nil {
				return fmt.Errorf("backup database: %w", err)
			}
		}

		batchID := fmt.Sprintf(
			"dedup-%s-%d-%s", time.Now().Format("20060102-150405"), src.ID, dedup.SanitizeFilenameComponent(src.Identifier),
		)
		summary, err := engineScoped.Execute(
			cmd.Context(), report, batchID,
		)
		if err != nil {
			if summary != nil && summary.GroupsMerged > 0 {
				printDedupSummary(summary)
				fmt.Println()
			}
			return fmt.Errorf("execute %s: %w", src.Identifier, err)
		}
		executedBatches = append(executedBatches, summary.BatchID)
		printDedupSummary(summary)
		fmt.Println()
	}

	if cfgBase.DryRun {
		fmt.Println("\nDry run complete. No changes made.")
	} else if !anyRan {
		fmt.Println("No duplicates found in any source.")
	} else if len(executedBatches) > 1 {
		var b strings.Builder
		b.WriteString("\nTo undo all of the above:\n  msgvault deduplicate")
		for _, id := range executedBatches {
			fmt.Fprintf(&b, " --undo %s", id)
		}
		b.WriteString("\n")
		fmt.Print(b.String())
	}
	return nil
}

func runDeduplicateOnce(
	cmd *cobra.Command,
	st *store.Store,
	dbPath string,
	cfgScoped dedup.Config,
	engine *dedup.Engine,
) error {
	fmt.Println("Scanning for duplicate messages...")
	report, err := engine.Scan(cmd.Context())
	if err != nil {
		return fmt.Errorf("scan: %w", err)
	}

	fmt.Print(engine.FormatMethodology())
	fmt.Print(engine.FormatReport(report))

	if cfgScoped.DryRun {
		fmt.Println("\nDry run complete. No changes made.")
		return nil
	}
	if report.DuplicateGroups == 0 {
		fmt.Println("\nNo duplicates found.")
		return nil
	}

	if !dedupYes {
		fmt.Printf(
			"\nProceed with deduplication? This will hide %d "+
				"duplicates (reversible with --undo). [y/N]: ",
			report.DuplicateMessages,
		)
		ok, err := readDedupYesNo(cmd)
		if err != nil {
			return err
		}
		if !ok {
			fmt.Println("Aborted.")
			return nil
		}
	}

	if !dedupNoBackup {
		backupPath := fmt.Sprintf(
			"%s.dedup-backup-%s", dbPath,
			time.Now().Format("20060102-150405"),
		)
		fmt.Printf("Backing up database to %s...\n",
			filepath.Base(backupPath))
		if err := backupDatabase(st, backupPath); err != nil {
			return fmt.Errorf("backup database: %w", err)
		}
	}

	batchID := fmt.Sprintf(
		"dedup-%s-run-%s",
		time.Now().Format("20060102-150405"),
		randomBatchToken(),
	)
	fmt.Println("Merging duplicates...")
	summary, err := engine.Execute(cmd.Context(), report, batchID)
	if err != nil {
		if summary != nil && summary.GroupsMerged > 0 {
			printDedupSummary(summary)
			fmt.Println()
		}
		return fmt.Errorf("execute: %w", err)
	}

	printDedupSummary(summary)
	fmt.Println("\nTo update analytics cache: " +
		"msgvault build-cache --full-rebuild")
	return nil
}

func printDedupSummary(summary *dedup.ExecutionSummary) {
	fmt.Printf("\n=== Deduplication Complete ===\n")
	fmt.Printf("Batch ID:            %s\n", summary.BatchID)
	fmt.Printf("Groups merged:       %d\n", summary.GroupsMerged)
	fmt.Printf("Messages pruned:     %d\n", summary.MessagesRemoved)
	fmt.Printf("Labels transferred:  %d\n", summary.LabelsTransferred)
	fmt.Printf("Raw MIME backfilled: %d\n", summary.RawMIMEBackfilled)

	if len(summary.StagedManifests) > 0 {
		fmt.Println("\nStaged deletion manifests (pending):")
		for _, m := range summary.StagedManifests {
			fmt.Printf("  %s  [%s]  %d messages  (%s)\n",
				m.ManifestID, m.SourceType, m.MessageCount, m.Account)
		}
		fmt.Println(
			"\nRun 'msgvault delete-staged' to remove the " +
				"duplicates from the remote server.",
		)
	}
	fmt.Printf("\nTo undo: msgvault deduplicate --undo %s\n",
		summary.BatchID)
}

func readDedupYesNo(cmd *cobra.Command) (bool, error) {
	reader := bufio.NewReader(cmd.InOrStdin())
	response, err := reader.ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return false, fmt.Errorf("read confirmation: %w", err)
	}
	response = strings.TrimSpace(strings.ToLower(response))
	return response == "y" || response == "yes", nil
}

// randomBatchToken returns a short random hex token used to disambiguate
// single-run dedup batch IDs from per-source batch IDs that may have been
// generated in the same second.
func randomBatchToken() string {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%08x", time.Now().UnixNano()&0xffffffff)
	}
	return hex.EncodeToString(b[:])
}

// backupDatabase writes a point-in-time consistent copy of the SQLite
// database to dst using VACUUM INTO. Unlike a file-system copy of the
// main/-wal/-shm triple, this is atomic and handles uncheckpointed WAL
// pages without any external coordination.
func backupDatabase(st *store.Store, dst string) error {
	if _, err := os.Stat(dst); err == nil {
		return fmt.Errorf("backup target already exists: %s", dst)
	}
	if _, err := st.DB().Exec("VACUUM INTO ?", dst); err != nil {
		return fmt.Errorf("vacuum into %s: %w", dst, err)
	}
	return nil
}

// loadPerSourceIdentities builds a per-source identity map for the given
// source IDs by calling GetIdentitiesForScope once per source.
func loadPerSourceIdentities(st *store.Store, sourceIDs []int64) (map[int64]map[string]struct{}, error) {
	out := make(map[int64]map[string]struct{}, len(sourceIDs))
	for _, id := range sourceIDs {
		addrs, err := st.GetIdentitiesForScope([]int64{id})
		if err != nil {
			return nil, fmt.Errorf("get identities for source %d: %w", id, err)
		}
		if len(addrs) > 0 {
			out[id] = addrs
		}
	}
	return out, nil
}

func printStillRunningWarning(ids []string) {
	if len(ids) == 0 {
		return
	}
	fmt.Printf(
		"\nWarning: the following deletion manifests are already in " +
			"progress\nand cannot be cancelled:\n",
	)
	for _, id := range ids {
		fmt.Printf("  - %s\n", id)
	}
}

func init() {
	rootCmd.AddCommand(deduplicateCmd)
	deduplicateCmd.Flags().BoolVar(&dedupDryRun, "dry-run", false,
		"Scan and report only; do not modify data")
	deduplicateCmd.Flags().BoolVar(&dedupNoBackup, "no-backup", false,
		"Skip database backup before merging")
	deduplicateCmd.Flags().StringVar(&dedupPrefer, "prefer", "",
		"Comma-separated source type preference order "+
			"(default: gmail,imap,mbox,emlx,hey)")
	deduplicateCmd.Flags().BoolVar(&dedupContentHash, "content-hash", false,
		"Also detect duplicates by normalized raw MIME content")
	deduplicateCmd.Flags().StringArrayVar(&dedupUndo, "undo", nil,
		"Undo a previous dedup run by batch ID "+
			"(repeat to undo multiple batches)")
	deduplicateCmd.Flags().StringVar(&dedupAccount, "account", "",
		"Dedup across all sources for this account")
	deduplicateCmd.Flags().StringVar(&dedupCollection, "collection", "",
		"Run dedup across all member accounts of one collection")
	deduplicateCmd.MarkFlagsMutuallyExclusive("account", "collection")
	deduplicateCmd.Flags().BoolVar(&dedupDeleteFromSourceSrvr,
		"delete-dups-from-source-server", false,
		"DESTRUCTIVE: stage pruned duplicates for remote deletion")
	deduplicateCmd.Flags().BoolVarP(&dedupYes, "yes", "y", false,
		"Skip confirmation prompt")
}
