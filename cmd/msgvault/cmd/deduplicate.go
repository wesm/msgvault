package cmd

import (
	"bufio"
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
	dedupUndo                 string
	dedupAccount              string
	dedupDeleteFromSourceSrvr bool
	dedupYes                  bool
)

func runDeduplicate(cmd *cobra.Command, _ []string) error {
	st, err := openStore()
	if err != nil {
		return err
	}
	defer func() { _ = st.Close() }()

	dbPath := cfg.DatabaseDSN()

	deletionsDir := filepath.Join(cfg.Data.DataDir, "deletions")

	preference := dedup.DefaultSourcePreference
	if dedupPrefer != "" {
		preference = strings.Split(dedupPrefer, ",")
		for i := range preference {
			preference[i] = strings.TrimSpace(preference[i])
		}
	}

	var (
		accountSourceIDs []int64
		canonicalAccount string
	)
	if dedupAccount != "" {
		scope, err := ResolveAccount(st, dedupAccount)
		if err != nil {
			return err
		}
		accountSourceIDs = scope.SourceIDs()
		canonicalAccount = scope.DisplayName()
	}

	identityAddrs := cfg.IdentityAddressSet()
	if len(identityAddrs) > 0 {
		logger.Info("dedup identity addresses loaded",
			"count", len(identityAddrs))
	}

	config := dedup.Config{
		SourcePreference:           preference,
		ContentHashFallback:        dedupContentHash,
		DryRun:                     dedupDryRun,
		AccountSourceIDs:           accountSourceIDs,
		Account:                    canonicalAccount,
		DeleteDupsFromSourceServer: dedupDeleteFromSourceSrvr,
		DeletionsDir:               deletionsDir,
		IdentityAddresses:          identityAddrs,
	}

	engine := dedup.NewEngine(st, config, logger)

	if dedupUndo != "" {
		restored, stillRunning, err := engine.Undo(dedupUndo)
		if err != nil {
			return fmt.Errorf("undo dedup: %w", err)
		}
		fmt.Printf("Restored %d messages from batch %q.\n",
			restored, dedupUndo)
		if len(stillRunning) > 0 {
			fmt.Printf(
				"\nWarning: the following deletion manifests are " +
					"already in progress\nand could not be cancelled:\n",
			)
			for _, id := range stillRunning {
				fmt.Printf("  - %s\n", id)
			}
		}
		return nil
	}

	if len(accountSourceIDs) == 0 {
		return runDeduplicatePerSource(cmd, st, config)
	}

	return runDeduplicateOnce(cmd, dbPath, config, engine)
}

func runDeduplicatePerSource(
	cmd *cobra.Command,
	st *store.Store,
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

	anyRan := false
	for _, src := range sources {
		cfgScoped := cfgBase
		cfgScoped.AccountSourceIDs = []int64{src.ID}
		cfgScoped.Account = src.Identifier
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

		batchID := fmt.Sprintf(
			"dedup-%s", time.Now().Format("20060102-150405"),
		)
		summary, err := engineScoped.Execute(
			cmd.Context(), report, batchID,
		)
		if err != nil {
			return fmt.Errorf("execute %s: %w", src.Identifier, err)
		}
		printDedupSummary(summary)
		fmt.Println()
	}

	if cfgBase.DryRun {
		fmt.Println("\nDry run complete. No changes made.")
	} else if !anyRan {
		fmt.Println("No duplicates found in any source.")
	}
	return nil
}

func runDeduplicateOnce(
	cmd *cobra.Command,
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
		if err := copyFileForBackup(dbPath, backupPath); err != nil {
			return fmt.Errorf("backup database: %w", err)
		}
	}

	batchID := fmt.Sprintf(
		"dedup-%s", time.Now().Format("20060102-150405"),
	)
	fmt.Println("Merging duplicates...")
	summary, err := engine.Execute(cmd.Context(), report, batchID)
	if err != nil {
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

func copyFileForBackup(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return err
	}
	return out.Close()
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
	deduplicateCmd.Flags().StringVar(&dedupUndo, "undo", "",
		"Undo a previous dedup run by batch ID")
	deduplicateCmd.Flags().StringVar(&dedupAccount, "account", "",
		"Dedup across all sources for this account")
	deduplicateCmd.Flags().BoolVar(&dedupDeleteFromSourceSrvr,
		"delete-dups-from-source-server", false,
		"DESTRUCTIVE: stage pruned duplicates for remote deletion")
	deduplicateCmd.Flags().BoolVarP(&dedupYes, "yes", "y", false,
		"Skip confirmation prompt")
}
