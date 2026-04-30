package cmd

import (
	"fmt"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"
)

var dedupPurgeCmd = &cobra.Command{
	Use:   "dedup-purge",
	Short: "Permanently delete dedup-hidden messages from the local archive",
	Long: `Permanently delete dedup-hidden messages from the local archive. This is
the third rung of the safety progression: scan -> hide -> local hard
delete -> remote delete. Each rung is a separate, explicit user action.

Use --batch <id> to purge rows hidden by a specific dedup batch.
Use --all-hidden to purge every dedup-hidden row regardless of batch.

Purged rows cannot be recovered with --undo. Pending remote-deletion
manifests still reference Gmail/IMAP message IDs and remain valid
after a local purge.

Vector and parquet caches may contain stale entries for purged rows
until rebuilt. Run 'msgvault build-cache --full-rebuild' after a large
purge.`,
	RunE: runDedupPurge,
}

var (
	purgeBatchIDs  []string
	purgeAllHidden bool
	purgeNoBackup  bool
	purgeYes       bool
)

func runDedupPurge(cmd *cobra.Command, _ []string) error {
	if IsRemoteMode() {
		if len(purgeBatchIDs) > 0 {
			return fmt.Errorf("--batch is not supported in remote mode")
		}
		if purgeAllHidden {
			return fmt.Errorf("--all-hidden is not supported in remote mode")
		}
	}

	if len(purgeBatchIDs) == 0 && !purgeAllHidden {
		return fmt.Errorf("must specify --batch or --all-hidden")
	}

	st, err := openStoreAndInit()
	if err != nil {
		return err
	}
	defer func() { _ = st.Close() }()

	dbPath := cfg.DatabaseDSN()

	// compute pre-purge stats and totalN before prompting.
	var totalN int64
	if purgeAllHidden {
		var distinctBatches int64
		err = st.DB().QueryRow(
			st.Rebind("SELECT COUNT(*), COUNT(DISTINCT delete_batch_id) FROM messages WHERE deleted_at IS NOT NULL AND delete_batch_id IS NOT NULL"),
		).Scan(&totalN, &distinctBatches)
		if err != nil {
			return fmt.Errorf("count hidden messages: %w", err)
		}
		fmt.Printf("Will permanently delete %d hidden message(s) from %d distinct batch(es).\n",
			totalN, distinctBatches)
	} else {
		type batchStat struct {
			id  string
			cnt int64
		}
		stats := make([]batchStat, 0, len(purgeBatchIDs))
		for _, id := range purgeBatchIDs {
			var cnt int64
			err = st.DB().QueryRow(
				st.Rebind("SELECT COUNT(*) FROM messages WHERE delete_batch_id = ? AND deleted_at IS NOT NULL"),
				id,
			).Scan(&cnt)
			if err != nil {
				return fmt.Errorf("count rows for batch %q: %w", id, err)
			}
			totalN += cnt
			stats = append(stats, batchStat{id: id, cnt: cnt})
		}
		fmt.Printf("Will permanently delete %d hidden message(s) from %d batch(es):\n",
			totalN, len(purgeBatchIDs))
		for _, s := range stats {
			fmt.Printf("  %s: %d row(s)\n", s.id, s.cnt)
		}
	}

	if totalN == 0 {
		fmt.Println("Nothing to purge.")
		return nil
	}

	if !purgeYes {
		fmt.Print("Proceed? This is irreversible. [y/N]: ")
		ok, err := readDedupYesNo(cmd)
		if err != nil {
			return err
		}
		if !ok {
			fmt.Println("Aborted.")
			return nil
		}
	}

	if !purgeNoBackup {
		backupPath := filepath.Join(
			filepath.Dir(dbPath),
			filepath.Base(dbPath)+".dedup-purge-backup-"+time.Now().Format("20060102-150405"),
		)
		fmt.Printf("Backing up database to %s...\n", filepath.Base(backupPath))
		if err := backupDatabase(st, backupPath); err != nil {
			return fmt.Errorf("backup database: %w", err)
		}
	}

	// Note: vector and parquet caches may contain entries for purged
	// rows; the user-facing summary recommends 'build-cache --full-rebuild'.

	var purgedTotal int64
	var batchCount int64
	if purgeAllHidden {
		deleted, distinct, err := st.PurgeAllHidden()
		if err != nil {
			return fmt.Errorf("purge all hidden: %w", err)
		}
		purgedTotal = deleted
		batchCount = distinct
	} else {
		batchCount = int64(len(purgeBatchIDs))
		for _, id := range purgeBatchIDs {
			deleted, err := st.PurgeBatch(id)
			if err != nil {
				return fmt.Errorf("purge batch %q: %w", id, err)
			}
			purgedTotal += deleted
		}
	}

	fmt.Printf("\nPurged %d message(s) from %d batch(es).\n\n", purgedTotal, batchCount)
	fmt.Println("Vector and parquet caches may have stale entries; run")
	fmt.Println("'msgvault build-cache --full-rebuild' to rebuild them.")

	return nil
}

func init() {
	rootCmd.AddCommand(dedupPurgeCmd)
	dedupPurgeCmd.Flags().StringArrayVar(&purgeBatchIDs, "batch", nil,
		"Purge rows hidden by this batch ID (repeat for multiple batches)")
	dedupPurgeCmd.Flags().BoolVar(&purgeAllHidden, "all-hidden", false,
		"Purge every dedup-hidden row regardless of batch")
	dedupPurgeCmd.MarkFlagsMutuallyExclusive("batch", "all-hidden")
	dedupPurgeCmd.Flags().BoolVar(&purgeNoBackup, "no-backup", false,
		"Skip database backup before purging")
	dedupPurgeCmd.Flags().BoolVarP(&purgeYes, "yes", "y", false,
		"Skip confirmation prompt")
}
