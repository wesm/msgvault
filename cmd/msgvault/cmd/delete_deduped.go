package cmd

import (
	"fmt"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"
)

var deleteDedupedCmd = &cobra.Command{
	Use:   "delete-deduped",
	Short: "Permanently delete dedup-hidden messages from the local archive",
	Long: `Permanently delete dedup-hidden messages from the local archive. This is
the third rung of the safety progression: scan -> hide -> local hard
delete -> remote delete. Each rung is a separate, explicit user action.

Use --batch <id> to delete rows hidden by a specific dedup batch.
Use --all-hidden to delete every dedup-hidden row regardless of batch.

Deleted rows cannot be recovered with --undo. Pending remote-deletion
manifests still reference Gmail/IMAP message IDs and remain valid
after a local delete.

Vector and parquet caches may contain stale entries for deleted rows
until rebuilt. Run 'msgvault build-cache --full-rebuild' after a large
delete.`,
	RunE: runDeleteDeduped,
}

var (
	deleteDedupedBatchIDs  []string
	deleteDedupedAllHidden bool
	deleteDedupedNoBackup  bool
	deleteDedupedYes       bool
)

func runDeleteDeduped(cmd *cobra.Command, _ []string) error {
	// delete-deduped mutates local SQLite directly, has no remote API
	// equivalent, and the local DB is not reachable in remote mode.
	// Reject upfront so the user gets a clear error rather than the
	// generic "must specify --batch or --all-hidden" hint.
	if IsRemoteMode() {
		return fmt.Errorf("delete-deduped is local-only; not supported in remote mode")
	}

	if len(deleteDedupedBatchIDs) == 0 && !deleteDedupedAllHidden {
		return fmt.Errorf("must specify --batch or --all-hidden")
	}

	st, err := openStoreAndInit()
	if err != nil {
		return err
	}
	defer func() { _ = st.Close() }()

	dbPath := cfg.DatabaseDSN()

	// compute pre-delete stats and totalN before prompting.
	var totalN int64
	if deleteDedupedAllHidden {
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
		stats := make([]batchStat, 0, len(deleteDedupedBatchIDs))
		for _, id := range deleteDedupedBatchIDs {
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
			totalN, len(deleteDedupedBatchIDs))
		for _, s := range stats {
			fmt.Printf("  %s: %d row(s)\n", s.id, s.cnt)
		}
	}

	if totalN == 0 {
		fmt.Println("Nothing to delete.")
		return nil
	}

	if !deleteDedupedYes {
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

	if !deleteDedupedNoBackup {
		backupPath := filepath.Join(
			filepath.Dir(dbPath),
			filepath.Base(dbPath)+".delete-deduped-backup-"+time.Now().Format("20060102-150405"),
		)
		fmt.Printf("Backing up database to %s...\n", filepath.Base(backupPath))
		if err := backupDatabase(st, backupPath); err != nil {
			return fmt.Errorf("backup database: %w", err)
		}
	}

	// Note: vector and parquet caches may contain entries for deleted
	// rows; the user-facing summary recommends 'build-cache --full-rebuild'.

	var deletedTotal int64
	var batchCount int64
	if deleteDedupedAllHidden {
		deleted, distinct, err := st.DeleteAllDeduped()
		if err != nil {
			return fmt.Errorf("delete all dedup-hidden: %w", err)
		}
		deletedTotal = deleted
		batchCount = distinct
	} else {
		batchCount = int64(len(deleteDedupedBatchIDs))
		for _, id := range deleteDedupedBatchIDs {
			deleted, err := st.DeleteDedupedBatch(id)
			if err != nil {
				return fmt.Errorf("delete dedup batch %q: %w", id, err)
			}
			deletedTotal += deleted
		}
	}

	fmt.Printf("\nDeleted %d message(s) from %d batch(es).\n\n", deletedTotal, batchCount)
	fmt.Println("Vector and parquet caches may have stale entries; run")
	fmt.Println("'msgvault build-cache --full-rebuild' to rebuild them.")

	return nil
}

func init() {
	rootCmd.AddCommand(deleteDedupedCmd)
	deleteDedupedCmd.Flags().StringArrayVar(&deleteDedupedBatchIDs, "batch", nil,
		"Delete rows hidden by this batch ID (repeat for multiple batches)")
	deleteDedupedCmd.Flags().BoolVar(&deleteDedupedAllHidden, "all-hidden", false,
		"Delete every dedup-hidden row regardless of batch")
	deleteDedupedCmd.MarkFlagsMutuallyExclusive("batch", "all-hidden")
	deleteDedupedCmd.Flags().BoolVar(&deleteDedupedNoBackup, "no-backup", false,
		"Skip database backup before deleting")
	deleteDedupedCmd.Flags().BoolVarP(&deleteDedupedYes, "yes", "y", false,
		"Skip confirmation prompt")
}
