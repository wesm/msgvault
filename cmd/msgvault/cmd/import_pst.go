package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/wesm/msgvault/internal/importer"
	"github.com/wesm/msgvault/internal/store"
)

var (
	importPstSourceType         string
	importPstSkipFolders        []string
	importPstNoResume           bool
	importPstCheckpointInterval int
	importPstNoAttachments      bool
)

var importPstCmd = &cobra.Command{
	Use:   "import-pst <identifier> <pst-file>",
	Short: "Import a PST (Outlook) archive into msgvault",
	Long: `Import a Microsoft Outlook PST file into msgvault.

All email messages are imported. Calendar items, contacts, tasks, and notes
are skipped automatically. The PST folder structure is preserved as labels
(e.g. the Inbox folder becomes the "Inbox" label).

The import is resumable: if interrupted with Ctrl+C, rerunning with the same
arguments will continue from where it left off. Use --no-resume to start fresh.

Examples:
  msgvault init-db
  msgvault import-pst you@company.com /path/to/archive.pst
  msgvault import-pst you@outlook.com backup.pst --skip-folder "Deleted Items"
  msgvault import-pst you@outlook.com backup.pst --no-resume
`,
	Args:         cobra.ExactArgs(2),
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		identifier := args[0]
		pstPath := args[1]

		// Graceful Ctrl+C: first signal saves checkpoint, second exits immediately.
		ctx, cancel := context.WithCancel(cmd.Context())
		defer cancel()

		sigChan := make(chan os.Signal, 2)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		done := make(chan struct{})
		defer func() {
			close(done)
			signal.Stop(sigChan)
			for {
				select {
				case <-sigChan:
				default:
					return
				}
			}
		}()

		go func() {
			signals := 0
			for {
				select {
				case <-done:
					return
				case <-sigChan:
					select {
					case <-done:
						return
					default:
					}
					signals++
					if signals == 1 {
						fmt.Fprintln(cmd.ErrOrStderr(), "\nInterrupted. Saving checkpoint...")
						cancel()
						continue
					}
					// NOTE: os.Exit bypasses all deferred cleanup (db.Close,
					// pstFile.Close, etc.). This is deliberate: the first
					// Ctrl+C already triggered graceful shutdown with checkpoint
					// saving via context cancellation. SQLite WAL journaling
					// ensures database consistency even on hard exit.
					fmt.Fprintln(cmd.ErrOrStderr(), "Interrupted again. Exiting immediately.")
					os.Exit(130)
				}
			}
		}()

		dbPath := cfg.DatabaseDSN()
		st, err := store.Open(dbPath)
		if err != nil {
			return fmt.Errorf("open database: %w", err)
		}
		defer st.Close()

		if err := st.InitSchema(); err != nil {
			return fmt.Errorf("init schema: %w", err)
		}

		attachmentsDir := cfg.AttachmentsDir()
		if importPstNoAttachments {
			attachmentsDir = ""
		}

		summary, err := importer.ImportPst(ctx, st, pstPath, importer.PstImportOptions{
			SourceType:         importPstSourceType,
			Identifier:         identifier,
			SkipFolders:        importPstSkipFolders,
			NoResume:           importPstNoResume,
			CheckpointInterval: importPstCheckpointInterval,
			AttachmentsDir:     attachmentsDir,
			Logger:             logger,
		})
		if err != nil {
			return err
		}

		out := cmd.OutOrStdout()
		switch {
		case ctx.Err() != nil:
			fmt.Fprintln(out, "Import interrupted. Run again to resume.")
		case summary.HardErrors:
			fmt.Fprintln(out, "Import complete (with errors).")
		default:
			fmt.Fprintln(out, "Import complete.")
		}

		if summary.WasResumed {
			fmt.Fprintln(out, "  Resumed from checkpoint.")
		}
		fmt.Fprintf(out, "  File:           %s\n", pstPath)
		fmt.Fprintf(out, "  Folders:        %d / %d\n", summary.FoldersImported, summary.FoldersTotal)
		fmt.Fprintf(out, "  Processed:      %d messages\n", summary.MessagesProcessed)
		fmt.Fprintf(out, "  Added:          %d messages\n", summary.MessagesAdded)
		fmt.Fprintf(out, "  Updated:        %d messages\n", summary.MessagesUpdated)
		fmt.Fprintf(out, "  Skipped:        %d messages\n", summary.MessagesSkipped)
		fmt.Fprintf(out, "  Errors:         %d\n", summary.Errors)
		fmt.Fprintf(out, "  Duration:       %s\n", summary.Duration.Round(1e9))

		if ctx.Err() != nil {
			return context.Canceled
		}
		if summary.HardErrors {
			return fmt.Errorf("import completed with %d errors", summary.Errors)
		}
		return nil
	},
}

func init() {
	rootCmd.AddCommand(importPstCmd)

	importPstCmd.Flags().StringVar(&importPstSourceType, "source-type", "pst", "Source type recorded in the database")
	importPstCmd.Flags().StringArrayVar(&importPstSkipFolders, "skip-folder", nil, "Folder name to skip (repeatable, case-insensitive)")
	importPstCmd.Flags().BoolVar(&importPstNoResume, "no-resume", false, "Do not resume from an interrupted import")
	importPstCmd.Flags().IntVar(&importPstCheckpointInterval, "checkpoint-interval", 200, "Save progress every N messages")
	importPstCmd.Flags().BoolVar(&importPstNoAttachments, "no-attachments", false, "Do not store attachments to disk (messages are still imported)")
}
