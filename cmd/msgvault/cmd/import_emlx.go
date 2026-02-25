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
	importEmlxSourceType         string
	importEmlxNoResume           bool
	importEmlxCheckpointInterval int
	importEmlxNoAttachments      bool
)

var importEmlxCmd = &cobra.Command{
	Use:   "import-emlx <identifier> <mail-dir>",
	Short: "Import Apple Mail .emlx files into msgvault",
	Long: `Import Apple Mail .emlx files into msgvault.

The mail directory should be an Apple Mail mailbox tree containing
.mbox or .imapmbox directories, each with a Messages/ subdirectory
of .emlx files. You can also point directly at a single .mbox directory.

Labels are derived from directory names. Messages that appear in
multiple mailboxes are deduplicated and given labels from each.

Examples:
  msgvault import-emlx me@gmail.com ~/Downloads/mail-2009/Mail/
  msgvault import-emlx me@gmail.com ~/Mail/INBOX.mbox/
`,
	Args:         cobra.ExactArgs(2),
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		identifier := args[0]
		mailDir := args[1]

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
						fmt.Fprintln(
							cmd.ErrOrStderr(),
							"\nInterrupted. Saving checkpoint...",
						)
						cancel()
						continue
					}
					fmt.Fprintln(
						cmd.ErrOrStderr(),
						"Interrupted again. Exiting immediately.",
					)
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
		if importEmlxNoAttachments {
			attachmentsDir = ""
		}

		summary, err := importer.ImportEmlxDir(
			ctx, st, mailDir, importer.EmlxImportOptions{
				SourceType:         importEmlxSourceType,
				Identifier:         identifier,
				NoResume:           importEmlxNoResume,
				CheckpointInterval: importEmlxCheckpointInterval,
				AttachmentsDir:     attachmentsDir,
				Logger:             logger,
			},
		)
		if err != nil {
			return err
		}

		out := cmd.OutOrStdout()
		if ctx.Err() != nil {
			fmt.Fprintln(out, "Import interrupted. Run again to resume.")
		} else if summary.Errors > 0 {
			fmt.Fprintln(out, "Import complete (with errors).")
		} else {
			fmt.Fprintln(out, "Import complete.")
		}

		fmt.Fprintf(out,
			"  Mailboxes:      %d discovered, %d imported\n",
			summary.MailboxesTotal, summary.MailboxesImported,
		)
		fmt.Fprintf(out,
			"  Processed:      %d messages\n",
			summary.MessagesProcessed,
		)
		fmt.Fprintf(out,
			"  Added:          %d messages\n",
			summary.MessagesAdded,
		)
		fmt.Fprintf(out,
			"  Updated:        %d messages\n",
			summary.MessagesUpdated,
		)
		fmt.Fprintf(out,
			"  Skipped (dup):  %d messages\n",
			summary.MessagesSkipped,
		)
		fmt.Fprintf(out,
			"  Errors:         %d\n",
			summary.Errors,
		)

		if ctx.Err() == nil && summary.HardErrors {
			return fmt.Errorf(
				"import completed with %d errors",
				summary.Errors,
			)
		}
		if ctx.Err() != nil {
			return context.Canceled
		}
		return nil
	},
}

func init() {
	rootCmd.AddCommand(importEmlxCmd)

	importEmlxCmd.Flags().StringVar(
		&importEmlxSourceType, "source-type", "apple-mail",
		"Source type to record in the database",
	)
	importEmlxCmd.Flags().BoolVar(
		&importEmlxNoResume, "no-resume", false,
		"Do not resume from an interrupted import",
	)
	importEmlxCmd.Flags().IntVar(
		&importEmlxCheckpointInterval, "checkpoint-interval", 200,
		"Save progress every N messages",
	)
	importEmlxCmd.Flags().BoolVar(
		&importEmlxNoAttachments, "no-attachments", false,
		"Do not store attachments on disk",
	)
}
