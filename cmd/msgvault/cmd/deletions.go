package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/wesm/msgvault/internal/deletion"
	"github.com/wesm/msgvault/internal/gmail"
	"github.com/wesm/msgvault/internal/oauth"
	"github.com/wesm/msgvault/internal/store"
)

var listDeletionsCmd = &cobra.Command{
	Use:   "list-deletions",
	Short: "List pending and recent deletion batches",
	Long: `List all deletion batches across all statuses.

Shows pending, in-progress, completed, and failed deletion batches
with their ID, status, message count, and creation date.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		deletionsDir := filepath.Join(cfg.Data.DataDir, "deletions")
		manager, err := deletion.NewManager(deletionsDir)
		if err != nil {
			return fmt.Errorf("create manager: %w", err)
		}

		// List all statuses
		pending, _ := manager.ListPending()
		inProgress, _ := manager.ListInProgress()
		completed, _ := manager.ListCompleted()
		failed, _ := manager.ListFailed()

		if len(pending) == 0 && len(inProgress) == 0 && len(completed) == 0 && len(failed) == 0 {
			fmt.Println("No deletion batches found.")
			fmt.Println("\nTo stage messages for deletion, use the TUI or create a manifest manually.")
			return nil
		}

		printManifestTable := func(status string, manifests []*deletion.Manifest) {
			if len(manifests) == 0 {
				return
			}
			fmt.Printf("\n%s:\n", status)
			fmt.Printf("  %-25s  %-10s  %10s  %s\n", "ID", "Status", "Messages", "Created")
			fmt.Printf("  %-25s  %-10s  %10s  %s\n", "---", "------", "--------", "-------")
			for _, m := range manifests {
				fmt.Printf("  %-25s  %-10s  %10d  %s\n",
					truncate(m.ID, 25),
					m.Status,
					len(m.GmailIDs),
					m.CreatedAt.Format("2006-01-02 15:04"),
				)
			}
		}

		printManifestTable("Pending", pending)
		printManifestTable("In Progress", inProgress)
		printManifestTable("Completed (recent)", limitManifests(completed, 10))
		printManifestTable("Failed", failed)

		return nil
	},
}

var showDeletionCmd = &cobra.Command{
	Use:   "show-deletion <batch-id>",
	Short: "Show details of a deletion batch",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		batchID := args[0]

		deletionsDir := filepath.Join(cfg.Data.DataDir, "deletions")
		manager, err := deletion.NewManager(deletionsDir)
		if err != nil {
			return fmt.Errorf("create manager: %w", err)
		}

		manifest, _, err := manager.GetManifest(batchID)
		if err != nil {
			return fmt.Errorf("get manifest: %w", err)
		}

		fmt.Print(manifest.FormatSummary())
		return nil
	},
}

var cancelAll bool

var cancelDeletionCmd = &cobra.Command{
	Use:   "cancel-deletion [batch-id]",
	Short: "Cancel pending or in-progress deletion batches",
	Long: `Cancel deletion batches by ID, or use --all to cancel all pending and in-progress batches.

Examples:
  msgvault cancel-deletion 20260202-195132-Senders-wingide-user
  msgvault cancel-deletion --all`,
	Args: cobra.MaximumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		if cancelAll && len(args) > 0 {
			return fmt.Errorf("cannot use --all with a batch ID argument")
		}

		deletionsDir := filepath.Join(cfg.Data.DataDir, "deletions")
		manager, err := deletion.NewManager(deletionsDir)
		if err != nil {
			return fmt.Errorf("create manager: %w", err)
		}

		if cancelAll {
			count := 0
			var listErrors []error
			for _, listFn := range []func() ([]*deletion.Manifest, error){
				manager.ListPending, manager.ListInProgress,
			} {
				manifests, err := listFn()
				if err != nil {
					listErrors = append(listErrors, err)
					continue
				}
				for _, m := range manifests {
					if err := manager.CancelManifest(m.ID); err != nil {
						fmt.Printf("  Failed to cancel %s: %v\n", m.ID, err)
					} else {
						fmt.Printf("  Cancelled: %s\n", m.ID)
						count++
					}
				}
			}
			if len(listErrors) > 0 {
				for _, e := range listErrors {
					fmt.Fprintf(os.Stderr, "Warning: failed to list batches: %v\n", e)
				}
			}
			if count == 0 {
				if len(listErrors) > 0 {
					return fmt.Errorf("could not list batches to cancel")
				}
				fmt.Println("No pending or in-progress batches to cancel.")
			} else {
				fmt.Printf("Cancelled %d batch(es).\n", count)
			}
			return nil
		}

		if len(args) == 0 {
			// List available batches to help the user
			fmt.Println("No batch ID specified. Available batches:")
			fmt.Println()
			found := false
			for _, item := range []struct {
				label  string
				listFn func() ([]*deletion.Manifest, error)
			}{
				{"Pending", manager.ListPending},
				{"In Progress", manager.ListInProgress},
			} {
				manifests, err := item.listFn()
				if err != nil || len(manifests) == 0 {
					continue
				}
				for _, m := range manifests {
					fmt.Printf("  [%s] %s (%d messages)\n", item.label, m.ID, len(m.GmailIDs))
					found = true
				}
			}
			if !found {
				fmt.Println("  (none)")
			}
			fmt.Println()
			return fmt.Errorf("provide a batch ID or use --all")
		}

		batchID := args[0]
		if err := manager.CancelManifest(batchID); err != nil {
			return fmt.Errorf("cancel manifest: %w", err)
		}

		fmt.Printf("Cancelled deletion batch: %s\n", batchID)
		return nil
	},
}

var (
	deleteTrash   bool // Use trash instead of permanent delete
	deleteYes     bool
	deleteDryRun  bool
	deleteList    bool
	deleteAccount string
)

var deleteStagedCmd = &cobra.Command{
	Use:   "delete-staged [batch-id]",
	Short: "Execute staged deletions",
	Long: `Execute pending deletion batches.

By default, messages are permanently deleted using batch API (fast, no recovery).
Use --trash to move messages to Gmail trash instead (recoverable for 30 days, slower).

Examples:
  msgvault delete-staged                # Permanent delete all pending (fast)
  msgvault delete-staged batch-123      # Delete specific batch
  msgvault delete-staged --list         # Show staged batches without executing
  msgvault delete-staged --trash        # Move to trash instead (slower)
  msgvault delete-staged --yes          # Skip confirmation`,
	RunE: func(cmd *cobra.Command, args []string) error {
		deletionsDir := filepath.Join(cfg.Data.DataDir, "deletions")
		manager, err := deletion.NewManager(deletionsDir)
		if err != nil {
			return fmt.Errorf("create manager: %w", err)
		}

		// Get manifests to execute
		var manifests []*deletion.Manifest
		if len(args) > 0 {
			manifest, _, err := manager.GetManifest(args[0])
			if err != nil {
				return fmt.Errorf("get manifest: %w", err)
			}
			if manifest.Status != deletion.StatusPending && manifest.Status != deletion.StatusInProgress {
				return fmt.Errorf("batch %s is %s, cannot execute", args[0], manifest.Status)
			}
			manifests = append(manifests, manifest)
		} else {
			pending, err := manager.ListPending()
			if err != nil {
				return fmt.Errorf("list pending: %w", err)
			}
			inProgress, err := manager.ListInProgress()
			if err != nil {
				return fmt.Errorf("list in progress: %w", err)
			}
			manifests = append(manifests, pending...)
			manifests = append(manifests, inProgress...)
		}

		if len(manifests) == 0 {
			fmt.Println("No staged deletions.")
			return nil
		}

		// --list: show staged batches (pending + in-progress) and exit
		if deleteList {
			fmt.Printf("Staged deletions: %d batch(es)\n\n", len(manifests))
			fmt.Printf("  %-25s  %-12s  %10s  %s\n", "ID", "Status", "Messages", "Description")
			fmt.Printf("  %-25s  %-12s  %10s  %s\n", "---", "------", "--------", "-----------")
			totalMessages := 0
			for _, m := range manifests {
				fmt.Printf("  %-25s  %-12s  %10d  %s\n",
					truncate(m.ID, 25),
					m.Status,
					len(m.GmailIDs),
					truncate(m.Description, 40),
				)
				totalMessages += len(m.GmailIDs)
			}
			fmt.Printf("\nTotal: %d messages across %d batch(es)\n", totalMessages, len(manifests))
			fmt.Println("\nUse 'msgvault delete-staged' to execute, or 'msgvault show-deletion <id>' for details.")
			return nil
		}

		// Calculate totals
		totalMessages := 0
		for _, m := range manifests {
			totalMessages += len(m.GmailIDs)
		}

		// Show summary
		method := "PERMANENT DELETE (fast, no recovery)"
		if deleteTrash {
			method = "trash (30-day recovery, slower)"
		}

		fmt.Printf("Deletion Summary:\n")
		fmt.Printf("  Batches:  %d\n", len(manifests))
		fmt.Printf("  Messages: %d\n", totalMessages)
		fmt.Printf("  Method:   %s\n", method)
		fmt.Println()

		// Show batch details
		for _, m := range manifests {
			fmt.Printf("  %s: %d messages - %s\n", m.ID, len(m.GmailIDs), m.Description)
		}
		fmt.Println()

		if deleteDryRun {
			fmt.Println("Dry run - no messages will be deleted.")
			return nil
		}

		// Require confirmation
		if !deleteYes {
			fmt.Print("Proceed with deletion? [y/N]: ")
			var response string
			_, _ = fmt.Scanln(&response)
			if response != "y" && response != "Y" {
				fmt.Println("Cancelled.")
				return nil
			}
		}

		// Validate config
		if cfg.OAuth.ClientSecrets == "" {
			return errOAuthNotConfigured()
		}

		// Collect unique accounts from manifests
		accountSet := make(map[string]bool)
		for _, m := range manifests {
			if m.Filters.Account != "" {
				accountSet[m.Filters.Account] = true
			}
		}

		// Determine which account to use
		account := deleteAccount
		if account == "" {
			accounts := make([]string, 0, len(accountSet))
			for a := range accountSet {
				accounts = append(accounts, a)
			}

			if len(accounts) == 0 {
				return fmt.Errorf("no account in deletion manifest - use --account flag")
			} else if len(accounts) == 1 {
				account = accounts[0]
			} else {
				return fmt.Errorf("multiple accounts in pending batches (%v) - use --account flag to specify which account", accounts)
			}
		} else {
			// Verify all manifests match the specified account
			for _, m := range manifests {
				if m.Filters.Account != "" && m.Filters.Account != account {
					return fmt.Errorf("batch %s is for account %s, not %s - filter batches by account or execute separately", m.ID, m.Filters.Account, account)
				}
			}
		}

		// Open database
		dbPath := cfg.DatabaseDSN()
		s, err := store.Open(dbPath)
		if err != nil {
			return fmt.Errorf("open database: %w", err)
		}
		defer s.Close()

		// Set up context with cancellation
		ctx, cancel := context.WithCancel(cmd.Context())
		defer cancel()

		// Handle Ctrl+C gracefully
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		go func() {
			<-sigChan
			fmt.Println("\nInterrupted. Saving checkpoint...")
			cancel()
		}()

		// Determine which scopes we need
		needsBatchDelete := !deleteTrash
		var requiredScopes []string
		if needsBatchDelete {
			requiredScopes = oauth.ScopesDeletion
		} else {
			requiredScopes = oauth.Scopes
		}

		// Create OAuth manager with appropriate scopes
		oauthMgr, err := oauth.NewManagerWithScopes(cfg.OAuth.ClientSecrets, cfg.TokensDir(), logger, requiredScopes)
		if err != nil {
			return wrapOAuthError(fmt.Errorf("create oauth manager: %w", err))
		}

		// Proactively check if we need scope escalation before making API calls.
		// Legacy tokens (saved before scope tracking) won't have scope metadata,
		// so we only trigger proactive escalation when we positively know the
		// token lacks the required scope.
		if needsBatchDelete && !oauthMgr.HasScope(account, "https://mail.google.com/") {
			// Only trigger proactive escalation when we have scope metadata.
			// Legacy tokens (saved before scope tracking) fall through to
			// reactive detection on the first API call.
			if oauthMgr.HasScopeMetadata(account) {
				// Token has scope metadata but lacks deletion scope — escalate now
				if err := promptScopeEscalation(ctx, oauthMgr, account, needsBatchDelete); err != nil {
					if errors.Is(err, errUserCanceled) {
						return nil
					}
					return err
				}
				// Re-create OAuth manager with new token
				oauthMgr, err = oauth.NewManagerWithScopes(cfg.OAuth.ClientSecrets, cfg.TokensDir(), logger, requiredScopes)
				if err != nil {
					return wrapOAuthError(fmt.Errorf("create oauth manager: %w", err))
				}
			}
			// If no scope metadata at all (legacy token), fall through to reactive detection
		}

		tokenSource, err := oauthMgr.TokenSource(ctx, account)
		if err != nil {
			return fmt.Errorf("get token source: %w", err)
		}

		// Create Gmail client
		rateLimiter := gmail.NewRateLimiter(float64(cfg.Sync.RateLimitQPS))
		client := gmail.NewClient(tokenSource,
			gmail.WithLogger(logger),
			gmail.WithRateLimiter(rateLimiter),
		)
		defer client.Close()

		// Create executor
		executor := deletion.NewExecutor(manager, s, client).
			WithLogger(logger).
			WithProgress(&CLIDeletionProgress{})

		// Execute each manifest
		for i, m := range manifests {
			if i > 0 {
				fmt.Println()
			}
			fmt.Printf("  [%d/%d] %s (%d messages)\n", i+1, len(manifests), m.Description, len(m.GmailIDs))

			var execErr error
			// For in-progress manifests, honor the stored method to avoid
			// accidentally switching from trash to permanent delete mid-batch
			useTrash := deleteTrash
			if m.Status == deletion.StatusInProgress && m.Execution != nil {
				useTrash = (m.Execution.Method == deletion.MethodTrash)
			}

			if useTrash {
				// Use individual trash calls (slower but recoverable)
				opts := deletion.DefaultExecuteOptions()
				opts.Method = deletion.MethodTrash
				execErr = executor.Execute(ctx, m.ID, opts)
			} else {
				// Use batch delete for permanent deletion (fast - 1 API call per 1000 messages)
				execErr = executor.ExecuteBatch(ctx, m.ID)
			}

			if execErr != nil {
				if ctx.Err() != nil {
					fmt.Println("\nInterrupted. Run again to resume.")
					return nil
				}

				// Check if this is a scope error - offer to re-authorize
				if isInsufficientScopeError(execErr) {
					if err := promptScopeEscalation(ctx, oauthMgr, account, !useTrash); err != nil {
						if errors.Is(err, errUserCanceled) {
							return nil
						}
						return err
					}
					fmt.Println("Run delete-staged again to continue.")
					return nil
				}

				logger.Warn("deletion failed", "batch", m.ID, "error", execErr)
				continue
			}
		}

		fmt.Println("\nDeletion complete!")
		return nil
	},
}

// isTTY reports whether stdout is connected to a terminal.
func isTTY() bool {
	fi, err := os.Stdout.Stat()
	if err != nil {
		return false
	}
	return fi.Mode()&os.ModeCharDevice != 0
}

// CLIDeletionProgress reports deletion progress to the terminal.
type CLIDeletionProgress struct {
	total     int
	startTime time.Time
	lastPrint time.Time
	tty       bool
}

func (p *CLIDeletionProgress) OnStart(total int) {
	p.total = total
	p.startTime = time.Now()
	p.lastPrint = time.Time{} // Force first print
	p.tty = isTTY()
	// Show initial progress immediately so it doesn't look like it's hanging
	p.OnProgress(0, 0, 0)
}

func (p *CLIDeletionProgress) formatDuration(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
	return fmt.Sprintf("%dm%02ds", int(d.Minutes()), int(d.Seconds())%60)
}

func (p *CLIDeletionProgress) OnProgress(processed, succeeded, failed int) {
	if p.total <= 0 {
		return
	}
	if time.Since(p.lastPrint) < 500*time.Millisecond {
		return
	}
	p.lastPrint = time.Now()

	pct := float64(processed) / float64(p.total) * 100
	elapsed := time.Since(p.startTime)

	bar := p.progressBar(pct, 30)

	var eta string
	if processed > 0 && processed < p.total {
		remaining := time.Duration(float64(elapsed) / float64(processed) * float64(p.total-processed))
		eta = p.formatDuration(remaining) + " remaining"
	} else if processed >= p.total {
		eta = p.formatDuration(elapsed) + " elapsed"
	} else {
		eta = "calculating..."
	}

	status := fmt.Sprintf("  %s %.1f%%  %d/%d", bar, pct, processed, p.total)
	if failed > 0 {
		status += fmt.Sprintf("  (%d failed)", failed)
	}
	status += fmt.Sprintf("  %s", eta)
	if p.tty {
		fmt.Printf("\r\033[K%s", status)
	} else {
		fmt.Println(status)
	}
}

func (p *CLIDeletionProgress) progressBar(pct float64, width int) string {
	filled := int(pct / 100 * float64(width))
	if filled > width {
		filled = width
	}
	bar := make([]byte, width)
	for i := range bar {
		if i < filled {
			bar[i] = '#'
		} else {
			bar[i] = '-'
		}
	}
	return "[" + string(bar) + "]"
}

func (p *CLIDeletionProgress) OnComplete(succeeded, failed int) {
	elapsed := time.Since(p.startTime)
	// Clear the progress line
	if p.tty {
		fmt.Print("\r\033[K")
	}
	if failed == 0 {
		fmt.Printf("  Done: %d deleted in %s\n", succeeded, p.formatDuration(elapsed))
	} else {
		fmt.Printf("  Done: %d deleted, %d failed in %s\n", succeeded, failed, p.formatDuration(elapsed))
	}
}

// Helper functions
func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max-3] + "..."
}

func limitManifests(manifests []*deletion.Manifest, max int) []*deletion.Manifest {
	if len(manifests) <= max {
		return manifests
	}
	return manifests[:max]
}

// errUserCanceled is returned when the user declines scope escalation.
var errUserCanceled = errors.New("user canceled scope escalation")

// promptScopeEscalation prompts the user to re-authorize with elevated scopes.
// It deletes the old token, runs the OAuth browser flow, and returns nil on
// success. The caller should re-create the OAuth manager after this returns.
func promptScopeEscalation(ctx context.Context, oauthMgr *oauth.Manager, account string, batchDelete bool) error {
	fmt.Println("\n" + strings.Repeat("=", 70))
	fmt.Println("PERMISSION UPGRADE REQUIRED")
	fmt.Println(strings.Repeat("=", 70))
	fmt.Println()

	var requiredScopes []string
	if !batchDelete {
		fmt.Println("Trash deletion requires Gmail modify permissions.")
		fmt.Println()
		fmt.Println("Your current OAuth token doesn't include the gmail.modify scope.")
		fmt.Println("To proceed, msgvault needs to re-authorize with modify access.")
		requiredScopes = oauth.Scopes
	} else {
		fmt.Println("Batch deletion requires elevated Gmail permissions.")
		fmt.Println()
		fmt.Println("Your current OAuth token was granted with limited permissions that")
		fmt.Println("don't include batch delete. To proceed, msgvault needs to:")
		fmt.Println()
		fmt.Println("  1. Delete your existing OAuth token")
		fmt.Println("  2. Re-authorize with full Gmail access (mail.google.com scope)")
		fmt.Println()
		fmt.Println("This elevated permission allows msgvault to permanently delete")
		fmt.Println("messages in bulk. You can revoke access anytime at:")
		fmt.Println("  https://myaccount.google.com/permissions")
		requiredScopes = oauth.ScopesDeletion
	}
	fmt.Println()

	fmt.Print("Upgrade permissions now? [y/N]: ")
	var response string
	_, _ = fmt.Scanln(&response)
	if response != "y" && response != "Y" {
		if batchDelete {
			fmt.Println("Cancelled. Use --trash for slower deletion without elevated permissions.")
		} else {
			fmt.Println("Cancelled.")
		}
		return errUserCanceled
	}

	// Delete old token and re-authorize
	fmt.Println("\nDeleting old token...")
	if err := oauthMgr.DeleteToken(account); err != nil {
		return fmt.Errorf("delete token: %w", err)
	}

	fmt.Println("Starting OAuth flow...")
	fmt.Println()

	newMgr, err := oauth.NewManagerWithScopes(cfg.OAuth.ClientSecrets, cfg.TokensDir(), logger, requiredScopes)
	if err != nil {
		return fmt.Errorf("create oauth manager: %w", err)
	}

	if err := newMgr.Authorize(ctx, account); err != nil {
		return fmt.Errorf("authorize: %w", err)
	}

	fmt.Println("\nAuthorization successful!")
	return nil
}

// isInsufficientScopeError checks if an error is due to missing OAuth scopes.
func isInsufficientScopeError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "ACCESS_TOKEN_SCOPE_INSUFFICIENT") ||
		strings.Contains(msg, "insufficient authentication scopes") ||
		strings.Contains(msg, "Insufficient Permission")
}

func init() {
	deleteStagedCmd.Flags().BoolVar(&deleteTrash, "trash", false, "Move to trash instead of permanent delete (slower)")
	deleteStagedCmd.Flags().BoolVarP(&deleteYes, "yes", "y", false, "Skip confirmation")
	deleteStagedCmd.Flags().BoolVar(&deleteDryRun, "dry-run", false, "Show what would be deleted")
	deleteStagedCmd.Flags().BoolVarP(&deleteList, "list", "l", false, "List staged batches without executing")
	deleteStagedCmd.Flags().StringVar(&deleteAccount, "account", "", "Gmail account to use")

	rootCmd.AddCommand(listDeletionsCmd)
	rootCmd.AddCommand(showDeletionCmd)
	cancelDeletionCmd.Flags().BoolVar(&cancelAll, "all", false, "Cancel all pending and in-progress batches")
	rootCmd.AddCommand(cancelDeletionCmd)
	rootCmd.AddCommand(deleteStagedCmd)
}
