package cmd

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
	imaplib "github.com/wesm/msgvault/internal/imap"
	"github.com/wesm/msgvault/internal/microsoft"
	"github.com/wesm/msgvault/internal/oauth"
	"github.com/wesm/msgvault/internal/store"
)

func newRemoveAccountCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "remove-account <email>",
		Short: "Remove an account and all its data",
		Long: `Remove an account and all associated messages, labels, and sync data
from the local database. This is irreversible.

If the same identifier exists for multiple source types (e.g., gmail
and mbox), use --type to specify which one to remove.

The Parquet analytics cache is deleted because it is shared across accounts
and must be rebuilt. Run 'msgvault build-cache' afterward to rebuild it.

Attachment files on disk that are not shared with another account are deleted.
Shared attachments (same content hash across multiple accounts) are kept.

Examples:
  msgvault remove-account you@gmail.com
  msgvault remove-account you@gmail.com --yes
  msgvault remove-account you@gmail.com --type mbox`,
		Args: cobra.ExactArgs(1),
		RunE: runRemoveAccount,
	}
	cmd.Flags().BoolP("yes", "y", false, "Skip confirmation prompt")
	cmd.Flags().String(
		"type", "",
		"Source type to remove (gmail, mbox, etc.)",
	)
	return cmd
}

func runRemoveAccount(cmd *cobra.Command, args []string) error {
	if err := MustBeLocal("remove-account"); err != nil {
		return err
	}

	yes, err := cmd.Flags().GetBool("yes")
	if err != nil {
		return fmt.Errorf("read --yes flag: %w", err)
	}
	sourceType, err := cmd.Flags().GetString("type")
	if err != nil {
		return fmt.Errorf("read --type flag: %w", err)
	}

	email := args[0]

	dbPath := cfg.DatabaseDSN()
	s, err := store.Open(dbPath)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer func() { _ = s.Close() }()

	if err := s.InitSchema(); err != nil {
		return fmt.Errorf("init schema: %w", err)
	}

	source, err := resolveSource(s, email, sourceType)
	if err != nil {
		return err
	}

	activeSync, err := s.GetActiveSync(source.ID)
	if err != nil {
		return fmt.Errorf("check active sync: %w", err)
	}
	if activeSync != nil && !yes {
		return fmt.Errorf(
			"account %s has an active sync in progress\n"+
				"Use --yes to force removal", email,
		)
	}

	msgCount, err := s.CountMessagesForSource(source.ID)
	if err != nil {
		return fmt.Errorf("count messages: %w", err)
	}

	fmt.Printf("Account:  %s\n", email)
	fmt.Printf("Type:     %s\n", source.SourceType)
	fmt.Printf("Messages: %s\n", formatCount(msgCount))

	if !yes {
		fmt.Print("\nRemove this account and all its data? [y/N] ")
		scanner := bufio.NewScanner(os.Stdin)
		if !scanner.Scan() {
			if err := scanner.Err(); err != nil {
				return fmt.Errorf("read confirmation: %w", err)
			}
			return fmt.Errorf(
				"no confirmation input (stdin closed); use --yes",
			)
		}
		answer := strings.TrimSpace(strings.ToLower(scanner.Text()))
		if answer != "y" && answer != "yes" {
			fmt.Println("Aborted.")
			return nil
		}
	}

	// Collect attachment paths unique to this source before the cascade deletes them.
	attachmentPaths, err := s.AttachmentPathsUniqueToSource(source.ID)
	if err != nil {
		return fmt.Errorf("collect attachment paths: %w", err)
	}

	if err := s.RemoveSource(source.ID); err != nil {
		return fmt.Errorf("remove account: %w", err)
	}

	// Delete attachment files that are no longer referenced by any source.
	// This runs under an exclusive DB lock to prevent races with concurrent
	// syncs: the lock blocks StartSync() (a write), so no new sync can start
	// and place a file on disk without a corresponding DB row while we're
	// checking references and deleting files.
	attachmentsDir := cfg.AttachmentsDir()
	var deletedFiles int
	if len(attachmentPaths) > 0 {
		cleanDir, err := filepath.Abs(attachmentsDir)
		if err != nil {
			fmt.Fprintf(os.Stderr,
				"Warning: could not resolve attachments dir; "+
					"skipping file deletion: %v\n"+
					"Orphaned files may remain in %s\n",
				err, attachmentsDir,
			)
		} else {
			lockErr := s.WithExclusiveLock(func() error {
				anySyncRunning, err := s.HasAnyActiveSync()
				if err != nil {
					fmt.Fprintf(os.Stderr,
						"Warning: could not check for active syncs: %v; "+
							"attachment files were not deleted.\n"+
							"Orphaned files may remain in %s\n",
						err, attachmentsDir,
					)
					return nil
				}
				if anySyncRunning {
					fmt.Fprintf(os.Stderr,
						"Warning: a sync is in progress; "+
							"attachment files were not deleted.\n"+
							"Orphaned files may remain in %s\n",
						attachmentsDir,
					)
					return nil
				}

				var failedFiles int
				for _, relPath := range attachmentPaths {
					absPath := filepath.Join(cleanDir, relPath)

					rel, err := filepath.Rel(cleanDir, absPath)
					if err != nil ||
						rel == ".." ||
						strings.HasPrefix(
							rel,
							".."+string(filepath.Separator),
						) {
						fmt.Fprintf(os.Stderr,
							"Warning: attachment path %q escapes "+
								"attachments directory, skipping\n",
							relPath,
						)
						failedFiles++
						continue
					}

					referenced, err := s.IsAttachmentPathReferenced(
						relPath,
					)
					if err != nil {
						fmt.Fprintf(os.Stderr,
							"Warning: could not verify attachment "+
								"%s is unreferenced: %v\n",
							relPath, err,
						)
						failedFiles++
						continue
					}
					if referenced {
						continue
					}
					if err := os.Remove(absPath); err != nil &&
						!os.IsNotExist(err) {
						failedFiles++
					} else {
						deletedFiles++
					}
				}
				if failedFiles > 0 {
					fmt.Fprintf(os.Stderr,
						"Warning: could not remove %d attachment "+
							"file(s) from disk.\n",
						failedFiles,
					)
				}
				return nil
			})
			if lockErr != nil {
				fmt.Fprintf(os.Stderr,
					"Warning: could not acquire exclusive lock; "+
						"skipping file deletion: %v\n"+
						"Orphaned files may remain in %s\n",
					lockErr, attachmentsDir,
				)
			}
		}
	}

	// Remove credentials for the source type.
	switch source.SourceType {
	case "gmail":
		tokenPath := oauth.TokenFilePath(
			cfg.TokensDir(), source.Identifier,
		)
		if err := os.Remove(tokenPath); err != nil && !os.IsNotExist(err) {
			fmt.Fprintf(os.Stderr,
				"Warning: could not remove token file %s: %v\n",
				tokenPath, err,
			)
		}
	case "imap":
		if source.SyncConfig.Valid && source.SyncConfig.String != "" {
			imapCfg, parseErr := imaplib.ConfigFromJSON(source.SyncConfig.String)
			if parseErr == nil {
				switch imapCfg.EffectiveAuthMethod() {
				case imaplib.AuthXOAuth2:
					msMgr := microsoft.NewManager(
						cfg.Microsoft.ClientID,
						cfg.Microsoft.EffectiveTenantID(),
						cfg.TokensDir(),
						logger,
					)
					if err := msMgr.DeleteToken(imapCfg.Username); err != nil {
						fmt.Fprintf(os.Stderr,
							"Warning: could not remove Microsoft token: %v\n", err,
						)
					}
				default:
					credPath := imaplib.CredentialsPath(
						cfg.TokensDir(), source.Identifier,
					)
					if err := os.Remove(credPath); err != nil && !os.IsNotExist(err) {
						fmt.Fprintf(os.Stderr,
							"Warning: could not remove credentials file %s: %v\n",
							credPath, err,
						)
					}
				}
			}
		} else {
			// No sync_config — try removing credential file as fallback.
			credPath := imaplib.CredentialsPath(
				cfg.TokensDir(), source.Identifier,
			)
			if err := os.Remove(credPath); err != nil && !os.IsNotExist(err) {
				fmt.Fprintf(os.Stderr,
					"Warning: could not remove credentials file %s: %v\n",
					credPath, err,
				)
			}
		}
	}

	// Remove analytics cache (shared across accounts, needs full rebuild)
	analyticsDir := cfg.AnalyticsDir()
	if err := os.RemoveAll(analyticsDir); err != nil {
		fmt.Fprintf(os.Stderr,
			"Warning: could not remove analytics cache %s: %v\n",
			analyticsDir, err,
		)
	}

	fmt.Printf("\nAccount %s removed.\n", email)
	if deletedFiles > 0 {
		fmt.Printf("Deleted %d attachment file(s) from disk.\n", deletedFiles)
	}
	fmt.Println(
		"Run 'msgvault build-cache' to rebuild the analytics cache.",
	)

	return nil
}

// resolveSource finds the unique source for the given identifier.
// If multiple source types share the identifier, sourceType is
// required to disambiguate.
func resolveSource(
	s *store.Store, identifier, sourceType string,
) (*store.Source, error) {
	sources, err := s.GetSourcesByIdentifierOrDisplayName(identifier)
	if err != nil {
		return nil, fmt.Errorf("look up account: %w", err)
	}
	if len(sources) == 0 {
		return nil, fmt.Errorf("account %q not found", identifier)
	}

	if sourceType != "" {
		for _, src := range sources {
			if src.SourceType == sourceType {
				return src, nil
			}
		}
		return nil, fmt.Errorf(
			"account %q with type %q not found",
			identifier, sourceType,
		)
	}

	if len(sources) == 1 {
		return sources[0], nil
	}

	// Multiple matches — require --type to disambiguate
	var types []string
	for _, src := range sources {
		types = append(types, src.SourceType)
	}
	return nil, fmt.Errorf(
		"multiple accounts found for %q (types: %s)\n"+
			"Use --type to specify which one to remove",
		identifier, strings.Join(types, ", "),
	)
}

func init() {
	rootCmd.AddCommand(newRemoveAccountCmd())
}
