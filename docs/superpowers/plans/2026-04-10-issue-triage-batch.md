# Issue Triage Batch Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix 8 open issues (#198, #197, #178, #189, #190, #113, #174, #173) in a single branch.

**Architecture:** Each task is an independent fix touching isolated files. Tasks 3-4 share the mbox import pipeline and must be done in order. Task 6 (#174) adds a new store method for structured search queries. All others are self-contained.

**Tech Stack:** Go, Cobra CLI, go-imap v2, SQLite FTS5, Docker

**Spec:** `docs/superpowers/specs/2026-04-10-issue-triage-batch-design.md`

---

### Task 1: IMAP — classify standard folders as system labels (#198)

**Files:**
- Modify: `internal/imap/client.go:583-608`
- Create: `internal/imap/labels.go`
- Create: `internal/imap/labels_test.go`

- [ ] **Step 1: Create labels.go with classifyLabelType function and test file**

Create `internal/imap/labels.go`:

```go
package imap

import (
	"strings"

	imaplib "github.com/emersion/go-imap/v2"
)

// specialUseAttrs maps RFC 6154 special-use attributes to "system".
var specialUseAttrs = map[imaplib.MailboxAttr]bool{
	imaplib.MailboxAttrSent:    true,
	imaplib.MailboxAttrDrafts:  true,
	imaplib.MailboxAttrTrash:   true,
	imaplib.MailboxAttrJunk:    true,
	imaplib.MailboxAttrAll:     true,
	imaplib.MailboxAttrArchive: true,
	imaplib.MailboxAttrFlagged: true,
}

// systemFolderNames maps common folder names (lowercase) to "system".
var systemFolderNames = map[string]bool{
	"inbox":            true,
	"sent":             true,
	"sent items":       true,
	"sent messages":    true,
	"drafts":           true,
	"draft":            true,
	"trash":            true,
	"deleted items":    true,
	"deleted messages": true,
	"junk":             true,
	"bulk mail":        true,
	"spam":             true,
	"archive":          true,
	"all mail":         true,
	"[gmail]/all mail": true,
}

// classifyLabelType returns "system" if the mailbox is a standard
// folder (by RFC 6154 attribute or common name), "user" otherwise.
func classifyLabelType(
	mailbox string, attrs []imaplib.MailboxAttr,
) string {
	for _, a := range attrs {
		if specialUseAttrs[a] {
			return "system"
		}
	}
	if systemFolderNames[strings.ToLower(mailbox)] {
		return "system"
	}
	return "user"
}
```

Create `internal/imap/labels_test.go`:

```go
package imap

import (
	"testing"

	imaplib "github.com/emersion/go-imap/v2"
)

func TestClassifyLabelType(t *testing.T) {
	tests := []struct {
		name    string
		mailbox string
		attrs   []imaplib.MailboxAttr
		want    string
	}{
		{"INBOX by name", "INBOX", nil, "system"},
		{"inbox lowercase", "inbox", nil, "system"},
		{"Sent by attr", "Outgoing", []imaplib.MailboxAttr{imaplib.MailboxAttrSent}, "system"},
		{"Sent by name", "Sent", nil, "system"},
		{"Sent Items by name", "Sent Items", nil, "system"},
		{"Drafts by attr", "MyDrafts", []imaplib.MailboxAttr{imaplib.MailboxAttrDrafts}, "system"},
		{"Drafts by name", "Drafts", nil, "system"},
		{"Draft by name", "Draft", nil, "system"},
		{"Trash by attr", "Bin", []imaplib.MailboxAttr{imaplib.MailboxAttrTrash}, "system"},
		{"Trash by name", "Trash", nil, "system"},
		{"Deleted Items by name", "Deleted Items", nil, "system"},
		{"Junk by attr", "Filtered", []imaplib.MailboxAttr{imaplib.MailboxAttrJunk}, "system"},
		{"Junk by name", "Junk", nil, "system"},
		{"Bulk Mail by name", "Bulk Mail", nil, "system"},
		{"Spam by name", "Spam", nil, "system"},
		{"Archive by attr", "Old", []imaplib.MailboxAttr{imaplib.MailboxAttrArchive}, "system"},
		{"Archive by name", "Archive", nil, "system"},
		{"All Mail by attr", "Everything", []imaplib.MailboxAttr{imaplib.MailboxAttrAll}, "system"},
		{"All Mail by name", "All Mail", nil, "system"},
		{"Gmail All Mail", "[Gmail]/All Mail", nil, "system"},
		{"Flagged by attr", "Stars", []imaplib.MailboxAttr{imaplib.MailboxAttrFlagged}, "system"},
		{"custom folder", "Receipts", nil, "user"},
		{"custom with NoSelect", "Parent", []imaplib.MailboxAttr{imaplib.MailboxAttrNoSelect}, "user"},
		{"attr takes priority", "RandomName", []imaplib.MailboxAttr{imaplib.MailboxAttrSent}, "system"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := classifyLabelType(tt.mailbox, tt.attrs)
			if got != tt.want {
				t.Errorf("classifyLabelType(%q, %v) = %q, want %q",
					tt.mailbox, tt.attrs, got, tt.want)
			}
		})
	}
}
```

- [ ] **Step 2: Run test to verify it passes**

Run: `go test ./internal/imap/ -run TestClassifyLabelType -v`
Expected: PASS

- [ ] **Step 3: Wire classifyLabelType into ListLabels**

In `internal/imap/client.go`, replace the inline label type logic in `ListLabels()`:

Replace:
```go
		for _, item := range items {
			labelType := "user"
			if item.Mailbox == "INBOX" {
				labelType = "system"
			}
```

With:
```go
		for _, item := range items {
			labelType := classifyLabelType(item.Mailbox, item.Attrs)
```

- [ ] **Step 4: Run tests and format**

Run: `go test ./internal/imap/... -v && go fmt ./internal/imap/ && go vet ./internal/imap/`

- [ ] **Step 5: Commit**

```bash
git add internal/imap/labels.go internal/imap/labels_test.go internal/imap/client.go
git commit -m "Classify standard IMAP folders as system labels (#198)

Use RFC 6154 special-use attributes with fallback to common folder
name matching. Covers Sent, Drafts, Trash, Junk, Archive, All Mail."
```

---

### Task 2: IMAP — support password via environment variable (#197)

**Files:**
- Modify: `cmd/msgvault/cmd/addimap.go:101-124`

- [ ] **Step 1: Add env var check before password strategy selection**

In `cmd/msgvault/cmd/addimap.go`, after the `imapCfg` construction (after line 100) and before the `prompt` line (line 102), add the env var check. Replace the password-reading block:

Replace:
```go
		prompt := fmt.Sprintf("Password for %s@%s:", imapUsername, imapHost)
		method, promptOut := choosePasswordStrategy(
			isatty.IsTerminal(os.Stdin.Fd()),
			isatty.IsCygwinTerminal(os.Stdin.Fd()),
			isatty.IsTerminal(os.Stderr.Fd()) || isatty.IsCygwinTerminal(os.Stderr.Fd()),
			isatty.IsTerminal(os.Stdout.Fd()) || isatty.IsCygwinTerminal(os.Stdout.Fd()),
		)

		var (
			password string
			err      error
		)
		switch method {
		case passwordInteractive:
			password, err = readPasswordInteractive(prompt, promptOut)
		case passwordNoPrompt:
			return fmt.Errorf("cannot read password: no terminal available for prompt (try piping the password via stdin)")
		case passwordPipe:
			password, err = readPasswordFromPipe(os.Stdin)
		}
		if err != nil {
			return err
		}
```

With:
```go
		var (
			password string
			err      error
		)
		if envPass := os.Getenv("MSGVAULT_IMAP_PASSWORD"); envPass != "" {
			password = envPass
			fmt.Fprintln(os.Stderr, "Using password from MSGVAULT_IMAP_PASSWORD environment variable")
		} else {
			prompt := fmt.Sprintf("Password for %s@%s:", imapUsername, imapHost)
			method, promptOut := choosePasswordStrategy(
				isatty.IsTerminal(os.Stdin.Fd()),
				isatty.IsCygwinTerminal(os.Stdin.Fd()),
				isatty.IsTerminal(os.Stderr.Fd()) || isatty.IsCygwinTerminal(os.Stderr.Fd()),
				isatty.IsTerminal(os.Stdout.Fd()) || isatty.IsCygwinTerminal(os.Stdout.Fd()),
			)
			switch method {
			case passwordInteractive:
				password, err = readPasswordInteractive(prompt, promptOut)
			case passwordNoPrompt:
				return fmt.Errorf("cannot read password: no terminal available for prompt (try piping the password via stdin or setting MSGVAULT_IMAP_PASSWORD)")
			case passwordPipe:
				password, err = readPasswordFromPipe(os.Stdin)
			}
			if err != nil {
				return err
			}
		}
```

- [ ] **Step 2: Update command help text to mention env var**

In the `Long` description of `addIMAPCmd`, add the env var option. Replace:
```go
	Long: `Add an IMAP email account using username/password authentication.

By default, connects using implicit TLS (IMAPS, port 993).
Use --starttls for STARTTLS upgrade on port 143.
Use --no-tls for a plain unencrypted connection (not recommended).

You will be prompted to enter your password interactively.
For scripting, pipe the password via stdin to avoid exposing it in
shell history or process listings:
  read -s PASS && echo "$PASS" | msgvault add-imap --host ... --username ...
```

With:
```go
	Long: `Add an IMAP email account using username/password authentication.

By default, connects using implicit TLS (IMAPS, port 993).
Use --starttls for STARTTLS upgrade on port 143.
Use --no-tls for a plain unencrypted connection (not recommended).

You will be prompted to enter your password interactively.
For scripting, pipe the password via stdin or set the environment variable:
  read -s PASS && echo "$PASS" | msgvault add-imap --host ... --username ...
  MSGVAULT_IMAP_PASSWORD="..." msgvault add-imap --host ... --username ...
```

- [ ] **Step 3: Run build and format**

Run: `go build ./cmd/msgvault/ && go fmt ./cmd/msgvault/cmd/ && go vet ./cmd/msgvault/cmd/`

- [ ] **Step 4: Commit**

```bash
git add cmd/msgvault/cmd/addimap.go
git commit -m "Support IMAP password via MSGVAULT_IMAP_PASSWORD env var (#197)

Enables headless/CI/Docker usage. Env var is checked before
interactive prompt. Security warning printed to stderr when used."
```

---

### Task 3: Stop enforcing .mbox extension on import files (#178)

**Files:**
- Modify: `internal/importer/mboxzip/mbox_zip.go:57-105`
- Create: `internal/importer/mboxzip/mbox_zip_test.go`

- [ ] **Step 1: Write test for extensionless bare file**

Create `internal/importer/mboxzip/mbox_zip_test.go`:

```go
package mboxzip

import (
	"log/slog"
	"os"
	"path/filepath"
	"testing"
)

func TestResolveMboxExport_NoExtension(t *testing.T) {
	tmp := t.TempDir()

	// Write a valid mbox file with no extension.
	content := "From sender@example.com Mon Jan 1 12:00:00 2024\n" +
		"From: sender@example.com\n" +
		"Subject: Test\n\n" +
		"Body.\n"
	noExtPath := filepath.Join(tmp, "mailarchive")
	if err := os.WriteFile(noExtPath, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}

	files, err := ResolveMboxExport(noExtPath, tmp, slog.Default())
	if err != nil {
		t.Fatalf("ResolveMboxExport() with no extension: %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("expected 1 file, got %d", len(files))
	}
}

func TestResolveMboxExport_NonStandardExtension(t *testing.T) {
	tmp := t.TempDir()

	content := "From sender@example.com Mon Jan 1 12:00:00 2024\n" +
		"From: sender@example.com\n" +
		"Subject: Test\n\n" +
		"Body.\n"
	mailPath := filepath.Join(tmp, "archive.mail")
	if err := os.WriteFile(mailPath, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}

	files, err := ResolveMboxExport(mailPath, tmp, slog.Default())
	if err != nil {
		t.Fatalf("ResolveMboxExport() with .mail extension: %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("expected 1 file, got %d", len(files))
	}
}

func TestResolveMboxExport_StandardExtensionsStillWork(t *testing.T) {
	tmp := t.TempDir()

	content := "From sender@example.com Mon Jan 1 12:00:00 2024\n" +
		"From: sender@example.com\n" +
		"Subject: Test\n\n" +
		"Body.\n"

	for _, ext := range []string{".mbox", ".mbx"} {
		path := filepath.Join(tmp, "archive"+ext)
		if err := os.WriteFile(path, []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
		files, err := ResolveMboxExport(path, tmp, slog.Default())
		if err != nil {
			t.Fatalf("ResolveMboxExport() with %s: %v", ext, err)
		}
		if len(files) != 1 {
			t.Fatalf("expected 1 file for %s, got %d", ext, len(files))
		}
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/importer/mboxzip/ -run TestResolveMboxExport_NoExtension -v`
Expected: FAIL with `unsupported export format`

- [ ] **Step 3: Modify ResolveMboxExport to accept any non-zip file**

In `internal/importer/mboxzip/mbox_zip.go`, replace the extension switch:

Replace:
```go
	ext := strings.ToLower(filepath.Ext(abs))
	switch ext {
	case ".mbox", ".mbx":
		return []string{abs}, nil
	case ".zip":
```

With:
```go
	ext := strings.ToLower(filepath.Ext(abs))
	if ext == ".zip" {
```

And replace the closing of the switch:

Replace:
```go
		return ExtractMboxFromZip(abs, destDir, log)
	default:
		return nil, fmt.Errorf("unsupported export format %q (expected .mbox/.mbx or .zip)", ext)
	}
```

With:
```go
		return ExtractMboxFromZip(abs, destDir, log)
	}
	// Any non-zip file is treated as mbox. Invalid files fail at parse
	// time when no "From " separators are found.
	return []string{abs}, nil
```

- [ ] **Step 4: Run tests**

Run: `go test ./internal/importer/mboxzip/ -v && go fmt ./internal/importer/mboxzip/ && go vet ./internal/importer/mboxzip/`
Expected: All PASS

- [ ] **Step 5: Update import command help text**

In `cmd/msgvault/cmd/import_mbox.go`, update the Long description. Replace:
```go
The export file may be a plain .mbox/.mbx file or a .zip containing one or
more .mbox files.
```

With:
```go
The export file may be a plain mbox file (any extension) or a .zip containing
one or more .mbox files.
```

- [ ] **Step 6: Commit**

```bash
git add internal/importer/mboxzip/mbox_zip.go internal/importer/mboxzip/mbox_zip_test.go cmd/msgvault/cmd/import_mbox.go
git commit -m "Stop enforcing .mbox/.mbx extension on import files (#178)

Treat any non-zip file as mbox. Invalid files fail at parse time
when no From separators are found. Zip entry filtering unchanged."
```

---

### Task 4: import-mbox — support multiple --label flags (#189)

**Files:**
- Modify: `cmd/msgvault/cmd/import_mbox.go:20,253,330`
- Modify: `internal/importer/mbox_import.go:19-50,187-196`

- [ ] **Step 1: Change MboxImportOptions.Label to Labels**

In `internal/importer/mbox_import.go`, replace:
```go
	// Label, if non-empty, is applied to all imported messages.
	Label string
```

With:
```go
	// Labels, if non-empty, are applied to all imported messages.
	Labels []string
```

- [ ] **Step 2: Update label resolution to handle multiple labels**

In `internal/importer/mbox_import.go`, replace the label resolution block:

Replace:
```go
	// Ensure label (once).
	var labelIDs []int64
	if opts.Label != "" {
		labelID, err := st.EnsureLabel(src.ID, opts.Label, opts.Label, "user")
		if err != nil {
			failSync(err.Error())
			return nil, fmt.Errorf("ensure label: %w", err)
		}
		labelIDs = []int64{labelID}
	}
```

With:
```go
	// Ensure labels (once). Deduplicate to avoid PK violations.
	var labelIDs []int64
	seen := make(map[string]bool)
	for _, lbl := range opts.Labels {
		lbl = strings.TrimSpace(lbl)
		if lbl == "" || seen[lbl] {
			continue
		}
		seen[lbl] = true
		labelID, err := st.EnsureLabel(src.ID, lbl, lbl, "user")
		if err != nil {
			failSync(err.Error())
			return nil, fmt.Errorf("ensure label %q: %w", lbl, err)
		}
		labelIDs = append(labelIDs, labelID)
	}
```

Ensure `"strings"` is in the import list (it already is).

- [ ] **Step 3: Update CLI flag from StringVar to StringSliceVar**

In `cmd/msgvault/cmd/import_mbox.go`, replace:
```go
	importMboxLabel              string
```

With:
```go
	importMboxLabels             []string
```

Replace the flag definition:
```go
	importMboxCmd.Flags().StringVar(&importMboxLabel, "label", "", "Label to apply to newly imported messages")
```

With:
```go
	importMboxCmd.Flags().StringSliceVar(&importMboxLabels, "label", nil, "Label(s) to apply to imported messages (repeatable, or comma-separated)")
```

Replace the options construction:
```go
				Label:              importMboxLabel,
```

With:
```go
				Labels:             importMboxLabels,
```

- [ ] **Step 4: Run build and existing tests**

Run: `go build ./cmd/msgvault/ && go test ./internal/importer/ -run TestImportMbox -v`
Expected: Build succeeds. Existing tests pass (they use `Label: "hey"` which now needs updating).

If tests fail because of the field rename, update test files. In `internal/importer/mbox_import_test.go`, replace all instances of `Label:` with `Labels: []string{` and close with `}`. For example:
```go
Label: "hey",
```
becomes:
```go
Labels: []string{"hey"},
```

- [ ] **Step 5: Format and vet**

Run: `go fmt ./... && go vet ./cmd/msgvault/cmd/ ./internal/importer/`

- [ ] **Step 6: Commit**

```bash
git add cmd/msgvault/cmd/import_mbox.go internal/importer/mbox_import.go internal/importer/mbox_import_test.go internal/importer/mbox_import_security_test.go
git commit -m "Support multiple --label flags for mbox import (#189)

Change --label from StringVar to StringSliceVar. Supports repeated
flags (--label a --label b) and comma-separated (--label a,b).
Deduplicates labels to avoid PK violations."
```

---

### Task 5: import-mbox — add labels on duplicate re-import (#190)

**Files:**
- Modify: `internal/importer/mbox_import.go:52-65,322-337`

- [ ] **Step 1: Add LabelsUpdated field to MboxImportSummary**

In `internal/importer/mbox_import.go`, add the field to the summary struct:

Replace:
```go
	MessagesSkipped   int64
```

With:
```go
	MessagesSkipped   int64
	LabelsUpdated     int64
```

- [ ] **Step 2: Add label update logic on duplicate detection**

In `internal/importer/mbox_import.go`, in the `flushPending` function, find the block where `exists` is true and messages are skipped. Replace:

```go
			if exists {
				summary.MessagesSkipped++

				// Update checkpoint offset even when skipping so resumption progresses.
```

With:
```go
			if exists {
				summary.MessagesSkipped++

				// Add labels to existing message (same pattern as emlx importer).
				if len(labelIDs) > 0 {
					if msgID, ok := existingWithRaw[p.SourceMsg]; ok && msgID > 0 {
						if err := st.AddMessageLabels(msgID, labelIDs); err != nil {
							log.Warn("failed to add labels to existing message",
								"source_message_id", p.SourceMsg, "error", err)
						} else {
							summary.LabelsUpdated++
						}
					}
				}

				// Update checkpoint offset even when skipping so resumption progresses.
```

- [ ] **Step 3: Check that existingWithRaw returns message IDs**

Run: `grep -n 'MessageExistsWithRawBatch' internal/store/messages.go | head -5`

Verify that the returned map is `map[string]int64` where the value is the message ID. If it returns `map[string]bool`, we need to use `MessageExistsBatch` instead (which may return IDs). Check the actual return type and adjust accordingly.

- [ ] **Step 4: Display LabelsUpdated in CLI output**

In `cmd/msgvault/cmd/import_mbox.go`, after the summary output, add a line for labels. Find the existing output block and add a line. After:
```go
		_, _ = fmt.Fprintf(out, "  Skipped:        %d messages\n", totalSkipped)
```

Add accumulation variable `totalLabelsUpdated` (similar to existing counters) and output:
```go
		_, _ = fmt.Fprintf(out, "  Labels updated: %d messages\n", totalLabelsUpdated)
```

Also add the accumulator in the loop:
```go
			totalLabelsUpdated += summary.LabelsUpdated
```

And declare it with the other accumulators at the top of the loop.

- [ ] **Step 5: Run tests**

Run: `go test ./internal/importer/ -v && go fmt ./... && go vet ./...`

- [ ] **Step 6: Commit**

```bash
git add internal/importer/mbox_import.go cmd/msgvault/cmd/import_mbox.go
git commit -m "Add labels to existing messages on mbox re-import (#190)

When a duplicate message is detected during mbox import, add
the current import's labels to the existing message instead of
silently skipping. Matches the emlx importer pattern."
```

---

### Task 6: CLI completion (#113)

**Files:**
- Create: `cmd/msgvault/cmd/completion.go`
- Modify: `cmd/msgvault/cmd/root.go:38`

- [ ] **Step 1: Create completion.go**

Create `cmd/msgvault/cmd/completion.go`:

```go
package cmd

import (
	"os"

	"github.com/spf13/cobra"
)

var completionCmd = &cobra.Command{
	Use:   "completion [bash|zsh|fish|powershell]",
	Short: "Generate shell completion script",
	Long: `Generate a shell completion script for msgvault.

To load completions:

Bash:
  $ source <(msgvault completion bash)
  # To load completions for each session, execute once:
  # Linux:
  $ msgvault completion bash > /etc/bash_completion.d/msgvault
  # macOS:
  $ msgvault completion bash > $(brew --prefix)/etc/bash_completion.d/msgvault

Zsh:
  # If shell completion is not already enabled in your environment,
  # enable it by executing the following once:
  $ echo "autoload -U compinit; compinit" >> ~/.zshrc
  # To load completions for each session, execute once:
  $ msgvault completion zsh > "${fpath[1]}/_msgvault"

Fish:
  $ msgvault completion fish > ~/.config/fish/completions/msgvault.fish

PowerShell:
  PS> msgvault completion powershell | Out-String | Invoke-Expression
`,
	DisableFlagsInUseLine: true,
	ValidArgs:             []string{"bash", "zsh", "fish", "powershell"},
	Args:                  cobra.MatchAll(cobra.ExactArgs(1), cobra.OnlyValidArgs),
	RunE: func(cmd *cobra.Command, args []string) error {
		switch args[0] {
		case "bash":
			return cmd.Root().GenBashCompletionV2(os.Stdout, true)
		case "zsh":
			return cmd.Root().GenZshCompletion(os.Stdout)
		case "fish":
			return cmd.Root().GenFishCompletion(os.Stdout, true)
		case "powershell":
			return cmd.Root().GenPowerShellCompletionWithDesc(os.Stdout)
		}
		return nil
	},
}

func init() {
	rootCmd.AddCommand(completionCmd)
}
```

- [ ] **Step 2: Add "completion" to config-loading skip list**

In `cmd/msgvault/cmd/root.go`, replace:
```go
		if cmd.Name() == "version" || cmd.Name() == "update" || cmd.Name() == "quickstart" {
```

With:
```go
		if cmd.Name() == "version" || cmd.Name() == "update" || cmd.Name() == "quickstart" || cmd.Name() == "completion" {
```

- [ ] **Step 3: Verify it works**

Run: `go build ./cmd/msgvault/ && ./msgvault completion bash | head -5 && ./msgvault completion zsh | head -5`
Expected: Non-empty completion script output for both shells.

- [ ] **Step 4: Format and vet**

Run: `go fmt ./cmd/msgvault/cmd/ && go vet ./cmd/msgvault/cmd/`

- [ ] **Step 5: Commit**

```bash
git add cmd/msgvault/cmd/completion.go cmd/msgvault/cmd/root.go
git commit -m "Add CLI completion command (#113)

Generate shell completion scripts for bash, zsh, fish, and
powershell via 'msgvault completion <shell>'. Skips config
loading so completion works without a valid config."
```

---

### Task 7: Web server search — support query syntax (#174)

**Files:**
- Modify: `internal/store/api.go` (add `SearchMessagesQuery`)
- Modify: `internal/api/handlers.go:250-293`
- Modify: `internal/api/server.go:23-28` (add to interface)
- Create: `internal/store/api_search_test.go`

- [ ] **Step 1: Add SearchMessagesQuery to the MessageStore interface**

In `internal/api/server.go`, add the method to the interface:

Replace:
```go
type MessageStore interface {
	GetStats() (*StoreStats, error)
	ListMessages(offset, limit int) ([]APIMessage, int64, error)
	GetMessage(id int64) (*APIMessage, error)
	SearchMessages(query string, offset, limit int) ([]APIMessage, int64, error)
}
```

With:
```go
type MessageStore interface {
	GetStats() (*StoreStats, error)
	ListMessages(offset, limit int) ([]APIMessage, int64, error)
	GetMessage(id int64) (*APIMessage, error)
	SearchMessages(query string, offset, limit int) ([]APIMessage, int64, error)
	SearchMessagesQuery(q *search.Query, offset, limit int) ([]APIMessage, int64, error)
}
```

Add `"github.com/wesm/msgvault/internal/search"` to the import list.

- [ ] **Step 2: Implement SearchMessagesQuery on Store**

In `internal/store/api.go`, add the method after `SearchMessages`:

```go
// SearchMessagesQuery searches messages using a parsed query with
// support for structured operators (from:, to:, label:, etc.).
func (s *Store) SearchMessagesQuery(
	q *search.Query, offset, limit int,
) ([]APIMessage, int64, error) {
	var conditions []string
	var args []interface{}

	conditions = append(conditions,
		"m.deleted_from_source_at IS NULL")

	// FTS5 text terms.
	ftsJoin := ""
	if len(q.TextTerms) > 0 {
		ftsExpr := buildFTSExpression(q.TextTerms)
		ftsJoin = "JOIN messages_fts fts ON fts.rowid = m.id"
		conditions = append(conditions, "messages_fts MATCH ?")
		args = append(args, ftsExpr)
	}

	// from: filter
	for _, addr := range q.FromAddrs {
		conditions = append(conditions, `EXISTS (
			SELECT 1 FROM message_recipients mr2
			JOIN participants p2 ON p2.id = mr2.participant_id
			WHERE mr2.message_id = m.id
			AND mr2.recipient_type = 'from'
			AND LOWER(p2.email_address) LIKE ?  ESCAPE '\'
		)`)
		args = append(args, "%"+escapeLike(strings.ToLower(addr))+"%")
	}

	// to: filter
	for _, addr := range q.ToAddrs {
		conditions = append(conditions, `EXISTS (
			SELECT 1 FROM message_recipients mr2
			JOIN participants p2 ON p2.id = mr2.participant_id
			WHERE mr2.message_id = m.id
			AND mr2.recipient_type = 'to'
			AND LOWER(p2.email_address) LIKE ? ESCAPE '\'
		)`)
		args = append(args, "%"+escapeLike(strings.ToLower(addr))+"%")
	}

	// cc: filter
	for _, addr := range q.CcAddrs {
		conditions = append(conditions, `EXISTS (
			SELECT 1 FROM message_recipients mr2
			JOIN participants p2 ON p2.id = mr2.participant_id
			WHERE mr2.message_id = m.id
			AND mr2.recipient_type = 'cc'
			AND LOWER(p2.email_address) LIKE ? ESCAPE '\'
		)`)
		args = append(args, "%"+escapeLike(strings.ToLower(addr))+"%")
	}

	// bcc: filter
	for _, addr := range q.BccAddrs {
		conditions = append(conditions, `EXISTS (
			SELECT 1 FROM message_recipients mr2
			JOIN participants p2 ON p2.id = mr2.participant_id
			WHERE mr2.message_id = m.id
			AND mr2.recipient_type = 'bcc'
			AND LOWER(p2.email_address) LIKE ? ESCAPE '\'
		)`)
		args = append(args, "%"+escapeLike(strings.ToLower(addr))+"%")
	}

	// label: filter
	for _, lbl := range q.Labels {
		conditions = append(conditions, `EXISTS (
			SELECT 1 FROM message_labels ml2
			JOIN labels l2 ON l2.id = ml2.label_id
			WHERE ml2.message_id = m.id
			AND LOWER(l2.name) LIKE ? ESCAPE '\'
		)`)
		args = append(args, "%"+escapeLike(strings.ToLower(lbl))+"%")
	}

	// subject: filter
	for _, term := range q.SubjectTerms {
		conditions = append(conditions,
			"m.subject LIKE ? ESCAPE '\\'")
		args = append(args, "%"+escapeLike(term)+"%")
	}

	// has:attachment
	if q.HasAttachment != nil && *q.HasAttachment {
		conditions = append(conditions, "m.has_attachments = 1")
	}

	// after: / before:
	if q.AfterDate != nil {
		conditions = append(conditions,
			"COALESCE(m.sent_at, m.received_at, m.internal_date) >= ?")
		args = append(args, q.AfterDate.Format(time.RFC3339))
	}
	if q.BeforeDate != nil {
		conditions = append(conditions,
			"COALESCE(m.sent_at, m.received_at, m.internal_date) < ?")
		args = append(args, q.BeforeDate.Format(time.RFC3339))
	}

	whereClause := strings.Join(conditions, " AND ")

	// Count query.
	countSQL := fmt.Sprintf(`
		SELECT COUNT(*)
		FROM messages m
		%s
		WHERE %s
	`, ftsJoin, whereClause)

	var total int64
	if err := s.db.QueryRow(countSQL, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count search results: %w", err)
	}

	// Results query.
	orderBy := "COALESCE(m.sent_at, m.received_at, m.internal_date) DESC"
	if ftsJoin != "" {
		orderBy = "rank, " + orderBy
	}
	searchSQL := fmt.Sprintf(`
		SELECT
			m.id,
			COALESCE(m.conversation_id, 0) as conversation_id,
			COALESCE(m.subject, '') as subject,
			COALESCE(p.email_address, '') as from_email,
			COALESCE(m.sent_at, m.received_at, m.internal_date) as sent_at,
			COALESCE(m.snippet, '') as snippet,
			m.has_attachments,
			m.size_estimate
		FROM messages m
		%s
		LEFT JOIN message_recipients mr ON mr.message_id = m.id AND mr.recipient_type = 'from'
		LEFT JOIN participants p ON p.id = mr.participant_id
		WHERE %s
		ORDER BY %s
		LIMIT ? OFFSET ?
	`, ftsJoin, whereClause, orderBy)

	resultArgs := append(args, limit, offset)
	rows, err := s.db.Query(searchSQL, resultArgs...)
	if err != nil {
		// FTS5 not available — fall back if we used it.
		if ftsJoin != "" {
			return s.searchMessagesQueryWithoutFTS(q, offset, limit)
		}
		return nil, 0, err
	}
	defer func() { _ = rows.Close() }()

	messages, ids, err := scanMessageRows(rows)
	if err != nil {
		return nil, 0, err
	}

	if len(ids) > 0 {
		if err := s.batchPopulate(messages, ids); err != nil {
			return nil, 0, err
		}
	}

	return messages, total, nil
}

// buildFTSExpression builds an FTS5 MATCH expression from text terms.
// Each term is quoted for exact-token matching and combined with AND.
func buildFTSExpression(terms []string) string {
	quoted := make([]string, len(terms))
	for i, t := range terms {
		quoted[i] = `"` + strings.ReplaceAll(t, `"`, `""`) + `"`
	}
	return strings.Join(quoted, " AND ")
}

// searchMessagesQueryWithoutFTS is a fallback when FTS5 is unavailable.
// It uses LIKE on subject/snippet for text terms.
func (s *Store) searchMessagesQueryWithoutFTS(
	q *search.Query, offset, limit int,
) ([]APIMessage, int64, error) {
	// Rebuild the query but replace FTS with LIKE.
	fallbackQ := *q
	for _, term := range q.TextTerms {
		fallbackQ.SubjectTerms = append(
			fallbackQ.SubjectTerms, term)
	}
	fallbackQ.TextTerms = nil
	return s.SearchMessagesQuery(&fallbackQ, offset, limit)
}
```

Add `"time"` to the imports if not already present.
Add `"github.com/wesm/msgvault/internal/search"` to imports.

- [ ] **Step 3: Update handleSearch to use parsed query**

In `internal/api/handlers.go`, replace the search handler body:

Replace:
```go
	messages, total, err := s.store.SearchMessages(query, offset, pageSize)
	if err != nil {
		s.logger.Error("search failed", "query", query, "error", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "Search failed")
		return
	}
```

With:
```go
	parsedQuery := search.Parse(query)
	parsedQuery.HideDeleted = true

	// Use structured query if operators are present; plain FTS otherwise.
	var (
		messages []store.APIMessage
		total    int64
		err      error
	)
	if parsedQuery.HasOperators() {
		messages, total, err = s.store.SearchMessagesQuery(parsedQuery, offset, pageSize)
	} else {
		messages, total, err = s.store.SearchMessages(query, offset, pageSize)
	}
	if err != nil {
		s.logger.Error("search failed", "query", query, "error", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "Search failed")
		return
	}
```

Add `"github.com/wesm/msgvault/internal/search"` and `"github.com/wesm/msgvault/internal/store"` to imports if not already present.

- [ ] **Step 4: Add HasOperators method to search.Query**

In `internal/search/parser.go`, add:

```go
// HasOperators returns true if the query contains any structured
// operators beyond plain text terms.
func (q *Query) HasOperators() bool {
	return len(q.FromAddrs) > 0 ||
		len(q.ToAddrs) > 0 ||
		len(q.CcAddrs) > 0 ||
		len(q.BccAddrs) > 0 ||
		len(q.SubjectTerms) > 0 ||
		len(q.Labels) > 0 ||
		q.HasAttachment != nil ||
		q.BeforeDate != nil ||
		q.AfterDate != nil ||
		q.LargerThan != nil ||
		q.SmallerThan != nil
}
```

- [ ] **Step 5: Update any test mocks that implement MessageStore**

Search for any test files that mock `MessageStore` and add the new method:

Run: `grep -rn 'SearchMessages(' internal/api/ --include='*_test.go'`

If a mock struct implements `MessageStore`, add:
```go
func (m *mockStore) SearchMessagesQuery(q *search.Query, offset, limit int) ([]store.APIMessage, int64, error) {
	return nil, 0, nil
}
```

- [ ] **Step 6: Run tests and format**

Run: `go test ./internal/store/ ./internal/api/ ./internal/search/ -v && go fmt ./... && go vet ./...`

- [ ] **Step 7: Commit**

```bash
git add internal/store/api.go internal/api/handlers.go internal/api/server.go internal/search/parser.go
git commit -m "Support query syntax in /api/v1/search endpoint (#174)

Parse structured operators (from:, to:, label:, subject:,
has:attachment, before:, after:) in the API search handler.
Uses store-layer SQL building to preserve FTS5 body search,
exact total count, and batch-populated recipients."
```

---

### Task 8: Dockerfile — fix GLIBC issue (#173)

**Files:**
- Modify: `Dockerfile:37-49`

- [ ] **Step 1: Update runtime stage to wolfi-base**

In `Dockerfile`, replace the runtime stage:

Replace:
```dockerfile
# Runtime stage
FROM debian:bookworm-slim@sha256:98f4b71de414932439ac6ac690d7060df1f27161073c5036a7553723881bffbe

# Install runtime dependencies (libstdc++6 required for CGO/DuckDB)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    tzdata \
    wget \
    libstdc++6 \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -m -u 1000 -s /bin/sh msgvault
```

With:
```dockerfile
# Runtime stage — wolfi-base provides current glibc for CGO/DuckDB bindings
FROM chainguard/wolfi-base:latest

# Install runtime dependencies (libstdc++ required for CGO/DuckDB)
RUN apk update && apk add --no-cache \
    ca-certificates \
    tzdata \
    wget \
    libstdc++

# Create non-root user
RUN adduser -D -h /home/msgvault -u 1000 -s /bin/sh msgvault
```

- [ ] **Step 2: Verify build (if Docker available)**

Run: `docker build -t msgvault-test . 2>&1 | tail -5`

If Docker is not available locally, this can be verified in CI.

- [ ] **Step 3: Commit**

```bash
git add Dockerfile
git commit -m "Switch Dockerfile runtime to wolfi-base for GLIBC compat (#173)

Replace debian:bookworm-slim with chainguard/wolfi-base which
provides current glibc. Fixes runtime GLIBC_2.38 requirement
error. Smaller image, rolling glibc updates."
```

---

## Verification

After all tasks are complete:

- [ ] Run full test suite: `make test`
- [ ] Run linter: `make lint`
- [ ] Run vet: `go vet ./...`
- [ ] Build binary: `make build`
