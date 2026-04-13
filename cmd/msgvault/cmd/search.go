package cmd

import (
	"fmt"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
	"github.com/wesm/msgvault/internal/query"
	"github.com/wesm/msgvault/internal/search"
	"github.com/wesm/msgvault/internal/store"
)

var (
	searchLimit   int
	searchOffset  int
	searchJSON    bool
	searchAccount string
)

var searchCmd = &cobra.Command{
	Use:   "search <query>",
	Short: "Search messages using Gmail-like query syntax",
	Long: `Search your email archive using Gmail-like query syntax.

Uses remote server if [remote].url is configured, otherwise uses local database.
Use --local to force local database.

Supported operators (local mode only - remote uses simple text search):
  from:        Sender email address
  to:          Recipient email address
  cc:          CC recipient
  bcc:         BCC recipient
  subject:     Subject text search
  label:       Gmail label (or l: shorthand)
  has:         has:attachment - messages with attachments
  before:      Messages before date (YYYY-MM-DD)
  after:       Messages after date (YYYY-MM-DD)
  older_than:  Relative date (7d, 2w, 1m, 1y)
  newer_than:  Relative date
  larger:      Size filter (5M, 100K)
  smaller:     Size filter

Bare words and "quoted phrases" perform full-text search.

Examples:
  msgvault search from:alice@example.com has:attachment
  msgvault search subject:meeting after:2024-01-01
  msgvault search project report newer_than:30d
  msgvault search '"exact phrase"' label:INBOX`,
	Args: cobra.ArbitraryArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		// Join all args to form the query (allows unquoted multi-term searches)
		queryStr := strings.Join(args, " ")

		if queryStr == "" && searchAccount == "" {
			return fmt.Errorf("provide a search query or --account flag")
		}

		// Use remote search if configured
		if IsRemoteMode() {
			if searchAccount != "" {
				return fmt.Errorf(
					"--account is not supported in remote mode",
				)
			}
			return runRemoteSearch(queryStr)
		}

		return runLocalSearch(cmd, queryStr)
	},
}

// runRemoteSearch performs a search against the remote API.
func runRemoteSearch(queryStr string) error {
	fmt.Fprintf(os.Stderr, "Searching %s...", cfg.Remote.URL)

	s, err := OpenRemoteStore()
	if err != nil {
		return fmt.Errorf("connect to remote: %w", err)
	}
	defer func() { _ = s.Close() }()

	results, total, err := s.SearchMessages(queryStr, searchOffset, searchLimit)
	fmt.Fprintf(os.Stderr, "\r                                                      \r")
	if err != nil {
		return fmt.Errorf("search: %w", err)
	}

	if len(results) == 0 {
		fmt.Println("No messages found.")
		return nil
	}

	if searchJSON {
		return outputRemoteSearchResultsJSON(results, total)
	}
	return outputRemoteSearchResultsTable(results, total)
}

// runLocalSearch performs a search against the local database.
func runLocalSearch(cmd *cobra.Command, queryStr string) error {
	// Parse the query
	q := search.Parse(queryStr)

	// Fail fast on invalid queries before touching the database,
	// unless --account is set (which requires a DB lookup to resolve).
	if searchAccount == "" && q.IsEmpty() {
		return fmt.Errorf("empty search query")
	}

	// Open database
	dbPath := cfg.DatabaseDSN()
	s, err := store.Open(dbPath)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer func() { _ = s.Close() }()

	// Ensure schema is up to date and FTS index is populated
	if err := s.InitSchema(); err != nil {
		return fmt.Errorf("init schema: %w", err)
	}

	// Resolve --account and recheck emptiness.
	if searchAccount != "" {
		src, err := s.GetSourceByIdentifier(searchAccount)
		if err != nil {
			return fmt.Errorf("look up account: %w", err)
		}
		if src == nil {
			return fmt.Errorf("account %q not found", searchAccount)
		}
		q.AccountID = &src.ID
	}

	if q.IsEmpty() {
		return fmt.Errorf("empty search query")
	}

	fmt.Fprintf(os.Stderr, "Searching...")

	if err := ensureFTSIndex(s); err != nil {
		return err
	}

	// Log the search operation. Raw query text and account
	// identifiers may contain PII — log coarse metadata at
	// info and full values only at debug.
	hasAccount := q.AccountID != nil
	logger.Info("search start",
		"query_len", len(queryStr),
		"has_account", hasAccount,
		"limit", searchLimit,
		"offset", searchOffset,
	)
	logger.Debug("search start detail",
		"query", queryStr,
		"account", searchAccount,
	)
	started := time.Now()

	// Create query engine and execute search
	engine := query.NewSQLiteEngine(s.DB())
	results, err := engine.Search(cmd.Context(), q, searchLimit, searchOffset)
	fmt.Fprintf(os.Stderr, "\r            \r")
	if err != nil {
		logger.Warn("search failed",
			"query_len", len(queryStr),
			"duration_ms", time.Since(started).Milliseconds(),
			"error", err.Error(),
		)
		return query.HintRepairEncoding(fmt.Errorf("search: %w", err))
	}
	logger.Info("search done",
		"query_len", len(queryStr),
		"has_account", hasAccount,
		"results", len(results),
		"duration_ms", time.Since(started).Milliseconds(),
	)

	if len(results) == 0 {
		fmt.Println("No messages found.")
		return nil
	}

	if searchJSON {
		return outputSearchResultsJSON(results)
	}
	return outputSearchResultsTable(results)
}

func outputSearchResultsTable(results []query.MessageSummary) error {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(w, "ID\tDATE\tFROM\tSUBJECT\tSIZE")
	_, _ = fmt.Fprintln(w, "──\t────\t────\t───────\t────")

	for _, msg := range results {
		date := msg.SentAt.Format("2006-01-02")
		from := truncate(msg.FromEmail, 30)
		subject := truncate(msg.Subject, 50)
		size := formatSize(msg.SizeEstimate)
		_, _ = fmt.Fprintf(w, "%d\t%s\t%s\t%s\t%s\n", msg.ID, date, from, subject, size)
	}

	_ = w.Flush()
	fmt.Printf("\nShowing %d results\n", len(results))
	return nil
}

func outputRemoteSearchResultsTable(results []store.APIMessage, total int64) error {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(w, "ID\tDATE\tFROM\tSUBJECT\tSIZE")
	_, _ = fmt.Fprintln(w, "──\t────\t────\t───────\t────")

	for _, msg := range results {
		date := msg.SentAt.Format("2006-01-02")
		from := truncate(msg.From, 30)
		subject := truncate(msg.Subject, 50)
		size := formatSize(msg.SizeEstimate)
		_, _ = fmt.Fprintf(w, "%d\t%s\t%s\t%s\t%s\n", msg.ID, date, from, subject, size)
	}

	_ = w.Flush()
	fmt.Printf("\nShowing %d of %d results\n", len(results), total)
	return nil
}

func outputRemoteSearchResultsJSON(results []store.APIMessage, total int64) error {
	return printJSON(map[string]interface{}{
		"total":   total,
		"results": results,
	})
}

func outputSearchResultsJSON(results []query.MessageSummary) error {
	output := make([]map[string]interface{}, len(results))
	for i, msg := range results {
		output[i] = map[string]interface{}{
			"id":                     msg.ID,
			"source_message_id":      msg.SourceMessageID,
			"conversation_id":        msg.ConversationID,
			"source_conversation_id": msg.SourceConversationID,
			"subject":                msg.Subject,
			"snippet":                msg.Snippet,
			"from_email":             msg.FromEmail,
			"from_name":              msg.FromName,
			"sent_at":                msg.SentAt.Format(time.RFC3339),
			"size_estimate":          msg.SizeEstimate,
			"has_attachments":        msg.HasAttachments,
			"attachment_count":       msg.AttachmentCount,
			"labels":                 msg.Labels,
		}
	}

	return printJSON(output)
}

func init() {
	rootCmd.AddCommand(searchCmd)
	searchCmd.Flags().IntVarP(&searchLimit, "limit", "n", 50, "Maximum number of results")
	searchCmd.Flags().IntVar(&searchOffset, "offset", 0, "Skip first N results")
	searchCmd.Flags().BoolVar(&searchJSON, "json", false, "Output as JSON")
	searchCmd.Flags().StringVar(&searchAccount, "account", "", "Limit results to a specific account (email address)")
}

// ensureFTSIndex checks if the FTS search index needs to be built and
// runs a one-time backfill if so. Shows a live progress bar since this
// can take a while on large archives. Blocks until complete.
func ensureFTSIndex(s *store.Store) error {
	if !s.NeedsFTSBackfill() {
		return nil
	}
	fmt.Fprintf(os.Stderr, "Building search index (one-time)...\n")
	n, err := s.BackfillFTS(func(done, total int64) {
		if total <= 0 {
			return
		}
		if done > total {
			done = total
		}
		pct := int(done * 100 / total)
		barWidth := 30
		filled := barWidth * pct / 100
		bar := strings.Repeat("=", filled) + strings.Repeat(" ", barWidth-filled)
		fmt.Fprintf(os.Stderr, "\r  [%s] %3d%%", bar, pct)
	})
	if err != nil {
		fmt.Fprintln(os.Stderr)
		return fmt.Errorf("build search index: %w", err)
	}
	fmt.Fprintf(os.Stderr, "\r  [%s] 100%%  %d messages indexed.\n", strings.Repeat("=", 30), n)
	return nil
}
