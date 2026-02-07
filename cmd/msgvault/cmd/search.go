package cmd

import (
	"encoding/json"
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
	searchLimit  int
	searchOffset int
	searchJSON   bool
)

var searchCmd = &cobra.Command{
	Use:   "search <query>",
	Short: "Search messages using Gmail-like query syntax",
	Long: `Search your email archive using Gmail-like query syntax.

Supported operators:
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
	Args: cobra.MinimumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		// Join all args to form the query (allows unquoted multi-term searches)
		queryStr := strings.Join(args, " ")

		// Parse the query
		q := search.Parse(queryStr)
		if q.IsEmpty() {
			return fmt.Errorf("empty search query")
		}

		fmt.Fprintf(os.Stderr, "Searching...")

		// Open database
		dbPath := cfg.DatabaseDSN()
		s, err := store.Open(dbPath)
		if err != nil {
			return fmt.Errorf("open database: %w", err)
		}
		defer s.Close()

		// Ensure schema is up to date and FTS index is populated
		if err := s.InitSchema(); err != nil {
			return fmt.Errorf("init schema: %w", err)
		}
		if err := ensureFTSIndex(s); err != nil {
			return err
		}

		// Create query engine and execute search
		engine := query.NewSQLiteEngine(s.DB())
		results, err := engine.Search(cmd.Context(), q, searchLimit, searchOffset)
		fmt.Fprintf(os.Stderr, "\r            \r")
		if err != nil {
			return query.HintRepairEncoding(fmt.Errorf("search: %w", err))
		}

		if len(results) == 0 {
			fmt.Println("No messages found.")
			return nil
		}

		if searchJSON {
			return outputSearchResultsJSON(results)
		}
		return outputSearchResultsTable(results)
	},
}

func outputSearchResultsTable(results []query.MessageSummary) error {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tDATE\tFROM\tSUBJECT\tSIZE")
	fmt.Fprintln(w, "──\t────\t────\t───────\t────")

	for _, msg := range results {
		date := msg.SentAt.Format("2006-01-02")
		from := truncate(msg.FromEmail, 30)
		subject := truncate(msg.Subject, 50)
		size := formatSize(msg.SizeEstimate)
		fmt.Fprintf(w, "%d\t%s\t%s\t%s\t%s\n", msg.ID, date, from, subject, size)
	}

	w.Flush()
	fmt.Printf("\nShowing %d results\n", len(results))
	return nil
}

func outputSearchResultsJSON(results []query.MessageSummary) error {
	output := make([]map[string]interface{}, len(results))
	for i, msg := range results {
		output[i] = map[string]interface{}{
			"id":                msg.ID,
			"source_message_id": msg.SourceMessageID,
			"conversation_id":   msg.ConversationID,
			"subject":           msg.Subject,
			"snippet":           msg.Snippet,
			"from_email":        msg.FromEmail,
			"from_name":         msg.FromName,
			"sent_at":           msg.SentAt.Format(time.RFC3339),
			"size_estimate":     msg.SizeEstimate,
			"has_attachments":   msg.HasAttachments,
			"attachment_count":  msg.AttachmentCount,
			"labels":            msg.Labels,
		}
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(output)
}

func formatSize(bytes int64) string {
	const (
		KB = 1024
		MB = 1024 * KB
		GB = 1024 * MB
	)

	switch {
	case bytes >= GB:
		return fmt.Sprintf("%.1fG", float64(bytes)/float64(GB))
	case bytes >= MB:
		return fmt.Sprintf("%.1fM", float64(bytes)/float64(MB))
	case bytes >= KB:
		return fmt.Sprintf("%.1fK", float64(bytes)/float64(KB))
	default:
		return fmt.Sprintf("%dB", bytes)
	}
}

func init() {
	rootCmd.AddCommand(searchCmd)
	searchCmd.Flags().IntVarP(&searchLimit, "limit", "n", 50, "Maximum number of results")
	searchCmd.Flags().IntVar(&searchOffset, "offset", 0, "Skip first N results")
	searchCmd.Flags().BoolVar(&searchJSON, "json", false, "Output as JSON")
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

// Common flag variables used across aggregate commands
var (
	aggLimit  int
	aggAfter  string
	aggBefore string
	aggJSON   bool
)

// parseCommonFlags converts string flags to AggregateOptions
func parseCommonFlags() (query.AggregateOptions, error) {
	opts := query.DefaultAggregateOptions()

	if aggLimit > 0 {
		opts.Limit = aggLimit
	}

	if aggAfter != "" {
		t, err := time.Parse("2006-01-02", aggAfter)
		if err != nil {
			return opts, fmt.Errorf("invalid after date: %w", err)
		}
		opts.After = &t
	}

	if aggBefore != "" {
		t, err := time.Parse("2006-01-02", aggBefore)
		if err != nil {
			return opts, fmt.Errorf("invalid before date: %w", err)
		}
		opts.Before = &t
	}

	return opts, nil
}

// addCommonAggregateFlags adds shared flags to aggregate commands
func addCommonAggregateFlags(cmd *cobra.Command) {
	cmd.Flags().IntVarP(&aggLimit, "limit", "n", 50, "Maximum number of results")
	cmd.Flags().StringVar(&aggAfter, "after", "", "Filter to messages after date (YYYY-MM-DD)")
	cmd.Flags().StringVar(&aggBefore, "before", "", "Filter to messages before date (YYYY-MM-DD)")
	cmd.Flags().BoolVar(&aggJSON, "json", false, "Output as JSON")
}

// outputAggregateTable prints aggregate results as a table
func outputAggregateTable(rows []query.AggregateRow, keyHeader string) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(w, "%s\tCOUNT\tSIZE\tATT SIZE\n", strings.ToUpper(keyHeader))
	fmt.Fprintln(w, strings.Repeat("─", len(keyHeader))+"\t─────\t────\t────────")

	for _, row := range rows {
		fmt.Fprintf(w, "%s\t%d\t%s\t%s\n",
			truncate(row.Key, 40),
			row.Count,
			formatSize(row.TotalSize),
			formatSize(row.AttachmentSize),
		)
	}
	w.Flush()
	fmt.Printf("\nShowing %d results\n", len(rows))
}

// outputAggregateJSON prints aggregate results as JSON
func outputAggregateJSON(rows []query.AggregateRow) error {
	output := make([]map[string]interface{}, len(rows))
	for i, row := range rows {
		output[i] = map[string]interface{}{
			"key":             row.Key,
			"count":           row.Count,
			"total_size":      row.TotalSize,
			"attachment_size": row.AttachmentSize,
		}
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(output)
}
