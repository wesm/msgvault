//go:build sqlite_vec

package cmd

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"text/tabwriter"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/spf13/cobra"
	"github.com/wesm/msgvault/internal/search"
	"github.com/wesm/msgvault/internal/vector"
	"github.com/wesm/msgvault/internal/vector/embed"
	"github.com/wesm/msgvault/internal/vector/hybrid"
	"github.com/wesm/msgvault/internal/vector/sqlitevec"
)

// runHybridSearch executes a vector or hybrid search against the local
// msgvault archive using the sqlite-vec backend and configured embedding
// endpoint. Invoked from search.go when --mode is "vector" or "hybrid".
func runHybridSearch(cmd *cobra.Command, queryStr, mode string, explain bool) error {
	if queryStr == "" {
		return fmt.Errorf("empty search query")
	}
	if !cfg.Vector.Enabled {
		return fmt.Errorf("vector search not enabled; set [vector].enabled = true in config")
	}
	if cfg.Vector.Embeddings.Endpoint == "" || cfg.Vector.Embeddings.Model == "" {
		return fmt.Errorf("vector search requires [vector.embeddings] endpoint and model in config")
	}

	ctx := cmd.Context()

	mainDB, err := sql.Open("sqlite3", cfg.DatabaseDSN())
	if err != nil {
		return fmt.Errorf("open main db: %w", err)
	}
	defer func() { _ = mainDB.Close() }()

	vecDBPath := cfg.Vector.DBPath
	if vecDBPath == "" {
		vecDBPath = filepath.Join(cfg.Data.DataDir, "vectors.db")
	}

	if err := sqlitevec.RegisterExtension(); err != nil {
		return fmt.Errorf("register sqlite-vec: %w", err)
	}

	backend, err := sqlitevec.Open(ctx, sqlitevec.Options{
		Path:      vecDBPath,
		MainPath:  cfg.DatabaseDSN(),
		Dimension: cfg.Vector.Embeddings.Dimension,
		MainDB:    mainDB,
	})
	if err != nil {
		return fmt.Errorf("open vectors.db: %w", err)
	}
	defer func() { _ = backend.Close() }()

	active, err := vector.ResolveActive(ctx, backend, cfg.Vector.Embeddings)
	if err != nil {
		return fmt.Errorf("resolve active generation: %w", err)
	}

	embedClient := embed.NewClient(embed.Config{
		Endpoint:   cfg.Vector.Embeddings.Endpoint,
		APIKey:     cfg.Vector.Embeddings.APIKey(),
		Model:      cfg.Vector.Embeddings.Model,
		Dimension:  cfg.Vector.Embeddings.Dimension,
		Timeout:    cfg.Vector.Embeddings.Timeout,
		MaxRetries: cfg.Vector.Embeddings.MaxRetries,
	})

	eng := hybrid.NewEngine(backend, mainDB, embedClient, hybrid.Config{
		ExpectedFingerprint: cfg.Vector.Embeddings.Fingerprint(),
		RRFK:                cfg.Vector.Search.RRFK,
		KPerSignal:          cfg.Vector.Search.KPerSignal,
		SubjectBoost:        cfg.Vector.Search.SubjectBoost,
	})

	q := search.Parse(queryStr)

	filter, err := buildVectorFilter(ctx, mainDB, q)
	if err != nil {
		return fmt.Errorf("build filter: %w", err)
	}

	freeText := strings.Join(q.TextTerms, " ")

	subjectTerms := make([]string, 0, len(q.TextTerms))
	for _, t := range q.TextTerms {
		subjectTerms = append(subjectTerms, strings.ToLower(t))
	}

	req := hybrid.SearchRequest{
		Mode:         hybrid.Mode(mode),
		FreeText:     freeText,
		Filter:       filter,
		Limit:        searchLimit,
		SubjectTerms: subjectTerms,
		Explain:      explain,
	}

	logger.Info("vector search start",
		"mode", mode,
		"query_len", len(queryStr),
		"limit", searchLimit,
		"explain", explain,
		"generation_id", int64(active.ID),
	)
	started := time.Now()

	hits, meta, err := eng.Search(ctx, req)
	if err != nil {
		logger.Warn("vector search failed",
			"mode", mode,
			"duration_ms", time.Since(started).Milliseconds(),
			"error", err.Error(),
		)
		return fmt.Errorf("hybrid search: %w", err)
	}
	logger.Info("vector search done",
		"mode", mode,
		"results", len(hits),
		"duration_ms", time.Since(started).Milliseconds(),
	)

	results, err := hydrateHybridResults(ctx, mainDB, hits)
	if err != nil {
		return fmt.Errorf("hydrate results: %w", err)
	}

	if searchJSON {
		return outputHybridResultsJSON(results, meta, explain)
	}
	return outputHybridResultsTable(results, meta, explain)
}

// buildVectorFilter translates a parsed Gmail-syntax query into a
// vector.Filter by resolving address/label tokens to IDs against the
// main DB. Unsupported operators (to, cc, bcc, subject, size) are
// silently ignored for T19; T27 may extend this.
func buildVectorFilter(ctx context.Context, db *sql.DB, q *search.Query) (vector.Filter, error) {
	var f vector.Filter

	if len(q.FromAddrs) > 0 {
		ids, err := resolveParticipantIDs(ctx, db, q.FromAddrs)
		if err != nil {
			return f, err
		}
		f.SenderIDs = ids
	}

	if len(q.Labels) > 0 {
		ids, err := resolveLabelIDs(ctx, db, q.Labels)
		if err != nil {
			return f, err
		}
		f.LabelIDs = ids
	}

	if q.HasAttachment != nil {
		v := *q.HasAttachment
		f.HasAttachment = &v
	}
	if q.AfterDate != nil {
		f.After = q.AfterDate
	}
	if q.BeforeDate != nil {
		f.Before = q.BeforeDate
	}
	return f, nil
}

func resolveParticipantIDs(ctx context.Context, db *sql.DB, addrs []string) ([]int64, error) {
	if len(addrs) == 0 {
		return nil, nil
	}
	placeholders := make([]string, len(addrs))
	args := make([]any, len(addrs))
	for i, a := range addrs {
		placeholders[i] = "?"
		args[i] = strings.ToLower(a)
	}
	q := fmt.Sprintf(
		`SELECT id FROM participants WHERE LOWER(email_address) IN (%s)`,
		strings.Join(placeholders, ","))
	rows, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("query participants: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan participant id: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate participants: %w", err)
	}
	return ids, nil
}

func resolveLabelIDs(ctx context.Context, db *sql.DB, labels []string) ([]int64, error) {
	if len(labels) == 0 {
		return nil, nil
	}
	placeholders := make([]string, len(labels))
	args := make([]any, len(labels))
	for i, l := range labels {
		placeholders[i] = "?"
		args[i] = l
	}
	q := fmt.Sprintf(
		`SELECT id FROM labels WHERE name IN (%s)`,
		strings.Join(placeholders, ","))
	rows, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("query labels: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan label id: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate labels: %w", err)
	}
	return ids, nil
}

type hybridResultRow struct {
	MessageID      int64
	Subject        string
	FromEmail      string
	SentAt         time.Time
	RRFScore       float64
	BM25Score      float64
	VectorScore    float64
	SubjectBoosted bool
}

// hydrateHybridResults fetches subject, sender, and sent_at for each
// hit from the main DB, preserving the RRF ordering of the input hits.
func hydrateHybridResults(ctx context.Context, db *sql.DB, hits []vector.FusedHit) ([]hybridResultRow, error) {
	if len(hits) == 0 {
		return nil, nil
	}
	placeholders := make([]string, len(hits))
	args := make([]any, len(hits))
	orderIdx := make(map[int64]int, len(hits))
	for i, h := range hits {
		placeholders[i] = "?"
		args[i] = h.MessageID
		orderIdx[h.MessageID] = i
	}
	q := fmt.Sprintf(`
		SELECT m.id, COALESCE(m.subject,''), COALESCE(p.email_address,''), m.sent_at
		FROM messages m
		LEFT JOIN participants p ON p.id = m.sender_id
		WHERE m.id IN (%s)`, strings.Join(placeholders, ","))
	rows, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("query messages: %w", err)
	}
	defer func() { _ = rows.Close() }()
	out := make([]hybridResultRow, len(hits))
	for rows.Next() {
		var id int64
		var subject, from string
		var sentAt sql.NullTime
		if err := rows.Scan(&id, &subject, &from, &sentAt); err != nil {
			return nil, fmt.Errorf("scan message row: %w", err)
		}
		idx, ok := orderIdx[id]
		if !ok {
			continue
		}
		h := hits[idx]
		row := hybridResultRow{
			MessageID:      id,
			Subject:        subject,
			FromEmail:      from,
			RRFScore:       h.RRFScore,
			BM25Score:      h.BM25Score,
			VectorScore:    h.VectorScore,
			SubjectBoosted: h.SubjectBoosted,
		}
		if sentAt.Valid {
			row.SentAt = sentAt.Time
		}
		out[idx] = row
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate messages: %w", err)
	}
	return out, nil
}

func outputHybridResultsTable(results []hybridResultRow, meta hybrid.ResultMeta, explain bool) error {
	if len(results) == 0 {
		fmt.Println("No messages found.")
		fmt.Printf("\nGeneration #%d (%s, fingerprint=%q)\n",
			int64(meta.Generation.ID), meta.Generation.State, meta.Generation.Fingerprint)
		return nil
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	if explain {
		_, _ = fmt.Fprintln(w, "ID\tDATE\tFROM\tSUBJECT\tRRF\tBM25\tVEC")
		_, _ = fmt.Fprintln(w, "──\t────\t────\t───────\t───\t────\t───")
	} else {
		_, _ = fmt.Fprintln(w, "ID\tDATE\tFROM\tSUBJECT")
		_, _ = fmt.Fprintln(w, "──\t────\t────\t───────")
	}
	for _, r := range results {
		date := r.SentAt.Format("2006-01-02")
		from := truncate(r.FromEmail, 30)
		subject := truncate(r.Subject, 50)
		if r.SubjectBoosted {
			subject += " *"
		}
		if explain {
			_, _ = fmt.Fprintf(w, "%d\t%s\t%s\t%s\t%.4f\t%s\t%s\n",
				r.MessageID, date, from, subject, r.RRFScore,
				formatOptionalScore(r.BM25Score), formatOptionalScore(r.VectorScore))
		} else {
			_, _ = fmt.Fprintf(w, "%d\t%s\t%s\t%s\n",
				r.MessageID, date, from, subject)
		}
	}
	_ = w.Flush()
	fmt.Printf("\nShowing %d results (generation #%d %s, fingerprint=%q)\n",
		len(results), int64(meta.Generation.ID), meta.Generation.State, meta.Generation.Fingerprint)
	return nil
}

func outputHybridResultsJSON(results []hybridResultRow, meta hybrid.ResultMeta, explain bool) error {
	rows := make([]map[string]any, len(results))
	for i, r := range results {
		row := map[string]any{
			"id":         r.MessageID,
			"subject":    r.Subject,
			"from_email": r.FromEmail,
			"sent_at":    r.SentAt.Format(time.RFC3339),
			"rrf_score":  r.RRFScore,
			"boosted":    r.SubjectBoosted,
		}
		if explain {
			if !math.IsNaN(r.BM25Score) {
				row["bm25_score"] = r.BM25Score
			}
			if !math.IsNaN(r.VectorScore) {
				row["vector_score"] = r.VectorScore
			}
		}
		rows[i] = row
	}
	return printJSON(map[string]any{
		"generation": map[string]any{
			"id":          int64(meta.Generation.ID),
			"model":       meta.Generation.Model,
			"dimension":   meta.Generation.Dimension,
			"fingerprint": meta.Generation.Fingerprint,
			"state":       string(meta.Generation.State),
		},
		"pool_saturated": meta.PoolSaturated,
		"returned_count": meta.ReturnedCount,
		"results":        rows,
	})
}

func formatOptionalScore(v float64) string {
	if math.IsNaN(v) {
		return "-"
	}
	return fmt.Sprintf("%.4f", v)
}
