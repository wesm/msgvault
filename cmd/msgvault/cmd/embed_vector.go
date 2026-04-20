//go:build sqlite_vec

package cmd

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/wesm/msgvault/internal/store"
	"github.com/wesm/msgvault/internal/vector"
	"github.com/wesm/msgvault/internal/vector/embed"
	"github.com/wesm/msgvault/internal/vector/sqlitevec"
)

func runEmbed(ctx context.Context) error {
	s, err := store.Open(cfg.DatabaseDSN())
	if err != nil {
		return fmt.Errorf("open main db: %w", err)
	}
	defer func() { _ = s.Close() }()

	if err := sqlitevec.RegisterExtension(); err != nil {
		return fmt.Errorf("register sqlite-vec: %w", err)
	}

	vecPath := cfg.Vector.DBPath
	if vecPath == "" {
		vecPath = filepath.Join(cfg.Data.DataDir, "vectors.db")
	}
	backend, err := sqlitevec.Open(ctx, sqlitevec.Options{
		Path:      vecPath,
		MainPath:  cfg.DatabaseDSN(),
		Dimension: cfg.Vector.Embeddings.Dimension,
		MainDB:    s.DB(),
	})
	if err != nil {
		return fmt.Errorf("open vectors.db: %w", err)
	}
	defer func() { _ = backend.Close() }()

	var gen vector.GenerationID
	if embedFullRebuild {
		if !embedYes {
			if !confirmEmbed("Start a full rebuild? This builds a new generation and atomically swaps it in when complete. ") {
				return fmt.Errorf("aborted")
			}
		}
		gen, err = backend.CreateGeneration(ctx, cfg.Vector.Embeddings.Model, cfg.Vector.Embeddings.Dimension)
		if err != nil {
			return fmt.Errorf("create generation: %w", err)
		}
		fmt.Fprintf(os.Stderr, "Created building generation %d (%s:%d).\n",
			gen, cfg.Vector.Embeddings.Model, cfg.Vector.Embeddings.Dimension)
	} else {
		active, err := vector.ResolveActive(ctx, backend, cfg.Vector.Embeddings)
		if err != nil {
			return fmt.Errorf("resolve active generation: %w (hint: run with --full-rebuild to start)", err)
		}
		gen = active.ID
		fmt.Fprintf(os.Stderr, "Using active generation %d (%s).\n", gen, active.Fingerprint)
	}

	client := embed.NewClient(embed.Config{
		Endpoint:   cfg.Vector.Embeddings.Endpoint,
		APIKey:     cfg.Vector.Embeddings.APIKey(),
		Model:      cfg.Vector.Embeddings.Model,
		Dimension:  cfg.Vector.Embeddings.Dimension,
		Timeout:    cfg.Vector.Embeddings.Timeout,
		MaxRetries: cfg.Vector.Embeddings.MaxRetries,
	})
	worker := embed.NewWorker(embed.WorkerDeps{
		Backend:   backend,
		VectorsDB: backend.DB(),
		MainDB:    s.DB(),
		Client:    client,
		Preprocess: embed.PreprocessConfig{
			StripQuotes:     cfg.Vector.Preprocess.StripQuotesEnabled(),
			StripSignatures: cfg.Vector.Preprocess.StripSignaturesEnabled(),
		},
		MaxInputChars: cfg.Vector.Embeddings.MaxInputChars,
		BatchSize:     cfg.Vector.Embeddings.BatchSize,
	})

	if n, err := worker.ReclaimStale(ctx); err != nil {
		return fmt.Errorf("reclaim stale: %w", err)
	} else if n > 0 {
		fmt.Fprintf(os.Stderr, "Reclaimed %d stale claims.\n", n)
	}

	res, err := worker.RunOnce(ctx, gen)
	if err != nil {
		return fmt.Errorf("embed run: %w", err)
	}
	fmt.Printf("Claimed: %d, succeeded: %d, failed: %d, truncated: %d\n",
		res.Claimed, res.Succeeded, res.Failed, res.Truncated)

	if embedFullRebuild && res.Failed == 0 {
		remaining, err := pendingCount(ctx, backend.DB(), gen)
		if err != nil {
			return fmt.Errorf("count pending: %w", err)
		}
		if remaining == 0 {
			if err := backend.ActivateGeneration(ctx, gen); err != nil {
				return fmt.Errorf("activate generation: %w", err)
			}
			fmt.Printf("Generation %d activated.\n", gen)
		} else {
			fmt.Fprintf(os.Stderr,
				"Generation %d still has %d pending rows; run `msgvault embed` again to finish, then it will activate automatically.\n",
				gen, remaining)
		}
	}
	return nil
}

func pendingCount(ctx context.Context, db *sql.DB, gen vector.GenerationID) (int, error) {
	var n int
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM pending_embeddings WHERE generation_id = ?`, int64(gen)).Scan(&n); err != nil {
		return 0, fmt.Errorf("query pending: %w", err)
	}
	return n, nil
}

// confirmEmbed reads a y/N answer from stdin. Default is no.
func confirmEmbed(prompt string) bool {
	fmt.Fprint(os.Stderr, prompt+"[y/N]: ")
	r := bufio.NewReader(os.Stdin)
	line, err := r.ReadString('\n')
	if err != nil {
		return false
	}
	line = strings.TrimSpace(strings.ToLower(line))
	return line == "y" || line == "yes"
}
