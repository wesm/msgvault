//go:build sqlite_vec

package cmd

import (
	"bufio"
	"context"
	"database/sql"
	"errors"
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

	gen, rebuildInProgress, err := pickEmbedGeneration(ctx, backend, embedGenerationOpts{
		FullRebuild: embedFullRebuild,
		Model:       cfg.Vector.Embeddings.Model,
		Dimension:   cfg.Vector.Embeddings.Dimension,
		Fingerprint: cfg.Vector.Embeddings.Fingerprint(),
		Confirm: func() bool {
			return embedYes ||
				confirmEmbed("Start a full rebuild? This builds a new generation and atomically swaps it in when complete. ")
		},
		Stderr: os.Stderr,
	})
	if err != nil {
		return err
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

	// Activation is a function of the generation's final state, not
	// of the cumulative retry counter — transient failures that the
	// worker later recovers from must not block activation, and an
	// active generation must not be re-activated.
	if rebuildInProgress {
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

// embedGenerationOpts bundles the inputs pickEmbedGeneration needs.
// Externalized so tests can drive the logic without the command-line
// globals.
type embedGenerationOpts struct {
	FullRebuild bool
	Model       string
	Dimension   int
	Fingerprint string // must equal Model:Dimension
	// Confirm is only called when FullRebuild is true. Returns
	// true if the user agreed to proceed.
	Confirm func() bool
	Stderr  *os.File
}

// pickEmbedGeneration resolves which generation this embed run
// should target. Returns (gen, rebuildInProgress, err):
//
//   - FullRebuild: prompt for confirmation, then call
//     CreateGeneration. That call reuses an existing building
//     generation with the matching fingerprint (so interrupted
//     rebuilds resume cleanly), or returns ErrBuildingInProgress
//     for a mismatch.
//   - default mode with an active generation: target it (incremental
//     top-up).
//   - default mode with no active but a building generation: resume
//     the building one. This path is the "run embed again to finish"
//     recovery flow — previously impossible because ResolveActive
//     returned ErrIndexBuilding and embed_vector would refuse to
//     proceed.
//   - otherwise: error with a hint to use --full-rebuild.
//
// rebuildInProgress is true whenever the target is a building
// generation; activation is considered only in that case.
func pickEmbedGeneration(ctx context.Context, backend vector.Backend, opts embedGenerationOpts) (vector.GenerationID, bool, error) {
	if opts.FullRebuild {
		if opts.Confirm != nil && !opts.Confirm() {
			return 0, false, fmt.Errorf("aborted")
		}
		gen, err := backend.CreateGeneration(ctx, opts.Model, opts.Dimension)
		if err != nil {
			return 0, false, fmt.Errorf("create generation: %w", err)
		}
		_, _ = fmt.Fprintf(opts.Stderr, "Building generation %d (%s:%d).\n",
			gen, opts.Model, opts.Dimension)
		return gen, true, nil
	}

	active, err := vector.ResolveActiveForFingerprint(ctx, backend, opts.Fingerprint)
	switch {
	case err == nil:
		_, _ = fmt.Fprintf(opts.Stderr, "Using active generation %d (%s).\n", active.ID, active.Fingerprint)
		return active.ID, false, nil
	case errors.Is(err, vector.ErrIndexBuilding):
		building, bErr := backend.BuildingGeneration(ctx)
		if bErr != nil {
			return 0, false, fmt.Errorf("lookup building generation: %w", bErr)
		}
		if building == nil {
			// ResolveActive said building but no row — race with a
			// concurrent Retire/Activate. Fall through to the error.
			return 0, false, fmt.Errorf("resolve active generation: %w (hint: run with --full-rebuild to start)", err)
		}
		if building.Fingerprint != opts.Fingerprint {
			return 0, false, fmt.Errorf("in-progress rebuild has fingerprint=%q, config has %q — activate or retire it before running with a different model",
				building.Fingerprint, opts.Fingerprint)
		}
		_, _ = fmt.Fprintf(opts.Stderr, "Resuming building generation %d (%s).\n",
			building.ID, building.Fingerprint)
		return building.ID, true, nil
	default:
		return 0, false, fmt.Errorf("resolve active generation: %w (hint: run with --full-rebuild to start)", err)
	}
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
