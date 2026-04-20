package scheduler

import (
	"context"
	"errors"
	"log/slog"

	"github.com/wesm/msgvault/internal/vector"
	"github.com/wesm/msgvault/internal/vector/embed"
)

// EmbedRunner is the subset of *embed.Worker that EmbedJob needs.
// Tests satisfy it with a fake.
type EmbedRunner interface {
	RunOnce(ctx context.Context, gen vector.GenerationID) (embed.RunResult, error)
	ReclaimStale(ctx context.Context) (int, error)
}

// EmbedJob runs the vector-embedding worker. Each invocation picks
// the active generation (or the building generation during first
// build), reclaims stale claims, and drains the queue via RunOnce.
// Errors are logged, not propagated, so cron can keep firing.
type EmbedJob struct {
	Worker  EmbedRunner
	Backend vector.Backend
	Log     *slog.Logger
}

// Run executes one embed cycle. Safe to call from cron or as a
// post-sync hook. Returns immediately when vector search has no
// pending work (no active and no building generation).
func (j *EmbedJob) Run(ctx context.Context) {
	if j == nil || j.Worker == nil || j.Backend == nil {
		return
	}
	log := j.Log
	if log == nil {
		log = slog.Default()
	}

	if _, err := j.Worker.ReclaimStale(ctx); err != nil {
		log.Warn("embed reclaim failed", "error", err)
	}

	var target vector.GenerationID
	active, err := j.Backend.ActiveGeneration(ctx)
	switch {
	case err == nil:
		target = active.ID
	case errors.Is(err, vector.ErrNoActiveGeneration):
		bg, bgErr := j.Backend.BuildingGeneration(ctx)
		if bgErr != nil {
			log.Warn("embed: building generation lookup failed", "error", bgErr)
			return
		}
		if bg == nil {
			return // nothing to do
		}
		target = bg.ID
	default:
		log.Warn("embed: active generation lookup failed", "error", err)
		return
	}

	res, err := j.Worker.RunOnce(ctx, target)
	if err != nil {
		log.Warn("embed run failed", "gen", target, "error", err)
		return
	}
	log.Info("embed run complete",
		"gen", target,
		"claimed", res.Claimed,
		"succeeded", res.Succeeded,
		"failed", res.Failed,
		"truncated", res.Truncated,
	)
}
