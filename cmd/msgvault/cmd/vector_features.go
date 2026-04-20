package cmd

import (
	"github.com/wesm/msgvault/internal/sync"
	"github.com/wesm/msgvault/internal/vector"
	"github.com/wesm/msgvault/internal/vector/embed"
	"github.com/wesm/msgvault/internal/vector/hybrid"
)

// vectorFeatures carries the optional vector-search components that the
// serve, mcp, sync, and sync-full commands wire into their servers and
// sync pipelines. It is populated only when cfg.Vector.Enabled is true
// AND the binary is built with -tags sqlite_vec; otherwise
// setupVectorFeatures returns (nil, nil) or a clear error.
//
// When non-nil, all fields are populated (invariant enforced by
// setupVectorFeatures). Callers only need to nil-check vf itself.
type vectorFeatures struct {
	Backend      vector.Backend
	HybridEngine *hybrid.Engine
	Enqueuer     sync.EmbedEnqueuer
	Worker       *embed.Worker
	Cfg          vector.Config
	// Close releases the underlying vectors.db handle. Every caller
	// that receives a non-nil vectorFeatures must invoke Close during
	// shutdown so WAL checkpoints complete.
	Close func() error
}
