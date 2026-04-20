package cmd

import (
	"github.com/wesm/msgvault/internal/sync"
	"github.com/wesm/msgvault/internal/vector"
	"github.com/wesm/msgvault/internal/vector/embed"
	"github.com/wesm/msgvault/internal/vector/hybrid"
)

// vectorFeatures carries the optional vector-search components that the
// serve daemon and mcp command need to wire into their servers. It is
// populated only when cfg.Vector.Enabled is true AND the binary is
// built with -tags sqlite_vec; otherwise setupVectorFeatures returns
// (nil, nil) or a clear error.
type vectorFeatures struct {
	Backend      vector.Backend
	HybridEngine *hybrid.Engine
	Enqueuer     sync.EmbedEnqueuer
	Worker       *embed.Worker
	Cfg          vector.Config
	// Close releases the underlying vectors.db handle. Daemon and mcp
	// callers must invoke Close during shutdown.
	Close func() error
}
