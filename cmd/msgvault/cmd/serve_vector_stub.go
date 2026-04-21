//go:build !sqlite_vec

package cmd

import (
	"context"
	"database/sql"
	"fmt"
)

// setupVectorFeatures is the no-sqlite-vec fallback. It returns
// (nil, nil) when vector search is disabled, and a descriptive error
// when the user enabled vector search in config but built the binary
// without -tags sqlite_vec.
func setupVectorFeatures(_ context.Context, _ *sql.DB, _ string) (*vectorFeatures, error) {
	if !cfg.Vector.Enabled {
		return nil, nil
	}
	return nil, fmt.Errorf(
		"vector search is enabled in config but this binary was built without -tags sqlite_vec; " +
			"rebuild with `make build` (or `go build -tags \"fts5 sqlite_vec\"`) " +
			"or set [vector] enabled = false")
}
