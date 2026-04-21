package vector

import (
	"context"
	"errors"
	"fmt"
)

// ResolveActive returns the active generation if its fingerprint matches
// the configured model+dimension. Error semantics:
//   - ErrIndexStale: an active generation exists but its fingerprint
//     does not match cfg.Fingerprint().
//   - ErrIndexBuilding: no active yet, but a first-ever build is in
//     progress.
//   - ErrNotEnabled: no generation exists at all (vector search not
//     initialized).
//
// Any other error from the Backend is wrapped and returned as-is.
func ResolveActive(ctx context.Context, b Backend, cfg EmbeddingsConfig) (Generation, error) {
	return ResolveActiveForFingerprint(ctx, b, cfg.Fingerprint())
}

// ResolveActiveForFingerprint is the same as ResolveActive but takes a
// pre-computed fingerprint string instead of a full EmbeddingsConfig.
// Useful for callers that already have a configured fingerprint (e.g.
// hybrid.Engine) and don't want to carry the whole config through.
func ResolveActiveForFingerprint(ctx context.Context, b Backend, fingerprint string) (Generation, error) {
	active, err := b.ActiveGeneration(ctx)
	if err == nil {
		if fingerprint != "" && active.Fingerprint != fingerprint {
			return Generation{}, fmt.Errorf("%w: active=%q configured=%q",
				ErrIndexStale, active.Fingerprint, fingerprint)
		}
		return active, nil
	}
	if !errors.Is(err, ErrNoActiveGeneration) {
		return Generation{}, fmt.Errorf("active generation: %w", err)
	}
	// No active generation. Check for a building one to distinguish
	// "first-time build" from "nothing configured".
	building, bErr := b.BuildingGeneration(ctx)
	if bErr != nil {
		return Generation{}, fmt.Errorf("building generation: %w", bErr)
	}
	if building != nil {
		return Generation{}, ErrIndexBuilding
	}
	return Generation{}, ErrNotEnabled
}
