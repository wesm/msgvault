package vector

import "errors"

// Sentinel errors used across the vector package. Callers should use
// errors.Is to check for these.
var (
	// ErrNotEnabled is returned when vector search is requested but
	// [vector] is not configured.
	ErrNotEnabled = errors.New("vector search not enabled")

	// ErrIndexStale is returned when the configured model/dimension
	// differs from the active generation's fingerprint.
	ErrIndexStale = errors.New("index stale: configured model does not match active generation")

	// ErrIndexBuilding is returned when no active generation exists and
	// a first-ever rebuild is in progress.
	ErrIndexBuilding = errors.New("index building: no active generation yet")

	// ErrNoActiveGeneration is returned internally when no generation is
	// in state='active'. Usually surfaced as ErrNotEnabled or ErrIndexBuilding.
	ErrNoActiveGeneration = errors.New("no active generation")

	// ErrDimensionMismatch is returned when a query or chunk vector has
	// a dimension different from the index.
	ErrDimensionMismatch = errors.New("dimension mismatch")

	// ErrPaginationUnsupported is returned for page>1 in vector/hybrid modes.
	ErrPaginationUnsupported = errors.New("pagination not supported for this mode")

	// ErrUnknownGeneration is returned when a caller references a
	// generation ID that does not exist in index_generations.
	ErrUnknownGeneration = errors.New("unknown generation")

	// ErrBuildingInProgress is returned when CreateGeneration is called
	// while another generation is already being built with a different
	// fingerprint, so the caller can surface an actionable message
	// instead of a raw unique-index violation.
	ErrBuildingInProgress = errors.New("a rebuild with a different fingerprint is already in progress")

	// ErrGenerationNotBuilding is returned by EnsureSeeded when the
	// target generation is no longer in state='building' — e.g. a
	// concurrent activation flipped it to active, or a retire call
	// moved it to retired, between the caller's BuildingGeneration
	// read and EnsureSeeded. Callers performing a resume can treat
	// this as a retryable race and re-resolve the active/building
	// state instead of aborting.
	ErrGenerationNotBuilding = errors.New("generation is not in state=building")

	// ErrEmbeddingTimeout is returned by the hybrid engine when the
	// embedding endpoint did not respond before the request context
	// was cancelled (typically because the HTTP server's per-request
	// timeout elapsed first). Callers should map this to a 503-style
	// "transient backend slow" response so clients can retry instead
	// of treating it as a permanent failure.
	ErrEmbeddingTimeout = errors.New("embedding request timed out")
)
