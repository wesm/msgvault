package vector

import (
	"context"
	"time"
)

// GenerationID identifies one index generation.
type GenerationID int64

// GenerationState is one of: building, active, retired.
type GenerationState string

const (
	GenerationBuilding GenerationState = "building"
	GenerationActive   GenerationState = "active"
	GenerationRetired  GenerationState = "retired"
)

// Generation describes an index generation — a complete corpus
// embedding under one model+dimension.
type Generation struct {
	ID           GenerationID
	Model        string
	Dimension    int
	Fingerprint  string // Model:Dimension
	State        GenerationState
	StartedAt    time.Time
	CompletedAt  *time.Time
	ActivatedAt  *time.Time
	MessageCount int64
}

// Chunk is a pre-computed embedding to persist in the index. In MVP
// there is one chunk per message; multi-chunk support (§13 future
// work) would extend this with a chunk sequence id.
type Chunk struct {
	MessageID     int64
	Vector        []float32
	SourceCharLen int
	Truncated     bool
}

// Filter carries the structured filters pushed into both signal CTEs
// in hybrid search. Values are pre-resolved to IDs at the Go layer.
type Filter struct {
	SourceIDs     []int64 // from [server/sources].identifier; empty = no source filter
	SenderIDs     []int64 // from participants.email_address
	LabelIDs      []int64 // from labels.name
	HasAttachment *bool
	After, Before *time.Time
}

// IsEmpty reports whether the filter has no restrictions. A zero-value
// Filter is empty and backends should skip filter resolution entirely.
func (f Filter) IsEmpty() bool {
	return len(f.SourceIDs) == 0 && len(f.SenderIDs) == 0 && len(f.LabelIDs) == 0 &&
		f.HasAttachment == nil && f.After == nil && f.Before == nil
}

// Hit is one search result.
type Hit struct {
	MessageID int64
	Score     float64 // backend-native score
	Rank      int     // 1-based rank within this signal
}

// Stats reports the size of one generation (or 0 for totals).
type Stats struct {
	EmbeddingCount int64
	PendingCount   int64
	StorageBytes   int64
}

// Backend is the minimum contract a vector store must implement.
type Backend interface {
	CreateGeneration(ctx context.Context, model string, dimension int) (GenerationID, error)
	ActivateGeneration(ctx context.Context, gen GenerationID) error
	RetireGeneration(ctx context.Context, gen GenerationID) error

	// ActiveGeneration returns the current active generation, or
	// ErrNoActiveGeneration if none exists.
	ActiveGeneration(ctx context.Context) (Generation, error)
	BuildingGeneration(ctx context.Context) (*Generation, error)

	Upsert(ctx context.Context, gen GenerationID, chunks []Chunk) error
	Search(ctx context.Context, gen GenerationID, queryVec []float32, k int, filter Filter) ([]Hit, error)
	Delete(ctx context.Context, gen GenerationID, messageIDs []int64) error
	Stats(ctx context.Context, gen GenerationID) (Stats, error)

	// LoadVector returns the embedding for a specific message in the
	// active generation. Returns ErrNoActiveGeneration if none exists, or
	// a descriptive error if the message isn't embedded in the active
	// generation.
	LoadVector(ctx context.Context, messageID int64) ([]float32, error)

	Close() error
}

// FusingBackend is an optional capability implemented by backends that
// can fuse FTS5 + ANN in a single SQL query. The hybrid engine checks
// for this via type assertion.
type FusingBackend interface {
	Backend
	FusedSearch(ctx context.Context, req FusedRequest) ([]FusedHit, error)
}

// FusedRequest is the parameter bundle for a single-query fused hybrid search.
type FusedRequest struct {
	FTSQuery     string    // pre-tokenized FTS5 MATCH expression; empty skips BM25
	QueryVec     []float32 // query embedding; nil skips ANN
	Generation   GenerationID
	KPerSignal   int
	Limit        int
	RRFK         int
	SubjectBoost float64
	SubjectTerms []string // lowercased query terms used for subject-boost check
	Filter       Filter
}

// FusedHit is one result from a fused query. BM25Score/VectorScore are
// NaN when the message did not appear in that signal.
type FusedHit struct {
	MessageID      int64
	RRFScore       float64
	BM25Score      float64 // math.NaN() if missing
	VectorScore    float64 // math.NaN() if missing
	SubjectBoosted bool
}
