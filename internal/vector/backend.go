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
// in hybrid search. Values are pre-resolved to IDs at the Go layer
// (addresses → participant IDs, labels → label IDs) so backend code
// only deals in integers.
//
// Semantics match the existing SQLite search path (internal/store/api.go,
// internal/query/sqlite.go):
//
//   - SenderIDs is single-valued: the message's sender_id (or its
//     `from` recipient row, for legacy rows where sender_id is NULL)
//     must appear in this set. Repeated `from:` tokens collapse to
//     the participants matching ALL substrings (intersection at the
//     participant lookup), since each message has one sender.
//   - To/Cc/Bcc/LabelGroups are multi-valued AND-of-OR groups: each
//     inner slice is one search-token resolution (substring → matching
//     IDs), and the message must have at least one matching recipient
//     OR label in EVERY group. A query like `to:alice to:bob` becomes
//     two ToGroups; the message must have a `to` recipient matching
//     "alice" AND a `to` recipient matching "bob".
//   - SubjectSubstrings each add one `m.subject LIKE ? ESCAPE '\'`
//     condition, ANDed together (all substrings must match).
//   - After/Before are half-open against m.sent_at:
//     `>= After` and `< Before`.
//   - LargerThan/SmallerThan compare against m.size_estimate.
type Filter struct {
	SourceIDs         []int64   // from [server/sources].identifier; empty = no source filter
	SenderIDs         []int64   // participant IDs for `from:` — intersected across tokens
	ToGroups          [][]int64 // one inner slice per `to:` token; AND across, OR within
	CcGroups          [][]int64 // one inner slice per `cc:` token; AND across, OR within
	BccGroups         [][]int64 // one inner slice per `bcc:` token; AND across, OR within
	LabelGroups       [][]int64 // one inner slice per `label:` token; AND across, OR within
	HasAttachment     *bool
	After, Before     *time.Time
	LargerThan        *int64   // `larger:` — strictly greater than
	SmallerThan       *int64   // `smaller:` — strictly less than
	SubjectSubstrings []string // one per `subject:` term (ANDed)
}

// IsEmpty reports whether the filter has no restrictions. A zero-value
// Filter is empty and backends should skip filter resolution entirely.
func (f Filter) IsEmpty() bool {
	return len(f.SourceIDs) == 0 &&
		len(f.SenderIDs) == 0 &&
		len(f.ToGroups) == 0 &&
		len(f.CcGroups) == 0 &&
		len(f.BccGroups) == 0 &&
		len(f.LabelGroups) == 0 &&
		f.HasAttachment == nil &&
		f.After == nil &&
		f.Before == nil &&
		f.LargerThan == nil &&
		f.SmallerThan == nil &&
		len(f.SubjectSubstrings) == 0
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
//
// FusedSearch returns the RRF-ordered hits, a saturation flag, and
// any error. saturated is true when either the BM25 or the ANN
// per-signal pool produced KPerSignal candidates — the pool hit its
// cap, and the final result set has truncated potentially-relevant
// hits. Callers surface this to clients as pool_saturated so the
// user can raise KPerSignal or narrow the query.
type FusingBackend interface {
	Backend
	FusedSearch(ctx context.Context, req FusedRequest) (hits []FusedHit, saturated bool, err error)
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
