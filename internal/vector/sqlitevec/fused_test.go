//go:build sqlite_vec

package sqlitevec

import (
	"math"
	"testing"

	"github.com/wesm/msgvault/internal/vector"
)

func TestFusedSearch_BothSignalsContribute(t *testing.T) {
	b, ctx, _ := newFusedBackendForTest(t)
	gid := seedAndEmbed(t, b, map[int64][]float32{
		1: unitVec(768, 0),
		2: unitVec(768, 1),
		3: unitVec(768, 2),
	})
	if err := b.ActivateGeneration(ctx, gid); err != nil {
		t.Fatalf("ActivateGeneration: %v", err)
	}

	req := vector.FusedRequest{
		FTSQuery:   "meeting",
		QueryVec:   unitVec(768, 1),
		Generation: gid,
		KPerSignal: 10,
		Limit:      5,
		RRFK:       60,
	}
	hits, err := b.FusedSearch(ctx, req)
	if err != nil {
		t.Fatalf("FusedSearch: %v", err)
	}
	if len(hits) == 0 || hits[0].MessageID != 2 {
		t.Fatalf("expected msg 2 at top, got %+v", hits)
	}
	if math.IsNaN(hits[0].BM25Score) || math.IsNaN(hits[0].VectorScore) {
		t.Errorf("top hit should have both scores, got %+v", hits[0])
	}
}
