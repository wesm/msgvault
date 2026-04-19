//go:build sqlite_vec

package sqlitevec

import (
	"errors"
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

func TestFusedSearch_FTSOnly_VectorScoreIsNaN(t *testing.T) {
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
		QueryVec:   nil, // FTS-only
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
	if math.IsNaN(hits[0].BM25Score) {
		t.Errorf("BM25Score should be present, got NaN")
	}
	if !math.IsNaN(hits[0].VectorScore) {
		t.Errorf("VectorScore should be NaN for FTS-only, got %v", hits[0].VectorScore)
	}
}

func TestFusedSearch_VectorOnly_BM25ScoreIsNaN(t *testing.T) {
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
		FTSQuery:   "",
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
	if !math.IsNaN(hits[0].BM25Score) {
		t.Errorf("BM25Score should be NaN for vector-only, got %v", hits[0].BM25Score)
	}
	if math.IsNaN(hits[0].VectorScore) {
		t.Errorf("VectorScore should be present, got NaN")
	}
}

func TestFusedSearch_NoSignals_Errors(t *testing.T) {
	b, ctx, _ := newFusedBackendForTest(t)
	gid := seedAndEmbed(t, b, map[int64][]float32{
		1: unitVec(768, 0),
	})
	_, err := b.FusedSearch(ctx, vector.FusedRequest{
		Generation: gid,
		KPerSignal: 10, Limit: 5, RRFK: 60,
	})
	if err == nil {
		t.Error("FusedSearch with no signals should error")
	}
}

func TestFusedSearch_UnknownGeneration(t *testing.T) {
	b, ctx, _ := newFusedBackendForTest(t)
	_, err := b.FusedSearch(ctx, vector.FusedRequest{
		FTSQuery:   "meeting",
		QueryVec:   unitVec(768, 0),
		Generation: vector.GenerationID(9999),
		KPerSignal: 10, Limit: 5, RRFK: 60,
	})
	if !errors.Is(err, vector.ErrUnknownGeneration) {
		t.Errorf("err = %v, want ErrUnknownGeneration", err)
	}
}

func TestFusedSearch_DimensionMismatch(t *testing.T) {
	b, ctx, _ := newFusedBackendForTest(t)
	gid := seedAndEmbed(t, b, map[int64][]float32{
		1: unitVec(768, 0),
	})
	_, err := b.FusedSearch(ctx, vector.FusedRequest{
		QueryVec:   unitVec(64, 0), // wrong dim
		Generation: gid,
		KPerSignal: 10, Limit: 5, RRFK: 60,
	})
	if !errors.Is(err, vector.ErrDimensionMismatch) {
		t.Errorf("err = %v, want ErrDimensionMismatch", err)
	}
}
