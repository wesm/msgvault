package vector

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

// fakeBackend implements Backend for ResolveActive tests. Only
// ActiveGeneration and BuildingGeneration are exercised.
type fakeBackend struct {
	active    *Generation
	building  *Generation
	activeErr error
	buildErr  error
}

func (f *fakeBackend) CreateGeneration(context.Context, string, int) (GenerationID, error) {
	return 0, errors.New("not implemented")
}
func (f *fakeBackend) ActivateGeneration(context.Context, GenerationID) error {
	return errors.New("not implemented")
}
func (f *fakeBackend) RetireGeneration(context.Context, GenerationID) error {
	return errors.New("not implemented")
}
func (f *fakeBackend) ActiveGeneration(context.Context) (Generation, error) {
	if f.activeErr != nil {
		return Generation{}, f.activeErr
	}
	if f.active == nil {
		return Generation{}, ErrNoActiveGeneration
	}
	return *f.active, nil
}
func (f *fakeBackend) BuildingGeneration(context.Context) (*Generation, error) {
	if f.buildErr != nil {
		return nil, f.buildErr
	}
	return f.building, nil
}
func (f *fakeBackend) Upsert(context.Context, GenerationID, []Chunk) error {
	return errors.New("not implemented")
}
func (f *fakeBackend) Search(context.Context, GenerationID, []float32, int, Filter) ([]Hit, error) {
	return nil, errors.New("not implemented")
}
func (f *fakeBackend) Delete(context.Context, GenerationID, []int64) error {
	return errors.New("not implemented")
}
func (f *fakeBackend) Stats(context.Context, GenerationID) (Stats, error) {
	return Stats{}, errors.New("not implemented")
}
func (f *fakeBackend) LoadVector(context.Context, int64) ([]float32, error) {
	return nil, errors.New("not implemented")
}
func (f *fakeBackend) Close() error { return nil }
func (f *fakeBackend) EnsureSeeded(context.Context, GenerationID) error {
	return errors.New("not implemented")
}

func TestResolveActive_Matches(t *testing.T) {
	b := &fakeBackend{active: &Generation{ID: 1, Fingerprint: "m:768"}}
	g, err := ResolveActive(context.Background(), b, EmbeddingsConfig{Model: "m", Dimension: 768})
	if err != nil {
		t.Fatalf("ResolveActive: %v", err)
	}
	if g.Fingerprint != "m:768" {
		t.Errorf("fingerprint = %q, want m:768", g.Fingerprint)
	}
	if g.ID != 1 {
		t.Errorf("ID = %d, want 1", g.ID)
	}
}

func TestResolveActive_Stale(t *testing.T) {
	b := &fakeBackend{active: &Generation{Fingerprint: "m:768"}}
	_, err := ResolveActive(context.Background(), b, EmbeddingsConfig{Model: "m", Dimension: 1024})
	if !errors.Is(err, ErrIndexStale) {
		t.Errorf("err = %v, want ErrIndexStale", err)
	}
}

func TestResolveActive_NoneAndBuildingReturnsBuildingError(t *testing.T) {
	b := &fakeBackend{building: &Generation{ID: 42, Fingerprint: "m:768"}}
	_, err := ResolveActive(context.Background(), b, EmbeddingsConfig{Model: "m", Dimension: 768})
	if !errors.Is(err, ErrIndexBuilding) {
		t.Errorf("err = %v, want ErrIndexBuilding", err)
	}
}

func TestResolveActive_NothingReturnsNotEnabled(t *testing.T) {
	b := &fakeBackend{}
	_, err := ResolveActive(context.Background(), b, EmbeddingsConfig{Model: "m", Dimension: 768})
	if !errors.Is(err, ErrNotEnabled) {
		t.Errorf("err = %v, want ErrNotEnabled", err)
	}
}

func TestResolveActive_BackendError(t *testing.T) {
	wantErr := fmt.Errorf("db down")
	b := &fakeBackend{activeErr: wantErr}
	_, err := ResolveActive(context.Background(), b, EmbeddingsConfig{Model: "m", Dimension: 768})
	if err == nil || !errors.Is(err, wantErr) {
		t.Errorf("err = %v, want wraps db down", err)
	}
}

func TestResolveActive_BuildingBackendError(t *testing.T) {
	wantErr := fmt.Errorf("building failed")
	b := &fakeBackend{buildErr: wantErr}
	_, err := ResolveActive(context.Background(), b, EmbeddingsConfig{Model: "m", Dimension: 768})
	if err == nil || !errors.Is(err, wantErr) {
		t.Errorf("err = %v, want wraps building failed", err)
	}
}
