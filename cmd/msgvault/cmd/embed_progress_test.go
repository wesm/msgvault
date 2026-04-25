package cmd

import (
	"math"
	"testing"
	"time"
)

func TestRateWindow_EmptyReturnsZero(t *testing.T) {
	w := newRateWindow(10)
	if r := w.Rate(); r != 0 {
		t.Fatalf("empty Rate: got %v, want 0", r)
	}
	if n := w.Samples(); n != 0 {
		t.Fatalf("empty Samples: got %d, want 0", n)
	}
}

func TestRateWindow_PartialFill(t *testing.T) {
	w := newRateWindow(10)
	w.Add(50, 1*time.Second)
	w.Add(100, 1*time.Second)
	if got, want := w.Samples(), 2; got != want {
		t.Fatalf("Samples: got %d, want %d", got, want)
	}
	// sum(msgs)/sum(seconds) = 150/2 = 75
	if r := w.Rate(); math.Abs(r-75) > 0.01 {
		t.Fatalf("Rate: got %v, want ~75", r)
	}
}

func TestRateWindow_EvictsOldestOnceFull(t *testing.T) {
	w := newRateWindow(3)
	// Fill with low-rate samples.
	w.Add(10, 1*time.Second) // 10 msg/s
	w.Add(10, 1*time.Second)
	w.Add(10, 1*time.Second)
	if r := w.Rate(); math.Abs(r-10) > 0.01 {
		t.Fatalf("pre-eviction Rate: got %v, want 10", r)
	}
	// Push a high-rate sample; oldest 10/1 should fall out.
	w.Add(1000, 1*time.Second)
	// Now window holds three samples: 10, 10, 1000 over 3s -> 340 msg/s.
	if got, want := w.Samples(), 3; got != want {
		t.Fatalf("Samples after eviction: got %d, want %d", got, want)
	}
	if r := w.Rate(); math.Abs(r-340) > 0.01 {
		t.Fatalf("post-eviction Rate: got %v, want 340", r)
	}
}

func TestRateWindow_WeightedNotMeanOfPerBatchRates(t *testing.T) {
	// Picking sizes that distinguish weighted from mean-of-rates.
	//   Batch A: 1 msg in 10s   -> 0.1 msg/s
	//   Batch B: 100 msgs in 1s -> 100 msg/s
	//   Mean of rates: 50.05
	//   Weighted: 101 / 11 = ~9.18
	w := newRateWindow(2)
	w.Add(1, 10*time.Second)
	w.Add(100, 1*time.Second)
	got := w.Rate()
	want := 101.0 / 11.0
	if math.Abs(got-want) > 0.01 {
		t.Fatalf("weighted Rate: got %v, want ~%v (must NOT equal mean-of-rates ~50)", got, want)
	}
}

func TestRateWindow_ZeroElapsedSampleSkipped(t *testing.T) {
	w := newRateWindow(10)
	w.Add(10, 1*time.Second)
	w.Add(50, 0) // skipped: would divide by zero
	w.Add(20, 1*time.Second)
	if got, want := w.Samples(), 2; got != want {
		t.Fatalf("Samples: got %d (zero-elapsed should be skipped), want %d", got, want)
	}
	// 30 msgs over 2s = 15.
	if r := w.Rate(); math.Abs(r-15) > 0.01 {
		t.Fatalf("Rate: got %v, want 15", r)
	}
}

func TestRateWindow_NegativeElapsedSampleSkipped(t *testing.T) {
	w := newRateWindow(10)
	w.Add(10, 1*time.Second)
	w.Add(50, -1*time.Second) // pathological: skip
	if got, want := w.Samples(), 1; got != want {
		t.Fatalf("Samples: got %d, want %d", got, want)
	}
	// 10 msgs over 1s = 10 — confirms the negative sample didn't slip
	// into the running totals (a regression where it did would either
	// pull totalElapsed to zero or leave a stale msg count behind).
	if r := w.Rate(); math.Abs(r-10) > 0.01 {
		t.Fatalf("Rate: got %v, want 10", r)
	}
}

func TestNewRateWindow_NormalizesNonPositiveCap(t *testing.T) {
	w := newRateWindow(0)
	// Should not panic on Add.
	w.Add(1, 1*time.Second)
	if got := w.Samples(); got < 1 {
		t.Fatalf("Samples after Add on zero-cap window: got %d, want >= 1", got)
	}
}
