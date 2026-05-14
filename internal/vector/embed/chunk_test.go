package embed

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func TestChunkText(t *testing.T) {
	t.Run("EmptyInputReturnsNil", func(t *testing.T) {
		if got := ChunkText("", 100, 10, 0); got != nil {
			t.Errorf("got %v, want nil", got)
		}
	})

	t.Run("ShortInputReturnsSingleSpan", func(t *testing.T) {
		got := ChunkText("hello world", 100, 10, 0)
		if len(got) != 1 {
			t.Fatalf("len = %d, want 1", len(got))
		}
		if got[0].Text != "hello world" {
			t.Errorf("Text = %q, want %q", got[0].Text, "hello world")
		}
		if got[0].CharStart != 0 || got[0].CharEnd != 11 {
			t.Errorf("span = [%d,%d), want [0,11)", got[0].CharStart, got[0].CharEnd)
		}
	})

	t.Run("MaxRunesZeroDisablesChunking", func(t *testing.T) {
		text := strings.Repeat("x", 1000)
		got := ChunkText(text, 0, 10, 0)
		if len(got) != 1 || got[0].Text != text {
			t.Errorf("expected single span covering whole text")
		}
	})

	t.Run("CutsAtParagraphBreakInBackQuarter", func(t *testing.T) {
		// Build 100 chars of text where a "\n\n" sits at offset 80.
		// The back quarter of a 100-rune window starts at offset 75,
		// so the cut should land at the paragraph break (offset 82,
		// right after "\n\n").
		first := strings.Repeat("a", 80)
		second := strings.Repeat("b", 50)
		text := first + "\n\n" + second
		got := ChunkText(text, 100, 10, 0)
		if len(got) < 2 {
			t.Fatalf("expected >= 2 chunks, got %d", len(got))
		}
		if got[0].CharEnd != 82 {
			t.Errorf("first chunk ends at %d, want 82 (right after \\n\\n)", got[0].CharEnd)
		}
		if !strings.HasSuffix(got[0].Text, "\n\n") {
			t.Errorf("first chunk should end with paragraph break; got %q", got[0].Text[len(got[0].Text)-5:])
		}
	})

	t.Run("CutsAtSentenceBoundaryWhenNoParagraph", func(t *testing.T) {
		first := strings.Repeat("a", 80)
		// Sentence terminator at offset 80 ("end. ").
		text := first + ". " + strings.Repeat("b", 50)
		got := ChunkText(text, 100, 10, 0)
		if len(got) < 2 {
			t.Fatalf("expected >= 2 chunks")
		}
		// findSoftBreak returns the index *after* ". " (so 82).
		if got[0].CharEnd != 82 {
			t.Errorf("first chunk ends at %d, want 82", got[0].CharEnd)
		}
	})

	t.Run("CutsAtWordBoundaryWhenNoSentence", func(t *testing.T) {
		// Construct a 100-rune window where the back quarter has only
		// a space (no sentence terminator). Cut should land at the
		// last space inside [75, 100).
		text := strings.Repeat("a", 90) + " " + strings.Repeat("b", 50)
		got := ChunkText(text, 100, 10, 0)
		if len(got) < 2 {
			t.Fatalf("expected >= 2 chunks")
		}
		if got[0].CharEnd != 91 {
			t.Errorf("first chunk ends at %d, want 91 (one past space at 90)", got[0].CharEnd)
		}
	})

	t.Run("HardCutsWhenNoSoftBreakInBackQuarter", func(t *testing.T) {
		// 1000 unbroken non-space chars; no soft break anywhere. Each
		// window should land on the hard cut at maxRunes.
		text := strings.Repeat("a", 1000)
		got := ChunkText(text, 100, 0, 0)
		if len(got) != 10 {
			t.Fatalf("len = %d, want 10", len(got))
		}
		for i, s := range got {
			if s.CharEnd-s.CharStart != 100 {
				t.Errorf("chunk %d: %d runes, want 100", i, s.CharEnd-s.CharStart)
			}
		}
	})

	t.Run("OverlapBetweenConsecutiveChunks", func(t *testing.T) {
		text := strings.Repeat("a", 300)
		got := ChunkText(text, 100, 20, 0)
		if len(got) < 2 {
			t.Fatalf("expected >= 2 chunks")
		}
		// With overlap=20, the second chunk should start 80 runes
		// after the first chunk's start.
		if got[1].CharStart != 80 {
			t.Errorf("chunk[1] starts at %d, want 80", got[1].CharStart)
		}
	})

	t.Run("OverlapClampedToHalfWindow", func(t *testing.T) {
		// overlap >= maxRunes would mean cursor never advances —
		// the function must clamp it. With maxRunes=100 and
		// overlap=500, effective overlap should be 50.
		text := strings.Repeat("a", 300)
		got := ChunkText(text, 100, 500, 0)
		if len(got) == 0 {
			t.Fatal("got no chunks (overlap not clamped → infinite loop)")
		}
		// With effective overlap=50, chunk[1] starts at 50.
		if len(got) >= 2 && got[1].CharStart != 50 {
			t.Errorf("chunk[1] starts at %d, want 50", got[1].CharStart)
		}
	})

	t.Run("AllSpansHaveValidUTF8AndCorrectText", func(t *testing.T) {
		// Mixed-script input with multi-byte runes scattered through.
		var b strings.Builder
		for i := 0; i < 50; i++ {
			b.WriteString("Hello world. ")
			b.WriteString("こんにちは世界。")
		}
		text := b.String()
		got := ChunkText(text, 80, 10, 0)
		if len(got) < 2 {
			t.Fatalf("expected >= 2 chunks")
		}
		for i, s := range got {
			if !utf8.ValidString(s.Text) {
				t.Errorf("chunk %d: invalid UTF-8 in span text", i)
			}
			// Span text must match the substring derived from the
			// CharStart/CharEnd offsets — guards against off-by-one in
			// the byte/rune translation.
			runes := []rune(text)
			if s.CharStart < 0 || s.CharEnd > len(runes) || s.CharStart >= s.CharEnd {
				t.Errorf("chunk %d: invalid span [%d, %d)", i, s.CharStart, s.CharEnd)
				continue
			}
			expect := string(runes[s.CharStart:s.CharEnd])
			if s.Text != expect {
				t.Errorf("chunk %d: Text != runes[Start:End]", i)
			}
		}
	})

	t.Run("MaxSpansCapsOutputAndDropsTail", func(t *testing.T) {
		// A pathological input — 10× window with no soft breaks — would
		// normally chunk into 10 spans. With maxSpans=3 we get exactly 3
		// covering the head, and the tail beyond the third chunk's end
		// is dropped on the floor (real semantic search doesn't gain
		// from embedding system-generated dumps).
		text := strings.Repeat("a", 1000)
		got := ChunkText(text, 100, 0, 3)
		if len(got) != 3 {
			t.Fatalf("len = %d, want 3 (capped)", len(got))
		}
		if got[0].CharStart != 0 {
			t.Errorf("first chunk should start at 0, got %d", got[0].CharStart)
		}
		last := got[len(got)-1]
		if last.CharEnd >= 1000 {
			t.Errorf("last capped chunk ends at %d; expected dropped tail beyond it", last.CharEnd)
		}
	})

	t.Run("MaxSpansZeroIsUnlimited", func(t *testing.T) {
		text := strings.Repeat("a", 1000)
		got := ChunkText(text, 100, 0, 0)
		if len(got) != 10 {
			t.Errorf("len = %d, want 10 (no cap)", len(got))
		}
	})

	t.Run("MaxSpansLargerThanNaturalChunkCountIsNoop", func(t *testing.T) {
		text := strings.Repeat("a", 300)
		got := ChunkText(text, 100, 0, 100)
		if len(got) != 3 {
			t.Errorf("len = %d, want 3 (cap above natural)", len(got))
		}
	})

	t.Run("ConcatenationCoversInputModuloOverlap", func(t *testing.T) {
		// Stitching the chunks back together (advancing by stride =
		// window - overlap from each chunk's start) must reconstruct
		// the input verbatim. This is the property the overlap
		// guarantee depends on for recall.
		text := strings.Repeat("Lorem ipsum dolor sit amet. ", 200)
		spans := ChunkText(text, 200, 30, 0)
		if len(spans) < 2 {
			t.Fatalf("need >= 2 chunks to test stitching")
		}
		// Each chunk starts at spans[i].CharStart; the unique part of
		// chunk i (not seen in chunk i-1) starts at spans[i].CharStart
		// + overlap-with-prev. For correctness it's enough to verify
		// spans cover [0, totalRunes) end-to-end.
		if spans[0].CharStart != 0 {
			t.Errorf("first chunk should start at 0, got %d", spans[0].CharStart)
		}
		last := spans[len(spans)-1]
		runeCount := utf8.RuneCountInString(text)
		if last.CharEnd != runeCount {
			t.Errorf("last chunk ends at %d, want %d (end of text)", last.CharEnd, runeCount)
		}
		// Every gap between consecutive chunks must be <= window so
		// no input runes are dropped on the floor.
		for i := 1; i < len(spans); i++ {
			if spans[i].CharStart > spans[i-1].CharEnd {
				t.Errorf("chunks %d,%d leave a gap [%d, %d)",
					i-1, i, spans[i-1].CharEnd, spans[i].CharStart)
			}
		}
	})
}
