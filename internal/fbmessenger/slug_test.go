package fbmessenger

import (
	"strings"
	"testing"
)

func TestSlug(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"Wes McKinney", "wes.mckinney"},
		{"Marie-Ève Côté", "marie.eve.cote"},
		{"  Alice  ", "alice"},
		{"alice@example.com", "alice.example.com"},
		{"小明", ""},
		{"", ""},
	}
	for _, c := range cases {
		if got := Slug(c.in); got != c.want {
			t.Errorf("Slug(%q)=%q want %q", c.in, got, c.want)
		}
	}
}

func TestAddressFallbackForEmptySlug(t *testing.T) {
	a := Address("小明")
	if !strings.HasPrefix(a.Email, "user.") || !strings.HasSuffix(a.Email, "@facebook.messenger") {
		t.Fatalf("unexpected fallback email: %q", a.Email)
	}
	if Address("小明").Email != a.Email {
		t.Fatal("fallback must be deterministic across calls")
	}
	if a.Domain != "facebook.messenger" {
		t.Fatalf("domain=%q", a.Domain)
	}
	if a.Name != "小明" {
		t.Fatalf("display name must be preserved unaltered, got %q", a.Name)
	}
}

func TestAddressRegular(t *testing.T) {
	a := Address("Wes McKinney")
	if a.Email != "wes.mckinney@facebook.messenger" {
		t.Fatalf("email=%q", a.Email)
	}
	if a.Name != "Wes McKinney" {
		t.Fatalf("name=%q", a.Name)
	}
	if a.Domain != "facebook.messenger" {
		t.Fatalf("domain=%q", a.Domain)
	}
}

func TestDecodeMojibake(t *testing.T) {
	// "é" (U+00E9) encoded as UTF-8 is bytes 0xC3 0xA9. Interpreted as
	// Latin-1, those are runes U+00C3 U+00A9, which Facebook then emits
	// as JSON. DecodeMojibake must reverse that.
	in := "caf\u00c3\u00a9"
	want := "café"
	if got := DecodeMojibake(in); got != want {
		t.Fatalf("DecodeMojibake(%q)=%q want %q", in, got, want)
	}
	// Non-Latin-1 input must round-trip unchanged.
	if got := DecodeMojibake("正常"); got != "正常" {
		t.Fatalf("non-Latin-1 round-trip failed: got %q", got)
	}
	// ASCII round-trips unchanged.
	if got := DecodeMojibake("hello"); got != "hello" {
		t.Fatalf("ascii round-trip: got %q", got)
	}
}

func TestStripDomain(t *testing.T) {
	if got := StripDomain("wes@facebook.messenger"); got != "wes" {
		t.Fatalf("got %q", got)
	}
	if got := StripDomain("wes"); got != "wes" {
		t.Fatalf("got %q", got)
	}
}
