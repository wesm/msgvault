package store

import "testing"

func TestEscapeLike(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"plain text", "hello", "hello"},
		{"percent", "100% done", `100\% done`},
		{"underscore", "file_name", `file\_name`},
		{"backslash", `path\to`, `path\\to`},
		{"all special", `50%_off\sale`, `50\%\_off\\sale`},
		{"empty", "", ""},
		{"multiple percents", "%%", `\%\%`},
		{"adjacent specials", `%_\`, `\%\_\\`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := escapeLike(tt.input)
			if got != tt.want {
				t.Errorf("escapeLike(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}
