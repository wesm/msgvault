package oauth

import (
	"testing"
)

func TestScopesToString(t *testing.T) {
	tests := []struct {
		name   string
		scopes []string
		want   string
	}{
		{
			name:   "empty scopes",
			scopes: []string{},
			want:   "",
		},
		{
			name:   "single scope",
			scopes: []string{"https://www.googleapis.com/auth/gmail.readonly"},
			want:   "https://www.googleapis.com/auth/gmail.readonly",
		},
		{
			name:   "multiple scopes",
			scopes: []string{"https://www.googleapis.com/auth/gmail.readonly", "https://www.googleapis.com/auth/gmail.modify"},
			want:   "https://www.googleapis.com/auth/gmail.readonly https://www.googleapis.com/auth/gmail.modify",
		},
		{
			name:   "three scopes",
			scopes: []string{"scope1", "scope2", "scope3"},
			want:   "scope1 scope2 scope3",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := scopesToString(tt.scopes)
			if got != tt.want {
				t.Errorf("scopesToString() = %q, want %q", got, tt.want)
			}
		})
	}
}
