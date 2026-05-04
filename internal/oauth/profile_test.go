package oauth

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"golang.org/x/oauth2"
)

func TestFetchTokenProfileEmail(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		body       string
		wantEmail  string
		wantErr    string
		wantMis    bool
	}{
		{
			name:       "happy path",
			statusCode: http.StatusOK,
			body:       `{"emailAddress":"user@gmail.com"}`,
			wantEmail:  "user@gmail.com",
		},
		{
			name:       "mismatch",
			statusCode: http.StatusOK,
			body:       `{"emailAddress":"other@gmail.com"}`,
			wantErr:    "token mismatch",
			wantMis:    true,
		},
		{
			name:       "http error",
			statusCode: http.StatusUnauthorized,
			body:       `{"error":"invalid credentials"}`,
			wantErr:    "gmail API returned HTTP 401",
		},
		{
			name:       "decode failure",
			statusCode: http.StatusOK,
			body:       `not json`,
			wantErr:    "parse profile",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if got := r.Header.Get("Authorization"); got != "Bearer test-token" {
					t.Errorf("Authorization = %q, want Bearer test-token", got)
				}
				w.WriteHeader(tt.statusCode)
				_, _ = fmt.Fprint(w, tt.body)
			}))
			defer srv.Close()

			ts := oauth2.StaticTokenSource(&oauth2.Token{
				AccessToken: "test-token",
				TokenType:   "Bearer",
			})
			got, err := fetchTokenProfileEmail(
				context.Background(),
				ts,
				srv.URL,
				"user@gmail.com",
				tokenProfileErrorServiceAccount,
			)
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("error = nil, want %q", tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("error = %v, want %q", err, tt.wantErr)
				}
				var mismatch *TokenMismatchError
				if errors.As(err, &mismatch) != tt.wantMis {
					t.Fatalf("TokenMismatchError presence = %v, want %v", errors.As(err, &mismatch), tt.wantMis)
				}
				return
			}
			if err != nil {
				t.Fatalf("fetchTokenProfileEmail: %v", err)
			}
			if got != tt.wantEmail {
				t.Errorf("email = %q, want %q", got, tt.wantEmail)
			}
		})
	}
}
