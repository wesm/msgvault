package gmail

import (
	"encoding/json"
	"fmt"
	"testing"
)

const quotaExceededMsg = "Quota exceeded for quota metric 'Queries'"

// gmailErrorBody builds a Gmail API error response JSON body.
// Optional fields (message, errors, details) are included only when non-zero.
func gmailErrorBody(code int, message string, errors []map[string]string, details []map[string]string) []byte {
	inner := map[string]any{"code": code}
	if message != "" {
		inner["message"] = message
	}
	if errors != nil {
		inner["errors"] = errors
	}
	if details != nil {
		inner["details"] = details
	}
	b, err := json.Marshal(map[string]any{"error": inner})
	if err != nil {
		panic(fmt.Sprintf("failed to marshal test body: %v", err))
	}
	return b
}

func errorWithReason(reason string) []byte {
	return gmailErrorBody(403, "", []map[string]string{{"reason": reason}}, nil)
}

func errorWithDetail(reason string) []byte {
	return gmailErrorBody(403, "", nil, []map[string]string{{"reason": reason}})
}

func TestIsRateLimitError(t *testing.T) {
	tests := []struct {
		name string
		body []byte
		want bool
	}{
		{
			name: "RateLimitExceeded",
			body: errorWithReason("rateLimitExceeded"),
			want: true,
		},
		{
			name: "RateLimitExceededByMessage",
			body: gmailErrorBody(403, quotaExceededMsg, []map[string]string{{"reason": "rateLimitExceeded"}}, nil),
			want: true,
		},
		{
			name: "RateLimitExceededUpperCase",
			body: errorWithDetail("RATE_LIMIT_EXCEEDED"),
			want: true,
		},
		{
			name: "QuotaExceeded",
			body: gmailErrorBody(403, quotaExceededMsg, nil, nil),
			want: true,
		},
		{
			name: "UserRateLimitExceeded",
			body: errorWithReason("userRateLimitExceeded"),
			want: true,
		},
		{
			name: "PermissionDenied",
			body: errorWithReason("forbidden"),
			want: false,
		},
		{
			name: "EmptyBody",
			body: []byte{},
			want: false,
		},
		{
			name: "InvalidJSON",
			body: []byte("not valid json but contains rateLimitExceeded"),
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isRateLimitError(tt.body); got != tt.want {
				t.Errorf("isRateLimitError() = %v, want %v", got, tt.want)
			}
		})
	}
}
