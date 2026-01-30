package gmail

import (
	"encoding/json"
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
	b, _ := json.Marshal(map[string]any{"error": inner})
	return b
}

func TestIsRateLimitError(t *testing.T) {
	tests := []struct {
		name string
		body []byte
		want bool
	}{
		{
			name: "RateLimitExceeded",
			body: gmailErrorBody(403, quotaExceededMsg,
				[]map[string]string{{"reason": "rateLimitExceeded"}}, nil),
			want: true,
		},
		{
			name: "RateLimitExceededUpperCase",
			body: gmailErrorBody(403, "",
				nil, []map[string]string{{"reason": "RATE_LIMIT_EXCEEDED"}}),
			want: true,
		},
		{
			name: "QuotaExceeded",
			body: gmailErrorBody(403, quotaExceededMsg, nil, nil),
			want: true,
		},
		{
			name: "UserRateLimitExceeded",
			body: gmailErrorBody(403, "",
				[]map[string]string{{"reason": "userRateLimitExceeded"}}, nil),
			want: true,
		},
		{
			name: "PermissionDenied",
			body: gmailErrorBody(403, "The caller does not have permission",
				[]map[string]string{{"reason": "forbidden"}}, nil),
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
