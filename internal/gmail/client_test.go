package gmail

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
)

// Gmail API error reason constants for tests.
const (
	reasonRateLimitExceeded     = "rateLimitExceeded"
	reasonUserRateLimitExceeded = "userRateLimitExceeded"
	reasonRateLimitExceededUC   = "RATE_LIMIT_EXCEEDED"
	reasonForbidden             = "forbidden"
	quotaExceededMsg            = "Quota exceeded for quota metric 'Queries'"
)

// GmailErrorBuilder constructs Gmail API error response JSON for tests.
type GmailErrorBuilder struct {
	code    int
	message string
	reasons []string
	details []string
}

// NewGmailError starts building a Gmail API error with the given HTTP status code.
func NewGmailError(code int) *GmailErrorBuilder {
	return &GmailErrorBuilder{code: code}
}

// WithMessage sets the error message.
func (b *GmailErrorBuilder) WithMessage(msg string) *GmailErrorBuilder {
	b.message = msg
	return b
}

// WithReason adds an entry to the errors[].reason array.
func (b *GmailErrorBuilder) WithReason(reason string) *GmailErrorBuilder {
	b.reasons = append(b.reasons, reason)
	return b
}

// WithDetail adds an entry to the details[].reason array.
func (b *GmailErrorBuilder) WithDetail(reason string) *GmailErrorBuilder {
	b.details = append(b.details, reason)
	return b
}

// toReasonMaps converts a string slice into a slice of {"reason": s} maps.
func toReasonMaps(items []string) []map[string]string {
	if len(items) == 0 {
		return nil
	}
	out := make([]map[string]string, len(items))
	for i, item := range items {
		out[i] = map[string]string{"reason": item}
	}
	return out
}

// Build serializes the error to JSON bytes.
func (b *GmailErrorBuilder) Build() []byte {
	inner := map[string]any{"code": b.code}
	if b.message != "" {
		inner["message"] = b.message
	}
	if errs := toReasonMaps(b.reasons); errs != nil {
		inner["errors"] = errs
	}
	if dets := toReasonMaps(b.details); dets != nil {
		inner["details"] = dets
	}
	data, err := json.Marshal(map[string]any{"error": inner})
	if err != nil {
		panic(fmt.Sprintf("failed to marshal test body: %v", err))
	}
	return data
}

func TestIsRateLimitError(t *testing.T) {
	tests := []struct {
		name string
		body []byte
		want bool
	}{
		{
			name: "RateLimitExceeded",
			body: NewGmailError(http.StatusForbidden).WithReason(reasonRateLimitExceeded).Build(),
			want: true,
		},
		{
			name: "RateLimitExceededByMessage",
			body: NewGmailError(http.StatusForbidden).WithMessage(quotaExceededMsg).WithReason(reasonRateLimitExceeded).Build(),
			want: true,
		},
		{
			name: "RateLimitExceededUpperCase",
			body: NewGmailError(http.StatusForbidden).WithDetail(reasonRateLimitExceededUC).Build(),
			want: true,
		},
		{
			name: "QuotaExceeded",
			body: NewGmailError(http.StatusForbidden).WithMessage(quotaExceededMsg).Build(),
			want: true,
		},
		{
			name: "UserRateLimitExceeded",
			body: NewGmailError(http.StatusForbidden).WithReason(reasonUserRateLimitExceeded).Build(),
			want: true,
		},
		{
			name: "PermissionDenied",
			body: NewGmailError(http.StatusForbidden).WithReason(reasonForbidden).Build(),
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
