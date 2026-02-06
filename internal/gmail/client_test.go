package gmail

import (
	"encoding/base64"
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

func TestDecodeBase64URL(t *testing.T) {
	// Test data: "Hello, World!" in various encodings
	plaintext := []byte("Hello, World!")
	// base64url unpadded (Gmail's typical format)
	unpadded := base64.RawURLEncoding.EncodeToString(plaintext)
	// base64url with padding
	padded := base64.URLEncoding.EncodeToString(plaintext)

	tests := []struct {
		name    string
		input   string
		want    []byte
		wantErr bool
	}{
		{
			name:    "unpadded base64url",
			input:   unpadded,
			want:    plaintext,
			wantErr: false,
		},
		{
			name:    "padded base64url",
			input:   padded,
			want:    plaintext,
			wantErr: false,
		},
		{
			name:    "empty string",
			input:   "",
			want:    []byte{},
			wantErr: false,
		},
		{
			name:    "single byte unpadded",
			input:   "QQ", // 'A'
			want:    []byte("A"),
			wantErr: false,
		},
		{
			name:    "single byte padded",
			input:   "QQ==", // 'A' with padding
			want:    []byte("A"),
			wantErr: false,
		},
		{
			name:    "two bytes unpadded",
			input:   "QUI", // 'AB'
			want:    []byte("AB"),
			wantErr: false,
		},
		{
			name:    "two bytes padded",
			input:   "QUI=", // 'AB' with single pad
			want:    []byte("AB"),
			wantErr: false,
		},
		{
			name:    "URL-safe characters unpadded",
			input:   "PDw_Pz4-", // "<<??>>", uses - and _ instead of + and /
			want:    []byte("<<??>>"),
			wantErr: false,
		},
		{
			name:    "URL-safe underscore unpadded",
			input:   "Pz8_", // "???" is exactly 3 bytes -> 4 chars, no padding needed
			want:    []byte("???"),
			wantErr: false,
		},
		{
			name:    "URL-safe dash unpadded",
			input:   "Pj4-", // ">>>" is exactly 3 bytes -> 4 chars, no padding needed
			want:    []byte(">>>"),
			wantErr: false,
		},
		{
			name:    "URL-safe dash with padding (1 byte)",
			input:   "-A==", // 0xf8 - exercises URLEncoding path with URL-safe char
			want:    []byte{0xf8},
			wantErr: false,
		},
		{
			name:    "URL-safe underscore with padding (1 byte)",
			input:   "_w==", // 0xff - exercises URLEncoding path with URL-safe char
			want:    []byte{0xff},
			wantErr: false,
		},
		{
			name:    "URL-safe dash with single pad (2 bytes)",
			input:   "A-A=", // 0x03 0xe0 - exercises URLEncoding with single = and URL-safe char
			want:    []byte{0x03, 0xe0},
			wantErr: false,
		},
		{
			name:    "invalid characters",
			input:   "!!!invalid!!!",
			want:    nil,
			wantErr: true,
		},
		{
			name:    "malformed padding single char with equals",
			input:   "A=", // Invalid: 1 char before padding is never valid
			want:    nil,
			wantErr: true,
		},
		{
			name:    "malformed padding excess equals",
			input:   "QQ===", // Invalid: too many padding chars
			want:    nil,
			wantErr: true,
		},
		{
			name:    "malformed padding wrong count",
			input:   "QUI==", // Invalid: "AB" should have single =, not ==
			want:    nil,
			wantErr: true,
		},
		{
			name:    "binary data unpadded",
			input:   base64.RawURLEncoding.EncodeToString([]byte{0x00, 0xFF, 0x80, 0x7F}),
			want:    []byte{0x00, 0xFF, 0x80, 0x7F},
			wantErr: false,
		},
		{
			name:    "binary data padded",
			input:   base64.URLEncoding.EncodeToString([]byte{0x00, 0xFF, 0x80, 0x7F}),
			want:    []byte{0x00, 0xFF, 0x80, 0x7F},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := decodeBase64URL(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("decodeBase64URL() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && string(got) != string(tt.want) {
				t.Errorf("decodeBase64URL() = %v, want %v", got, tt.want)
			}
		})
	}
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
