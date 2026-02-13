package cmd

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSanitizeExportTokenPath(t *testing.T) {
	tokensDir := "/data/tokens"

	tests := []struct {
		name  string
		email string
		want  string
	}{
		{
			"normal email",
			"user@gmail.com",
			filepath.Join(tokensDir, "user@gmail.com.json"),
		},
		{
			"email with dots",
			"first.last@example.co.uk",
			filepath.Join(tokensDir, "first.last@example.co.uk.json"),
		},
		{
			"email with plus",
			"user+tag@gmail.com",
			filepath.Join(tokensDir, "user+tag@gmail.com.json"),
		},
		{
			"strips slashes",
			"user/evil@gmail.com",
			filepath.Join(tokensDir, "userevil@gmail.com.json"),
		},
		{
			"strips backslashes",
			"user\\evil@gmail.com",
			filepath.Join(tokensDir, "userevil@gmail.com.json"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := sanitizeExportTokenPath(tokensDir, tt.email)
			if got != tt.want {
				t.Errorf("sanitizeExportTokenPath(%q) = %q, want %q", tt.email, got, tt.want)
			}
		})
	}
}

func TestEmailValidation(t *testing.T) {
	tests := []struct {
		name    string
		email   string
		wantErr bool
	}{
		{"normal email", "user@gmail.com", false},
		{"dotted local", "first.last@example.com", false},
		{"dotted domain", "user@mail.example.co.uk", false},
		{"plus tag", "user+tag@gmail.com", false},
		{"missing @", "usergmail.com", true},
		{"missing dot", "user@localhost", true},
		{"path traversal slash", "user/@gmail.com", true},
		{"path traversal backslash", "user\\@gmail.com", true},
		{"double dot traversal", "user@../evil.com", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateExportEmail(tt.email)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateExportEmail(%q) error = %v, wantErr %v",
					tt.email, err, tt.wantErr)
			}
		})
	}
}

func TestResolveParam(t *testing.T) {
	tests := []struct {
		name      string
		flag      string
		envKey    string
		envVal    string
		configVal string
		want      string
	}{
		{"flag wins over all", "from-flag", "TEST_RESOLVE_1", "from-env", "from-config", "from-flag"},
		{"env wins over config", "", "TEST_RESOLVE_2", "from-env", "from-config", "from-env"},
		{"config as fallback", "", "TEST_RESOLVE_3", "", "from-config", "from-config"},
		{"all empty", "", "TEST_RESOLVE_4", "", "", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.envVal != "" {
				t.Setenv(tt.envKey, tt.envVal)
			}
			got := resolveParam(tt.flag, tt.envKey, tt.configVal)
			if got != tt.want {
				t.Errorf("resolveParam(%q, %q, %q) = %q, want %q",
					tt.flag, tt.envKey, tt.configVal, got, tt.want)
			}
		})
	}
}

// newTestExporter creates a tokenExporter backed by the given httptest server.
func newTestExporter(srv *httptest.Server, tokensDir string) *tokenExporter {
	return &tokenExporter{
		httpClient: srv.Client(),
		tokensDir:  tokensDir,
		stdout:     io.Discard,
		stderr:     io.Discard,
	}
}

// writeTestToken writes a fake token file and returns the email used.
func writeTestToken(t *testing.T, tokensDir, email, content string) {
	t.Helper()
	if err := os.MkdirAll(tokensDir, 0700); err != nil {
		t.Fatalf("mkdir tokens: %v", err)
	}
	path := filepath.Join(tokensDir, email+".json")
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatalf("write token: %v", err)
	}
}

func TestExport_UploadSuccess(t *testing.T) {
	var gotPath string
	var gotBody []byte
	var gotAPIKey string

	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasPrefix(r.URL.Path, "/api/v1/auth/token/"):
			gotPath = r.URL.Path
			gotAPIKey = r.Header.Get("X-API-Key")
			gotBody, _ = io.ReadAll(r.Body)
			w.WriteHeader(http.StatusCreated)
		case r.URL.Path == "/api/v1/accounts":
			w.WriteHeader(http.StatusCreated)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	tokensDir := t.TempDir()
	writeTestToken(t, tokensDir, "user@gmail.com", `{"token":"secret"}`)

	e := newTestExporter(srv, tokensDir)
	result, err := e.export("user@gmail.com", srv.URL, "my-key", false)
	if err != nil {
		t.Fatalf("export error = %v", err)
	}

	// httptest decodes percent-encoding in r.URL.Path, so we see the
	// decoded form even though url.PathEscape encodes @ on the wire.
	if gotPath != "/api/v1/auth/token/user@gmail.com" {
		t.Errorf("path = %q, want /api/v1/auth/token/user@gmail.com", gotPath)
	}

	// Verify API key header
	if gotAPIKey != "my-key" {
		t.Errorf("X-API-Key = %q, want my-key", gotAPIKey)
	}

	// Verify token body
	if string(gotBody) != `{"token":"secret"}` {
		t.Errorf("body = %q, want token content", string(gotBody))
	}

	// Verify result
	if result.remoteURL != srv.URL {
		t.Errorf("result.remoteURL = %q, want %q", result.remoteURL, srv.URL)
	}
	if result.apiKey != "my-key" {
		t.Errorf("result.apiKey = %q, want my-key", result.apiKey)
	}
}

func TestExport_UploadFailure(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("server error"))
	}))
	defer srv.Close()

	tokensDir := t.TempDir()
	writeTestToken(t, tokensDir, "user@gmail.com", `{"token":"secret"}`)

	e := newTestExporter(srv, tokensDir)
	_, err := e.export("user@gmail.com", srv.URL, "key", false)
	if err == nil {
		t.Fatal("export should fail on 500")
	}
	if !strings.Contains(err.Error(), "500") {
		t.Errorf("error = %q, want mention of 500", err.Error())
	}
	if !strings.Contains(err.Error(), "server error") {
		t.Errorf("error = %q, want 'server error'", err.Error())
	}
}

func TestExport_MissingToken(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Error("server should not be called when token is missing")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	e := newTestExporter(srv, t.TempDir())
	_, err := e.export("nobody@gmail.com", srv.URL, "key", false)
	if err == nil {
		t.Fatal("export should fail with missing token")
	}
	if !strings.Contains(err.Error(), "no token found") {
		t.Errorf("error = %q, want 'no token found'", err.Error())
	}
}

func TestExport_HTTPSRequired(t *testing.T) {
	e := &tokenExporter{
		httpClient: http.DefaultClient,
		tokensDir:  t.TempDir(),
		stdout:     io.Discard,
		stderr:     io.Discard,
	}

	_, err := e.export("user@gmail.com", "http://nas:8080", "key", false)
	if err == nil {
		t.Fatal("export should reject http:// without allowInsecure")
	}
	if !strings.Contains(err.Error(), "HTTPS required") {
		t.Errorf("error = %q, want 'HTTPS required'", err.Error())
	}
}

func TestExport_HTTPAllowedWithInsecure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/api/v1/auth/token/") {
			w.WriteHeader(http.StatusCreated)
			return
		}
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()

	tokensDir := t.TempDir()
	writeTestToken(t, tokensDir, "user@gmail.com", `{"token":"data"}`)

	e := &tokenExporter{
		httpClient: srv.Client(),
		tokensDir:  tokensDir,
		stdout:     io.Discard,
		stderr:     io.Discard,
	}

	result, err := e.export("user@gmail.com", srv.URL, "key", true)
	if err != nil {
		t.Fatalf("export error = %v", err)
	}
	if !result.allowInsecure {
		t.Error("result.allowInsecure should be true")
	}
}

func TestExport_HTTPWarning(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/api/v1/auth/token/") {
			w.WriteHeader(http.StatusCreated)
			return
		}
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()

	tokensDir := t.TempDir()
	writeTestToken(t, tokensDir, "user@gmail.com", `{"token":"data"}`)

	var stderr bytes.Buffer
	e := &tokenExporter{
		httpClient: srv.Client(),
		tokensDir:  tokensDir,
		stdout:     io.Discard,
		stderr:     &stderr,
	}

	_, err := e.export("user@gmail.com", srv.URL, "key", true)
	if err != nil {
		t.Fatalf("export error = %v", err)
	}
	if !strings.Contains(stderr.String(), "WARNING") {
		t.Errorf("stderr = %q, want HTTP warning", stderr.String())
	}
}

func TestExport_InvalidEmail(t *testing.T) {
	e := &tokenExporter{
		httpClient: http.DefaultClient,
		tokensDir:  t.TempDir(),
		stdout:     io.Discard,
		stderr:     io.Discard,
	}

	_, err := e.export("not-an-email", "https://nas:8080", "key", false)
	if err == nil {
		t.Fatal("export should reject invalid email")
	}
	if !strings.Contains(err.Error(), "invalid email") {
		t.Errorf("error = %q, want 'invalid email'", err.Error())
	}
}

func TestExport_AccountPostSuccess(t *testing.T) {
	var accountEmail string
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasPrefix(r.URL.Path, "/api/v1/auth/token/"):
			w.WriteHeader(http.StatusCreated)
		case r.URL.Path == "/api/v1/accounts":
			var body map[string]interface{}
			_ = json.NewDecoder(r.Body).Decode(&body)
			if email, ok := body["email"].(string); ok {
				accountEmail = email
			}
			w.WriteHeader(http.StatusCreated)
		}
	}))
	defer srv.Close()

	tokensDir := t.TempDir()
	writeTestToken(t, tokensDir, "user@gmail.com", `{}`)

	e := newTestExporter(srv, tokensDir)
	_, err := e.export("user@gmail.com", srv.URL, "key", false)
	if err != nil {
		t.Fatalf("export error = %v", err)
	}

	if accountEmail != "user@gmail.com" {
		t.Errorf("account email = %q, want user@gmail.com", accountEmail)
	}
}

func TestExport_AccountPostFailureIsNonFatal(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/api/v1/auth/token/") {
			w.WriteHeader(http.StatusCreated)
			return
		}
		// Account POST fails
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("db error"))
	}))
	defer srv.Close()

	tokensDir := t.TempDir()
	writeTestToken(t, tokensDir, "user@gmail.com", `{}`)

	var stderr bytes.Buffer
	e := &tokenExporter{
		httpClient: srv.Client(),
		tokensDir:  tokensDir,
		stdout:     io.Discard,
		stderr:     &stderr,
	}

	// Should succeed — account POST is best-effort
	result, err := e.export("user@gmail.com", srv.URL, "key", false)
	if err != nil {
		t.Fatalf("export should succeed even when account POST fails: %v", err)
	}
	if result == nil {
		t.Fatal("result should not be nil")
	}
	if !strings.Contains(stderr.String(), "Warning") {
		t.Errorf("stderr = %q, want warning about account POST failure", stderr.String())
	}
}

func TestExport_InvalidScheme(t *testing.T) {
	e := &tokenExporter{
		httpClient: http.DefaultClient,
		tokensDir:  t.TempDir(),
		stdout:     io.Discard,
		stderr:     io.Discard,
	}

	_, err := e.export("user@gmail.com", "ftp://nas:8080", "key", false)
	if err == nil {
		t.Fatal("export should reject ftp:// scheme")
	}
	if !strings.Contains(err.Error(), "http or https") {
		t.Errorf("error = %q, want mention of http or https", err.Error())
	}
}
