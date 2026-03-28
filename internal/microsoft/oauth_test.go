package microsoft

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"golang.org/x/oauth2"
)

func TestTokenPath(t *testing.T) {
	m := &Manager{tokensDir: "/tmp/tokens"}
	path := m.TokenPath("user@example.com")
	want := "/tmp/tokens/microsoft_user@example.com.json"
	if path != want {
		t.Errorf("TokenPath = %q, want %q", path, want)
	}
}

func TestSaveAndLoadToken(t *testing.T) {
	dir := t.TempDir()
	m := &Manager{tokensDir: dir}
	token := &oauth2.Token{
		AccessToken:  "access-123",
		RefreshToken: "refresh-456",
		TokenType:    "Bearer",
	}
	scopes := []string{"IMAP.AccessAsUser.All", "offline_access"}

	if err := m.saveToken("user@example.com", token, scopes, ""); err != nil {
		t.Fatal(err)
	}

	loaded, err := m.loadTokenFile("user@example.com")
	if err != nil {
		t.Fatal(err)
	}
	if loaded.AccessToken != "access-123" {
		t.Errorf("AccessToken = %q, want %q", loaded.AccessToken, "access-123")
	}
	if loaded.RefreshToken != "refresh-456" {
		t.Errorf("RefreshToken = %q, want %q", loaded.RefreshToken, "refresh-456")
	}
	if len(loaded.Scopes) != 2 {
		t.Errorf("Scopes len = %d, want 2", len(loaded.Scopes))
	}

	// Verify file permissions
	path := m.TokenPath("user@example.com")
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0600 {
		t.Errorf("permissions = %o, want 0600", info.Mode().Perm())
	}
}

func TestHasToken(t *testing.T) {
	dir := t.TempDir()
	m := &Manager{tokensDir: dir}

	if m.HasToken("nobody@example.com") {
		t.Error("HasToken should be false for non-existent token")
	}

	token := &oauth2.Token{AccessToken: "test"}
	if err := m.saveToken("user@example.com", token, nil, ""); err != nil {
		t.Fatal(err)
	}
	if !m.HasToken("user@example.com") {
		t.Error("HasToken should be true after save")
	}
}

func TestDeleteToken(t *testing.T) {
	dir := t.TempDir()
	m := &Manager{tokensDir: dir}

	token := &oauth2.Token{AccessToken: "test"}
	if err := m.saveToken("user@example.com", token, nil, ""); err != nil {
		t.Fatal(err)
	}
	if err := m.DeleteToken("user@example.com"); err != nil {
		t.Fatal(err)
	}
	if m.HasToken("user@example.com") {
		t.Error("HasToken should be false after delete")
	}
	// Delete non-existent should not error
	if err := m.DeleteToken("nobody@example.com"); err != nil {
		t.Errorf("DeleteToken non-existent: %v", err)
	}
}

func TestIsPersonalMicrosoftAccount(t *testing.T) {
	tests := []struct {
		email    string
		personal bool
	}{
		{"user@hotmail.com", true},
		{"user@outlook.com", true},
		{"user@live.com", true},
		{"user@msn.com", true},
		{"user@hotmail.co.uk", true},
		{"user@hotmail.co.jp", true},
		{"user@hotmail.com.au", true},
		{"user@hotmail.com.br", true},
		{"user@outlook.jp", true},
		{"user@outlook.kr", true},
		{"user@outlook.com.br", true},
		{"user@outlook.com.au", true},
		{"user@live.com.au", true},
		{"user@live.jp", true},
		{"user@company.com", false},
		{"user@5.life", false},
		{"user@gmail.com", false},
	}
	for _, tt := range tests {
		got := isPersonalMicrosoftAccount(tt.email)
		if got != tt.personal {
			t.Errorf("isPersonalMicrosoftAccount(%q) = %v, want %v", tt.email, got, tt.personal)
		}
	}
}

func TestScopesForEmail(t *testing.T) {
	orgScopes := scopesForEmail("user@company.com")
	if orgScopes[0] != ScopeIMAPOrg {
		t.Errorf("org scope = %q, want %q", orgScopes[0], ScopeIMAPOrg)
	}
	personalScopes := scopesForEmail("user@hotmail.com")
	if personalScopes[0] != ScopeIMAPPersonal {
		t.Errorf("personal scope = %q, want %q", personalScopes[0], ScopeIMAPPersonal)
	}
}

func TestSanitizeEmail(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"user@example.com", "user@example.com"},
		// / replaced first, then .. → _.._; double underscore before "evil"
		// because _.._  ends with _ and the original _evil starts with _.
		{"../evil", "_..__evil"},
		{"a/b", "a_b"},
		{"a\\b", "a_b"},
		// double dot in domain — sanitized in place
		{"user@sub..domain.com", "user@sub_.._domain.com"},
	}
	for _, tt := range tests {
		got := sanitizeEmail(tt.input)
		if got != tt.want {
			t.Errorf("sanitizeEmail(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestSanitizeEmail_NoPathTraversal(t *testing.T) {
	// None of these should produce a string containing a path separator or
	// result in a different filepath.Base (i.e. no directory component).
	inputs := []string{
		"../../etc/passwd",
		"../tokens/admin@example.com",
		"/etc/passwd",
		"C:\\Windows\\system32",
		"user@sub..domain.com",
		"....@example.com",
	}
	for _, input := range inputs {
		result := sanitizeEmail(input)
		if strings.ContainsAny(result, "/\\") {
			t.Errorf("sanitizeEmail(%q) = %q still contains path separator", input, result)
		}
		if result != filepath.Base(result) {
			t.Errorf("sanitizeEmail(%q) = %q has directory component (filepath.Base differs)", input, result)
		}
	}
}

// makeIDToken builds a minimal unsigned JWT with the given claims.
// Used in tests with verifyIDTokenFn to bypass OIDC signature validation.
func makeIDToken(claims map[string]any) string {
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"RS256","typ":"JWT"}`))
	payload, _ := json.Marshal(claims)
	body := base64.RawURLEncoding.EncodeToString(payload)
	return header + "." + body + ".fake-sig"
}

// testVerifyFn decodes an unsigned test JWT, bypassing OIDC validation.
// Only for use in tests via Manager.verifyIDTokenFn.
func testVerifyFn(_ context.Context, rawIDToken string) (*idTokenClaims, error) {
	parts := splitJWT(rawIDToken)
	if len(parts) != 3 {
		return nil, nil
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, err
	}
	var raw struct {
		Email             string `json:"email"`
		PreferredUsername string `json:"preferred_username"`
		TenantID          string `json:"tid"`
	}
	if err := json.Unmarshal(payload, &raw); err != nil {
		return nil, err
	}
	return &idTokenClaims{
		Email:             raw.Email,
		PreferredUsername: raw.PreferredUsername,
		TenantID:          raw.TenantID,
	}, nil
}

// splitJWT is a test helper to avoid importing strings just for Split.
func splitJWT(s string) []string {
	var parts []string
	for {
		idx := -1
		for i := 0; i < len(s); i++ {
			if s[i] == '.' {
				idx = i
				break
			}
		}
		if idx < 0 {
			parts = append(parts, s)
			break
		}
		parts = append(parts, s[:idx])
		s = s[idx+1:]
	}
	return parts
}

func TestPeekTIDFromJWT(t *testing.T) {
	idToken := makeIDToken(map[string]any{
		"email":              "user@example.com",
		"preferred_username": "user@tenant.onmicrosoft.com",
		"tid":                "some-tenant-id",
	})
	tid, err := peekTIDFromJWT(idToken)
	if err != nil {
		t.Fatal(err)
	}
	if tid != "some-tenant-id" {
		t.Errorf("tid = %q, want %q", tid, "some-tenant-id")
	}
}

func TestImapScopeForTenant(t *testing.T) {
	if got := imapScopeForTenant(MicrosoftConsumerTenantID); got != ScopeIMAPPersonal {
		t.Errorf("consumer tenant: got %q, want %q", got, ScopeIMAPPersonal)
	}
	if got := imapScopeForTenant("some-org-tenant-id"); got != ScopeIMAPOrg {
		t.Errorf("org tenant: got %q, want %q", got, ScopeIMAPOrg)
	}
}

func TestResolveTokenEmail_Match(t *testing.T) {
	m := &Manager{
		clientID:        "test-client",
		tenantID:        "common",
		tokensDir:       t.TempDir(),
		logger:          slog.Default(),
		verifyIDTokenFn: testVerifyFn,
	}
	idToken := makeIDToken(map[string]any{"email": "user@example.com", "tid": "org-tid"})
	token := (&oauth2.Token{AccessToken: "test-token", TokenType: "Bearer"}).
		WithExtra(map[string]any{"id_token": idToken})

	actual, claims, err := m.resolveTokenEmail(t.Context(), "user@example.com", token, "test-nonce")
	if err != nil {
		t.Fatal(err)
	}
	if actual != "user@example.com" {
		t.Errorf("actual = %q, want %q", actual, "user@example.com")
	}
	if claims.TenantID != "org-tid" {
		t.Errorf("TenantID = %q, want %q", claims.TenantID, "org-tid")
	}
}

func TestResolveTokenEmail_Mismatch(t *testing.T) {
	m := &Manager{
		clientID:        "test-client",
		tenantID:        "common",
		tokensDir:       t.TempDir(),
		verifyIDTokenFn: testVerifyFn,
	}
	idToken := makeIDToken(map[string]any{"email": "other@example.com"})
	token := (&oauth2.Token{AccessToken: "test-token", TokenType: "Bearer"}).
		WithExtra(map[string]any{"id_token": idToken})

	_, _, err := m.resolveTokenEmail(t.Context(), "user@example.com", token, "test-nonce")
	if err == nil {
		t.Fatal("expected error for mismatch")
	}
	if _, ok := err.(*TokenMismatchError); !ok {
		t.Errorf("expected *TokenMismatchError, got %T: %v", err, err)
	}
}

func TestResolveTokenEmail_FallbackToUPN(t *testing.T) {
	// Some accounts omit "email" and only have "preferred_username".
	m := &Manager{
		clientID:        "test-client",
		tenantID:        "common",
		tokensDir:       t.TempDir(),
		logger:          slog.Default(),
		verifyIDTokenFn: testVerifyFn,
	}
	idToken := makeIDToken(map[string]any{"preferred_username": "user@example.com"})
	token := (&oauth2.Token{AccessToken: "test-token", TokenType: "Bearer"}).
		WithExtra(map[string]any{"id_token": idToken})

	actual, _, err := m.resolveTokenEmail(t.Context(), "user@example.com", token, "test-nonce")
	if err != nil {
		t.Fatal(err)
	}
	if actual != "user@example.com" {
		t.Errorf("actual = %q, want %q", actual, "user@example.com")
	}
}

func TestResolveTokenEmail_UPNDiffersFromExpected(t *testing.T) {
	// When "email" claim is absent and UPN differs from expected address,
	// resolveTokenEmail should accept the user-entered email (not the UPN)
	// because Entra UPN can legitimately differ from the SMTP mailbox address.
	m := &Manager{
		clientID:        "test-client",
		tenantID:        "common",
		tokensDir:       t.TempDir(),
		logger:          slog.Default(),
		verifyIDTokenFn: testVerifyFn,
	}
	idToken := makeIDToken(map[string]any{
		"preferred_username": "john.doe@company.onmicrosoft.com",
		"tid":                "org-tenant-id",
	})
	token := (&oauth2.Token{AccessToken: "test-token", TokenType: "Bearer"}).
		WithExtra(map[string]any{"id_token": idToken})

	actual, claims, err := m.resolveTokenEmail(t.Context(), "john@company.com", token, "test-nonce")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if actual != "john@company.com" {
		t.Errorf("actual = %q, want user-entered email %q", actual, "john@company.com")
	}
	if claims.TenantID != "org-tenant-id" {
		t.Errorf("TenantID = %q, want %q", claims.TenantID, "org-tenant-id")
	}
}

func TestResolveTokenEmail_EmailClaimMismatchStillErrors(t *testing.T) {
	// When the authoritative "email" claim IS present but doesn't match,
	// it should still error (user authenticated the wrong account).
	m := &Manager{
		clientID:        "test-client",
		tenantID:        "common",
		tokensDir:       t.TempDir(),
		verifyIDTokenFn: testVerifyFn,
	}
	idToken := makeIDToken(map[string]any{
		"email":              "wrong@other.com",
		"preferred_username": "john@company.com",
	})
	token := (&oauth2.Token{AccessToken: "test-token", TokenType: "Bearer"}).
		WithExtra(map[string]any{"id_token": idToken})

	_, _, err := m.resolveTokenEmail(t.Context(), "john@company.com", token, "test-nonce")
	if err == nil {
		t.Fatal("expected TokenMismatchError when email claim is wrong")
	}
	if _, ok := err.(*TokenMismatchError); !ok {
		t.Errorf("expected *TokenMismatchError, got %T: %v", err, err)
	}
}

func TestAuthorize_ScopeCorrection(t *testing.T) {
	// Simulate: user@custom-domain.com guessed as org, but tid reveals consumer.
	// The browser flow should be called twice: once with org scope, once with personal.
	dir := t.TempDir()
	m := &Manager{
		clientID:        "test-client",
		tenantID:        "common",
		tokensDir:       dir,
		logger:          slog.Default(),
		verifyIDTokenFn: testVerifyFn,
	}

	consumerTID := MicrosoftConsumerTenantID
	callCount := 0

	m.browserFlowFn = func(ctx context.Context, email string, scopes []string) (*oauth2.Token, string, error) {
		callCount++
		idToken := makeIDToken(map[string]any{
			"email": "user@custom-domain.com",
			"tid":   consumerTID,
		})
		switch callCount {
		case 1:
			// First call: should have org scope (domain-based guess).
			if scopes[0] != ScopeIMAPOrg {
				t.Errorf("first call scope = %q, want org scope", scopes[0])
			}
		case 2:
			// Second call: should have personal scope (corrected via tid).
			if scopes[0] != ScopeIMAPPersonal {
				t.Errorf("second call scope = %q, want personal scope", scopes[0])
			}
		}
		tok := (&oauth2.Token{
			AccessToken:  "access-token",
			RefreshToken: "refresh-token",
			TokenType:    "Bearer",
		}).WithExtra(map[string]any{"id_token": idToken})
		return tok, "test-nonce", nil
	}

	if err := m.Authorize(t.Context(), "user@custom-domain.com"); err != nil {
		t.Fatal(err)
	}

	if callCount != 2 {
		t.Errorf("browserFlowFn called %d times, want 2", callCount)
	}

	// Verify saved scopes are personal (corrected).
	tf, err := m.loadTokenFile("user@custom-domain.com")
	if err != nil {
		t.Fatal(err)
	}
	if len(tf.Scopes) == 0 || tf.Scopes[0] != ScopeIMAPPersonal {
		t.Errorf("saved scopes[0] = %q, want %q", tf.Scopes[0], ScopeIMAPPersonal)
	}
}

func TestAuthorize_NoScopeCorrection(t *testing.T) {
	// When the domain guess matches tid, no correction should happen.
	// user@outlook.com → guessed personal, tid confirms consumer → no correction.
	dir := t.TempDir()
	m := &Manager{
		clientID:        "test-client",
		tenantID:        "common",
		tokensDir:       dir,
		logger:          slog.Default(),
		verifyIDTokenFn: testVerifyFn,
	}

	consumerTID := MicrosoftConsumerTenantID
	callCount := 0

	m.browserFlowFn = func(ctx context.Context, email string, scopes []string) (*oauth2.Token, string, error) {
		callCount++
		// Should already have personal scope.
		if scopes[0] != ScopeIMAPPersonal {
			t.Errorf("initial scope = %q, want personal scope", scopes[0])
		}
		idToken := makeIDToken(map[string]any{
			"email": "user@outlook.com",
			"tid":   consumerTID,
		})
		tok := (&oauth2.Token{
			AccessToken:  "access-token",
			RefreshToken: "refresh-token",
			TokenType:    "Bearer",
		}).WithExtra(map[string]any{"id_token": idToken})
		return tok, "test-nonce", nil
	}

	if err := m.Authorize(t.Context(), "user@outlook.com"); err != nil {
		t.Fatal(err)
	}

	if callCount != 1 {
		t.Errorf("browserFlowFn called %d times, want 1 (no correction needed)", callCount)
	}

	// Verify saved scopes are personal (no correction needed).
	tf, err := m.loadTokenFile("user@outlook.com")
	if err != nil {
		t.Fatal(err)
	}
	if len(tf.Scopes) == 0 || tf.Scopes[0] != ScopeIMAPPersonal {
		t.Errorf("saved scopes[0] = %q, want %q", tf.Scopes[0], ScopeIMAPPersonal)
	}
}

func TestAuthorize_PersistsTenantID(t *testing.T) {
	dir := t.TempDir()
	m := &Manager{
		clientID:        "test-client",
		tenantID:        "common",
		tokensDir:       dir,
		logger:          slog.Default(),
		verifyIDTokenFn: testVerifyFn,
	}

	m.browserFlowFn = func(ctx context.Context, email string, scopes []string) (*oauth2.Token, string, error) {
		idToken := makeIDToken(map[string]any{
			"email": "user@company.com",
			"tid":   "org-tenant-123",
		})
		tok := (&oauth2.Token{
			AccessToken:  "access-token",
			RefreshToken: "refresh-token",
			TokenType:    "Bearer",
		}).WithExtra(map[string]any{"id_token": idToken})
		return tok, "test-nonce", nil
	}

	if err := m.Authorize(t.Context(), "user@company.com"); err != nil {
		t.Fatal(err)
	}

	tf, err := m.loadTokenFile("user@company.com")
	if err != nil {
		t.Fatal(err)
	}
	if tf.TenantID != "org-tenant-123" {
		t.Errorf("TenantID = %q, want %q", tf.TenantID, "org-tenant-123")
	}
}

func TestTokenSource_StaleScopeReturnsError(t *testing.T) {
	dir := t.TempDir()
	m := &Manager{
		clientID:  "test-client",
		tenantID:  "common",
		tokensDir: dir,
		logger:    slog.Default(),
	}

	// Save a token with org IMAP scope but consumer tenant ID (stale).
	token := &oauth2.Token{
		AccessToken:  "access-token",
		RefreshToken: "refresh-token",
		TokenType:    "Bearer",
	}
	if err := m.saveToken("user@custom.com", token, []string{ScopeIMAPOrg, "offline_access"}, MicrosoftConsumerTenantID); err != nil {
		t.Fatal(err)
	}

	_, err := m.TokenSource(t.Context(), "user@custom.com")
	if err == nil {
		t.Fatal("expected error for stale scope")
	}
	if !strings.Contains(err.Error(), "stale IMAP scope") {
		t.Errorf("error = %q, want it to mention stale IMAP scope", err.Error())
	}
}

func TestTokenSource_CorrectScopeSucceeds(t *testing.T) {
	dir := t.TempDir()
	m := &Manager{
		clientID:  "test-client",
		tenantID:  "common",
		tokensDir: dir,
		logger:    slog.Default(),
	}

	// Save a token with correct personal IMAP scope and consumer tenant ID.
	token := &oauth2.Token{
		AccessToken:  "access-token",
		RefreshToken: "refresh-token",
		TokenType:    "Bearer",
	}
	if err := m.saveToken("user@outlook.com", token, []string{ScopeIMAPPersonal, "offline_access"}, MicrosoftConsumerTenantID); err != nil {
		t.Fatal(err)
	}

	ts, err := m.TokenSource(t.Context(), "user@outlook.com")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ts == nil {
		t.Fatal("TokenSource returned nil")
	}
}

func TestTokenSource_NoTenantIDSkipsValidation(t *testing.T) {
	// Pre-migration tokens without tenant_id should still work.
	dir := t.TempDir()
	m := &Manager{
		clientID:  "test-client",
		tenantID:  "common",
		tokensDir: dir,
		logger:    slog.Default(),
	}

	token := &oauth2.Token{
		AccessToken:  "access-token",
		RefreshToken: "refresh-token",
		TokenType:    "Bearer",
	}
	if err := m.saveToken("user@custom.com", token, []string{ScopeIMAPOrg, "offline_access"}, ""); err != nil {
		t.Fatal(err)
	}

	ts, err := m.TokenSource(t.Context(), "user@custom.com")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ts == nil {
		t.Fatal("TokenSource returned nil")
	}
}

func TestOAuthConfigWithTenant(t *testing.T) {
	m := &Manager{
		clientID: "test-client",
		tenantID: "common",
	}
	cfg := m.oauthConfigWithTenant("my-org", []string{"IMAP.AccessAsUser.All"})
	if !strings.Contains(cfg.Endpoint.AuthURL, "my-org") {
		t.Errorf("AuthURL = %q, want it to contain %q", cfg.Endpoint.AuthURL, "my-org")
	}
	if !strings.Contains(cfg.Endpoint.TokenURL, "my-org") {
		t.Errorf("TokenURL = %q, want it to contain %q", cfg.Endpoint.TokenURL, "my-org")
	}
	if strings.Contains(cfg.Endpoint.AuthURL, "common") {
		t.Errorf("AuthURL = %q, should not contain %q", cfg.Endpoint.AuthURL, "common")
	}
}

func TestTokenSource_PersistedTenantOverridesManager(t *testing.T) {
	dir := t.TempDir()
	m := &Manager{
		clientID:  "test-client",
		tenantID:  "common",
		tokensDir: dir,
		logger:    slog.Default(),
	}

	// Save token with a specific tenant ID (simulating post-authorization state).
	token := &oauth2.Token{
		AccessToken:  "access-token",
		RefreshToken: "refresh-token",
		TokenType:    "Bearer",
	}
	if err := m.saveToken("user@company.com", token, []string{ScopeIMAPOrg, "offline_access"}, "my-org-tenant"); err != nil {
		t.Fatal(err)
	}

	// TokenSource should succeed and use "my-org-tenant", not "common".
	ts, err := m.TokenSource(t.Context(), "user@company.com")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ts == nil {
		t.Fatal("TokenSource returned nil")
	}
}

func TestTokenSource_ConcurrentAccess(t *testing.T) {
	dir := t.TempDir()
	m := &Manager{
		clientID:  "test-client",
		tenantID:  "common",
		tokensDir: dir,
		logger:    slog.Default(),
	}

	token := &oauth2.Token{
		AccessToken:  "access-token",
		RefreshToken: "refresh-token",
		TokenType:    "Bearer",
	}
	if err := m.saveToken("user@outlook.com", token, []string{ScopeIMAPPersonal, "offline_access"}, MicrosoftConsumerTenantID); err != nil {
		t.Fatal(err)
	}

	fn, err := m.TokenSource(t.Context(), "user@outlook.com")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = fn(t.Context())
		}()
	}
	wg.Wait()
}

// --- IMAPHost ---

func TestIMAPHost_PersonalAccount(t *testing.T) {
	dir := t.TempDir()
	m := &Manager{tokensDir: dir}
	token := &oauth2.Token{AccessToken: "access", RefreshToken: "refresh"}
	if err := m.saveToken("user@hotmail.com", token, []string{ScopeIMAPPersonal, "offline_access"}, MicrosoftConsumerTenantID); err != nil {
		t.Fatal(err)
	}
	host, err := m.IMAPHost("user@hotmail.com")
	if err != nil {
		t.Fatal(err)
	}
	if host != "outlook.office.com" {
		t.Errorf("IMAPHost = %q, want %q", host, "outlook.office.com")
	}
}

func TestIMAPHost_OrgAccount(t *testing.T) {
	dir := t.TempDir()
	m := &Manager{tokensDir: dir}
	token := &oauth2.Token{AccessToken: "access", RefreshToken: "refresh"}
	if err := m.saveToken("user@company.com", token, []string{ScopeIMAPOrg, "offline_access"}, "org-tenant"); err != nil {
		t.Fatal(err)
	}
	host, err := m.IMAPHost("user@company.com")
	if err != nil {
		t.Fatal(err)
	}
	if host != "outlook.office365.com" {
		t.Errorf("IMAPHost = %q, want %q", host, "outlook.office365.com")
	}
}

func TestIMAPHost_NoToken(t *testing.T) {
	m := &Manager{tokensDir: t.TempDir()}
	_, err := m.IMAPHost("nobody@example.com")
	if err == nil {
		t.Fatal("expected error for missing token")
	}
}

// IMAPHost with no scopes saved falls back to org host (default).
func TestIMAPHost_NoScopesFallsBackToOrg(t *testing.T) {
	dir := t.TempDir()
	m := &Manager{tokensDir: dir}
	token := &oauth2.Token{AccessToken: "access"}
	if err := m.saveToken("user@company.com", token, nil, "org-tenant"); err != nil {
		t.Fatal(err)
	}
	host, err := m.IMAPHost("user@company.com")
	if err != nil {
		t.Fatal(err)
	}
	if host != "outlook.office365.com" {
		t.Errorf("IMAPHost = %q, want %q", host, "outlook.office365.com")
	}
}

// --- TokenSource edge cases ---

func TestTokenSource_MissingToken(t *testing.T) {
	m := &Manager{
		clientID:  "test-client",
		tenantID:  "common",
		tokensDir: t.TempDir(),
		logger:    slog.Default(),
	}
	_, err := m.TokenSource(t.Context(), "nobody@example.com")
	if err == nil {
		t.Fatal("expected error for missing token")
	}
	if !strings.Contains(err.Error(), "no valid token") {
		t.Errorf("error = %q, want it to mention 'no valid token'", err.Error())
	}
}

// Pre-migration tokens without scopes fall back to email-based scope detection.
func TestTokenSource_EmptyScopesFallback(t *testing.T) {
	dir := t.TempDir()
	m := &Manager{
		clientID:  "test-client",
		tenantID:  "common",
		tokensDir: dir,
		logger:    slog.Default(),
	}
	token := &oauth2.Token{AccessToken: "access", RefreshToken: "refresh", TokenType: "Bearer"}
	if err := m.saveToken("user@outlook.com", token, nil, ""); err != nil {
		t.Fatal(err)
	}
	ts, err := m.TokenSource(t.Context(), "user@outlook.com")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ts == nil {
		t.Fatal("TokenSource returned nil")
	}
}

// --- Authorize edge cases ---

func TestAuthorize_BrowserFlowError(t *testing.T) {
	m := &Manager{
		clientID:        "test-client",
		tenantID:        "common",
		tokensDir:       t.TempDir(),
		logger:          slog.Default(),
		verifyIDTokenFn: testVerifyFn,
	}
	wantErr := errors.New("user denied access")
	m.browserFlowFn = func(_ context.Context, _ string, _ []string) (*oauth2.Token, string, error) {
		return nil, "", wantErr
	}
	err := m.Authorize(t.Context(), "user@company.com")
	if !errors.Is(err, wantErr) {
		t.Errorf("Authorize error = %v, want %v", err, wantErr)
	}
}

func TestAuthorize_ContextCancelled(t *testing.T) {
	m := &Manager{
		clientID:        "test-client",
		tenantID:        "common",
		tokensDir:       t.TempDir(),
		logger:          slog.Default(),
		verifyIDTokenFn: testVerifyFn,
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	m.browserFlowFn = func(ctx context.Context, _ string, _ []string) (*oauth2.Token, string, error) {
		return nil, "", ctx.Err()
	}
	err := m.Authorize(ctx, "user@company.com")
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Authorize error = %v, want context.Canceled", err)
	}
}

// Scope correction triggers a second browser flow; a TokenMismatchError on
// that second flow should propagate rather than be swallowed.
func TestAuthorize_ScopeCorrectionMismatchOnReauth(t *testing.T) {
	m := &Manager{
		clientID:        "test-client",
		tenantID:        "common",
		tokensDir:       t.TempDir(),
		logger:          slog.Default(),
		verifyIDTokenFn: testVerifyFn,
	}
	callCount := 0
	m.browserFlowFn = func(_ context.Context, email string, _ []string) (*oauth2.Token, string, error) {
		callCount++
		var claimsEmail string
		if callCount == 1 {
			claimsEmail = email // first flow succeeds, tid triggers correction
		} else {
			claimsEmail = "someone-else@other.com" // second flow authenticates wrong account
		}
		idToken := makeIDToken(map[string]any{"email": claimsEmail, "tid": MicrosoftConsumerTenantID})
		tok := (&oauth2.Token{AccessToken: "tok", TokenType: "Bearer"}).
			WithExtra(map[string]any{"id_token": idToken})
		return tok, "nonce", nil
	}
	err := m.Authorize(t.Context(), "user@custom-domain.com")
	if err == nil {
		t.Fatal("expected error when re-auth produces wrong email")
	}
	var mismatch *TokenMismatchError
	if !errors.As(err, &mismatch) {
		t.Errorf("expected *TokenMismatchError, got %T: %v", err, err)
	}
}

// --- resolveTokenEmail edge cases ---

func TestResolveTokenEmail_MissingIDToken(t *testing.T) {
	m := &Manager{
		clientID:        "test-client",
		tenantID:        "common",
		tokensDir:       t.TempDir(),
		verifyIDTokenFn: testVerifyFn,
	}
	token := &oauth2.Token{AccessToken: "test", TokenType: "Bearer"} // no id_token extra
	_, _, err := m.resolveTokenEmail(t.Context(), "user@example.com", token, "nonce")
	if err == nil {
		t.Fatal("expected error for missing id_token")
	}
	if !strings.Contains(err.Error(), "no id_token") {
		t.Errorf("error = %q, want mention of 'no id_token'", err.Error())
	}
}

func TestResolveTokenEmail_NeitherEmailNorUPN(t *testing.T) {
	m := &Manager{
		clientID:        "test-client",
		tenantID:        "common",
		tokensDir:       t.TempDir(),
		verifyIDTokenFn: testVerifyFn,
	}
	// ID token has only tid — no email or preferred_username.
	idToken := makeIDToken(map[string]any{"tid": "some-tenant"})
	token := (&oauth2.Token{AccessToken: "test", TokenType: "Bearer"}).
		WithExtra(map[string]any{"id_token": idToken})
	_, _, err := m.resolveTokenEmail(t.Context(), "user@example.com", token, "nonce")
	if err == nil {
		t.Fatal("expected error when neither email nor preferred_username is present")
	}
	if !strings.Contains(err.Error(), "preferred_username") {
		t.Errorf("error = %q, want mention of 'preferred_username'", err.Error())
	}
}

// --- peekTIDFromJWT edge cases ---

func TestPeekTIDFromJWT_TooFewParts(t *testing.T) {
	for _, input := range []string{"onlyone", "header.payload"} {
		_, err := peekTIDFromJWT(input)
		if err == nil {
			t.Errorf("peekTIDFromJWT(%q): expected error for malformed JWT", input)
		}
	}
}

func TestPeekTIDFromJWT_TooManyParts(t *testing.T) {
	_, err := peekTIDFromJWT("a.b.c.d")
	if err == nil {
		t.Error("expected error for JWT with more than 3 parts")
	}
}

func TestPeekTIDFromJWT_InvalidBase64Payload(t *testing.T) {
	_, err := peekTIDFromJWT("header.!!!not-base64!!!.sig")
	if err == nil {
		t.Fatal("expected error for invalid base64 payload")
	}
}

func TestPeekTIDFromJWT_MissingTIDClaim(t *testing.T) {
	// Valid JWT but payload has no "tid" field.
	idToken := makeIDToken(map[string]any{"email": "user@example.com"})
	_, err := peekTIDFromJWT(idToken)
	if err == nil {
		t.Fatal("expected error for JWT without tid claim")
	}
	if !strings.Contains(err.Error(), "no tid claim") {
		t.Errorf("error = %q, want mention of 'no tid claim'", err.Error())
	}
}
