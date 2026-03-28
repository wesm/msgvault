package imap

import "testing"

func TestIdentifier(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
		want string
	}{
		{
			name: "TLS",
			cfg:  Config{Host: "imap.example.com", Port: 993, TLS: true, Username: "user@example.com"},
			want: "imaps://user@example.com@imap.example.com:993",
		},
		{
			name: "STARTTLS",
			cfg:  Config{Host: "mail.example.com", Port: 143, STARTTLS: true, Username: "user@example.com"},
			want: "imap+starttls://user@example.com@mail.example.com:143",
		},
		{
			name: "Plaintext",
			cfg:  Config{Host: "mail.example.com", Port: 143, Username: "user@example.com"},
			want: "imap://user@example.com@mail.example.com:143",
		},
		{
			name: "TLS default port",
			cfg:  Config{Host: "imap.example.com", TLS: true, Username: "user"},
			want: "imaps://user@imap.example.com:993",
		},
		{
			name: "Non-TLS default port",
			cfg:  Config{Host: "mail.example.com", Username: "user"},
			want: "imap://user@mail.example.com:143",
		},
		{
			name: "IPv6 host unbracketed",
			cfg:  Config{Host: "::1", Port: 1993, Username: "user"},
			want: "imap://user@[::1]:1993",
		},
		{
			name: "IPv6 host bracketed",
			cfg:  Config{Host: "[::1]", Port: 1993, Username: "user"},
			want: "imap://user@[::1]:1993",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.cfg.Identifier()
			if got != tt.want {
				t.Errorf("Identifier() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestAddr_IPv6(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
		want string
	}{
		{
			name: "unbracketed",
			cfg:  Config{Host: "::1", Port: 1993},
			want: "[::1]:1993",
		},
		{
			name: "bracketed",
			cfg:  Config{Host: "[::1]", Port: 1993},
			want: "[::1]:1993",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.cfg.Addr()
			if got != tt.want {
				t.Errorf("Addr() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestIdentifier_STARTTLSDistinctFromPlaintext(t *testing.T) {
	starttls := Config{
		Host: "mail.example.com", Port: 143,
		STARTTLS: true, Username: "user@example.com",
	}
	plain := Config{
		Host: "mail.example.com", Port: 143,
		Username: "user@example.com",
	}
	if starttls.Identifier() == plain.Identifier() {
		t.Errorf(
			"STARTTLS and plaintext should have distinct identifiers; both = %q",
			starttls.Identifier(),
		)
	}
}

func TestParseIdentifier_RoundTrip(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
	}{
		{
			name: "TLS",
			cfg:  Config{Host: "imap.example.com", Port: 993, TLS: true, Username: "user@example.com"},
		},
		{
			name: "STARTTLS",
			cfg:  Config{Host: "mail.example.com", Port: 143, STARTTLS: true, Username: "user@example.com"},
		},
		{
			name: "Plaintext",
			cfg:  Config{Host: "mail.example.com", Port: 143, Username: "user@example.com"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id := tt.cfg.Identifier()
			parsed, err := ParseIdentifier(id)
			if err != nil {
				t.Fatalf("ParseIdentifier(%q): %v", id, err)
			}
			if parsed.Host != tt.cfg.Host {
				t.Errorf("Host = %q, want %q", parsed.Host, tt.cfg.Host)
			}
			if parsed.Port != tt.cfg.Port {
				t.Errorf("Port = %d, want %d", parsed.Port, tt.cfg.Port)
			}
			if parsed.TLS != tt.cfg.TLS {
				t.Errorf("TLS = %v, want %v", parsed.TLS, tt.cfg.TLS)
			}
			if parsed.STARTTLS != tt.cfg.STARTTLS {
				t.Errorf("STARTTLS = %v, want %v", parsed.STARTTLS, tt.cfg.STARTTLS)
			}
			if parsed.Username != tt.cfg.Username {
				t.Errorf("Username = %q, want %q", parsed.Username, tt.cfg.Username)
			}
		})
	}
}

func TestParseIdentifier_InvalidScheme(t *testing.T) {
	_, err := ParseIdentifier("pop3://user@host:110")
	if err == nil {
		t.Error("expected error for unsupported scheme")
	}
}

func TestConfigAuthMethod_DefaultsToPassword(t *testing.T) {
	// Existing JSON without auth_method should default to password
	cfg, err := ConfigFromJSON(`{"host":"imap.example.com","port":993,"tls":true,"username":"user"}`)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.AuthMethod != "" && cfg.AuthMethod != AuthPassword {
		t.Errorf("AuthMethod = %q, want empty or %q", cfg.AuthMethod, AuthPassword)
	}
	if cfg.EffectiveAuthMethod() != AuthPassword {
		t.Errorf("EffectiveAuthMethod() = %q, want %q", cfg.EffectiveAuthMethod(), AuthPassword)
	}
}

func TestConfigAuthMethod_XOAuth2(t *testing.T) {
	cfg, err := ConfigFromJSON(`{"host":"outlook.office365.com","port":993,"tls":true,"username":"user@company.com","auth_method":"xoauth2"}`)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.AuthMethod != AuthXOAuth2 {
		t.Errorf("AuthMethod = %q, want %q", cfg.AuthMethod, AuthXOAuth2)
	}
	if cfg.EffectiveAuthMethod() != AuthXOAuth2 {
		t.Errorf("EffectiveAuthMethod() = %q, want %q", cfg.EffectiveAuthMethod(), AuthXOAuth2)
	}
}
