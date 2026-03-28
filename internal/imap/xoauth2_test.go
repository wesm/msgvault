package imap

import "testing"

func TestXOAuth2Client_Start(t *testing.T) {
	tests := []struct {
		name     string
		username string
		token    string
		wantMech string
		wantIR   string
	}{
		{
			name:     "basic",
			username: "user@example.com",
			token:    "ya29.access-token",
			wantMech: "XOAUTH2",
			wantIR:   "user=user@example.com\x01auth=Bearer ya29.access-token\x01\x01",
		},
		{
			name:     "empty token",
			username: "user@example.com",
			token:    "",
			wantMech: "XOAUTH2",
			wantIR:   "user=user@example.com\x01auth=Bearer \x01\x01",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := NewXOAuth2Client(tt.username, tt.token)
			mech, ir, err := c.Start()
			if err != nil {
				t.Fatalf("Start() error: %v", err)
			}
			if mech != tt.wantMech {
				t.Errorf("mech = %q, want %q", mech, tt.wantMech)
			}
			if string(ir) != tt.wantIR {
				t.Errorf("ir = %q, want %q", string(ir), tt.wantIR)
			}
		})
	}
}

func TestXOAuth2Client_Next(t *testing.T) {
	c := NewXOAuth2Client("user@example.com", "token")
	// On auth failure the server sends a JSON error challenge.  The correct
	// XOAUTH2 response is an empty byte slice; the server then sends NO and
	// the IMAP AUTHENTICATE command returns the server's error message.
	resp, err := c.Next([]byte(`{"status":"401","schemes":"bearer","scope":"..."}`))
	if err != nil {
		t.Fatalf("Next() returned unexpected error: %v", err)
	}
	if len(resp) != 0 {
		t.Errorf("Next() = %q, want empty response", resp)
	}
}
