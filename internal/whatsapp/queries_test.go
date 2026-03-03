package whatsapp

import (
	"database/sql"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func TestFetchLidMap(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Create the jid and jid_map tables matching WhatsApp's actual schema.
	// In WhatsApp: jid_map.lid_row_id is PK (= jid._id for the lid entry),
	// jid_map.jid_row_id points to the phone jid._id.
	_, err = db.Exec(`
		CREATE TABLE jid (
			_id INTEGER PRIMARY KEY,
			user TEXT,
			server TEXT,
			raw_string TEXT
		);
		CREATE TABLE jid_map (
			lid_row_id INTEGER PRIMARY KEY NOT NULL,
			jid_row_id INTEGER NOT NULL
		);

		-- lid JID entries (these are the lid_row_id values)
		INSERT INTO jid (_id, user, server, raw_string) VALUES (10, '12345abcde', 'lid', '12345abcde@lid');
		INSERT INTO jid (_id, user, server, raw_string) VALUES (20, '67890fghij', 'lid', '67890fghij@lid');

		-- phone JID entries (these are the jid_row_id values)
		INSERT INTO jid (_id, user, server, raw_string) VALUES (11, '447957366403', 's.whatsapp.net', '447957366403@s.whatsapp.net');
		INSERT INTO jid (_id, user, server, raw_string) VALUES (21, '12025551234', 's.whatsapp.net', '12025551234@s.whatsapp.net');

		-- Map lid → phone
		INSERT INTO jid_map (lid_row_id, jid_row_id) VALUES (10, 11);
		INSERT INTO jid_map (lid_row_id, jid_row_id) VALUES (20, 21);
	`)
	if err != nil {
		t.Fatal(err)
	}

	lidMap, err := fetchLidMap(db)
	if err != nil {
		t.Fatal(err)
	}

	if len(lidMap) != 2 {
		t.Fatalf("expected 2 lid mappings, got %d", len(lidMap))
	}

	m1, ok := lidMap[10]
	if !ok {
		t.Fatal("expected lid row 10 in map")
	}
	if m1.PhoneUser != "447957366403" || m1.PhoneServer != "s.whatsapp.net" {
		t.Errorf("lid 10: got user=%q server=%q, want 447957366403@s.whatsapp.net", m1.PhoneUser, m1.PhoneServer)
	}

	m2, ok := lidMap[20]
	if !ok {
		t.Fatal("expected lid row 20 in map")
	}
	if m2.PhoneUser != "12025551234" {
		t.Errorf("lid 20: got user=%q, want 12025551234", m2.PhoneUser)
	}
}

func TestFetchLidMapMissingTable(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// No jid_map table — should return empty map, not error.
	lidMap, err := fetchLidMap(db)
	if err != nil {
		t.Fatalf("expected no error for missing table, got: %v", err)
	}
	if len(lidMap) != 0 {
		t.Errorf("expected empty map, got %d entries", len(lidMap))
	}
}
