package whatsapp

import (
	"database/sql"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func TestFetchChatsOldSchema(t *testing.T) {
	// Old WhatsApp schemas (pre-2022) lack the group_type column on chat.
	// fetchChats should handle this gracefully, defaulting group_type to 0.
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	resetColumnCache()

	// Create old-style chat table without group_type.
	_, err = db.Exec(`
		CREATE TABLE jid (
			_id INTEGER PRIMARY KEY,
			user TEXT,
			server TEXT,
			raw_string TEXT
		);
		CREATE TABLE chat (
			_id INTEGER PRIMARY KEY,
			jid_row_id INTEGER UNIQUE,
			hidden INTEGER,
			subject TEXT,
			sort_timestamp INTEGER
		);

		INSERT INTO jid (_id, user, server, raw_string)
			VALUES (1, '447700900000', 's.whatsapp.net', '447700900000@s.whatsapp.net');
		INSERT INTO jid (_id, user, server, raw_string)
			VALUES (2, '120363001234567890', 'g.us', '120363001234567890@g.us');

		INSERT INTO chat (_id, jid_row_id, hidden, subject, sort_timestamp)
			VALUES (10, 1, 0, NULL, 1609459200000);
		INSERT INTO chat (_id, jid_row_id, hidden, subject, sort_timestamp)
			VALUES (20, 2, 0, 'Family Group', 1609459300000);
	`)
	if err != nil {
		t.Fatal(err)
	}

	chats, err := fetchChats(db)
	if err != nil {
		t.Fatalf("fetchChats with old schema: %v", err)
	}

	if len(chats) != 2 {
		t.Fatalf("expected 2 chats, got %d", len(chats))
	}

	// All chats should have GroupType=0 since column is missing.
	for _, c := range chats {
		if c.GroupType != 0 {
			t.Errorf("chat %d: GroupType = %d, want 0", c.RowID, c.GroupType)
		}
	}

	// Group chat (g.us) should still be detected via server.
	group := chats[0] // sorted by sort_timestamp DESC
	if group.Server != "g.us" {
		t.Errorf("expected first chat to be group (g.us), got server=%q", group.Server)
	}
	if !isGroupChat(group) {
		t.Error("g.us chat should be detected as group even without group_type column")
	}
}

func TestFetchChatsNewSchema(t *testing.T) {
	// New WhatsApp schemas have group_type on chat.
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	resetColumnCache()

	_, err = db.Exec(`
		CREATE TABLE jid (
			_id INTEGER PRIMARY KEY,
			user TEXT,
			server TEXT,
			raw_string TEXT
		);
		CREATE TABLE chat (
			_id INTEGER PRIMARY KEY,
			jid_row_id INTEGER UNIQUE,
			hidden INTEGER,
			subject TEXT,
			sort_timestamp INTEGER,
			group_type INTEGER
		);

		INSERT INTO jid (_id, user, server, raw_string)
			VALUES (1, '120363009999', 'g.us', '120363009999@g.us');
		INSERT INTO chat (_id, jid_row_id, hidden, subject, sort_timestamp, group_type)
			VALUES (10, 1, 0, 'Work Chat', 1609459200000, 1);
	`)
	if err != nil {
		t.Fatal(err)
	}

	chats, err := fetchChats(db)
	if err != nil {
		t.Fatalf("fetchChats with new schema: %v", err)
	}

	if len(chats) != 1 {
		t.Fatalf("expected 1 chat, got %d", len(chats))
	}
	if chats[0].GroupType != 1 {
		t.Errorf("GroupType = %d, want 1", chats[0].GroupType)
	}
}

func TestFetchMediaOldSchema(t *testing.T) {
	// Old WhatsApp schemas lack media_caption on message_media.
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	resetColumnCache()

	_, err = db.Exec(`
		CREATE TABLE message_media (
			message_row_id INTEGER PRIMARY KEY,
			mime_type TEXT,
			file_size INTEGER,
			file_path TEXT,
			width INTEGER,
			height INTEGER,
			media_duration INTEGER
		);

		INSERT INTO message_media (message_row_id, mime_type, file_size, file_path, width, height, media_duration)
			VALUES (100, 'image/jpeg', 54321, 'Media/IMG-20200101.jpg', 1920, 1080, 0);
	`)
	if err != nil {
		t.Fatal(err)
	}

	mediaMap, err := fetchMedia(db, []int64{100})
	if err != nil {
		t.Fatalf("fetchMedia with old schema: %v", err)
	}

	m, ok := mediaMap[100]
	if !ok {
		t.Fatal("expected media for message 100")
	}
	if m.MediaCaption.Valid {
		t.Error("MediaCaption should be NULL for old schema")
	}
	if !m.MimeType.Valid || m.MimeType.String != "image/jpeg" {
		t.Errorf("MimeType = %v, want image/jpeg", m.MimeType)
	}
}

func TestColumnCacheScopedPerDB(t *testing.T) {
	// Verify that inspecting an old-schema DB then a new-schema DB
	// (and vice versa) produces correct results without resetColumnCache.
	resetColumnCache()

	// DB 1: old schema, no group_type.
	oldDB, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = oldDB.Close() }()
	_, err = oldDB.Exec(`
		CREATE TABLE jid (_id INTEGER PRIMARY KEY, user TEXT, server TEXT, raw_string TEXT);
		CREATE TABLE chat (_id INTEGER PRIMARY KEY, jid_row_id INTEGER UNIQUE, hidden INTEGER, subject TEXT, sort_timestamp INTEGER);
		INSERT INTO jid VALUES (1, '441234567890', 's.whatsapp.net', '441234567890@s.whatsapp.net');
		INSERT INTO chat VALUES (1, 1, 0, NULL, 1000);
	`)
	if err != nil {
		t.Fatal(err)
	}

	// DB 2: new schema, has group_type.
	newDB, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = newDB.Close() }()
	_, err = newDB.Exec(`
		CREATE TABLE jid (_id INTEGER PRIMARY KEY, user TEXT, server TEXT, raw_string TEXT);
		CREATE TABLE chat (_id INTEGER PRIMARY KEY, jid_row_id INTEGER UNIQUE, hidden INTEGER, subject TEXT, sort_timestamp INTEGER, group_type INTEGER);
		INSERT INTO jid VALUES (1, '120363009999', 'g.us', '120363009999@g.us');
		INSERT INTO chat VALUES (1, 1, 0, 'Test Group', 2000, 3);
	`)
	if err != nil {
		t.Fatal(err)
	}

	// Query old DB first — should NOT cache "no group_type" for new DB.
	oldChats, err := fetchChats(oldDB)
	if err != nil {
		t.Fatalf("old DB: %v", err)
	}
	if oldChats[0].GroupType != 0 {
		t.Errorf("old DB: GroupType = %d, want 0", oldChats[0].GroupType)
	}

	// Query new DB — must see group_type despite old DB being queried first.
	newChats, err := fetchChats(newDB)
	if err != nil {
		t.Fatalf("new DB: %v", err)
	}
	if newChats[0].GroupType != 3 {
		t.Errorf("new DB: GroupType = %d, want 3", newChats[0].GroupType)
	}

	// Reverse: query new DB again then old DB again — still correct.
	newChats2, err := fetchChats(newDB)
	if err != nil {
		t.Fatalf("new DB (2nd): %v", err)
	}
	if newChats2[0].GroupType != 3 {
		t.Errorf("new DB (2nd): GroupType = %d, want 3", newChats2[0].GroupType)
	}

	oldChats2, err := fetchChats(oldDB)
	if err != nil {
		t.Fatalf("old DB (2nd): %v", err)
	}
	if oldChats2[0].GroupType != 0 {
		t.Errorf("old DB (2nd): GroupType = %d, want 0", oldChats2[0].GroupType)
	}
}

func TestFetchLidMap(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

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
	defer func() { _ = db.Close() }()

	// No jid_map table — should return empty map, not error.
	lidMap, err := fetchLidMap(db)
	if err != nil {
		t.Fatalf("expected no error for missing table, got: %v", err)
	}
	if len(lidMap) != 0 {
		t.Errorf("expected empty map, got %d entries", len(lidMap))
	}
}
