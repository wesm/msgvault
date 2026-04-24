package store

import "testing"

func TestPostgreSQLDialect_Rebind(t *testing.T) {
	d := &PostgreSQLDialect{}
	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "empty query",
			in:   "",
			want: "",
		},
		{
			name: "no placeholders",
			in:   "SELECT 1",
			want: "SELECT 1",
		},
		{
			name: "single placeholder",
			in:   "SELECT * FROM t WHERE id = ?",
			want: "SELECT * FROM t WHERE id = $1",
		},
		{
			name: "multiple placeholders",
			in:   "INSERT INTO t (a, b, c) VALUES (?, ?, ?)",
			want: "INSERT INTO t (a, b, c) VALUES ($1, $2, $3)",
		},
		{
			name: "placeholder inside quoted string is not converted",
			in:   "SELECT * FROM t WHERE name = 'what?' AND id = ?",
			want: "SELECT * FROM t WHERE name = 'what?' AND id = $1",
		},
		{
			name: "multiple quoted strings",
			in:   "SELECT * FROM t WHERE a = 'foo?' AND b = 'bar?' AND c = ?",
			want: "SELECT * FROM t WHERE a = 'foo?' AND b = 'bar?' AND c = $1",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := d.Rebind(tc.in)
			if got != tc.want {
				t.Errorf("Rebind(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestPostgreSQLDialect_Now(t *testing.T) {
	d := &PostgreSQLDialect{}
	if got := d.Now(); got != "NOW()" {
		t.Errorf("Now() = %q, want %q", got, "NOW()")
	}
}

func TestPostgreSQLDialect_InsertOrIgnore(t *testing.T) {
	d := &PostgreSQLDialect{}
	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "complete statement gets ON CONFLICT DO NOTHING",
			in:   "INSERT OR IGNORE INTO t (a) VALUES (?)",
			want: "INSERT INTO t (a) VALUES (?) ON CONFLICT DO NOTHING",
		},
		{
			name: "multi-value complete statement",
			in:   "INSERT OR IGNORE INTO t (a, b) VALUES (?, ?)",
			want: "INSERT INTO t (a, b) VALUES (?, ?) ON CONFLICT DO NOTHING",
		},
		{
			name: "prefix-only (ends with VALUES ) leaves suffix to caller",
			in:   "INSERT OR IGNORE INTO message_labels (message_id, label_id) VALUES ",
			want: "INSERT INTO message_labels (message_id, label_id) VALUES ",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := d.InsertOrIgnore(tc.in)
			if got != tc.want {
				t.Errorf("InsertOrIgnore(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestPostgreSQLDialect_InsertOrIgnoreSuffix(t *testing.T) {
	d := &PostgreSQLDialect{}
	if got := d.InsertOrIgnoreSuffix(); got != " ON CONFLICT DO NOTHING" {
		t.Errorf("InsertOrIgnoreSuffix() = %q, want %q", got, " ON CONFLICT DO NOTHING")
	}
}

func TestPostgreSQLDialect_FTSSearchClause(t *testing.T) {
	d := &PostgreSQLDialect{}
	join, where, orderBy, orderArgCount := d.FTSSearchClause()
	if join != "" {
		t.Errorf("join = %q, want empty (PostgreSQL needs no JOIN)", join)
	}
	if where != "m.search_fts @@ plainto_tsquery('simple', ?)" {
		t.Errorf("where = %q, unexpected", where)
	}
	if orderBy != "ts_rank(m.search_fts, plainto_tsquery('simple', ?)) DESC" {
		t.Errorf("orderBy = %q, unexpected", orderBy)
	}
	if orderArgCount != 1 {
		t.Errorf("orderArgCount = %d, want 1 (ts_rank needs query a second time)", orderArgCount)
	}
}

func TestPostgreSQLDialect_InsertOrIgnorePrefix(t *testing.T) {
	d := &PostgreSQLDialect{}
	in := "INSERT OR IGNORE INTO message_labels (message_id, label_id) VALUES "
	want := "INSERT INTO message_labels (message_id, label_id) VALUES "
	if got := d.InsertOrIgnorePrefix(in); got != want {
		t.Errorf("InsertOrIgnorePrefix(%q) = %q, want %q", in, got, want)
	}
}
