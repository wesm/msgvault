# SQL Analytical Query Interface Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expose msgvault's Parquet analytics via DuckDB views, queryable through a CLI command and HTTP endpoint.

**Architecture:** `RegisterViews()` creates DuckDB views over Parquet files on a single connection. The CLI opens its own DuckDB instance, the HTTP handler reuses the `DuckDBEngine`'s connection. Both serialize results through a shared `QueryResult` type.

**Tech Stack:** Go, DuckDB (go-duckdb driver), Cobra CLI, chi HTTP router

**Spec:** `docs/superpowers/specs/2026-04-08-sql-query-interface-design.md`

---

### Task 1: Base View Registration

**Files:**
- Create: `internal/query/views.go`
- Create: `internal/query/views_test.go`

- [ ] **Step 1: Write failing test for RegisterViews with base views**

```go
// internal/query/views_test.go
package query

import (
	"context"
	"testing"
)

func TestRegisterViews_BaseViews(t *testing.T) {
	builder := NewTestDataBuilder(t)
	srcID := builder.AddSource("alice@example.com")
	partID := builder.AddParticipant(
		"bob@example.com", "example.com", "Bob",
	)
	lblID := builder.AddLabel("INBOX")
	msgID := builder.AddMessage(MessageOpt{
		Subject:  "Hello",
		SourceID: srcID,
	})
	builder.AddFrom(msgID, partID, "Bob")
	builder.AddMessageLabel(msgID, lblID)

	dir, cleanup := builder.Build()
	defer cleanup()

	engine := builder.BuildEngine()
	defer func() { _ = engine.Close() }()

	// RegisterViews should succeed on the engine's DB
	if err := RegisterViews(engine.db, dir); err != nil {
		t.Fatalf("RegisterViews: %v", err)
	}

	// Each base view should be queryable
	tables := []string{
		"messages", "participants", "message_recipients",
		"labels", "message_labels", "attachments",
		"conversations", "sources",
	}
	for _, table := range tables {
		var count int
		err := engine.db.QueryRowContext(
			context.Background(),
			"SELECT COUNT(*) FROM "+table,
		).Scan(&count)
		if err != nil {
			t.Errorf("query %s: %v", table, err)
		}
	}

	// Verify messages view has expected columns including optionals
	var id int64
	var subject, messageType string
	var attachmentCount int
	err := engine.db.QueryRowContext(
		context.Background(),
		"SELECT id, subject, attachment_count, message_type FROM messages LIMIT 1",
	).Scan(&id, &subject, &attachmentCount, &messageType)
	if err != nil {
		t.Fatalf("scan messages: %v", err)
	}
	if subject != "Hello" {
		t.Errorf("subject = %q, want %q", subject, "Hello")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/query/ -run TestRegisterViews_BaseViews -v -tags fts5`
Expected: FAIL — `RegisterViews` undefined

- [ ] **Step 3: Implement RegisterViews with base views**

```go
// internal/query/views.go
package query

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
)

// RegisterViews creates DuckDB views over Parquet files in analyticsDir.
// The db connection must be constrained to a single connection
// (SetMaxOpenConns(1)) before calling this function, because DuckDB
// session state (CREATE VIEW) does not propagate across pooled
// connections.
func RegisterViews(db *sql.DB, analyticsDir string) error {
	optCols := probeAllOptionalColumns(db, analyticsDir)

	if err := createBaseViews(db, analyticsDir, optCols); err != nil {
		return fmt.Errorf("create base views: %w", err)
	}

	return nil
}

// probeAllOptionalColumns checks which optional columns exist in each
// Parquet table. Returns map[table][column] = exists. Uses the same
// DESCRIBE technique as DuckDBEngine.probeParquetColumns.
func probeAllOptionalColumns(
	db *sql.DB, analyticsDir string,
) map[string]map[string]bool {
	probe := func(pathPattern string, hive bool) map[string]bool {
		cols := make(map[string]bool)
		hiveOpt := ""
		if hive {
			hiveOpt = ", hive_partitioning=true"
		}
		escaped := strings.ReplaceAll(pathPattern, "'", "''")
		q := fmt.Sprintf(
			"DESCRIBE SELECT * FROM read_parquet('%s'%s)",
			escaped, hiveOpt,
		)
		rows, err := db.Query(q)
		if err != nil {
			return cols
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var name, colType, isNull, key, dflt, extra sql.NullString
			if err := rows.Scan(
				&name, &colType, &isNull, &key, &dflt, &extra,
			); err != nil {
				continue
			}
			if name.Valid {
				cols[name.String] = true
			}
		}
		return cols
	}

	msgGlob := filepath.Join(analyticsDir, "messages", "**", "*.parquet")
	return map[string]map[string]bool{
		"messages": probe(msgGlob, true),
		"participants": probe(
			filepath.Join(analyticsDir, "participants", "*.parquet"), false,
		),
		"conversations": probe(
			filepath.Join(analyticsDir, "conversations", "*.parquet"), false,
		),
		"sources": probe(
			filepath.Join(analyticsDir, "sources", "*.parquet"), false,
		),
	}
}

func hasOptCol(
	optCols map[string]map[string]bool, table, col string,
) bool {
	if m, ok := optCols[table]; ok {
		return m[col]
	}
	return false
}

// escapePath escapes single quotes in a file path for DuckDB SQL.
func escapePath(p string) string {
	return strings.ReplaceAll(p, "'", "''")
}

func createBaseViews(
	db *sql.DB,
	analyticsDir string,
	optCols map[string]map[string]bool,
) error {
	msgGlob := filepath.Join(
		analyticsDir, "messages", "**", "*.parquet",
	)

	// --- messages view ---
	msgReplace := []string{
		"CAST(id AS BIGINT) AS id",
		"CAST(conversation_id AS BIGINT) AS conversation_id",
		"CAST(source_id AS BIGINT) AS source_id",
		"CAST(source_message_id AS VARCHAR) AS source_message_id",
		"CAST(subject AS VARCHAR) AS subject",
		"CAST(snippet AS VARCHAR) AS snippet",
		"CAST(size_estimate AS BIGINT) AS size_estimate",
		"CAST(has_attachments AS BOOLEAN) AS has_attachments",
	}
	var msgExtra []string
	if hasOptCol(optCols, "messages", "attachment_count") {
		msgReplace = append(msgReplace,
			"COALESCE(TRY_CAST(attachment_count AS INTEGER), 0) "+
				"AS attachment_count")
	} else {
		msgExtra = append(msgExtra, "0 AS attachment_count")
	}
	if hasOptCol(optCols, "messages", "sender_id") {
		msgReplace = append(msgReplace,
			"TRY_CAST(sender_id AS BIGINT) AS sender_id")
	} else {
		msgExtra = append(msgExtra, "NULL::BIGINT AS sender_id")
	}
	if hasOptCol(optCols, "messages", "message_type") {
		msgReplace = append(msgReplace,
			"COALESCE(CAST(message_type AS VARCHAR), '') "+
				"AS message_type")
	} else {
		msgExtra = append(msgExtra, "'' AS message_type")
	}

	msgSelect := fmt.Sprintf(
		"SELECT * REPLACE (%s)", strings.Join(msgReplace, ", "),
	)
	if len(msgExtra) > 0 {
		msgSelect += ", " + strings.Join(msgExtra, ", ")
	}
	msgDDL := fmt.Sprintf(
		"CREATE OR REPLACE VIEW messages AS %s "+
			"FROM read_parquet('%s', "+
			"hive_partitioning=true, union_by_name=true)",
		msgSelect, escapePath(msgGlob),
	)

	// --- participants view ---
	pReplace := []string{
		"CAST(id AS BIGINT) AS id",
		"CAST(email_address AS VARCHAR) AS email_address",
		"CAST(domain AS VARCHAR) AS domain",
		"CAST(display_name AS VARCHAR) AS display_name",
	}
	var pExtra []string
	if hasOptCol(optCols, "participants", "phone_number") {
		pReplace = append(pReplace,
			"COALESCE(CAST(phone_number AS VARCHAR), '') "+
				"AS phone_number")
	} else {
		pExtra = append(pExtra, "'' AS phone_number")
	}
	pSelect := fmt.Sprintf(
		"SELECT * REPLACE (%s)", strings.Join(pReplace, ", "),
	)
	if len(pExtra) > 0 {
		pSelect += ", " + strings.Join(pExtra, ", ")
	}

	// --- conversations view ---
	convReplace := []string{
		"CAST(id AS BIGINT) AS id",
		"CAST(source_conversation_id AS VARCHAR) " +
			"AS source_conversation_id",
	}
	var convExtra []string
	if hasOptCol(optCols, "conversations", "title") {
		convReplace = append(convReplace,
			"COALESCE(CAST(title AS VARCHAR), '') AS title")
	} else {
		convExtra = append(convExtra, "'' AS title")
	}
	if hasOptCol(optCols, "conversations", "conversation_type") {
		convReplace = append(convReplace,
			"COALESCE(CAST(conversation_type AS VARCHAR), "+
				"'email') AS conversation_type")
	} else {
		convExtra = append(convExtra, "'email' AS conversation_type")
	}
	convSelect := fmt.Sprintf(
		"SELECT * REPLACE (%s)", strings.Join(convReplace, ", "),
	)
	if len(convExtra) > 0 {
		convSelect += ", " + strings.Join(convExtra, ", ")
	}

	// --- sources view ---
	srcReplace := []string{
		"CAST(id AS BIGINT) AS id",
	}
	var srcExtra []string
	if hasOptCol(optCols, "sources", "source_type") {
		srcReplace = append(srcReplace,
			"COALESCE(CAST(source_type AS VARCHAR), 'gmail') "+
				"AS source_type")
	} else {
		srcExtra = append(srcExtra, "'gmail' AS source_type")
	}
	srcSelect := fmt.Sprintf(
		"SELECT * REPLACE (%s)", strings.Join(srcReplace, ", "),
	)
	if len(srcExtra) > 0 {
		srcSelect += ", " + strings.Join(srcExtra, ", ")
	}

	pqPath := func(table string) string {
		return escapePath(
			filepath.Join(analyticsDir, table, "*.parquet"),
		)
	}

	views := []string{
		msgDDL,
		fmt.Sprintf(
			"CREATE OR REPLACE VIEW participants AS %s "+
				"FROM read_parquet('%s')",
			pSelect, pqPath("participants"),
		),
		fmt.Sprintf(
			"CREATE OR REPLACE VIEW message_recipients AS "+
				"SELECT * REPLACE ("+
				"CAST(message_id AS BIGINT) AS message_id, "+
				"CAST(participant_id AS BIGINT) AS participant_id, "+
				"CAST(recipient_type AS VARCHAR) AS recipient_type, "+
				"CAST(display_name AS VARCHAR) AS display_name"+
				") FROM read_parquet('%s')",
			pqPath("message_recipients"),
		),
		fmt.Sprintf(
			"CREATE OR REPLACE VIEW labels AS "+
				"SELECT * REPLACE ("+
				"CAST(id AS BIGINT) AS id, "+
				"CAST(name AS VARCHAR) AS name"+
				") FROM read_parquet('%s')",
			pqPath("labels"),
		),
		fmt.Sprintf(
			"CREATE OR REPLACE VIEW message_labels AS "+
				"SELECT * REPLACE ("+
				"CAST(message_id AS BIGINT) AS message_id, "+
				"CAST(label_id AS BIGINT) AS label_id"+
				") FROM read_parquet('%s')",
			pqPath("message_labels"),
		),
		fmt.Sprintf(
			"CREATE OR REPLACE VIEW attachments AS "+
				"SELECT * FROM read_parquet('%s')",
			pqPath("attachments"),
		),
		fmt.Sprintf(
			"CREATE OR REPLACE VIEW conversations AS %s "+
				"FROM read_parquet('%s')",
			convSelect, pqPath("conversations"),
		),
		fmt.Sprintf(
			"CREATE OR REPLACE VIEW sources AS %s "+
				"FROM read_parquet('%s')",
			srcSelect, pqPath("sources"),
		),
	}

	for _, ddl := range views {
		if _, err := db.Exec(ddl); err != nil {
			return fmt.Errorf("exec DDL: %w\nSQL: %s", err, ddl)
		}
	}
	return nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/query/ -run TestRegisterViews_BaseViews -v -tags fts5`
Expected: PASS

- [ ] **Step 5: Run full query test suite for regressions**

Run: `go test ./internal/query/ -v -tags fts5 -count=1`
Expected: All existing tests still pass

- [ ] **Step 6: Commit**

```bash
git add internal/query/views.go internal/query/views_test.go
git commit -m "feat: add RegisterViews with base DuckDB views over Parquet"
```

---

### Task 2: Convenience Views

**Files:**
- Modify: `internal/query/views.go`
- Modify: `internal/query/views_test.go`

- [ ] **Step 1: Write failing tests for convenience views**

Add to `internal/query/views_test.go`:

```go
func TestRegisterViews_ConvenienceViews(t *testing.T) {
	builder := NewTestDataBuilder(t)
	srcID := builder.AddSource("alice@example.com")
	bob := builder.AddParticipant(
		"bob@corp.com", "corp.com", "Bob Smith",
	)
	carol := builder.AddParticipant(
		"carol@corp.com", "corp.com", "Carol",
	)
	inbox := builder.AddLabel("INBOX")
	sent := builder.AddLabel("SENT")

	msg1 := builder.AddMessage(MessageOpt{
		Subject:      "First",
		SourceID:     srcID,
		SizeEstimate: 1000,
	})
	builder.AddFrom(msg1, bob, "Bob Smith")
	builder.AddTo(msg1, carol, "Carol")
	builder.AddMessageLabel(msg1, inbox)
	builder.AddAttachment(msg1, 500, "doc.pdf")

	msg2 := builder.AddMessage(MessageOpt{
		Subject:      "Second",
		SourceID:     srcID,
		SizeEstimate: 2000,
	})
	builder.AddFrom(msg2, bob, "Bob Smith")
	builder.AddMessageLabel(msg2, inbox)
	builder.AddMessageLabel(msg2, sent)

	dir, cleanup := builder.Build()
	defer cleanup()

	engine := builder.BuildEngine()
	defer func() { _ = engine.Close() }()

	if err := RegisterViews(engine.db, dir); err != nil {
		t.Fatalf("RegisterViews: %v", err)
	}
	ctx := context.Background()

	t.Run("v_messages", func(t *testing.T) {
		var fromEmail, fromDomain, labels string
		err := engine.db.QueryRowContext(ctx,
			"SELECT from_email, from_domain, labels "+
				"FROM v_messages WHERE subject = 'First'",
		).Scan(&fromEmail, &fromDomain, &labels)
		if err != nil {
			t.Fatalf("query v_messages: %v", err)
		}
		if fromEmail != "bob@corp.com" {
			t.Errorf("from_email = %q, want bob@corp.com", fromEmail)
		}
		if fromDomain != "corp.com" {
			t.Errorf("from_domain = %q, want corp.com", fromDomain)
		}
		if labels != `["INBOX"]` {
			t.Errorf("labels = %q, want [\"INBOX\"]", labels)
		}
	})

	t.Run("v_messages_multi_labels", func(t *testing.T) {
		var labels string
		err := engine.db.QueryRowContext(ctx,
			"SELECT labels FROM v_messages WHERE subject = 'Second'",
		).Scan(&labels)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		// Labels sorted alphabetically
		if labels != `["INBOX","SENT"]` {
			t.Errorf("labels = %q, want [\"INBOX\",\"SENT\"]", labels)
		}
	})

	t.Run("v_senders", func(t *testing.T) {
		var email, domain string
		var count int64
		var totalSize int64
		err := engine.db.QueryRowContext(ctx,
			"SELECT from_email, from_domain, message_count, "+
				"total_size FROM v_senders "+
				"WHERE from_email = 'bob@corp.com'",
		).Scan(&email, &domain, &count, &totalSize)
		if err != nil {
			t.Fatalf("query v_senders: %v", err)
		}
		if count != 2 {
			t.Errorf("message_count = %d, want 2", count)
		}
		if totalSize != 3000 {
			t.Errorf("total_size = %d, want 3000", totalSize)
		}
	})

	t.Run("v_domains", func(t *testing.T) {
		var count int64
		var senderCount int64
		err := engine.db.QueryRowContext(ctx,
			"SELECT message_count, sender_count FROM v_domains "+
				"WHERE domain = 'corp.com'",
		).Scan(&count, &senderCount)
		if err != nil {
			t.Fatalf("query v_domains: %v", err)
		}
		if count != 2 {
			t.Errorf("message_count = %d, want 2", count)
		}
		if senderCount != 1 {
			t.Errorf("sender_count = %d, want 1", senderCount)
		}
	})

	t.Run("v_labels", func(t *testing.T) {
		var count int64
		err := engine.db.QueryRowContext(ctx,
			"SELECT message_count FROM v_labels "+
				"WHERE name = 'INBOX'",
		).Scan(&count)
		if err != nil {
			t.Fatalf("query v_labels: %v", err)
		}
		if count != 2 {
			t.Errorf("message_count = %d, want 2", count)
		}
	})

	t.Run("v_threads", func(t *testing.T) {
		var msgCount int64
		var participants string
		err := engine.db.QueryRowContext(ctx,
			"SELECT message_count, participant_emails "+
				"FROM v_threads LIMIT 1",
		).Scan(&msgCount, &participants)
		if err != nil {
			t.Fatalf("query v_threads: %v", err)
		}
		if msgCount < 1 {
			t.Errorf("message_count = %d, want >= 1", msgCount)
		}
	})
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/query/ -run TestRegisterViews_ConvenienceViews -v -tags fts5`
Expected: FAIL — views not defined

- [ ] **Step 3: Add convenience views to RegisterViews**

Add to `internal/query/views.go`, called at the end of `RegisterViews`:

```go
// Add to RegisterViews, after createBaseViews:
if err := createConvenienceViews(db); err != nil {
	return fmt.Errorf("create convenience views: %w", err)
}
```

```go
func createConvenienceViews(db *sql.DB) error {
	views := []string{
		// v_messages: messages with sender info + labels as JSON
		`CREATE OR REPLACE VIEW v_messages AS
SELECT
	m.id,
	m.conversation_id,
	m.source_id,
	m.source_message_id,
	m.subject,
	m.snippet,
	m.sent_at,
	m.size_estimate,
	m.has_attachments,
	m.attachment_count,
	m.year,
	m.month,
	m.message_type,
	m.deleted_from_source_at,
	COALESCE(ms.from_email, ds.from_email, '') AS from_email,
	COALESCE(ms.from_name, ds.from_name, '') AS from_name,
	COALESCE(ms.from_phone, ds.from_phone, '') AS from_phone,
	COALESCE(ms.from_domain, ds.from_domain, '') AS from_domain,
	CAST(COALESCE(to_json(ml_agg.labels), '[]') AS VARCHAR)
		AS labels
FROM messages m
LEFT JOIN (
	SELECT
		mr.message_id,
		FIRST(p.email_address) AS from_email,
		FIRST(COALESCE(mr.display_name, p.display_name, ''))
			AS from_name,
		FIRST(COALESCE(p.phone_number, '')) AS from_phone,
		FIRST(p.domain) AS from_domain
	FROM message_recipients mr
	JOIN participants p ON p.id = mr.participant_id
	WHERE mr.recipient_type = 'from'
	GROUP BY mr.message_id
) ms ON ms.message_id = m.id
LEFT JOIN (
	SELECT
		m2.id AS message_id,
		COALESCE(p.email_address, '') AS from_email,
		COALESCE(p.display_name, '') AS from_name,
		COALESCE(p.phone_number, '') AS from_phone,
		COALESCE(p.domain, '') AS from_domain
	FROM messages m2
	JOIN participants p ON p.id = m2.sender_id
	WHERE m2.sender_id IS NOT NULL
) ds ON ds.message_id = m.id AND ms.message_id IS NULL
LEFT JOIN (
	SELECT
		ml.message_id,
		LIST(l.name ORDER BY l.name) AS labels
	FROM message_labels ml
	JOIN labels l ON l.id = ml.label_id
	GROUP BY ml.message_id
) ml_agg ON ml_agg.message_id = m.id`,

		// v_senders: per-sender aggregates
		`CREATE OR REPLACE VIEW v_senders AS
SELECT
	p.email_address AS from_email,
	COALESCE(
		NULLIF(TRIM(p.display_name), ''), p.email_address
	) AS from_name,
	p.domain AS from_domain,
	COUNT(*) AS message_count,
	COALESCE(SUM(CAST(m.size_estimate AS BIGINT)), 0)
		AS total_size,
	CAST(COALESCE(SUM(att.attachment_size), 0) AS BIGINT)
		AS attachment_size,
	CAST(COALESCE(SUM(att.attachment_count), 0) AS BIGINT)
		AS attachment_count,
	MIN(m.sent_at) AS first_message_at,
	MAX(m.sent_at) AS last_message_at
FROM messages m
JOIN message_recipients mr
	ON mr.message_id = m.id AND mr.recipient_type = 'from'
JOIN participants p ON p.id = mr.participant_id
LEFT JOIN (
	SELECT
		CAST(message_id AS BIGINT) AS message_id,
		SUM(COALESCE(TRY_CAST(size AS BIGINT), 0))
			AS attachment_size,
		COUNT(*) AS attachment_count
	FROM attachments
	GROUP BY 1
) att ON att.message_id = m.id
WHERE p.email_address IS NOT NULL
GROUP BY p.email_address, p.display_name, p.domain`,

		// v_domains: per-domain aggregates
		`CREATE OR REPLACE VIEW v_domains AS
SELECT
	p.domain,
	COUNT(*) AS message_count,
	COALESCE(SUM(CAST(m.size_estimate AS BIGINT)), 0)
		AS total_size,
	COUNT(DISTINCT p.email_address) AS sender_count
FROM messages m
JOIN message_recipients mr
	ON mr.message_id = m.id AND mr.recipient_type = 'from'
JOIN participants p ON p.id = mr.participant_id
WHERE p.domain IS NOT NULL AND p.domain != ''
GROUP BY p.domain`,

		// v_labels: label name + message count + total size
		`CREATE OR REPLACE VIEW v_labels AS
SELECT
	l.name,
	COUNT(*) AS message_count,
	COALESCE(SUM(CAST(m.size_estimate AS BIGINT)), 0)
		AS total_size
FROM labels l
JOIN message_labels ml ON ml.label_id = l.id
JOIN messages m ON m.id = ml.message_id
GROUP BY l.name`,

		// v_threads: per-conversation aggregates with participants
		`CREATE OR REPLACE VIEW v_threads AS
SELECT
	c.id AS conversation_id,
	c.source_conversation_id,
	c.title AS conversation_title,
	c.conversation_type,
	COUNT(DISTINCT m.id) AS message_count,
	MIN(m.sent_at) AS first_message_at,
	MAX(m.sent_at) AS last_message_at,
	CAST(COALESCE(
		to_json(LIST(DISTINCT p.email_address
			ORDER BY p.email_address)),
		'[]'
	) AS VARCHAR) AS participant_emails
FROM conversations c
JOIN messages m ON m.conversation_id = c.id
LEFT JOIN message_recipients mr ON mr.message_id = m.id
LEFT JOIN participants p ON p.id = mr.participant_id
GROUP BY c.id, c.source_conversation_id, c.title,
	c.conversation_type`,
	}

	for _, ddl := range views {
		if _, err := db.Exec(ddl); err != nil {
			return fmt.Errorf("create convenience view: %w\nSQL: %s",
				err, ddl)
		}
	}
	return nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/query/ -run TestRegisterViews_ConvenienceViews -v -tags fts5`
Expected: PASS

- [ ] **Step 5: Run full query test suite for regressions**

Run: `go test ./internal/query/ -v -tags fts5 -count=1`
Expected: All tests pass

- [ ] **Step 6: Commit**

```bash
git add internal/query/views.go internal/query/views_test.go
git commit -m "feat: add convenience views (v_messages, v_senders, v_domains, v_labels, v_threads)"
```

---

### Task 3: QuerySQL Method + Wire Into DuckDBEngine

**Files:**
- Modify: `internal/query/views.go` (add QueryResult, SQLQuerier)
- Modify: `internal/query/duckdb.go` (add QuerySQL, call RegisterViews)
- Modify: `internal/query/views_test.go` (add QuerySQL test)

- [ ] **Step 1: Write failing test for QuerySQL**

Add to `internal/query/views_test.go`:

```go
func TestDuckDBEngine_QuerySQL(t *testing.T) {
	builder := NewTestDataBuilder(t)
	srcID := builder.AddSource("test@example.com")
	bob := builder.AddParticipant(
		"bob@example.com", "example.com", "Bob",
	)
	msgID := builder.AddMessage(MessageOpt{
		Subject:      "Test",
		SourceID:     srcID,
		SizeEstimate: 100,
	})
	builder.AddFrom(msgID, bob, "Bob")

	engine := builder.BuildEngine()
	defer func() { _ = engine.Close() }()

	ctx := context.Background()
	result, err := engine.QuerySQL(ctx,
		"SELECT from_email, message_count FROM v_senders")
	if err != nil {
		t.Fatalf("QuerySQL: %v", err)
	}
	if len(result.Columns) < 2 {
		t.Fatalf("columns = %v, want at least 2", result.Columns)
	}
	if result.Columns[0] != "from_email" {
		t.Errorf("columns[0] = %q, want from_email",
			result.Columns[0])
	}
	if result.RowCount != 1 {
		t.Errorf("row_count = %d, want 1", result.RowCount)
	}
}

func TestDuckDBEngine_QuerySQL_Error(t *testing.T) {
	builder := NewTestDataBuilder(t)
	builder.AddSource("test@example.com")
	engine := builder.BuildEngine()
	defer func() { _ = engine.Close() }()

	_, err := engine.QuerySQL(
		context.Background(), "SELECT * FROM nonexistent_table",
	)
	if err == nil {
		t.Fatal("expected error for bad SQL")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/query/ -run TestDuckDBEngine_QuerySQL -v -tags fts5`
Expected: FAIL — `QuerySQL` method undefined

- [ ] **Step 3: Add QueryResult type and SQLQuerier interface**

Add to `internal/query/views.go`:

```go
import "context"

// QueryResult holds raw SQL query results in a columnar format
// suitable for JSON, CSV, and table serialization.
type QueryResult struct {
	Columns  []string        `json:"columns"`
	Rows     [][]interface{} `json:"rows"`
	RowCount int             `json:"row_count"`
}

// SQLQuerier is implemented by engines that support raw SQL queries.
// Used by the HTTP handler to type-assert the Engine interface.
type SQLQuerier interface {
	QuerySQL(ctx context.Context, sql string) (*QueryResult, error)
}
```

- [ ] **Step 4: Implement QuerySQL on DuckDBEngine**

Add to `internal/query/duckdb.go`:

```go
// QuerySQL executes an arbitrary SQL query against the DuckDB
// connection (which has views registered over Parquet files) and
// returns results in a columnar format.
func (e *DuckDBEngine) QuerySQL(
	ctx context.Context, sqlStr string,
) (*QueryResult, error) {
	rows, err := e.db.QueryContext(ctx, sqlStr)
	if err != nil {
		return nil, fmt.Errorf("execute query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("get columns: %w", err)
	}

	result := &QueryResult{Columns: cols}
	for rows.Next() {
		vals := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, fmt.Errorf("scan row: %w", err)
		}
		// Normalize types for JSON serialization: convert
		// []byte to string, leave numbers and strings as-is.
		for i, v := range vals {
			if b, ok := v.([]byte); ok {
				vals[i] = string(b)
			}
		}
		result.Rows = append(result.Rows, vals)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate rows: %w", err)
	}
	result.RowCount = len(result.Rows)
	return result, nil
}
```

- [ ] **Step 5: Wire RegisterViews into NewDuckDBEngine**

In `internal/query/duckdb.go`, add after the `optionalCols` setup
(after the missing-columns log, around line 165):

```go
// Register SQL views over Parquet files for raw SQL access.
if err := RegisterViews(db, analyticsDir); err != nil {
	log.Printf("[warn] failed to register SQL views: %v", err)
	// Non-fatal: existing CTE-based queries still work.
}
```

- [ ] **Step 6: Run tests to verify everything passes**

Run: `go test ./internal/query/ -run TestDuckDBEngine_QuerySQL -v -tags fts5`
Expected: PASS

Run: `go test ./internal/query/ -v -tags fts5 -count=1`
Expected: All tests pass (existing tests unaffected — views don't
conflict with CTE aliases `msg`, `mr`, `p`, etc.)

- [ ] **Step 7: Commit**

```bash
git add internal/query/views.go internal/query/duckdb.go \
    internal/query/views_test.go
git commit -m "feat: add QuerySQL method, wire views into DuckDBEngine startup"
```

---

### Task 4: CLI Command

**Files:**
- Create: `cmd/msgvault/cmd/query.go`
- Create: `cmd/msgvault/cmd/query_test.go`

- [ ] **Step 1: Write failing test for query command**

```go
// cmd/msgvault/cmd/query_test.go
package cmd

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/wesm/msgvault/internal/query"
)

func TestQueryCommand_JSON(t *testing.T) {
	// Create test Parquet data
	builder := query.NewTestDataBuilder(t)
	srcID := builder.AddSource("test@example.com")
	bob := builder.AddParticipant(
		"bob@example.com", "example.com", "Bob",
	)
	msgID := builder.AddMessage(query.MessageOpt{
		Subject:      "Hello World",
		SourceID:     srcID,
		SizeEstimate: 100,
	})
	builder.AddFrom(msgID, bob, "Bob")

	dir, cleanup := builder.Build()
	defer cleanup()

	// Run query command by calling runQuery directly
	var buf bytes.Buffer
	err := executeQuery(dir,
		"SELECT subject FROM v_messages LIMIT 1", "json", &buf)
	if err != nil {
		t.Fatalf("executeQuery: %v", err)
	}

	var result query.QueryResult
	if err := json.Unmarshal(buf.Bytes(), &result); err != nil {
		t.Fatalf("unmarshal: %v\nraw: %s", err, buf.String())
	}
	if result.RowCount != 1 {
		t.Errorf("row_count = %d, want 1", result.RowCount)
	}
}

func TestQueryCommand_CSV(t *testing.T) {
	builder := query.NewTestDataBuilder(t)
	srcID := builder.AddSource("test@example.com")
	bob := builder.AddParticipant(
		"bob@example.com", "example.com", "Bob",
	)
	msgID := builder.AddMessage(query.MessageOpt{
		Subject:  "Hello",
		SourceID: srcID,
	})
	builder.AddFrom(msgID, bob, "Bob")

	dir, cleanup := builder.Build()
	defer cleanup()

	var buf bytes.Buffer
	err := executeQuery(dir,
		"SELECT subject FROM v_messages LIMIT 1", "csv", &buf)
	if err != nil {
		t.Fatalf("executeQuery: %v", err)
	}

	output := buf.String()
	if !strings.Contains(output, "subject") {
		t.Errorf("missing header in CSV output: %s", output)
	}
	if !strings.Contains(output, "Hello") {
		t.Errorf("missing data in CSV output: %s", output)
	}
}

func TestQueryCommand_MissingCache(t *testing.T) {
	emptyDir := t.TempDir()
	var buf bytes.Buffer
	err := executeQuery(emptyDir,
		"SELECT 1", "json", &buf)
	if err == nil {
		t.Fatal("expected error for missing cache")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./cmd/msgvault/cmd/ -run TestQueryCommand -v -tags fts5`
Expected: FAIL — `executeQuery` undefined

- [ ] **Step 3: Implement query command**

```go
// cmd/msgvault/cmd/query.go
package cmd

import (
	"bytes"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"

	_ "github.com/marcboeker/go-duckdb"
	"github.com/spf13/cobra"
	"github.com/wesm/msgvault/internal/query"
)

var queryFormat string

var queryCmd = &cobra.Command{
	Use:   "query [sql]",
	Short: "Run a SQL query against the analytics views",
	Long: `Execute a read-only SQL query against DuckDB views
over the Parquet analytics cache.

Available base views: messages, participants,
message_recipients, labels, message_labels, attachments,
conversations, sources.

Available convenience views: v_messages, v_senders,
v_domains, v_labels, v_threads.`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		analyticsDir := cfg.AnalyticsDir()
		if !query.HasCompleteParquetData(analyticsDir) {
			return fmt.Errorf(
				"analytics cache not found — run "+
					"'msgvault build-cache' first")
		}
		return executeQuery(
			analyticsDir, args[0], queryFormat, os.Stdout,
		)
	},
}

func init() {
	queryCmd.Flags().StringVar(&queryFormat, "format", "json",
		"output format: json, csv, or table")
	rootCmd.AddCommand(queryCmd)
}

// executeQuery opens DuckDB, registers views, runs the SQL, and
// writes results to w. Extracted for testability.
func executeQuery(
	analyticsDir, sqlStr, format string, w io.Writer,
) error {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return fmt.Errorf("open duckdb: %w", err)
	}
	defer func() { _ = db.Close() }()

	db.SetMaxOpenConns(1)
	threads := runtime.GOMAXPROCS(0)
	if _, err := db.Exec(
		fmt.Sprintf("SET threads = %d", threads),
	); err != nil {
		return fmt.Errorf("set threads: %w", err)
	}

	if err := query.RegisterViews(db, analyticsDir); err != nil {
		return fmt.Errorf("register views: %w", err)
	}

	rows, err := db.Query(sqlStr)
	if err != nil {
		return fmt.Errorf("query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	cols, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("columns: %w", err)
	}

	switch format {
	case "json":
		return writeJSON(rows, cols, w)
	case "csv":
		return writeCSV(rows, cols, w)
	case "table":
		return writeTable(rows, cols, w)
	default:
		return fmt.Errorf("unknown format: %s", format)
	}
}

func scanRow(
	rows *sql.Rows, ncols int,
) ([]interface{}, error) {
	vals := make([]interface{}, ncols)
	ptrs := make([]interface{}, ncols)
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	if err := rows.Scan(ptrs...); err != nil {
		return nil, err
	}
	for i, v := range vals {
		if b, ok := v.([]byte); ok {
			vals[i] = string(b)
		}
	}
	return vals, nil
}

func writeJSON(
	rows *sql.Rows, cols []string, w io.Writer,
) error {
	result := query.QueryResult{Columns: cols}
	for rows.Next() {
		vals, err := scanRow(rows, len(cols))
		if err != nil {
			return fmt.Errorf("scan: %w", err)
		}
		result.Rows = append(result.Rows, vals)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("rows: %w", err)
	}
	result.RowCount = len(result.Rows)

	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(result)
}

func writeCSV(
	rows *sql.Rows, cols []string, w io.Writer,
) error {
	cw := csv.NewWriter(w)
	if err := cw.Write(cols); err != nil {
		return err
	}
	for rows.Next() {
		vals, err := scanRow(rows, len(cols))
		if err != nil {
			return fmt.Errorf("scan: %w", err)
		}
		record := make([]string, len(vals))
		for i, v := range vals {
			record[i] = fmt.Sprintf("%v", v)
		}
		if err := cw.Write(record); err != nil {
			return err
		}
	}
	cw.Flush()
	return cw.Error()
}

func writeTable(
	rows *sql.Rows, cols []string, w io.Writer,
) error {
	// Collect all rows first for column width calculation
	var allRows [][]string
	for rows.Next() {
		vals, err := scanRow(rows, len(cols))
		if err != nil {
			return fmt.Errorf("scan: %w", err)
		}
		record := make([]string, len(vals))
		for i, v := range vals {
			record[i] = fmt.Sprintf("%v", v)
		}
		allRows = append(allRows, record)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("rows: %w", err)
	}

	// Calculate column widths
	widths := make([]int, len(cols))
	for i, c := range cols {
		widths[i] = len(c)
	}
	for _, row := range allRows {
		for i, v := range row {
			if len(v) > widths[i] {
				widths[i] = len(v)
			}
		}
	}

	// Print header
	for i, c := range cols {
		if i > 0 {
			fmt.Fprint(w, " | ")
		}
		fmt.Fprintf(w, "%-*s", widths[i], c)
	}
	fmt.Fprintln(w)

	// Print separator
	for i, width := range widths {
		if i > 0 {
			fmt.Fprint(w, "-+-")
		}
		fmt.Fprint(w, strings.Repeat("-", width))
	}
	fmt.Fprintln(w)

	// Print rows
	for _, row := range allRows {
		for i, v := range row {
			if i > 0 {
				fmt.Fprint(w, " | ")
			}
			fmt.Fprintf(w, "%-*s", widths[i], v)
		}
		fmt.Fprintln(w)
	}

	fmt.Fprintf(w, "(%d rows)\n", len(allRows))
	return nil
}
```

Note: the test file needs `"bytes"` and `"strings"` imports added.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./cmd/msgvault/cmd/ -run TestQueryCommand -v -tags fts5`
Expected: PASS

- [ ] **Step 5: Verify build succeeds**

Run: `go build ./cmd/msgvault/`
Expected: Success

- [ ] **Step 6: Commit**

```bash
git add cmd/msgvault/cmd/query.go cmd/msgvault/cmd/query_test.go
git commit -m "feat: add 'msgvault query' CLI command with json/csv/table output"
```

---

### Task 5: HTTP Endpoint

**Files:**
- Modify: `internal/api/handlers.go` (add handleQuery)
- Modify: `internal/api/server.go` (register route)
- Modify: `internal/api/handlers_test.go` (add test)

- [ ] **Step 1: Write failing test for query endpoint**

Add to `internal/api/handlers_test.go`:

```go
func TestHandleQuery(t *testing.T) {
	// Build a DuckDB engine with test data
	builder := query.NewTestDataBuilder(t)
	srcID := builder.AddSource("test@example.com")
	bob := builder.AddParticipant(
		"bob@example.com", "example.com", "Bob",
	)
	msgID := builder.AddMessage(query.MessageOpt{
		Subject:      "Test",
		SourceID:     srcID,
		SizeEstimate: 100,
	})
	builder.AddFrom(msgID, bob, "Bob")
	engine := builder.BuildEngine()
	defer func() { _ = engine.Close() }()

	srv := NewServerWithOptions(ServerOptions{
		Config: testConfig(t),
		Engine: engine,
		Logger: slog.Default(),
	})

	body := `{"sql": "SELECT from_email FROM v_senders LIMIT 1"}`
	req := httptest.NewRequest("POST", "/api/v1/query",
		strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+testAPIKey)
	w := httptest.NewRecorder()
	srv.router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200\nbody: %s",
			w.Code, w.Body.String())
	}

	var result query.QueryResult
	if err := json.NewDecoder(w.Body).Decode(&result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if result.RowCount != 1 {
		t.Errorf("row_count = %d, want 1", result.RowCount)
	}
}

func TestHandleQuery_SQLiteEngine503(t *testing.T) {
	// SQLiteEngine does not implement SQLQuerier — expect 503
	srv := NewServerWithOptions(ServerOptions{
		Config: testConfig(t),
		Engine: query.NewSQLiteEngine(nil),
		Logger: slog.Default(),
	})

	body := `{"sql": "SELECT 1"}`
	req := httptest.NewRequest("POST", "/api/v1/query",
		strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+testAPIKey)
	w := httptest.NewRecorder()
	srv.router.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", w.Code)
	}
}
```

Check the test file to find how `testConfig` and `testAPIKey` are
defined — adapt the above to match the existing test patterns.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/api/ -run TestHandleQuery -v -tags fts5`
Expected: FAIL — handler not defined

- [ ] **Step 3: Implement handleQuery handler**

Add to `internal/api/handlers.go`:

```go
// queryRequest represents a SQL query request.
type queryRequest struct {
	SQL string `json:"sql"`
}

// handleQuery executes a raw SQL query against the DuckDB views.
// POST /api/v1/query
//
// Returns 503 if the engine does not support SQL queries (SQLite
// fallback mode).
func (s *Server) handleQuery(
	w http.ResponseWriter, r *http.Request,
) {
	querier, ok := s.engine.(query.SQLQuerier)
	if !ok {
		writeError(w, http.StatusServiceUnavailable,
			"engine_unavailable",
			"SQL query requires DuckDB engine "+
				"(analytics cache may not be built)")
		return
	}

	var req queryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest,
			"invalid_json", "Invalid request body")
		return
	}
	if req.SQL == "" {
		writeError(w, http.StatusBadRequest,
			"missing_sql", "Field 'sql' is required")
		return
	}

	result, err := querier.QuerySQL(r.Context(), req.SQL)
	if err != nil {
		writeError(w, http.StatusBadRequest,
			"query_error", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, result)
}
```

- [ ] **Step 4: Register route in server.go**

In `internal/api/server.go`'s `setupRouter()`, add within the
authenticated API group alongside the other engine-dependent endpoints:

```go
r.Post("/query", s.handleQuery)
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./internal/api/ -run TestHandleQuery -v -tags fts5`
Expected: PASS

Run: `go test ./internal/api/ -v -tags fts5 -count=1`
Expected: All API tests pass

- [ ] **Step 6: Commit**

```bash
git add internal/api/handlers.go internal/api/server.go \
    internal/api/handlers_test.go
git commit -m "feat: add POST /api/v1/query endpoint for raw SQL"
```

---

### Task 6: Claude Code Skill

**Files:**
- Create: `skills/claude-code/SKILL.md`
- Create: `skills/claude-code/references/views.md`

- [ ] **Step 1: Write SKILL.md**

```markdown
---
name: msgvault-query
description: Query msgvault email archive analytics via SQL views
triggers:
  - msgvault
  - email archive
  - email search
  - email analytics
  - sender analysis
  - domain analysis
---

# msgvault Query Skill

Query a local email archive using SQL against DuckDB views.

## Quick Start

```bash
# Ensure analytics cache is built
msgvault build-cache

# Query the archive
msgvault query "SELECT * FROM v_senders ORDER BY message_count DESC LIMIT 20"
msgvault query --format table "SELECT * FROM v_labels ORDER BY message_count DESC"
msgvault query --format csv "SELECT * FROM v_messages WHERE from_domain = 'example.com' LIMIT 100"
```

## Available Views

See `references/views.md` for full schema. Summary:

**Base views** (raw Parquet tables):
`messages`, `participants`, `message_recipients`, `labels`,
`message_labels`, `attachments`, `conversations`, `sources`

**Convenience views** (pre-joined):
- `v_messages` — messages with sender info + labels as JSON
- `v_senders` — per-sender aggregates
- `v_domains` — per-domain aggregates
- `v_labels` — label name + count
- `v_threads` — per-conversation aggregates

## Output Formats

- `--format json` (default) — structured JSON
- `--format csv` — CSV with header row
- `--format table` — aligned text table

## Common Queries

Top senders by volume:
```sql
SELECT from_email, from_name, message_count, total_size
FROM v_senders ORDER BY message_count DESC LIMIT 20
```

Domain breakdown:
```sql
SELECT domain, message_count, sender_count
FROM v_domains ORDER BY message_count DESC LIMIT 20
```

Messages from a domain in a date range:
```sql
SELECT subject, from_email, sent_at
FROM v_messages
WHERE from_domain = 'example.com'
  AND year = 2024
ORDER BY sent_at DESC LIMIT 50
```

Label distribution:
```sql
SELECT name, message_count, total_size
FROM v_labels ORDER BY message_count DESC
```

Thread analysis (busiest conversations):
```sql
SELECT conversation_title, message_count,
       participant_emails, first_message_at, last_message_at
FROM v_threads ORDER BY message_count DESC LIMIT 20
```

Large messages with attachments:
```sql
SELECT subject, from_email, size_estimate, sent_at
FROM v_messages
WHERE has_attachments = true
ORDER BY size_estimate DESC LIMIT 20
```

Messages per month:
```sql
SELECT year, month, COUNT(*) AS count
FROM messages
GROUP BY year, month
ORDER BY year, month
```

## CLI Commands (non-SQL)

For operations beyond analytics queries:

```bash
msgvault search "from:alice@example.com"     # Full-text search
msgvault sync-full alice@example.com          # Sync from Gmail
msgvault sync-incremental alice@example.com   # Incremental sync
msgvault stats                                # Archive summary
msgvault tui                                  # Interactive TUI
```

## Tips

- Use `WHERE year = YYYY` to leverage Hive partitioning
- Labels column in v_messages is JSON: use
  `json_array_contains(labels, 'INBOX')` to filter
- Sender resolution uses email recipients first, falls back to
  direct sender_id for chat sources
- All queries are read-only (Parquet files)
```

- [ ] **Step 2: Write references/views.md**

```markdown
# View Schema Reference

## Base Views

### messages
Hive-partitioned by year. One row per message.

| Column | Type | Description |
|--------|------|-------------|
| id | BIGINT | Message ID |
| conversation_id | BIGINT | Thread/conversation ID |
| source_id | BIGINT | Account/source ID |
| source_message_id | VARCHAR | Gmail message ID |
| subject | VARCHAR | Subject line |
| snippet | VARCHAR | Body preview |
| sent_at | TIMESTAMP | Send timestamp |
| size_estimate | BIGINT | Size in bytes |
| has_attachments | BOOLEAN | Has attachments flag |
| attachment_count | INTEGER | Number of attachments |
| sender_id | BIGINT | Direct sender (chat sources) |
| message_type | VARCHAR | Source type (email, etc.) |
| deleted_from_source_at | TIMESTAMP | Deletion timestamp |
| year | INTEGER | Partition key |
| month | INTEGER | Month (1-12) |

### participants
| Column | Type | Description |
|--------|------|-------------|
| id | BIGINT | Participant ID |
| email_address | VARCHAR | Email address |
| domain | VARCHAR | Email domain |
| display_name | VARCHAR | Display name |
| phone_number | VARCHAR | Phone (chat sources) |

### message_recipients
| Column | Type | Description |
|--------|------|-------------|
| message_id | BIGINT | FK to messages |
| participant_id | BIGINT | FK to participants |
| recipient_type | VARCHAR | from, to, cc, bcc |
| display_name | VARCHAR | Per-message display name |

### labels
| Column | Type | Description |
|--------|------|-------------|
| id | BIGINT | Label ID |
| name | VARCHAR | Label name |

### message_labels
| Column | Type | Description |
|--------|------|-------------|
| message_id | BIGINT | FK to messages |
| label_id | BIGINT | FK to labels |

### attachments
| Column | Type | Description |
|--------|------|-------------|
| message_id | BIGINT | FK to messages |
| size | BIGINT | Size in bytes |
| filename | VARCHAR | File name |

### conversations
| Column | Type | Description |
|--------|------|-------------|
| id | BIGINT | Conversation ID |
| source_conversation_id | VARCHAR | Gmail thread ID |
| title | VARCHAR | Thread title |
| conversation_type | VARCHAR | email, whatsapp, etc. |

### sources
| Column | Type | Description |
|--------|------|-------------|
| id | BIGINT | Source/account ID |
| source_type | VARCHAR | gmail, imap, etc. |

## Convenience Views

### v_messages
Messages with sender info and labels pre-joined.

| Column | Type | Description |
|--------|------|-------------|
| id | BIGINT | Message ID |
| conversation_id | BIGINT | Thread ID |
| source_id | BIGINT | Account ID |
| source_message_id | VARCHAR | Gmail message ID |
| subject | VARCHAR | Subject line |
| snippet | VARCHAR | Body preview |
| sent_at | TIMESTAMP | Send timestamp |
| size_estimate | BIGINT | Size in bytes |
| has_attachments | BOOLEAN | Has attachments |
| attachment_count | INTEGER | Attachment count |
| year | INTEGER | Partition year |
| month | INTEGER | Month |
| message_type | VARCHAR | Source type |
| deleted_from_source_at | TIMESTAMP | Deletion time |
| from_email | VARCHAR | Sender email |
| from_name | VARCHAR | Sender display name |
| from_phone | VARCHAR | Sender phone |
| from_domain | VARCHAR | Sender domain |
| labels | VARCHAR | JSON array of label names |

### v_senders
Per-sender aggregate statistics.

| Column | Type | Description |
|--------|------|-------------|
| from_email | VARCHAR | Sender email |
| from_name | VARCHAR | Display name |
| from_domain | VARCHAR | Domain |
| message_count | BIGINT | Total messages |
| total_size | BIGINT | Total size (bytes) |
| attachment_size | BIGINT | Attachment size |
| attachment_count | BIGINT | Attachment count |
| first_message_at | TIMESTAMP | Earliest message |
| last_message_at | TIMESTAMP | Latest message |

### v_domains
Per-domain aggregate statistics.

| Column | Type | Description |
|--------|------|-------------|
| domain | VARCHAR | Sender domain |
| message_count | BIGINT | Total messages |
| total_size | BIGINT | Total size (bytes) |
| sender_count | BIGINT | Unique senders |

### v_labels
Label statistics.

| Column | Type | Description |
|--------|------|-------------|
| name | VARCHAR | Label name |
| message_count | BIGINT | Messages with label |
| total_size | BIGINT | Total size (bytes) |

### v_threads
Per-conversation aggregate statistics.

| Column | Type | Description |
|--------|------|-------------|
| conversation_id | BIGINT | Conversation ID |
| source_conversation_id | VARCHAR | Gmail thread ID |
| conversation_title | VARCHAR | Thread title |
| conversation_type | VARCHAR | Type (email, etc.) |
| message_count | BIGINT | Messages in thread |
| first_message_at | TIMESTAMP | Earliest message |
| last_message_at | TIMESTAMP | Latest message |
| participant_emails | VARCHAR | JSON array of emails |
```

- [ ] **Step 3: Commit**

```bash
mkdir -p skills/claude-code/references
git add skills/claude-code/SKILL.md \
    skills/claude-code/references/views.md
git commit -m "feat: add Claude Code skill for SQL query interface"
```

---

### Task 7: Final Verification

- [ ] **Step 1: Run full test suite**

Run: `make test`
Expected: All tests pass

- [ ] **Step 2: Run linter**

Run: `make lint`
Expected: No errors

- [ ] **Step 3: Build binary and smoke test**

```bash
make build
./msgvault query --help
```

Expected: Help text shows query command with `--format` flag

- [ ] **Step 4: Run go vet**

Run: `go vet ./...`
Expected: No issues
