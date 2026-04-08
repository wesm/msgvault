package query

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
)

// probeColumns checks which columns exist in a Parquet file.
// Returns a set of column names present in the schema.
// On any error, returns an empty map (callers supply defaults).
func probeColumns(
	db *sql.DB, pathPattern string, hivePartitioning bool,
) map[string]bool {
	cols := make(map[string]bool)
	hiveOpt := ""
	if hivePartitioning {
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

// viewDef holds the parameters needed to create one DuckDB view
// over a Parquet table.
type viewDef struct {
	name             string
	pathPattern      string
	hivePartitioning bool
	replaceCols      []string
	optionalCols     []optionalCol
}

// optionalCol defines a column that may or may not exist in the
// Parquet schema. If present, replaceExpr is added to the REPLACE
// clause; if absent, defaultExpr is appended as an extra SELECT column.
type optionalCol struct {
	name        string
	replaceExpr string
	defaultExpr string
}

// buildViewSQL generates the CREATE OR REPLACE VIEW statement for
// a single view definition, using the probed column set to decide
// how to handle optional columns.
func buildViewSQL(def viewDef, probedCols map[string]bool) string {
	replace := make([]string, len(def.replaceCols))
	copy(replace, def.replaceCols)

	var extra []string
	for _, oc := range def.optionalCols {
		if probedCols[oc.name] {
			replace = append(replace, oc.replaceExpr)
		} else {
			extra = append(extra, oc.defaultExpr)
		}
	}

	hiveOpt := ""
	if def.hivePartitioning {
		hiveOpt = ", hive_partitioning=true, union_by_name=true"
	}
	escaped := strings.ReplaceAll(def.pathPattern, "'", "''")

	selectClause := fmt.Sprintf(
		"SELECT * REPLACE (%s)",
		strings.Join(replace, ", "),
	)
	if len(extra) > 0 {
		selectClause += ", " + strings.Join(extra, ", ")
	}

	return fmt.Sprintf(
		"CREATE OR REPLACE VIEW %s AS %s FROM read_parquet('%s'%s)",
		def.name, selectClause, escaped, hiveOpt,
	)
}

// RegisterViews creates DuckDB views over the Parquet files in
// analyticsDir. Each view normalises types and supplies defaults
// for optional columns that may be absent in older cache files.
func RegisterViews(db *sql.DB, analyticsDir string) error {
	msgGlob := filepath.Join(
		analyticsDir, "messages", "**", "*.parquet",
	)
	tablePath := func(name string) string {
		return filepath.Join(analyticsDir, name, "*.parquet")
	}

	// Probe schemas for optional columns.
	msgCols := probeColumns(db, msgGlob, true)
	partCols := probeColumns(db, tablePath("participants"), false)
	convCols := probeColumns(db, tablePath("conversations"), false)
	srcCols := probeColumns(db, tablePath("sources"), false)

	defs := []struct {
		def   viewDef
		probe map[string]bool
	}{
		{
			def: viewDef{
				name:             "messages",
				pathPattern:      msgGlob,
				hivePartitioning: true,
				replaceCols: []string{
					"CAST(id AS BIGINT) AS id",
					"CAST(source_id AS BIGINT) AS source_id",
					"CAST(source_message_id AS VARCHAR) AS source_message_id",
					"CAST(conversation_id AS BIGINT) AS conversation_id",
					"CAST(subject AS VARCHAR) AS subject",
					"CAST(snippet AS VARCHAR) AS snippet",
					"CAST(size_estimate AS BIGINT) AS size_estimate",
					"COALESCE(TRY_CAST(has_attachments AS BOOLEAN), false) AS has_attachments",
				},
				optionalCols: []optionalCol{
					{
						name:        "attachment_count",
						replaceExpr: "COALESCE(TRY_CAST(attachment_count AS INTEGER), 0) AS attachment_count",
						defaultExpr: "0 AS attachment_count",
					},
					{
						name:        "sender_id",
						replaceExpr: "TRY_CAST(sender_id AS BIGINT) AS sender_id",
						defaultExpr: "NULL::BIGINT AS sender_id",
					},
					{
						name:        "message_type",
						replaceExpr: "COALESCE(CAST(message_type AS VARCHAR), '') AS message_type",
						defaultExpr: "'' AS message_type",
					},
				},
			},
			probe: msgCols,
		},
		{
			def: viewDef{
				name:        "participants",
				pathPattern: tablePath("participants"),
				replaceCols: []string{
					"CAST(id AS BIGINT) AS id",
					"CAST(email_address AS VARCHAR) AS email_address",
					"CAST(domain AS VARCHAR) AS domain",
					"CAST(display_name AS VARCHAR) AS display_name",
				},
				optionalCols: []optionalCol{
					{
						name:        "phone_number",
						replaceExpr: "COALESCE(CAST(phone_number AS VARCHAR), '') AS phone_number",
						defaultExpr: "'' AS phone_number",
					},
				},
			},
			probe: partCols,
		},
		{
			def: viewDef{
				name:        "message_recipients",
				pathPattern: tablePath("message_recipients"),
				replaceCols: []string{
					"CAST(message_id AS BIGINT) AS message_id",
					"CAST(participant_id AS BIGINT) AS participant_id",
					"CAST(recipient_type AS VARCHAR) AS recipient_type",
					"CAST(display_name AS VARCHAR) AS display_name",
				},
			},
		},
		{
			def: viewDef{
				name:        "labels",
				pathPattern: tablePath("labels"),
				replaceCols: []string{
					"CAST(id AS BIGINT) AS id",
					"CAST(name AS VARCHAR) AS name",
				},
			},
		},
		{
			def: viewDef{
				name:        "message_labels",
				pathPattern: tablePath("message_labels"),
				replaceCols: []string{
					"CAST(message_id AS BIGINT) AS message_id",
					"CAST(label_id AS BIGINT) AS label_id",
				},
			},
		},
		{
			def: viewDef{
				name:        "attachments",
				pathPattern: tablePath("attachments"),
				replaceCols: []string{
					"CAST(message_id AS BIGINT) AS message_id",
					"CAST(size AS BIGINT) AS size",
					"CAST(filename AS VARCHAR) AS filename",
				},
			},
		},
		{
			def: viewDef{
				name:        "conversations",
				pathPattern: tablePath("conversations"),
				replaceCols: []string{
					"CAST(id AS BIGINT) AS id",
					"CAST(source_conversation_id AS VARCHAR) AS source_conversation_id",
				},
				optionalCols: []optionalCol{
					{
						name:        "title",
						replaceExpr: "COALESCE(CAST(title AS VARCHAR), '') AS title",
						defaultExpr: "'' AS title",
					},
					{
						name:        "conversation_type",
						replaceExpr: "COALESCE(CAST(conversation_type AS VARCHAR), 'email') AS conversation_type",
						defaultExpr: "'email' AS conversation_type",
					},
				},
			},
			probe: convCols,
		},
		{
			def: viewDef{
				name:        "sources",
				pathPattern: tablePath("sources"),
				replaceCols: []string{
					"CAST(id AS BIGINT) AS id",
				},
				optionalCols: []optionalCol{
					{
						name:        "source_type",
						replaceExpr: "COALESCE(CAST(source_type AS VARCHAR), 'gmail') AS source_type",
						defaultExpr: "'gmail' AS source_type",
					},
				},
			},
			probe: srcCols,
		},
	}

	for _, d := range defs {
		probe := d.probe
		if probe == nil {
			probe = map[string]bool{}
		}
		stmt := buildViewSQL(d.def, probe)
		if _, err := db.Exec(stmt); err != nil {
			return fmt.Errorf("create view %s: %w", d.def.name, err)
		}
	}
	return nil
}
