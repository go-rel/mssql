package mssql

import (
	"encoding/json"
	"strconv"

	"github.com/go-rel/rel"
)

type ColumnMapper func(*rel.Column) (string, int, int)

// ApplyTableSQL builder.
type ApplyTableSQL struct {
	fieldSQL     FieldSQL
	columnMapper ColumnMapper
}

// Build SQL query for table creation and modification.
func (ats ApplyTableSQL) Build(table rel.Table) string {
	var buffer buffer

	switch table.Op {
	case rel.SchemaCreate:
		ats.WriteCreateTable(&buffer, table)
	case rel.SchemaAlter:
		ats.WriteAlterTable(&buffer, table)
	case rel.SchemaRename:
		ats.WriteRenameTable(&buffer, table)
	case rel.SchemaDrop:
		ats.WriteDropTable(&buffer, table)
	}

	return buffer.String()
}

// WriteCreateTable query to buffer.
func (ats ApplyTableSQL) WriteCreateTable(buffer *buffer, table rel.Table) {

	if table.Optional {
		buffer.WriteString("IF OBJECT_ID('")
		buffer.WriteString(ats.fieldSQL.Build(table.Name))
		buffer.WriteString("', 'U') IS NULL ")
	}

	buffer.WriteString("CREATE TABLE ")
	buffer.WriteString(ats.fieldSQL.Build(table.Name))
	buffer.WriteString(" (")

	for i, def := range table.Definitions {
		if i > 0 {
			buffer.WriteString(", ")
		}
		switch v := def.(type) {
		case rel.Column:
			ats.WriteColumn(buffer, v)
		case rel.Key:
			ats.WriteKey(buffer, v)
		case rel.Raw:
			buffer.WriteString(string(v))
		}
	}

	buffer.WriteByte(')')
	ats.WriteOptions(buffer, table.Options)
	buffer.WriteByte(';')
}

// WriteAlterTable query to buffer.
func (ats ApplyTableSQL) WriteAlterTable(buffer *buffer, table rel.Table) {
	for _, def := range table.Definitions {
		if v, ok := def.(rel.Column); ok && v.Op == rel.SchemaRename {
			buffer.WriteString("EXEC sp_rename ")
			buffer.WriteString(ats.fieldSQL.Build(table.Name))
			buffer.WriteString(", ")
			buffer.WriteString(ats.fieldSQL.Build(v.Name))
			buffer.WriteString(", ")
			buffer.WriteString(ats.fieldSQL.Build(v.Rename))
			buffer.WriteByte(';')
			continue
		}

		buffer.WriteString("ALTER TABLE ")
		buffer.WriteString(ats.fieldSQL.Build(table.Name))
		buffer.WriteByte(' ')

		switch v := def.(type) {
		case rel.Column:
			switch v.Op {
			case rel.SchemaCreate:
				buffer.WriteString("ADD ")
				ats.WriteColumn(buffer, v)
			case rel.SchemaDrop:
				buffer.WriteString("DROP COLUMN ")
				buffer.WriteString(ats.fieldSQL.Build(v.Name))
			}
		case rel.Key:
			// TODO: Rename and Drop, PR welcomed.
			switch v.Op {
			case rel.SchemaCreate:
				buffer.WriteString("ADD ")
				ats.WriteKey(buffer, v)
			}
		}

		ats.WriteOptions(buffer, table.Options)
		buffer.WriteByte(';')
	}
}

// WriteRenameTable query to buffer.
func (ats ApplyTableSQL) WriteRenameTable(buffer *buffer, table rel.Table) {
	buffer.WriteString("EXEC sp_rename ")
	buffer.WriteString(ats.fieldSQL.Build(table.Name))
	buffer.WriteString(", ")
	buffer.WriteString(ats.fieldSQL.Build(table.Rename))
	buffer.WriteByte(';')
}

// WriteDropTable query to buffer.
func (ats ApplyTableSQL) WriteDropTable(buffer *buffer, table rel.Table) {
	if table.Optional {
		buffer.WriteString("IF OBJECT_ID('")
		buffer.WriteString(ats.fieldSQL.Build(table.Name))
		buffer.WriteString("', 'U') IS NOT NULL ")
	}

	buffer.WriteString("DROP TABLE ")
	buffer.WriteString(ats.fieldSQL.Build(table.Name))
	buffer.WriteByte(';')
}

// WriteColumn definition to buffer.
func (ats ApplyTableSQL) WriteColumn(buffer *buffer, column rel.Column) {
	var (
		typ, m, n = ats.columnMapper(&column)
	)

	buffer.WriteString(ats.fieldSQL.Build(column.Name))
	buffer.WriteByte(' ')
	buffer.WriteString(typ)

	if m != 0 {
		buffer.WriteByte('(')
		buffer.WriteString(strconv.Itoa(m))

		if n != 0 {
			buffer.WriteByte(',')
			buffer.WriteString(strconv.Itoa(n))
		}

		buffer.WriteByte(')')
	}

	if column.Unique {
		buffer.WriteString(" UNIQUE")
	}

	if column.Required {
		buffer.WriteString(" NOT NULL")
	}

	if column.Default != nil {
		buffer.WriteString(" DEFAULT ")
		switch v := column.Default.(type) {
		case string:
			// TODO: single quote only required by postgres.
			buffer.WriteByte('\'')
			buffer.WriteString(v)
			buffer.WriteByte('\'')
		case bool:
			if v {
				buffer.WriteString("'1'")
			} else {
				buffer.WriteString("'0'")
			}
		default:
			// TODO: improve
			bytes, _ := json.Marshal(column.Default)
			buffer.Write(bytes)
		}
	}

	ats.WriteOptions(buffer, column.Options)
}

// WriteKey definition to buffer.
func (ats ApplyTableSQL) WriteKey(buffer *buffer, key rel.Key) {
	var (
		typ = string(key.Type)
	)

	buffer.WriteString(typ)

	if key.Name != "" {
		buffer.WriteByte(' ')
		buffer.WriteString(ats.fieldSQL.Build(key.Name))
	}

	buffer.WriteString(" (")
	for i, col := range key.Columns {
		if i > 0 {
			buffer.WriteString(", ")
		}
		buffer.WriteString(ats.fieldSQL.Build(col))
	}
	buffer.WriteString(")")

	if key.Type == rel.ForeignKey {
		buffer.WriteString(" REFERENCES ")
		buffer.WriteString(ats.fieldSQL.Build(key.Reference.Table))

		buffer.WriteString(" (")
		for i, col := range key.Reference.Columns {
			if i > 0 {
				buffer.WriteString(", ")
			}
			buffer.WriteString(ats.fieldSQL.Build(col))
		}
		buffer.WriteString(")")

		if onDelete := key.Reference.OnDelete; onDelete != "" {
			buffer.WriteString(" ON DELETE ")
			buffer.WriteString(onDelete)
		}

		if onUpdate := key.Reference.OnUpdate; onUpdate != "" {
			buffer.WriteString(" ON UPDATE ")
			buffer.WriteString(onUpdate)
		}
	}

	ats.WriteOptions(buffer, key.Options)
}

// WriteOptions sql to buffer.
func (ats ApplyTableSQL) WriteOptions(buffer *buffer, options string) {
	if options == "" {
		return
	}

	buffer.WriteByte(' ')
	buffer.WriteString(options)
}

// NewApplyTableSQL builder.
func NewApplyTableSQL(fieldSQL FieldSQL, columnMapper ColumnMapper) ApplyTableSQL {
	return ApplyTableSQL{
		fieldSQL:     fieldSQL,
		columnMapper: columnMapper,
	}
}
