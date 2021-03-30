package mssql

import (
	"github.com/go-rel/rel"
)

// ApplyIndexSQL builder.
type ApplyIndexSQL struct {
	fieldSQL FieldSQL
}

// Build sql query for index.
func (ais ApplyIndexSQL) Build(index rel.Index) string {
	var buffer buffer

	switch index.Op {
	case rel.SchemaCreate:
		ais.WriteCreateIndex(&buffer, index)
	case rel.SchemaDrop:
		ais.WriteDropIndex(&buffer, index)
	}

	ais.WriteOptions(&buffer, index.Options)
	buffer.WriteByte(';')

	return buffer.String()
}

// WriteCreateIndex to buffer
func (ais ApplyIndexSQL) WriteCreateIndex(buffer *buffer, index rel.Index) {
	buffer.WriteString("CREATE ")
	if index.Unique {
		buffer.WriteString("UNIQUE NONCLUSTERED ")
	}
	buffer.WriteString("INDEX ")

	if index.Optional {
		buffer.WriteString("IF NOT EXISTS ")
	}

	buffer.WriteString(ais.fieldSQL.Build(index.Name))
	buffer.WriteString(" ON ")
	buffer.WriteString(ais.fieldSQL.Build(index.Table))

	buffer.WriteString(" (")
	for i, col := range index.Columns {
		if i > 0 {
			buffer.WriteString(", ")
		}
		buffer.WriteString(ais.fieldSQL.Build(col))
	}
	buffer.WriteString(")")

	if index.Unique {
		buffer.WriteString(" WHERE ")
		for i, col := range index.Columns {
			if i > 0 {
				buffer.WriteString(" AND ")
			}
			buffer.WriteString(ais.fieldSQL.Build(col))
			buffer.WriteString(" IS NOT NULL")
		}
	}
}

// WriteDropIndex to buffer
func (ais ApplyIndexSQL) WriteDropIndex(buffer *buffer, index rel.Index) {
	buffer.WriteString("DROP INDEX ")

	if index.Optional {
		buffer.WriteString("IF EXISTS ")
	}

	buffer.WriteString(ais.fieldSQL.Build(index.Name))

	// if a.config.DropIndexOnTable {
	buffer.WriteString(" ON ")
	buffer.WriteString(ais.fieldSQL.Build(index.Table))
	// }
}

// WriteOptions sql to buffer.
func (ais ApplyIndexSQL) WriteOptions(buffer *buffer, options string) {
	if options == "" {
		return
	}

	buffer.WriteByte(' ')
	buffer.WriteString(options)
}

// NewApplyIndexSQL builder.
func NewApplyIndexSQL(fieldSQL FieldSQL) ApplyIndexSQL {
	return ApplyIndexSQL{
		fieldSQL: fieldSQL,
	}
}
