package builder

import (
	"github.com/go-rel/rel"
	"github.com/go-rel/sql/builder"
)

// Index builder.
type Index struct {
	BufferFactory builder.BufferFactory
	Query         builder.QueryWriter
	Filter        builder.Filter
}

// Build sql query for index.
func (i Index) Build(index rel.Index) string {
	var (
		buffer = i.BufferFactory.Create()
	)

	switch index.Op {
	case rel.SchemaCreate:
		i.WriteCreateIndex(&buffer, index)
	case rel.SchemaDrop:
		i.WriteDropIndex(&buffer, index)
	}

	i.WriteOptions(&buffer, index.Options)
	buffer.WriteByte(';')

	return buffer.String()
}

// WriteCreateIndex to buffer
func (i Index) WriteCreateIndex(buffer *builder.Buffer, index rel.Index) {
	buffer.WriteString("CREATE ")
	if index.Unique {
		buffer.WriteString("UNIQUE NONCLUSTERED ")
	}
	buffer.WriteString("INDEX ")

	if index.Optional {
		buffer.WriteString("IF NOT EXISTS ")
	}

	buffer.WriteEscape(index.Name)
	buffer.WriteString(" ON ")
	buffer.WriteEscape(index.Table)

	buffer.WriteString(" (")
	for i, col := range index.Columns {
		if i > 0 {
			buffer.WriteString(", ")
		}
		buffer.WriteEscape(col)
	}
	buffer.WriteString(")")

	if index.Unique {
		buffer.WriteString(" WHERE ")
		for i, col := range index.Columns {
			if i > 0 {
				buffer.WriteString(" AND ")
			}
			buffer.WriteEscape(col)
			buffer.WriteString(" IS NOT NULL")
		}
	}
	if !index.Filter.None() {
		if index.Unique {
			buffer.WriteString(" AND ")
		} else {
			buffer.WriteString(" WHERE ")
		}
		i.Filter.Write(buffer, "", index.Filter, i.Query)
	}
}

// WriteDropIndex to buffer
func (i Index) WriteDropIndex(buffer *builder.Buffer, index rel.Index) {
	buffer.WriteString("DROP INDEX ")

	if index.Optional {
		buffer.WriteString("IF EXISTS ")
	}

	buffer.WriteEscape(index.Name)
	buffer.WriteString(" ON ")
	buffer.WriteEscape(index.Table)
}

// WriteOptions sql to buffer.
func (i Index) WriteOptions(buffer *builder.Buffer, options string) {
	if options == "" {
		return
	}

	buffer.WriteByte(' ')
	buffer.WriteString(options)
}
