package mssql

import (
	"github.com/go-rel/rel"
	"github.com/go-rel/rel/adapter/sql"
)

// InsertSQL builder.
type InsertSQL struct {
	fieldSQL    FieldSQL
	placeholder string
}

// Build sql query and its arguments.
func (is InsertSQL) Build(table string, primaryField string, mutates map[string]rel.Mutate) (string, []interface{}) {
	var (
		buffer            sql.Buffer
		count             = len(mutates)
		_, identityInsert = mutates[primaryField]
	)

	buffer.Arguments = make([]interface{}, count)

	if identityInsert {
		buffer.WriteString("SET IDENTITY_INSERT ")
		buffer.WriteString(is.fieldSQL.Build(table))
		buffer.WriteString(" ON; ")
	}

	buffer.WriteString("INSERT INTO ")
	buffer.WriteString(is.fieldSQL.Build(table))
	buffer.WriteString(" (")

	index := 0
	for field, mut := range mutates {
		if mut.Type == rel.ChangeSetOp {
			buffer.WriteString(is.fieldSQL.Build(field))
			buffer.Arguments[index] = mut.Value
		}

		if index < count-1 {
			buffer.WriteByte(',')
		}
		index++
	}

	buffer.WriteString(")")

	if primaryField != "" {
		buffer.WriteString(" OUTPUT [INSERTED].")
		buffer.WriteString(is.fieldSQL.Build(primaryField))
	}

	buffer.WriteString(" VALUES ")

	buffer.WriteByte('(')
	for index := 0; index < len(buffer.Arguments); index++ {
		buffer.WriteString(is.placeholder)

		if index < len(buffer.Arguments)-1 {
			buffer.WriteByte(',')
		}
	}
	buffer.WriteString(");")

	if identityInsert {
		buffer.WriteString(" SET IDENTITY_INSERT ")
		buffer.WriteString(is.fieldSQL.Build(table))
		buffer.WriteString(" OFF; ")
	}

	return buffer.String(), buffer.Arguments
}

// NewInsertSQL builder.
func NewInsertSQL(fieldSQL FieldSQL, placeholder string) InsertSQL {
	return InsertSQL{
		fieldSQL:    fieldSQL,
		placeholder: placeholder,
	}
}
