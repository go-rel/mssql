package mssql

import (
	"github.com/go-rel/rel"
)

// InsertSQL builder.
type InsertSQL struct {
	fieldSQL FieldSQL
}

// Build sql query and its arguments.
func (is InsertSQL) Build(table string, primaryField string, mutates map[string]rel.Mutate) (string, []interface{}) {
	var (
		buffer            buffer
		_, identityInsert = mutates[primaryField]
		arguments         = make([]interface{}, len(mutates))
	)

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
			if index > 0 {
				buffer.WriteByte(',')
			}

			buffer.WriteString(is.fieldSQL.Build(field))
			arguments[index] = mut.Value
			index++
		}
	}

	buffer.WriteString(")")

	if primaryField != "" {
		buffer.WriteString(" OUTPUT [INSERTED].")
		buffer.WriteString(is.fieldSQL.Build(primaryField))
	}

	buffer.WriteString(" VALUES (")

	for i := range arguments {
		if i > 0 {
			buffer.WriteByte(',')
		}

		buffer.WriteValue(arguments[i])
	}

	buffer.WriteString(");")

	if identityInsert {
		buffer.WriteString(" SET IDENTITY_INSERT ")
		buffer.WriteString(is.fieldSQL.Build(table))
		buffer.WriteString(" OFF; ")
	}

	return buffer.String(), buffer.Arguments()
}

// NewInsertSQL builder.
func NewInsertSQL(fieldSQL FieldSQL) InsertSQL {
	return InsertSQL{
		fieldSQL: fieldSQL,
	}
}
