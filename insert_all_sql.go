package mssql

import (
	"github.com/go-rel/rel"
	"github.com/go-rel/rel/adapter/sql"
)

// InsertAllSQL builder.
type InsertAllSQL struct {
	fieldSQL    FieldSQL
	placeholder string
}

// Build SQL string and its arguments.
func (ias InsertAllSQL) Build(table string, primaryField string, fields []string, bulkMutates []map[string]rel.Mutate) (string, []interface{}) {
	var (
		buffer         sql.Buffer
		fieldsCount    = len(fields)
		mutatesCount   = len(bulkMutates)
		identityInsert = false
	)

	for i := range fields {
		if primaryField == fields[i] {
			identityInsert = true
			break
		}
	}

	buffer.Arguments = make([]interface{}, 0, fieldsCount*mutatesCount)

	if identityInsert {
		buffer.WriteString("SET IDENTITY_INSERT ")
		buffer.WriteString(ias.fieldSQL.Build(table))
		buffer.WriteString(" ON; ")
	}

	buffer.WriteString("INSERT INTO ")
	buffer.WriteString(ias.fieldSQL.Build(table))
	buffer.WriteString(" (")

	for i := range fields {
		buffer.WriteString(ias.fieldSQL.Build(fields[i]))

		if i < fieldsCount-1 {
			buffer.WriteByte(',')
		}
	}

	buffer.WriteString(")")

	if primaryField != "" {
		buffer.WriteString(" OUTPUT [INSERTED].")
		buffer.WriteString(ias.fieldSQL.Build(primaryField))
	}

	buffer.WriteString(" VALUES ")

	for i, mutates := range bulkMutates {
		buffer.WriteByte('(')

		for j, field := range fields {
			if mut, ok := mutates[field]; ok && mut.Type == rel.ChangeSetOp {
				buffer.WriteString(ias.placeholder)
				buffer.Append(mut.Value)
			} else {
				buffer.WriteString("DEFAULT")
			}

			if j < fieldsCount-1 {
				buffer.WriteByte(',')
			}
		}

		if i < mutatesCount-1 {
			buffer.WriteString("),")
		} else {
			buffer.WriteByte(')')
		}
	}

	buffer.WriteString(";")

	if identityInsert {
		buffer.WriteString(" SET IDENTITY_INSERT ")
		buffer.WriteString(ias.fieldSQL.Build(table))
		buffer.WriteString(" OFF; ")
	}

	return buffer.String(), buffer.Arguments
}

// NewInsertAllSQL builder.
func NewInsertAllSQL(fieldSQL FieldSQL, placeholder string) InsertAllSQL {
	return InsertAllSQL{
		fieldSQL:    fieldSQL,
		placeholder: placeholder,
	}
}
