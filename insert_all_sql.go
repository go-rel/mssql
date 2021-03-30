package mssql

import (
	"github.com/go-rel/rel"
)

// InsertAllSQL builder.
type InsertAllSQL struct {
	fieldSQL FieldSQL
}

// Build SQL string and its arguments.
func (ias InsertAllSQL) Build(table string, primaryField string, fields []string, bulkMutates []map[string]rel.Mutate) (string, []interface{}) {
	var (
		buffer         buffer
		mutatesCount   = len(bulkMutates)
		identityInsert = false
	)

	for i := range fields {
		if primaryField == fields[i] {
			identityInsert = true
			break
		}
	}

	if identityInsert {
		buffer.WriteString("SET IDENTITY_INSERT ")
		buffer.WriteString(ias.fieldSQL.Build(table))
		buffer.WriteString(" ON; ")
	}

	buffer.WriteString("INSERT INTO ")
	buffer.WriteString(ias.fieldSQL.Build(table))
	buffer.WriteString(" (")

	for i := range fields {
		if i > 0 {
			buffer.WriteByte(',')
		}

		buffer.WriteString(ias.fieldSQL.Build(fields[i]))
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
			if j > 0 {
				buffer.WriteByte(',')
			}
			if mut, ok := mutates[field]; ok && mut.Type == rel.ChangeSetOp {
				buffer.WriteValue(mut.Value)
			} else {
				buffer.WriteString("DEFAULT")
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

	return buffer.String(), buffer.Arguments()
}

// NewInsertAllSQL builder.
func NewInsertAllSQL(fieldSQL FieldSQL) InsertAllSQL {
	return InsertAllSQL{
		fieldSQL: fieldSQL,
	}
}
