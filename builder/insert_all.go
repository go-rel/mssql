package builder

import (
	"github.com/go-rel/rel"
	"github.com/go-rel/sql/builder"
)

// InsertAll builder.
type InsertAll struct {
	BufferFactory builder.BufferFactory
}

// Build SQL string and its arguments.
func (ia InsertAll) Build(table string, primaryField string, fields []string, bulkMutates []map[string]rel.Mutate) (string, []interface{}) {
	var (
		buffer         = ia.BufferFactory.Create()
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
		buffer.WriteEscape(table)
		buffer.WriteString(" ON; ")
	}

	buffer.WriteString("INSERT INTO ")
	buffer.WriteEscape(table)
	buffer.WriteString(" (")

	for i := range fields {
		if i > 0 {
			buffer.WriteByte(',')
		}

		buffer.WriteEscape(fields[i])
	}

	buffer.WriteString(")")

	if primaryField != "" {
		buffer.WriteString(" OUTPUT [INSERTED].")
		buffer.WriteEscape(primaryField)
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
		buffer.WriteEscape(table)
		buffer.WriteString(" OFF; ")
	}

	return buffer.String(), buffer.Arguments()
}
