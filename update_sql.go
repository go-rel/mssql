package mssql

import (
	"github.com/go-rel/rel"
	"github.com/go-rel/rel/adapter/sql"
)

// UpdateSQL builder.
type UpdateSQL struct {
	fieldSQL    FieldSQL
	filterSQL   FilterSQL
	placeholder string
}

// Build SQL string and it arguments.
func (us UpdateSQL) Build(table string, primaryField string, mutates map[string]rel.Mutate, filter rel.FilterQuery) (string, []interface{}) {
	var (
		buffer sql.Buffer
	)

	buffer.WriteString("UPDATE ")
	buffer.WriteString(us.fieldSQL.Build(table))
	buffer.WriteString(" SET ")

	i := 0
	for field, mut := range mutates {
		if field == primaryField {
			continue
		}

		if i > 0 {
			buffer.WriteByte(',')
		}
		i++

		switch mut.Type {
		case rel.ChangeSetOp:
			buffer.WriteString(us.fieldSQL.Build(field))
			buffer.WriteByte('=')
			buffer.WriteString(us.placeholder)
			buffer.Append(mut.Value)
		case rel.ChangeIncOp:
			buffer.WriteString(us.fieldSQL.Build(field))
			buffer.WriteByte('=')
			buffer.WriteString(us.fieldSQL.Build(field))
			buffer.WriteByte('+')
			buffer.WriteString(us.placeholder)
			buffer.Append(mut.Value)
		case rel.ChangeFragmentOp:
			buffer.WriteString(field)
			buffer.Append(mut.Value.([]interface{})...)
		}
	}

	if !filter.None() {
		buffer.WriteString(" WHERE ")
		us.filterSQL.Write(&buffer, filter)
	}

	buffer.WriteString(";")

	return buffer.String(), buffer.Arguments
}

// NewUpdateSQL builder.
func NewUpdateSQL(fieldSQL FieldSQL, filterSQL FilterSQL, placeholder string) UpdateSQL {
	return UpdateSQL{
		fieldSQL:    fieldSQL,
		filterSQL:   filterSQL,
		placeholder: placeholder,
	}
}
