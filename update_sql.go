package mssql

import (
	"github.com/go-rel/rel"
)

// UpdateSQL builder.
type UpdateSQL struct {
	fieldSQL  FieldSQL
	querySQL  QuerySQL
	filterSQL FilterSQL
}

// Build SQL string and it arguments.
func (us UpdateSQL) Build(table string, primaryField string, mutates map[string]rel.Mutate, filter rel.FilterQuery) (string, []interface{}) {
	var (
		buffer buffer
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
			buffer.WriteValue(mut.Value)
		case rel.ChangeIncOp:
			buffer.WriteString(us.fieldSQL.Build(field))
			buffer.WriteByte('=')
			buffer.WriteString(us.fieldSQL.Build(field))
			buffer.WriteByte('+')
			buffer.WriteValue(mut.Value)
		case rel.ChangeFragmentOp:
			buffer.WriteString(field)
			buffer.AddArguments(mut.Value.([]interface{})...)
		}
	}

	if !filter.None() {
		buffer.WriteString(" WHERE ")
		us.filterSQL.Write(&buffer, filter, us.querySQL)
	}

	buffer.WriteString(";")

	return buffer.String(), buffer.Arguments()
}

// NewUpdateSQL builder.
func NewUpdateSQL(fieldSQL FieldSQL, querySQL QuerySQL, filterSQL FilterSQL) UpdateSQL {
	return UpdateSQL{
		fieldSQL:  fieldSQL,
		querySQL:  querySQL,
		filterSQL: filterSQL,
	}
}
