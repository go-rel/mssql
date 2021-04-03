package mssql

import (
	"github.com/go-rel/rel"
)

// DeleteSQL builder.
type DeleteSQL struct {
	fieldSQL  FieldSQL
	querySQL  QuerySQL
	filterSQL FilterSQL
}

// Build SQL query and its arguments.
func (ds DeleteSQL) Build(table string, filter rel.FilterQuery) (string, []interface{}) {
	var buffer buffer

	buffer.WriteString("DELETE FROM ")
	buffer.WriteString(ds.fieldSQL.Build(table))

	if !filter.None() {
		buffer.WriteString(" WHERE ")
		ds.filterSQL.Write(&buffer, filter, ds.querySQL)
	}

	buffer.WriteString(";")

	return buffer.String(), buffer.Arguments()
}

// NewDeleteSQL builder.
func NewDeleteSQL(fieldSQL FieldSQL, querySQL QuerySQL, filterSQL FilterSQL) DeleteSQL {
	return DeleteSQL{
		fieldSQL:  fieldSQL,
		querySQL:  querySQL,
		filterSQL: filterSQL,
	}
}
