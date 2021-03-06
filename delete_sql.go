package mssql

import (
	"github.com/go-rel/rel"
	"github.com/go-rel/rel/adapter/sql"
)

// DeleteSQL builder.
type DeleteSQL struct {
	fieldSQL  FieldSQL
	filterSQL FilterSQL
}

// Build SQL query and its arguments.
func (ds DeleteSQL) Build(table string, filter rel.FilterQuery) (string, []interface{}) {
	var buffer sql.Buffer

	buffer.WriteString("DELETE FROM ")
	buffer.WriteString(ds.fieldSQL.Build(table))

	if !filter.None() {
		buffer.WriteString(" WHERE ")
		ds.filterSQL.Write(&buffer, filter)
	}

	buffer.WriteString(";")

	return buffer.String(), buffer.Arguments
}

// NewDeleteSQL builder.
func NewDeleteSQL(fieldSQL FieldSQL, filterSQL FilterSQL) DeleteSQL {
	return DeleteSQL{
		fieldSQL:  fieldSQL,
		filterSQL: filterSQL,
	}
}
