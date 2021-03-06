package mssql

import (
	"github.com/go-rel/rel"
	"github.com/go-rel/rel/adapter/sql"
)

// FilterSQL builder.
type FilterSQL struct {
	fieldSQL    FieldSQL
	placeholder string
}

// Write SQL to buffer.
func (fs FilterSQL) Write(buffer *sql.Buffer, filter rel.FilterQuery) {
	switch filter.Type {
	case rel.FilterAndOp:
		fs.BuildLogical(buffer, "AND", filter.Inner)
	case rel.FilterOrOp:
		fs.BuildLogical(buffer, "OR", filter.Inner)
	case rel.FilterNotOp:
		buffer.WriteString("NOT ")
		fs.BuildLogical(buffer, "AND", filter.Inner)
	case rel.FilterEqOp,
		rel.FilterNeOp,
		rel.FilterLtOp,
		rel.FilterLteOp,
		rel.FilterGtOp,
		rel.FilterGteOp:
		fs.BuildComparison(buffer, filter)
	case rel.FilterNilOp:
		buffer.WriteString(fs.fieldSQL.Build(filter.Field))
		buffer.WriteString(" IS NULL")
	case rel.FilterNotNilOp:
		buffer.WriteString(fs.fieldSQL.Build(filter.Field))
		buffer.WriteString(" IS NOT NULL")
	case rel.FilterInOp,
		rel.FilterNinOp:
		fs.BuildInclusion(buffer, filter)
	case rel.FilterLikeOp:
		buffer.WriteString(fs.fieldSQL.Build(filter.Field))
		buffer.WriteString(" LIKE ")
		buffer.WriteString(fs.placeholder)
		buffer.Append(filter.Value)
	case rel.FilterNotLikeOp:
		buffer.WriteString(fs.fieldSQL.Build(filter.Field))
		buffer.WriteString(" NOT LIKE ")
		buffer.WriteString(fs.placeholder)
		buffer.Append(filter.Value)
	case rel.FilterFragmentOp:
		buffer.WriteString(filter.Field)
		buffer.Append(filter.Value.([]interface{})...)
	}
}

// BuildLogical SQL to buffer.
func (fs FilterSQL) BuildLogical(buffer *sql.Buffer, op string, inner []rel.FilterQuery) {
	var (
		length = len(inner)
	)

	if length > 1 {
		buffer.WriteByte('(')
	}

	for i, c := range inner {
		fs.Write(buffer, c)

		if i < length-1 {
			buffer.WriteByte(' ')
			buffer.WriteString(op)
			buffer.WriteByte(' ')
		}
	}

	if length > 1 {
		buffer.WriteByte(')')
	}
}

// BuildComparison SQL to buffer.
func (fs FilterSQL) BuildComparison(buffer *sql.Buffer, filter rel.FilterQuery) {
	buffer.WriteString(fs.fieldSQL.Build(filter.Field))

	switch filter.Type {
	case rel.FilterEqOp:
		buffer.WriteByte('=')
	case rel.FilterNeOp:
		buffer.WriteString("<>")
	case rel.FilterLtOp:
		buffer.WriteByte('<')
	case rel.FilterLteOp:
		buffer.WriteString("<=")
	case rel.FilterGtOp:
		buffer.WriteByte('>')
	case rel.FilterGteOp:
		buffer.WriteString(">=")
	}

	buffer.WriteString(fs.placeholder)
	buffer.Append(filter.Value)
}

// BuildInclusion SQL to buffer.
func (fs FilterSQL) BuildInclusion(buffer *sql.Buffer, filter rel.FilterQuery) {
	var (
		values = filter.Value.([]interface{})
	)

	buffer.WriteString(fs.fieldSQL.Build(filter.Field))

	if filter.Type == rel.FilterInOp {
		buffer.WriteString(" IN (")
	} else {
		buffer.WriteString(" NOT IN (")
	}

	buffer.WriteString(fs.placeholder)
	for i := 1; i <= len(values)-1; i++ {
		buffer.WriteByte(',')
		buffer.WriteString(fs.placeholder)
	}
	buffer.WriteByte(')')
	buffer.Append(values...)
}

// NewFilterSQL builder.
func NewFilterSQL(fieldSQL FieldSQL, placeholder string) FilterSQL {
	return FilterSQL{
		fieldSQL:    fieldSQL,
		placeholder: placeholder,
	}
}
