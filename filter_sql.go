package mssql

import (
	"github.com/go-rel/rel"
)

// FilterSQL builder.
type FilterSQL struct {
	fieldSQL FieldSQL
}

// Write SQL to buffer.
func (fs FilterSQL) Write(buffer *buffer, filter rel.FilterQuery, qs QuerySQL) {
	switch filter.Type {
	case rel.FilterAndOp:
		fs.BuildLogical(buffer, "AND", filter.Inner, qs)
	case rel.FilterOrOp:
		fs.BuildLogical(buffer, "OR", filter.Inner, qs)
	case rel.FilterNotOp:
		buffer.WriteString("NOT ")
		fs.BuildLogical(buffer, "AND", filter.Inner, qs)
	case rel.FilterEqOp,
		rel.FilterNeOp,
		rel.FilterLtOp,
		rel.FilterLteOp,
		rel.FilterGtOp,
		rel.FilterGteOp:
		fs.BuildComparison(buffer, filter, qs)
	case rel.FilterNilOp:
		buffer.WriteString(fs.fieldSQL.Build(filter.Field))
		buffer.WriteString(" IS NULL")
	case rel.FilterNotNilOp:
		buffer.WriteString(fs.fieldSQL.Build(filter.Field))
		buffer.WriteString(" IS NOT NULL")
	case rel.FilterInOp,
		rel.FilterNinOp:
		fs.BuildInclusion(buffer, filter, qs)
	case rel.FilterLikeOp:
		buffer.WriteString(fs.fieldSQL.Build(filter.Field))
		buffer.WriteString(" LIKE ")
		buffer.WriteValue(filter.Value)
	case rel.FilterNotLikeOp:
		buffer.WriteString(fs.fieldSQL.Build(filter.Field))
		buffer.WriteString(" NOT LIKE ")
		buffer.WriteValue(filter.Value)
	case rel.FilterFragmentOp:
		buffer.WriteString(filter.Field)
		buffer.AddArguments(filter.Value.([]interface{})...)
	}
}

// BuildLogical SQL to buffer.
func (fs FilterSQL) BuildLogical(buffer *buffer, op string, inner []rel.FilterQuery, qs QuerySQL) {
	var (
		length = len(inner)
	)

	if length > 1 {
		buffer.WriteByte('(')
	}

	for i, c := range inner {
		fs.Write(buffer, c, qs)

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
func (fs FilterSQL) BuildComparison(buffer *buffer, filter rel.FilterQuery, qs QuerySQL) {
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

	switch v := filter.Value.(type) {
	case rel.SubQuery:
		// For warped sub-queries
		fs.buildSubQuery(buffer, v, qs)
	case rel.Query:
		// For sub-queries without warp
		fs.buildSubQuery(buffer, rel.SubQuery{Query: v}, qs)
	default:
		// For simple values
		buffer.WriteValue(filter.Value)
	}
}

// BuildInclusion SQL to buffer.
func (fs FilterSQL) BuildInclusion(buffer *buffer, filter rel.FilterQuery, qs QuerySQL) {
	var (
		values = filter.Value.([]interface{})
	)

	buffer.WriteString(fs.fieldSQL.Build(filter.Field))

	if filter.Type == rel.FilterInOp {
		buffer.WriteString(" IN ")
	} else {
		buffer.WriteString(" NOT IN ")
	}

	fs.buildInclusionValues(buffer, values, qs)
}

func (fs *FilterSQL) buildInclusionValues(buffer *buffer, values []interface{}, qs QuerySQL) {
	if len(values) == 1 {
		if value, ok := values[0].(rel.Query); ok {
			fs.buildSubQuery(buffer, rel.SubQuery{Query: value}, qs)
			return
		}
	}

	buffer.WriteByte('(')
	for i := 0; i < len(values); i++ {
		if i > 0 {
			buffer.WriteByte(',')
		}
		buffer.WriteValue(values[i])
	}
	buffer.WriteByte(')')
}

func (fs FilterSQL) buildSubQuery(buffer *buffer, sub rel.SubQuery, qs QuerySQL) {
	buffer.WriteString(sub.Prefix)
	buffer.WriteByte('(')
	qs.Write(buffer, sub.Query)
	buffer.WriteByte(')')
}

// NewFilterSQL builder.
func NewFilterSQL(fieldSQL FieldSQL) FilterSQL {
	return FilterSQL{
		fieldSQL: fieldSQL,
	}
}
