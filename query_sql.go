package mssql

import (
	"strconv"
	"strings"

	"github.com/go-rel/rel"
)

// QuerySQL builder.
type QuerySQL struct {
	fieldSQL  FieldSQL
	filterSQL FilterSQL
}

// Build SQL string and it arguments.
func (qs QuerySQL) Build(query rel.Query) (string, []interface{}) {
	var buffer buffer
	qs.Write(&buffer, query)
	buffer.WriteString(";")

	return buffer.String(), buffer.Arguments()
}

// Write SQL to buffer.
func (qs QuerySQL) Write(buffer *buffer, query rel.Query) {
	if query.SQLQuery.Statement != "" {
		buffer.WriteString(query.SQLQuery.Statement)
		buffer.AddArguments(query.SQLQuery.Values...)
		return
	}

	if len(query.SortQuery) == 0 && query.OffsetQuery > 0 && query.LimitQuery > 0 {
		query = query.Sort("^1")
	}

	qs.BuildSelect(buffer, query.SelectQuery, query.LimitQuery, query.OffsetQuery)
	qs.BuildQuery(buffer, query)
}

// BuildSelect SQL to buffer.
func (qs QuerySQL) BuildSelect(buffer *buffer, selectQuery rel.SelectQuery, limit rel.Limit, offset rel.Offset) {
	buffer.WriteString("SELECT")

	if selectQuery.OnlyDistinct {
		buffer.WriteString(" DISTINCT ")
	}

	if limit > 0 && offset == 0 {
		buffer.WriteString(" TOP ")
		buffer.WriteString(strconv.Itoa(int(limit)))
	}

	if len(selectQuery.Fields) > 0 {
		buffer.WriteByte(' ')

		l := len(selectQuery.Fields) - 1
		for i, f := range selectQuery.Fields {
			buffer.WriteString(qs.fieldSQL.Build(f))

			if i < l {
				buffer.WriteString(", ")
			}
		}
	} else {
		buffer.WriteString(" *")
	}
}

// BuildQuery SQL to buffer.
func (qs QuerySQL) BuildQuery(buffer *buffer, query rel.Query) {
	qs.BuildFrom(buffer, query.Table)
	qs.BuildJoin(buffer, query.Table, query.JoinQuery)
	qs.BuildWhere(buffer, query.WhereQuery)

	if len(query.GroupQuery.Fields) > 0 {
		qs.BuildGroupBy(buffer, query.GroupQuery.Fields)
		qs.BuildHaving(buffer, query.GroupQuery.Filter)
	}

	qs.BuildOrderBy(buffer, query.SortQuery)
	qs.BuildLimitOffset(buffer, query.LimitQuery, query.OffsetQuery)

	if query.LockQuery != "" {
		buffer.WriteByte(' ')
		buffer.WriteString(string(query.LockQuery))
	}
}

// BuildFrom SQL to buffer.
func (qs QuerySQL) BuildFrom(buffer *buffer, table string) {
	buffer.WriteString(" FROM ")
	buffer.WriteString(qs.fieldSQL.Build(table))
}

// BuildJoin SQL to buffer.
func (qs QuerySQL) BuildJoin(buffer *buffer, table string, joins []rel.JoinQuery) {
	if len(joins) == 0 {
		return
	}

	for _, join := range joins {
		var (
			from = qs.fieldSQL.Build(join.From)
			to   = qs.fieldSQL.Build(join.To)
		)

		// TODO: move this to core functionality, and infer join condition using assoc data.
		if join.Arguments == nil && (join.From == "" || join.To == "") {
			from = qs.fieldSQL.Build(table + "." + strings.TrimSuffix(join.Table, "s") + "_id")
			to = qs.fieldSQL.Build(join.Table + ".id")
		}

		buffer.WriteByte(' ')
		buffer.WriteString(join.Mode)
		buffer.WriteByte(' ')

		if join.Table != "" {
			buffer.WriteString(qs.fieldSQL.Build(join.Table))
			buffer.WriteString(" ON ")
			buffer.WriteString(from)
			buffer.WriteString("=")
			buffer.WriteString(to)
		}

		buffer.AddArguments(join.Arguments...)
	}
}

// BuildWhere SQL to buffer.
func (qs QuerySQL) BuildWhere(buffer *buffer, filter rel.FilterQuery) {
	if filter.None() {
		return
	}

	buffer.WriteString(" WHERE ")
	qs.filterSQL.Write(buffer, filter, qs)
}

// BuildGroupBy SQL to buffer.
func (qs QuerySQL) BuildGroupBy(buffer *buffer, fields []string) {
	buffer.WriteString(" GROUP BY ")

	l := len(fields) - 1
	for i, f := range fields {
		buffer.WriteString(qs.fieldSQL.Build(f))

		if i < l {
			buffer.WriteByte(',')
		}
	}
}

// BuildHaving SQL to buffer.
func (qs QuerySQL) BuildHaving(buffer *buffer, filter rel.FilterQuery) {
	if filter.None() {
		return
	}

	buffer.WriteString(" HAVING ")
	qs.filterSQL.Write(buffer, filter, qs)
}

// BuildOrderBy SQL to buffer.
func (qs QuerySQL) BuildOrderBy(buffer *buffer, orders []rel.SortQuery) {
	var (
		length = len(orders)
	)

	if length == 0 {
		return
	}

	buffer.WriteString(" ORDER BY ")
	for i, order := range orders {
		buffer.WriteString(qs.fieldSQL.Build(order.Field))

		if order.Asc() {
			buffer.WriteString(" ASC")
		} else {
			buffer.WriteString(" DESC")
		}

		if i < length-1 {
			buffer.WriteByte(',')
		}
	}
}

// BuildLimitOffset SQL to buffer.
func (qs QuerySQL) BuildLimitOffset(buffer *buffer, limit rel.Limit, offset rel.Offset) {
	if limit > 0 && offset > 0 {
		buffer.WriteString(" OFFSET ")
		buffer.WriteString(strconv.Itoa(int(offset)))
		buffer.WriteString(" ROWS FETCH NEXT ")
		buffer.WriteString(strconv.Itoa(int(limit)))
		buffer.WriteString(" ROWS ONLY")
	}
}

// NewQuerySQL builder.
func NewQuerySQL(fieldSQL FieldSQL, filterSQL FilterSQL) QuerySQL {
	return QuerySQL{
		fieldSQL:  fieldSQL,
		filterSQL: filterSQL,
	}
}
