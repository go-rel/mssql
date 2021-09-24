package builder

import (
	"strconv"

	"github.com/go-rel/rel"
	"github.com/go-rel/sql/builder"
)

// Query builder.
type Query struct {
	builder.Query
}

// Build SQL string and it arguments.
func (q Query) Build(query rel.Query) (string, []interface{}) {
	var (
		buffer = q.BufferFactory.Create()
	)

	q.Write(&buffer, query)

	return buffer.String(), buffer.Arguments()
}

// Write SQL to buffer.
func (q Query) Write(buffer *builder.Buffer, query rel.Query) {
	if query.SQLQuery.Statement != "" {
		buffer.WriteString(query.SQLQuery.Statement)
		buffer.AddArguments(query.SQLQuery.Values...)
		return
	}

	if len(query.SortQuery) == 0 && query.OffsetQuery > 0 && query.LimitQuery > 0 {
		query = query.Sort("^1")
	}

	rootQuery := buffer.Len() == 0

	q.BuildSelect(buffer, query.SelectQuery, query.LimitQuery, query.OffsetQuery)
	q.BuildQuery(buffer, query)

	if rootQuery {
		buffer.WriteByte(';')
	}
}

// BuildSelect SQL to buffer.
func (q Query) BuildSelect(buffer *builder.Buffer, selectQuery rel.SelectQuery, limit rel.Limit, offset rel.Offset) {
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
			buffer.WriteEscape(f)

			if i < l {
				buffer.WriteString(", ")
			}
		}
	} else {
		buffer.WriteString(" *")
	}
}

// BuildQuery SQL to buffer.
func (q Query) BuildQuery(buffer *builder.Buffer, query rel.Query) {
	q.BuildFrom(buffer, query.Table)
	q.BuildJoin(buffer, query.Table, query.JoinQuery)
	q.BuildWhere(buffer, query.WhereQuery)

	if len(query.GroupQuery.Fields) > 0 {
		q.BuildGroupBy(buffer, query.GroupQuery.Fields)
		q.BuildHaving(buffer, query.GroupQuery.Filter)
	}

	q.BuildOrderBy(buffer, query.SortQuery)
	q.BuildLimitOffset(buffer, query.LimitQuery, query.OffsetQuery)

	if query.LockQuery != "" {
		buffer.WriteByte(' ')
		buffer.WriteString(string(query.LockQuery))
	}
}

// // BuildFrom SQL to buffer.
// func (q Query) BuildFrom(buffer *builder.Buffer, table string) {
// 	buffer.WriteString(" FROM ")
// 	buffer.WriteString(q.fieldSQL.Build(table))
// }

// BuildJoin SQL to buffer.
// func (q Query) BuildJoin(buffer *builder.Buffer, table string, joins []rel.JoinQuery) {
// 	if len(joins) == 0 {
// 		return
// 	}

// 	for _, join := range joins {
// 		var (
// 			from = q.fieldSQL.Build(join.From)
// 			to   = q.fieldSQL.Build(join.To)
// 		)

// 		// TODO: move this to core functionality, and infer join condition using assoc data.
// 		if join.Arguments == nil && (join.From == "" || join.To == "") {
// 			from = q.fieldSQL.Build(table + "." + strings.TrimSuffix(join.Table, "s") + "_id")
// 			to = q.fieldSQL.Build(join.Table + ".id")
// 		}

// 		buffer.WriteByte(' ')
// 		buffer.WriteString(join.Mode)
// 		buffer.WriteByte(' ')

// 		if join.Table != "" {
// 			buffer.WriteString(q.fieldSQL.Build(join.Table))
// 			buffer.WriteString(" ON ")
// 			buffer.WriteString(from)
// 			buffer.WriteString("=")
// 			buffer.WriteString(to)
// 		}

// 		buffer.AddArguments(join.Arguments...)
// 	}
// }

// BuildWhere SQL to buffer.
func (q Query) BuildWhere(buffer *builder.Buffer, filter rel.FilterQuery) {
	if filter.None() {
		return
	}

	buffer.WriteString(" WHERE ")
	q.Filter.Write(buffer, filter, q)
}

// // BuildGroupBy SQL to buffer.
// func (q Query) BuildGroupBy(buffer *builder.Buffer, fields []string) {
// 	buffer.WriteString(" GROUP BY ")

// 	l := len(fields) - 1
// 	for i, f := range fields {
// 		buffer.WriteString(q.fieldSQL.Build(f))

// 		if i < l {
// 			buffer.WriteByte(',')
// 		}
// 	}
// }

// BuildHaving SQL to buffer.
func (q Query) BuildHaving(buffer *builder.Buffer, filter rel.FilterQuery) {
	if filter.None() {
		return
	}

	buffer.WriteString(" HAVING ")
	q.Filter.Write(buffer, filter, q)
}

// BuildOrderBy SQL to buffer.
// func (q Query) BuildOrderBy(buffer *builder.Buffer, orders []rel.SortQuery) {
// 	var (
// 		length = len(orders)
// 	)

// 	if length == 0 {
// 		return
// 	}

// 	buffer.WriteString(" ORDER BY ")
// 	for i, order := range orders {
// 		buffer.WriteString(q.fieldSQL.Build(order.Field))

// 		if order.Asc() {
// 			buffer.WriteString(" ASC")
// 		} else {
// 			buffer.WriteString(" DESC")
// 		}

// 		if i < length-1 {
// 			buffer.WriteByte(',')
// 		}
// 	}
// }

// BuildLimitOffset SQL to buffer.
func (q Query) BuildLimitOffset(buffer *builder.Buffer, limit rel.Limit, offset rel.Offset) {
	if limit > 0 && offset > 0 {
		buffer.WriteString(" OFFSET ")
		buffer.WriteString(strconv.Itoa(int(offset)))
		buffer.WriteString(" ROWS FETCH NEXT ")
		buffer.WriteString(strconv.Itoa(int(limit)))
		buffer.WriteString(" ROWS ONLY")
	}
}
