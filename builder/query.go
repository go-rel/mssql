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

	q.WriteSelect(buffer, query.Table, query.SelectQuery, query.LimitQuery, query.OffsetQuery)
	q.WriteQuery(buffer, query)

	if rootQuery {
		buffer.WriteByte(';')
	}
}

// WriteSelect SQL to buffer.
func (q Query) WriteSelect(buffer *builder.Buffer, table string, selectQuery rel.SelectQuery, limit rel.Limit, offset rel.Offset) {
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
			buffer.WriteField(table, f)

			if i < l {
				buffer.WriteString(", ")
			}
		}
	} else {
		buffer.WriteString(" *")
	}
}

// WriteQuery SQL to buffer.
func (q Query) WriteQuery(buffer *builder.Buffer, query rel.Query) {
	q.WriteFrom(buffer, query.Table)
	q.WriteJoin(buffer, query.Table, query.JoinQuery)
	q.WriteWhere(buffer, query.Table, query.WhereQuery)

	if len(query.GroupQuery.Fields) > 0 {
		q.WriteGroupBy(buffer, query.Table, query.GroupQuery.Fields)
		q.WriteHaving(buffer, query.Table, query.GroupQuery.Filter)
	}

	q.WriteOrderBy(buffer, query.Table, query.SortQuery)
	q.WriteLimitOffet(buffer, query.LimitQuery, query.OffsetQuery)

	if query.LockQuery != "" {
		buffer.WriteByte(' ')
		buffer.WriteString(string(query.LockQuery))
	}
}

// WriteWhere SQL to buffer.
func (q Query) WriteWhere(buffer *builder.Buffer, table string, filter rel.FilterQuery) {
	if filter.None() {
		return
	}

	buffer.WriteString(" WHERE ")
	q.Filter.Write(buffer, table, filter, q)
}

// WriteHaving SQL to buffer.
func (q Query) WriteHaving(buffer *builder.Buffer, table string, filter rel.FilterQuery) {
	if filter.None() {
		return
	}

	buffer.WriteString(" HAVING ")
	q.Filter.Write(buffer, table, filter, q)
}

// WriteLimitOffet SQL to buffer.
func (q Query) WriteLimitOffet(buffer *builder.Buffer, limit rel.Limit, offset rel.Offset) {
	if limit > 0 && offset > 0 {
		buffer.WriteString(" OFFSET ")
		buffer.WriteString(strconv.Itoa(int(offset)))
		buffer.WriteString(" ROWS FETCH NEXT ")
		buffer.WriteString(strconv.Itoa(int(limit)))
		buffer.WriteString(" ROWS ONLY")
	}
}
