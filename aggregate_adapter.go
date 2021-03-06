package mssql

import (
	"context"
	"database/sql"

	"github.com/go-rel/rel"
)

// AggregateAdapter component.
type AggregateAdapter struct {
	connectionAdapter ConnectionAdapter
	querySQL          QuerySQL
}

// Aggregate record using given query.
func (aa AggregateAdapter) Aggregate(ctx context.Context, query rel.Query, mode string, field string) (int, error) {
	var (
		out             sql.NullInt64
		aggregateField  = "^" + mode + "(" + aa.querySQL.fieldSQL.Build(field) + ") AS result"
		aggregateQuery  = query.Select(append([]string{aggregateField}, query.GroupQuery.Fields...)...)
		statement, args = aa.querySQL.Build(aggregateQuery)
		rows, err       = aa.connectionAdapter.DoQuery(ctx, statement, args)
	)

	defer rows.Close()
	if err == nil && rows.Next() {
		rows.Scan(&out)
	}

	return int(out.Int64), err
}

// NewAggregateAdapter component.
func NewAggregateAdapter(connectionAdapter ConnectionAdapter, querySQL QuerySQL) AggregateAdapter {
	return AggregateAdapter{
		connectionAdapter: connectionAdapter,
		querySQL:          querySQL,
	}
}
