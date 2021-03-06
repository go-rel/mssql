package mssql

import (
	"context"

	"github.com/go-rel/rel"
	adapter "github.com/go-rel/rel/adapter/sql"
)

// QueryAdapter component.
type QueryAdapter struct {
	connectionAdapter ConnectionAdapter
	querySQL          QuerySQL
	errorMapper       ErrorMapper
}

// Query performs query operation.
func (qa QueryAdapter) Query(ctx context.Context, query rel.Query) (rel.Cursor, error) {
	var (
		statement, args = qa.querySQL.Build(query)
		rows, err       = qa.connectionAdapter.DoQuery(ctx, statement, args)
	)

	return &adapter.Cursor{Rows: rows}, qa.errorMapper(err)
}

// NewQueryAdapter component.
func NewQueryAdapter(connectionAdapter ConnectionAdapter, querySQL QuerySQL, errorMapper ErrorMapper) QueryAdapter {
	return QueryAdapter{
		connectionAdapter: connectionAdapter,
		querySQL:          querySQL,
		errorMapper:       errorMapper,
	}
}
