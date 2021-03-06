package mssql

import (
	"context"

	"github.com/go-rel/rel"
)

// InsertAdapter component.
type InsertAdapter struct {
	connectionAdapter ConnectionAdapter
	insertSQL         InsertSQL
}

// Insert inserts a record to database and returns its id.
func (ia InsertAdapter) Insert(ctx context.Context, query rel.Query, primaryField string, mutates map[string]rel.Mutate) (interface{}, error) {
	var (
		id              int64
		statement, args = ia.insertSQL.Build(query.Table, primaryField, mutates)
		rows, err       = ia.connectionAdapter.DoQuery(ctx, statement, args)
	)

	defer rows.Close()
	if err == nil && rows.Next() {
		rows.Scan(&id)
	}

	// hack for go-mssqldb driver which doesn't return unique error correctly.
	if primaryField != "" && id == 0 {
		return id, rel.ConstraintError{
			Type: rel.UniqueConstraint,
			Err:  err,
		}
	}

	return id, err
}

// NewInsertAdapter component.
func NewInsertAdapter(connectionAdapter ConnectionAdapter, insertSQL InsertSQL) InsertAdapter {
	return InsertAdapter{
		connectionAdapter: connectionAdapter,
		insertSQL:         insertSQL,
	}
}
