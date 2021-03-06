package mssql

import (
	"context"

	"github.com/go-rel/rel"
)

// InsertAllAdapter component.
type InsertAllAdapter struct {
	connectionAdapter ConnectionAdapter
	insertAllSQL      InsertAllSQL
}

// InsertAll inserts multiple records to database and returns its ids.
func (iaa InsertAllAdapter) InsertAll(ctx context.Context, query rel.Query, primaryField string, fields []string, bulkMutates []map[string]rel.Mutate) ([]interface{}, error) {
	var (
		ids             []interface{}
		statement, args = iaa.insertAllSQL.Build(query.Table, primaryField, fields, bulkMutates)
		rows, err       = iaa.connectionAdapter.DoQuery(ctx, statement, args)
	)

	defer rows.Close()
	if err == nil {
		for rows.Next() {
			var id int64
			rows.Scan(&id)
			ids = append(ids, id)
		}
	}

	// hack for go-mssqldb driver which doesn't return unique error correctly.
	if primaryField != "" && len(ids) == 0 {
		return ids, rel.ConstraintError{
			Type: rel.UniqueConstraint,
			Err:  err,
		}
	}

	return ids, err
}

// NewInsertAllAdapter component.
func NewInsertAllAdapter(connectionAdapter ConnectionAdapter, insertAllSQL InsertAllSQL) InsertAllAdapter {
	return InsertAllAdapter{
		connectionAdapter: connectionAdapter,
		insertAllSQL:      insertAllSQL,
	}
}
