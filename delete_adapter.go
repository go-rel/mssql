package mssql

import (
	"context"

	"github.com/go-rel/rel"
)

// DeleteAdapter component.
type DeleteAdapter struct {
	execAdapter ExecAdapter
	deleteSQL   DeleteSQL
}

// Delete deletes all results that match the query.
func (da DeleteAdapter) Delete(ctx context.Context, query rel.Query) (int, error) {
	var (
		statement, args      = da.deleteSQL.Build(query.Table, query.WhereQuery)
		_, deletedCount, err = da.execAdapter.Exec(ctx, statement, args)
	)

	return int(deletedCount), err
}

// NewDeleteAdapter component.
func NewDeleteAdapter(execAdapter ExecAdapter, deleteSQL DeleteSQL) DeleteAdapter {
	return DeleteAdapter{
		execAdapter: execAdapter,
		deleteSQL:   deleteSQL,
	}
}
