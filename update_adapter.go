package mssql

import (
	"context"

	"github.com/go-rel/rel"
)

// UpdateAdapter component.
type UpdateAdapter struct {
	execAdapter ExecAdapter
	updateSQL   UpdateSQL
}

// Update updates a record in database.
func (ua UpdateAdapter) Update(ctx context.Context, query rel.Query, mutates map[string]rel.Mutate) (int, error) {
	// TODO: pass primaryField from repo
	var (
		statement, args      = ua.updateSQL.Build(query.Table, "id", mutates, query.WhereQuery)
		_, updatedCount, err = ua.execAdapter.Exec(ctx, statement, args)
	)

	return int(updatedCount), err
}

// NewUpdateAdapter component.
func NewUpdateAdapter(execAdapter ExecAdapter, updateSQL UpdateSQL) UpdateAdapter {
	return UpdateAdapter{
		execAdapter: execAdapter,
		updateSQL:   updateSQL,
	}
}
