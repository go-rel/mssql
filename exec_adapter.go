package mssql

import (
	"context"
)

// ErrorMapper function.
type ErrorMapper func(error) error

// ExecAdapter component.
type ExecAdapter struct {
	connectionAdapter ConnectionAdapter
	errorMapper       ErrorMapper
}

// Exec performs exec operation.
func (ea ExecAdapter) Exec(ctx context.Context, statement string, args []interface{}) (int64, int64, error) {
	var (
		res, err = ea.connectionAdapter.DoExec(ctx, statement, args)
	)

	if err != nil {
		return 0, 0, ea.errorMapper(err)
	}

	lastID, _ := res.LastInsertId()
	rowCount, _ := res.RowsAffected()

	return lastID, rowCount, nil
}

// NewExecAdapter component.
func NewExecAdapter(connectionAdapter ConnectionAdapter, errorMapper ErrorMapper) ExecAdapter {
	return ExecAdapter{
		connectionAdapter: connectionAdapter,
		errorMapper:       errorMapper,
	}
}
