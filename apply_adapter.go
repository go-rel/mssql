package mssql

import (
	"context"

	"github.com/go-rel/rel"
)

// ApplyAdapter component.
type ApplyAdapter struct {
	execAdapter   ExecAdapter
	applyTableSQL ApplyTableSQL
	applyIndexSQL ApplyIndexSQL
}

// Apply table.
func (aa ApplyAdapter) Apply(ctx context.Context, migration rel.Migration) error {
	var (
		statement string
	)

	switch v := migration.(type) {
	case rel.Table:
		statement = aa.applyTableSQL.Build(v)
	case rel.Index:
		statement = aa.applyIndexSQL.Build(v)
	case rel.Raw:
		statement = string(v)
	}

	_, _, err := aa.execAdapter.Exec(ctx, statement, nil)
	return err
}

// NewApplyAdapter component.
func NewApplyAdapter(execAdapter ExecAdapter, applyTableSQL ApplyTableSQL, applyIndexSQL ApplyIndexSQL) ApplyAdapter {
	return ApplyAdapter{
		execAdapter:   execAdapter,
		applyTableSQL: applyTableSQL,
		applyIndexSQL: applyIndexSQL,
	}
}
