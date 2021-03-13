package mssql

import (
	"context"
	"database/sql"
	"errors"
	"strconv"

	"github.com/go-rel/rel"
)

// AdapterFactory function.
type AdapterFactory func(db *sql.DB, tx *sql.Tx, savepoint int) rel.Adapter

// ConnectionAdapter component.
type ConnectionAdapter struct {
	instrumentationAdapter *InstrumentationAdapter
	db                     *sql.DB
	tx                     *sql.Tx
	savepoint              int
	adapterFactory         AdapterFactory
}

// DoExec using active database connection.
func (ca ConnectionAdapter) DoExec(ctx context.Context, statement string, args []interface{}) (sql.Result, error) {
	var (
		err    error
		result sql.Result
		finish = ca.instrumentationAdapter.Instrumenter.Observe(ctx, "adapter-exec", statement)
	)

	if ca.tx != nil {
		result, err = ca.tx.ExecContext(ctx, statement, args...)
	} else {
		result, err = ca.db.ExecContext(ctx, statement, args...)
	}

	finish(err)
	return result, err
}

// DoQuery using active database connection.
func (ca ConnectionAdapter) DoQuery(ctx context.Context, statement string, args []interface{}) (*sql.Rows, error) {
	var (
		err  error
		rows *sql.Rows
	)

	finish := ca.instrumentationAdapter.Instrumenter.Observe(ctx, "adapter-query", statement)
	if ca.tx != nil {
		rows, err = ca.tx.QueryContext(ctx, statement, args...)
	} else {
		rows, err = ca.db.QueryContext(ctx, statement, args...)
	}
	finish(err)

	return rows, err
}

// Begin begins a new transaction.
func (ca ConnectionAdapter) Begin(ctx context.Context) (rel.Adapter, error) {
	var (
		tx        *sql.Tx
		savepoint int
		err       error
	)

	finish := ca.instrumentationAdapter.Instrumenter.Observe(ctx, "adapter-begin", "begin transaction")

	if ca.tx != nil {
		tx = ca.tx
		savepoint = ca.savepoint + 1
		_, err = ca.tx.ExecContext(ctx, "SAVEPOINT s"+strconv.Itoa(savepoint)+";")
	} else {
		tx, err = ca.db.BeginTx(ctx, nil)
	}

	finish(err)

	newAdapter := ca.adapterFactory(nil, tx, savepoint)
	newAdapter.Instrumentation(ca.instrumentationAdapter.Instrumenter)

	return newAdapter, err
}

// Commit commits current transaction.
func (ca ConnectionAdapter) Commit(ctx context.Context) error {
	var err error

	finish := ca.instrumentationAdapter.Instrumenter.Observe(ctx, "adapter-commit", "commit transaction")

	if ca.tx == nil {
		err = errors.New("unable to commit outside transaction")
	} else if ca.savepoint > 0 {
		_, err = ca.tx.ExecContext(ctx, "RELEASE SAVEPOINT s"+strconv.Itoa(ca.savepoint)+";", []interface{}{})
	} else {
		err = ca.tx.Commit()
	}

	finish(err)

	return err
}

// Rollback revert current transaction.
func (ca ConnectionAdapter) Rollback(ctx context.Context) error {
	var err error

	finish := ca.instrumentationAdapter.Instrumenter.Observe(ctx, "adapter-rollback", "rollback transaction")

	if ca.tx == nil {
		err = errors.New("unable to rollback outside transaction")
	} else if ca.savepoint > 0 {
		_, err = ca.tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT s"+strconv.Itoa(ca.savepoint)+";")
	} else {
		err = ca.tx.Rollback()
	}

	finish(err)

	return err
}

// Ping database.
func (ca ConnectionAdapter) Ping(ctx context.Context) error {
	return ca.db.PingContext(ctx)
}

// Close database connection.
func (ca ConnectionAdapter) Close() error {
	return ca.db.Close()
}

// NewConnectionAdapter components.
func NewConnectionAdapter(instrumentationAdapter *InstrumentationAdapter, db *sql.DB, tx *sql.Tx, savepoint int, adapterFactory AdapterFactory) ConnectionAdapter {
	return ConnectionAdapter{
		instrumentationAdapter: instrumentationAdapter,
		db:                     db,
		tx:                     tx,
		savepoint:              savepoint,
		adapterFactory:         adapterFactory,
	}
}
