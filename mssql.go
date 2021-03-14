// Package mssql wraps mssql driver as an adapter for REL.
package mssql

import (
	db "database/sql"
	"strings"
	"time"

	"github.com/go-rel/rel"
	"github.com/go-rel/rel/adapter/sql"
)

// Adapter definition for mssql database.
type Adapter struct {
	*InstrumentationAdapter
	ConnectionAdapter
	ExecAdapter
	QueryAdapter
	AggregateAdapter
	ApplyAdapter
	InsertAdapter
	InsertAllAdapter
	UpdateAdapter
	DeleteAdapter
}

var (
	_ rel.Adapter = (*Adapter)(nil)
)

// New mssql adapter using existing connection.
func New(db *db.DB) Adapter {
	return new(db, nil, 0).(Adapter)
}

func new(db *db.DB, tx *db.Tx, savepoint int) rel.Adapter {
	var (
		placeholder       = "?"
		fieldSQL          = NewFieldSQL("[", "]")
		filterSQL         = NewFilterSQL(fieldSQL, placeholder)
		querySQL          = NewQuerySQL(fieldSQL, filterSQL)
		instrumentation   = NewInstrumentationAdapter()
		connectionAdapter = NewConnectionAdapter(instrumentation, db, tx, savepoint, new)
		execAdapter       = NewExecAdapter(connectionAdapter, mapError)
		queryAdapter      = NewQueryAdapter(connectionAdapter, querySQL, mapError)
		aggregateAdapter  = NewAggregateAdapter(connectionAdapter, querySQL)
		applyAdapter      = NewApplyAdapter(execAdapter, NewApplyTableSQL(fieldSQL, mapColumn), NewApplyIndexSQL(fieldSQL))
		insertAdapter     = NewInsertAdapter(connectionAdapter, NewInsertSQL(fieldSQL, placeholder))
		insertAllAdapter  = NewInsertAllAdapter(connectionAdapter, NewInsertAllSQL(fieldSQL, placeholder))
		updateAdapter     = NewUpdateAdapter(execAdapter, NewUpdateSQL(fieldSQL, filterSQL, placeholder))
		deleteAdapter     = NewDeleteAdapter(execAdapter, NewDeleteSQL(fieldSQL, filterSQL))
	)

	return Adapter{
		InstrumentationAdapter: instrumentation,
		ConnectionAdapter:      connectionAdapter,
		ExecAdapter:            execAdapter,
		QueryAdapter:           queryAdapter,
		AggregateAdapter:       aggregateAdapter,
		ApplyAdapter:           applyAdapter,
		InsertAdapter:          insertAdapter,
		InsertAllAdapter:       insertAllAdapter,
		UpdateAdapter:          updateAdapter,
		DeleteAdapter:          deleteAdapter,
	}
}

// Open mssql connection using dsn.
func Open(dsn string) (Adapter, error) {
	var database, err = db.Open("mssql", dsn)
	return New(database), err
}

func mapError(err error) error {
	if err == nil {
		return nil
	}

	var msg = err.Error()

	switch {
	case strings.HasPrefix(msg, "mssql: Violation of PRIMARY KEY"):
		return rel.ConstraintError{
			Key:  sql.ExtractString(msg, "constraint '", "'"),
			Type: rel.UniqueConstraint,
			Err:  err,
		}
	case strings.HasPrefix(msg, "mssql: Violation of UNIQUE KEY"):
		return rel.ConstraintError{
			Key:  sql.ExtractString(msg, "constraint '", "'"),
			Type: rel.UniqueConstraint,
			Err:  err,
		}
	case strings.HasPrefix(msg, "mssql: The UPDATE statement conflicted with the FOREIGN KEY"):
		return rel.ConstraintError{
			Key:  sql.ExtractString(msg, "FOREIGN KEY constraint \"", "\""),
			Type: rel.ForeignKeyConstraint,
			Err:  err,
		}
	case strings.HasPrefix(msg, "mssql: The UPDATE statement conflicted with the CHECK"):
		return rel.ConstraintError{
			Key:  sql.ExtractString(msg, "FOREIGN KEY constraint \"", "\""),
			Type: rel.CheckConstraint,
			Err:  err,
		}
	default:
		return err
	}
}

// mapColumn func.
func mapColumn(column *rel.Column) (string, int, int) {
	var (
		typ        string
		m, n       int
		timeLayout = "2006-01-02 15:04:05"
	)

	switch column.Type {
	case rel.ID:
		typ = "INT NOT NULL IDENTITY(1,1) PRIMARY KEY"
	case rel.Bool:
		typ = "BIT"
	case rel.Int:
		typ = "INT"
	case rel.BigInt:
		typ = "BIGINT"
	case rel.Float:
		typ = "FLOAT"
		m = column.Precision
	case rel.Decimal:
		typ = "DECIMAL"
		m = column.Precision
		n = column.Scale
	case rel.String:
		typ = "NVARCHAR"
		m = column.Limit
		if m == 0 {
			m = 255
		} else if m > 4000 {
			m = 4000
		}
	case rel.Text:
		typ = "NVARCHAR(MAX)"
	case rel.Date:
		typ = "DATE"
		timeLayout = "2006-01-02"
	case rel.DateTime:
		typ = "DATETIMEOFFSET"
	case rel.Time:
		typ = "TIME"
		timeLayout = "15:04:05"
	case rel.Timestamp:
		typ = "TIMESTAMP"
	default:
		typ = string(column.Type)
	}

	if t, ok := column.Default.(time.Time); ok {
		column.Default = t.Format(timeLayout)
	}

	return typ, m, n
}
