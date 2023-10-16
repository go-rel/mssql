// Package mssql wraps mssql driver as an adapter for REL.
package mssql

import (
	"context"
	db "database/sql"
	"strings"
	"time"

	mssqlbuilder "github.com/go-rel/mssql/builder"
	"github.com/go-rel/rel"
	"github.com/go-rel/sql"
	"github.com/go-rel/sql/builder"
)

// MSSQL Adapter.
type MSSQL struct {
	sql.SQL
}

// Name of database type this adapter implements.
const Name string = "mssql"

var _ rel.Adapter = (*MSSQL)(nil)

// Begin begins a new transaction.
func (m MSSQL) Begin(ctx context.Context) (rel.Adapter, error) {
	txSql, err := m.SQL.Begin(ctx)

	return &MSSQL{SQL: *txSql.(*sql.SQL)}, err
}

// Insert inserts a record to database and returns its id.
func (m MSSQL) Insert(ctx context.Context, query rel.Query, primaryField string, mutates map[string]rel.Mutate, onConflict rel.OnConflict) (interface{}, error) {
	var (
		id              int64
		statement, args = m.InsertBuilder.Build(query.Table, primaryField, mutates, onConflict)
		rows, err       = m.DoQuery(ctx, statement, args)
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

// InsertAll inserts multiple records to database and returns its ids.
func (m MSSQL) InsertAll(ctx context.Context, query rel.Query, primaryField string, fields []string, bulkMutates []map[string]rel.Mutate, onConflict rel.OnConflict) ([]interface{}, error) {
	var (
		ids             []interface{}
		statement, args = m.InsertAllBuilder.Build(query.Table, primaryField, fields, bulkMutates, onConflict)
		rows, err       = m.DoQuery(ctx, statement, args)
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

// Name of database adapter.
func (MSSQL) Name() string {
	return Name
}

// New mssql adapter using existing connection.
func New(db *db.DB) rel.Adapter {
	var (
		bufferFactory    = builder.BufferFactory{AllowTableSchema: true, ArgumentPlaceholder: "@p", ArgumentOrdinal: true, BoolTrueValue: "1", BoolFalseValue: "0", Quoter: builder.Quote{IDPrefix: "[", IDSuffix: "]", IDSuffixEscapeChar: "]", ValueQuote: "'", ValueQuoteEscapeChar: "'"}}
		filterBuilder    = builder.Filter{}
		queryBuilder     = mssqlbuilder.Query{Query: builder.Query{BufferFactory: bufferFactory, Filter: filterBuilder}}
		InsertBuilder    = mssqlbuilder.Insert{BufferFactory: bufferFactory}
		insertAllBuilder = mssqlbuilder.InsertAll{BufferFactory: bufferFactory}
		updateBuilder    = builder.Update{BufferFactory: bufferFactory, Query: queryBuilder, Filter: filterBuilder}
		deleteBuilder    = builder.Delete{BufferFactory: bufferFactory, Query: queryBuilder, Filter: filterBuilder}
		ddlBufferFactory = builder.BufferFactory{InlineValues: true, BoolTrueValue: "1", BoolFalseValue: "0", Quoter: builder.Quote{IDPrefix: "[", IDSuffix: "]", IDSuffixEscapeChar: "]", ValueQuote: "'", ValueQuoteEscapeChar: "'"}}
		ddlQueryBuilder  = builder.Query{BufferFactory: ddlBufferFactory, Filter: filterBuilder}
		tableBuilder     = mssqlbuilder.Table{BufferFactory: ddlBufferFactory, ColumnMapper: columnMapper, DropKeyMapper: sql.DropKeyMapper}
		indexBuilder     = mssqlbuilder.Index{BufferFactory: ddlBufferFactory, Query: ddlQueryBuilder, Filter: filterBuilder}
	)

	return &MSSQL{
		SQL: sql.SQL{
			QueryBuilder:     queryBuilder,
			InsertBuilder:    InsertBuilder,
			InsertAllBuilder: insertAllBuilder,
			UpdateBuilder:    updateBuilder,
			DeleteBuilder:    deleteBuilder,
			TableBuilder:     tableBuilder,
			IndexBuilder:     indexBuilder,
			ErrorMapper:      errorMapper,
			DB:               db,
		},
	}
}

// Open mssql connection using dsn.
func Open(dsn string) (rel.Adapter, error) {
	database, err := db.Open("sqlserver", dsn)
	return New(database), err
}

// MustOpen mssql connection using dsn.
func MustOpen(dsn string) rel.Adapter {
	adapter, err := Open(dsn)
	if err != nil {
		panic(err)
	}
	return adapter
}

func errorMapper(err error) error {
	if err == nil {
		return nil
	}

	msg := err.Error()

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

// columnMapper function.
func columnMapper(column *rel.Column) (string, int, int) {
	var (
		typ        string
		m, n       int
		timeLayout = "2006-01-02 15:04:05"
	)

	switch column.Type {
	case rel.ID:
		typ = "INT NOT NULL IDENTITY(1,1)"
	case rel.BigID:
		typ = "BIGINT NOT NULL IDENTITY(1,1)"
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
	case rel.Text, rel.JSON:
		typ = "NVARCHAR(MAX)"
	case rel.Date:
		typ = "DATE"
		timeLayout = "2006-01-02"
	case rel.DateTime:
		typ = "DATETIMEOFFSET"
	case rel.Time:
		typ = "TIME"
		timeLayout = "15:04:05"
	default:
		typ = string(column.Type)
	}

	if t, ok := column.Default.(time.Time); ok {
		column.Default = t.Format(timeLayout)
	}

	return typ, m, n
}
