package mssql_test

import (
	"context"
	"testing"

	_ "github.com/denisenkom/go-mssqldb"
	"github.com/go-rel/mssql"
	"github.com/go-rel/rel"
)

func TestExample(t *testing.T) {
	// open mssql connection.
	adapter, err := mssql.Open("sqlserver://sa:REL2021-mssql@localhost:1433?database=rel")
	if err != nil {
		panic(err)
	}
	defer adapter.Close()

	// initialize REL's repo.
	repo := rel.New(adapter)
	repo.Ping(context.TODO())
}
