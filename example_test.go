package mssql_test

import (
	"context"

	"github.com/go-rel/mssql"
	"github.com/go-rel/rel"
	_ "github.com/microsoft/go-mssqldb"
)

func Example() {
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
