package mssql

import (
	"context"
	"os"
	"testing"

	_ "github.com/denisenkom/go-mssqldb"
	"github.com/go-rel/rel"
	"github.com/go-rel/rel/adapter/specs"
	"github.com/stretchr/testify/assert"
)

var ctx = context.TODO()

func dsn() string {
	if os.Getenv("MSSQL_DATABASE") != "" {
		return os.Getenv("MSSQL_DATABASE")
	}

	return "sqlserver://sa:REL2021-mssql@localhost:1433?database=rel&log=32"
}

func TestAdapter_specs(t *testing.T) {
	adapter, err := Open(dsn())
	assert.Nil(t, err)
	defer adapter.Close()

	repo := rel.New(adapter)

	// Prepare tables
	teardown := specs.Setup(t, repo)
	defer teardown()

	// Migration Specs
	specs.Migrate(t, repo)

	// Query Specs
	specs.Query(t, repo)
	specs.QueryJoin(t, repo)
	specs.QueryNotFound(t, repo)

	// Preload specs
	specs.PreloadHasMany(t, repo)
	specs.PreloadHasManyWithQuery(t, repo)
	specs.PreloadHasManySlice(t, repo)
	specs.PreloadHasOne(t, repo)
	specs.PreloadHasOneWithQuery(t, repo)
	specs.PreloadHasOneSlice(t, repo)
	specs.PreloadBelongsTo(t, repo)
	specs.PreloadBelongsToWithQuery(t, repo)
	specs.PreloadBelongsToSlice(t, repo)

	// // Aggregate Specs
	specs.Aggregate(t, repo)

	// Insert Specs
	specs.Insert(t, repo)
	specs.InsertHasMany(t, repo)
	specs.InsertHasOne(t, repo)
	specs.InsertBelongsTo(t, repo)
	specs.Inserts(t, repo)
	specs.InsertAll(t, repo)
	// specs.InsertAllPartialCustomPrimary(t, repo) - not supported

	// Update Specs
	specs.Update(t, repo)
	specs.UpdateNotFound(t, repo)
	specs.UpdateHasManyInsert(t, repo)
	specs.UpdateHasManyUpdate(t, repo)
	specs.UpdateHasManyReplace(t, repo)
	specs.UpdateHasOneInsert(t, repo)
	specs.UpdateHasOneUpdate(t, repo)
	specs.UpdateBelongsToInsert(t, repo)
	specs.UpdateBelongsToUpdate(t, repo)
	specs.UpdateAtomic(t, repo)
	specs.Updates(t, repo)
	specs.UpdateAll(t, repo)

	// Delete specs
	specs.Delete(t, repo)
	specs.DeleteBelongsTo(t, repo)
	specs.DeleteHasOne(t, repo)
	specs.DeleteHasMany(t, repo)
	specs.DeleteAll(t, repo)

	// Constraint specs
	// Unique and Foreign constraint is not fully supported.
	// Because of driver bug, any error occured when inserting is treated as unique constraint error.
	// specs.UniqueConstraintOnUpdate(t, repo)
	specs.ForeignKeyConstraintOnUpdate(t, repo)
	specs.CheckConstraintOnUpdate(t, repo)
}

func TestAdapter_Open(t *testing.T) {
	// with parameter
	assert.NotPanics(t, func() {
		adapter, _ := Open("root@tcp(localhost:3306)/rel_test?charset=utf8")
		defer adapter.Close()
	})

	// without paremeter
	assert.NotPanics(t, func() {
		adapter, _ := Open("root@tcp(localhost:3306)/rel_test")
		defer adapter.Close()
	})
}

func TestAdapter_Transaction_commitError(t *testing.T) {
	adapter, err := Open(dsn())
	assert.Nil(t, err)
	defer adapter.Close()

	assert.NotNil(t, adapter.Commit(ctx))
}

func TestAdapter_Transaction_rollbackError(t *testing.T) {
	adapter, err := Open(dsn())
	assert.Nil(t, err)
	defer adapter.Close()

	assert.NotNil(t, adapter.Rollback(ctx))
}

func TestAdapter_Exec_error(t *testing.T) {
	adapter, err := Open(dsn())
	assert.Nil(t, err)
	defer adapter.Close()

	_, _, err = adapter.Exec(ctx, "error", nil)
	assert.NotNil(t, err)
}
