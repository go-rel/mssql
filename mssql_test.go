package mssql

import (
	"context"
	db "database/sql"
	"os"
	"testing"

	"github.com/go-rel/rel"
	"github.com/go-rel/sql/specs"
	_ "github.com/microsoft/go-mssqldb"
	"github.com/stretchr/testify/assert"
)

var ctx = context.TODO()

func dsn() string {
	if os.Getenv("MSSQL_DATABASE") != "" {
		return os.Getenv("MSSQL_DATABASE")
	}

	return "sqlserver://sa:REL2021-mssql@localhost:1433"
}

func TestAdapter_specs(t *testing.T) {
	adapter, err := Open(dsn())
	assert.Nil(t, err)
	defer adapter.Close()

	repo := rel.New(adapter)

	// Prepare tables
	teardown := specs.Setup(repo)
	defer teardown()

	// Migration Specs
	specs.Migrate()

	// Query Specs
	specs.Query(t, repo)
	specs.QueryJoin(t, repo)
	specs.QueryJoinAssoc(t, repo)
	specs.QueryWhereSubQuery(t, repo)
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
	specs.UpdateAny(t, repo)

	// Delete specs
	specs.Delete(t, repo)
	specs.DeleteBelongsTo(t, repo)
	specs.DeleteHasOne(t, repo)
	specs.DeleteHasMany(t, repo)
	specs.DeleteAll(t, repo)
	specs.DeleteAny(t, repo)

	// Constraint specs
	// Unique and Foreign constraint is not fully supported.
	// Because of driver bug, any error occurred when inserting is treated as unique constraint error.
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

func TestAdapter_MustOpen(t *testing.T) {
	t.Cleanup(func() {
		dbOpen = db.Open
	})

	assert.Panics(t, func() {
		dbOpen = func(driverName string, dataSourceName string) (*db.DB, error) {
			return nil, assert.AnError
		}

		_ = MustOpen("root@tcp(localhost:3306)/rel_test?charset=utf8")
	})
}

func TestAdapter_Name(t *testing.T) {
	adapter := MustOpen(dsn())
	defer adapter.Close()

	assert.Equal(t, Name, adapter.Name())
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

func TestAdapter_TableBuilder(t *testing.T) {
	adapter, err := Open(dsn())
	assert.Nil(t, err)
	defer adapter.Close()

	tests := []struct {
		result string
		table  rel.Table
	}{
		{
			result: `ALTER TABLE [table] DROP CONSTRAINT [key];`,
			table: rel.Table{
				Op:   rel.SchemaAlter,
				Name: "table",
				Definitions: []rel.TableDefinition{
					rel.Key{Op: rel.SchemaDrop, Name: "key", Type: rel.ForeignKey},
				},
			},
		},
		{
			result: `ALTER TABLE [table] DROP CONSTRAINT [key];`,
			table: rel.Table{
				Op:   rel.SchemaAlter,
				Name: "table",
				Definitions: []rel.TableDefinition{
					rel.Key{Op: rel.SchemaDrop, Name: "key", Type: rel.UniqueKey},
				},
			},
		},
		{
			result: `ALTER TABLE [table] DROP CONSTRAINT [key];`,
			table: rel.Table{
				Op:   rel.SchemaAlter,
				Name: "table",
				Definitions: []rel.TableDefinition{
					rel.Key{Op: rel.SchemaDrop, Name: "key", Type: rel.PrimaryKey},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.result, func(t *testing.T) {
			assert.Equal(t, test.result, adapter.(*MSSQL).TableBuilder.Build(test.table))
		})
	}
}
