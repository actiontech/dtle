package config

import (
	"github.com/actiontech/dtle/internal/client/driver/mysql/binlog"
	"github.com/actiontech/dtle/internal/config/mysql"
	"testing"
)

func newTableContextWithWhere(t *testing.T,
	schemaName string, tableName string, where string, columnNames ...string) *TableContext {

	table := NewTable(schemaName, tableName)
	table.OriginalTableColumns = mysql.NewColumnList(mysql.NewColumns(columnNames))
	whereCtx, err := NewWhereCtx(where, table)
	if err != nil {
		t.Fatal(err)
	}
	tbCtx := NewTableContext(table, whereCtx)
	return tbCtx
}
func buildColumnValues(vals ...interface{}) *mysql.ColumnValues {
	return binlog.ToColumnValuesV2(vals, nil)
}

func TestWhereTrue(t *testing.T) {
	var tbCtx *TableContext

	tbCtx = newTableContextWithWhere(t, "db1", "tb1", "a % 2 = 0", "id", "a")
	for i := 0; i < 1000; i++ {
		r, err := tbCtx.WhereTrue(buildColumnValues(i, i))
		if err != nil {
			t.Fatal(err)
		}
		if r != (i % 2 == 0) {
			t.Fatalf("i: %v, r: %v", i, r)
		}
	}

	////////

	tbCtx = newTableContextWithWhere(t, "db1", "tb1", "a < 5", "id", "a")
	for i := -100; i < 1000; i++ {
		r, err := tbCtx.WhereTrue(buildColumnValues(i, i))
		if err != nil {
			t.Fatal(err)
		}
		if r != (i < 5) {
			t.Fatalf("i: %v, r: %v", i, r)
		}
	}

	////////

	tbCtx = newTableContextWithWhere(t, "db1", "tb1", "a >= 5", "id", "a")
	for i := -100; i < 1000; i++ {
		r, err := tbCtx.WhereTrue(buildColumnValues(i, i))
		if err != nil {
			t.Fatal(err)
		}
		if r != (i >= 5) {
			t.Fatalf("i: %v, r: %v", i, r)
		}
	}

	////////
	tbCtx = newTableContextWithWhere(t, "db1", "tb1", "a >= 5 or b <=5 ", "id", "a", "b")
	{
		a,b := 2,3
		r, err := tbCtx.WhereTrue(buildColumnValues(1,2,3))
		if err != nil {
			t.Fatal(err)
		}
		if r != (a >= 5 || b <= 5) {
			t.Fatalf("r: %v", r)
		}
	}
}

func TestWhereTrueText(t *testing.T) {
	var tbCtx *TableContext

	tbCtx = newTableContextWithWhere(t, "db1", "tb1", "a = 'hello'", "id", "a")
	r, err := tbCtx.WhereTrue(buildColumnValues(1, []byte("hello")))
	if err != nil {
		t.Fatal(err)
	}
	if r != true {
		t.Fatalf("it is hello")
	}

	tbCtx = newTableContextWithWhere(t, "db1", "tb1", "a = 'hello'", "id", "a")
	r, err = tbCtx.WhereTrue(buildColumnValues(2, "hello2"))
	if err != nil {
		t.Fatal(err)
	}
	if r != false {
		t.Fatalf("it is not hello")
	}
}
