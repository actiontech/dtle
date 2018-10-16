package config

import (
	"github.com/actiontech/dtle/internal/config/mysql"
	"testing"
)

func TestWhereTrue(t *testing.T) {
	var err error
	table := NewTable("a", "a")
	table.OriginalTableColumns = mysql.NewColumnList(mysql.NewColumns([]string{
		"id", "a",
	}))
	whereCtx, err := NewWhereCtx("a % 2 = 0", table)
	if err != nil {
		t.Fail()
	}
	tbCtx := NewTableContext(table, whereCtx)

	for i := 0; i < 10000; i++ {
		absValues := []interface{}{i, i}
		colValues := mysql.ToColumnValues(absValues)
		b, err := tbCtx.WhereTrue(colValues)
		if err != nil {
			t.Fail()
		}
		if b != (i % 2 == 0) {
			t.Errorf("i: %v, b: %v", i, b)
		}
	}
}
