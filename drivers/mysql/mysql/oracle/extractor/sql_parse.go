package extractor

import (
	"encoding/json"
	"strings"

	"github.com/actiontech/dtle/drivers/mysql/common"

	"github.com/pingcap/parser/format"

	"github.com/pingcap/parser/ast"
	_ "github.com/pingcap/tidb/types/parser_driver"
)

type Stmt struct {
	Schema            string
	Table             string
	Columns           []string
	Operation         int8
	Data              map[string]interface{}
	Before            map[string]interface{}
	WhereExpr         string
	WhereColumnValues *common.ColumnValues
	NewColumnValues   *common.ColumnValues
}

// WARNING: sql parser Format() has be discrepancy ,be is instead of Restore()
func (v *Stmt) Enter(in ast.Node) (ast.Node, bool) {
	if node, ok := in.(*ast.TableName); ok {
		v.Schema = node.Schema.String()
		v.Table = node.Name.String()
	}

	if node, ok := in.(*ast.UpdateStmt); ok {
		v.Operation = common.UpdateDML
		v.Before = make(map[string]interface{}, 1)
		v.WhereColumnValues = new(common.ColumnValues)
		if node.Where != nil {
			beforeData(node.Where, v.Before)
		}
	}

	if node, ok := in.(*ast.InsertStmt); ok {
		v.NewColumnValues = new(common.ColumnValues)
		v.Operation = common.InsertDML
		v.Data = make(map[string]interface{}, 1)
		for i, col := range node.Columns {
			v.Columns = append(v.Columns, StringsBuilder("`", strings.ToUpper(col.String()), "`"))
			for _, lists := range node.Lists {
				var sb strings.Builder
				flags := format.DefaultRestoreFlags
				err := lists[i].Restore(format.NewRestoreCtx(flags, &sb))
				if err != nil {
					//service.Logger.Error("sql parser failed",
					//	zap.String("stmt", v.Marshal()))
				}
				data := strings.TrimLeft(strings.TrimRight(sb.String(), "'"), "'")
				v.NewColumnValues.AbstractValues = append(v.NewColumnValues.AbstractValues, data)
			}
		}
	}

	if node, ok := in.(*ast.DeleteStmt); ok {
		v.Operation = common.DeleteDML
		v.Before = make(map[string]interface{}, 1)
		v.WhereColumnValues = new(common.ColumnValues)
		if node.Where != nil {
			beforeData(node.Where, v.Before)
		}
	}
	return in, false
}

func (v *Stmt) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}

func (v *Stmt) Marshal() string {
	b, err := json.Marshal(&v)
	if err != nil {
	}
	return string(b)
}

func beforeData(where ast.ExprNode, before map[string]interface{}) {
	if binaryNode, ok := where.(*ast.BinaryOperationExpr); ok {
		switch binaryNode.Op.String() {
		case ast.LogicAnd:
			beforeData(binaryNode.L, before)
			beforeData(binaryNode.R, before)
		case ast.EQ:
			var value strings.Builder
			var column strings.Builder
			flags := format.DefaultRestoreFlags
			err := binaryNode.R.Restore(format.NewRestoreCtx(flags, &value))
			if err != nil {
			}
			err = binaryNode.L.Restore(format.NewRestoreCtx(flags, &column))
			if err != nil {
			}
			before[strings.TrimLeft(strings.TrimRight(column.String(), "`"), "`")] = value.String()
		}
	}
}
