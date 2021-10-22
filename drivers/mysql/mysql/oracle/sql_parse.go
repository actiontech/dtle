package oracle

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
		v.Schema = StringsBuilder("`", strings.ToUpper(node.Schema.String()), "`")
		v.Table = StringsBuilder("`", strings.ToUpper(node.Name.String()), "`")
	}

	if node, ok := in.(*ast.UpdateStmt); ok {
		v.Operation = common.UpdateDML
		v.Data = make(map[string]interface{}, 1)
		v.Before = make(map[string]interface{}, 1)

		// Set 修改值 -> data
		for _, val := range node.List {
			var sb strings.Builder
			flags := format.DefaultRestoreFlags
			err := val.Expr.Restore(format.NewRestoreCtx(flags, &sb))
			if err != nil {
			}
		}

		// 如果存在 WHERE 条件 -> before
		if node.Where != nil {
			if node, ok := node.Where.Accept(v); ok {
				if exprNode, ok := node.(ast.ExprNode); ok {
					var sb strings.Builder
					sb.WriteString("WHERE ")
					flags := format.DefaultRestoreFlags
					err := exprNode.Restore(format.NewRestoreCtx(flags, &sb))
					if err != nil {
						//service.Logger.Error("sql parser failed",
						//	zap.String("stmt", v.Marshal()))
					}
					v.WhereExpr = sb.String()
				}
			}
			beforeData(node.Where, v.Before)
		}

	}

	if node, ok := in.(*ast.InsertStmt); ok {
		v.WhereColumnValues = new(common.ColumnValues)
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
				//v.Data[StringsBuilder("`", strings.ToUpper(col.String()), "`")] = sb.String()
				//ColumnsTypeOracle2MySQL()
				v.NewColumnValues.AbstractValues = append(v.NewColumnValues.AbstractValues, sb.String())
			}
		}
	}

	if node, ok := in.(*ast.DeleteStmt); ok {
		v.Operation = common.DeleteDML
		v.Before = make(map[string]interface{}, 1)
		// 如果存在 WHERE 条件 -> before
		if node.Where != nil {
			if node, ok := node.Where.Accept(v); ok {
				if exprNode, ok := node.(ast.ExprNode); ok {
					var sb strings.Builder
					sb.WriteString("WHERE ")
					flags := format.DefaultRestoreFlags
					err := exprNode.Restore(format.NewRestoreCtx(flags, &sb))
					if err != nil {
					}
					v.WhereExpr = sb.String()
				}
			}
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
			before[strings.ToUpper(column.String())] = value.String()
		}
	}
}

func ColumnsTypeOracle2MySQL() {

}
