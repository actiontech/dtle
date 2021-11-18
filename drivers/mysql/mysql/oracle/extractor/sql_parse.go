package extractor

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/sjjian/oracle-sql-parser/ast/element"

	"github.com/actiontech/dtle/drivers/mysql/common"

	"github.com/pingcap/parser/format"

	"github.com/pingcap/parser/ast"
	_ "github.com/pingcap/tidb/types/parser_driver"
	oracle_ast "github.com/sjjian/oracle-sql-parser/ast"
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
	v.WhereColumnValues = new(common.ColumnValues)
	if node, ok := in.(*ast.TableName); ok {
		v.Schema = node.Schema.String()
		v.Table = node.Name.String()
	}

	if node, ok := in.(*ast.UpdateStmt); ok {
		v.Operation = common.UpdateDML
		v.Before = make(map[string]interface{}, 1)
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

// mysql ddl sql build

const (
	MySQLColTypeCHAR      = "CHAR"
	MySQLColTypeVARCHAR   = "VARCHAR"
	MySQLColTypeNCHAR     = "NCHAR"
	MySQLColTypeNVARCHAR  = "NVARCHAR"
	MySQLColTypeFLOAT     = "FLOAT"
	MySQLColTypeDOUBLE    = "DOUBLE"
	MySQLColTypeDECIMAL   = "DECIMAL"
	MySQLColTypeDEC       = "DEC"
	MySQLColTypeINT       = "INT"
	MySQLColTypeLONGBLOB  = "LONGBLOB"
	MySQLColTypeDATETIME  = "DATETIME"
	MySQLColTypeLONGTEXT  = "LONGTEXT"
	MySQLColTypeTINYINT   = "TINYINT"
	MySQLColTypeSMALLINT  = "SMALLINT"
	MySQLColTypeBIGINT    = "BIGINT"
	MySQLColTypeNUMERIC   = "NUMERIC"
	MySQLColTypeVARBINARY = "VARBINARY"
	MySQLColTypeTEXT      = "TEXT"
)

// colDefinetione = column type
// example : colName colType(size) NOT NULL DEFAULT testDef
func OracleTypeParse(td *oracle_ast.ColumnDef) string {
	var colDefinition string
	switch td.Datatype.DataDef() {
	case element.DataDefBFile:
		colDefinition = fmt.Sprintf("%s %s(%d)%s", IdentifierToString(td.ColumnName), MySQLColTypeVARCHAR, 255, colDefaultString(td.Default))
	case element.DataDefBinaryFloat:
		colDefinition = fmt.Sprintf("%s %s%s", IdentifierToString(td.ColumnName), MySQLColTypeFLOAT, colDefaultString(td.Default))
	case element.DataDefBinaryDouble:
		colDefinition = fmt.Sprintf("%s %s%s", IdentifierToString(td.ColumnName), MySQLColTypeDOUBLE, colDefaultString(td.Default))
	case element.DataDefBlob:
		colDefinition = fmt.Sprintf("%s %s%s", IdentifierToString(td.ColumnName), MySQLColTypeLONGBLOB, colDefaultString(td.Default))
	case element.DataDefChar:
		if *td.Datatype.(*element.Char).Size >= 1 && *td.Datatype.(*element.Char).Size <= 255 {
			colDefinition = fmt.Sprintf("%s %s(%d)%s", IdentifierToString(td.ColumnName), MySQLColTypeCHAR, *td.Datatype.(*element.Char).Size, colDefaultString(td.Default))
		} else if *td.Datatype.(*element.Char).Size >= 256 && *td.Datatype.(*element.Char).Size <= 2000 {
			colDefinition = fmt.Sprintf("%s %s(%d)%s", IdentifierToString(td.ColumnName), MySQLColTypeVARCHAR, *td.Datatype.(*element.Char).Size, colDefaultString(td.Default))
		}
	case element.DataDefCharacter:
		if *td.Datatype.(*element.Char).Size >= 1 && *td.Datatype.(*element.Char).Size <= 255 {
			colDefinition = fmt.Sprintf("%s %s(%d)%s", IdentifierToString(td.ColumnName), MySQLColTypeCHAR, *td.Datatype.(*element.Char).Size, colDefaultString(td.Default))
		} else if *td.Datatype.(*element.Char).Size >= 256 && *td.Datatype.(*element.Char).Size <= 2000 {
			colDefinition = fmt.Sprintf("%s %s(%d)%s", IdentifierToString(td.ColumnName), MySQLColTypeVARCHAR, *td.Datatype.(*element.Char).Size, colDefaultString(td.Default))
		}
	case element.DataDefClob:
		colDefinition = fmt.Sprintf("%s %s%s", IdentifierToString(td.ColumnName), MySQLColTypeLONGTEXT, colDefaultString(td.Default))
	case element.DataDefDate:
		colDefinition = fmt.Sprintf("%s %s%s", IdentifierToString(td.ColumnName), MySQLColTypeDATETIME, colDefaultString(td.Default))
	case element.DataDefDecimal:
		colDefinition = fmt.Sprintf("%s %s(%d,%d)%s", IdentifierToString(td.ColumnName), MySQLColTypeDECIMAL,
			td.Datatype.(*element.Number).Precision.Number, *td.Datatype.(*element.Number).Scale, colDefaultString(td.Default))
	case element.DataDefDec:
		colDefinition = fmt.Sprintf("%s %s(%d,%d)%s", IdentifierToString(td.ColumnName), MySQLColTypeDEC,
			td.Datatype.(*element.Number).Precision.Number, *td.Datatype.(*element.Number).Scale, colDefaultString(td.Default))
	case element.DataDefDoublePrecision:
		colDefinition = fmt.Sprintf("%s %s%s", IdentifierToString(td.ColumnName), MySQLColTypeDOUBLE, colDefaultString(td.Default))
	case element.DataDefFloat:
		colDefinition = fmt.Sprintf("%s %s%s", IdentifierToString(td.ColumnName), MySQLColTypeDOUBLE, colDefaultString(td.Default))
	case element.DataDefInteger:
		colDefinition = fmt.Sprintf("%s %s%s", IdentifierToString(td.ColumnName), MySQLColTypeINT, colDefaultString(td.Default))
	case element.DataDefInt:
		colDefinition = fmt.Sprintf("%s %s%s", IdentifierToString(td.ColumnName), MySQLColTypeINT, colDefaultString(td.Default))
	case element.DataDefIntervalYear:
		colDefinition = fmt.Sprintf("%s %s(%d)%s", IdentifierToString(td.ColumnName), MySQLColTypeVARCHAR, 30, colDefaultString(td.Default))
	case element.DataDefIntervalDay:
		colDefinition = fmt.Sprintf("%s %s(%d)%s", IdentifierToString(td.ColumnName), MySQLColTypeVARCHAR, 30, colDefaultString(td.Default))
	case element.DataDefLong:
		colDefinition = fmt.Sprintf("%s %s%s", IdentifierToString(td.ColumnName), MySQLColTypeLONGTEXT, colDefaultString(td.Default))
	case element.DataDefLongRaw:
		colDefinition = fmt.Sprintf("%s %s%s", IdentifierToString(td.ColumnName), MySQLColTypeLONGBLOB, colDefaultString(td.Default))
	case element.DataDefNChar:
		if *td.Datatype.(*element.NChar).Size >= 1 && *td.Datatype.(*element.NChar).Size <= 255 {
			colDefinition = fmt.Sprintf("%s %s(%d)%s", IdentifierToString(td.ColumnName), MySQLColTypeNCHAR, *td.Datatype.(*element.NChar).Size, colDefaultString(td.Default))
		} else if *td.Datatype.(*element.NChar).Size >= 256 && *td.Datatype.(*element.NChar).Size <= 2000 {
			colDefinition = fmt.Sprintf("%s %s(%d)%s", IdentifierToString(td.ColumnName), MySQLColTypeNVARCHAR, *td.Datatype.(*element.NChar).Size, colDefaultString(td.Default))
		}
	case element.DataDefNCharVarying:
		colDefinition = fmt.Sprintf("%s %s(%d)%s", IdentifierToString(td.ColumnName), MySQLColTypeNVARCHAR, *td.Datatype.(*element.NVarchar2).Size, colDefaultString(td.Default))
	case element.DataDefNClob:
		colDefinition = fmt.Sprintf("%s %s%s", IdentifierToString(td.ColumnName), MySQLColTypeTEXT, colDefaultString(td.Default))
	case element.DataDefNumber:
		var mysqlNumberType string
		num := td.Datatype.(*element.Number)
		if num.Precision == nil { // p == nil s == nil
			mysqlNumberType = MySQLColTypeDOUBLE
		} else {
			p := num.Precision.Number
			if num.Scale != nil { // p !=nil  s == nil
				mysqlNumberType = fmt.Sprintf("%s(%d,%d)", MySQLColTypeDECIMAL, p, *num.Scale)
			} else { // p != nil s != nil
				switch {
				case p <= 0:
					mysqlNumberType = MySQLColTypeDOUBLE
				case p < 3:
					mysqlNumberType = MySQLColTypeTINYINT
				case p < 5:
					mysqlNumberType = MySQLColTypeSMALLINT
				case p < 9:
					mysqlNumberType = MySQLColTypeINT
				case p < 19:
					mysqlNumberType = MySQLColTypeBIGINT
				case p <= 38:
					mysqlNumberType = fmt.Sprintf("%s(%d)", MySQLColTypeDECIMAL, p)
				}
			}
		}
		colDefinition = fmt.Sprintf("%s %s%s", IdentifierToString(td.ColumnName), mysqlNumberType, colDefaultString(td.Default))
	case element.DataDefNumeric:
		colDefinition = fmt.Sprintf("%s %s(%d,%d)%s", IdentifierToString(td.ColumnName), MySQLColTypeNUMERIC,
			td.Datatype.(*element.Number).Precision.Number, *td.Datatype.(*element.Number).Scale, colDefaultString(td.Default))
	case element.DataDefNVarChar2:
		colDefinition = fmt.Sprintf("%s %s(%d)%s", IdentifierToString(td.ColumnName), MySQLColTypeNVARCHAR, *td.Datatype.(*element.NVarchar2).Size, colDefaultString(td.Default))
	case element.DataDefRaw:
		colDefinition = fmt.Sprintf("%s %s(%d)%s", IdentifierToString(td.ColumnName), MySQLColTypeVARBINARY, *td.Datatype.(*element.Raw).Size, colDefaultString(td.Default))
	case element.DataDefReal:
		colDefinition = fmt.Sprintf("%s %s%s", IdentifierToString(td.ColumnName), MySQLColTypeDOUBLE, colDefaultString(td.Default))
	case element.DataDefRowId:
		colDefinition = fmt.Sprintf("%s %s(%d)%s", IdentifierToString(td.ColumnName), MySQLColTypeCHAR, 10, colDefaultString(td.Default))
	case element.DataDefSmallInt:
		colDefinition = fmt.Sprintf("%s %s(%d)%s", IdentifierToString(td.ColumnName), MySQLColTypeDECIMAL, 38, colDefaultString(td.Default))
	case element.DataDefTimestamp:
		// todo with time zone
		colDefinition = fmt.Sprintf("%s %s(%d)%s", IdentifierToString(td.ColumnName), MySQLColTypeDATETIME, *td.Datatype.(*element.Timestamp).FractionalSecondsPrecision, colDefaultString(td.Default))
	case element.DataDefURowId:
		colDefinition = fmt.Sprintf("%s %s(%d)%s", IdentifierToString(td.ColumnName), MySQLColTypeVARCHAR, *td.Datatype.(*element.URowId).Size, colDefaultString(td.Default))
	case element.DataDefVarchar:
		colDefinition = fmt.Sprintf("%s %s(%d)%s", IdentifierToString(td.ColumnName), MySQLColTypeVARCHAR, *td.Datatype.(*element.Varchar2).Size, colDefaultString(td.Default))
	case element.DataDefVarchar2:
		colDefinition = fmt.Sprintf("%s %s(%d)%s", IdentifierToString(td.ColumnName), MySQLColTypeVARCHAR, *td.Datatype.(*element.Varchar2).Size, colDefaultString(td.Default))
		//case element.DataDefXML:
		//	colDefinition = fmt.Sprintf("%s %s(%d)%s", IdentifierToString(td.ColumnName), MySQLColTypeVARCHAR, 30, colDefaultString(td.Default))
	}
	return colDefinition
}
func IdentifierToString(i *element.Identifier) string {
	if i.Typ == element.IdentifierTypeNonQuoted {
		return strings.ToUpper(i.Value)
	} else if i.Typ == element.IdentifierTypeQuoted {
		return strings.TrimLeft(strings.TrimRight(i.Value, `"`), `"`)
	}
	return ""
}
func colDefaultString(defaultCol *oracle_ast.ColumnDefault) string {
	// todo Not currently supported
	var defValue string
	if defaultCol == nil {
		return ""
	}
	if defaultCol.OnNull {
		defValue = " NULL"
	} else {
		defValue = " NOT NULL"
	}
	if defaultCol.Value != nil {
		defValue = fmt.Sprintf("DEFAULT %s", defaultCol.Value)
	}
	return defValue
}
