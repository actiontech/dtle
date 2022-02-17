package extractor

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/actiontech/dtle/drivers/mysql/common"
	"github.com/actiontech/dtle/g"
	"github.com/pkg/errors"

	"github.com/pingcap/tidb/parser/format"
	"github.com/pingcap/tidb/types"

	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	oracle_ast "github.com/sjjian/oracle-sql-parser/ast"
	oracle_element "github.com/sjjian/oracle-sql-parser/ast/element"
)

type Stmt struct {
	logger            g.LoggerType
	Schema            string
	Table             string
	Columns           []string
	Operation         int8
	Data              map[string]interface{}
	Before            map[string]interface{}
	WhereExpr         string
	WhereColumnValues []interface{}
	NewColumnValues   []interface{}
	Error             error
}

func (v *Stmt) Enter(in ast.Node) (ast.Node, bool) {
	if node, ok := in.(*ast.TableName); ok {
		v.Schema = node.Schema.String()
		v.Table = node.Name.String()
	}

	if node, ok := in.(*ast.UpdateStmt); ok {
		v.Operation = common.UpdateDML
		v.Before = make(map[string]interface{}, 1)
		if node.Where != nil {
			if err := beforeData(v.logger, node.Where, v.Before); err != nil {
				v.Error = errors.Wrap(err, "update sql restore column failed")
				return node, true
			}
		}
	}

	if node, ok := in.(*ast.InsertStmt); ok {
		v.Operation = common.InsertDML
		v.Data = make(map[string]interface{}, 1)
		for i, col := range node.Columns {
			v.Columns = append(v.Columns, StringsBuilder("`", col.String(), "`"))
			for _, lists := range node.Lists {
				var columnValue strings.Builder
				flags := format.RestoreStringWithoutDefaultCharset
				err := lists[i].Restore(format.NewRestoreCtx(flags, &columnValue))
				if err != nil {
					v.Error = errors.Wrap(err, "inserl sql restore column value failed")
					return in, true
				}
				v.NewColumnValues = append(v.NewColumnValues, columnsValueConverter(columnValue.String()))
			}
		}
	}

	if node, ok := in.(*ast.DeleteStmt); ok {
		v.Operation = common.DeleteDML
		v.Before = make(map[string]interface{}, 1)
		if node.Where != nil {
			if err := beforeData(v.logger, node.Where, v.Before); err != nil {
				v.Error = errors.Wrap(err, "delete sql restore column failed")
				return in, true
			}
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
		v.logger.Error("stmt unmarshal fail")
	}
	return string(b)
}

func beforeData(logger g.LoggerType, where ast.ExprNode, before map[string]interface{}) (err error) {
	if binaryNode, ok := where.(*ast.BinaryOperationExpr); ok {
		switch binaryNode.Op.String() {
		case ast.LogicAnd:
			if err = beforeData(logger, binaryNode.L, before); err != nil {
				return
			}
			if err = beforeData(logger, binaryNode.R, before); err != nil {
				return
			}
		case ast.EQ:
			var column strings.Builder
			var columnValue strings.Builder
			flags := format.RestoreStringWithoutDefaultCharset
			err = binaryNode.L.Restore(format.NewRestoreCtx(flags, &column))
			if err != nil {
				return errors.Wrap(err, "restore column name failed")
			}
			err = binaryNode.R.Restore(format.NewRestoreCtx(flags, &columnValue))
			if err != nil {
				return errors.Wrap(err, "restore column value failed")
			}
			before[strings.TrimLeft(strings.TrimRight(column.String(), "`"), "`")] = columnsValueConverter(columnValue.String())
		}
	}
	return
}

// type OracleFuncName string

const (
	NullValue              = "NULL"
	EmptyCLOBFunction      = "EMPTY_CLOB()"
	EmptyBLOBFunction      = "EMPTY_BLOB()"
	FunctionHEXTORAWStart  = `HEXTORAW(`
	CommonFunctionEnd      = `)`
	InfValue               = `Inf`
	NInfValue              = `-Inf`
	NanValue               = `Nan`
	ToDSintervalStart      = "TO_DSINTERVAL("
	ToYMintervalStart      = "TO_YMINTERVAL("
	FunctionUNITSTRStart   = "UNISTR("
	ToDateFuncStart        = "TO_DATE("
	ToDateFuncEnd          = ", SYYYY-MM-DD HH24:MI:SS)"
	ToTimestampFuncStart   = "TO_TIMESTAMP("
	ToTimestampTzFuncStart = "TO_TIMESTAMP_TZ("
)

var CONCATENATIONPATTERN = "\\|\\|"

func columnsValueConverter(value string) interface{} {
	value = strings.TrimLeft(strings.TrimRight(value, "'"), "'")
	value = strings.ReplaceAll(value, `\\`, `\`)
	// value = value[1 : len(value)-1]
	switch {
	case value == "":
		return nil
	case value == NullValue:
		return nil
	case value == EmptyCLOBFunction:
		return ""
	case value == EmptyBLOBFunction:
		return ""
	case strings.HasPrefix(value, FunctionHEXTORAWStart) && strings.HasSuffix(value, CommonFunctionEnd):
		hexval, err := hex.DecodeString(value[9 : len(value)-1])
		if err != nil {
			return ""
		}
		return hexval
	case strings.HasPrefix(value, ToDSintervalStart) && strings.HasSuffix(value, CommonFunctionEnd):
		return value[14 : len(value)-1]
	case strings.HasPrefix(value, ToYMintervalStart) && strings.HasSuffix(value, CommonFunctionEnd):
		return value[14 : len(value)-1]
	case isUnistrFunction(value):
		return UnitstrConvert(value)
	case strings.HasPrefix(value, ToDateFuncStart) && strings.HasSuffix(value, ToDateFuncEnd):
		return value[8 : len(value)-25]
	case strings.HasPrefix(value, ToTimestampFuncStart) && strings.HasSuffix(value, CommonFunctionEnd):
		return value[13 : len(value)-1]
	case strings.HasPrefix(value, ToTimestampTzFuncStart) && strings.HasSuffix(value, CommonFunctionEnd):
		return value[16 : len(value)-2]
	// mysql no support (inf -inf nan)
	case value == InfValue:
		return nil
	case value == NInfValue:
		return nil
	case value == NInfValue:
		return nil
	case value == NanValue:
		return nil
	}

	return value
}

func isUnistrFunction(data string) bool {
	return strings.HasPrefix(data, FunctionUNITSTRStart) && strings.HasSuffix(data, CommonFunctionEnd)
}

func UnitstrConvert(data string) string {
	if data == "" {
		return data
	}
	// no unit case
	// Multiple UNISTR function calls maybe concatenated together using "||".
	// We split the values into their respective parts before parsing each one separately.
	regex, err := regexp.Compile(CONCATENATIONPATTERN)
	if err != nil {
		return ""
	}
	parts := regex.Split(data, -1)
	results := make([]string, len(parts))
	for i := range parts {
		trimPart := strings.TrimSpace(parts[i])
		if isUnistrFunction(trimPart) {
			results = append(results, UnitstrDecode(trimPart[7:len(trimPart)-1]))
		} else {
			results = append(results, data)
		}
	}
	return strings.Join(results, "")
}

func UnitstrDecode(value string) string {
	strconv.ParseInt(value, 10, 16)
	results := make([]string, 0)
	lens := len(value)

	for i := 0; i < lens; {
		if value[i] != '\\' {
			results = append(results, string(value[i]))
			i += 1
		} else {
			if lens >= (i + 4) {
				// Read next 4 character hex and convert to character.
				temp, err := strconv.ParseInt(string(value[i+1:i+5]), 16, 0)
				if err != nil {
					return ""
				}

				results = append(results, fmt.Sprintf("%c", temp))
				i += 5
				continue
			}
		}

	}
	return strings.Join(results, "")
}

func oracleTp2MySQLTp(td *oracle_ast.ColumnDef) *ast.ColumnDef {
	columnName := IdentifierToString(td.ColumnName)
	column := &ast.ColumnDef{
		Name: &ast.ColumnName{
			Name: model.NewCIStr(columnName),
		},
	}
	for i := range td.Constraints {
		column.Options = append(column.Options, transConstraintOtoM(td.Constraints[i]))
	}

	tp := &types.FieldType{
		Flen:    types.UnspecifiedLength,
		Decimal: types.UnspecifiedLength,
	}

	switch td.Datatype.DataDef() {
	case oracle_element.DataDefBFile:
		tp.Tp = mysql.TypeVarchar
		tp.Flen = 255
	case oracle_element.DataDefBinaryFloat:
		tp.Tp = mysql.TypeFloat
	case oracle_element.DataDefBinaryDouble:
		tp.Tp = mysql.TypeDouble
	case oracle_element.DataDefBlob:
		tp.Tp = mysql.TypeLongBlob
		tp.Flag = mysql.BinaryFlag
		tp.Charset = "binary"
		tp.Collate = "binary"
	case oracle_element.DataDefChar:
		if *td.Datatype.(*oracle_element.Char).Size >= 1 && *td.Datatype.(*oracle_element.Char).Size <= 255 {
			tp.Tp = mysql.TypeString
			tp.Flen = *td.Datatype.(*oracle_element.Char).Size
		} else if *td.Datatype.(*oracle_element.Char).Size >= 256 && *td.Datatype.(*oracle_element.Char).Size <= 2000 {
			tp.Tp = mysql.TypeVarchar
			tp.Flen = *td.Datatype.(*oracle_element.Char).Size
		}
	case oracle_element.DataDefCharacter:
		if *td.Datatype.(*oracle_element.Char).Size >= 1 && *td.Datatype.(*oracle_element.Char).Size <= 255 {
			tp.Tp = mysql.TypeString
			tp.Flen = *td.Datatype.(*oracle_element.Char).Size
		} else if *td.Datatype.(*oracle_element.Char).Size >= 256 && *td.Datatype.(*oracle_element.Char).Size <= 2000 {
			tp.Tp = mysql.TypeVarchar
			tp.Flen = *td.Datatype.(*oracle_element.Char).Size
		}
	case oracle_element.DataDefClob:
		tp.Tp = mysql.TypeLongBlob
	case oracle_element.DataDefDate:
		tp.Tp = mysql.TypeDatetime
	case oracle_element.DataDefDecimal, oracle_element.DataDefDec:
		tp.Tp = mysql.TypeNewDecimal
		tp.Flen = td.Datatype.(*oracle_element.Number).Precision.Number
		tp.Decimal = LimitSize(*td.Datatype.(*oracle_element.Number).Scale)
	case oracle_element.DataDefDoublePrecision:
		tp.Tp = mysql.TypeDouble
	case oracle_element.DataDefFloat:
		tp.Tp = mysql.TypeDouble
	case oracle_element.DataDefInteger:
		tp.Tp = mysql.TypeLong
	case oracle_element.DataDefInt:
		tp.Tp = mysql.TypeLong
	case oracle_element.DataDefIntervalYear, oracle_element.DataDefIntervalDay:
		tp.Tp = mysql.TypeVarchar
		tp.Flen = 30
	case oracle_element.DataDefLong:
		tp.Tp = mysql.TypeLongBlob
	case oracle_element.DataDefLongRaw:
		tp.Tp = mysql.TypeLongBlob
		tp.Flag = mysql.BlobFlag
		tp.Charset = "binary"
		tp.Collate = "binary"
	case oracle_element.DataDefNChar:
		if *td.Datatype.(*oracle_element.NChar).Size >= 1 && *td.Datatype.(*oracle_element.NChar).Size <= 255 {
			tp.Tp = mysql.TypeString
			tp.Flen = *td.Datatype.(*oracle_element.NChar).Size
		} else if *td.Datatype.(*oracle_element.NChar).Size >= 256 && *td.Datatype.(*oracle_element.NChar).Size <= 2000 {
			tp.Tp = mysql.TypeVarchar
			//tp.Flag = 0
			tp.Flen = *td.Datatype.(*oracle_element.NChar).Size
		}
	case oracle_element.DataDefNCharVarying:
		tp.Tp = mysql.TypeVarchar
		tp.Flen = *td.Datatype.(*oracle_element.NVarchar2).Size
	case oracle_element.DataDefNClob:
		tp.Tp = mysql.TypeBlob
	case oracle_element.DataDefNumber:
		num := td.Datatype.(*oracle_element.Number)
		if num.Precision == nil { // p == nil s == nil
			tp.Tp = mysql.TypeDouble
		} else {
			p := num.Precision.Number
			if num.Scale != nil && *num.Scale != 0 {
				tp.Tp = mysql.TypeNewDecimal
				tp.Flen = p
				tp.Decimal = LimitSize(*num.Scale)
			} else {
				switch {
				case p <= 0:
					tp.Tp = mysql.TypeDouble
				case p < 3:
					tp.Tp = mysql.TypeTiny
				case p < 5:
					tp.Tp = mysql.TypeShort
				case p < 9:
					tp.Tp = mysql.TypeLong
				case p < 19:
					tp.Tp = mysql.TypeLonglong
				case p <= 38:
					tp.Tp = mysql.TypeNewDecimal
					tp.Flen = p
				}
			}
		}
	case oracle_element.DataDefNumeric:
		tp.Tp = mysql.TypeNewDecimal
		tp.Flen = td.Datatype.(*oracle_element.Number).Precision.Number
		tp.Decimal = LimitSize(*td.Datatype.(*oracle_element.Number).Scale)
	case oracle_element.DataDefNVarChar2:
		tp.Tp = mysql.TypeVarchar
		tp.Flen = *td.Datatype.(*oracle_element.NVarchar2).Size
	case oracle_element.DataDefRaw:
		tp.Tp = mysql.TypeVarchar
		tp.Flen = *td.Datatype.(*oracle_element.Raw).Size
		tp.Flag = mysql.BinaryFlag
		tp.Charset = "binary"
		tp.Collate = "binary"
	case oracle_element.DataDefReal:
		tp.Tp = mysql.TypeDouble
	case oracle_element.DataDefRowId:
		tp.Tp = mysql.TypeString
		tp.Flen = 100
	case oracle_element.DataDefSmallInt:
		tp.Tp = mysql.TypeNewDecimal
		tp.Flen = 38
	case oracle_element.DataDefTimestamp:
		// todo with time zone
		fractionalSecondsPrecision := *td.Datatype.(*oracle_element.Timestamp).FractionalSecondsPrecision
		if fractionalSecondsPrecision > 6 {
			fractionalSecondsPrecision = 6
		}
		tp.Tp = mysql.TypeDatetime
		tp.Decimal = fractionalSecondsPrecision
	case oracle_element.DataDefURowId:
		tp.Tp = mysql.TypeVarchar
		tp.Flen = *td.Datatype.(*oracle_element.URowId).Size
	case oracle_element.DataDefVarchar:
		tp.Tp = mysql.TypeVarchar
		tp.Flen = *td.Datatype.(*oracle_element.Varchar2).Size
	case oracle_element.DataDefVarchar2:
		tp.Tp = mysql.TypeVarchar
		tp.Flen = *td.Datatype.(*oracle_element.Varchar2).Size
	case oracle_element.DataDefXMLType:
		tp.Tp = mysql.TypeLongBlob
	}
	column.Tp = tp
	return column
}

func transConstraintOtoM(oracleConstraint *oracle_ast.InlineConstraint) (constraint *ast.ColumnOption) {
	constraint = new(ast.ColumnOption)
	switch oracleConstraint.Type {
	case oracle_ast.ConstraintTypeNotNull:
		constraint.Tp = ast.ColumnOptionNotNull
	case oracle_ast.ConstraintTypeNull:
		constraint.Tp = ast.ColumnOptionNull
	case oracle_ast.ConstraintTypeUnique:
		constraint.Tp = ast.ColumnOptionUniqKey
	case oracle_ast.ConstraintTypePK:
		constraint.Tp = ast.ColumnOptionPrimaryKey
	case oracle_ast.ConstraintTypeReferences:
		// todo
	}
	return
}

// MySQL DEC/DECIMAL/NUMERIC type max scale is 30
func LimitSize(dec int) int {
	if dec > 30 {
		return 30
	}
	return dec
}

func IdentifierToString(i *oracle_element.Identifier) string {
	if i.Typ == oracle_element.IdentifierTypeNonQuoted {
		return strings.ToUpper(i.Value)
	} else if i.Typ == oracle_element.IdentifierTypeQuoted {
		return strings.TrimLeft(strings.TrimRight(i.Value, `"`), `"`)
	}
	return ""
}

func HandlingForSpecialCharacters(i *oracle_element.Identifier) string {
	return fmt.Sprintf("`%s`", IdentifierToString(i))
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
