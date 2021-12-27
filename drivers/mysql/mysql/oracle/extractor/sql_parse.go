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

	"github.com/pingcap/tidb/parser/format"

	"github.com/pingcap/tidb/parser/ast"
	parser "github.com/pingcap/tidb/types/parser_driver"
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
			beforeData(v.logger, node.Where, v.Before)
		}
	}

	if node, ok := in.(*ast.InsertStmt); ok {
		v.Operation = common.InsertDML
		v.Data = make(map[string]interface{}, 1)
		for i, col := range node.Columns {
			v.Columns = append(v.Columns, StringsBuilder("`", col.String(), "`"))
			for _, lists := range node.Lists {
				valueExpr, ok := lists[i].(*parser.ValueExpr)
				if !ok {
					v.logger.Error("Assertion failed")
					continue
				}
				v.NewColumnValues = append(v.NewColumnValues, columnsValueConverter(valueExpr.GetString()))
			}
		}
	}

	if node, ok := in.(*ast.DeleteStmt); ok {
		v.Operation = common.DeleteDML
		v.Before = make(map[string]interface{}, 1)
		if node.Where != nil {
			beforeData(v.logger, node.Where, v.Before)
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

func beforeData(logger g.LoggerType, where ast.ExprNode, before map[string]interface{}) {
	if binaryNode, ok := where.(*ast.BinaryOperationExpr); ok {
		switch binaryNode.Op.String() {
		case ast.LogicAnd:
			beforeData(logger, binaryNode.L, before)
			beforeData(logger, binaryNode.R, before)
		case ast.EQ:
			var value strings.Builder
			var column strings.Builder
			flags := format.DefaultRestoreFlags
			err := binaryNode.R.Restore(format.NewRestoreCtx(flags, &value))
			if err != nil {
				logger.Error("restore column value failed")
			}
			err = binaryNode.L.Restore(format.NewRestoreCtx(flags, &column))
			if err != nil {
				logger.Error("restore column name failed")
			}
			before[strings.TrimLeft(strings.TrimRight(column.String(), "`"), "`")] = columnsValueConverter(value.String())
		}
	}
}

// type OracleFuncName string

const (
	NullValue              = "NULL"
	EmptyCLOBFunction      = "EMPTY_CLOB()"
	EmptyBLOBFunction      = "EMPTY_BLOB()"
	FunctionHEXTORAWStart  = `HEXTORAW('`
	CommonFunctionEnd      = `')`
	InfValue               = `Inf`
	NInfValue              = `-Inf`
	NanValue               = `Nan`
	ToDSintervalStart      = "TO_DSINTERVAL('"
	ToYMintervalStart      = "TO_YMINTERVAL('"
	FunctionUNITSTRStart   = "UNISTR('"
	ToDateFuncStart        = "TO_DATE('"
	ToDateFuncEnd          = "', 'YYYY-MM-DD HH24:MI:SS')"
	ToTimestampFuncStart   = "TO_TIMESTAMP('"
	ToTimestampTzFuncStart = "TO_TIMESTAMP_TZ('"
)

var CONCATENATIONPATTERN = "\\|\\|"

func columnsValueConverter(value string) interface{} {
	value = strings.TrimLeft(strings.TrimRight(value, "'"), "'")
	// value = value[1 : len(value)-1]
	switch {
	case value == NullValue:
		return nil
	case value == EmptyCLOBFunction:
		return ""
	case value == EmptyBLOBFunction:
		return ""
	case strings.HasPrefix(value, FunctionHEXTORAWStart) && strings.HasSuffix(value, CommonFunctionEnd):
		hexval, err := hex.DecodeString(value[10 : len(value)-2])
		if err != nil {
			return ""
		}
		return hexval
	case strings.HasPrefix(value, ToDSintervalStart) && strings.HasSuffix(value, CommonFunctionEnd):
		return value[15 : len(value)-2]
	case strings.HasPrefix(value, ToYMintervalStart) && strings.HasSuffix(value, CommonFunctionEnd):
		return value[15 : len(value)-2]
	case isUnistrFunction(value):
		return UnitstrConvert(value)
	case strings.HasPrefix(value, ToDateFuncStart) && strings.HasSuffix(value, ToDateFuncEnd):
		return value[9 : len(value)-27]
	case strings.HasPrefix(value, ToTimestampFuncStart) && strings.HasSuffix(value, CommonFunctionEnd):
		return value[14 : len(value)-2]
	case strings.HasPrefix(value, ToTimestampTzFuncStart) && strings.HasSuffix(value, CommonFunctionEnd):
		return value[17 : len(value)-2]
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
			results = append(results, UnitstrDecode(trimPart[8:len(trimPart)-2]))
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
		// if c == '\\' {
		if value[i] == ' ' {
			results = append(results, string(value[i]))
			i++
		} else {
			if lens >= (i + 4) {
				// Read next 4 character hex and convert to character.
				temp, err := strconv.ParseInt(string(value[i:i+4]), 16, 0)
				if err != nil {
					return ""
				}

				results = append(results, fmt.Sprintf("%c", temp))
				i += 4
				continue
			}
		}

	}
	return strings.Join(results, "")
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
	case oracle_element.DataDefBFile:
		colDefinition = fmt.Sprintf("%s %s(%d)%s", HandlingForSpecialCharacters(td.ColumnName), MySQLColTypeVARCHAR, 255, colDefaultString(td.Default))
	case oracle_element.DataDefBinaryFloat:
		colDefinition = fmt.Sprintf("%s %s%s", HandlingForSpecialCharacters(td.ColumnName), MySQLColTypeFLOAT, colDefaultString(td.Default))
	case oracle_element.DataDefBinaryDouble:
		colDefinition = fmt.Sprintf("%s %s%s", HandlingForSpecialCharacters(td.ColumnName), MySQLColTypeDOUBLE, colDefaultString(td.Default))
	case oracle_element.DataDefBlob:
		colDefinition = fmt.Sprintf("%s %s%s", HandlingForSpecialCharacters(td.ColumnName), MySQLColTypeLONGBLOB, colDefaultString(td.Default))
	case oracle_element.DataDefChar:
		if *td.Datatype.(*oracle_element.Char).Size >= 1 && *td.Datatype.(*oracle_element.Char).Size <= 255 {
			colDefinition = fmt.Sprintf("%s %s(%d)%s", HandlingForSpecialCharacters(td.ColumnName), MySQLColTypeCHAR, *td.Datatype.(*oracle_element.Char).Size, colDefaultString(td.Default))
		} else if *td.Datatype.(*oracle_element.Char).Size >= 256 && *td.Datatype.(*oracle_element.Char).Size <= 2000 {
			colDefinition = fmt.Sprintf("%s %s(%d)%s", HandlingForSpecialCharacters(td.ColumnName), MySQLColTypeVARCHAR, *td.Datatype.(*oracle_element.Char).Size, colDefaultString(td.Default))
		}
	case oracle_element.DataDefCharacter:
		if *td.Datatype.(*oracle_element.Char).Size >= 1 && *td.Datatype.(*oracle_element.Char).Size <= 255 {
			colDefinition = fmt.Sprintf("%s %s(%d)%s", HandlingForSpecialCharacters(td.ColumnName), MySQLColTypeCHAR, *td.Datatype.(*oracle_element.Char).Size, colDefaultString(td.Default))
		} else if *td.Datatype.(*oracle_element.Char).Size >= 256 && *td.Datatype.(*oracle_element.Char).Size <= 2000 {
			colDefinition = fmt.Sprintf("%s %s(%d)%s", HandlingForSpecialCharacters(td.ColumnName), MySQLColTypeVARCHAR, *td.Datatype.(*oracle_element.Char).Size, colDefaultString(td.Default))
		}
	case oracle_element.DataDefClob:
		colDefinition = fmt.Sprintf("%s %s%s", HandlingForSpecialCharacters(td.ColumnName), MySQLColTypeLONGTEXT, colDefaultString(td.Default))
	case oracle_element.DataDefDate:
		colDefinition = fmt.Sprintf("%s %s%s", HandlingForSpecialCharacters(td.ColumnName), MySQLColTypeDATETIME, colDefaultString(td.Default))
	case oracle_element.DataDefDecimal:
		colDefinition = fmt.Sprintf("%s %s(%d,%d)%s", HandlingForSpecialCharacters(td.ColumnName), MySQLColTypeDECIMAL,
			td.Datatype.(*oracle_element.Number).Precision.Number, LimitSize(*td.Datatype.(*oracle_element.Number).Scale), colDefaultString(td.Default))
	case oracle_element.DataDefDec:
		colDefinition = fmt.Sprintf("%s %s(%d,%d)%s", HandlingForSpecialCharacters(td.ColumnName), MySQLColTypeDEC,
			td.Datatype.(*oracle_element.Number).Precision.Number, LimitSize(*td.Datatype.(*oracle_element.Number).Scale), colDefaultString(td.Default))
	case oracle_element.DataDefDoublePrecision:
		colDefinition = fmt.Sprintf("%s %s%s", HandlingForSpecialCharacters(td.ColumnName), MySQLColTypeDOUBLE, colDefaultString(td.Default))
	case oracle_element.DataDefFloat:
		colDefinition = fmt.Sprintf("%s %s%s", HandlingForSpecialCharacters(td.ColumnName), MySQLColTypeDOUBLE, colDefaultString(td.Default))
	case oracle_element.DataDefInteger:
		colDefinition = fmt.Sprintf("%s %s%s", HandlingForSpecialCharacters(td.ColumnName), MySQLColTypeINT, colDefaultString(td.Default))
	case oracle_element.DataDefInt:
		colDefinition = fmt.Sprintf("%s %s%s", HandlingForSpecialCharacters(td.ColumnName), MySQLColTypeINT, colDefaultString(td.Default))
	case oracle_element.DataDefIntervalYear:
		colDefinition = fmt.Sprintf("%s %s(%d)%s", HandlingForSpecialCharacters(td.ColumnName), MySQLColTypeVARCHAR, 30, colDefaultString(td.Default))
	case oracle_element.DataDefIntervalDay:
		colDefinition = fmt.Sprintf("%s %s(%d)%s", HandlingForSpecialCharacters(td.ColumnName), MySQLColTypeVARCHAR, 30, colDefaultString(td.Default))
	case oracle_element.DataDefLong:
		colDefinition = fmt.Sprintf("%s %s%s", HandlingForSpecialCharacters(td.ColumnName), MySQLColTypeLONGTEXT, colDefaultString(td.Default))
	case oracle_element.DataDefLongRaw:
		colDefinition = fmt.Sprintf("%s %s%s", HandlingForSpecialCharacters(td.ColumnName), MySQLColTypeLONGBLOB, colDefaultString(td.Default))
	case oracle_element.DataDefNChar:
		if *td.Datatype.(*oracle_element.NChar).Size >= 1 && *td.Datatype.(*oracle_element.NChar).Size <= 255 {
			colDefinition = fmt.Sprintf("%s %s(%d)%s", HandlingForSpecialCharacters(td.ColumnName), MySQLColTypeNCHAR, *td.Datatype.(*oracle_element.NChar).Size, colDefaultString(td.Default))
		} else if *td.Datatype.(*oracle_element.NChar).Size >= 256 && *td.Datatype.(*oracle_element.NChar).Size <= 2000 {
			colDefinition = fmt.Sprintf("%s %s(%d)%s", HandlingForSpecialCharacters(td.ColumnName), MySQLColTypeNVARCHAR, *td.Datatype.(*oracle_element.NChar).Size, colDefaultString(td.Default))
		}
	case oracle_element.DataDefNCharVarying:
		colDefinition = fmt.Sprintf("%s %s(%d)%s", HandlingForSpecialCharacters(td.ColumnName), MySQLColTypeNVARCHAR, *td.Datatype.(*oracle_element.NVarchar2).Size, colDefaultString(td.Default))
	case oracle_element.DataDefNClob:
		colDefinition = fmt.Sprintf("%s %s%s", HandlingForSpecialCharacters(td.ColumnName), MySQLColTypeTEXT, colDefaultString(td.Default))
	case oracle_element.DataDefNumber:
		var mysqlNumberType string
		num := td.Datatype.(*oracle_element.Number)
		if num.Precision == nil { // p == nil s == nil
			mysqlNumberType = MySQLColTypeDOUBLE
		} else {
			p := num.Precision.Number
			if num.Scale != nil && *num.Scale != 0 { // p !=nil  s == nil
				mysqlNumberType = fmt.Sprintf("%s(%d,%d)", MySQLColTypeDECIMAL, p, LimitSize(*num.Scale))
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
		colDefinition = fmt.Sprintf("%s %s%s", HandlingForSpecialCharacters(td.ColumnName), mysqlNumberType, colDefaultString(td.Default))
	case oracle_element.DataDefNumeric:
		colDefinition = fmt.Sprintf("%s %s(%d,%d)%s", HandlingForSpecialCharacters(td.ColumnName), MySQLColTypeNUMERIC,
			td.Datatype.(*oracle_element.Number).Precision.Number, LimitSize(*td.Datatype.(*oracle_element.Number).Scale), colDefaultString(td.Default))
	case oracle_element.DataDefNVarChar2:
		colDefinition = fmt.Sprintf("%s %s(%d)%s", HandlingForSpecialCharacters(td.ColumnName), MySQLColTypeNVARCHAR, *td.Datatype.(*oracle_element.NVarchar2).Size, colDefaultString(td.Default))
	case oracle_element.DataDefRaw:
		colDefinition = fmt.Sprintf("%s %s(%d)%s", HandlingForSpecialCharacters(td.ColumnName), MySQLColTypeVARBINARY, *td.Datatype.(*oracle_element.Raw).Size, colDefaultString(td.Default))
	case oracle_element.DataDefReal:
		colDefinition = fmt.Sprintf("%s %s%s", HandlingForSpecialCharacters(td.ColumnName), MySQLColTypeDOUBLE, colDefaultString(td.Default))
	case oracle_element.DataDefRowId:
		colDefinition = fmt.Sprintf("%s %s(%d)%s", HandlingForSpecialCharacters(td.ColumnName), MySQLColTypeCHAR, 100, colDefaultString(td.Default))
	case oracle_element.DataDefSmallInt:
		colDefinition = fmt.Sprintf("%s %s(%d)%s", HandlingForSpecialCharacters(td.ColumnName), MySQLColTypeDECIMAL, 38, colDefaultString(td.Default))
	case oracle_element.DataDefTimestamp:
		// todo with time zone
		fractionalSecondsPrecision := *td.Datatype.(*oracle_element.Timestamp).FractionalSecondsPrecision
		if fractionalSecondsPrecision > 6 {
			fractionalSecondsPrecision = 6
		}
		colDefinition = fmt.Sprintf("%s %s(%d)%s", HandlingForSpecialCharacters(td.ColumnName), MySQLColTypeDATETIME, fractionalSecondsPrecision, colDefaultString(td.Default))
	case oracle_element.DataDefURowId:
		colDefinition = fmt.Sprintf("%s %s(%d)%s", HandlingForSpecialCharacters(td.ColumnName), MySQLColTypeVARCHAR, *td.Datatype.(*oracle_element.URowId).Size, colDefaultString(td.Default))
	case oracle_element.DataDefVarchar:
		colDefinition = fmt.Sprintf("%s %s(%d)%s", HandlingForSpecialCharacters(td.ColumnName), MySQLColTypeVARCHAR, *td.Datatype.(*oracle_element.Varchar2).Size, colDefaultString(td.Default))
	case oracle_element.DataDefVarchar2:
		colDefinition = fmt.Sprintf("%s %s(%d)%s", HandlingForSpecialCharacters(td.ColumnName), MySQLColTypeVARCHAR, *td.Datatype.(*oracle_element.Varchar2).Size, colDefaultString(td.Default))
	case oracle_element.DataDefXMLType:
		colDefinition = fmt.Sprintf("%s %s%s", HandlingForSpecialCharacters(td.ColumnName), MySQLColTypeLONGTEXT, colDefaultString(td.Default))
	}
	return colDefinition
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
