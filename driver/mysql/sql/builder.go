/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package sql

import (
	"bytes"
	gosql "database/sql"
	"fmt"
	"github.com/actiontech/dtle/g"
	"strings"

	"github.com/actiontech/dtle/driver/common"

	umconf "github.com/actiontech/dtle/driver/mysql/mysqlconfig"
)

type ValueComparisonSign string

const (
	LessThanComparisonSign            ValueComparisonSign = "<"
	LessThanOrEqualsComparisonSign                        = "<="
	EqualsComparisonSign                                  = "="
	IsEqualsComparisonSign                                = "is"
	GreaterThanOrEqualsComparisonSign                     = ">="
	GreaterThanComparisonSign                             = ">"
	NotEqualsComparisonSign                               = "!="
)

func EscapeColRawToString(col *[]byte) string {
	if col != nil {
		return fmt.Sprintf("'%s'", EscapeValue(string(*col)))
	} else {
		return "NULL"
	}
}

func EscapeValue(colValue string) string {
    // https://dev.mysql.com/doc/refman/8.0/en/string-literals.html
	var esc string
	colBuffer := *new(bytes.Buffer)
	last := 0
	for i, c := range colValue {
		switch c {
		case 0:
			esc = `\0`
		case '\'':
			esc = `\'`
		case '"':
			esc = `\"`
		case '\n':
			esc = `\n`
		case '\r':
			esc = `\r`
		case '\t':
			esc = `\t`
		case '\032':
			esc = `\Z`
		case '\\':
			esc = `\\`
		default:
			continue
		}
		colBuffer.WriteString(colValue[last:i])
		colBuffer.WriteString(esc)
		last = i + 1
	}
	colBuffer.WriteString(colValue[last:])
	return colBuffer.String()
}

func BuildValueComparison(columnEscaped string, value string, comparisonSign ValueComparisonSign) (result string, err error) {
	if columnEscaped == "``" {
		return "", fmt.Errorf("Empty column in GetValueComparison")
	}
	if value == "" {
		return "", fmt.Errorf("Empty value in GetValueComparison")
	}
	comparison := fmt.Sprintf("(%s %s %s)", columnEscaped, string(comparisonSign), value)
	return comparison, err
}

func BuildDMLDeleteQuery(databaseName, tableName string, tableColumns *common.ColumnList, columnMapTo []string,
	args []interface{}, stmt *gosql.Stmt) (result string, columnArgs []interface{}, hasUK bool, err error) {

	if !(len(args) >= tableColumns.Len() || len(args) == len(columnMapTo)) {
		return result, columnArgs, hasUK, fmt.Errorf("args count differs from table column count in BuildDMLDeleteQuery %v %v %v",
			len(args), tableColumns.Len(), len(columnMapTo))
	}

	comparisons := []string{}
	uniqueKeyComparisons := []string{}
	uniqueKeyArgs := make([]interface{}, 0)

	for i := range args {
		column := getColumnWithMapTo(i, columnMapTo, tableColumns)

		if column == nil {
			g.Logger.Warn("BuildDMLDeleteQuery: unable to find column. ignoring",
				"columnMapTo", columnMapTo, "i", i, "len", tableColumns.Len())
			continue
		}

		if args[i] == nil {
			comparison, err := BuildValueComparison(column.EscapedName, "NULL", IsEqualsComparisonSign)
			if err != nil {
				return result, columnArgs, hasUK, err
			}
			comparisons = append(comparisons, comparison)
		} else {
			if column.Type == umconf.BinaryColumnType {
				arg := column.ConvertArg(args[i])
				comparison, err := BuildValueComparison(column.EscapedName, fmt.Sprintf("cast('%v' as %s)", arg, column.ColumnType), EqualsComparisonSign)
				if err != nil {
					return result, columnArgs, hasUK, err
				}
				if column.IsPk() {
					uniqueKeyComparisons = append(uniqueKeyComparisons, comparison)
				} else {
					comparisons = append(comparisons, comparison)
				}
			} else {
				arg := column.ConvertArg(args[i])
				comparison, err := BuildValueComparison(column.EscapedName, "?", EqualsComparisonSign)
				if err != nil {
					return result, columnArgs, hasUK, err
				}
				if column.IsPk() {
					uniqueKeyArgs = append(uniqueKeyArgs, arg)
					uniqueKeyComparisons = append(uniqueKeyComparisons, comparison)
				} else {
					columnArgs = append(columnArgs, arg)
					comparisons = append(comparisons, comparison)
				}
			}
		}
	}
	if len(uniqueKeyComparisons) > 0 {
		hasUK = true
		comparisons = uniqueKeyComparisons
		columnArgs = uniqueKeyArgs
	}

	if hasUK && stmt != nil {
		result = ""
	} else {
		databaseName = umconf.EscapeName(databaseName)
		tableName = umconf.EscapeName(tableName)
		result = fmt.Sprintf(`delete from %s.%s where
%s limit 1`, databaseName, tableName,
			fmt.Sprintf("(%s)", strings.Join(comparisons, " and ")),
		)
	}
	// hasUK: true: use saved PS to execute; false: execute a new query.
	return result, columnArgs, hasUK, nil
}

func BuildDMLInsertQuery(databaseName, tableName string, tableColumns *common.ColumnList, columnMapTo []string,
	rows [][]interface{}, stmt *gosql.Stmt) (result string, sharedArgs []interface{}, err error) {

	if len(rows) == 0 {
		return "", nil, fmt.Errorf("BuildDMLInsertQuery: rows is empty %v.%v", databaseName, tableName)
	}

	var placeholders []string

	for iRow := range rows {
		args := rows[iRow]

		if iRow == 0 {
			if !(len(args) >= tableColumns.Len() || len(args) == len(columnMapTo)) {
				return "", nil, fmt.Errorf("BuildDMLInsertQuery: args count differs from table column count %v %v %v",
					len(args), tableColumns.Len(), len(columnMapTo))
			}
		} else {
			if len(args) != len(rows[0]) {
				return "", nil, fmt.Errorf("BuildDMLInsertQuery: args count differs from args0 %v %v %v",
					len(args), len(rows[0]))
			}
		}

		for i := range args {
			column := getColumnWithMapTo(i, columnMapTo, tableColumns)

			if iRow == 0 {
				if column != nil && column.TimezoneConversion != nil {
					placeholders = append(placeholders,
						fmt.Sprintf("convert_tz(?, '%s', '%s')", column.TimezoneConversion.ToTimezone, "+00:00"))
				} else {
					placeholders = append(placeholders, "?")
				}
			}

			if column != nil {
				sharedArgs = append(sharedArgs, column.ConvertArg(args[i]))
			} else {
				sharedArgs = append(sharedArgs, args[i])
			}
		}
	}

	if stmt != nil {
		result = ""
	} else {
		placeholdersStr := strings.Join(placeholders, ",")

		sb := strings.Builder{}
		sb.WriteString("replace into ")
		sb.WriteString(umconf.EscapeName(databaseName))
		sb.WriteByte('.')
		sb.WriteString(umconf.EscapeName(tableName))
		sb.WriteByte(' ')
		sb.WriteString(umconf.BuildInsertColumnList(columnMapTo))
		sb.WriteString(" values (")
		for i := 0; i < len(rows); i++ {
			if i > 0 {
				sb.WriteString("),(")
			}
			sb.WriteString(placeholdersStr)
		}
		sb.WriteByte(')')

		result = sb.String()
	}
	return result, sharedArgs, nil
}

func getColumnWithMapTo(columnIndex int, columnMapTo []string, tableColumns *common.ColumnList) *umconf.Column {
	if len(columnMapTo) > 0 {
		return tableColumns.GetColumn(columnMapTo[columnIndex])
	} else if columnIndex < tableColumns.Len() {
		return &tableColumns.ColumnList()[columnIndex]
	}
	return nil
}

func BuildDMLUpdateQuery(databaseName, tableName string, tableColumns *common.ColumnList, columnMapTo []string, valueArgs, whereArgs []interface{}, stmt *gosql.Stmt) (result string, sharedArgs, columnArgs []interface{}, hasUK bool, err error) {
	//if len(valueArgs) < tableColumns.Len() {
	//	return result, sharedArgs, columnArgs, hasUK, fmt.Errorf("value args count differs from table column count in BuildDMLUpdateQuery %v, %v",
	//		len(valueArgs), tableColumns.Len())
	//}
	//if len(whereArgs) < tableColumns.Len() {
	//	return result, sharedArgs, columnArgs, hasUK, fmt.Errorf("where args count differs from table column count in BuildDMLUpdateQuery %v, %v",
	//		len(whereArgs), tableColumns.Len())
	//}

	databaseName = umconf.EscapeName(databaseName)
	tableName = umconf.EscapeName(tableName)

	comparisons := []string{}
	uniqueKeyComparisons := []string{}
	uniqueKeyArgs := make([]interface{}, 0)
	setTokens := []string{}

	for i := range whereArgs {
		column := getColumnWithMapTo(i, columnMapTo, tableColumns)

		if valueArgs[i] == nil || valueArgs[i] == "NULL" ||
			fmt.Sprintf("%v", valueArgs[i]) == "" {
			sharedArgs = append(sharedArgs, valueArgs[i])
		} else {
			arg := column.ConvertArg(valueArgs[i])
			sharedArgs = append(sharedArgs, arg)
		}

		if column == nil {
			g.Logger.Warn("BuildDMLDeleteQuery: unable to find column. ignoring",
				"columnMapTo", columnMapTo, "i", i, "len", tableColumns.Len())
			continue
		}

		if whereArgs[i] == nil {
			comparison, err := BuildValueComparison(column.EscapedName, "NULL", IsEqualsComparisonSign)
			if err != nil {
				return result, sharedArgs, columnArgs, hasUK, err
			}
			comparisons = append(comparisons, comparison)
		} else {
			if column.Type == umconf.BinaryColumnType {
				arg := column.ConvertArg(whereArgs[i])
				comparison, err := BuildValueComparison(column.EscapedName, fmt.Sprintf("cast('%v' as %s)", arg, column.ColumnType), EqualsComparisonSign)
				if err != nil {
					return result, sharedArgs, columnArgs, hasUK, err
				}
				if column.IsPk() {
					uniqueKeyComparisons = append(uniqueKeyComparisons, comparison)
				} else {
					comparisons = append(comparisons, comparison)
				}
			} else {
				arg := column.ConvertArg(whereArgs[i])
				comparison, err := BuildValueComparison(column.EscapedName, "?", EqualsComparisonSign)
				if err != nil {
					return result, sharedArgs, columnArgs, hasUK, err
				}
				if column.IsPk() {
					uniqueKeyArgs = append(uniqueKeyArgs, arg)
					uniqueKeyComparisons = append(uniqueKeyComparisons, comparison)
				} else {
					columnArgs = append(columnArgs, arg)
					comparisons = append(comparisons, comparison)
				}
			}
		}

		var setToken string
		if column.TimezoneConversion != nil {
			setToken = fmt.Sprintf("%s=convert_tz(?, '%s', '%s')", column.EscapedName, column.TimezoneConversion.ToTimezone, "+00:00")
		} else {
			setToken = fmt.Sprintf("%s=?", column.EscapedName)
		}
		setTokens = append(setTokens, setToken)
	}

	if len(uniqueKeyComparisons) > 0 {
		hasUK = true
		comparisons = uniqueKeyComparisons
		columnArgs = uniqueKeyArgs
	}

	if hasUK && stmt != nil {
		result = ""
	} else {
		result = fmt.Sprintf(`update %s.%s set
%s
where
%s limit 1`, databaseName, tableName,
			strings.Join(setTokens, ", "),
			fmt.Sprintf("(%s)", strings.Join(comparisons, " and ")),
		)
	}
	return result, sharedArgs, columnArgs, hasUK, nil
}
