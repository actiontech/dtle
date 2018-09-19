/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package sql

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"

	umconf "github.com/actiontech/dtle/internal/config/mysql"
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

// EscapeName will escape a db/table/column/... name by wrapping with backticks.
// It is not fool proof. I'm just trying to do the right thing here, not solving
// SQL injection issues, which should be irrelevant for this tool.
func EscapeName(name string) string {
	if unquoted, err := strconv.Unquote(name); err == nil {
		name = unquoted
	}
	return fmt.Sprintf("`%s`", name)
}

func EscapeColRawToString(col *[]byte) string {
	if col != nil {
		return fmt.Sprintf("'%s'", EscapeValue(string(*col)))
	} else {
		return "NULL"
	}
}

func EscapeValue(colValue string) string {
	var esc string
	colBuffer := *new(bytes.Buffer)
	last := 0
	for i, c := range colValue {
		switch c {
		case 0:
			esc = `\0`
		case '\n':
			esc = `\n`
		case '\r':
			esc = `\r`
		case '\\':
			esc = `\\`
		case '\'':
			esc = `\'`
		case '"':
			esc = `\"`
		case '\032':
			esc = `\Z`
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

func buildColumnsPreparedValues(columns *umconf.ColumnList) []string {
	values := make([]string, columns.Len(), columns.Len())
	for i, column := range columns.ColumnList() {
		var token string
		if column.TimezoneConversion != nil {
			token = fmt.Sprintf("convert_tz(?, '%s', '%s')", column.TimezoneConversion.ToTimezone, "+00:00")
		} else {
			token = "?"
		}
		values[i] = token
	}
	return values
}

func duplicateNames(names []string) []string {
	duplicate := make([]string, len(names), len(names))
	copy(duplicate, names)
	return duplicate
}

func BuildValueComparison(column string, value string, comparisonSign ValueComparisonSign) (result string, err error) {
	if column == "" {
		return "", fmt.Errorf("Empty column in GetValueComparison")
	}
	if value == "" {
		return "", fmt.Errorf("Empty value in GetValueComparison")
	}
	comparison := fmt.Sprintf("(%s %s %s)", EscapeName(column), string(comparisonSign), value)
	return comparison, err
}

func BuildSetPreparedClause(columns *umconf.ColumnList) (result string, err error) {
	if columns.Len() == 0 {
		return "", fmt.Errorf("Got 0 columns in BuildSetPreparedClause")
	}
	setTokens := []string{}
	for _, column := range columns.ColumnList() {
		var setToken string
		if column.TimezoneConversion != nil {
			setToken = fmt.Sprintf("%s=convert_tz(?, '%s', '%s')", EscapeName(column.Name), column.TimezoneConversion.ToTimezone, "+00:00")
		} else {
			setToken = fmt.Sprintf("%s=?", EscapeName(column.Name))
		}
		setTokens = append(setTokens, setToken)
	}
	return strings.Join(setTokens, ", "), nil
}

func BuildDMLDeleteQuery(databaseName, tableName string, tableColumns *umconf.ColumnList, args []*interface{}) (result string, columnArgs []interface{}, err error) {
	if len(args) < tableColumns.Len() {
		return result, columnArgs, fmt.Errorf("args count differs from table column count in BuildDMLDeleteQuery %v, %v",
			len(args), tableColumns.Len())
	}
	comparisons := []string{}
	uniqueKeyComparisons := []string{}
	uniqueKeyArgs := make([]interface{}, 0)
	for _, column := range tableColumns.ColumnList() {
		tableOrdinal := tableColumns.Ordinals[column.Name]
		if *args[tableOrdinal] == nil {
			comparison, err := BuildValueComparison(column.Name, "NULL", IsEqualsComparisonSign)
			if err != nil {
				return result, columnArgs, err
			}
			comparisons = append(comparisons, comparison)
		} else {
			if strings.HasPrefix(column.ColumnType, "binary") {
				arg := column.ConvertArg(*args[tableOrdinal])
				comparison, err := BuildValueComparison(column.Name, fmt.Sprintf("cast('%v' as %s)", arg, column.ColumnType), EqualsComparisonSign)
				if err != nil {
					return result, columnArgs, err
				}
				if strings.ToUpper(column.Key) == "PRI" {
					uniqueKeyComparisons = append(uniqueKeyComparisons, comparison)
				} else {
					comparisons = append(comparisons, comparison)
				}
			} else {
				arg := column.ConvertArg(*args[tableOrdinal])
				comparison, err := BuildValueComparison(column.Name, "?", EqualsComparisonSign)
				if err != nil {
					return result, columnArgs, err
				}
				if strings.ToUpper(column.Key) == "PRI" {
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
		comparisons = uniqueKeyComparisons
	}
	if len(uniqueKeyArgs) > 0 {
		columnArgs = uniqueKeyArgs
	}
	databaseName = EscapeName(databaseName)
	tableName = EscapeName(tableName)
	if err != nil {
		return result, columnArgs, err
	}
	result = fmt.Sprintf(`
			delete
				from
					%s.%s
				where
					%s
		`, databaseName, tableName,
		fmt.Sprintf("(%s)", strings.Join(comparisons, " and ")),
	)
	return result, columnArgs, nil
}

func BuildDMLInsertQuery(databaseName, tableName string, tableColumns, sharedColumns, mappedSharedColumns *umconf.ColumnList, args []*interface{}) (result string, sharedArgs []interface{}, err error) {
	if len(args) < tableColumns.Len() {
		return result, sharedArgs, fmt.Errorf("args count differs from table column count in BuildDMLInsertQuery %v, %v",
			len(args), tableColumns.Len())
	}

	if !sharedColumns.IsSubsetOf(tableColumns) {
		return result, sharedArgs, fmt.Errorf("shared columns is not a subset of table columns in BuildDMLInsertQuery")
	}
	if sharedColumns.Len() == 0 {
		return result, sharedArgs, fmt.Errorf("No shared columns found in BuildDMLInsertQuery")
	}
	databaseName = EscapeName(databaseName)
	tableName = EscapeName(tableName)

	for _, column := range tableColumns.ColumnList() {
		tableOrdinal := tableColumns.Ordinals[column.Name]
		if *args[tableOrdinal] == nil {
			sharedArgs = append(sharedArgs, *args[tableOrdinal])
		} else {
			arg := column.ConvertArg(*args[tableOrdinal])
			sharedArgs = append(sharedArgs, arg)
		}
	}

	mappedSharedColumnNames := duplicateNames(tableColumns.Names())
	for i := range mappedSharedColumnNames {
		mappedSharedColumnNames[i] = EscapeName(mappedSharedColumnNames[i])
	}
	preparedValues := buildColumnsPreparedValues(tableColumns)

	result = fmt.Sprintf(`
			replace into
				%s.%s
					(%s)
				values
					(%s)
		`, databaseName, tableName,
		strings.Join(mappedSharedColumnNames, ", "),
		strings.Join(preparedValues, ", "),
	)
	return result, sharedArgs, nil
}

func BuildDMLUpdateQuery(databaseName, tableName string, tableColumns, sharedColumns, mappedSharedColumns, uniqueKeyColumns *umconf.ColumnList, valueArgs, whereArgs []*interface{}) (result string, sharedArgs, columnArgs []interface{}, err error) {
	if len(valueArgs) < tableColumns.Len() {
		return result, sharedArgs, columnArgs, fmt.Errorf("value args count differs from table column count in BuildDMLUpdateQuery %v, %v",
			len(valueArgs), tableColumns.Len())
	}
	if len(whereArgs) < tableColumns.Len() {
		return result, sharedArgs, columnArgs, fmt.Errorf("where args count differs from table column count in BuildDMLUpdateQuery %v, %v",
			len(whereArgs), tableColumns.Len())
	}
	if !sharedColumns.IsSubsetOf(tableColumns) {
		return result, sharedArgs, columnArgs, fmt.Errorf("shared columns is not a subset of table columns in BuildDMLUpdateQuery")
	}
	if sharedColumns.Len() == 0 {
		return result, sharedArgs, columnArgs, fmt.Errorf("No shared columns found in BuildDMLUpdateQuery")
	}
	databaseName = EscapeName(databaseName)
	tableName = EscapeName(tableName)

	for _, column := range tableColumns.ColumnList() {
		tableOrdinal := tableColumns.Ordinals[column.Name]
		if *valueArgs[tableOrdinal] == nil || *valueArgs[tableOrdinal] == "NULL" ||
			fmt.Sprintf("%v", *valueArgs[tableOrdinal]) == "" {
			sharedArgs = append(sharedArgs, *valueArgs[tableOrdinal])
		} else {
			arg := column.ConvertArg(*valueArgs[tableOrdinal])
			sharedArgs = append(sharedArgs, arg)
		}
	}

	comparisons := []string{}
	uniqueKeyComparisons := []string{}
	uniqueKeyArgs := make([]interface{}, 0)
	for _, column := range tableColumns.ColumnList() {
		tableOrdinal := tableColumns.Ordinals[column.Name]
		if *whereArgs[tableOrdinal] == nil {
			comparison, err := BuildValueComparison(column.Name, "NULL", IsEqualsComparisonSign)
			if err != nil {
				return result, sharedArgs, columnArgs, err
			}
			comparisons = append(comparisons, comparison)
		} else {
			if strings.HasPrefix(column.ColumnType, "binary") {
				arg := column.ConvertArg(*whereArgs[tableOrdinal])
				comparison, err := BuildValueComparison(column.Name, fmt.Sprintf("cast('%v' as %s)", arg, column.ColumnType), EqualsComparisonSign)
				if err != nil {
					return result, sharedArgs, columnArgs, err
				}
				if strings.ToUpper(column.Key) == "PRI" {
					uniqueKeyComparisons = append(uniqueKeyComparisons, comparison)
				} else {
					comparisons = append(comparisons, comparison)
				}
			} else {
				arg := column.ConvertArg(*whereArgs[tableOrdinal])
				comparison, err := BuildValueComparison(column.Name, "?", EqualsComparisonSign)
				if err != nil {
					return result, sharedArgs, columnArgs, err
				}
				if strings.ToUpper(column.Key) == "PRI" {
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
		comparisons = uniqueKeyComparisons
	}
	if len(uniqueKeyArgs) > 0 {
		columnArgs = uniqueKeyArgs
	}
	setClause, err := BuildSetPreparedClause(mappedSharedColumns)

	result = fmt.Sprintf(`
 			update
 					%s.%s
				set
					%s
				where
 					%s
 				limit 1
 		`, databaseName, tableName,
		setClause,
		fmt.Sprintf("(%s)", strings.Join(comparisons, " and ")),
	)
	return result, sharedArgs, columnArgs, nil
}
