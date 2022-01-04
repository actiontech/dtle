/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package base

import (
	"bytes"
	gosql "database/sql"
	"fmt"
	"github.com/actiontech/dtle/drivers/common"
	"github.com/actiontech/dtle/g"
	"regexp"
	"strings"
	"time"

	sqle "github.com/actiontech/dtle/drivers/mysql/sqle/inspector"

	"github.com/pingcap/tidb/parser/ast"
	parsermysql "github.com/pingcap/tidb/parser/mysql"

	"database/sql"

	umconf "github.com/actiontech/dtle/drivers/mysql/mysqlconfig"
	usql "github.com/actiontech/dtle/drivers/mysql/sql"
	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/siddontang/go/hack"
)

var (
	prettifyDurationRegexp = regexp.MustCompile("([.][0-9]+)")
)

func PrettifyDurationOutput(d time.Duration) string {
	if d < time.Second {
		return "0s"
	}
	result := fmt.Sprintf("%s", d)
	result = prettifyDurationRegexp.ReplaceAllString(result, "")
	return result
}

// StringContainsAll returns true if `s` contains all non empty given `substrings`
// The function returns `false` if no non-empty arguments are given.
func StringContainsAll(s string, substrings ...string) bool {
	nonEmptyStringsFound := false
	for _, substring := range substrings {
		if substring == "" {
			continue
		}
		if strings.Contains(s, substring) {
			nonEmptyStringsFound = true
		} else {
			// Immediate failure
			return false
		}
	}
	return nonEmptyStringsFound
}

func GetSelfBinlogCoordinates(db usql.QueryAble) (selfBinlogCoordinates *common.BinlogCoordinatesX, err error) {
	return ParseBinlogCoordinatesFromRow(usql.ShowMasterStatus(db))
}

func ParseBinlogCoordinatesFromRow(row *sql.Row) (r *common.BinlogCoordinatesX, err error) {
	r = &common.BinlogCoordinatesX{}
	var dummy interface{}
	err = row.Scan(&r.LogFile, &r.LogPos, &dummy, &dummy, &r.GtidSet)
	if err != nil {
		return nil, err
	}
	return r, nil
}

// GetTableColumns reads column list from given table
func GetTableColumns(db usql.QueryAble, databaseName, tableName string) (*common.ColumnList, error) {
	databaseNameEscaped := umconf.EscapeName(databaseName)
	tableNameEscaped := umconf.EscapeName(tableName)

	query := fmt.Sprintf(`show columns from %s.%s`, databaseNameEscaped, tableNameEscaped)
	columns := []umconf.Column{}
	err := usql.QueryRowsMap(db, query, func(rowMap usql.RowMap) error {
		aColumn := umconf.Column{
			RawName:    rowMap.GetString("Field"),
			ColumnType: rowMap.GetString("Type"),
			Default:    rowMap.GetString("Default"),
			Key:        strings.ToUpper(rowMap.GetString("Key")),
			Nullable:   strings.ToUpper(rowMap.GetString("Null")) == "YES",
		}
		aColumn.EscapedName = umconf.EscapeName(aColumn.RawName)
		columns = append(columns, aColumn)
		return nil
	})
	if err != nil {
		return nil, err
	}
	if len(columns) == 0 {
		return nil, fmt.Errorf("found 0 columns on %s.%s", databaseName, tableName)
	}

	return common.NewColumnList(columns), nil
}

func GetSomeSysVars(db usql.QueryAble, logger g.LoggerType) (r struct {
	Err                 error
	Version             string
	TimeZome            string
	LowerCaseTableNames umconf.LowerCaseTableNamesValue
	NetWriteTimeout     int
}) {
	query := `select @@version, @@time_zone, @@lower_case_table_names, @@net_write_timeout`
	r.Err = db.QueryRow(query).Scan(&r.Version, &r.TimeZome, &r.LowerCaseTableNames, &r.NetWriteTimeout)
	if r.Err != nil {
		return
	}

	if r.LowerCaseTableNames == umconf.LowerCaseTableNames2 {
		r.Err = fmt.Errorf("MySQL lower_case_table_names = 2 is not supported")
		return
	}

	logger.Info("got sys_var version", "value", r.Version)
	logger.Info("got sys_var timezone", "value", r.TimeZome)
	logger.Info("got sys_var lower_case_table_names", "value", r.LowerCaseTableNames)
	logger.Info("got sys_var net_write_timeout", "value", r.NetWriteTimeout)

	return r
}

func ShowCreateTable(db usql.QueryAble, databaseName, tableName string) (statement string, err error) {
	var dummy, createTableStatement string
	query := fmt.Sprintf(`show create table %s.%s`, umconf.EscapeName(databaseName), umconf.EscapeName(tableName))
	err = db.QueryRow(query).Scan(&dummy, &createTableStatement)
	return createTableStatement, err
}

func ShowCreateView(db *gosql.DB, databaseName, tableName string, dropTableIfExists bool) (createTableStatement string, err error) {
	var dummy, character_set_client, collation_connection string
	query := fmt.Sprintf(`show create table %s.%s`, umconf.EscapeName(databaseName), umconf.EscapeName(tableName))
	err = db.QueryRow(query).Scan(&dummy, &createTableStatement, &character_set_client, &collation_connection)
	statement := fmt.Sprintf("USE %s", umconf.EscapeName(databaseName))
	if dropTableIfExists {
		statement = fmt.Sprintf("%s;DROP TABLE IF EXISTS `%s`", statement, tableName)
	}
	return fmt.Sprintf("%s;%s", statement, createTableStatement), err
}

func StringInterval(intervals gomysql.IntervalSlice) string {
	buf := new(bytes.Buffer)

	for idx, i := range intervals {
		if idx != 0 {
			buf.WriteString(":")
		}
		buf.WriteString(i.String())
	}

	return hack.String(buf.Bytes())
}

// applyColumnTypes
func ApplyColumnTypes(db usql.QueryAble, databaseName, tableName string, columnsLists ...*common.ColumnList) error {
	query := `
		select
				*
			from
				information_schema.columns
			where
				table_schema=?
				and table_name=?
		`
	err := usql.QueryRowsMap(db, query, func(m usql.RowMap) error {
		columnName := m.GetString("COLUMN_NAME")
		columnType := m.GetString("COLUMN_TYPE")
		if strings.Contains(columnType, "unsigned") {
			for _, columnsList := range columnsLists {
				columnsList.SetUnsigned(columnName)
			}
		}
		if strings.Contains(columnType, "mediumint") {
			for _, columnsList := range columnsLists {
				columnsList.GetColumn(columnName).Type = umconf.MediumIntColumnType
			}
		}
		if strings.Contains(columnType, "timestamp") {
			for _, columnsList := range columnsLists {
				columnsList.GetColumn(columnName).Type = umconf.TimestampColumnType
				columnsList.GetColumn(columnName).ColumnType = columnType
			}
		}
		if strings.Contains(columnType, "datetime") {
			for _, columnsList := range columnsLists {
				col := columnsList.GetColumn(columnName)
				col.Type = umconf.DateTimeColumnType
				col.Precision = m.GetInt("DATETIME_PRECISION")
			}
		}
		if strings.HasPrefix(columnType, "enum") {
			for _, columnsList := range columnsLists {
				columnsList.GetColumn(columnName).Type = umconf.EnumColumnType
				columnsList.GetColumn(columnName).ColumnType = columnType
			}
		}
		if strings.HasPrefix(columnType, "binary") {
			for _, columnsList := range columnsLists {
				columnsList.GetColumn(columnName).Type = umconf.BinaryColumnType
				columnsList.GetColumn(columnName).ColumnType = columnType
			}
		}
		if strings.Contains(columnType, "text") {
			for _, columnsList := range columnsLists {
				columnsList.GetColumn(columnName).Type = umconf.TextColumnType
				columnsList.GetColumn(columnName).ColumnType = columnType
			}
		}
		if strings.Contains(columnType, "json") {
			for _, columnsList := range columnsLists {
				columnsList.GetColumn(columnName).Type = umconf.JSONColumnType
			}
		}
		if strings.Contains(columnType, "float") {
			for _, columnsList := range columnsLists {
				columnsList.GetColumn(columnName).Type = umconf.FloatColumnType
			}
		}
		if strings.HasPrefix(columnType, "varbinary") {
			for _, columnsList := range columnsLists {
				columnsList.GetColumn(columnName).Type = umconf.VarbinaryColumnType
				columnsList.GetColumn(columnName).ColumnType = columnType
			}
		}
		if strings.HasPrefix(columnType, "char") {
			for _, columnsList := range columnsLists {
				columnsList.GetColumn(columnName).Type = umconf.CharColumnType
				columnsList.GetColumn(columnName).ColumnType = columnType
			}
		}
		if strings.HasPrefix(columnType, "varchar") {
			for _, columnsList := range columnsLists {
				columnsList.GetColumn(columnName).Type = umconf.VarcharColumnType
				columnsList.GetColumn(columnName).ColumnType = columnType
			}
		}
		if strings.HasPrefix(columnType, "date") {
			for _, columnsList := range columnsLists {
				columnsList.GetColumn(columnName).Type = umconf.DateColumnType
			}
		}
		if strings.HasPrefix(columnType, "year") {
			for _, columnsList := range columnsLists {
				columnsList.GetColumn(columnName).Type = umconf.YearColumnType
			}
		}
		if strings.HasPrefix(columnType, "time") {
			for _, columnsList := range columnsLists {
				col := columnsList.GetColumn(columnName)
				col.Type = umconf.TimeColumnType
				col.Precision = m.GetInt("DATETIME_PRECISION")
			}
		}
		if strings.Contains(columnType, "blob") {
			for _, columnsList := range columnsLists {
				columnsList.GetColumn(columnName).Type = umconf.BlobColumnType
			}
		}
		if strings.HasPrefix(columnType, "bit") {
			for _, columnsList := range columnsLists {
				columnsList.GetColumn(columnName).Type = umconf.BitColumnType
			}
		}
		if strings.HasPrefix(columnType, "int") {
			for _, columnsList := range columnsLists {
				columnsList.GetColumn(columnName).Type = umconf.IntColumnType
				columnsList.GetColumn(columnName).ColumnType = columnType
			}
		}
		if strings.HasPrefix(columnType, "tinyint") {
			for _, columnsList := range columnsLists {
				columnsList.GetColumn(columnName).Type = umconf.TinyintColumnType
				columnsList.GetColumn(columnName).ColumnType = columnType
			}
		}
		if strings.HasPrefix(columnType, "smallint") {
			for _, columnsList := range columnsLists {
				columnsList.GetColumn(columnName).Type = umconf.SmallintColumnType
				columnsList.GetColumn(columnName).ColumnType = columnType
			}
		}
		if strings.HasPrefix(columnType, "bigint") {
			for _, columnsList := range columnsLists {
				columnsList.GetColumn(columnName).Type = umconf.BigIntColumnType
				columnsList.GetColumn(columnName).ColumnType = columnType
			}
		}
		if strings.HasPrefix(columnType, "decimal") {
			for _, columnsList := range columnsLists {
				col := columnsList.GetColumn(columnName)
				col.Type = umconf.DecimalColumnType
				col.ColumnType = columnType
				col.Precision = m.GetInt("NUMERIC_PRECISION")
				col.Scale = m.GetInt("NUMERIC_SCALE")
			}
		}
		if strings.HasPrefix(columnType, "double") {
			for _, columnsList := range columnsLists {
				columnsList.GetColumn(columnName).Type = umconf.DoubleColumnType
				columnsList.GetColumn(columnName).ColumnType = columnType
			}
		}
		if strings.HasPrefix(columnType, "tinytext") {
			for _, columnsList := range columnsLists {
				columnsList.GetColumn(columnName).Type = umconf.TinytextColumnType
				columnsList.GetColumn(columnName).ColumnType = columnType
			}
		}
		if strings.HasPrefix(columnType, "set") {
			for _, columnsList := range columnsLists {
				columnsList.GetColumn(columnName).Type = umconf.SetColumnType
				columnsList.GetColumn(columnName).ColumnType = columnType
			}
		}
		// TODO return err on unknown type?
		if charset := m.GetString("CHARACTER_SET_NAME"); charset != "" {
			for _, columnsList := range columnsLists {
				columnsList.SetCharset(columnName, charset)
			}
		}
		return nil
	}, databaseName, tableName)
	return err
}

func GtidSetDiff(set1 string, set2 string) (string, error) {
	gStartHelper, err := gomysql.ParseMysqlGTIDSet(set2)
	if err != nil {
		return "", err
	}

	gStart, ok := gStartHelper.(*gomysql.MysqlGTIDSet)
	if !ok {
		return "", fmt.Errorf("internal error: cannot cast MysqlGTIDSet")
	}

	gExecutedHelper, err := gomysql.ParseMysqlGTIDSet(set1)
	if err != nil {
		return "", err
	}

	gExecuted, ok := gExecutedHelper.(*gomysql.MysqlGTIDSet)
	if !ok {
		return "", fmt.Errorf("internal error: cannot cast MysqlGTIDSet")
	}

	for sid, startSet := range gStart.Sets {
		// one for each UUID
		if startSet.Intervals.Len() != 1 {
			return "", fmt.Errorf("bad format for GtidStart")
		}
		// only start
		if startSet.Intervals[0].Start+1 != startSet.Intervals[0].Stop {
			return "", fmt.Errorf("bad format for GtidStart")
		}

		startPoint := startSet.Intervals[0].Start
		execSets, ok := gExecuted.Sets[sid]
		if !ok {
			// do nothing
		} else {
			newIntervals := gomysql.IntervalSlice{}
			for i, _ := range execSets.Intervals {
				if execSets.Intervals[i].Start >= startPoint {
					continue
				} else if execSets.Intervals[i].Stop >= startPoint {
					newIntervals = append(newIntervals, gomysql.Interval{
						Start: execSets.Intervals[i].Start,
						Stop:  startPoint,
					})
				} else {
					newIntervals = append(newIntervals, execSets.Intervals[i])
				}
			}
			execSets.Intervals = newIntervals
		}
	}

	return gExecuted.String(), nil
}

func GetTableColumnsSqle(sqleContext *sqle.Context, schema string,
	table string) (r *common.ColumnList, fkParents []*ast.TableName, err error) {
	tableInfo, exists := sqleContext.GetTable(schema, table)
	if !exists {
		return nil, nil, fmt.Errorf("table does not exists in sqle context. table: %v.%v", schema, table)
	}

	cStmt := tableInfo.MergedTable
	if cStmt == nil {
		cStmt = tableInfo.OriginalTable
	}

	columns := []umconf.Column{}

	pks, _ := sqle.GetPrimaryKey(cStmt)

	for _, col := range cStmt.Cols {
		newColumn := umconf.Column{
			RawName:  col.Name.Name.String(),
			Nullable: true, // by default
		}
		newColumn.EscapedName = umconf.EscapeName(newColumn.RawName)
		if _, inPk := pks[newColumn.RawName]; inPk {
			newColumn.Key = "PRI"
		}

		if parsermysql.HasUnsignedFlag(col.Tp.Flag) {
			newColumn.IsUnsigned = true
		}

		newColumn.ColumnType = col.Tp.String()

		switch col.Tp.Tp {
		case parsermysql.TypeUnspecified, parsermysql.TypeNewDecimal:
			newColumn.Type = umconf.DecimalColumnType
			newColumn.Precision = col.Tp.Flen
			newColumn.Scale = col.Tp.Decimal
		case parsermysql.TypeTiny:
			newColumn.Type = umconf.TinyintColumnType
		case parsermysql.TypeShort:
			newColumn.Type = umconf.SmallintColumnType
		case parsermysql.TypeLong:
			newColumn.Type = umconf.IntColumnType
		case parsermysql.TypeLonglong:
			newColumn.Type = umconf.BigIntColumnType
		case parsermysql.TypeInt24:
			newColumn.Type = umconf.MediumIntColumnType
		case parsermysql.TypeFloat:
			newColumn.Type = umconf.FloatColumnType
		case parsermysql.TypeDouble:
			newColumn.Type = umconf.DoubleColumnType
		case parsermysql.TypeNull:
			newColumn.Type = umconf.UnknownColumnType
		case parsermysql.TypeTimestamp:
			newColumn.Type = umconf.TimestampColumnType
		case parsermysql.TypeDate:
			newColumn.Type = umconf.DateColumnType
		case parsermysql.TypeDuration:
			newColumn.Type = umconf.TimeColumnType
			newColumn.Precision = col.Tp.Decimal
		case parsermysql.TypeDatetime:
			newColumn.Type = umconf.DateTimeColumnType
			newColumn.Precision = col.Tp.Decimal
		case parsermysql.TypeYear:
			newColumn.Type = umconf.YearColumnType
		case parsermysql.TypeNewDate:
			newColumn.Type = umconf.DateColumnType
		case parsermysql.TypeVarchar:
			newColumn.Type = umconf.VarcharColumnType
		case parsermysql.TypeBit:
			newColumn.Type = umconf.BitColumnType
		case parsermysql.TypeJSON:
			newColumn.Type = umconf.JSONColumnType
		case parsermysql.TypeEnum:
			newColumn.Type = umconf.EnumColumnType
		case parsermysql.TypeSet:
			newColumn.Type = umconf.SetColumnType
		case parsermysql.TypeTinyBlob:
			newColumn.Type = umconf.BlobColumnType
		case parsermysql.TypeMediumBlob:
			newColumn.Type = umconf.BlobColumnType
		case parsermysql.TypeLongBlob:
			newColumn.Type = umconf.BlobColumnType
		case parsermysql.TypeBlob:
			newColumn.Type = umconf.BlobColumnType
		case parsermysql.TypeVarString:
			newColumn.Type = umconf.TextColumnType
		case parsermysql.TypeString:
			newColumn.Type = umconf.VarcharColumnType
		case parsermysql.TypeGeometry:
			newColumn.Type = umconf.UnknownColumnType
		}

		for _, colOpt := range col.Options {
			switch colOpt.Tp {
			case ast.ColumnOptionNoOption:
			case ast.ColumnOptionPrimaryKey:
				// TODO multiple value?
				newColumn.Key = "PRI"
			case ast.ColumnOptionNotNull:
				newColumn.Nullable = false
			case ast.ColumnOptionAutoIncrement:
			case ast.ColumnOptionDefaultValue:
				value, ok := colOpt.Expr.(ast.ValueExpr)
				if !ok {
					newColumn.Default = nil
				} else {
					newColumn.Default = value.GetValue()
				}
			case ast.ColumnOptionUniqKey:
				newColumn.Key = "UNI"
			case ast.ColumnOptionNull:
				newColumn.Nullable = true
				// `not null` and `null` can occurred multiple times and the latter wins
			case ast.ColumnOptionOnUpdate:
			case ast.ColumnOptionFulltext:
			case ast.ColumnOptionComment:
			case ast.ColumnOptionGenerated:
			case ast.ColumnOptionReference:
			}
		}

		columns = append(columns, newColumn)
	}

	for _, cons := range cStmt.Constraints {
		switch cons.Tp {
		case ast.ConstraintPrimaryKey:
		case ast.ConstraintKey:
		case ast.ConstraintIndex:
		case ast.ConstraintUniq:
		case ast.ConstraintUniqKey:
		case ast.ConstraintUniqIndex:
		case ast.ConstraintForeignKey:
			fkParents = append(fkParents, cons.Refer.Table)
		case ast.ConstraintFulltext:
		}
	}

	r = common.NewColumnList(columns)
	//r.SetCharset() // TODO
	return r, fkParents, nil
}

func GetCandidateUniqueKeys(logger g.LoggerType, db usql.QueryAble, databaseName, tableName string,
	columns *common.ColumnList) (uniqueKeys []*common.UniqueKey, err error) {

	/* example query result:
	+------------+--------------+-------------------+--------------+
	| INDEX_NAME | COLUMN_NAMES | is_auto_increment | has_nullable |
	+------------+--------------+-------------------+--------------+
	| PRIMARY    | id           |                 1 |            0 |
	| val1       | val1,val2    |                 0 |            0 |
	+------------+--------------+-------------------+--------------+
	*/
	query := `
SELECT UNIQUES.INDEX_NAME, UNIQUES.COLUMN_NAMES, LOCATE('auto_increment', EXTRA) > 0 as is_auto_increment, has_nullable
FROM INFORMATION_SCHEMA.COLUMNS
     INNER JOIN
     (SELECT TABLE_SCHEMA, TABLE_NAME, INDEX_NAME,
             GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX ASC) AS COLUMN_NAMES,
             SUBSTRING_INDEX(GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX ASC), ',', 1) AS FIRST_COLUMN_NAME,
             SUM(NULLABLE='YES') > 0 AS has_nullable
      FROM INFORMATION_SCHEMA.STATISTICS
      WHERE NON_UNIQUE=0 AND TABLE_SCHEMA = ? AND TABLE_NAME = ?
      GROUP BY TABLE_SCHEMA,TABLE_NAME,INDEX_NAME) AS UNIQUES
     ON (COLUMNS.TABLE_SCHEMA = UNIQUES.TABLE_SCHEMA
         AND COLUMNS.TABLE_NAME = UNIQUES.TABLE_NAME
         AND COLUMNS.COLUMN_NAME = UNIQUES.FIRST_COLUMN_NAME)
WHERE COLUMNS.TABLE_SCHEMA = ? AND COLUMNS.TABLE_NAME = ?`
	/*query := `
	    SELECT
	      COLUMNS.TABLE_SCHEMA,
	      COLUMNS.TABLE_NAME,
	      COLUMNS.COLUMN_NAME,
	      UNIQUES.INDEX_NAME,
	      UNIQUES.COLUMN_NAMES,
	      UNIQUES.COUNT_COLUMN_IN_INDEX,
	      COLUMNS.DATA_TYPE,
	      COLUMNS.CHARACTER_SET_NAME,
				LOCATE('auto_increment', EXTRA) > 0 as is_auto_increment,
	      has_nullable
	    FROM INFORMATION_SCHEMA.COLUMNS INNER JOIN (
	      SELECT
	        TABLE_SCHEMA,
	        TABLE_NAME,
	        INDEX_NAME,
	        COUNT(*) AS COUNT_COLUMN_IN_INDEX,
	        GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX ASC) AS COLUMN_NAMES,
	        SUBSTRING_INDEX(GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX ASC), ',', 1) AS FIRST_COLUMN_NAME,
	        SUM(NULLABLE='YES') > 0 AS has_nullable
	      FROM INFORMATION_SCHEMA.STATISTICS
	      WHERE
					NON_UNIQUE=0
					AND TABLE_SCHEMA = ?
	      	AND TABLE_NAME = ?
	      GROUP BY TABLE_SCHEMA, TABLE_NAME, INDEX_NAME
	    ) AS UNIQUES
	    ON (
	      COLUMNS.TABLE_SCHEMA = UNIQUES.TABLE_SCHEMA AND
	      COLUMNS.TABLE_NAME = UNIQUES.TABLE_NAME AND
	      COLUMNS.COLUMN_NAME = UNIQUES.FIRST_COLUMN_NAME
	    )
	    WHERE
	      COLUMNS.TABLE_SCHEMA = ?
	      AND COLUMNS.TABLE_NAME = ?
	    ORDER BY
	      COLUMNS.TABLE_SCHEMA, COLUMNS.TABLE_NAME,
	      CASE UNIQUES.INDEX_NAME
	        WHEN 'PRIMARY' THEN 0
	        ELSE 1
	      END,
	      CASE has_nullable
	        WHEN 0 THEN 0
	        ELSE 1
	      END,
	      CASE IFNULL(CHARACTER_SET_NAME, '')
	          WHEN '' THEN 0
	          ELSE 1
	      END,
	      CASE DATA_TYPE
	        WHEN 'tinyint' THEN 0
	        WHEN 'smallint' THEN 1
	        WHEN 'int' THEN 2
	        WHEN 'bigint' THEN 3
	        ELSE 100
	      END,
	      COUNT_COLUMN_IN_INDEX
	  `*/
	err = usql.QueryRowsMap(db, query, func(m usql.RowMap) error {
		columns := common.ParseColumnList(m.GetString("COLUMN_NAMES"), columns)
		uniqueKey := &common.UniqueKey{
			Name:            m.GetString("INDEX_NAME"),
			Columns:         *columns,
			HasNullable:     m.GetBool("has_nullable"),
			IsAutoIncrement: m.GetBool("is_auto_increment"),
			LastMaxVals:     make([]string, len(columns.Columns)),
		}
		uniqueKeys = append(uniqueKeys, uniqueKey)
		return nil
	}, databaseName, tableName, databaseName, tableName)
	if err != nil {
		return uniqueKeys, err
	}
	logger.Debug("Potential unique keys.", "schema", databaseName, "table", tableName, "uniqueKeys", uniqueKeys)
	return uniqueKeys, nil
}
