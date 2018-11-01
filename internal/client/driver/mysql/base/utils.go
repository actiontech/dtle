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
	"github.com/actiontech/dtle/internal/g"
	"regexp"
	"strconv"
	"strings"
	"time"

	"database/sql"

	"github.com/satori/go.uuid"
	gomysql "github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go/hack"

	usql "github.com/actiontech/dtle/internal/client/driver/mysql/sql"
	umconf "github.com/actiontech/dtle/internal/config/mysql"
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

func GetSelfBinlogCoordinates(db *gosql.DB) (selfBinlogCoordinates *BinlogCoordinatesX, err error) {
	err = usql.QueryRowsMap(db, `show master status`, func(m usql.RowMap) error {
		selfBinlogCoordinates = &BinlogCoordinatesX{
			LogFile: m.GetString("File"),
			LogPos:  m.GetInt64("Position"),
			GtidSet: m.GetString("Executed_Gtid_Set"),
		}
		return nil
	})
	return selfBinlogCoordinates, err
}

func ParseBinlogCoordinatesFromRows(rows *sql.Rows) (selfBinlogCoordinates *BinlogCoordinatesX, err error) {
	err = usql.ScanRowsToMaps(rows, func(m usql.RowMap) error {
		selfBinlogCoordinates = &BinlogCoordinatesX{
			LogFile: m.GetString("File"),
			LogPos:  m.GetInt64("Position"),
			GtidSet: m.GetString("Executed_Gtid_Set"),
		}
		return nil
	})
	return selfBinlogCoordinates, err
}

// GetTableColumns reads column list from given table
func GetTableColumns(db usql.QueryAble, databaseName, tableName string) (*umconf.ColumnList, error) {
	query := fmt.Sprintf(`
		show columns from %s.%s
		`,
		usql.EscapeName(databaseName),
		usql.EscapeName(tableName),
	)
	columns := []umconf.Column{}
	err := usql.QueryRowsMap(db, query, func(rowMap usql.RowMap) error {
		columns = append(columns, umconf.Column{
			Name:       rowMap.GetString("Field"),
			ColumnType: rowMap.GetString("Type"),
			Key:        strings.ToUpper(rowMap.GetString("Key")),
			Nullable:   strings.ToUpper(rowMap.GetString("Null")) == "YES",
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	if len(columns) == 0 {
		return nil, fmt.Errorf("Found 0 columns on %s.%s. Bailing out",
			usql.EscapeName(databaseName),
			usql.EscapeName(tableName),
		)
	}
	return umconf.NewColumnList(columns), nil
}

func ShowCreateTable(db *gosql.DB, databaseName, tableName string, dropTableIfExists bool) (statement []string, err error) {
	var dummy, createTableStatement string
	query := fmt.Sprintf(`show create table %s.%s`, usql.EscapeName(databaseName), usql.EscapeName(tableName))
	err = db.QueryRow(query).Scan(&dummy, &createTableStatement)
	statement = append(statement, fmt.Sprintf("USE %s", databaseName))
	if dropTableIfExists {
		statement = append(statement, fmt.Sprintf("DROP TABLE IF EXISTS `%s`", tableName))
	}
	statement = append(statement, createTableStatement)
	return statement, err
}

func ShowCreateView(db *gosql.DB, databaseName, tableName string, dropTableIfExists bool) (createTableStatement string, err error) {
	var dummy, character_set_client, collation_connection string
	query := fmt.Sprintf(`show create table %s.%s`, usql.EscapeName(databaseName), usql.EscapeName(tableName))
	err = db.QueryRow(query).Scan(&dummy, &createTableStatement, &character_set_client, &collation_connection)
	statement := fmt.Sprintf("USE %s", databaseName)
	if dropTableIfExists {
		statement = fmt.Sprintf("%s;DROP TABLE IF EXISTS `%s`", statement, tableName)
	}
	return fmt.Sprintf("%s;%s", statement, createTableStatement), err
}

// Interval is [start, stop), but the GTID string's format is [n] or [n1-n2], closed interval
func parseInterval(str string) (i gomysql.Interval, err error) {
	p := strings.Split(str, "-")
	switch len(p) {
	case 1:
		i.Start, err = strconv.ParseInt(p[0], 10, 64)
		i.Stop = i.Start + 1
	case 2:
		i.Start, err = strconv.ParseInt(p[0], 10, 64)
		i.Stop, err = strconv.ParseInt(p[1], 10, 64)
		i.Stop = i.Stop + 1
	default:
		err = fmt.Errorf("invalid interval format, must n[-n]")
	}

	if err != nil {
		return
	}

	if i.Stop <= i.Start {
		err = fmt.Errorf("invalid interval format, must n[-n] and the end must >= start")
	}

	return
}

// return: normalized GtidSet
func SelectAllGtidExecuted(db usql.QueryAble, jid uuid.UUID) (gtidSet GtidSet, err error) {
	query := fmt.Sprintf(`SELECT source_uuid,interval_gtid FROM %v.%v where job_uuid=?`,
		g.DtleSchemaName, g.GtidExecutedTableV3)

	rows, err := db.Query(query, jid.Bytes())
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	gtidSet = make(GtidSet)

	for rows.Next() {
		var sidUUID uuid.UUID
		var interval string
		err = rows.Scan(&sidUUID, &interval)

		if err != nil {
			return nil, err
		}

		item, ok := gtidSet[sidUUID]
		if !ok {
			item = &GtidExecutedItem{
				NRow:      0,
				Intervals: nil,
			}
			gtidSet[sidUUID] = item
		}
		item.NRow += 1
		sep := strings.Split(interval, ":")
		// Handle interval
		for i := 0; i < len(sep); i++ {
			if in, err := parseInterval(sep[i]); err != nil {
				return nil, err
			} else {
				item.Intervals = append(item.Intervals, in)
			}
		}
	}

	for sid, item := range gtidSet {
		gtidSet[sid].Intervals = item.Intervals.Normalize()
	}

	return gtidSet, err
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
func ApplyColumnTypes(db usql.QueryAble, databaseName, tableName string, columnsLists ...*umconf.ColumnList) error {
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
