package base

import (
	"bytes"
	gosql "database/sql"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"database/sql"
	"github.com/satori/go.uuid"
	gomysql "github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go/hack"

	usql "udup/internal/client/driver/mysql/sql"
	umconf "udup/internal/config/mysql"
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
		gtidSet, err := gomysql.ParseMysqlGTIDSet(m.GetString("Executed_Gtid_Set"))
		if err != nil {
			return err
		}
		ms := new(gomysql.MysqlGTIDSet)
		ms.Sets = make(map[string]*gomysql.UUIDSet)
		for _, gset := range strings.Split(gtidSet.String(), ",") {
			gset = strings.TrimSpace(gset)
			sep := strings.Split(gset, ":")
			if len(sep) < 2 {
				return fmt.Errorf("invalid GTID format, must UUID:interval[:interval]")
			}

			s := new(gomysql.UUIDSet)
			if s.SID, err = uuid.FromString(sep[0]); err != nil {
				return err
			}
			// Handle interval
			for i := 1; i < len(sep); i++ {
				if in, err := parseInterval(sep[i]); err != nil {
					return err
				} else {
					in.Start = 1
					s.Intervals = append(s.Intervals, in)
				}
			}
			s.Intervals = s.Intervals.Normalize()
			ms.AddSet(s)
		}

		selfBinlogCoordinates = &BinlogCoordinatesX{
			LogFile: m.GetString("File"),
			LogPos:  m.GetInt64("Position"),
			GtidSet: ms.String(),
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
			Name:rowMap.GetString("Field"),
			ColumnType:rowMap.GetString("Type"),
			Key:strings.ToUpper(rowMap.GetString("Key")),
			Nullable:strings.ToUpper(rowMap.GetString("Null")) == "YES",
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

func ContrastGtidSet(contrastGtid, currentGtid string) (bool, error) {
	for _, gset := range strings.Split(contrastGtid, ",") {
		gset = strings.TrimSpace(gset)
		sep := strings.Split(gset, ":")
		if len(sep) < 2 {
			return false, fmt.Errorf("invalid GTID format, must UUID:interval[:interval]")
		}
		sid, err := uuid.FromString(sep[0])
		if err != nil {
			return false, err
		}
		// Handle interval
		for i := 1; i < len(sep); i++ {
			if ein, err := parseInterval(sep[i]); err != nil {
				return false, err
			} else {
				for _, bgset := range strings.Split(currentGtid, ",") {
					bgset = strings.TrimSpace(bgset)
					bsep := strings.Split(bgset, ":")
					if len(bsep) < 2 {
						return false, fmt.Errorf("invalid GTID format, must UUID:interval[:interval]")
					}
					bsid, err := uuid.FromString(bsep[0])
					if err != nil {
						return false, err
					}
					if sid == bsid {
						for i := 1; i < len(bsep); i++ {
							if bin, err := parseInterval(bsep[i]); err != nil {
								return false, err
							} else {
								if bin.Stop != ein.Stop {
									return false, nil
								}
							}
						}
					}
				}
			}
		}
	}
	return true, nil
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

func SelectGtidExecuted(db usql.QueryAble, sid, jid string) (gtidset gomysql.IntervalSlice, err error) {
	query := fmt.Sprintf(`SELECT interval_gtid FROM actiontech_udup.gtid_executed where source_uuid='%s' and job_uuid='%s'`,
		sid, jid,
	)

	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var intervals gomysql.IntervalSlice
	for rows.Next() {
		var (
			interval string
		)

		err = rows.Scan(&interval)

		if err != nil {
			return nil, err
		}
		sep := strings.Split(interval, ":")
		// Handle interval
		for i := 0; i < len(sep); i++ {
			if in, err := parseInterval(sep[i]); err != nil {
				return nil, err
			} else {
				intervals = append(intervals, in)
			}
		}
	}

	if rows.Err() != nil {
		return nil, rows.Err()
	}

	return intervals, nil
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
				columnsList.GetColumn(columnName).Type = umconf.DateTimeColumnType
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
				columnsList.GetColumn(columnName).Type = umconf.TimeColumnType
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
				columnsList.GetColumn(columnName).Type = umconf.DecimalColumnType
				columnsList.GetColumn(columnName).ColumnType = columnType
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
		if startSet.Intervals[0].Start + 1 != startSet.Intervals[0].Stop {
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
				} else if execSets.Intervals[i].Stop>= startPoint {
					newIntervals = append(newIntervals, gomysql.Interval{
						Start:execSets.Intervals[i].Start,
						Stop:startPoint,
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
