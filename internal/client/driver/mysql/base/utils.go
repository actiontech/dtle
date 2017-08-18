package base

import (
	"bytes"
	gosql "database/sql"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/satori/go.uuid"
	gomysql "github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go/hack"

	usql "udup/internal/client/driver/mysql/sql"
	uconf "udup/internal/config"
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

func FileExists(fileName string) bool {
	if _, err := os.Stat(fileName); err == nil {
		return true
	}
	return false
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

const MaxTableNameLength = 64

type ReplicationLagResult struct {
	Key umconf.InstanceKey
	Lag time.Duration
	Err error
}

func NewNoReplicationLagResult() *ReplicationLagResult {
	return &ReplicationLagResult{Lag: 0, Err: nil}
}

func (r *ReplicationLagResult) HasLag() bool {
	return r.Lag > 0
}

// GetReplicationLag returns replication lag for a given connection config; either by explicit query
// or via SHOW SLAVE STATUS
func GetReplicationLag(connectionConfig *umconf.ConnectionConfig) (replicationLag time.Duration, err error) {
	dbUri := connectionConfig.GetDBUri()
	var db *gosql.DB
	if db, _, err = usql.GetDB(dbUri); err != nil {
		return replicationLag, err
	}

	err = usql.QueryRowsMap(db, `show slave status`, func(m usql.RowMap) error {
		slaveIORunning := m.GetString("Slave_IO_Running")
		slaveSQLRunning := m.GetString("Slave_SQL_Running")
		secondsBehindMaster := m.GetNullInt64("Seconds_Behind_Master")
		if !secondsBehindMaster.Valid {
			return fmt.Errorf("replication not running; Slave_IO_Running=%+v, Slave_SQL_Running=%+v", slaveIORunning, slaveSQLRunning)
		}
		replicationLag = time.Duration(secondsBehindMaster.Int64) * time.Second
		return nil
	})
	return replicationLag, err
}

func GetReplicationBinlogCoordinates(db *gosql.DB) (readBinlogCoordinates *BinlogCoordinates, executeBinlogCoordinates *BinlogCoordinates, err error) {
	err = usql.QueryRowsMap(db, `show slave status`, func(m usql.RowMap) error {
		readBinlogCoordinates = &BinlogCoordinates{
			LogFile: m.GetString("Master_Log_File"),
			LogPos:  m.GetInt64("Read_Master_Log_Pos"),
		}
		executeBinlogCoordinates = &BinlogCoordinates{
			LogFile: m.GetString("Relay_Master_Log_File"),
			LogPos:  m.GetInt64("Exec_Master_Log_Pos"),
		}
		return nil
	})
	return readBinlogCoordinates, executeBinlogCoordinates, err
}

func GetSelfBinlogCoordinates(db *gosql.DB) (selfBinlogCoordinates *BinlogCoordinates, err error) {
	err = usql.QueryRowsMap(db, `show master status`, func(m usql.RowMap) error {
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

		selfBinlogCoordinates = &BinlogCoordinates{
			LogFile: m.GetString("File"),
			LogPos:  m.GetInt64("Position"),
			GtidSet: ms.String(),
		}
		return nil
	})
	return selfBinlogCoordinates, err
}

// GetTableColumns reads column list from given table
func GetTableColumns(db *gosql.DB, databaseName, tableName string) (*umconf.ColumnList, error) {
	query := fmt.Sprintf(`
		show columns from %s.%s
		`,
		usql.EscapeName(databaseName),
		usql.EscapeName(tableName),
	)
	columnNames := []string{}
	err := usql.QueryRowsMap(db, query, func(rowMap usql.RowMap) error {
		columnNames = append(columnNames, rowMap.GetString("Field"))
		return nil
	})
	if err != nil {
		return nil, err
	}
	if len(columnNames) == 0 {
		return nil, fmt.Errorf("Found 0 columns on %s.%s. Bailing out",
			usql.EscapeName(databaseName),
			usql.EscapeName(tableName),
		)
	}
	return umconf.NewColumnList(columnNames), nil
}

func GetTableColumnsWithTx(db *gosql.Tx, databaseName, tableName string) (*umconf.ColumnList, error) {
	query := fmt.Sprintf(`
		show columns from %s.%s
		`,
		usql.EscapeName(databaseName),
		usql.EscapeName(tableName),
	)
	columnNames := []string{}
	err := usql.QueryRowsMapWithTx(db, query, func(rowMap usql.RowMap) error {
		columnNames = append(columnNames, rowMap.GetString("Field"))
		return nil
	})
	if err != nil {
		return nil, err
	}
	if len(columnNames) == 0 {
		return nil, fmt.Errorf("Found 0 columns on %s.%s. Bailing out",
			usql.EscapeName(databaseName),
			usql.EscapeName(tableName),
		)
	}
	return umconf.NewColumnList(columnNames), nil
}

func ApplyColumnTypesWithTx(db *gosql.Tx, database, tablename string, columnsLists ...*umconf.ColumnList) ([]*umconf.ColumnList, error) {
	query := `
		select
				*
			from
				information_schema.columns
			where
				table_schema=?
				and table_name=?
		`
	err := usql.QueryRowsMapWithTx(db, query, func(m usql.RowMap) error {
		columnName := m.GetString("COLUMN_NAME")
		columnType := m.GetString("COLUMN_TYPE")
		if strings.Contains(columnType, "unsigned") {
			for _, columnsList := range columnsLists {
				columnsList.SetUnsigned(columnName)
			}
		}
		if strings.Contains(columnType, "decimal") {
			for _, columnsList := range columnsLists {
				columnsList.GetColumn(columnName).Type = umconf.DecimalColumnType
			}
		}
		if strings.Contains(columnType, "float") {
			for _, columnsList := range columnsLists {
				columnsList.GetColumn(columnName).Type = umconf.FloatColumnType
			}
		}
		if strings.Contains(columnType, "double") {
			for _, columnsList := range columnsLists {
				columnsList.GetColumn(columnName).Type = umconf.DoubleColumnType
			}
		}
		if strings.Contains(columnType, "bigint") {
			for _, columnsList := range columnsLists {
				columnsList.GetColumn(columnName).Type = umconf.BigIntColumnType
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
		if charset := m.GetString("CHARACTER_SET_NAME"); charset != "" {
			for _, columnsList := range columnsLists {
				columnsList.SetCharset(columnName, charset)
			}
		}
		return nil
	}, database, tablename)
	return columnsLists, err
}

func ShowCreateTable(db *gosql.DB, databaseName, tableName string, dropTableIfExists bool) (createTableStatement string, err error) {
	var dummy string
	query := fmt.Sprintf(`show create table %s.%s`, usql.EscapeName(databaseName), usql.EscapeName(tableName))
	err = db.QueryRow(query).Scan(&dummy, &createTableStatement)
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

func SelectGtidExecuted(db *gosql.DB, sid string, gno int64) (gtidset string, err error) {
	query := fmt.Sprintf(`SELECT interval_gtid FROM actiontech_udup.gtid_executed where source_uuid='%s'`,
		sid,
	)

	rows, err := db.Query(query)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	var intervals gomysql.IntervalSlice
	for rows.Next() {
		var (
			interval string
		)

		err = rows.Scan(&interval)

		if err != nil {
			return "", err
		}
		sep := strings.Split(interval, ":")
		// Handle interval
		for i := 0; i < len(sep); i++ {
			if in, err := parseInterval(sep[i]); err != nil {
				return "", err
			} else {
				intervals = append(intervals, in)
			}
		}
	}

	if rows.Err() != nil {
		return "", rows.Err()
	}

	if len(intervals) == 0 {
		return fmt.Sprintf("%d", gno), nil
	} else {
		in, err := parseInterval(fmt.Sprintf("%d", gno))
		if err != nil {
			return "", err
		}
		if intervals.Contain([]gomysql.Interval{in}) {
			return "", nil
		}
		intervals = append(intervals, in)
	}

	intervals = intervals.Normalize()

	return stringInterval(intervals), nil
}

func stringInterval(intervals gomysql.IntervalSlice) string {
	buf := new(bytes.Buffer)

	for idx, i := range intervals {
		if idx != 0 {
			buf.WriteString(":")
		}
		buf.WriteString(i.String())
	}

	return hack.String(buf.Bytes())
}

// getSharedColumns returns the intersection of two lists of columns in same order as the first list
func getSharedColumns(doTb uconf.Table) (*umconf.ColumnList, *umconf.ColumnList) {
	columnsInGhost := make(map[string]bool)
	for _, ghostColumn := range doTb.OriginalTableColumns.Names() {
		columnsInGhost[ghostColumn] = true
	}
	sharedColumnNames := []string{}
	for _, originalColumn := range doTb.OriginalTableColumns.Names() {
		isSharedColumn := false
		if columnsInGhost[originalColumn] || columnsInGhost[doTb.ColumnRenameMap[originalColumn]] {
			isSharedColumn = true
		}
		if doTb.DroppedColumnsMap[originalColumn] {
			isSharedColumn = false
		}
		if isSharedColumn {
			sharedColumnNames = append(sharedColumnNames, originalColumn)
		}
	}
	mappedSharedColumnNames := []string{}
	for _, columnName := range sharedColumnNames {
		if mapped, ok := doTb.ColumnRenameMap[columnName]; ok {
			mappedSharedColumnNames = append(mappedSharedColumnNames, mapped)
		} else {
			mappedSharedColumnNames = append(mappedSharedColumnNames, columnName)
		}
	}
	return umconf.NewColumnList(sharedColumnNames), umconf.NewColumnList(mappedSharedColumnNames)
}

//It extracts the list of shared columns and the chosen extract unique key
func InspectTables(db *gosql.DB, databaseName string, doTb *uconf.Table, timeZone string) (err error) {
	/*originalNamesOnApplier := doTb.OriginalTableColumnsOnApplier.Names()
	originalNames := doTb.OriginalTableColumns.Names()
	if !reflect.DeepEqual(originalNames, originalNamesOnApplier) {
		return fmt.Errorf("It seems like table structure is not identical between master and replica. This scenario is not supported.")
	}*/

	doTb.SharedColumns, doTb.MappedSharedColumns = getSharedColumns(*doTb)
	//i.logger.Debugf("mysql.inspector: Shared columns are %s", doTb.SharedColumns)
	// By fact that a non-empty unique key exists we also know the shared columns are non-empty

	// This additional step looks at which columns are unsigned. We could have merged this within
	// the `getTableColumns()` function, but it's a later patch and introduces some complexity; I feel
	// comfortable in doing this as a separate step.
	applyColumnTypes(db, databaseName, doTb.TableName, doTb.OriginalTableColumns, doTb.SharedColumns)

	/*for c := range doTb.SharedColumns.ColumnList() {
		column := doTb.SharedColumns.ColumnList()[c]
		mappedColumn := doTb.MappedSharedColumns.ColumnList()[c]
		if column.Name == mappedColumn.Name && column.Type == umconf.DateTimeColumnType && mappedColumn.Type == umconf.TimestampColumnType {
			doTb.MappedSharedColumns.SetConvertDatetimeToTimestamp(column.Name, timeZone)
		}
	}*/

	return nil
}

// applyColumnTypes
func applyColumnTypes(db *gosql.DB, databaseName, tableName string, columnsLists ...*umconf.ColumnList) error {
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
		if strings.Contains(columnType, "binary") {
			for _, columnsList := range columnsLists {
				columnsList.GetColumn(columnName).Type = umconf.BinaryColumnType
				columnsList.GetColumn(columnName).ColumnType = columnType
			}
		}
		if charset := m.GetString("CHARACTER_SET_NAME"); charset != "" {
			for _, columnsList := range columnsLists {
				columnsList.SetCharset(columnName, charset)
			}
		}
		return nil
	}, databaseName, tableName)
	return err
}
