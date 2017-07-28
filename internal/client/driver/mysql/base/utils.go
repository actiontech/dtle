package base

import (
	gosql "database/sql"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	gomysql "github.com/siddontang/go-mysql/mysql"

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
		selfBinlogCoordinates = &BinlogCoordinates{
			LogFile: m.GetString("File"),
			LogPos:  m.GetInt64("Position"),
			GtidSet: gtidSet.String(),
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

func ApplyColumnTypes(db *gosql.Tx, database, tablename string, columnsLists ...*umconf.ColumnList) ([]*umconf.ColumnList, error) {
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
