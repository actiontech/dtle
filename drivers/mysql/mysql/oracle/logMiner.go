package oracle

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"time"

	"github.com/pingcap/parser"

	"github.com/actiontech/dtle/drivers/mysql/common"
	"github.com/thinkeridea/go-extend/exbytes"

	"strings"

	"github.com/godror/godror"
	"github.com/godror/godror/dsn"
	_ "github.com/pingcap/tidb/types/parser_driver"
)

type OracleDBMeta struct {
	User        string
	Password    string
	Host        string
	Port        uint32
	ServiceName string
}

func (m *OracleDBMeta) ConnectString() string {
	return fmt.Sprintf("%s:%d/%s", m.Host, m.Port, m.ServiceName)
}

type OracleDB struct {
	db *sql.DB
}

func NewDB(meta *OracleDBMeta) (*OracleDB, error) {
	oraDsn := godror.ConnectionParams{
		CommonParams: godror.CommonParams{
			Username:      meta.User,
			ConnectString: meta.ConnectString(),
			Password:      godror.NewPassword(meta.Password),
		},
		PoolParams: godror.PoolParams{
			MinSessions:    dsn.DefaultPoolMinSessions,
			MaxSessions:    dsn.DefaultPoolMaxSessions,
			WaitTimeout:    dsn.DefaultWaitTimeout,
			MaxLifeTime:    dsn.DefaultMaxLifeTime,
			SessionTimeout: dsn.DefaultSessionTimeout,
		},
	}
	sqlDB := sql.OpenDB(godror.NewConnector(oraDsn))

	err := sqlDB.Ping()
	if err != nil {
		return nil, fmt.Errorf("error on ping oracle database connection:%v", err)
	}
	return &OracleDB{db: sqlDB}, nil
}

func (o *OracleDB) Query(querySQL string, args ...interface{}) ([]map[string]string, error) {
	var (
		cols []string
		res  []map[string]string
	)
	rows, err := o.db.Query(querySQL, args...)
	if err != nil {
		return nil, fmt.Errorf("[%v] error on general query SQL [%v] failed", err.Error(), querySQL)
	}
	defer rows.Close()

	//不确定字段通用查询，自动获取字段名称
	cols, err = rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("[%v] error on general query rows.Columns failed", err.Error())
	}

	values := make([][]byte, len(cols))
	scans := make([]interface{}, len(cols))
	for i := range values {
		scans[i] = &values[i]
	}
	for rows.Next() {
		err = rows.Scan(scans...)
		if err != nil {
			return nil, fmt.Errorf("[%v] error on general query rows.Scan failed", err.Error())
		}
		row := make(map[string]string)
		for k, v := range values {
			key := cols[k]
			if v == nil {
				row[key] = ""
			} else {
				row[key] = string(v)
			}
		}
		res = append(res, row)
	}
	return res, nil
}

func (o *OracleDB) GetCurrentSnapshotSCN() (int64, error) {
	// 获取当前 SCN 号
	res, err := o.Query("select min(current_scn) CURRENT_SCN from gv$database")
	var globalSCN int64
	if err != nil {
		return globalSCN, err
	}
	globalSCN, err = strconv.ParseInt(res[0]["CURRENT_SCN"], 10, 64)
	if err != nil {
		return globalSCN, err
	}
	return globalSCN, nil
}

type LogFile struct {
	Name        string
	FirstChange int64
}

func (o *OracleDB) GetLogFileBySCN(scn int64) ([]*LogFile, error) {
	query := fmt.Sprintf(`
SELECT
    MIN(name) name,
    first_change#
FROM
    (
        SELECT
            MIN(member) AS name,
            first_change#,
            281474976710655 AS next_change#
        FROM
            v$log       l
            INNER JOIN v$logfile   f ON l.group# = f.group#
        WHERE l.STATUS = 'CURRENT' OR l.STATUS = 'ACTIVE'
        GROUP BY
            first_change#
        UNION
        SELECT
            name,
            first_change#,
            next_change#
        FROM
            v$archived_log
        WHERE
            name IS NOT NULL
    )
WHERE
    first_change# >= %d
    OR %d < next_change#
GROUP BY
    first_change#
ORDER BY
    first_change#
`, scn, scn)

	rows, err := o.db.QueryContext(context.TODO(), query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var fs []*LogFile
	for rows.Next() {
		f := &LogFile{}
		err = rows.Scan(&f.Name, &f.FirstChange)
		if err != nil {
			return nil, err
		}
		fs = append(fs, f)
	}
	return fs, nil
}

func (o *OracleDB) AddLogMinerFile(fs []*LogFile) error {
	for i, f := range fs {
		var query string
		if i == 0 {
			query = fmt.Sprintf(`BEGIN
DBMS_LOGMNR.add_logfile ( '%s', DBMS_LOGMNR.new );
END;`, f.Name)
		} else {
			query = fmt.Sprintf(`BEGIN
DBMS_LOGMNR.add_logfile ( '%s' );
END;`, f.Name)
		}
		_, err := o.db.ExecContext(context.TODO(), query)
		if err != nil {
			return err
		}
	}
	return nil
}

func (o *OracleDB) BuildLogMiner() error {
	query := `BEGIN 
DBMS_LOGMNR_D.build (options => DBMS_LOGMNR_D.STORE_IN_REDO_LOGS);
END;`
	_, err := o.db.ExecContext(context.TODO(), query)
	return err
}

func (o *OracleDB) StartLogMinerBySCN2(startScn, endScn int64) error {
	query := fmt.Sprintf(`
BEGIN
DBMS_LOGMNR.start_logmnr (
startSCN => %d,
endScn => %d,
options => SYS.DBMS_LOGMNR.skip_corruption +
SYS.DBMS_LOGMNR.no_sql_delimiter +
SYS.DBMS_LOGMNR.NO_ROWID_IN_STMT +
SYS.DBMS_LOGMNR.DICT_FROM_REDO_LOGS +
SYS.DBMS_LOGMNR.DDL_DICT_TRACKING 
);
END;`, startScn, endScn)
	_, err := o.db.ExecContext(context.TODO(), query)
	return err
}

func (o *OracleDB) StartLogMinerBySCN(scn int64) error {
	query := fmt.Sprintf(`
BEGIN
DBMS_LOGMNR.start_logmnr (
startSCN => %d,
options => SYS.DBMS_LOGMNR.skip_corruption +
SYS.DBMS_LOGMNR.no_sql_delimiter +
SYS.DBMS_LOGMNR.no_rowid_in_stmt +
SYS.DBMS_LOGMNR.dict_from_online_catalog +
SYS.DBMS_LOGMNR.string_literals_in_stmt 
);
END;`, scn)
	_, err := o.db.ExecContext(context.TODO(), query)
	return err
}

func (o *OracleDB) EndLogMiner() error {
	query := `
BEGIN
DBMS_LOGMNR.end_logmnr ();
END;`
	_, err := o.db.ExecContext(context.TODO(), query)
	return err
}

type LogMinerRecord struct {
	SCN       int64
	SegOwner  string
	TableName string
	SQLRedo   string
	Operation string
}

func (r *LogMinerRecord) String() string {
	return fmt.Sprintf(`
scn: %d, seg_owner: %s, table_name: %s, op: %s
sql: %s`, r.SCN, r.SegOwner, r.TableName, r.Operation, r.SQLRedo,
	)
}

func (o *OracleDB) GetLogMinerRecord(schemaName string, sourceTableNames []string, startScn, endScn int64) ([]*LogMinerRecord, error) {
	// AND table_name IN ('%s')
	// strings.Join(sourceTableNames, `','`),
	query := fmt.Sprintf(`
SELECT 
	scn,
	seg_owner,
	table_name,
	sql_redo,
	operation
FROM 
	V$LOGMNR_CONTENTS
WHERE 
	seg_owner = '%s'
	AND SCN > %d
    AND SCN <= %d
`, schemaName, startScn, endScn)

	rows, err := o.db.QueryContext(context.TODO(), query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var lrs []*LogMinerRecord
	for rows.Next() {
		lr := &LogMinerRecord{}
		err = rows.Scan(&lr.SCN, &lr.SegOwner, &lr.TableName, &lr.SQLRedo, &lr.Operation)
		if err != nil {
			return nil, err
		}
		lrs = append(lrs, lr)
	}
	return lrs, nil
}

func (o *OracleDB) getTables(schema string) ([]string, error) {
	query := fmt.Sprintf(`
SELECT 
	table_name
FROM 
	all_tables 
WHERE 
	owner = '%s'`, schema)

	rows, err := o.db.QueryContext(context.TODO(), query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var table string
		err = rows.Scan(&table)
		if err != nil {
			return nil, err
		}
		tables = append(tables, table)
	}
	return tables, nil
}

type ColumnDefinition struct {
	Name          string
	Datatype      string
	DataLength    uint32
	DataPrecision sql.NullInt32
	DataScale     sql.NullInt32
	DefaultLength sql.NullInt32
	CharLength    sql.NullInt32
	Nullable      OracleBoolean
	DataDefault   sql.NullString
}

func (c *ColumnDefinition) String() string {
	return fmt.Sprintf("name: %s, data_type: %s, data_length: %d, char_length: %d, p: %d, s: %d\n",
		c.Name, c.Datatype, c.DataLength, c.CharLength.Int32, c.DataPrecision.Int32, c.DataScale.Int32)
}

type OracleBoolean bool

func (o *OracleBoolean) Scan(value interface{}) error {
	ns := &sql.NullString{}
	err := ns.Scan(value)
	if err != nil {
		return err
	}
	if ns.String == "Y" {
		*o = true
	} else {
		*o = false
	}
	return nil
}

func (o *OracleDB) getTableColumn(schema, table string) ([]*ColumnDefinition, error) {
	query := fmt.Sprintf(`
SELECT 
	column_name,
	data_type,
	data_length,
	data_precision,
	data_scale,
	default_length,
	CHAR_LENGTH,
	nullable,
	data_default
FROM 
	ALL_TAB_COLUMNS 
WHERE 
	owner = '%s' AND table_name='%s'`, schema, table)

	rows, err := o.db.QueryContext(context.TODO(), query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []*ColumnDefinition
	for rows.Next() {
		column := &ColumnDefinition{}
		err = rows.Scan(
			&column.Name,
			&column.Datatype,
			&column.DataLength,
			&column.DataPrecision,
			&column.DataScale,
			&column.DefaultLength,
			&column.CharLength,
			&column.Nullable,
			&column.DataDefault,
		)
		if err != nil {
			return nil, err
		}
		columns = append(columns, column)
	}
	return columns, nil
}

func (o *OracleDB) currentRedoLogSequenceFp() (string, error) {
	query := fmt.Sprintf(`SELECT GROUP#, THREAD#, SEQUENCE# FROM V$LOG WHERE STATUS = 'CURRENT'`)
	rows, err := o.Query(query)
	if err != nil {
		return "", err
	}
	buf := bytes.Buffer{}
	for _, row := range rows {
		buf.WriteString(fmt.Sprintf("group:%s,thread:%s,sequence:%s",
			row["GROUP#"], row["THREAD#"], row["SEQUENCE#"]))
		buf.WriteString(";")
	}
	return buf.String(), nil
}

var db *OracleDB

func (e *ExtractorOracle) DataStreamEvents(entriesChannel chan<- *common.BinlogEntryContext) error {
	e.logger.Debug("start oracle. DataStreamEvents")
	if db == nil {
		oracleDb, err := NewDB(&OracleDBMeta{
			User:        "roma_logminer",
			Password:    "oracle",
			Host:        "10.186.63.15",
			Port:        1521,
			ServiceName: "XE",
		})
		if err != nil {
			fmt.Println(err)
			return err
		}
		db = oracleDb
	}

	schema := "TEST"
	tables, err := db.getTables(schema)
	if err != nil {
		fmt.Println(err)
		return err
	}
	for _, table := range tables {
		fmt.Println("===============================")
		fmt.Printf("get table \"%s\" column\n", table)
		fmt.Println("===============================")
		columns, err := db.getTableColumn(schema, table)
		if err != nil {
			fmt.Println(err)
			return err
		}
		fmt.Println(columns)
	}

	scn, err := db.GetCurrentSnapshotSCN()
	if err != nil {
		fmt.Println(err)
		return err
	}
	e.logger.Debug("current scn", "scn", scn)

	ls := NewLogMinerStream(db, scn, 100000)
	err = ls.start()
	if err != nil {
		fmt.Println(err)
		return err
	}
	defer ls.stop()

	for {
		time.Sleep(5 * time.Second)
		rs, err := ls.queue()
		if err != nil {
			return err
		}
		for _, r := range rs {
			e.logger.Debug("r SQL", "REDOSQL", r.SQLRedo)
		}

		Entry := handleSQLs(rs)
		e.logger.Debug("handle SQLs", "Entry", Entry)
		entriesChannel <- &common.BinlogEntryContext{
			Entry:        Entry,
			TableItems:   nil,
			OriginalSize: 0,
		}
	}
}

func handleSQLs(rows []*LogMinerRecord) *common.BinlogEntry {
	entry := common.NewBinlogEntry()
	entry.Coordinates = common.BinlogCoordinateTx{
		LogFile: "",
		LogPos:  0,
		//SID:           [16]byte(0),
		GNO:           0,
		LastCommitted: 0,
		SeqenceNumber: 0,
	}
	entry.Index = 0
	entry.Final = true
	for _, row := range rows {
		dataEvent, _ := parseToDataEvent(row)
		entry.Events = append(entry.Events, dataEvent)
	}
	return entry
}

type LogMinerStream struct {
	db                       *OracleDB
	currentScn               int64
	interval                 int64
	initialized              bool
	currentRedoLogSequenceFP string
}

func NewLogMinerStream(db *OracleDB, startScn, interval int64) *LogMinerStream {
	return &LogMinerStream{
		db:         db,
		currentScn: startScn,
		interval:   interval,
	}
}

func (l *LogMinerStream) checkRedoLogChanged() (bool, error) {
	fp, err := l.db.currentRedoLogSequenceFp()
	if err != nil {
		return false, err
	}
	fmt.Printf("cur fp: %s\nlatest fp: %s\n", l.currentRedoLogSequenceFP, fp)
	if l.currentRedoLogSequenceFP == fp {
		return false, nil
	}
	return true, nil
}

func (l *LogMinerStream) start() error {
	fmt.Println("build logminer")
	err := l.db.BuildLogMiner()
	if err != nil {
		fmt.Println("BuildLogMiner ", err)
		return err
	}

	fs, err := l.db.GetLogFileBySCN(l.currentScn)
	if err != nil {
		fmt.Println(err)
		return err
	}
	for _, f := range fs {
		fmt.Println(f.Name)
	}

	fmt.Println("add logminer file")
	err = l.db.AddLogMinerFile(fs)
	if err != nil {
		fmt.Println("AddLogMinerFile ", err)
		return err
	}
	fp, err := l.db.currentRedoLogSequenceFp()
	if err != nil {
		return err
	}
	l.currentRedoLogSequenceFP = fp
	return nil
}

func (l *LogMinerStream) stop() error {
	return l.db.EndLogMiner()
}

func (l *LogMinerStream) getEndScn() (int64, error) {
	latestScn, err := l.db.GetCurrentSnapshotSCN()
	if err != nil {
		fmt.Println(err)
		return 0, err
	}
	endScn := l.currentScn + l.interval
	if endScn < latestScn {
		return endScn, nil
	} else {
		return latestScn, nil
	}
}

func (l *LogMinerStream) queue() ([]*LogMinerRecord, error) {
	changed, err := l.checkRedoLogChanged()
	if err != nil {
		return nil, err
	}
	if changed {
		err := l.stop()
		if err != nil {
			return nil, err
		}
		err = l.start()
		if err != nil {
			return nil, err
		}
	}
	endScn, err := l.getEndScn()
	if err != nil {
		return nil, err
	}
	if endScn == l.currentScn {
		return nil, err
	}
	fmt.Println("start logminer")
	err = l.db.StartLogMinerBySCN2(l.currentScn, endScn)
	if err != nil {
		fmt.Println("StartLMBySCN ", err)
		return nil, err
	}

	fmt.Printf("get logminer recore form scn %d to %d\n", l.currentScn, endScn)
	records, err := l.db.GetLogMinerRecord("TEST", []string{"t1"}, l.currentScn, endScn)
	if err != nil {
		fmt.Println("GetLogMinerRecord ", err)
		return nil, err
	}
	l.currentScn = endScn
	return records, nil
}

func parseToDataEvent(row *LogMinerRecord) (common.DataEvent, error) {
	dataEvent := common.DataEvent{}
	row.SQLRedo = ReplaceQuotesString(row.SQLRedo)
	row.SQLRedo = ReplaceSpecifiedString(row.SQLRedo, ";", "")

	p := parser.New()
	stmt, warns, err := p.Parse(row.SQLRedo, "", "")
	if err != nil {
		return dataEvent, err
	}
	visitor := &Stmt{}
	(stmt[0]).Accept(visitor)
	fmt.Println(stmt, warns, err)

	dataEvent = common.DataEvent{
		Query:         "",
		CurrentSchema: visitor.Schema,
		DatabaseName:  visitor.Schema,
		TableName:     visitor.Table,
		DML:           visitor.Operation,
		ColumnCount:   uint64(len(visitor.Columns)),
		//WhereColumnValues: visitor.WhereColumnValues,
		NewColumnValues: visitor.NewColumnValues,
		Table:           nil,
		LogPos:          0,
		Timestamp:       0,
	}
	return dataEvent, nil
}

// 替换字符串引号字符
func ReplaceQuotesString(s string) string {
	return string(exbytes.Replace([]byte(s), []byte("\""), []byte(""), -1))
}

// 替换指定字符
func ReplaceSpecifiedString(s string, oldStr, newStr string) string {
	return string(exbytes.Replace([]byte(s), []byte(oldStr), []byte(newStr), -1))
}

// 字符串拼接
func StringsBuilder(str ...string) string {
	var b strings.Builder
	for _, p := range str {
		b.WriteString(p)
	}
	return b.String() // no copying
}
