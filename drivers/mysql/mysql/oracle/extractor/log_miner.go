package extractor

import (
	"bytes"
	"context"
	"database/sql"
	gosql "database/sql"
	"fmt"
	"strconv"
	"time"

	"github.com/actiontech/dtle/g"

	"github.com/pingcap/parser"

	"github.com/actiontech/dtle/drivers/mysql/common"
	"github.com/thinkeridea/go-extend/exbytes"

	"strings"

	_ "github.com/pingcap/tidb/types/parser_driver"
)

func (l *LogMinerStream) GetCurrentSnapshotSCN() (int64, error) {
	// 获取当前 SCN 号
	res, err := Query(l.db, "select min(current_scn) CURRENT_SCN from gv$database")
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

func (l *LogMinerStream) GetLogFileBySCN(scn int64) ([]*LogFile, error) {
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

	rows, err := l.db.QueryContext(context.TODO(), query)
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

func (l *LogMinerStream) AddLogMinerFile(fs []*LogFile) error {
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
		_, err := l.db.ExecContext(context.TODO(), query)
		if err != nil {
			return err
		}
	}
	return nil
}

func (l *LogMinerStream) BuildLogMiner() error {
	query := `BEGIN 
DBMS_LOGMNR_D.build (options => DBMS_LOGMNR_D.STORE_IN_REDO_LOGS);
END;`
	_, err := l.db.ExecContext(context.TODO(), query)
	return err
}

func (l *LogMinerStream) StartLogMinerBySCN2(startScn, endScn int64) error {
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
	l.logger.Debug("startLogMiner2", "query", query)
	_, err := l.db.ExecContext(context.TODO(), query)
	return err
}

func (l *LogMinerStream) StartLogMinerBySCN(scn int64) error {
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
	_, err := l.db.ExecContext(context.TODO(), query)
	return err
}

func (l *LogMinerStream) EndLogMiner() error {
	query := `
BEGIN
DBMS_LOGMNR.end_logmnr ();
END;`
	_, err := l.db.ExecContext(context.TODO(), query)
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
func (l *LogMinerStream) buildFilterSchemaTable() (filterSchemaTable string) {
	// demo :
	// AND(
	//    ( seg_owner = 'TEST1' AND table_name in ('t1','t2'))
	//OR  ( seg_owner = 'TEST2' AND table_name in ('t3','t4'))
	//OR  ( seg_owner = 'test')
	//    )

	for _, dataSource := range l.dataSources {
		if len(dataSource.Tables) == 0 {
			filterSchemaTable = fmt.Sprintf(`%s OR ( seg_owner = '%s')`, filterSchemaTable, dataSource.TableSchema)
		} else {
			tables := make([]string, 0)
			for _, table := range dataSource.Tables {
				tables = append(tables, fmt.Sprintf("'%s'", table.TableName))
			}
			filterSchemaTable = fmt.Sprintf(`%s OR ( seg_owner = '%s' AND table_name in (%s))`, filterSchemaTable, dataSource.TableSchema, strings.Join(tables, ","))
		}
	}

	if len(filterSchemaTable) > 0 {
		filterSchemaTable = strings.Replace(filterSchemaTable, "OR", "AND(", 1)
		filterSchemaTable = fmt.Sprintf("%s%s", filterSchemaTable, ")")
	}
	return
}

func (l *LogMinerStream) GetLogMinerRecord(startScn, endScn int64) ([]*LogMinerRecord, error) {
	l.logger.Debug("Get logMiner record", "QuerySql", "")
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
	SCN > %d
    AND SCN <= %d
    %s
`, startScn, endScn, l.buildFilterSchemaTable())
	l.logger.Debug("Get logMiner record", "QuerySql", query)
	rows, err := l.db.QueryContext(context.TODO(), query)
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

func (o *ExtractorOracle) getTables(schema string) ([]string, error) {
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

func (o *ExtractorOracle) getTableColumn(schema, table string) ([]*ColumnDefinition, error) {
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

func (l *LogMinerStream) currentRedoLogSequenceFp() (string, error) {
	query := fmt.Sprintf(`SELECT GROUP#, THREAD#, SEQUENCE# FROM V$LOG WHERE STATUS = 'CURRENT'`)
	rows, err := Query(l.db, query)
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

func (e *ExtractorOracle) DataStreamEvents(entriesChannel chan<- *common.BinlogEntryContext) error {
	e.logger.Debug("start oracle. DataStreamEvents")
	//schema := "TEST"
	//tables, err := e.getTables(schema)
	//if err != nil {
	//	fmt.Println(err)
	//	return err
	//}
	//for _, table := range tables {
	//	e.logger.Debug("get table \"%s\" column\n", "table", table)
	//	columns, err := e.getTableColumn(schema, table)
	//	if err != nil {
	//		fmt.Println(err)
	//		return err
	//	}
	//	fmt.Println(columns)
	//}

	if e.mysqlContext.OracleConfig.SCN == 0 {
		scn, err := e.LogMinerStream.GetCurrentSnapshotSCN()
		if err != nil {
			fmt.Println(err)
			return err
		}
		e.logger.Debug("current scn", "scn", scn)
		e.LogMinerStream.currentScn = scn
	}

	err := e.LogMinerStream.start()
	if err != nil {
		fmt.Println(err)
		return err
	}
	defer e.LogMinerStream.stop()

	for {
		time.Sleep(5 * time.Second)
		rs, err := e.LogMinerStream.queue()
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
	for _, row := range rows {
		dataEvent, _ := parseToDataEvent(row)
		entry.Events = append(entry.Events, dataEvent)
	}
	return entry
}

type LogMinerStream struct {
	db                       *gosql.DB
	currentScn               int64
	interval                 int64
	initialized              bool
	currentRedoLogSequenceFP string
	logger                   g.LoggerType
	dataSources              []*common.DataSource
}

func NewLogMinerStream(db *gosql.DB, logger g.LoggerType, dataSource []*common.DataSource,
	startScn, interval int64) *LogMinerStream {
	return &LogMinerStream{
		db:          db,
		logger:      logger,
		dataSources: dataSource,
		currentScn:  startScn,
		interval:    interval,
	}
}

func (l *LogMinerStream) checkRedoLogChanged() (bool, error) {
	fp, err := l.currentRedoLogSequenceFp()
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
	err := l.BuildLogMiner()
	if err != nil {
		fmt.Println("BuildLogMiner ", err)
		return err
	}

	fs, err := l.GetLogFileBySCN(l.currentScn)
	if err != nil {
		fmt.Println(err)
		return err
	}
	for _, f := range fs {
		fmt.Println(f.Name)
	}

	fmt.Println("add logminer file")
	err = l.AddLogMinerFile(fs)
	if err != nil {
		fmt.Println("AddLogMinerFile ", err)
		return err
	}
	fp, err := l.currentRedoLogSequenceFp()
	if err != nil {
		return err
	}
	l.currentRedoLogSequenceFP = fp
	return nil
}

func (l *LogMinerStream) stop() error {
	return l.EndLogMiner()
}

func (l *LogMinerStream) getEndScn() (int64, error) {
	latestScn, err := l.GetCurrentSnapshotSCN()
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
	l.logger.Info("Start logminer")
	err = l.StartLogMinerBySCN2(l.currentScn, endScn)
	if err != nil {
		l.logger.Error("StartLMBySCN ", "err", err)
		return nil, err
	}

	l.logger.Info("Get log miner record form", "StartScn", l.currentScn, "EndScn", endScn)
	records, err := l.GetLogMinerRecord(l.currentScn, endScn)
	if err != nil {
		l.logger.Error("GetLogMinerRecord ", "err", err)
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
	if len(stmt) <= 0 {
		return dataEvent, nil
	}
	(stmt[0]).Accept(visitor)
	fmt.Println(stmt, warns, err)

	dataEvent = common.DataEvent{
		Query:             "",
		CurrentSchema:     visitor.Schema,
		DatabaseName:      visitor.Schema,
		TableName:         visitor.Table,
		DML:               visitor.Operation,
		ColumnCount:       uint64(len(visitor.Columns)),
		WhereColumnValues: visitor.WhereColumnValues,
		NewColumnValues:   visitor.NewColumnValues,
		Table:             nil,
		LogPos:            0,
		Timestamp:         0,
	}
	return dataEvent, nil
}

func Query(db *gosql.DB, querySQL string, args ...interface{}) ([]map[string]string, error) {
	var (
		cols []string
		res  []map[string]string
	)
	rows, err := db.Query(querySQL, args...)
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
