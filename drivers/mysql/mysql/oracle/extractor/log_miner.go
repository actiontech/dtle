package extractor

import (
	"container/list"
	"context"
	"crypto/md5"
	"database/sql"
	gosql "database/sql"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/hashicorp/go-hclog"

	oracleParser "github.com/sjjian/oracle-sql-parser"
	"github.com/sjjian/oracle-sql-parser/ast"

	"github.com/actiontech/dtle/drivers/mysql/mysql/oracle/config"

	"github.com/actiontech/dtle/g"

	"github.com/pingcap/parser"

	"github.com/actiontech/dtle/drivers/mysql/common"
	"github.com/thinkeridea/go-extend/exbytes"

	"strings"

	_ "github.com/pingcap/tidb/types/parser_driver"
)

func (l *LogMinerStream) GetCurrentSnapshotSCN() (int64, error) {
	var globalSCN int64
	// 获取当前 SCN 号
	err := l.oracleDB.LogMinerConn.QueryRowContext(context.TODO(), "SELECT CURRENT_SCN FROM V$DATABASE").Scan(&globalSCN)
	if err != nil {
		return 0, err
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

	rows, err := l.oracleDB.LogMinerConn.QueryContext(context.TODO(), query)
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
		l.logger.Debug("logFileName", "name", f.Name)
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
		_, err := l.oracleDB.LogMinerConn.ExecContext(context.TODO(), query)
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
	_, err := l.oracleDB.LogMinerConn.ExecContext(context.TODO(), query)
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
	_, err := l.oracleDB.LogMinerConn.ExecContext(context.TODO(), query)
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
	_, err := l.oracleDB.LogMinerConn.ExecContext(context.TODO(), query)
	return err
}

func (l *LogMinerStream) EndLogMiner() error {
	query := `
BEGIN
DBMS_LOGMNR.end_logmnr ();
END;`
	_, err := l.oracleDB.LogMinerConn.ExecContext(context.TODO(), query)
	return err
}

type LogMinerRecord struct {
	SCN       int64
	SegOwner  string
	TableName string
	SQLRedo   string
	SQLUndo   string
	Operation int
	XId       []byte
	Csf       int
	RowId     string
	Rollback  int
	RsId      string
	StartTime string
	Username  string
}

func (r *LogMinerRecord) TxId() string {
	h := md5.New()
	h.Write(r.XId)
	return hex.EncodeToString(h.Sum(nil))
}

func (r *LogMinerRecord) String() string {
	return fmt.Sprintf(`
scn: %d, seg_owner: %s, table_name: %s, op: %d, xid: %s, rb: %d
row_id: %s, username: %s,
sql: %s`, r.SCN, r.SegOwner, r.TableName, r.Operation, r.TxId(), r.Rollback,
		r.RowId, r.Username,
		r.SQLRedo,
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
	sql_undo,
	operation_code,
	xid,
	csf,
	row_id,
	rollback,
	rs_id,
	timestamp,
	username
FROM 
	V$LOGMNR_CONTENTS
WHERE
	SCN > %d
    AND SCN <= %d
	AND (
		(operation_code IN (6,7,34,36))
		OR 
		(operation_code IN (1,2,3,5) AND seg_owner not in ('SYS','SYSTEM','APPQOSSYS','AUDSYS','CTXSYS','DVSYS','DBSFWUSER',
		'DBSNMP','GSMADMIN_INTERNAL','LBACSYS','MDSYS','OJVMSYS','OLAPSYS','ORDDATA',
    	'ORDSYS','OUTLN','WMSYS','XDB') %s
		)
	)
`, startScn, endScn, l.buildFilterSchemaTable())

	//l.logger.Debug("Get logMiner record", "QuerySql", query)
	rows, err := l.oracleDB.LogMinerConn.QueryContext(context.TODO(), query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	scan := func(rows *sql.Rows) (*LogMinerRecord, error) {
		lr := &LogMinerRecord{}
		err = rows.Scan(&lr.SCN, &lr.SegOwner, &lr.TableName, &lr.SQLRedo, &lr.SQLUndo, &lr.Operation, &lr.XId,
			&lr.Csf, &lr.RowId, &lr.Rollback, &lr.RsId, &lr.StartTime, &lr.Username)
		if err != nil {
			return nil, err
		}
		return lr, nil
	}
	var lrs []*LogMinerRecord
	for rows.Next() {
		lr, err := scan(rows)
		if err != nil {
			return nil, err
		}
		// 1 = indicates that either SQL_REDO or SQL_UNDO is greater than 4000 bytes in size
		// and is continued in the next row returned by the view
		if lr.Csf == 1 {
			redoLog := strings.Builder{}
			undoLog := strings.Builder{}
			redoLog.WriteString(lr.SQLRedo)
			undoLog.WriteString(lr.SQLUndo)
			for rows.Next() {
				lr2, err := scan(rows)
				if err != nil {
					return nil, err
				}
				redoLog.WriteString(lr2.SQLRedo)
				undoLog.WriteString(lr2.SQLUndo)
				if lr2.Csf != 1 {
					break
				}
			}
			lr.SQLRedo = redoLog.String()
			lr.SQLUndo = undoLog.String()
		}

		//l.logger.Debug("Get logMiner record", "RedoSQL", lr.SQLRedo, "UndoSQL", lr.SQLUndo, "opretation", lr.Operation)

		lrs = append(lrs, lr)
	}
	return lrs, nil
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

//func (l *LogMinerStream) currentRedoLogSequenceFp() (string, error) {
//	query := fmt.Sprintf(`SELECT GROUP#, THREAD#, SEQUENCE# FROM V$LOG WHERE STATUS = 'CURRENT'`)
//	rows, err := Query(l.oracleDB.LogMinerConn, query)
//	if err != nil {
//		return "", err
//	}
//	buf := bytes.Buffer{}
//	for _, row := range rows {
//		buf.WriteString(fmt.Sprintf("group:%s,thread:%s,sequence:%s",
//			row["GROUP#"], row["THREAD#"], row["SEQUENCE#"]))
//		buf.WriteString(";")
//	}
//	return buf.String(), nil
//}

type LogMinerTx struct {
	transactionId string
	startScn      int64
	endScn        int64
	records       []*LogMinerRecord
}

func (l *LogMinerTx) String() string {
	if len(l.records) == 0 {
		return ""
	}
	d := strings.Builder{}
	d.WriteString("================\n")
	for _, r := range l.records {
		d.WriteString(r.String())
		d.WriteString("\n")
	}
	d.WriteString("================\n")
	return fmt.Sprintf(`transaction id: %s, start scn: %d, end scn: %d
%s`, l.transactionId, l.startScn, l.endScn, d.String())
}

type LogMinerTxCache struct {
	index   map[string] /*transaction id */ *list.Element
	cache   *list.List
	Handler func(tx *LogMinerTx) error
}

func NewLogMinerTxCache() *LogMinerTxCache {
	return &LogMinerTxCache{
		index: map[string]*list.Element{},
		cache: list.New(),
		Handler: func(tx *LogMinerTx) error {
			return nil
		}, // default empty handler
	}
}

func (lc *LogMinerTxCache) getFirstActiveTx() *LogMinerTx {
	el := lc.cache.Front()
	if el == nil {
		return nil
	}
	return el.Value.(*LogMinerTx)
}

func (lc *LogMinerTxCache) startTx(txId string, startScn int64) {
	if _, ok := lc.index[txId]; !ok {
		tx := &LogMinerTx{
			transactionId: txId,
			startScn:      startScn,
			records:       []*LogMinerRecord{},
		}
		el := lc.cache.PushBack(tx)
		lc.index[txId] = el
	}
}

func (lc *LogMinerTxCache) commitTx(txId string, endScn int64) {
	if el, ok := lc.index[txId]; ok {
		tx := el.Value.(*LogMinerTx)
		tx.endScn = endScn
		if len(tx.records) != 0 {
			lc.Handler(tx)
			//l.ProcessedScn = t.endScn // TODO: support restart, no data lose or no data repeat exec.
		} else {
			fmt.Printf("empty transaction %s, start scn: %d, end scn: %d\n",
				tx.transactionId, tx.startScn, tx.endScn)
		}
		delete(lc.index, txId)
		lc.cache.Remove(el)
	}
}

func (lc *LogMinerTxCache) rollbackTx(txId string, endScn int64) {
	if el, ok := lc.index[txId]; ok {
		tx := el.Value.(*LogMinerTx)
		tx.endScn = endScn
		delete(lc.index, txId)
		lc.cache.Remove(el)
	}
}

func (lc *LogMinerTxCache) addTxRecord(newRecord *LogMinerRecord) {
	if el, ok := lc.index[newRecord.TxId()]; ok {
		tx := el.Value.(*LogMinerTx)
		// 1 = if the redo record was generated because of a partial or a full rollback of the associated transaction
		if newRecord.Rollback == 1 {
			for i, r := range tx.records {
				if r.RowId == newRecord.RowId {
					// delete record
					tx.records = append(tx.records[:i], tx.records[i+1:]...)
				}
			}
		} else {
			tx.records = append(tx.records, newRecord)
		}
	}
}

func (e *ExtractorOracle) DataStreamEvents(entriesChannel chan<- *common.BinlogEntryContext) error {
	e.logger.Debug("start oracle. DataStreamEvents")

	if e.mysqlContext.OracleConfig.SCN == 0 {
		scn, err := e.LogMinerStream.GetCurrentSnapshotSCN()
		if err != nil {
			e.logger.Error("GetCurrentSnapshotSCN", "err", err)
			return err
		}
		e.logger.Debug("current scn", "scn", scn)
		e.LogMinerStream.CollectedScn = scn
	}

	e.LogMinerStream.txCache.Handler = func(tx *LogMinerTx) error {
		Entry := e.handleSQLs(tx)
		e.logger.Debug("handle SQLs", "Entry", Entry)
		entriesChannel <- &common.BinlogEntryContext{
			Entry:        Entry,
			TableItems:   nil,
			OriginalSize: 0,
		}
		return nil
	}

	err := e.LogMinerStream.Loop()
	if err != nil {
		e.logger.Error("LogMinerStreamLoop", "err", err)
		return err
	}
	return nil

	//err := e.LogMinerStream.start()
	//if err != nil {
	//	e.logger.Error("StartLogMiner", "err", err)
	//	return err
	//}
	//defer e.LogMinerStream.stop()
	//
	//for {
	//	time.Sleep(5 * time.Second)
	//	rs, err := e.LogMinerStream.queue()
	//	if err != nil {
	//		return err
	//	}
	//
	//	Entry := e.handleSQLs(rs)
	//	e.logger.Debug("handle SQLs", "Entry", Entry)
	//	entriesChannel <- &common.BinlogEntryContext{
	//		Entry:        Entry,
	//		TableItems:   nil,
	//		OriginalSize: 0,
	//	}
	//}
}

func (e *ExtractorOracle) handleSQLs(tx *LogMinerTx) *common.BinlogEntry {
	entry := common.NewBinlogEntry()
	for _, row := range tx.records {
		dataEvent, err := e.parseToDataEvent(row)
		if err != nil {
			e.logger.Error("parseOracleToMySQL", "err", err)
		}
		entry.Events = append(entry.Events, dataEvent)
	}
	return entry
}

type LogMinerStream struct {
	oracleDB                 *config.OracleDB
	CollectedScn             int64
	ProcessedScn             int64
	interval                 int64
	initialized              bool
	currentRedoLogSequenceFP string
	logger                   g.LoggerType
	txCache                  *LogMinerTxCache
	dataSources              []*common.DataSource
}

func NewLogMinerStream(db *config.OracleDB, logger g.LoggerType, dataSource []*common.DataSource,
	startScn, interval int64) *LogMinerStream {
	return &LogMinerStream{
		oracleDB:     db,
		logger:       logger,
		dataSources:  dataSource,
		CollectedScn: startScn,
		interval:     interval,
		txCache:      NewLogMinerTxCache(),
	}
}

func (l *LogMinerStream) checkRedoLogChanged() (bool, error) {
	fp, err := l.oracleDB.CurrentRedoLogSequenceFp()
	if err != nil {
		return false, err
	}
	l.logger.Info("getRedoLogFp:", "currentRedoLogSequenceFP", l.currentRedoLogSequenceFP, "lastFp", fp)
	if l.currentRedoLogSequenceFP == fp {
		return false, nil
	}
	return true, nil
}

func (l *LogMinerStream) initLogMiner() error {
	l.logger.Debug("build logminer")
	err := l.BuildLogMiner()
	if err != nil {
		l.logger.Error("BuildLogMiner ", "err", err)
		return err
	}

	fs, err := l.GetLogFileBySCN(l.CollectedScn)
	if err != nil {
		l.logger.Error("GetLogFileBySCN", "err", err)
		return err
	}

	l.logger.Debug("add logminer file")
	err = l.AddLogMinerFile(fs)
	if err != nil {
		fmt.Println("AddLogMinerFile ", err)
		return err
	}
	fp, err := l.oracleDB.CurrentRedoLogSequenceFp()
	if err != nil {
		l.logger.Error("currentRedoLogSequenceFp ", "err", err)
		return err
	}
	l.currentRedoLogSequenceFP = fp
	return nil
}

func (l *LogMinerStream) stopLogMiner() error {
	return l.EndLogMiner()
}

//func (l *LogMinerStream) start() error {
//	l.logger.Debug("build logminer")
//	err := l.BuildLogMiner()
//	if err != nil {
//		l.logger.Error("BuildLogMiner ", "err", err)
//		return err
//	}
//
//	fs, err := l.GetLogFileBySCN(l.currentScn)
//	if err != nil {
//		l.logger.Error("GetLogFileBySCN", "err", err)
//		return err
//	}
//
//	l.logger.Debug("add logminer file")
//	err = l.AddLogMinerFile(fs)
//	if err != nil {
//		l.logger.Error("AddLogMinerFile ", "err", err)
//		return err
//	}
//	fp, err := l.oracleDB.CurrentRedoLogSequenceFp()
//	if err != nil {
//		l.logger.Error("currentRedoLogSequenceFp ", "err", err)
//		return err
//	}
//	l.currentRedoLogSequenceFP = fp
//	return nil
//}

//func (l *LogMinerStream) stop() error {
//	return l.EndLogMiner()
//}

func (l *LogMinerStream) getEndScn() (int64, error) {
	latestScn, err := l.GetCurrentSnapshotSCN()
	if err != nil {
		l.logger.Error("GetCurrentSnapshotSCN", "err", err)
		return 0, err
	}
	endScn := l.CollectedScn + l.interval
	if endScn < latestScn {
		return endScn, nil
	} else {
		return latestScn, nil
	}
}

func (l *LogMinerStream) Loop() error {
	err := l.initLogMiner()
	if err != nil {
		fmt.Println(err)
		return err
	}
	defer l.stopLogMiner()

	for {
		time.Sleep(5 * time.Second)
		changed, err := l.checkRedoLogChanged()
		if err != nil {
			return err
		}
		if changed {
			err := l.stopLogMiner()
			if err != nil {
				return err
			}
			err = l.initLogMiner()
			if err != nil {
				return err
			}
		}
		endScn, err := l.getEndScn()
		if err != nil {
			return err
		}
		if endScn == l.CollectedScn {
			continue
		}

		err = l.StartLogMinerBySCN2(l.CollectedScn, endScn)
		if err != nil {
			fmt.Println("StartLMBySCN ", err)
			return err
		}
		l.logger.Info("Get log miner record form", "StartScn", l.CollectedScn, "EndScn", endScn)
		records, err := l.GetLogMinerRecord(l.CollectedScn, endScn)
		if err != nil {
			l.logger.Error("GetLogMinerRecord ", "err", err)
			return err
		}
		err = l.HandlerRecords(records)
		if err != nil {
			return err
		}
	}
}

const (
	OperationCodeInsert   = 1
	OperationCodeDelete   = 2
	OperationCodeUpdate   = 3
	OperationCodeDDL      = 5
	OperationCodeStart    = 6
	OperationCodeCommit   = 7
	OperationCodeMissScn  = 34
	OperationCodeRollback = 36
)

func (l *LogMinerStream) HandlerRecords(records []*LogMinerRecord) error {
	for _, r := range records {
		switch r.Operation {
		case OperationCodeStart:
			l.txCache.startTx(r.TxId(), r.SCN)
		case OperationCodeCommit:
			l.txCache.commitTx(r.TxId(), r.SCN)
		case OperationCodeRollback:
			l.txCache.rollbackTx(r.TxId(), r.SCN)
		case OperationCodeDDL:
			l.txCache.Handler(&LogMinerTx{
				transactionId: "",
				startScn:      r.SCN,
				endScn:        r.SCN,
				records:       []*LogMinerRecord{r},
			})
		case OperationCodeInsert, OperationCodeDelete, OperationCodeUpdate:
			l.txCache.addTxRecord(r)
		}
		l.CollectedScn = r.SCN
	}
	return nil
}

//func (l *LogMinerStream) queue() ([]*LogMinerRecord, error) {
//	changed, err := l.checkRedoLogChanged()
//	if err != nil {
//		return nil, err
//	}
//	if changed {
//		err := l.stop()
//		if err != nil {
//			return nil, err
//		}
//		err = l.start()
//		if err != nil {
//			return nil, err
//		}
//	}
//	endScn, err := l.getEndScn()
//	if err != nil {
//		return nil, err
//	}
//	if endScn == l.currentScn {
//		return nil, err
//	}
//	l.logger.Info("Start logminer")
//	err = l.StartLogMinerBySCN2(l.currentScn, endScn)
//	if err != nil {
//		l.logger.Error("StartLMBySCN ", "err", err)
//		return nil, err
//	}
//
//	l.logger.Info("Get log miner record form", "StartScn", l.currentScn, "EndScn", endScn)
//	records, err := l.GetLogMinerRecord(l.currentScn, endScn)
//	if err != nil {
//		l.logger.Error("GetLogMinerRecord ", "err", err)
//		return nil, err
//	}
//	l.currentScn = endScn
//	return records, nil
//}

func (e *ExtractorOracle) parseToDataEvent(row *LogMinerRecord) (common.DataEvent, error) {
	e.logger.Debug("============= parse To DataEvent===============", "redoSQL", row.SQLRedo)
	dataEvent := common.DataEvent{}

	// parse ddl and dml
	if row.Operation == OperationCodeDDL {
		//dataEvent.Query = "CREATE TABLE IF NOT EXISTS `runoob_tbl`(\n   `runoob_id` INT UNSIGNED AUTO_INCREMENT,\n   `runoob_title` VARCHAR(100) NOT NULL,\n   `runoob_author` VARCHAR(40) NOT NULL,\n   `submission_date` DATE,\n   PRIMARY KEY ( `runoob_id` )\n)ENGINE=InnoDB DEFAULT CHARSET=utf8"
		e.logger.Debug("============= ddl stmt parse start===============", "redoSQL", row.SQLRedo)
		//tableStruct := e.OracleContext.schemas[visitor.Schema].Tables[visitor.Table].RelTable.TableStructs
		// 列排序问题
		// 字符集问题
		dataEvent, err := parseDDLSQL(e.logger, row.SQLRedo)
		if err != nil {
			return common.DataEvent{}, err
		}
		e.logger.Debug("============= ddl stmt parse end =========", "ddl", dataEvent.Query)
		return dataEvent, nil
	} else if row.Operation == OperationCodeDelete || row.Operation == OperationCodeUpdate ||
		row.Operation == OperationCodeInsert { // insert delete update
		// dml
		// redoSQL parse
		// todo 大小写问题
		row.SQLRedo = ReplaceQuotesString(row.SQLRedo)
		row.SQLRedo = ReplaceSpecifiedString(row.SQLRedo, ";", "")
		p := parser.New()
		stmt, _, err := p.Parse(row.SQLRedo, "", "")
		if err != nil {
			return dataEvent, err
		}
		visitor := &Stmt{}
		if len(stmt) <= 0 {
			return dataEvent, fmt.Errorf("parse dml err,stmt lens %d", len(stmt))
		}
		(stmt[0]).Accept(visitor)
		dataEvent = common.DataEvent{
			CurrentSchema: visitor.Schema,
			DatabaseName:  visitor.Schema,
			TableName:     visitor.Table,
			DML:           visitor.Operation,
		}
		e.logger.Debug("SchemaTable", "schema", visitor.Schema, "table", visitor.Table)
		// todo 转换成通用格式
		columns, err := e.oracleDB.GetColumns(visitor.Schema, visitor.Table)
		if err != nil {
			return dataEvent, err
		}
		e.logger.Debug("columns", "columns", columns, "visitorBefore", visitor.Before)
		for _, column := range columns {
			data, ok := visitor.Before[column]
			if !ok {
				continue
			}
			data = strings.TrimLeft(strings.TrimRight(data.(string), "'"), "'")
			visitor.WhereColumnValues.AbstractValues = append(visitor.WhereColumnValues.AbstractValues, data)
		}
		dataEvent = common.DataEvent{
			CurrentSchema:     visitor.Schema,
			DatabaseName:      visitor.Schema,
			TableName:         visitor.Table,
			DML:               visitor.Operation,
			ColumnCount:       uint64(len(visitor.Columns)),
			WhereColumnValues: visitor.WhereColumnValues,
			NewColumnValues:   visitor.NewColumnValues,
			Table:             nil,
		}
		if visitor.Operation == common.UpdateDML {
			row.SQLUndo = ReplaceQuotesString(row.SQLUndo)
			row.SQLUndo = ReplaceSpecifiedString(row.SQLUndo, ";", "")
			undoP := parser.New()
			stmtP, _, err := undoP.Parse(row.SQLUndo, "", "")
			if err != nil {
				return dataEvent, err
			}
			undoVisitor := &Stmt{}
			if len(stmtP) <= 0 {
				return dataEvent, nil
			}
			(stmtP[0]).Accept(undoVisitor)
			for _, column := range columns {
				data, ok := undoVisitor.Before[column]
				if !ok {
					continue
				}
				data = strings.TrimLeft(strings.TrimRight(data.(string), "'"), "'")
				undoVisitor.WhereColumnValues.AbstractValues = append(undoVisitor.WhereColumnValues.AbstractValues, data)
			}
			dataEvent.NewColumnValues = undoVisitor.WhereColumnValues
			dataEvent.Query = undoVisitor.WhereColumnValues.String()
		}
		e.logger.Debug("============= ddl stmt parse end =========", "dml")
		return dataEvent, nil
	}

	return dataEvent, fmt.Errorf("parese dateEvent fail")
}

func parseDDLSQL(logger hclog.Logger, redoSQL string) (dataEvent common.DataEvent, err error) {
	stmt, err := oracleParser.Parser(redoSQL)
	if err != nil {
		logger.Error("============= ddl parse err===============", "redoSQL", redoSQL)
		return dataEvent, err
	}
	switch s := stmt[0].(type) {
	case *ast.CreateTableStmt:
		var columns []string
		logger.Debug("CreateTableStmt", "schema:", s.TableName.Schema.Value, " table", s.TableName.Table.Value)
		for _, ts := range s.RelTable.TableStructs {
			switch td := ts.(type) {
			case *ast.ColumnDef:
				logger.Debug("caseColumnDef", "column:", td.ColumnName.Value, " type: ", td.Datatype.DataDef())
				columns = append(columns, OracleTypeParse(td))
			case *ast.OutOfLineConstraint:
				columns := []string{}
				for _, c := range td.Columns {
					columns = append(columns, c.Value)
				}
				logger.Debug("caseOutOfLineConstraint", "constraint type: ", td.Type, "constraint value :",
					strings.Join(columns, ","))
			}
		}
		schemaName := IdentifierToString(s.TableName.Schema)
		tableName := IdentifierToString(s.TableName.Table)
		dataEvent = common.DataEvent{
			Query:         fmt.Sprintf(`CREATE TABLE %s.%s (%s)`, schemaName, tableName, strings.Join(columns, ",")),
			CurrentSchema: schemaName,
			DatabaseName:  schemaName,
			TableName:     tableName,
			DML:           common.NotDML,
		}
	}
	return
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
