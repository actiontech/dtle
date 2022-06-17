package extractor

import (
	"container/list"
	"context"
	"crypto/md5"
	"database/sql"
	gosql "database/sql"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"
	"sync/atomic"
	"time"

	"github.com/actiontech/dtle/driver/common"
	"github.com/actiontech/dtle/driver/mysql/base"
	"github.com/actiontech/dtle/driver/oracle/config"
	"github.com/actiontech/dtle/g"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	_ "github.com/pingcap/tidb/types/parser_driver"
	"github.com/pkg/errors"
	oracleParser "github.com/sjjian/oracle-sql-parser"
	oracleAst "github.com/sjjian/oracle-sql-parser/ast"
	oracle_element "github.com/sjjian/oracle-sql-parser/ast/element"
	"github.com/thinkeridea/go-extend/exbytes"
)

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
	// AND ( seg_owner = 'TEST3')
	// AND ( seg_owner = 'TEST4' AND table_name in ('t3','t4'))

	for _, replicateDataSource := range l.replicateDB {
		if len(replicateDataSource.Tables) == 0 {
			filterSchemaTable = fmt.Sprintf(`%s OR ( seg_owner = '%s')`, filterSchemaTable, replicateDataSource.TableSchema)
		} else {
			tables := make([]string, 0)
			for _, table := range replicateDataSource.Tables {
				tables = append(tables, fmt.Sprintf("'%s'", table.TableName))
			}
			filterSchemaTable = fmt.Sprintf(`%s OR ( seg_owner = '%s' AND table_name in (%s))`, filterSchemaTable, replicateDataSource.TableSchema, strings.Join(tables, ","))
		}
	}
	if len(filterSchemaTable) > 0 {
		filterSchemaTable = strings.Replace(filterSchemaTable, "OR", "AND(", 1)
		filterSchemaTable = fmt.Sprintf("%s%s", filterSchemaTable, ")")
	}

	for _, ignoreDataSource := range l.ignoreReplicateDB {
		if len(ignoreDataSource.Tables) == 0 {
			filterSchemaTable = fmt.Sprintf(`%s AND ( seg_owner <> '%s')`, filterSchemaTable, ignoreDataSource.TableSchema)
		} else {
			tables := make([]string, 0)
			for _, table := range ignoreDataSource.Tables {
				tables = append(tables, fmt.Sprintf("'%s'", table.TableName))
			}
			filterSchemaTable = fmt.Sprintf(`%s AND ( seg_owner = '%s' AND table_name not in (%s))`, filterSchemaTable, ignoreDataSource.TableSchema, strings.Join(tables, ","))
		}
	}
	return
}

func (e *ExtractorOracle) GetLogMinerRecord(startScn, endScn int64, records chan *LogMinerRecord) error {
	l := e.LogMinerStream
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
		return err
	}
	defer rows.Close()

	scan := func(rows *sql.Rows) (*LogMinerRecord, error) {
		lr := &LogMinerRecord{}
		var segOwner, tableName, undoSQL sql.NullString
		err = rows.Scan(&lr.SCN, &segOwner, &tableName, &lr.SQLRedo, &undoSQL, &lr.Operation, &lr.XId,
			&lr.Csf, &lr.RowId, &lr.Rollback, &lr.RsId, &lr.StartTime, &lr.Username)
		if err != nil {
			return nil, err
		}
		lr.SegOwner = segOwner.String
		lr.TableName = tableName.String
		lr.SQLUndo = undoSQL.String
		return lr, nil
	}
	recordsNum := 0
	// var lrs []*LogMinerRecord
	for rows.Next() && !e.shutdown {
		lr, err := scan(rows)
		if err != nil {
			return err
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
					return err
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
		recordsNum += 1
		records <- lr
	}
	l.logger.Debug("Get logMiner record end", "recordsNum", recordsNum)
	return nil
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
	transactionId        string
	oldestUncommittedScn int64
	startScn             int64
	endScn               int64
	records              []*LogMinerRecord
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

func (lc *LogMinerTxCache) getOldestUncommittedSCN() int64 {
	ft := lc.getFirstActiveTx()
	if ft != nil {
		return ft.startScn // this is the oldest tx start scn in cache.
	} else {
		return 0
	}
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
		// delete tx from cache
		lc.cache.Remove(el)
		delete(lc.index, txId)

		tx := el.Value.(*LogMinerTx)
		tx.endScn = endScn
		tx.oldestUncommittedScn = lc.getOldestUncommittedSCN()

		if len(tx.records) != 0 {
			lc.Handler(tx)
		} else {
			fmt.Printf("empty transaction %s, start scn: %d, end scn: %d\n",
				tx.transactionId, tx.startScn, tx.endScn)
		}
	}
}

func (lc *LogMinerTxCache) rollbackTx(txId string, endScn int64) {
	if el, ok := lc.index[txId]; ok {
		delete(lc.index, txId)
		lc.cache.Remove(el)
	}
}

func (lc *LogMinerTxCache) addTxRecord(newRecord *LogMinerRecord) {
	if el, ok := lc.index[newRecord.TxId()]; ok {
		tx := el.Value.(*LogMinerTx)
		// 1 = if the redo record was generated because of a partial or a full rollback of the associated transaction
		if newRecord.Rollback == 1 {
			for i := len(tx.records) - 1; i >= 0; i-- {
				r := tx.records[i]
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

/*
=================================  Crash Recovery Logic ======================================

    scn1-1   scn2-1              scn3-1       scn2-n             scn1-n      scn3-n
 |----+--------+-------------------+-------------+-------  * ------------------------------->  // redo log
      ^        ^                   ^             ^         ^
    start1   start2              start3       commit2    crash
      |-------------------------------------------------------------                         // tx 1: active tx
               |---------------------------------|                                           // tx 2: committed tx
                                   |--------------------------------------------             // tx 3: active tx

If progress crash between `scn2-n` in `scn1-n`, the `tx 2` has been committed.
When progress restarted, we can't start with `crash` (scn), this will lose `tx 1` and `tx 3`.
So, we need to start with `scn1-1`, and ignore the `tx 2`.

scn1-1: oldestUncommittedScn
scn2-n: committedSCN
*/

func (e *ExtractorOracle) calculateSCNPos() (startSCN, committedSCN int64, err error) {
	var oldestUncommittedScn int64
	oldestUncommittedScn, committedSCN, err = e.storeManager.GetOracleSCNPosForJob(e.subject)
	if err != nil {
		return 0, 0, errors.Wrap(err, "GetOracleSCNPosForJob")
	}
	// first start
	if committedSCN == 0 {
		return e.mysqlContext.OracleConfig.Scn, 0, nil
	}
	// all tx has been committed
	if oldestUncommittedScn == 0 {
		startSCN = committedSCN
	} else {
		startSCN = oldestUncommittedScn - 1
	}
	return
}

func (e *ExtractorOracle) DataStreamEvents(entriesChannel chan<- *common.EntryContext) error {
	e.logger.Debug("start oracle. DataStreamEvents")

	if e.LogMinerStream.startScn == 0 {
		scn, err := e.LogMinerStream.oracleDB.GetCurrentSnapshotSCN()
		if err != nil {
			e.logger.Error("GetCurrentSnapshotSCN", "err", err)
			return err
		}
		e.logger.Debug("current scn", "scn", scn)
		e.LogMinerStream.startScn = scn
	}

	e.LogMinerStream.txCache.Handler = func(tx *LogMinerTx) error {
		if tx.endScn <= e.LogMinerStream.committedScn {
			e.logger.Debug("skip SQLs", "transactionId", tx.transactionId,
				"startSCN", tx.startScn, "endSCN", tx.endScn)
			return nil
		}
		atomic.AddUint32(&e.LogMinerStream.OracleTxNum, 1)
		Entry := e.handleSQLs(tx)
		entriesChannel <- &common.EntryContext{
			Entry:        Entry,
			TableItems:   nil,
			OriginalSize: 0,
		}
		return nil
	}

	err := e.LoopLogminerRecord()
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

func (e *ExtractorOracle) handleSQLs(tx *LogMinerTx) *common.DataEntry {
	entry := common.NewBinlogEntry()
	entry.Final = true
	oracleCoordinateTx := &common.OracleCoordinateTx{
		OldestUncommittedScn: tx.oldestUncommittedScn,
		EndSCN:               tx.endScn,
	}
	entry.Coordinates = oracleCoordinateTx
	for _, row := range tx.records {
		dataEvent, err := e.parseToDataEvent(row)
		if err != nil {
			e.logger.Error("parseOracleToMySQL", "err", err)
			continue
		}
		times, err := time.Parse(time.RFC3339, row.StartTime)
		if err != nil {
			e.logger.Error("parse timestamp", "err", err)
		}
		dataEvent.Timestamp = uint32(times.Unix())
		entry.Events = append(entry.Events, dataEvent)
	}
	return entry
}

type LogMinerStream struct {
	oracleDB                 *config.OracleDB
	startScn                 int64
	committedScn             int64
	interval                 int64
	initialized              bool
	currentRedoLogSequenceFP string
	logger                   g.LoggerType
	txCache                  *LogMinerTxCache
	replicateDB              []*common.DataSource
	ignoreReplicateDB        []*common.DataSource
	OracleTxNum              uint32
}

func NewLogMinerStream(db *config.OracleDB, logger g.LoggerType, replicateDB, ignoreReplicateDB []*common.DataSource,
	startScn, committedScn, interval int64) *LogMinerStream {
	return &LogMinerStream{
		oracleDB:          db,
		logger:            logger,
		replicateDB:       replicateDB,
		ignoreReplicateDB: ignoreReplicateDB,
		startScn:          startScn,
		committedScn:      committedScn,
		interval:          interval,
		txCache:           NewLogMinerTxCache(),
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

	fs, err := l.GetLogFileBySCN(l.startScn)
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

	err = l.oracleDB.NLS_DATE_FORMAT()
	if err != nil {
		l.logger.Error("alter date format ", "err", err)
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
	latestScn, err := l.oracleDB.GetCurrentSnapshotSCN()
	if err != nil {
		l.logger.Error("GetCurrentSnapshotSCN", "err", err)
		return 0, err
	}
	endScn := l.startScn + l.interval
	if endScn < latestScn {
		return endScn, nil
	} else {
		return latestScn, nil
	}
}

func (e *ExtractorOracle) LoopLogminerRecord() error {
	l := e.LogMinerStream
	err := l.initLogMiner()
	if err != nil {
		fmt.Println(err)
		return err
	}
	defer l.stopLogMiner()

	records := make(chan *LogMinerRecord, 100)
	defer close(records)
	go func() {
		t := time.NewTicker(time.Second * 5)
		defer func() {
			t.Stop()
			e.logger.Info("Handler Records goroutine exited")
		}()
		for !e.shutdown {
			select {
			case r, ok := <-records:
				if !ok {
					continue
				}
				atomic.AddInt64(&e.mysqlContext.DeltaEstimate, 1)
				switch r.Operation {
				case OperationCodeStart:
					l.txCache.startTx(r.TxId(), r.SCN)
				case OperationCodeCommit:
					l.txCache.commitTx(r.TxId(), r.SCN)
				case OperationCodeRollback:
					l.txCache.rollbackTx(r.TxId(), r.SCN)
				case OperationCodeDDL:
					l.txCache.Handler(&LogMinerTx{
						transactionId:        "",
						oldestUncommittedScn: l.txCache.getOldestUncommittedSCN(),
						startScn:             r.SCN,
						endScn:               r.SCN,
						records:              []*LogMinerRecord{r},
					})
				case OperationCodeInsert, OperationCodeDelete, OperationCodeUpdate:
					l.txCache.addTxRecord(r)
				}
				l.startScn = r.SCN
			case <-t.C:
				continue
			}
		}
	}()

	for !e.shutdown {
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
		if endScn == l.startScn {
			continue
		}

		err = l.StartLogMinerBySCN2(l.startScn, endScn)
		if err != nil {
			l.logger.Error("StartLMBySCN ", "err", err)
			return err
		}
		l.logger.Info("Get log miner record form", "StartScn", l.startScn, "EndScn", endScn)
		err = e.GetLogMinerRecord(l.startScn, endScn, records)
		if err != nil {
			l.logger.Error("GetLogMinerRecord ", "err", err)
			return err
		}
	}
	return nil
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

// func (l *LogMinerStream) HandlerRecords(records chan *LogMinerRecord) {
// 	for !e.shutdown {
// 		select {
// 		case r := <-records:
// 			switch r.Operation {
// 			case OperationCodeStart:
// 				l.txCache.startTx(r.TxId(), r.SCN)
// 			case OperationCodeCommit:
// 				l.txCache.commitTx(r.TxId(), r.SCN)
// 			case OperationCodeRollback:
// 				l.txCache.rollbackTx(r.TxId(), r.SCN)
// 			case OperationCodeDDL:
// 				l.txCache.Handler(&LogMinerTx{
// 					transactionId:        "",
// 					oldestUncommittedScn: l.txCache.getOldestUncommittedSCN(),
// 					startScn:             r.SCN,
// 					endScn:               r.SCN,
// 					records:              []*LogMinerRecord{r},
// 				})
// 			case OperationCodeInsert, OperationCodeDelete, OperationCodeUpdate:
// 				l.txCache.addTxRecord(r)
// 			}
// 			l.startScn = r.SCN
// 		}
// 	}
// }

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
	dataEvent := common.DataEvent{}
	// parse ddl and dml
	if row.Operation == OperationCodeDDL {
		// 字符集问题
		dataEvent, err := e.parseDDLSQL(row.SQLRedo, row.SegOwner)
		if err != nil {
			return common.DataEvent{}, err
		}
		return dataEvent, nil
	} else if row.Operation == OperationCodeDelete || row.Operation == OperationCodeUpdate ||
		row.Operation == OperationCodeInsert {
		dataEvent, err := e.parseDMLSQL(row.SQLRedo, row.SQLUndo)
		if err != nil {
			return common.DataEvent{}, err
		}
		return dataEvent, nil
	}
	return dataEvent, fmt.Errorf("parese dateEvent fail , operation Code %v", row.Operation)
}
func (e *ExtractorOracle) parseDMLSQL(oracleRedoSQL, oracleUndoSQL string) (dataEvent common.DataEvent, err error) {
	// Convert oracle SQL to MySQL format that can be parsed by tidb parese
	OracleToMySQLFormat := func(oracleSQL string) string {
		if strings.HasPrefix(oracleSQL, "insert into") {
			insertSqlSlice := strings.Split(oracleSQL, ") values (")
			if len(insertSqlSlice) == 2 {
				oracleSQL = strings.Replace(oracleSQL, insertSqlSlice[0], ReplaceSpecifiedString(insertSqlSlice[0], `"`, "`"), 1)
				oracleSQL = strings.Replace(oracleSQL, insertSqlSlice[1], strings.ReplaceAll(insertSqlSlice[1], `\`, `\\`), 1)
			} else {
				e.logger.Warn("schema/table/column names contain unexpected characters ) values (")
			}

		} else if strings.HasPrefix(oracleSQL, "delete") {
			delSqlSlice := strings.Split(oracleSQL, " where ")
			if len(delSqlSlice) == 2 {
				oracleSQL = strings.Replace(oracleSQL, delSqlSlice[0], ReplaceSpecifiedString(delSqlSlice[0], `"`, "`"), 1)
				whereExper := delSqlSlice[1]
				whereExperSli := strings.Split(whereExper, " and ")
				for i := range whereExperSli {
					colAndVal := strings.Split(whereExperSli[i], " = ")
					if len(colAndVal) == 2 {
						whereExperSli[i] = fmt.Sprintf("%s = %s", ReplaceSpecifiedString(colAndVal[0], `"`, "`"),
							ReplaceSpecifiedString(colAndVal[1], `\`, `\\`))
					}
				}
				whereExper = strings.Join(whereExperSli, " and ")
				oracleSQL = strings.Replace(oracleSQL, delSqlSlice[1], whereExper, 1)
			}
		} else if strings.HasPrefix(oracleSQL, "update") {
			// update "TEST"."BINARY_FLOAT6" set "COL2" ='500'  where "COL1" = '3' and "COL2" = 'NULL';
			updateSqlSlice := strings.Split(oracleSQL, " where ")
			if len(updateSqlSlice) == 2 {
				// "COL1" = '3' and "COL2" = 'NULL';
				whereExper := updateSqlSlice[1]
				whereExperSli := strings.Split(whereExper, " and ")
				for i := range whereExperSli {
					colAndVal := strings.Split(whereExperSli[i], " = ")
					if len(colAndVal) == 2 {
						whereExperSli[i] = fmt.Sprintf("%s = %s", ReplaceSpecifiedString(colAndVal[0], `"`, "`"),
							ReplaceSpecifiedString(colAndVal[1], `\`, `\\`))
					}
				}
				whereExper = strings.Join(whereExperSli, " and ")
				updateSqlSlice[1] = whereExper

				// update "TEST"."BINARY_FLOAT6" set "COL2" ='500' and "COL1" = 'ss'
				headerExper := updateSqlSlice[0]
				headExperSli := strings.Split(headerExper, " set ")
				if len(headExperSli) == 2 {
					// update "TEST"."BINARY_FLOAT6"
					headExperSli[0] = ReplaceSpecifiedString(headExperSli[0], `"`, "`")
					// "COL2" ='500' and "COL1" = 'ss'
					setExperSli := strings.Split(headExperSli[1], " and ")
					for i := range setExperSli {
						colAndVal := strings.Split(setExperSli[i], " = ")
						setExperSli[i] = fmt.Sprintf("%s = %s", ReplaceSpecifiedString(colAndVal[0], `"`, "`"),
							ReplaceSpecifiedString(colAndVal[1], `\`, `\\`))
					}
					headExperSli[1] = strings.Join(setExperSli, " and ")

					updateSqlSlice[0] = strings.Join(headExperSli, " set ")
					oracleSQL = strings.Join(updateSqlSlice, " where ")
				}
			}
			oracleSQL = ReplaceSpecifiedString(oracleSQL, `"`, "`")
		} else {
			e.logger.Warn("Special grammar, not as expected, sql %v", oracleSQL)
			oracleSQL = ReplaceSpecifiedString(oracleSQL, `"`, "`")
		}
		if strings.HasSuffix(oracleSQL, ";") {
			oracleSQL = ReplaceSpecifiedString(oracleSQL, ";", "")
		}
		return oracleSQL
	}
	redoSQL := OracleToMySQLFormat(oracleRedoSQL)
	p := parser.New()
	stmt, _, err := p.Parse(redoSQL, "", "")
	if err != nil {
		return dataEvent, err
	}
	visitor := &Stmt{logger: e.logger}
	if len(stmt) <= 0 {
		return dataEvent, fmt.Errorf("parse dml err,stmt lens %d", len(stmt))
	}
	(stmt[0]).Accept(visitor)
	if visitor.Error != nil {
		return dataEvent, visitor.Error
	}
	dataEvent = common.DataEvent{
		CurrentSchema: visitor.Schema,
		DatabaseName:  visitor.Schema,
		TableName:     visitor.Table,
		DML:           visitor.Operation,
	}
	// e.logger.Debug("SchemaTable", "schema", visitor.Schema, "table", visitor.Table)
	schemaConfig := e.findSchemaConfig(visitor.Schema)
	tableConfig := findTableConfig(schemaConfig, visitor.Table)
	ordinals := tableConfig.OriginalTableColumns.Ordinals
	// e.logger.Debug("columns", "columns", ordinals, "visitorBefore", visitor.Before)
	visitor.WhereColumnValues = make([]interface{}, len(ordinals))
	for column, index := range ordinals {
		data, ok := visitor.Before[column]
		if !ok {
			continue
		}
		visitor.WhereColumnValues[index] = data
	}
	dataEvent = common.DataEvent{
		CurrentSchema: visitor.Schema,
		DatabaseName:  visitor.Schema,
		TableName:     visitor.Table,
		DML:           visitor.Operation,
		ColumnCount:   uint64(len(visitor.Columns)),
		Table:         nil,
	}
	switch visitor.Operation {
	case common.InsertDML:
		dataEvent.Rows = [][]interface{}{visitor.NewColumnValues}
	case common.DeleteDML:
		dataEvent.Rows = [][]interface{}{visitor.WhereColumnValues}
	case common.UpdateDML:
		undoSQL := OracleToMySQLFormat(oracleUndoSQL)
		undoP := parser.New()
		untoStmt, _, err := undoP.Parse(undoSQL, "", "")
		if err != nil {
			return dataEvent, err
		}
		undoVisitor := &Stmt{logger: e.logger}
		if len(untoStmt) <= 0 {
			return dataEvent, nil
		}
		(untoStmt[0]).Accept(undoVisitor)
		if undoVisitor.Error != nil {
			return dataEvent, undoVisitor.Error
		}
		undoVisitor.WhereColumnValues = make([]interface{}, len(ordinals))
		for column, index := range ordinals {
			data, ok := undoVisitor.Before[column]
			if !ok {
				continue
			}
			undoVisitor.WhereColumnValues[index] = data
		}
		dataEvent.Rows = [][]interface{}{
			visitor.WhereColumnValues,
			undoVisitor.WhereColumnValues,
		}
	}
	// e.logger.Debug("============= dml stmt parse end =========")
	return dataEvent, nil
}

var constrainto2m = map[oracleAst.ConstraintType]ast.ConstraintType{
	oracleAst.ConstraintTypeUnique: ast.ConstraintUniq,
	oracleAst.ConstraintTypePK:     ast.ConstraintPrimaryKey,
}

func (e *ExtractorOracle) parseDDLSQL(redoSQL string, segOwner string) (dataEvent common.DataEvent, err error) {
	e.logger.Debug("ddl stmt parse start", "redoSQL", redoSQL)
	stmt, err := oracleParser.Parser(redoSQL)
	if err != nil {
		e.logger.Error("ddl parse err", "redoSQL", redoSQL)
		return dataEvent, err
	}
	getSchemaName := func(schema *oracle_element.Identifier) string {
		schemaName := ""
		if schema == nil {
			schemaName = segOwner
		} else {
			schemaName = IdentifierToString(schema)
		}
		return schemaName
	}
	switch s := stmt[0].(type) {
	case *oracleAst.CreateTableStmt:
		schemaName := getSchemaName(s.TableName.Schema)
		tableName := IdentifierToString(s.TableName.Table)

		// database table structure record
		schemaConfig := e.findSchemaConfig(schemaName)
		tableConfig := findTableConfig(schemaConfig, tableName)
		ordinals := make(map[string]int, 0)
		tableConfig.OriginalTableColumns = &common.ColumnList{Ordinals: ordinals}

		// generate MySQL create table stmt
		createTableStmt := &ast.CreateTableStmt{
			TemporaryKeyword: ast.TemporaryNone,
			Table: &ast.TableName{
				Schema: model.NewCIStr(schemaName),
				Name:   model.NewCIStr(tableName),
			},
			Options: []*ast.TableOption{{
				Tp:       ast.TableOptionCharset,
				StrValue: "utf8mb4",
			}},
		}
		var columns []*ast.ColumnDef
		var constraints []*ast.Constraint
		for _, ts := range s.RelTable.TableStructs {
			switch td := ts.(type) {
			case *oracleAst.ColumnDef:
				columns = append(columns, oracleTp2MySQLTp(td))
				ordinals[IdentifierToString(td.ColumnName)] = len(ordinals)
			case *oracleAst.OutOfLineConstraint:
				keys := make([]*ast.IndexPartSpecification, 0)
				for i := range td.Columns {
					keys = append(keys, &ast.IndexPartSpecification{
						Column: &ast.ColumnName{
							Name: model.NewCIStr(td.Columns[i].Value),
						},
					})
				}
				constraints = append(constraints, &ast.Constraint{
					Tp:   constrainto2m[td.Type],
					Name: IdentifierToString(td.Name),
					Keys: keys,
				})
			}
		}
		createTableStmt.Cols = columns
		createTableStmt.Constraints = constraints
		createSQL, err := base.ParserRestore(createTableStmt)
		if err != nil {
			e.logger.Error("restore ddl err", "err", err)
			return dataEvent, err
		}
		dataEvent = common.DataEvent{
			Query:         createSQL,
			CurrentSchema: schemaName,
			DatabaseName:  schemaName,
			TableName:     tableName,
			DML:           common.NotDML,
		}
		dataEvent.DtleFlags |= common.DtleFlagCreateSchemaIfNotExists
	case *oracleAst.AlterTableStmt:
		schemaName := getSchemaName(s.TableName.Schema)
		tableName := IdentifierToString(s.TableName.Table)
		schemaConfig := e.findSchemaConfig(schemaName)
		tableConfig := findTableConfig(schemaConfig, tableName)
		ordinals := make(map[string]int, 0)
		tableConfig.OriginalTableColumns = &common.ColumnList{Ordinals: ordinals}

		alterTableStmt := &ast.AlterTableStmt{
			Table: &ast.TableName{
				Schema: model.NewCIStr(schemaName),
				Name:   model.NewCIStr(tableName),
			},
		}
		alterOptions := make([]*ast.AlterTableSpec, 0)
		for _, alter := range s.AlterTableClauses {
			switch a := alter.(type) {
			case *oracleAst.AddColumnClause:
				colDefinitions := make([]*ast.ColumnDef, 0)
				for _, column := range a.Columns {
					colDefinitions = append(colDefinitions, oracleTp2MySQLTp(column))
					ordinals[IdentifierToString(column.ColumnName)] = len(ordinals)
				}
				alterOptions = append(alterOptions, &ast.AlterTableSpec{
					NewColumns: colDefinitions,
					Tp:         ast.AlterTableAddColumns,
				})
			case *oracleAst.ModifyColumnClause:
				colDefinitions := make([]*ast.ColumnDef, 0)
				for _, column := range a.Columns {
					colDefinitions = append(colDefinitions, oracleTp2MySQLTp(column))
					ordinals[IdentifierToString(column.ColumnName)] = len(ordinals)
				}
				alterOptions = append(alterOptions, &ast.AlterTableSpec{
					NewColumns: colDefinitions,
					Position: &ast.ColumnPosition{
						Tp: ast.ColumnPositionNone,
					},
					Tp: ast.AlterTableModifyColumn,
				})
			case *oracleAst.DropColumnClause:
				for _, column := range a.Columns {
					columnName := IdentifierToString(column)
					alterOptions = append(alterOptions, &ast.AlterTableSpec{
						OldColumnName: &ast.ColumnName{
							Schema: model.NewCIStr(schemaName),
							Table:  model.NewCIStr(tableName),
							Name:   model.NewCIStr(columnName),
						},
						Tp: ast.AlterTableDropColumn,
					})
					// sort columns ordinals
					dropIndex := ordinals[IdentifierToString(column)]
					delete(ordinals, IdentifierToString(column))
					for k, index := range ordinals {
						if index > dropIndex {
							ordinals[k] -= 1
						}
					}
				}
			case *oracleAst.RenameColumnClause:
				oldName := IdentifierToString(a.OldName)
				newName := IdentifierToString(a.NewName)
				alterOptions = append(alterOptions, &ast.AlterTableSpec{
					OldColumnName: &ast.ColumnName{
						Schema: model.NewCIStr(schemaName),
						Table:  model.NewCIStr(tableName),
						Name:   model.NewCIStr(oldName),
					},
					NewColumnName: &ast.ColumnName{
						Schema: model.NewCIStr(schemaName),
						Table:  model.NewCIStr(tableName),
						Name:   model.NewCIStr(newName),
					},
					Tp: ast.AlterTableRenameColumn,
				})

				ordinals[newName] = ordinals[oldName]
				delete(ordinals, oldName)
			case *oracleAst.AddConstraintClause:
				// todo
			case *oracleAst.ModifyConstraintClause:
				// todo
			case *oracleAst.RenameConstraintClause:
				// todo
			case *oracleAst.DropConstraintClause:
				// todo
			}
		}
		alterTableStmt.Specs = alterOptions

		alterSQL, err := base.ParserRestore(alterTableStmt)
		if err != nil {
			e.logger.Error("restore alter ddl err", "err", err)
			return dataEvent, err
		}
		dataEvent = common.DataEvent{
			Query:         alterSQL,
			CurrentSchema: schemaName,
			DatabaseName:  schemaName,
			TableName:     tableName,
			DML:           common.NotDML,
		}
	case *oracleAst.DropTableStmt:
		schemaName := getSchemaName(s.TableName.Schema)
		tableName := IdentifierToString(s.TableName.Table)
		dropTableStmt := ast.DropTableStmt{
			Tables: []*ast.TableName{},
		}
		dropTableStmt.Tables = append(dropTableStmt.Tables, &ast.TableName{
			Schema: model.NewCIStr(schemaName),
			Name:   model.NewCIStr(tableName),
		})
		dropSQL, err := base.ParserRestore(&dropTableStmt)
		if err != nil {
			return dataEvent, err
		}
		dataEvent = common.DataEvent{
			Query:         dropSQL,
			CurrentSchema: schemaName,
			DatabaseName:  schemaName,
			TableName:     tableName,
			DML:           common.NotDML,
		}
	}
	e.logger.Debug("ddl stmt parse end", "ddl", dataEvent.Query)
	return
}

func loadTableConfig(schemaConfig *common.DataSource, tableName string) *common.Table {
	newtb := &common.Table{}

	newtb.TableSchema = schemaConfig.TableSchema
	newtb.TableName = tableName
	schemaConfig.Tables = append(schemaConfig.Tables, newtb)
	return newtb
}

func (e *ExtractorOracle) findSchemaConfig(schemaName string) *common.DataSource {
	if schemaName == "" {
		return nil
	}
	for i := range e.replicateDoDb {
		if e.replicateDoDb[i].TableSchema == schemaName {
			return e.replicateDoDb[i]
		} else if regStr := e.replicateDoDb[i].TableSchemaRegex; regStr != "" {
			reg, err := regexp.Compile(regStr)
			if nil != err {
				continue
			}
			if reg.MatchString(schemaName) {
				return e.replicateDoDb[i]
			}
		}
	}
	schemaConfig := &common.DataSource{TableSchema: schemaName}
	e.replicateDoDb = append(e.replicateDoDb, schemaConfig)
	return schemaConfig
}

func findTableConfig(schema *common.DataSource, tableName string) *common.Table {
	if schema != nil {
		for j := range schema.Tables {
			if schema.Tables[j].TableName == tableName {
				return schema.Tables[j]
			}
		}
	}
	return loadTableConfig(schema, tableName)
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
	return string(exbytes.Replace([]byte(s), []byte("\""), []byte("`"), -1))
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
