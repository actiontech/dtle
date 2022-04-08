/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package extractor

import (
	gosql "database/sql"
	"encoding/binary"
	"fmt"
	"math"
	"regexp"

	"github.com/sjjian/oracle-sql-parser/ast"

	"github.com/actiontech/dtle/drivers/driver/oracle/config"

	mysql "github.com/actiontech/dtle/drivers/driver/mysql"
	"github.com/actiontech/dtle/drivers/driver/mysql/base"
	"github.com/actiontech/dtle/drivers/driver/mysql/binlog"
	"github.com/actiontech/dtle/drivers/driver/mysql/mysqlconfig"

	"github.com/actiontech/dtle/drivers/driver/common"
	"github.com/cznic/mathutil"
	"github.com/hashicorp/nomad/plugins/drivers"

	"github.com/actiontech/dtle/g"
	"github.com/pkg/errors"

	"strconv"
	"sync"
	"sync/atomic"
	"time"

	sqle "github.com/actiontech/dtle/drivers/driver/mysql/sqle/inspector"
	gonats "github.com/nats-io/go-nats"
)

// ExtractorOracle is the main schema extract flow manager.
type ExtractorOracle struct {
	execCtx      *common.ExecContext
	logger       g.LoggerType
	subject      string
	mysqlContext *common.MySQLDriverConfig

	systemVariables       map[string]string
	sqlMode               string
	lowerCaseTableNames   mysqlconfig.LowerCaseTableNamesValue
	MySQLVersion          string
	TotalTransferredBytes int
	// Original comment: TotalRowsCopied returns the accurate number of rows being copied (affected)
	// This is not exactly the same as the rows being iterated via chunks, but potentially close enough.
	// TODO What is the difference between mysqlContext.RowsEstimate ?
	TotalRowsCopied int64
	natsAddr        string

	mysqlVersionDigit int
	oracleDB          *config.OracleDB
	db                *gosql.DB
	singletonDB       *gosql.DB
	//dumpers           []*mysql.dumper
	// db.tb exists when creating the job, for full-copy.
	// vs e.mysqlContext.ReplicateDoDb: all user assigned db.tb
	replicateDoDb            []*common.DataSource
	dataChannel              chan *common.BinlogEntryContext
	inspector                *mysql.Inspector
	binlogReader             *binlog.BinlogReader
	LogMinerStream           *LogMinerStream
	initialBinlogCoordinates *common.OracleCoordinates
	currentBinlogCoordinates *common.OracleCoordinateTx
	//rowCopyComplete          chan bool
	rowCopyCompleteFlag int64
	tableCount          int

	sendByTimeoutCounter  int
	sendBySizeFullCounter int

	natsConn *gonats.Conn
	waitCh   chan *drivers.ExitResult

	shutdown     bool
	shutdownCh   chan struct{}
	shutdownLock sync.Mutex

	testStub1Delay int64

	context *sqle.Context

	// This must be `<-` after `getSchemaTablesAndMeta()`.
	gotCoordinateCh chan struct{}
	streamerReadyCh chan error
	fullCopyDone    chan struct{}
	storeManager    *common.StoreManager

	timestampCtx *TimestampContext

	memory1   *int64
	memory2   *int64
	finishing bool

	// we need to close all data channel while pausing task runner. and these data channel will be recreate when restart the runner.
	// to avoid writing closed channel, we need to wait for all goroutines that deal with data channels finishing. wg is used for the waiting.
	wg         sync.WaitGroup
	targetGtid string

	OracleContext *OracleContext
}
type OracleContext struct {
	// currentSchema will change after sql "use database"
	currentSchema string

	schemas map[string]*OracleSchemaInfo
	// if schemas info has collected, set true
	schemaHasLoad bool
}
type OracleSchemaInfo struct {
	Tables map[string]*ast.CreateTableStmt
}

func NewExtractorOracle(execCtx *common.ExecContext, cfg *common.MySQLDriverConfig, logger g.LoggerType, storeManager *common.StoreManager, waitCh chan *drivers.ExitResult) (*ExtractorOracle, error) {
	logger.Info("NewExtractorOracle", "job", execCtx.Subject)

	e := &ExtractorOracle{
		logger:       logger.Named("ExtractorOracle").With("job", execCtx.Subject),
		execCtx:      execCtx,
		subject:      execCtx.Subject,
		mysqlContext: cfg,
		//rowCopyComplete: make(chan bool),
		waitCh:          waitCh,
		shutdownCh:      make(chan struct{}),
		testStub1Delay:  0,
		context:         sqle.NewContext(nil),
		gotCoordinateCh: make(chan struct{}),
		streamerReadyCh: make(chan error),
		fullCopyDone:    make(chan struct{}),
		storeManager:    storeManager,
		memory1:         new(int64),
		memory2:         new(int64),
		OracleContext:   new(OracleContext),
	}
	e.dataChannel = make(chan *common.BinlogEntryContext, cfg.ReplChanBufferSize*4)
	e.timestampCtx = NewTimestampContext(e.shutdownCh, e.logger, func() bool {
		return len(e.dataChannel) == 0
		// TODO need a more reliable method to determine queue.empty.
	})

	return e, nil
}

// Run executes the complete extract logic.
func (e *ExtractorOracle) Run() {
	var err error

	{
		jobStatus, _ := e.storeManager.GetJobStatus(e.subject)
		if jobStatus == common.TargetGtidFinished {
			_ = e.Shutdown()
			return
		}
	}

	e.logger.Info("src watch Nats")
	e.natsAddr, err = e.storeManager.SrcWatchNats(e.subject, e.shutdownCh, func(err error) {
		e.onError(common.TaskStateDead, err)
	})
	if err != nil {
		e.onError(common.TaskStateDead, errors.Wrap(err, "SrcWatchNats"))
		return
	}
	// init nats
	e.logger.Info("initNatsPubClient")
	e.logger.Debug("begin Connect nats server", "NatAddr", e.natsAddr)
	sc, err := gonats.Connect(e.natsAddr)
	if err != nil {
		e.logger.Error("cannot connect nats server", "natsAddr", e.natsAddr, "err", err)
		e.onError(common.TaskStateDead, errors.Wrap(err, "Connect Nats"))
		return
	}
	e.logger.Info("Connect nats server", "natsAddr", e.natsAddr)
	e.natsConn = sc

	_, err = e.natsConn.Subscribe(fmt.Sprintf("%s_control2", e.subject), func(m *gonats.Msg) {
		if m.Data == nil {
			e.onError(common.TaskStateDead, fmt.Errorf("zero-byte control msg"))
			return
		}

		ctrlMsg := &common.ControlMsg{}
		_, err := ctrlMsg.Unmarshal(m.Data)
		if err != nil {
			e.onError(common.TaskStateDead, fmt.Errorf("failed to unmarshal a control msg"))
			return
		}

		switch ctrlMsg.Type {
		case common.ControlMsgError:
			e.onError(common.TaskStateDead, fmt.Errorf("applier error/restart: %v", ctrlMsg.Msg))
			return
		}
	})
	if err != nil {
		e.onError(common.TaskStateDead, errors.Wrap(err, "Subscribe control2"))
		return
	}

	startSCN, committedSCN, err := e.calculateSCNPos()
	if err != nil {
		e.onError(common.TaskStateDead, errors.Wrap(err, "calculateSCNPos"))
		return
	}

	e.initDBConnections()
	e.getSchemaTablesAndMeta()

	e.LogMinerStream = NewLogMinerStream(e.oracleDB, e.logger, e.mysqlContext.ReplicateDoDb, e.mysqlContext.ReplicateIgnoreDb,
		startSCN, committedSCN, 100000)
	//e.logger.Info("CheckAndApplyLowerCaseTableNames")
	//e.CheckAndApplyLowerCaseTableNames()
	// 字符集同步 todo
	fullCopy := false
	if fullCopy {
		e.logger.Debug("mysqlDump. before")
	} else { // no full copy
		// Will not get consistent table meta-info for an incremental only job.
		// https://github.com/actiontech/dtle/issues/321#issuecomment-441191534
		// 获取需要同步的表结构数据
		//if err := e.getSchemaTablesAndMeta(); err != nil {
		//	e.onError(common.TaskStateDead, err)
		//	return
		//}
	}
	//err = e.sendFullComplete()
	{
		//if err != nil {
		//	e.logger.Error("error after streamerReadyCh", "err", err)
		//	e.onError(common.TaskStateDead, err)
		//	return
		//}
		e.logger.Debug("start .initiateStreaming before")
		if err := e.initiateStreaming(); err != nil {
			e.logger.Error("error at initiateStreaming", "err", err)
			e.onError(common.TaskStateDead, err)
			return
		}
	}
}

func (e *ExtractorOracle) Stats() (*common.TaskStatistics, error) {
	totalRowsCopied := atomic.LoadInt64(&e.TotalRowsCopied)
	rowsEstimate := atomic.LoadInt64(&e.mysqlContext.RowsEstimate)
	deltaEstimate := atomic.LoadInt64(&e.mysqlContext.DeltaEstimate)
	if atomic.LoadInt64(&e.rowCopyCompleteFlag) == 1 {
		// Done copying rows. The totalRowsCopied value is the de-facto number of rows,
		// and there is no further need to keep updating the value.
		rowsEstimate = totalRowsCopied
	}
	var progressPct float64
	if rowsEstimate == 0 {
		progressPct = 0.0
	} else {
		progressPct = 100.0 * float64(totalRowsCopied) / float64(rowsEstimate)
	}

	var etaSeconds float64 = math.MaxFloat64
	var eta string
	eta = "N/A"

	if progressPct >= 100.0 {
		eta = "0s"
		e.mysqlContext.Stage = common.StageMasterHasSentAllBinlogToSlave
	} else if progressPct >= 1.0 {
		elapsedRowCopySeconds := e.mysqlContext.ElapsedRowCopyTime().Seconds()
		totalExpectedSeconds := elapsedRowCopySeconds * float64(rowsEstimate) / float64(totalRowsCopied)
		etaSeconds = totalExpectedSeconds - elapsedRowCopySeconds
		if etaSeconds >= 0 {
			etaDuration := time.Duration(etaSeconds) * time.Second
			eta = base.PrettifyDurationOutput(etaDuration)
		} else {
			eta = "0s"
		}
	}

	var extractedTxCount uint32
	if e.LogMinerStream != nil {
		extractedTxCount = atomic.LoadUint32(&e.LogMinerStream.OracleTxNum)
	}

	taskResUsage := common.TaskStatistics{
		ExecMasterRowCount: totalRowsCopied,
		ExecMasterTxCount:  deltaEstimate,
		ReadMasterRowCount: rowsEstimate,
		ReadMasterTxCount:  deltaEstimate,
		ProgressPct:        strconv.FormatFloat(progressPct, 'f', 1, 64),
		ETA:                eta,
		Backlog:            fmt.Sprintf("%d/%d", len(e.dataChannel), cap(e.dataChannel)),
		Stage:              e.mysqlContext.Stage,
		BufferStat: common.BufferStat{
			BinlogEventQueueSize: e.binlogReader.GetQueueSize(),
			ExtractorTxQueueSize: len(e.dataChannel),
			SendByTimeout:        e.sendByTimeoutCounter,
			SendBySizeFull:       e.sendBySizeFullCounter,
		},
		DelayCount: &common.DelayCount{
			Num: 0,
			//Time: e.timestampCtx.GetDelay(),
		},
		Timestamp: time.Now().UTC().UnixNano(),
		MemoryStat: common.MemoryStat{
			Full: *e.memory1,
			Incr: *e.memory2 + e.binlogReader.GetQueueMem(),
		},
		HandledTxCount: common.TxCount{
			ExtractedTxCount: &extractedTxCount,
		},
	}
	if e.natsConn != nil {
		taskResUsage.MsgStat = e.natsConn.Statistics
		e.TotalTransferredBytes = int(taskResUsage.MsgStat.OutBytes)
		if e.mysqlContext.TrafficAgainstLimits > 0 && int(taskResUsage.MsgStat.OutBytes)/1024/1024/1024 >= e.mysqlContext.TrafficAgainstLimits {
			e.onError(common.TaskStateDead, fmt.Errorf("traffic limit exceeded : %d/%d", e.mysqlContext.TrafficAgainstLimits, int(taskResUsage.MsgStat.OutBytes)/1024/1024/1024))
		}
	}

	if e.binlogReader != nil {
		currentBinlogCoordinates := e.binlogReader.GetCurrentBinlogCoordinates()
		taskResUsage.CurrentCoordinates = &common.CurrentCoordinates{
			File:     currentBinlogCoordinates.LogFile,
			Position: currentBinlogCoordinates.LogPos,
			GtidSet:  currentBinlogCoordinates.GtidSet,
		}
	} else {
		taskResUsage.CurrentCoordinates = &common.CurrentCoordinates{
			File:     "",
			Position: 0,
			GtidSet:  "",
		}
	}

	return &taskResUsage, nil
}

// Shutdown is used to tear down the ExtractorOracle
func (e *ExtractorOracle) Shutdown() error {
	e.logger.Debug("ExtractorOracle shutdown")
	e.shutdownLock.Lock()
	defer e.shutdownLock.Unlock()

	if e.shutdown {
		return nil
	}
	e.logger.Info("ExtractorOracle shutdown")

	e.shutdown = true
	close(e.shutdownCh)

	if e.natsConn != nil {
		e.natsConn.Close()
	}

	if err := e.oracleDB.Close(); err != nil {
		e.logger.Error("Shutdown error close oracleDB.", "err", err)
	}

	e.wg.Wait()

	e.logger.Info("Shutting down")
	return nil
}

func (e *ExtractorOracle) Finish1() (err error) {
	// TODO shutdown job on error

	if e.finishing {
		return nil
	}
	e.finishing = true

	coord, err := base.GetSelfBinlogCoordinates(e.db)
	if err != nil {
		return errors.Wrap(err, "GetSelfBinlogCoordinates")
	}
	e.targetGtid = coord.GtidSet

	e.logger.Info("Finish. got target GTIDSet", "gs", e.targetGtid)

	err = e.storeManager.PutTargetGtid(e.subject, e.targetGtid)
	if err != nil {
		return errors.Wrap(err, "PutTargetGtid")
	}

	err = e.binlogReader.SetTargetGtid(e.targetGtid)
	if err != nil {
		return errors.Wrap(err, "afterGetTargetGtid")
	}

	return nil
}

func (e *ExtractorOracle) getSchemaTablesAndMeta() error {
	if err := e.inspectTables(); err != nil {
		return err
	}
	for _, db := range e.replicateDoDb {
		for _, tb := range db.Tables {
			columns, err := e.oracleDB.GetColumns(db.TableSchema, tb.TableName)
			if err != nil {
				return err
			}
			tb.OriginalTableColumns = &common.ColumnList{
				Ordinals: map[string]int{},
			}
			for i, column := range columns {
				tb.OriginalTableColumns.Ordinals[column] = i
			}
		}
	}

	return nil
}

func (e *ExtractorOracle) getCurrentSchema(schema, table string) map[string]int {
	for _, db := range e.replicateDoDb {
		for _, tb := range db.Tables {
			if db.TableSchema == schema && tb.TableName == table {
				return tb.OriginalTableColumns.Ordinals
			}
		}
	}
	return map[string]int{}
}

func (e *ExtractorOracle) inspectTables() (err error) {
	// Creates a MYSQL Dump based on the options supplied through the dumper.
	dbsExisted, err := e.oracleDB.GetSchemas() // todo merged
	if err != nil {
		return err
	}
	dbsFiltered := []string{}
	for _, db := range dbsExisted {
		if len(e.mysqlContext.ReplicateIgnoreDb) > 0 && common.IgnoreDbByReplicateIgnoreDb(e.mysqlContext.ReplicateIgnoreDb, db) {
			continue
		}
		dbsFiltered = append(dbsFiltered, db)
	}

	if len(e.mysqlContext.ReplicateDoDb) > 0 {
		var doDbs []*common.DataSource
		// Get all db from TableSchemaRegex regex and get all tableSchemaRename
		for _, doDb := range e.mysqlContext.ReplicateDoDb {
			var regex string
			if doDb.TableSchemaRegex != "" && doDb.TableSchemaRename != "" {
				regex = doDb.TableSchemaRegex
				schemaRenameRegex := doDb.TableSchemaRename
				for _, db := range dbsFiltered {
					newdb := &common.DataSource{}
					*newdb = *doDb
					reg, err := regexp.Compile(regex)
					if err != nil {
						return errors.Wrapf(err, "SchemaRegex %v", regex)
					}
					if !reg.MatchString(db) {
						continue
					}
					newdb.TableSchema = db
					match := reg.FindStringSubmatchIndex(db)
					newdb.TableSchemaRename = string(reg.ExpandString(nil, schemaRenameRegex, db, match))
					doDbs = append(doDbs, newdb)
				}
				//if doDbs == nil {
				//	return fmt.Errorf("src schmea was nil")
				//}
			} else if doDb.TableSchemaRegex == "" { // use doDb.TableSchema
				for _, db := range dbsFiltered {
					if db == doDb.TableSchema {
						doDbs = append(doDbs, doDb)
					}
				}
			} else {
				return fmt.Errorf("TableSchema configuration error")
			}
		}
		for _, doDb := range doDbs {
			db := &common.DataSource{
				TableSchema:       doDb.TableSchema,
				TableSchemaRegex:  doDb.TableSchemaRegex,
				TableSchemaRename: doDb.TableSchemaRename,
			}

			existedTables, err := e.oracleDB.GetTables(doDb.TableSchema)
			if err != nil {
				return err
			}
			tbsFiltered := []*common.Table{}
			for _, tb := range existedTables {
				if len(e.mysqlContext.ReplicateIgnoreDb) > 0 && common.IgnoreTbByReplicateIgnoreDb(e.mysqlContext.ReplicateIgnoreDb, db.TableSchema, tb) {
					continue
				}
				tbsFiltered = append(tbsFiltered, &common.Table{
					TableName:   tb,
					TableSchema: db.TableSchema,
				})
			}

			if len(doDb.Tables) == 0 { // replicate all tables
				for _, doTb := range tbsFiltered {
					doTb.TableSchema = doDb.TableSchema
					doTb.TableSchemaRename = doDb.TableSchemaRename
					//if err := e.inspector.ValidateOriginalTable(doDb.TableSchema, doTb.TableName, doTb); err != nil {
					//	e.logger.Warn("ValidateOriginalTable error", "err", err,
					//		"schema", doDb.TableSchema, "table", doTb.TableName)
					//	continue
					//}
					db.Tables = append(db.Tables, doTb)
				}
			} else { // replicate selected tables
				for _, doTb := range doDb.Tables {
					doTb.TableSchema = doDb.TableSchema
					doTb.TableSchemaRename = doDb.TableSchemaRename

					var regex string
					if doTb.TableRegex != "" && doTb.TableRename != "" {
						regex = doTb.TableRegex
						tableRenameRegex := doTb.TableRename
						for _, table := range tbsFiltered {
							reg, err := regexp.Compile(regex)
							if err != nil {
								return errors.Wrapf(err, "TableRegex %v", regex)
							}
							if !reg.MatchString(table.TableName) {
								continue
							}
							newTable := &common.Table{}
							*newTable = *doTb
							newTable.TableName = table.TableName
							match := reg.FindStringSubmatchIndex(table.TableName)
							newTable.TableRename = string(reg.ExpandString(nil, tableRenameRegex, table.TableName, match))
							//if err := e.inspector.ValidateOriginalTable(doDb.TableSchema, table.TableName, newTable); err != nil {
							//	e.logger.Warn("ValidateOriginalTable error", "TableSchema", doDb.TableSchema, "TableName", doTb.TableName, "err", err)
							//	continue
							//}
							db.Tables = append(db.Tables, newTable)
						}
						//if db.Tables == nil {
						//	return fmt.Errorf("src table was nil")
						//}

					} else if doTb.TableRegex == "" {
						for _, existedTable := range tbsFiltered {
							if existedTable.TableName != doTb.TableName {
								continue
							}
							//if err := e.inspector.ValidateOriginalTable(doDb.TableSchema, doTb.TableName, doTb); err != nil {
							//	e.logger.Warn("ValidateOriginalTable error", "TableSchema", doDb.TableSchema, "TableName", doTb.TableName, "err", err)
							//	continue
							//}
							newTable := &common.Table{}
							*newTable = *doTb
							db.Tables = append(db.Tables, newTable)
						}
					} else {
						return fmt.Errorf("table configuration error")
					}

				}
			}
			e.replicateDoDb = append(e.replicateDoDb, db)
		}
		//	e.mysqlContext.ReplicateDoDb = e.replicateDoDb
	} else { // empty DoDB. replicate all db/tb
		for _, dbName := range dbsFiltered {
			ds := &common.DataSource{
				TableSchema: dbName,
			}

			tbs, err := e.oracleDB.GetTables(dbName)
			if err != nil {
				return err
			}

			for _, tb := range tbs {
				if len(e.mysqlContext.ReplicateIgnoreDb) > 0 && common.IgnoreTbByReplicateIgnoreDb(e.mysqlContext.ReplicateIgnoreDb, dbName, tb) {
					continue
				}
				//if err := e.inspector.ValidateOriginalTable(dbName, tb.TableName, tb); err != nil {
				//	e.logger.Warn("ValidateOriginalTable error", "TableSchema", dbName, "TableName", tb.TableName, "err", err)
				//	continue
				//}

				ds.Tables = append(ds.Tables, &common.Table{
					TableName:   tb,
					TableSchema: dbName,
				})
			}
			e.replicateDoDb = append(e.replicateDoDb, ds)
		}
	}
	/*if e.mysqlContext.ExpandSyntaxSupport {
		db_mysql := &config.DataSource{
			TableSchema: "mysql",
		}
		db_mysql.Tables = append(db_mysql.Tables,
			&config.Table{
				TableSchema: "mysql",
				TableName:   "user",
			},
			&config.Table{
				TableSchema: "mysql",
				TableName:   "proc",
			},
			&config.Table{
				TableSchema: "mysql",
				TableName:   "func",
			},
		)
		e.replicateDoDb = append(e.replicateDoDb, db_mysql)
	}*/

	return nil
}

// initiateStreaming begins treaming of binary log events and registers listeners for such events
func (e *ExtractorOracle) initiateStreaming() error {
	e.wg.Add(1)
	go func() {
		e.wg.Done()
		e.logger.Info("Beginning streaming")
		err := e.StreamEvents()
		if err != nil {
			e.onError(common.TaskStateDead, err)
		}
	}()

	return nil
}

func (e *ExtractorOracle) initDBConnections() {
	oracleDB, err := config.NewDB(e.mysqlContext.OracleConfig)
	if err != nil {
		e.onError(common.TaskStateDead, err)
	}
	e.oracleDB = oracleDB
}

type TimestampContext struct {
	stopCh chan struct{}
	// Do not pass 0 to the chan.
	TimestampCh    chan uint32
	logger         g.LoggerType
	emptyQueueFunc func() bool
	delay          int64
}

func NewTimestampContext(stopCh chan struct{}, logger g.LoggerType, emptyQueueFunc func() bool) *TimestampContext {
	return &TimestampContext{
		stopCh:         stopCh,
		logger:         logger,
		emptyQueueFunc: emptyQueueFunc,

		TimestampCh: make(chan uint32, 16),
	}
}

// StreamEvents will begin streaming events. It will be blocking, so should be
// executed by a goroutine
func (e *ExtractorOracle) StreamEvents() error {
	e.wg.Add(1)
	go func() {
		defer func() {
			e.wg.Done()
			e.logger.Debug("StreamEvents goroutine exited")
		}()
		entries := common.DataEntries{}
		entriesSize := 0
		sendEntriesAndClear := func() error {
			var gno int64 = 0
			if len(entries.Entries) > 0 {
				theEntries := entries.Entries[0]
				// gno = theEntries.Coordinates.GNO
				if theEntries.Events != nil && len(theEntries.Events) > 0 {
					//e.timestampCtx.TimestampCh <- theEntries.Events[0].Timestamp
				}
			}

			txMsg, err := common.Encode(&entries)
			if err != nil {
				return err
			}
			e.logger.Debug("publish.before", "gno", gno, "n", len(entries.Entries))
			if err = e.publish(fmt.Sprintf("%s_incr_hete", e.subject), txMsg, gno); err != nil {
				return err
			}

			for _, entry := range entries.Entries {
				atomic.AddInt64(e.memory2, -int64(entry.Size()))
			}

			e.logger.Debug("publish.after", "gno", gno, "n", len(entries.Entries))
			entries.Entries = nil
			entriesSize = 0

			return nil
		}

		groupTimeoutDuration := time.Duration(e.mysqlContext.GroupTimeout) * time.Millisecond
		timer := time.NewTimer(groupTimeoutDuration)
		defer timer.Stop()

	LOOP:
		for !e.shutdown {
			select {
			case entryCtx := <-e.dataChannel:
				binlogEntry := entryCtx.Entry

				entries.Entries = append(entries.Entries, binlogEntry)
				entriesSize += entryCtx.OriginalSize

				if entriesSize >= e.mysqlContext.GroupMaxSize {
					e.logger.Debug("incr. send by GroupLimit",
						"entriesSize", entriesSize,
						"groupMaxSize", e.mysqlContext.GroupMaxSize,
						"Entries.len", len(entries.Entries))

					e.sendBySizeFullCounter += 1

					err := sendEntriesAndClear()
					if err != nil {
						e.onError(common.TaskStateDead, err)
						break LOOP
					}
					if !timer.Stop() {
						<-timer.C
					}
					timer.Reset(groupTimeoutDuration)
				}

			case <-timer.C:
				nEntries := len(entries.Entries)
				if nEntries > 0 {
					e.logger.Debug("incr. send by timeout.", "entriesSize", entriesSize,
						"timeout", e.mysqlContext.GroupTimeout)
					e.sendByTimeoutCounter += 1
					err := sendEntriesAndClear()
					if err != nil {
						e.onError(common.TaskStateDead, err)
						break LOOP
					}
				}
				timer.Reset(groupTimeoutDuration)
			}
			e.mysqlContext.Stage = common.StageSendingBinlogEventToSlave
			atomic.AddInt64(&e.mysqlContext.DeltaEstimate, 1)
		} // end for keepGoing && !e.shutdown
	}()
	// The next should block and execute forever, unless there's a serious error
	if err := e.DataStreamEvents(e.dataChannel); err != nil {
		if e.shutdown {
			return nil
		}
		return fmt.Errorf("StreamEvents encountered unexpected error: %+v", err)
	}
	return nil
}

// retryOperation attempts up to `count` attempts at running given function,
// exiting as soon as it returns with non-error.
// gno: only for logging
func (e *ExtractorOracle) publish(subject string, txMsg []byte, gno int64) (err error) {
	msgLen := len(txMsg)

	data := txMsg
	lenData := len(data)

	// lenData < NatsMaxMsg: 1 msg
	// lenData = k * NatsMaxMsg + b, where k >= 1 && b >= 0: (k+1) msg
	// b could be 0. we send a zero-len msg as a sign of termination.
	nSeg := lenData/g.NatsMaxMsg + 1
	e.logger.Debug("publish. msg", "subject", subject, "gno", gno, "nSeg", nSeg, "spanLen", lenData, "msgLen", msgLen)
	bak := make([]byte, 4)
	if nSeg > 1 {
		// ensure there are 4 bytes to save iSeg
		data = append(data, 0, 0, 0, 0)
	}
	for iSeg := 0; iSeg < nSeg; iSeg++ {
		var part []byte
		if nSeg == 1 { // not big msg
			part = data
		} else {
			begin := iSeg * g.NatsMaxMsg
			end := mathutil.Min(lenData, (iSeg+1)*g.NatsMaxMsg)
			// use extra 4 bytes to save iSeg
			if iSeg > 0 {
				copy(data[begin:begin+4], bak)
			}
			copy(bak, data[end:end+4])
			part = data[begin : end+4]
			binary.LittleEndian.PutUint32(data[end:], uint32(iSeg))
		}

		e.logger.Debug("publish", "subject", subject, "gno", gno, "partLen", len(part), "iSeg", iSeg)
		_, err := e.natsConn.Request(subject, part, 24*time.Hour)
		if err != nil {
			e.logger.Error("unexpected error on publish", "err", err)
			return err
		}
	}
	return nil
}

func (e *ExtractorOracle) onError(state int, err error) {
	e.logger.Error("onError", "err", err)
	if e.shutdown {
		return
	}
	e.waitCh <- &drivers.ExitResult{
		ExitCode:  state,
		Signal:    0,
		OOMKilled: false,
		Err:       err,
	}
	_ = e.Shutdown()
}

func (e *ExtractorOracle) sendFullComplete() (err error) {
	dumpMsg, err := common.Encode(&common.DumpStatResult{
		Coord: e.initialBinlogCoordinates,
	})
	if err != nil {
		return err
	}
	if err := e.publish(fmt.Sprintf("%s_full_complete", e.subject), dumpMsg, 0); err != nil {
		return err
	}
	return nil
}

func (e *ExtractorOracle) CheckAndApplyLowerCaseTableNames() {
	if e.lowerCaseTableNames != mysqlconfig.LowerCaseTableNames0 {
		lowerConfigItem := func(configItem []*common.DataSource) {
			for _, d := range configItem {
				g.LowerString(&d.TableSchemaRename)
				g.LowerString(&d.TableSchemaRegex)
				g.LowerString(&d.TableSchemaRename)
				for _, table := range d.Tables {
					g.LowerString(&table.TableName)
					g.LowerString(&table.TableRegex)
					g.LowerString(&table.TableRename)
				}
			}
		}
		lowerConfigItem(e.mysqlContext.ReplicateDoDb)
		lowerConfigItem(e.mysqlContext.ReplicateIgnoreDb)
	}
}
