package mysql

import (
	"context"
	gosql "database/sql"
	"encoding/binary"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/actiontech/dtle/driver/common"
	"github.com/actiontech/dtle/driver/mysql/base"
	"github.com/actiontech/dtle/driver/mysql/mysqlconfig"
	sql "github.com/actiontech/dtle/driver/mysql/sql"
	"github.com/actiontech/dtle/g"
	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
)

const (
	querySetFKChecksOff = "set @@session.foreign_key_checks = 0 /*dtle*/"
	querySetFKChecksOn  = "set @@session.foreign_key_checks = 1 /*dtle*/"
)

type ApplierIncr struct {
	logger       g.LoggerType
	subject      string
	mysqlContext *common.MySQLDriverConfig

	incrBytesQueue   chan []byte
	binlogEntryQueue chan *common.DataEntry
	// only TX can be executed should be put into this chan
	applyBinlogMtsTxQueue chan *common.EntryContext

	db              *gosql.DB
	dbs             []*sql.Conn
	MySQLServerUuid string

	ctx        context.Context
	shutdownCh chan struct{}

	memory2           *int64
	printTps          bool
	txLastNSeconds    uint32
	appliedTxCount    uint32
	appliedQueryCount uint64
	timestampCtx      *TimestampContext
	TotalDeltaCopied  int64

	EntryExecutedHook func(entry *common.DataEntry)

	tableItems mapSchemaTableItems

	OnError func(int, error)

	prevDDL             bool
	replayingBinlogFile string

	wg                    sync.WaitGroup
	SkipGtidExecutedTable bool
	logTxCommit           bool
	noBigTxDMLPipe        bool

	mtsManager  *MtsManager
	wsManager   *WritesetManager
	gtidSet     *gomysql.MysqlGTIDSet
	gtidSetLock *sync.RWMutex
	gtidItemMap base.GtidItemMap
	sourceType  string
	tableSpecs  []*common.TableSpec

	inBigTx         bool
	bigTxEventQueue chan *dmlExecItem
	bigTxEventWg    sync.WaitGroup

	fwdExtractor *Extractor
}

func NewApplierIncr(applier *Applier, sourcetype string) (*ApplierIncr, error) {
	driverContext := applier.mysqlContext
	a := &ApplierIncr{
		ctx:                   applier.ctx,
		logger:                applier.logger,
		subject:               applier.subject,
		mysqlContext:          driverContext,
		incrBytesQueue:        make(chan []byte, driverContext.ReplChanBufferSize),
		binlogEntryQueue:      make(chan *common.DataEntry, driverContext.ReplChanBufferSize*2),
		applyBinlogMtsTxQueue: make(chan *common.EntryContext, driverContext.ReplChanBufferSize*2),
		db:                    applier.db,
		dbs:                   applier.dbs,
		shutdownCh:            applier.shutdownCh,
		memory2:               applier.memory2,
		printTps:              g.EnvIsTrue(g.ENV_PRINT_TPS),
		gtidSet:               applier.gtidSet,
		gtidSetLock:           applier.gtidSetLock,
		tableItems:            make(mapSchemaTableItems),
		sourceType:            sourcetype,
		bigTxEventQueue:       make(chan *dmlExecItem, 16),
	}

	if g.EnvIsTrue(g.ENV_SKIP_GTID_EXECUTED_TABLE) {
		a.SkipGtidExecutedTable = true
	}

	if g.EnvIsTrue(g.ENV_DTLE_LOG_TX_COMMIT) {
		a.logTxCommit = true
	}
	if g.EnvIsTrue(g.ENV_DTLE_NO_BIG_TX_DML_PIPE) {
		a.logger.Info("found DTLE_NO_BIG_TX_DML_PIPE")
		a.noBigTxDMLPipe = true
	}

	a.timestampCtx = NewTimestampContext(a.shutdownCh, a.logger, func() bool {
		return len(a.binlogEntryQueue) == 0 && len(a.applyBinlogMtsTxQueue) == 0
		// TODO need a more reliable method to determine queue.empty.
	})

	a.mtsManager = NewMtsManager(a.shutdownCh, a.logger)
	a.wsManager = NewWritesetManager(a.mysqlContext.DependencyHistorySize)

	go a.mtsManager.LcUpdater()

	return a, nil
}

func (a *ApplierIncr) Run() (err error) {
	a.logger.Debug("Run. GetServerUUID. before")
	a.MySQLServerUuid, err = sql.GetServerUUID(a.db)
	if err != nil {
		return err
	}
	a.logger.Info("GetServerUUID", "uuid", a.MySQLServerUuid)

	if a.sourceType == "mysql" {
		err = (&GtidExecutedCreater{
			db:     a.db,
			logger: a.logger,
		}).createTableGtidExecutedV4()
		if err != nil {
			return err
		}
		a.logger.Debug("after createTableGtidExecutedV4")

		for i := range a.dbs {
			a.dbs[i].PsDeleteExecutedGtid, err = a.dbs[i].Db.PrepareContext(a.ctx,
				fmt.Sprintf("delete from %v.%v where job_name = ? and hex(source_uuid) = ?",
					g.DtleSchemaName, g.GtidExecutedTableV4))
			if err != nil {
				return err
			}
			a.dbs[i].PsInsertExecutedGtid, err = a.dbs[i].Db.PrepareContext(a.ctx,
				fmt.Sprintf("replace into %v.%v (job_name,source_uuid,gtid,gtid_set) values (?, ?, ?, null)",
					g.DtleSchemaName, g.GtidExecutedTableV4))
			if err != nil {
				return err
			}

		}
		a.logger.Debug("after prepare stmt for gtid_executed table")

		a.gtidItemMap, err = SelectAllGtidExecuted(a.db, a.subject, a.gtidSet)
		if err != nil {
			return err
		}

		a.logger.Debug("after SelectAllGtidExecuted")
	}

	for i := 0; i < a.mysqlContext.ParallelWorkers; i++ {
		go a.MtsWorker(i)
	}

	go a.timestampCtx.Handle()

	go a.heterogeneousReplay()

	if a.printTps {
		go func() {
			for {
				select {
				case <-a.shutdownCh:
					return
				default:
					// keep loop
				}
				time.Sleep(5 * time.Second)
				n := atomic.SwapUint32(&a.txLastNSeconds, 0)
				a.logger.Info("txLastNSeconds", "n", n)
			}
		}()
	}

	return nil
}

func (a *ApplierIncr) bigTxQueueExecutor() {
	for {
		item := <-a.bigTxEventQueue
		if item == nil {
			// chan closed in Shutdown()
			break
		}

		if !a.HasShutdown() {
			err := a.prepareIfNilAndExecute(item, 0)
			if err != nil {
				a.OnError(common.TaskStateDead, err)
			}
		}
		a.bigTxEventWg.Done()
	}
}

func (a *ApplierIncr) MtsWorker(workerIndex int) {
	keepLoop := true

	logger := a.logger.With("worker", workerIndex)

	if workerIndex == 0 {
		go a.bigTxQueueExecutor()
	}

	t := time.NewTicker(pingInterval)
	defer t.Stop()
	hasEntry := false
	for keepLoop {
		select {
		case <-a.shutdownCh:
			keepLoop = false
		case entryContext := <-a.applyBinlogMtsTxQueue:
			hasEntry = true
			logger.Debug("a binlogEntry MTS dequeue", "gno", entryContext.Entry.Coordinates.GetGNO())
			for iTry := 0; ; iTry++ {
				err := a.ApplyBinlogEvent(workerIndex, entryContext)
				if errIsMysqlDeadlock(err) && iTry < a.mysqlContext.RetryTxLimit {
					logger.Info("found deadlock. will retry tx", "gno", entryContext.Entry.Coordinates.GetGNO(),
						"iTry", iTry)
					time.Sleep(retryTxDelay)
					continue
				} else if err != nil {
					a.OnError(common.TaskStateDead, err) // TODO coordinate with other goroutine
					keepLoop = false
				}
				break
			}
			logger.Debug("after ApplyBinlogEvent.", "gno", entryContext.Entry.Coordinates.GetGNO())
		case <-t.C:
			if !hasEntry {
				err := a.dbs[workerIndex].Db.PingContext(a.ctx)
				if err != nil {
					logger.Error("bad connection for mts worker.", "err", err, "index", workerIndex)
					a.OnError(common.TaskStateDead, errors.Wrap(err, "mts worker"))
					keepLoop = false
				}
			}
			hasEntry = false
		}
	}
}

func (a *ApplierIncr) handleEntry(entryCtx *common.EntryContext) (err error) {
	binlogEntry := entryCtx.Entry
	txGno := binlogEntry.Coordinates.GetGNO()
	a.logger.Debug("a binlogEntry.", "remaining", len(a.incrBytesQueue), "gno", txGno,
		"lc", binlogEntry.Coordinates.GetLastCommit(), "seq", binlogEntry.Coordinates.GetSequenceNumber(),
		"index", binlogEntry.Index, "final", binlogEntry.Final)

	if binlogEntry.Coordinates.GetSid() == uuid.UUID([16]byte{0}) {
		return a.handleEntryOracle(entryCtx)
	}

	isBig := binlogEntry.IsPartOfBigTx()
	txSid := binlogEntry.Coordinates.GetSidStr()

	if a.inBigTx && binlogEntry.Index == 0 {
		a.logger.Info("bigtx: found resent BinlogEntry", "gno", txGno)
		// src is resending an earlier BinlogEntry
		_, err = a.dbs[0].Db.ExecContext(a.ctx, "rollback")
		if err != nil {
			return errors.Wrapf(err, "rollback on resent big tx")
		}
		a.mtsManager.lastEnqueue = 0
		a.inBigTx = false
	}

	// region TestIfExecuted
	skipEntry := false
	if txSid == a.MySQLServerUuid {
		a.logger.Debug("skipping a binlogEntry with the same sid as target.", "sid", txSid)
		skipEntry = true
	} else if a.fwdExtractor != nil {
		if a.fwdExtractor.binlogReader != nil {
			if base.GtidSetContains(&a.fwdExtractor.binlogReader.CurrentGtidSetMutex,
				a.fwdExtractor.binlogReader.CurrentGtidSet, txSid, txGno) {
				skipEntry = true
				a.logger.Info("skip an fwd executed tx", "sid", txSid, "gno", txGno)
			}
		}
	} else if base.GtidSetContains(a.gtidSetLock, a.gtidSet, txSid, txGno) {
		a.logger.Info("skip an executed tx", "sid", txSid, "gno", txGno)
		skipEntry = true
	}

	// Note: the gtidExecuted will be updated after commit.
	// For a big-tx, we determine whether to skip for each parts.
	if skipEntry {
		a.EntryExecutedHook(binlogEntry) // make gtid continuous
		return nil
	}
	// endregion

	rotated := false
	if binlogEntry.Index == 0 {
		// this must be after duplication check
		if a.replayingBinlogFile != binlogEntry.Coordinates.GetLogFile() {
			rotated = true
			a.replayingBinlogFile = binlogEntry.Coordinates.GetLogFile()
		}

		gtidSetItem := a.gtidItemMap.GetItem(binlogEntry.Coordinates.GetSid().(uuid.UUID))
		a.logger.Debug("gtidSetItem", "NRow", gtidSetItem.NRow)
		if gtidSetItem.NRow >= cleanupGtidExecutedLimit {
			err = a.cleanGtidExecuted(binlogEntry.Coordinates.GetSid().(uuid.UUID), txSid)
			if err != nil {
				return err
			}
			gtidSetItem.NRow = 1
		}
		gtidSetItem.NRow += 1
	}

	if binlogEntry.Coordinates.GetSequenceNumber() == 0 {
		// MySQL 5.6: non mts
		if isBig {
			a.inBigTx = true
		}
		err := a.setTableItemForBinlogEntry(entryCtx)
		if err != nil {
			return err
		}
		if err := a.ApplyBinlogEvent(0, entryCtx); err != nil {
			return err
		}
	} else {
		if binlogEntry.Index == 0 {
			if rotated {
				if !a.mtsManager.WaitForAllCommitted(a.logger.With("rotate", a.replayingBinlogFile)) {
					return nil // TODO shutdown
				}
				a.mtsManager.lastCommitted = 0
				a.mtsManager.lastEnqueue = 0
				a.wsManager.resetCommonParent(0)
				nPending := len(a.mtsManager.m)
				if nPending != 0 {
					a.logger.Warn("DTLE_BUG: lcPendingTx should be 0", "nPending", nPending,
						"file", a.replayingBinlogFile, "gno", txGno)
				}
			}

			// If there are TXs skipped by udup source-side
			if a.mtsManager.lastEnqueue+1 < binlogEntry.Coordinates.GetSequenceNumber() {
				a.logger.Info("found skipping seq_num",
					"lastEnqueue", a.mtsManager.lastEnqueue, "seqNum", binlogEntry.Coordinates.GetSequenceNumber(),
					"uuid", txSid, "gno", txGno)
			}
			for a.mtsManager.lastEnqueue+1 < binlogEntry.Coordinates.GetSequenceNumber() {
				a.mtsManager.lastEnqueue += 1
				a.mtsManager.chExecuted <- a.mtsManager.lastEnqueue
			}

			hasDDL := binlogEntry.HasDDL()
			inMiddleDDL := hasDDL || a.prevDDL // DDL must be executed separatedly
			if inMiddleDDL || isBig {
				if !a.mtsManager.WaitForAllCommitted(a.logger.With("gno", txGno,
					"seq", binlogEntry.Coordinates.GetSequenceNumber(),
					"lc", binlogEntry.Coordinates.GetLastCommit(),
					"hasDDL", hasDDL, "prevDDL", a.prevDDL,
					"bigtx", isBig, "index", binlogEntry.Index)) {
					return nil // shutdown
				}
			}
			a.prevDDL = hasDDL

			if isBig {
				a.inBigTx = true
				a.wsManager.resetCommonParent(binlogEntry.Coordinates.GetSequenceNumber())
			}
		}

		err = a.setTableItemForBinlogEntry(entryCtx)
		if err != nil {
			return err
		}

		if !isBig && !a.mysqlContext.UseMySQLDependency {
			newLC := a.wsManager.GatLastCommit(entryCtx, a.logger)
			binlogEntry.Coordinates.(*common.MySQLCoordinateTx).LastCommitted = newLC
			a.logger.Debug("WritesetManager", "lc", newLC, "seq", binlogEntry.Coordinates.GetSequenceNumber(),
				"gno", txGno)
		}

		if isBig {
			if binlogEntry.Index == 0 {
				a.mtsManager.lastEnqueue = binlogEntry.Coordinates.GetSequenceNumber()
			}
			a.logger.Info("bigtx ApplyBinlogEvent", "gno", txGno, "index", binlogEntry.Index)
			err = a.ApplyBinlogEvent(0, entryCtx)
			if err != nil {
				return err
			}
		} else {
			if !a.mtsManager.WaitForExecution(binlogEntry) {
				return nil // shutdown
			}
			a.logger.Debug("a binlogEntry MTS enqueue.", "gno", txGno)
			a.applyBinlogMtsTxQueue <- entryCtx
		}
	}
	return nil
}

func (a *ApplierIncr) heterogeneousReplay() {
	a.wg.Add(1)
	defer a.wg.Done()

	a.wg.Add(1)
	go func() {
		for {
			select {
			case <-a.shutdownCh:
				a.wg.Done()
				return
			case entry := <-a.binlogEntryQueue:
				err := a.handleEntry(&common.EntryContext{
					Entry:      entry,
					TableItems: nil,
				})
				if err != nil {
					a.wg.Done()
					a.OnError(common.TaskStateDead, err)
					return
				}
				atomic.AddInt64(&a.mysqlContext.DeltaEstimate, 1)
			}
		}
	}()

	t := time.NewTicker(10 * time.Second)
	defer t.Stop()
	hasEntry := false
	for {
		select {
		case <-a.shutdownCh:
			return

		case bs := <-a.incrBytesQueue:
			atomic.AddInt64(a.memory2, -int64(len(bs)))
			hasEntry = true

			binlogEntries := &common.DataEntries{}
			if err := common.Decode(bs, binlogEntries); err != nil {
				a.OnError(common.TaskStateDead, err)
				return
			}

			for _, entry := range binlogEntries.Entries {
				select {
				case <-a.shutdownCh:
					return
				case a.binlogEntryQueue <- entry:
					atomic.AddInt64(a.memory2, int64(entry.Size()))
				}
			}

		case <-t.C:
			if !hasEntry {
				a.logger.Debug("no binlogEntry for 10s")
			}
			hasEntry = false
		}
	}
}

func (a *ApplierIncr) HasShutdown() bool {
	select {
	case <-a.shutdownCh:
		return true
	default:
		return false
	}
}
func (a *ApplierIncr) prepareIfNilAndExecute(item *dmlExecItem, workerIdx int) (err error) {
	// hasUK bool, pstmt **gosql.Stmt, query string, args []interface{}
	var r gosql.Result

	if item.hasUK {
		if *item.pstmt == nil {
			a.logger.Debug("buildDMLEventQuery prepare query", "query", item.query)
			*item.pstmt, err = a.dbs[workerIdx].Db.PrepareContext(a.ctx, item.query)
			if err != nil {
				a.logger.Error("buildDMLEventQuery prepare query", "query", item.query, "err", err)
				return err
			}
		}

		r, err = (*item.pstmt).ExecContext(a.ctx, item.args...)
	} else {
		r, err = a.dbs[workerIdx].Db.ExecContext(a.ctx, item.query, item.args...)
	}

	if err != nil {
		a.logger.Error("error at exec", "gno", item.gno, "err", err, "worker", workerIdx)
		return err
	}

	nr, err := r.RowsAffected()
	if err != nil {
		a.logger.Error("RowsAffected error", "gno", item.gno, "event", 0, "err", err)
	} else {
		a.logger.Debug("RowsAffected.after", "gno", item.gno, "event", 0, "nr", nr)
	}
	return nil
}

// ApplyEventQueries applies multiple DML queries onto the dest table
func (a *ApplierIncr) ApplyBinlogEvent(workerIdx int, binlogEntryCtx *common.EntryContext) (err error) {
	logger := a.logger.Named("ApplyBinlogEvent")
	binlogEntryCtx.Rows = 0 // count for logging
	binlogEntry := binlogEntryCtx.Entry
	defer atomic.AddInt64(a.memory2, -int64(binlogEntry.Size()))

	dbApplier := a.dbs[workerIdx]

	var timestamp uint32
	gno := binlogEntry.Coordinates.GetGNO()

	dbApplier.DbMutex.Lock()
	defer dbApplier.DbMutex.Unlock()

	// Note: gtid_next cannot be set when there is an ongoing transaction.
	if a.mysqlContext.SetGtidNext {
		_, err = dbApplier.Db.ExecContext(a.ctx, fmt.Sprintf("set gtid_next = '%v:%v' /*dtle*/",
			binlogEntry.Coordinates.GetSidStr(), gno))
		if err != nil {
			return errors.Wrap(err, "set gtid_next")
		}

		defer func() {
			if err != nil {
				return
			}
			err1 := dbApplier.SetGtidNextAutomatic(a.ctx)
			if err1 != nil {
				err = errors.Wrapf(err1, "restore gtid_next")
			}
		}()
	}

	if binlogEntry.Index == 0 && !binlogEntry.IsOneStmtDDL() {
		_, err = dbApplier.Db.ExecContext(a.ctx, "begin")
		if err != nil {
			return err
		}
	}

	execQuery := func(query string) error {
		a.logger.Debug("execQuery", "query", query)
		_, err = dbApplier.Db.ExecContext(a.ctx, query)
		if err != nil {
			errCtx := errors.Wrapf(err, "tx.Exec. gno %v queryBegin %v workerIdx %v",
				gno, g.StrLim(query, 10), workerIdx)
			if sql.IgnoreError(err) {
				logger.Warn("Ignore error", "err", errCtx)
				return nil
			} else {
				logger.Error("Exec sql error", "err", errCtx)
				return errCtx
			}
		}
		return nil
	}

	queueOrExec := func(item *dmlExecItem) error {
		// TODO check if shutdown?
		if a.inBigTx && !a.noBigTxDMLPipe {
			a.bigTxEventWg.Add(1)
			select {
			case <-a.shutdownCh:
				return fmt.Errorf("queueOrExec: ApplierIncr shutdown")
			case a.bigTxEventQueue <- item:
			}
			return nil
		} else {
			return a.prepareIfNilAndExecute(item, workerIdx)
		}
	}

	for i, event := range binlogEntry.Events {
		if a.HasShutdown() {
			break
		}
		logger.Debug("binlogEntry.Events", "gno", gno, "event", i)

		if event.DML == common.NotDML {
			var err error
			logger.Debug("not dml", "query", event.Query)

			if event.DtleFlags&common.DtleFlagCreateSchemaIfNotExists != 0 {
				// TODO CHARACTER SET & COLLATE
				query := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", mysqlconfig.EscapeName(event.DatabaseName))
				err := execQuery(query)
				if err != nil {
					return err
				}
			}

			if event.CurrentSchema != "" {
				query := fmt.Sprintf("USE %s", mysqlconfig.EscapeName(event.CurrentSchema))
				err := execQuery(query)
				if err != nil {
					return err
				}
			}

			if event.TableName != "" {
				var schema string
				if event.DatabaseName != "" {
					schema = event.DatabaseName
				} else {
					schema = event.CurrentSchema
				}
				logger.Debug("reset tableItem", "schema", schema, "table", event.TableName)
				a.getTableItem(schema, event.TableName).Reset()
			} else { // TableName == ""
				if event.DatabaseName != "" {
					if schemaItem, ok := a.tableItems[event.DatabaseName]; ok {
						for tableName, v := range schemaItem {
							logger.Debug("reset tableItem", "schema", event.DatabaseName, "table", tableName)
							v.Reset()
						}
					}
					delete(a.tableItems, event.DatabaseName)
				}
			}

			flag, err := common.ParseQueryEventFlags(event.Flags, logger)
			if err != nil {
				return err
			}
			if flag.NoForeignKeyChecks && a.mysqlContext.ForeignKeyChecks {
				err = execQuery(querySetFKChecksOff)
				if err != nil {
					return err
				}
			}

			err = execQuery(event.Query)
			if err != nil {
				return err
			}
			logger.Debug("Exec.after", "query", event.Query)

			if flag.NoForeignKeyChecks && a.mysqlContext.ForeignKeyChecks {
				err = execQuery(querySetFKChecksOn)
				if err != nil {
					return err
				}
			}
		} else {
			logger.Debug("a dml event")

			flag := uint16(0)
			if len(event.Flags) > 0 {
				flag = binary.LittleEndian.Uint16(event.Flags)
			} else {
				// Oracle
			}
			noFKCheckFlag := flag&common.RowsEventFlagNoForeignKeyChecks != 0
			if noFKCheckFlag && a.mysqlContext.ForeignKeyChecks {
				_, err = a.dbs[workerIdx].Db.ExecContext(a.ctx, querySetFKChecksOff)
				if err != nil {
					return errors.Wrap(err, "querySetFKChecksOff")
				}
			}

			tableItem := binlogEntryCtx.TableItems[i]

			switch event.DML {
			case common.InsertDML:
				nRows := len(event.Rows)
				binlogEntryCtx.Rows += nRows
				for i := 0; i < nRows; {
					var pstmt **gosql.Stmt
					var rows [][]interface{}
					if nRows-i >= a.mysqlContext.BulkInsert3 {
						pstmt = &tableItem.PsInsert3[workerIdx]
						rows = event.Rows[i : i+a.mysqlContext.BulkInsert3]
						i += a.mysqlContext.BulkInsert3
					} else if nRows-i >= a.mysqlContext.BulkInsert2 {
						pstmt = &tableItem.PsInsert2[workerIdx]
						rows = event.Rows[i : i+a.mysqlContext.BulkInsert2]
						i += a.mysqlContext.BulkInsert2
					} else if nRows-i >= a.mysqlContext.BulkInsert1 {
						pstmt = &tableItem.PsInsert1[workerIdx]
						rows = event.Rows[i : i+a.mysqlContext.BulkInsert1]
						i += a.mysqlContext.BulkInsert1
					} else {
						pstmt = &tableItem.PsInsert0[workerIdx]
						rows = event.Rows[i : i+1]
						i += 1
					}

					query, sharedArgs, err := sql.BuildDMLInsertQuery(event.DatabaseName, event.TableName,
						tableItem.Columns, tableItem.ColumnMapTo, rows, *pstmt)
					if err != nil {
						return err
					}
					a.logger.Debug("BuildDMLInsertQuery", "query", query)

					err = queueOrExec(&dmlExecItem{true, pstmt, query, sharedArgs, gno})
					if err != nil {
						return err
					}
				}
			case common.DeleteDML:
				binlogEntryCtx.Rows += len(event.Rows)
				for _, row := range event.Rows {
					pstmt := &tableItem.PsDelete[workerIdx]
					query, uniqueKeyArgs, hasUK, err := sql.BuildDMLDeleteQuery(event.DatabaseName, event.TableName,
						tableItem.Columns, tableItem.ColumnMapTo, row, *pstmt)
					if err != nil {
						return err
					}
					a.logger.Debug("BuildDMLDeleteQuery", "query", query)

					err = queueOrExec(&dmlExecItem{hasUK, pstmt, query, uniqueKeyArgs, gno})
					if err != nil {
						return err
					}
				}
			case common.UpdateDML:
				if len(event.Rows) % 2 != 0 {
					return fmt.Errorf("bad update event. row number is not 2N %v gno %v",
						len(event.Rows), gno)
				}
				binlogEntryCtx.Rows += len(event.Rows) / 2
				for i := 0; i < len(event.Rows); i += 2 {
					rowBefore := event.Rows[i]
					rowAfter  := event.Rows[i+1]

					if len(rowBefore) == 0 && len(rowAfter) == 0 {
						return fmt.Errorf("bad update event. row number is not 2N %v gno %v",
							len(event.Rows), gno)
					}

					if len(rowBefore) == 0 { // insert
						pstmt := &tableItem.PsInsert0[workerIdx]
						query, sharedArgs, err := sql.BuildDMLInsertQuery(event.DatabaseName, event.TableName,
							tableItem.Columns, tableItem.ColumnMapTo, event.Rows[i+1:i+2], *pstmt)
						if err != nil {
							return err
						}

						err = queueOrExec(&dmlExecItem{true, pstmt, query, sharedArgs, gno})
						if err != nil {
							return err
						}
					} else if len(rowAfter) == 0 { // delete
						pstmt := &tableItem.PsDelete[workerIdx]
						query, uniqueKeyArgs, hasUK, err := sql.BuildDMLDeleteQuery(event.DatabaseName, event.TableName,
							tableItem.Columns, tableItem.ColumnMapTo, rowBefore, *pstmt)
						if err != nil {
							return err
						}
						a.logger.Debug("BuildDMLDeleteQuery", "query", query)

						err = queueOrExec(&dmlExecItem{hasUK, pstmt, query, uniqueKeyArgs, gno})
						if err != nil {
							return err
						}
					} else {
						pstmt := &tableItem.PsUpdate[workerIdx]
						query, sharedArgs, uniqueKeyArgs, hasUK, err := sql.BuildDMLUpdateQuery(event.DatabaseName, event.TableName, tableItem.Columns, tableItem.ColumnMapTo, rowAfter, rowBefore, *pstmt)
						if err != nil {
							return err
						}

						var args []interface{}
						args = append(args, sharedArgs...)
						args = append(args, uniqueKeyArgs...)

						err = queueOrExec(&dmlExecItem{hasUK, pstmt, query, args, gno})
						if err != nil {
							return err
						}
					}
				}
			}

			if noFKCheckFlag && a.mysqlContext.ForeignKeyChecks {
				_, err = a.dbs[workerIdx].Db.ExecContext(a.ctx, querySetFKChecksOn)
				if err != nil {
					return errors.Wrap(err, "querySetFKChecksOn")
				}
			}
		}

		timestamp = event.Timestamp
		atomic.AddUint64(&a.appliedQueryCount, uint64(1))
	}
	if a.inBigTx && !a.noBigTxDMLPipe {
		a.logger.Info("a.bigTxEventWg.Wait before", "gno", gno, "index", binlogEntry.Index)
	}
	a.bigTxEventWg.Wait()
	if a.HasShutdown() {
		return fmt.Errorf("ApplyBinlogEvent: applier has been shutdown. gno %v", gno)
	}

	if binlogEntry.Final {
		isBigTx := binlogEntry.IsPartOfBigTx()
		if !a.SkipGtidExecutedTable && a.sourceType == "mysql" {
			if binlogEntry.IsOneStmtDDL() && a.mysqlContext.SetGtidNext {
				err1 := dbApplier.SetGtidNextAutomatic(a.ctx)
				if err1 != nil {
					err = errors.Wrapf(err1, "restore gtid_next")
				}
			}

			if a.logTxCommit || isBigTx {
				logger.Info("insert gno", "gno", gno, "bigtx", isBigTx, "index", binlogEntry.Index,
					"rows", binlogEntryCtx.Rows)
			} else {
				logger.Debug("insert gno", "gno", gno, "rows", binlogEntryCtx.Rows)
			}

			_, err = dbApplier.PsInsertExecutedGtid.ExecContext(a.ctx,
				a.subject, binlogEntry.Coordinates.GetSid().(uuid.UUID).Bytes(), gno)
			if err != nil {
				return errors.Wrap(err, "insert gno")
			}
		}

		if a.logTxCommit || isBigTx {
			logger.Info("committing tx", "gno", gno, "bigtx", isBigTx, "index", binlogEntry.Index,
				"rows", binlogEntryCtx.Rows)
		}
		if _, err := dbApplier.Db.ExecContext(a.ctx, "commit"); err != nil {
			return errors.Wrap(err, "dbApplier.Tx.Commit")
		} else {
			a.mtsManager.Executed(binlogEntry)
		}
		a.inBigTx = false
		if a.printTps {
			atomic.AddUint32(&a.txLastNSeconds, 1)
		}
		if a.logTxCommit || isBigTx {
			logger.Info("applier tx committed", "gno", gno, "bigtx", isBigTx, "index", binlogEntry.Index,
				"rows", binlogEntryCtx.Rows)
		} else {
			logger.Debug("applier tx committed", "gno", gno, "rows", binlogEntryCtx.Rows)
		}
		atomic.AddUint32(&a.appliedTxCount, 1)
	} else {
		logger.Info("uncommitted bigtx part", "gno", gno, "index", binlogEntry.Index, "rows", binlogEntryCtx.Rows)
	}
	a.EntryExecutedHook(binlogEntry)

	// no error
	a.mysqlContext.Stage = common.StageWaitingForGtidToBeCommitted
	atomic.AddInt64(&a.TotalDeltaCopied, 1)
	logger.Debug("event delay time", "timestamp", timestamp)
	if timestamp != 0 {
		a.timestampCtx.TimestampCh <- timestamp
	}
	return nil
}

func (a *ApplierIncr) getTableItem(schema string, table string) *common.ApplierTableItem {
	schemaItem, ok := a.tableItems[schema]
	if !ok {
		schemaItem = make(map[string]*common.ApplierTableItem)
		a.tableItems[schema] = schemaItem
	}

	tableItem, ok := schemaItem[table]
	if !ok {
		tableItem = common.NewApplierTableItem(a.mysqlContext.ParallelWorkers)
		for _, tableSpec := range a.tableSpecs {
			if tableSpec.Schema == schema && tableSpec.Table == table {
				tableItem.ColumnMapTo = tableSpec.ColumnMapTo
			}
		}
		schemaItem[table] = tableItem
	}

	return tableItem
}

type mapSchemaTableItems map[string](map[string](*common.ApplierTableItem))

func (a *ApplierIncr) setTableItemForBinlogEntry(binlogEntry *common.EntryContext) error {
	var err error
	binlogEntry.TableItems = make([]*common.ApplierTableItem, len(binlogEntry.Entry.Events))

	for i := range binlogEntry.Entry.Events {
		dmlEvent := &binlogEntry.Entry.Events[i]
		switch dmlEvent.DML {
		case common.NotDML:
			// do nothing
		default:
			tableItem := a.getTableItem(dmlEvent.DatabaseName, dmlEvent.TableName)
			if tableItem.Columns == nil {
				a.logger.Debug("get tableColumns", "schema", dmlEvent.DatabaseName, "table", dmlEvent.TableName)
				tableItem.Columns, err = base.GetTableColumns(a.db, dmlEvent.DatabaseName, dmlEvent.TableName)
				if err != nil {
					err = errors.Wrapf(err, "GetTableColumns. %v %v", dmlEvent.DatabaseName, dmlEvent.TableName)
					a.logger.Error(err.Error())
					return err
				}
				uk, err := base.GetCandidateUniqueKeys(a.logger, a.db, dmlEvent.DatabaseName, dmlEvent.TableName, tableItem.Columns)
				if err != nil {
					return err
				}
				tableItem.Columns.UniqueKeys = uk
				err = base.ApplyColumnTypes(a.db, dmlEvent.DatabaseName, dmlEvent.TableName, tableItem.Columns)
				if err != nil {
					err = errors.Wrapf(err, "ApplyColumnTypes. %v %v", dmlEvent.DatabaseName, dmlEvent.TableName)
					a.logger.Error(err.Error())
					return err
				}
			} else {
				a.logger.Debug("reuse tableColumns", "schema", dmlEvent.DatabaseName, "table", dmlEvent.TableName)
			}
			binlogEntry.TableItems[i] = tableItem
		}
	}
	return nil
}

func (a *ApplierIncr) handleEntryOracle(entryCtx *common.EntryContext) (err error) {
	err = a.setTableItemForBinlogEntry(entryCtx)
	if err != nil {
		return err
	}
	if err := a.ApplyBinlogEvent(0, entryCtx); err != nil {
		if os.Getenv("SkipErr") == "true" {
			a.logger.Error("skip : apply binlog event err", "err", err, "entryCtx", entryCtx)
			return nil
		}
		return err
	}
	return nil
}

func (a *ApplierIncr) Shutdown() {
	close(a.bigTxEventQueue)
	a.wg.Wait()
	a.logger.Debug("Shutdown. ApplierIncr.wg.Wait. after")
}

type dmlExecItem struct {
	hasUK bool
	pstmt **gosql.Stmt
	query string
	args []interface{}
	gno  int64 // for log only
}
