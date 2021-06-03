package mysql

import (
	"context"
	gosql "database/sql"
	"fmt"
	"github.com/actiontech/dtle/drivers/mysql/common"
	"github.com/actiontech/dtle/drivers/mysql/mysql/base"
	"github.com/actiontech/dtle/drivers/mysql/mysql/mysqlconfig"
	sql "github.com/actiontech/dtle/drivers/mysql/mysql/sql"
	"github.com/actiontech/dtle/g"
	"github.com/hashicorp/go-hclog"
	"github.com/pkg/errors"
	"github.com/satori/go.uuid"
	gomysql "github.com/siddontang/go-mysql/mysql"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type ApplierIncr struct {
	logger       hclog.Logger
	subject      string
	mysqlContext *common.MySQLDriverConfig

	incrBytesQueue   chan []byte
	binlogEntryQueue chan *common.BinlogEntry
	// only TX can be executed should be put into this chan
	applyBinlogMtsTxQueue chan *common.BinlogEntryContext

	mtsManager *MtsManager

	db              *gosql.DB
	dbs             []*sql.Conn
	MySQLServerUuid string

	shutdownCh chan struct{}

	memory2          *int64
	printTps         bool
	txLastNSeconds   uint32
	appliedTxCount   uint32
	timestampCtx     *TimestampContext
	TotalDeltaCopied int64

	gtidSet        *gomysql.MysqlGTIDSet
	gtidSetLock    *sync.RWMutex
	gtidItemMap    base.GtidItemMap
	GtidUpdateHook func(*common.BinlogCoordinateTx)

	tableItems mapSchemaTableItems

	OnError func(int, error)

	prevDDL             bool
	replayingBinlogFile string

	wg sync.WaitGroup
}

func NewApplierIncr(subject string, mysqlContext *common.MySQLDriverConfig,
	logger hclog.Logger, gtidSet *gomysql.MysqlGTIDSet, memory2 *int64,
	db *gosql.DB, dbs []*sql.Conn, shutdownCh chan struct{},
	gtidSetLock *sync.RWMutex) (*ApplierIncr, error) {

	a := &ApplierIncr{
		logger:                logger,
		subject:               subject,
		mysqlContext:          mysqlContext,
		incrBytesQueue:        make(chan []byte, mysqlContext.ReplChanBufferSize),
		binlogEntryQueue:      make(chan *common.BinlogEntry, mysqlContext.ReplChanBufferSize * 2),
		applyBinlogMtsTxQueue: make(chan *common.BinlogEntryContext, mysqlContext.ReplChanBufferSize * 2),
		db:                    db,
		dbs:                   dbs,
		shutdownCh:            shutdownCh,
		memory2:               memory2,
		printTps:              os.Getenv(g.ENV_PRINT_TPS) != "",
		gtidSet:               gtidSet,
		gtidSetLock:           gtidSetLock,
		tableItems:            make(mapSchemaTableItems),
	}

	a.timestampCtx = NewTimestampContext(a.shutdownCh, a.logger, func() bool {
		return len(a.binlogEntryQueue) == 0 && len(a.applyBinlogMtsTxQueue) == 0
		// TODO need a more reliable method to determine queue.empty.
	})

	a.mtsManager = NewMtsManager(a.shutdownCh)
	go a.mtsManager.LcUpdater()

	return a, nil
}

func (a *ApplierIncr) Run() (err error) {
	a.logger.Debug("Run. GetServerUUID. before")
	a.MySQLServerUuid, err = sql.GetServerUUID(a.db)
	if err != nil {
		return err
	}

	err = (&GtidExecutedCreater{
		db:     a.db,
		logger: a.logger,
	}).createTableGtidExecutedV4()
	if err != nil {
		return err
	}
	a.logger.Debug("after createTableGtidExecutedV4")

	for i := range a.dbs {
		a.dbs[i].PsDeleteExecutedGtid, err = a.dbs[i].Db.PrepareContext(context.Background(),
			fmt.Sprintf("delete from %v.%v where job_name = ? and source_uuid = ?",
				g.DtleSchemaName, g.GtidExecutedTableV4))
		if err != nil {
			return err
		}
		a.dbs[i].PsInsertExecutedGtid, err = a.dbs[i].Db.PrepareContext(context.Background(),
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
func (a *ApplierIncr) MtsWorker(workerIndex int) {
	keepLoop := true

	logger := a.logger.With("worker", workerIndex)
	for keepLoop {
		timer := time.NewTimer(pingInterval)
		select {
		case <-a.shutdownCh:
			keepLoop = false
		case entryContext := <-a.applyBinlogMtsTxQueue:
			logger.Debug("a binlogEntry MTS dequeue", "gno", entryContext.Entry.Coordinates.GNO)
			if err := a.ApplyBinlogEvent(workerIndex, entryContext); err != nil {
				a.OnError(TaskStateDead, err) // TODO coordinate with other goroutine
				keepLoop = false
			} else {
				// do nothing
			}
			logger.Debug("after ApplyBinlogEvent.", "gno", entryContext.Entry.Coordinates.GNO)
		case <-timer.C:
			err := a.dbs[workerIndex].Db.PingContext(context.Background())
			if err != nil {
				logger.Error("bad connection for mts worker.", "err", err, "index", workerIndex)
				a.OnError(TaskStateDead, errors.Wrap(err, "mts worker"))
				keepLoop = false
			}
		}
		timer.Stop()
	}
}

func (a *ApplierIncr) handleEntry(entryCtx *common.BinlogEntryContext) (err error) {
	binlogEntry := entryCtx.Entry
	a.logger.Debug("a binlogEntry.", "remaining", len(a.incrBytesQueue),
		"gno", binlogEntry.Coordinates.GNO, "lc", binlogEntry.Coordinates.LastCommitted,
		"seq", binlogEntry.Coordinates.SeqenceNumber)

	if binlogEntry.Coordinates.OSID == a.MySQLServerUuid {
		a.logger.Debug("skipping a dtle tx.", "osid", binlogEntry.Coordinates.OSID)
		a.GtidUpdateHook(&binlogEntry.Coordinates) // make gtid continuous
		return nil
	}
	// region TestIfExecuted
	txSid := binlogEntry.Coordinates.GetSid()

	gtidSetItem := a.gtidItemMap.GetItem(binlogEntry.Coordinates.SID)
	txExecuted := func() bool {
		a.gtidSetLock.RLock()
		defer a.gtidSetLock.RUnlock()
		intervals := base.GetIntervals(a.gtidSet, txSid)
		return base.IntervalSlicesContainOne(intervals, binlogEntry.Coordinates.GNO)
	}()
	if txExecuted {
		a.logger.Info("skip an executed tx", "sid", txSid, "gno", binlogEntry.Coordinates.GNO)
		return nil
	}
	// endregion
	// this must be after duplication check
	var rotated bool
	if a.replayingBinlogFile == binlogEntry.Coordinates.LogFile {
		rotated = false
	} else {
		rotated = true
		a.replayingBinlogFile = binlogEntry.Coordinates.LogFile
	}

	a.logger.Debug("gtidSetItem", "NRow", gtidSetItem.NRow)
	if gtidSetItem.NRow >= cleanupGtidExecutedLimit {
		err = a.cleanGtidExecuted(binlogEntry.Coordinates.SID, txSid)
		if err != nil {
			return err
		}
		gtidSetItem.NRow = 1
	}

	gtidSetItem.NRow += 1
	if binlogEntry.Coordinates.SeqenceNumber == 0 {
		// MySQL 5.6: non mts
		err := a.setTableItemForBinlogEntry(entryCtx)
		if err != nil {
			return err
		}
		if err := a.ApplyBinlogEvent(0, entryCtx); err != nil {
			return err
		}
	} else {
		if rotated {
			a.logger.Debug("binlog rotated", "file", a.replayingBinlogFile)
			if !a.mtsManager.WaitForAllCommitted() {
				return nil // TODO shutdown
			}
			a.mtsManager.lastCommitted = 0
			a.mtsManager.lastEnqueue = 0
			nPending := len(a.mtsManager.m)
			if nPending != 0 {
				a.logger.Warn("DTLE_BUG: lcPendingTx should be 0", "nPending", nPending,
					"file", a.replayingBinlogFile, "gno", binlogEntry.Coordinates.GNO)
			}
		}
		// If there are TXs skipped by udup source-side
		if a.mtsManager.lastEnqueue + 1 < binlogEntry.Coordinates.SeqenceNumber {
			a.logger.Info("found skipping seq_num",
				"lastEnqueue", a.mtsManager.lastEnqueue, "seqNum", binlogEntry.Coordinates.SeqenceNumber,
				"uuid", txSid, "gno", binlogEntry.Coordinates.GNO)
		}
		for a.mtsManager.lastEnqueue+1 < binlogEntry.Coordinates.SeqenceNumber {
			a.mtsManager.lastEnqueue += 1
			a.mtsManager.chExecuted <- a.mtsManager.lastEnqueue
		}
		hasDDL := binlogEntry.HasDDL()
		// DDL must be executed separatedly
		if hasDDL || a.prevDDL {
			a.logger.Debug("MTS found DDL. WaitForAllCommitted",
				"gno", binlogEntry.Coordinates.GNO, "hasDDL", hasDDL, "prevDDL", a.prevDDL)
			if !a.mtsManager.WaitForAllCommitted() {
				return nil // shutdown
			}
		}
		a.prevDDL = hasDDL

		if !a.mtsManager.WaitForExecution(binlogEntry) {
			return nil // shutdown
		}
		a.logger.Debug("a binlogEntry MTS enqueue.", "gno", binlogEntry.Coordinates.GNO)
		err = a.setTableItemForBinlogEntry(entryCtx)
		if err != nil {
			return err
		}
		a.applyBinlogMtsTxQueue <- entryCtx
	}
	return nil
}

func (a *ApplierIncr) heterogeneousReplay() {
	a.wg.Add(1)
	defer a.wg.Done()

	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		for {
			select {
			case <-a.shutdownCh:
				return
			case entry := <-a.binlogEntryQueue:
				err := a.handleEntry(&common.BinlogEntryContext{
					Entry:       entry,
					TableItems:  nil,
				})
				if err != nil {
					a.OnError(TaskStateDead, err)
					return
				}
				atomic.AddInt64(&a.mysqlContext.DeltaEstimate, 1)
			}
		}
	}()

	t := time.NewTimer(10 * time.Second)
	for {
		select {
		case <-a.shutdownCh:
			return

		case bs := <-a.incrBytesQueue:
			if !t.Stop() {
				<-t.C
			}

			atomic.AddInt64(a.memory2, -int64(len(bs)))

			binlogEntries := &common.BinlogEntries{}
			if err := common.Decode(bs, binlogEntries); err != nil {
				a.OnError(TaskStateDead, err)
				return
			}

			for _, entry := range binlogEntries.Entries {
				a.binlogEntryQueue <- entry
			}

		case <-t.C:
			a.logger.Debug("no binlogEntry for 10s")
		}
		t.Reset(10 * time.Second)
	}
}

// buildDMLEventQuery creates a query to operate on the ghost table, based on an intercepted binlog
// event entry on the original table.
func (a *ApplierIncr) buildDMLEventQuery(dmlEvent common.DataEvent, workerIdx int,
	tableItem *common.ApplierTableItem) (stmt *gosql.Stmt, query string, args []interface{}, rowsDelta int64, err error) {
	// Large piece of code deleted here. See git annotate.
	var tableColumns = tableItem.Columns
	doPrepareIfNil := func(stmts []*gosql.Stmt, query string) (*gosql.Stmt, error) {
		var err error
		if stmts[workerIdx] == nil {
			a.logger.Debug("buildDMLEventQuery prepare query", "query", query)
			stmts[workerIdx], err = a.dbs[workerIdx].Db.PrepareContext(context.Background(), query)
			if err != nil {
				a.logger.Error("buildDMLEventQuery prepare query", "query", query, "err", err)
			}
		}
		return stmts[workerIdx], err
	}

	switch dmlEvent.DML {
	case common.DeleteDML:
		{
			query, uniqueKeyArgs, hasUK, err := sql.BuildDMLDeleteQuery(dmlEvent.DatabaseName, dmlEvent.TableName, tableColumns, dmlEvent.WhereColumnValues.GetAbstractValues())
			if err != nil {
				return nil, "", nil, -1, err
			}
			if hasUK {
				stmt, err := doPrepareIfNil(tableItem.PsDelete, query)
				if err != nil {
					return nil, "", nil, -1, err
				}
				return stmt, "", uniqueKeyArgs, -1, nil
			} else {
				return nil, query, uniqueKeyArgs, -1, nil
			}
		}
	case common.InsertDML:
		{
			// TODO no need to generate query string every time
			query, sharedArgs, err := sql.BuildDMLInsertQuery(dmlEvent.DatabaseName, dmlEvent.TableName, tableColumns, tableColumns, tableColumns, dmlEvent.NewColumnValues.GetAbstractValues())
			if err != nil {
				return nil, "", nil, -1, err
			}
			stmt, err := doPrepareIfNil(tableItem.PsInsert, query)
			if err != nil {
				return nil, "", nil, -1, err
			}
			return stmt, "", sharedArgs, 1, err
		}
	case common.UpdateDML:
		{
			query, sharedArgs, uniqueKeyArgs, hasUK, err := sql.BuildDMLUpdateQuery(dmlEvent.DatabaseName, dmlEvent.TableName, tableColumns, tableColumns, tableColumns, tableColumns, dmlEvent.NewColumnValues.GetAbstractValues(), dmlEvent.WhereColumnValues.GetAbstractValues())
			if err != nil {
				return nil, "", nil, -1, err
			}
			args = append(args, sharedArgs...)
			args = append(args, uniqueKeyArgs...)

			if hasUK {
				stmt, err := doPrepareIfNil(tableItem.PsUpdate, query)
				if err != nil {
					return nil, "", nil, -1, err
				}

				return stmt, "", args, 0, err
			} else {
				return nil, query, args, 0, err
			}
		}
	}
	return nil, "", args, 0, fmt.Errorf("Unknown dml event type: %+v", dmlEvent.DML)
}

// ApplyEventQueries applies multiple DML queries onto the dest table
func (a *ApplierIncr) ApplyBinlogEvent(workerIdx int, binlogEntryCtx *common.BinlogEntryContext) error {
	logger := a.logger.Named("ApplyBinlogEvent")
	binlogEntry := binlogEntryCtx.Entry

	dbApplier := a.dbs[workerIdx]

	var totalDelta int64
	var err error
	var timestamp uint32
	txSid := binlogEntry.Coordinates.GetSid()

	dbApplier.DbMutex.Lock()
	tx, err := dbApplier.Db.BeginTx(context.Background(), &gosql.TxOptions{})
	if err != nil {
		return err
	}
	defer func() {
		if err := tx.Commit(); err != nil {
			a.OnError(TaskStateDead, err)
		} else {
			a.mtsManager.Executed(binlogEntry)
			a.GtidUpdateHook(&binlogEntry.Coordinates)
		}
		if a.printTps {
			atomic.AddUint32(&a.txLastNSeconds, 1)
		}
		atomic.AddUint32(&a.appliedTxCount, 1)

		dbApplier.DbMutex.Unlock()
		atomic.AddInt64(a.memory2, -int64(binlogEntry.Size()))
	}()
	for i, event := range binlogEntry.Events {
		logger.Debug("binlogEntry.Events", "gno", binlogEntry.Coordinates.GNO, "event", i)
		switch event.DML {
		case common.NotDML:
			var err error
			logger.Debug("not dml", "query", event.Query)

			execQuery := func(query string) error {
				_, err = tx.Exec(query)
				if err != nil {
					errCtx := errors.Wrapf(err, "tx.Exec. gno %v iEvent %v queryBegin %v workerIdx %v",
						binlogEntry.Coordinates.GNO, i, g.StrLim(query, 10), workerIdx)
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

			if event.CurrentSchema != "" {
				query := fmt.Sprintf("USE %s", mysqlconfig.EscapeName(event.CurrentSchema))
				logger.Debug("use", "query", query)
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

			err = execQuery(event.Query)
			if err != nil {
				return err
			}
			logger.Debug("Exec.after", "query", event.Query)
		default:
			logger.Debug("a dml event")
			stmt, query, args, rowDelta, err := a.buildDMLEventQuery(event, workerIdx,
				binlogEntryCtx.TableItems[i])
			if err != nil {
				logger.Error("buildDMLEventQuery error", "err", err)
				return err
			}

			logger.Debug("buildDMLEventQuery.after", "nArgs", len(args))

			var r gosql.Result
			if stmt != nil {
				r, err = stmt.Exec(args...)
			} else {
				r, err = a.dbs[workerIdx].Db.ExecContext(context.Background(), query, args...)
			}

			if err != nil {
				logger.Error("error at exec", "gtid", hclog.Fmt("%s:%d", txSid, binlogEntry.Coordinates.GNO),
					"err", err)
				return err
			}
			nr, err := r.RowsAffected()
			if err != nil {
				logger.Error("RowsAffected error", "gno", binlogEntry.Coordinates.GNO, "event", i, "err", err)
			} else {
				logger.Debug("RowsAffected.after", "gno", binlogEntry.Coordinates.GNO, "event", i, "nr", nr)
			}
			totalDelta += rowDelta
		}
		timestamp = event.Timestamp
	}

	logger.Debug("insert gno", "gno", binlogEntry.Coordinates.GNO)
	_, err = dbApplier.PsInsertExecutedGtid.Exec(a.subject, uuid.UUID(binlogEntry.Coordinates.SID).Bytes(), binlogEntry.Coordinates.GNO)
	if err != nil {
		return errors.Wrap(err, "insert gno")
	}

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
		schemaItem[table] = tableItem
	}

	return tableItem
}

type mapSchemaTableItems map[string](map[string](*common.ApplierTableItem))

func (a *ApplierIncr) setTableItemForBinlogEntry(binlogEntry *common.BinlogEntryContext) error {
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
