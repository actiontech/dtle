package mysql

import (
	"context"
	gosql "database/sql"
	"encoding/binary"
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
	"sync"
	"sync/atomic"
	"time"
)

const (
	RowsEventFlagEndOfStatement     uint16 = 1
	RowsEventFlagNoForeignKeyChecks uint16 = 2
	RowsEventFlagNoUniqueKeyChecks  uint16 = 4
	RowsEventFlagRowHasAColumns     uint16 = 8

	Q_FLAGS2_CODE               byte = 0x0  // 4
	Q_SQL_MODE_CODE             byte = 0x01 // 8
	Q_CATALOG                   byte = 0x02 // 1 + n + 1
	Q_AUTO_INCREMENT            byte = 0x03 // 2 + 2
	Q_CHARSET_CODE              byte = 0x04 // 2 + 2 + 2
	Q_TIME_ZONE_CODE            byte = 0x05 // 1 + n
	Q_CATALOG_NZ_CODE           byte = 0x06 // 1 + n
	Q_LC_TIME_NAMES_CODE        byte = 0x07 // 2
	Q_CHARSET_DATABASE_CODE     byte = 0x08 // 2
	Q_TABLE_MAP_FOR_UPDATE_CODE byte = 0x09 // 8
	Q_MASTER_DATA_WRITTEN_CODE  byte = 0x0a // 4
	Q_INVOKERS                  byte = 0x0b // 1 + n + 1 + n
	Q_UPDATED_DB_NAMES          byte = 0x0c // 1 + n*nul-term-string
	Q_MICROSECONDS              byte = 0x0d // 3

	OPTION_AUTO_IS_NULL          uint32 = 0x00004000
	OPTION_NOT_AUTOCOMMIT        uint32 = 0x00080000
	OPTION_NO_FOREIGN_KEY_CHECKS uint32 = 0x04000000
	OPTION_RELAXED_UNIQUE_CHECKS uint32 = 0x08000000

	querySetFKChecksOff = "set @@session.foreign_key_checks = 0"
	querySetFKChecksOn  = "set @@session.foreign_key_checks = 1"
)

type ApplierIncr struct {
	logger       g.LoggerType
	subject      string
	mysqlContext *common.MySQLDriverConfig

	incrBytesQueue   chan []byte
	binlogEntryQueue chan *common.BinlogEntry
	// only TX can be executed should be put into this chan
	applyBinlogMtsTxQueue chan *common.BinlogEntryContext

	mtsManager *MtsManager
	wsManager  *WritesetManager

	db              *gosql.DB
	dbs             []*sql.Conn
	MySQLServerUuid string

	ctx        context.Context
	shutdownCh chan struct{}

	memory2          *int64
	printTps         bool
	txLastNSeconds   uint32
	appliedTxCount   uint32
	timestampCtx     *TimestampContext
	TotalDeltaCopied int64

	gtidSet           *gomysql.MysqlGTIDSet
	gtidSetLock       *sync.RWMutex
	gtidItemMap       base.GtidItemMap
	EntryExecutedHook func(entry *common.BinlogEntry)

	tableItems mapSchemaTableItems

	OnError func(int, error)

	prevDDL             bool
	replayingBinlogFile string

	wg                    sync.WaitGroup
	SkipGtidExecutedTable bool
	ForeignKeyChecks      bool
}

func NewApplierIncr(ctx context.Context, subject string, mysqlContext *common.MySQLDriverConfig,
	logger g.LoggerType, gtidSet *gomysql.MysqlGTIDSet, memory2 *int64,
	db *gosql.DB, dbs []*sql.Conn, shutdownCh chan struct{},
	gtidSetLock *sync.RWMutex) (*ApplierIncr, error) {

	a := &ApplierIncr{
		ctx:                   ctx,
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
		printTps:              g.EnvIsTrue(g.ENV_PRINT_TPS),
		gtidSet:               gtidSet,
		gtidSetLock:           gtidSetLock,
		tableItems:            make(mapSchemaTableItems),
		ForeignKeyChecks:      true,
	}

	if g.EnvIsTrue(g.ENV_SKIP_GTID_EXECUTED_TABLE) {
		a.SkipGtidExecutedTable = true
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

	t := time.NewTicker(pingInterval)
	defer t.Stop()
	hasEntry := false
	for keepLoop {
		select {
		case <-a.shutdownCh:
			keepLoop = false
		case entryContext := <-a.applyBinlogMtsTxQueue:
			hasEntry = true
			logger.Debug("a binlogEntry MTS dequeue", "gno", entryContext.Entry.Coordinates.GNO)
			if err := a.ApplyBinlogEvent(workerIndex, entryContext); err != nil {
				a.OnError(common.TaskStateDead, err) // TODO coordinate with other goroutine
				keepLoop = false
			} else {
				// do nothing
			}
			logger.Debug("after ApplyBinlogEvent.", "gno", entryContext.Entry.Coordinates.GNO)
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

func (a *ApplierIncr) handleEntry(entryCtx *common.BinlogEntryContext) (err error) {
	binlogEntry := entryCtx.Entry
	a.logger.Debug("a binlogEntry.", "remaining", len(a.incrBytesQueue),
		"gno", binlogEntry.Coordinates.GNO, "lc", binlogEntry.Coordinates.LastCommitted,
		"seq", binlogEntry.Coordinates.SeqenceNumber)

	if binlogEntry.Coordinates.OSID == a.MySQLServerUuid {
		a.logger.Debug("skipping a dtle tx.", "osid", binlogEntry.Coordinates.OSID)
		a.EntryExecutedHook(binlogEntry) // make gtid continuous
		return nil
	}
	txSid := binlogEntry.Coordinates.GetSid()

	// Note: the gtidExecuted will be updated after commit. For a big-tx, we determine
	// whether to skip for each parts.

	// region TestIfExecuted
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
		if binlogEntry.Index == 0 {
			if rotated {
				a.logger.Debug("binlog rotated", "file", a.replayingBinlogFile)
				if !a.mtsManager.WaitForAllCommitted() {
					return nil // TODO shutdown
				}
				a.mtsManager.lastCommitted = 0
				a.mtsManager.lastEnqueue = 0
				a.wsManager.resetCommonParent(0)
				nPending := len(a.mtsManager.m)
				if nPending != 0 {
					a.logger.Warn("DTLE_BUG: lcPendingTx should be 0", "nPending", nPending,
						"file", a.replayingBinlogFile, "gno", binlogEntry.Coordinates.GNO)
				}
			}

			// If there are TXs skipped by udup source-side
			if a.mtsManager.lastEnqueue+1 < binlogEntry.Coordinates.SeqenceNumber {
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

			if binlogEntry.IsPartOfBigTx() {
				if !a.mtsManager.WaitForAllCommitted() {
					return nil // shutdown
				}
				a.wsManager.resetCommonParent(binlogEntry.Coordinates.SeqenceNumber)
			}
		}

		err = a.setTableItemForBinlogEntry(entryCtx)
		if err != nil {
			return err
		}

		if !binlogEntry.IsPartOfBigTx() && !a.mysqlContext.UseMySQLDependency {
			newLC := a.wsManager.GatLastCommit(entryCtx)
			binlogEntry.Coordinates.LastCommitted = newLC
			a.logger.Debug("WritesetManager", "lc", newLC, "seq", binlogEntry.Coordinates.SeqenceNumber,
				"gno", binlogEntry.Coordinates.GNO)
		}

		if binlogEntry.IsPartOfBigTx() {
			err = a.ApplyBinlogEvent(0, entryCtx)
			if err != nil {
				return err
			}
		} else {
			if !a.mtsManager.WaitForExecution(binlogEntry) {
				return nil // shutdown
			}
			a.logger.Debug("a binlogEntry MTS enqueue.", "gno", binlogEntry.Coordinates.GNO)
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

			binlogEntries := &common.BinlogEntries{}
			if err := common.Decode(bs, binlogEntries); err != nil {
				a.OnError(common.TaskStateDead, err)
				return
			}

			for _, entry := range binlogEntries.Entries {
				a.binlogEntryQueue <- entry
				a.logger.Debug("")
				atomic.AddInt64(a.memory2, int64(entry.Size()))
			}

		case <-t.C:
			if !hasEntry {
				a.logger.Debug("no binlogEntry for 10s")
			}
			hasEntry = false
		}
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
			stmts[workerIdx], err = a.dbs[workerIdx].Db.PrepareContext(a.ctx, query)
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
	if dbApplier.Tx == nil {
		dbApplier.Tx, err = dbApplier.Db.BeginTx(a.ctx, &gosql.TxOptions{})
		if err != nil {
			return err
		}
	}
	defer func() {
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
				a.logger.Debug("execQuery", "query", query)
				_, err = dbApplier.Tx.ExecContext(a.ctx, query)
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

			flag := ParseQueryEventFlags(event.Flags)
			if flag.NoForeignKeyChecks && a.ForeignKeyChecks {
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

			if flag.NoForeignKeyChecks && a.ForeignKeyChecks {
				err = execQuery(querySetFKChecksOn)
				if err != nil {
					return err
				}
			}
		default:
			flag := binary.LittleEndian.Uint16(event.Flags)
			noFKCheckFlag := flag & RowsEventFlagNoForeignKeyChecks != 0
			if noFKCheckFlag && a.ForeignKeyChecks {
				_, err = a.dbs[workerIdx].Db.ExecContext(a.ctx, querySetFKChecksOff)
				if err != nil {
					return errors.Wrap(err, "querySetFKChecksOff")
				}
			}
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
				r, err = stmt.ExecContext(a.ctx, args...)
			} else {
				r, err = a.dbs[workerIdx].Db.ExecContext(a.ctx, query, args...)
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

			if noFKCheckFlag && a.ForeignKeyChecks {
				_, err = a.dbs[workerIdx].Db.ExecContext(a.ctx, querySetFKChecksOn)
				if err != nil {
					return errors.Wrap(err, "querySetFKChecksOn")
				}
			}
		}
		timestamp = event.Timestamp
	}

	if binlogEntry.Final {
		if !a.SkipGtidExecutedTable {
			logger.Debug("insert gno", "gno", binlogEntry.Coordinates.GNO)
			_, err = dbApplier.PsInsertExecutedGtid.ExecContext(a.ctx,
			a.subject, uuid.UUID(binlogEntry.Coordinates.SID).Bytes(), binlogEntry.Coordinates.GNO)
			if err != nil {
				return errors.Wrap(err, "insert gno")
			}
		}

		if err := dbApplier.Tx.Commit(); err != nil {
			return errors.Wrap(err, "dbApplier.Tx.Commit")
		} else {
			a.mtsManager.Executed(binlogEntry)
		}
		dbApplier.Tx = nil
		if a.printTps {
			atomic.AddUint32(&a.txLastNSeconds, 1)
		}
		atomic.AddUint32(&a.appliedTxCount, 1)
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

type QueryEventFlags struct {
	NoForeignKeyChecks bool
}

func ParseQueryEventFlags(bs []byte) (r QueryEventFlags) {
	for i := 0; i < len(bs); {
		flag := bs[i]
		i += 1
		switch flag {
		case Q_FLAGS2_CODE: // Q_FLAGS2_CODE
			v := binary.LittleEndian.Uint32(bs[i:i+4])
			i += 4
			if v & OPTION_AUTO_IS_NULL != 0 {
				//log.Printf("OPTION_AUTO_IS_NULL")
			}
			if v & OPTION_NOT_AUTOCOMMIT != 0 {
				//log.Printf("OPTION_NOT_AUTOCOMMIT")
			}
			if v & OPTION_NO_FOREIGN_KEY_CHECKS != 0 {
				r.NoForeignKeyChecks = true
			}
			if v & OPTION_RELAXED_UNIQUE_CHECKS != 0 {
				//log.Printf("OPTION_RELAXED_UNIQUE_CHECKS")
			}
		case Q_SQL_MODE_CODE:
			_ = binary.LittleEndian.Uint64(bs[i:i+8])
			i += 8
		case Q_CATALOG:
			n := int(bs[i])
			i += 1
			i += n
			i += 1 // TODO What does 'only written length > 0' mean?
		case Q_AUTO_INCREMENT:
			i += 2 + 2
		case Q_CHARSET_CODE:
			i += 2 + 2 + 2
		case Q_TIME_ZONE_CODE:
			n := int(bs[i])
			i += 1
			i += n
		case Q_CATALOG_NZ_CODE:
			length := int(bs[i])
			i += 1
			_ = string(bs[i:i+length])
		case Q_LC_TIME_NAMES_CODE:
			i += 2
		case Q_CHARSET_DATABASE_CODE:
			i += 2
		case Q_TABLE_MAP_FOR_UPDATE_CODE:
			i += 8
		case Q_MASTER_DATA_WRITTEN_CODE:
			i += 4
		case Q_INVOKERS:
			n := int(bs[i])
			i += 1
			_ = string(bs[i:i+n])
			i += n
			n = int(bs[i])
			_ = string(bs[i:i+n])
		case Q_UPDATED_DB_NAMES:
			count := int(bs[i])
			i += 1
			for j := 0; j < count; j++ {
				i0 := i
				for bs[i] != 0 {
					i += 1
				}
				_ = string(bs[i0:i])
				i += 1 // nul-terminated
			}
		case Q_MICROSECONDS:
			i += 3
		}
	}

	return r
}
