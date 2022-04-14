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

	"github.com/actiontech/dtle/drivers/mysql/common"
	"github.com/actiontech/dtle/drivers/mysql/mysql/base"
	"github.com/actiontech/dtle/drivers/mysql/mysql/mysqlconfig"
	sql "github.com/actiontech/dtle/drivers/mysql/mysql/sql"
	"github.com/actiontech/dtle/g"
	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/hashicorp/go-hclog"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
)

const (
	querySetFKChecksOff = "set @@session.foreign_key_checks = 0"
	querySetFKChecksOn  = "set @@session.foreign_key_checks = 1"
)

type ApplierIncr struct {
	logger       g.LoggerType
	subject      string
	mysqlContext *common.MySQLDriverConfig

	incrBytesQueue   chan []byte
	binlogEntryQueue chan *common.DataEntry
	// only TX can be executed should be put into this chan
	applyBinlogMtsTxQueue chan *common.EntryContext

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
	EntryExecutedHook func(entry *common.DataEntry)

	tableItems mapSchemaTableItems

	OnError func(int, error)

	prevDDL             bool
	replayingBinlogFile string

	wg                    sync.WaitGroup
	SkipGtidExecutedTable bool
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
		binlogEntryQueue:      make(chan *common.DataEntry, mysqlContext.ReplChanBufferSize * 2),
		applyBinlogMtsTxQueue: make(chan *common.EntryContext, mysqlContext.ReplChanBufferSize * 2),
		db:                    db,
		dbs:                   dbs,
		shutdownCh:            shutdownCh,
		memory2:               memory2,
		printTps:              g.EnvIsTrue(g.ENV_PRINT_TPS),
		gtidSet:               gtidSet,
		gtidSetLock:           gtidSetLock,
		tableItems:            make(mapSchemaTableItems),
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
			logger.Debug("a binlogEntry MTS dequeue", "gno", entryContext.Entry.Coordinates.GetGNO())
			if err := a.ApplyBinlogEvent(workerIndex, entryContext); err != nil {
				a.OnError(common.TaskStateDead, err) // TODO coordinate with other goroutine
				keepLoop = false
			} else {
				// do nothing
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

	if binlogEntry.Coordinates.GetSid() == uuid.UUID([16]byte{0}) {
		return a.handleEntryOracle(entryCtx)
	}
	
	a.logger.Debug("a binlogEntry.", "remaining", len(a.incrBytesQueue),
		"gno", binlogEntry.Coordinates.GetGNO(), "lc", binlogEntry.Coordinates.GetLastCommit(),
		"seq", binlogEntry.Coordinates.GetSequenceNumber())

	if binlogEntry.Coordinates.GetOSID() == a.MySQLServerUuid {
		a.logger.Debug("skipping a dtle tx.", "osid", binlogEntry.Coordinates.GetOSID())
		a.EntryExecutedHook(binlogEntry) // make gtid continuous
		return nil
	}
	txSid := binlogEntry.Coordinates.GetSid()

	// Note: the gtidExecuted will be updated after commit. For a big-tx, we determine
	// whether to skip for each parts.

	// region TestIfExecuted

	gtidSetItem := a.gtidItemMap.GetItem(binlogEntry.Coordinates.GetSid().(uuid.UUID))
	txExecuted := func() bool {
		a.gtidSetLock.RLock()
		defer a.gtidSetLock.RUnlock()
		intervals := base.GetIntervals(a.gtidSet, txSid.(uuid.UUID).String())
		return base.IntervalSlicesContainOne(intervals, binlogEntry.Coordinates.GetGNO())
	}()
	if txExecuted {
		a.logger.Info("skip an executed tx", "sid", txSid, "gno", binlogEntry.Coordinates.GetGNO())
		return nil
	}
	// endregion
	// this must be after duplication check
	var rotated bool
	if a.replayingBinlogFile == binlogEntry.Coordinates.GetLogFile() {
		rotated = false
	} else {
		rotated = true
		a.replayingBinlogFile = binlogEntry.Coordinates.GetLogFile()
	}

	a.logger.Debug("gtidSetItem", "NRow", gtidSetItem.NRow)
	if gtidSetItem.NRow >= cleanupGtidExecutedLimit {
		err = a.cleanGtidExecuted(binlogEntry.Coordinates.GetSid().(uuid.UUID), txSid.(string))
		if err != nil {
			return err
		}
		gtidSetItem.NRow = 1
	}

	gtidSetItem.NRow += 1
	if binlogEntry.Coordinates.GetSequenceNumber() == 0 {
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
				a.logger.Debug("binlog rotated. WaitForAllCommitted before", "file", a.replayingBinlogFile)
				if !a.mtsManager.WaitForAllCommitted() {
					return nil // TODO shutdown
				}
				a.logger.Debug("binlog rotated. WaitForAllCommitted after", "file", a.replayingBinlogFile)
				a.mtsManager.lastCommitted = 0
				a.mtsManager.lastEnqueue = 0
				a.wsManager.resetCommonParent(0)
				nPending := len(a.mtsManager.m)
				if nPending != 0 {
					a.logger.Warn("DTLE_BUG: lcPendingTx should be 0", "nPending", nPending,
						"file", a.replayingBinlogFile, "gno", binlogEntry.Coordinates.GetGNO())
				}
			}

			// If there are TXs skipped by udup source-side
			if a.mtsManager.lastEnqueue+1 < binlogEntry.Coordinates.GetSequenceNumber() {
				a.logger.Info("found skipping seq_num",
					"lastEnqueue", a.mtsManager.lastEnqueue, "seqNum", binlogEntry.Coordinates.GetSequenceNumber(),
					"uuid", txSid, "gno", binlogEntry.Coordinates.GetGNO())
			}
			for a.mtsManager.lastEnqueue+1 < binlogEntry.Coordinates.GetSequenceNumber() {
				a.mtsManager.lastEnqueue += 1
				a.mtsManager.chExecuted <- a.mtsManager.lastEnqueue
			}

			hasDDL := binlogEntry.HasDDL()
			// DDL must be executed separatedly
			if hasDDL || a.prevDDL {
				a.logger.Debug("MTS found DDL. WaitForAllCommitted",
					"gno", binlogEntry.Coordinates.GetGNO(), "hasDDL", hasDDL, "prevDDL", a.prevDDL)
				if !a.mtsManager.WaitForAllCommitted() {
					return nil // shutdown
				}
			}
			a.prevDDL = hasDDL

			if binlogEntry.IsPartOfBigTx() {
				if !a.mtsManager.WaitForAllCommitted() {
					return nil // shutdown
				}
				a.wsManager.resetCommonParent(binlogEntry.Coordinates.GetSequenceNumber())
			}
		}

		err = a.setTableItemForBinlogEntry(entryCtx)
		if err != nil {
			return err
		}

		if !binlogEntry.IsPartOfBigTx() && !a.mysqlContext.UseMySQLDependency {
			newLC := a.wsManager.GatLastCommit(entryCtx, a.logger)
			binlogEntry.Coordinates.(*common.MySQLCoordinateTx).LastCommitted = newLC
			a.logger.Debug("WritesetManager", "lc", newLC, "seq", binlogEntry.Coordinates.GetSequenceNumber(),
				"gno", binlogEntry.Coordinates.GetGNO())
		}

		if binlogEntry.IsPartOfBigTx() {
			if binlogEntry.Index == 0 {
				a.mtsManager.lastEnqueue = binlogEntry.Coordinates.GetSequenceNumber()
			}
			err = a.ApplyBinlogEvent(0, entryCtx)
			if err != nil {
				return err
			}
		} else {
			if !a.mtsManager.WaitForExecution(binlogEntry) {
				return nil // shutdown
			}
			a.logger.Debug("a binlogEntry MTS enqueue.", "gno", binlogEntry.Coordinates.GetGNO())
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
					Entry:       entry,
					TableItems:  nil,
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
			query, uniqueKeyArgs, hasUK, err := sql.BuildDMLDeleteQuery(dmlEvent.DatabaseName, dmlEvent.TableName, tableColumns, dmlEvent.Rows[0])
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
			query, sharedArgs, err := sql.BuildDMLInsertQuery(dmlEvent.DatabaseName, dmlEvent.TableName, tableColumns, tableColumns, tableColumns, dmlEvent.Rows[0])
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
			query, sharedArgs, uniqueKeyArgs, hasUK, err := sql.BuildDMLUpdateQuery(dmlEvent.DatabaseName, dmlEvent.TableName, tableColumns, tableColumns, tableColumns, tableColumns, dmlEvent.Rows[1], dmlEvent.Rows[0])
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
func (a *ApplierIncr) ApplyBinlogEvent(workerIdx int, binlogEntryCtx *common.EntryContext) error {
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
		logger.Debug("binlogEntry.Events", "gno", binlogEntry.Coordinates.GetGNO(), "event", i)
		switch event.DML {
		case common.NotDML:
			var err error
			logger.Debug("not dml", "query", event.Query)

			execQuery := func(query string) error {
				a.logger.Debug("execQuery", "query", query)
				_, err = dbApplier.Tx.ExecContext(a.ctx, query)
				if err != nil {
					errCtx := errors.Wrapf(err, "tx.Exec. gno %v iEvent %v queryBegin %v workerIdx %v",
						binlogEntry.Coordinates.GetGNO(), i, g.StrLim(query, 10), workerIdx)
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

			if event.DtleFlags & common.DtleFlagCreateSchemaIfNotExists != 0 {
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
		default: // is DML
			flag := uint16(0)
			if len(event.Flags) > 0 {
				flag = binary.LittleEndian.Uint16(event.Flags)
			} else {
				// Oracle
			}
			noFKCheckFlag := flag &common.RowsEventFlagNoForeignKeyChecks != 0
			if noFKCheckFlag && a.mysqlContext.ForeignKeyChecks {
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
				logger.Error("error at exec", "gtid", hclog.Fmt("%s:%d", txSid, binlogEntry.Coordinates.GetGNO()),
					"err", err)
				return err
			}

			nr, err := r.RowsAffected()
			if err != nil {
				logger.Error("RowsAffected error", "gno", binlogEntry.Coordinates.GetGNO(), "event", i, "err", err)
			} else {
				logger.Debug("RowsAffected.after", "gno", binlogEntry.Coordinates.GetGNO(), "event", i, "nr", nr)
			}
			totalDelta += rowDelta

			if noFKCheckFlag && a.mysqlContext.ForeignKeyChecks {
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
			logger.Debug("insert gno", "gno", binlogEntry.Coordinates.GetGNO())
			_, err = dbApplier.PsInsertExecutedGtid.ExecContext(a.ctx,
			a.subject, binlogEntry.Coordinates.GetSid().(uuid.UUID).Bytes(), binlogEntry.Coordinates.GetGNO())
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

