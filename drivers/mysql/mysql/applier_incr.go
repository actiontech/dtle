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
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/satori/go.uuid"
	gomysql "github.com/siddontang/go-mysql/mysql"
	"os"
	"sync/atomic"
	"time"
)

type ApplierIncr struct {
	logger hclog.Logger
	subject string
	mysqlContext *common.MySQLDriverConfig

	applyDataEntryQueue chan *common.BinlogEntryContext
	// only TX can be executed should be put into this chan
	applyBinlogMtsTxQueue chan *common.BinlogEntryContext

	mtsManager *MtsManager

	db     *gosql.DB
	dbs    []*sql.Conn
	MySQLServerUuid   string

	shutdownCh chan struct{}

	memory2    *int64
	printTps       bool
	txLastNSeconds uint32
	timestampCtx       *TimestampContext
	TotalDeltaCopied  int64

	gtidSet        *gomysql.MysqlGTIDSet
	gtidItemMap    base.GtidItemMap
	GtidUpdateHook func(*common.BinlogCoordinateTx)

	tableItems  mapSchemaTableItems

	OnError func(int, error)
}

func NewApplierIncr(subject string, mysqlContext *common.MySQLDriverConfig,
	logger hclog.Logger, gtidSet *gomysql.MysqlGTIDSet, memory2 *int64,
	db *gosql.DB, dbs []*sql.Conn, shutdownCh chan struct{}) (*ApplierIncr, error) {

	a := &ApplierIncr{
		logger:                logger,
		subject:               subject,
		mysqlContext:          mysqlContext,
		applyDataEntryQueue:   make(chan *common.BinlogEntryContext, mysqlContext.ReplChanBufferSize * 2),
		applyBinlogMtsTxQueue: make(chan *common.BinlogEntryContext, mysqlContext.ReplChanBufferSize * 2),
		db:                    db,
		dbs:                   dbs,
		shutdownCh:            shutdownCh,
		memory2:               memory2,
		printTps:              os.Getenv(g.ENV_PRINT_TPS) != "",
		gtidSet:               gtidSet,
		tableItems:            make(mapSchemaTableItems),
	}

	a.timestampCtx = NewTimestampContext(a.shutdownCh, a.logger, func() bool {
		return len(a.applyDataEntryQueue) == 0 && len(a.applyBinlogMtsTxQueue) == 0
		// TODO need a more reliable method to determine queue.empty.
	})

	a.mtsManager = NewMtsManager(a.shutdownCh)
	go a.mtsManager.LcUpdater()

	return a, nil
}

func (a *ApplierIncr) Run() (err error) {
	a.logger.Debug("beging connetion mysql 4 validate  serverid")
	if err := a.validateServerUUID(); err != nil {
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
		case entryContext := <-a.applyBinlogMtsTxQueue:
			logger.Debug("a binlogEntry MTS dequeue", "gno", entryContext.Entry.Coordinates.GNO)
			if err := a.ApplyBinlogEvent(nil, workerIndex, entryContext); err != nil {
				a.OnError(TaskStateDead, err) // TODO coordinate with other goroutine
				keepLoop = false
			} else {
				// do nothing
			}
			logger.Debug("after ApplyBinlogEvent.", "gno", entryContext.Entry.Coordinates.GNO)
		case <-a.shutdownCh:
			keepLoop = false
		case <-timer.C:
			err := a.dbs[workerIndex].Db.PingContext(context.Background())
			if err != nil {
				logger.Error("bad connection for mts worker.", "err", err)
			}
		}
		timer.Stop()
	}
}

func (a *ApplierIncr) heterogeneousReplay() {
	var err error
	stopSomeLoop := false
	prevDDL := false
	var ctx context.Context

	replayingBinlogFile := ""

	for !stopSomeLoop {
		select {
		case entryCtx := <-a.applyDataEntryQueue:
			if nil == entryCtx {
				continue
			}
			binlogEntry := entryCtx.Entry
			spanContext := entryCtx.SpanContext
			span := opentracing.GlobalTracer().StartSpan("dest use binlogEntry  ", opentracing.FollowsFrom(spanContext))
			ctx = opentracing.ContextWithSpan(ctx, span)
			a.logger.Debug("a binlogEntry.", "remaining", len(a.applyDataEntryQueue),
				"gno", binlogEntry.Coordinates.GNO, "lc", binlogEntry.Coordinates.LastCommitted,
				"seq", binlogEntry.Coordinates.SeqenceNumber)

			if binlogEntry.Coordinates.OSID == a.MySQLServerUuid {
				a.logger.Debug("skipping a dtle tx.", "osid", binlogEntry.Coordinates.OSID)
				continue
			}
			// region TestIfExecuted
			txSid := binlogEntry.Coordinates.GetSid()

			gtidSetItem := a.gtidItemMap.GetItem(binlogEntry.Coordinates.SID)
			intervals := base.GetIntervals(a.gtidSet, txSid)
			if base.IntervalSlicesContainOne(intervals, binlogEntry.Coordinates.GNO) {
				// entry executed
				a.logger.Debug("skip an executed tx", "sid", txSid, "gno", binlogEntry.Coordinates.GNO)
				continue
			}
			// endregion
			// this must be after duplication check
			var rotated bool
			if replayingBinlogFile == binlogEntry.Coordinates.LogFile {
				rotated = false
			} else {
				rotated = true
				replayingBinlogFile = binlogEntry.Coordinates.LogFile
			}

			a.logger.Debug("gtidSetItem", "NRow", gtidSetItem.NRow)
			if gtidSetItem.NRow >= cleanupGtidExecutedLimit {
				err = a.cleanGtidExecuted(binlogEntry.Coordinates.SID, base.StringInterval(intervals))
				if err != nil {
					a.OnError(TaskStateDead, err)
					return
				}
				gtidSetItem.NRow = 1
			}

			gtidSetItem.NRow += 1
			if binlogEntry.Coordinates.SeqenceNumber == 0 {
				// MySQL 5.6: non mts
				err := a.setTableItemForBinlogEntry(entryCtx)
				if err != nil {
					a.OnError(TaskStateDead, err)
					return
				}
				if err := a.ApplyBinlogEvent(ctx, 0, entryCtx); err != nil {
					a.OnError(TaskStateDead, err)
					return
				}
			} else {
				if rotated {
					a.logger.Debug("binlog rotated", "file", replayingBinlogFile)
					if !a.mtsManager.WaitForAllCommitted() {
						return // shutdown
					}
					a.mtsManager.lastCommitted = 0
					a.mtsManager.lastEnqueue = 0
					if len(a.mtsManager.m) != 0 {
						a.logger.Warn("DTLE_BUG: len(a.mtsManager.m) should be 0")
					}
				}
				// If there are TXs skipped by udup source-side
				for a.mtsManager.lastEnqueue+1 < binlogEntry.Coordinates.SeqenceNumber {
					a.mtsManager.lastEnqueue += 1
					a.mtsManager.chExecuted <- a.mtsManager.lastEnqueue
				}
				hasDDL := func() bool {
					for i := range binlogEntry.Events {
						dmlEvent := &binlogEntry.Events[i]
						switch dmlEvent.DML {
						case common.NotDML:
							return true
						default:
						}
					}
					return false
				}()
				// DDL must be executed separatedly
				if hasDDL || prevDDL {
					a.logger.Debug("MTS found DDL. WaitForAllCommitted",
						"gno", binlogEntry.Coordinates.GNO, "hasDDL", hasDDL, "prevDDL", prevDDL)
					if !a.mtsManager.WaitForAllCommitted() {
						return // shutdown
					}
				}
				if hasDDL {
					prevDDL = true
				} else {
					prevDDL = false
				}

				if !a.mtsManager.WaitForExecution(binlogEntry) {
					return // shutdown
				}
				a.logger.Debug("a binlogEntry MTS enqueue.", "gno", binlogEntry.Coordinates.GNO)
				err = a.setTableItemForBinlogEntry(entryCtx)
				if err != nil {
					a.OnError(TaskStateDead, err)
					return
				}
				entryCtx.SpanContext = span.Context()
				a.applyBinlogMtsTxQueue <- entryCtx
			}
			span.Finish()
		case <-time.After(10 * time.Second):
			a.logger.Debug("no binlogEntry for 10s")
		case <-a.shutdownCh:
			stopSomeLoop = true
		}
	}
}

// buildDMLEventQuery creates a query to operate on the ghost table, based on an intercepted binlog
// event entry on the original table.
func (a *ApplierIncr) buildDMLEventQuery(dmlEvent common.DataEvent, workerIdx int, spanContext opentracing.SpanContext,
	tableItem *common.ApplierTableItem) (stmt *gosql.Stmt, query string, args []interface{}, rowsDelta int64, err error) {
	// Large piece of code deleted here. See git annotate.
	var tableColumns = tableItem.Columns
	span := opentracing.GlobalTracer().StartSpan("desc  buildDMLEventQuery ", opentracing.FollowsFrom(spanContext))
	defer span.Finish()
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
func (a *ApplierIncr) ApplyBinlogEvent(ctx context.Context, workerIdx int, binlogEntryCtx *common.BinlogEntryContext) error {
	logger := a.logger.Named("ApplyBinlogEvent")
	binlogEntry := binlogEntryCtx.Entry

	dbApplier := a.dbs[workerIdx]

	var totalDelta int64
	var err error
	var spanContext opentracing.SpanContext
	var span opentracing.Span
	var timestamp uint32
	if ctx != nil {
		spanContext = opentracing.SpanFromContext(ctx).Context()
		span = opentracing.GlobalTracer().StartSpan(" desc single binlogEvent transform to sql ",
			opentracing.ChildOf(spanContext))
		span.SetTag("start insert sql ", time.Now().UnixNano()/1e6)
		defer span.Finish()

	} else {
		spanContext = binlogEntryCtx.SpanContext
		span = opentracing.GlobalTracer().StartSpan("desc mts binlogEvent transform to sql ",
			opentracing.ChildOf(spanContext))
		span.SetTag("start insert sql ", time.Now().UnixNano()/1e6)
		defer span.Finish()
		spanContext = span.Context()
	}
	txSid := binlogEntry.Coordinates.GetSid()

	dbApplier.DbMutex.Lock()
	tx, err := dbApplier.Db.BeginTx(context.Background(), &gosql.TxOptions{})
	if err != nil {
		return err
	}
	defer func() {
		span.SetTag("begin commit sql ", time.Now().UnixNano()/1e6)
		if err := tx.Commit(); err != nil {
			a.OnError(TaskStateDead, err)
		} else {
			a.mtsManager.Executed(binlogEntry)
			a.GtidUpdateHook(&binlogEntry.Coordinates)
		}
		if a.printTps {
			atomic.AddUint32(&a.txLastNSeconds, 1)
		}
		span.SetTag("after  commit sql ", time.Now().UnixNano()/1e6)

		dbApplier.DbMutex.Unlock()
		atomic.AddInt64(a.memory2, -int64(binlogEntry.Size()))
	}()
	span.SetTag("begin transform binlogEvent to sql time  ", time.Now().UnixNano()/1e6)
	for i, event := range binlogEntry.Events {
		logger.Debug("binlogEntry.Events", "gno", binlogEntry.Coordinates.GNO, "event", i)
		switch event.DML {
		case common.NotDML:
			var err error
			logger.Debug("not dml", "query", event.Query)

			if event.CurrentSchema != "" {
				query := fmt.Sprintf("USE %s", mysqlconfig.EscapeName(event.CurrentSchema))
				logger.Debug("use", "query", query)
				_, err = tx.Exec(query)
				if err != nil {
					if !sql.IgnoreError(err) {
						logger.Error("Exec sql error", "err", err)
						return err
					} else {
						logger.Warn("Ignore error", "err", err)
					}
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

			_, err = tx.Exec(event.Query)
			if err != nil {
				if !sql.IgnoreError(err) {
					logger.Error("Exec sql error", "err", err)
					return err
				} else {
					logger.Warn("Ignore error", "err", err)
				}
			}
			logger.Debug("Exec.after", "query", event.Query)
		default:
			logger.Debug("a dml event")
			stmt, query, args, rowDelta, err := a.buildDMLEventQuery(event, workerIdx, spanContext,
				binlogEntryCtx.TableItems[i])
			if err != nil {
				logger.Error("buildDMLEventQuery error", "err", err)
				return err
			}

			logger.Debug("buildDMLEventQuery.after", "args", args)

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

	span.SetTag("after  transform  binlogEvent to sql  ", time.Now().UnixNano()/1e6)
	logger.Debug("insert gno", "gno", binlogEntry.Coordinates.GNO)
	_, err = dbApplier.PsInsertExecutedGtid.Exec(a.subject, uuid.UUID(binlogEntry.Coordinates.SID).Bytes(), binlogEntry.Coordinates.GNO)
	if err != nil {
		return errors.Wrap(err, "insert gno")
	}

	// no error
	a.mysqlContext.Stage = common.StageWaitingForGtidToBeCommitted
	atomic.AddInt64(&a.TotalDeltaCopied, 1)
	logger.Debug("event delay time", "timestamp", timestamp)
	a.timestampCtx.TimestampCh <- timestamp
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
					a.logger.Error("GetTableColumns error.", "err", err)
					return err
				}
				err = base.ApplyColumnTypes(a.db, dmlEvent.DatabaseName, dmlEvent.TableName, tableItem.Columns)
				if err != nil {
					a.logger.Error("ApplyColumnTypes error.", "err", err)
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

func (a *ApplierIncr) validateServerUUID() error {
	query := `SELECT @@SERVER_UUID`
	if err := a.db.QueryRow(query).Scan(&a.MySQLServerUuid); err != nil {
		return err
	}
	return nil
}

func (a *ApplierIncr) AddEvent(entryContext *common.BinlogEntryContext) {
	a.applyDataEntryQueue <- entryContext
}
