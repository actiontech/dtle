package applier

import (
	"context"
	gosql "database/sql"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/actiontech/dtle/drivers/mysql/common"
	"github.com/actiontech/dtle/drivers/mysql/mysql/base"
	sql "github.com/actiontech/dtle/drivers/mysql/mysql/sql"
	"github.com/actiontech/dtle/g"
	"github.com/hashicorp/go-hclog"
	"github.com/pkg/errors"
	gomysql "github.com/siddontang/go-mysql/mysql"
)

type ApplierOracleIncr struct {
	logger       g.LoggerType
	subject      string
	mysqlContext *common.MySQLDriverConfig

	incrBytesQueue   chan []byte
	binlogEntryQueue chan *common.BinlogEntry
	// only TX can be executed should be put into this chan
	applyBinlogMtsTxQueue chan *common.BinlogEntryContext

	//mtsManager *MtsManager
	//wsManager  *WritesetManager

	db              *gosql.DB
	dbs             []*sql.Conn
	MySQLServerUuid string

	ctx        context.Context
	shutdownCh chan struct{}

	memory2        *int64
	printTps       bool
	txLastNSeconds uint32
	appliedTxCount uint32
	//timestampCtx     *TimestampContext
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
}

func NewApplierOracleIncr(ctx context.Context, subject string, mysqlContext *common.MySQLDriverConfig,
	logger g.LoggerType, gtidSet *gomysql.MysqlGTIDSet, memory2 *int64,
	db *gosql.DB, dbs []*sql.Conn, shutdownCh chan struct{},
	gtidSetLock *sync.RWMutex) (*ApplierOracleIncr, error) {

	a := &ApplierOracleIncr{
		ctx:                   ctx,
		logger:                logger,
		subject:               subject,
		mysqlContext:          mysqlContext,
		incrBytesQueue:        make(chan []byte, mysqlContext.ReplChanBufferSize),
		binlogEntryQueue:      make(chan *common.BinlogEntry, mysqlContext.ReplChanBufferSize*2),
		applyBinlogMtsTxQueue: make(chan *common.BinlogEntryContext, mysqlContext.ReplChanBufferSize*2),
		db:                    db,
		dbs:                   dbs,
		shutdownCh:            shutdownCh,
		memory2:               memory2,
		printTps:              g.EnvIsTrue(g.ENV_PRINT_TPS),
		gtidSet:               gtidSet,
		gtidSetLock:           gtidSetLock,
		tableItems:            make(mapSchemaTableItems),
	}
	return a, nil
}

func (a *ApplierOracleIncr) Run() (err error) {
	a.logger.Debug("Run. GetServerUUID. before")
	a.MySQLServerUuid, err = sql.GetServerUUID(a.db)
	if err != nil {
		return err
	}

	go a.heterogeneousReplay()
	return nil
}

func (a *ApplierOracleIncr) handleEntry(entryCtx *common.BinlogEntryContext) (err error) {
	binlogEntry := entryCtx.Entry
	a.logger.Debug("a binlogEntry.", "remaining", len(a.incrBytesQueue),
		"gno", binlogEntry.Coordinates.GNO, "lc", binlogEntry.Coordinates.LastCommitted,
		"seq", binlogEntry.Coordinates.SeqenceNumber)
	//txSid := binlogEntry.Coordinates.GetSid()

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

func (a *ApplierOracleIncr) heterogeneousReplay() {
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
					Entry:      entry,
					TableItems: nil,
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
			//a.handleEntry(NewTestEntryContext())
			hasEntry = false
		}
	}
}

// buildDMLEventQuery creates a query to operate on the ghost table, based on an intercepted binlog
// event entry on the original table.
func (a *ApplierOracleIncr) buildDMLEventQuery(dmlEvent common.DataEvent, workerIdx int,
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

var once sync.Once

// ApplyEventQueries applies multiple DML queries onto the dest table
func (a *ApplierOracleIncr) ApplyBinlogEvent(workerIdx int, binlogEntryCtx *common.BinlogEntryContext) error {
	logger := a.logger.Named("ApplyBinlogEvent")
	binlogEntry := binlogEntryCtx.Entry

	dbApplier := a.dbs[workerIdx]

	var totalDelta int64
	var err error
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
		logger.Debug("binlogEntry.Events", "gno", binlogEntry.Coordinates.GNO, "event", event)
		switch event.DML {
		case common.NotDML:
			var err error
			logger.Debug("not dml", "query", event.Query)

			execQuery := func(query string) error {
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

			//if event.CurrentSchema != "" {
			//	query := fmt.Sprintf("USE %s", mysqlconfig.EscapeName(event.CurrentSchema))
			//	logger.Debug("use", "query", query)
			//	err := execQuery(query)
			//	if err != nil {
			//		return err
			//	}
			//}

			if event.TableName != "" {
				var schema string
				if event.DatabaseName != "" {
					schema = event.DatabaseName
				} else {
					//schema = event.CurrentSchema
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
			logger.Debug("a dml query", "query", query, "args", args)

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
			//if nr > 0 {
			//	dbApplier.Tx.Commit()
			//}
			totalDelta += rowDelta
		}
	}
	if true { // todo commit
		dbApplier.Tx.Commit()
		dbApplier.Tx = nil
	}
	// TODO: need be executed after tx.Commit success
	a.EntryExecutedHook(binlogEntry)

	return nil
}

func (a *ApplierOracleIncr) getTableItem(schema string, table string) *common.ApplierTableItem {
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

func (a *ApplierOracleIncr) setTableItemForBinlogEntry(binlogEntry *common.BinlogEntryContext) error {
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
