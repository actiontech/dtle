/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package mysql

import (
	gosql "database/sql"
	"encoding/binary"
	"fmt"
	"github.com/actiontech/dtle/drivers/mysql/common"
	"github.com/cznic/mathutil"
	"github.com/hashicorp/nomad/plugins/drivers"

	"github.com/actiontech/dtle/g"
	"github.com/pkg/errors"

	"bytes"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	gonats "github.com/nats-io/go-nats"
	gomysql "github.com/siddontang/go-mysql/mysql"

	"os"
	"regexp"

	"github.com/actiontech/dtle/drivers/mysql/mysql/base"
	"github.com/actiontech/dtle/drivers/mysql/mysql/binlog"
	"github.com/actiontech/dtle/drivers/mysql/mysql/mysqlconfig"
	"github.com/actiontech/dtle/drivers/mysql/mysql/sql"
	sqle "github.com/actiontech/dtle/drivers/mysql/mysql/sqle/inspector"
	"github.com/hashicorp/go-hclog"
)

// Extractor is the main schema extract flow manager.
type Extractor struct {
	execCtx      *common.ExecContext
	logger       hclog.Logger
	subject      string
	mysqlContext *common.MySQLDriverConfig

	systemVariables       map[string]string
	sqlMode               string
	MySQLVersion          string
	TotalTransferredBytes int
	// Original comment: TotalRowsCopied returns the accurate number of rows being copied (affected)
	// This is not exactly the same as the rows being iterated via chunks, but potentially close enough.
	// TODO What is the difference between mysqlContext.RowsEstimate ?
	TotalRowsCopied       int64
	natsAddr              string

	mysqlVersionDigit int
	db                *gosql.DB
	singletonDB       *gosql.DB
	dumpers           []*dumper
	// db.tb exists when creating the job, for full-copy.
	// vs e.mysqlContext.ReplicateDoDb: all user assigned db.tb
	replicateDoDb            []*common.DataSource
	dataChannel              chan *common.BinlogEntryContext
	inspector                *Inspector
	binlogReader             *binlog.BinlogReader
	initialBinlogCoordinates *base.BinlogCoordinatesX
	currentBinlogCoordinates *common.BinlogCoordinateTx
	rowCopyComplete          chan bool
	rowCopyCompleteFlag      int64
	tableCount               int

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

	memory1 *int64
	memory2 *int64
}

func NewExtractor(execCtx *common.ExecContext, cfg *common.MySQLDriverConfig, logger hclog.Logger, storeManager *common.StoreManager, waitCh chan *drivers.ExitResult) (*Extractor, error) {
	logger.Info("NewExtractor", "job", execCtx.Subject)

	e := &Extractor{
		logger:          logger.Named("extractor").With("job", execCtx.Subject),
		execCtx:         execCtx,
		subject:         execCtx.Subject,
		mysqlContext:    cfg,
		dataChannel:     make(chan *common.BinlogEntryContext, cfg.ReplChanBufferSize * 4),
		rowCopyComplete: make(chan bool),
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
	}

	e.timestampCtx = NewTimestampContext(e.shutdownCh, e.logger, func() bool {
		return len(e.dataChannel) == 0
		// TODO need a more reliable method to determine queue.empty.
	})

	e.context.LoadSchemas(nil)
	logger.Debug("NewExtractor. after LoadSchemas")
	if delay, err := strconv.ParseInt(os.Getenv(g.ENV_TESTSTUB1_DELAY), 10, 64); err == nil {
		e.logger.Info("env", g.ENV_TESTSTUB1_DELAY, delay)
		e.testStub1Delay = delay
	}

	return e, nil
}

// Run executes the complete extract logic.
func (e *Extractor) Run() {
	var err error

	e.logger.Debug("consul put ReplChanBufferSize")
	err = e.storeManager.PutKey(e.subject, "ReplChanBufferSize",
		[]byte(strconv.Itoa(int(e.mysqlContext.ReplChanBufferSize))))
	if err != nil {
		e.onError(TaskStateDead, errors.Wrap(err, "PutKey ReplChanBufferSize"))
		return
	}

	e.natsAddr, err = e.storeManager.SrcWatchNats(e.subject, e.shutdownCh, func(err error) {
		e.onError(TaskStateDead, err)
	})
	if err != nil {
		e.onError(TaskStateDead, errors.Wrap(err, "SrcWatchNats"))
		return
	}

	err = common.GetGtidFromConsul(e.storeManager, e.subject, e.logger, e.mysqlContext)
	if err != nil {
		e.onError(TaskStateDead, errors.Wrap(err, "GetGtidFromConsul"))
		return
	}

	e.logger.Info("Extract binlog events", "mysql", e.mysqlContext.ConnectionConfig.GetAddr())

	// Validate job arguments
/*	{
		if e.mysqlContext.SkipCreateDbTable && e.mysqlContext.DropTableIfExists {
			e.onError(TaskStateDead,
				fmt.Errorf("conflicting job argument: SkipCreateDbTable=true and DropTableIfExists=true"))
			return
		}
	}*/
	e.logger.Info("initiateInspector")
	if err := e.initiateInspector(); err != nil {
		e.onError(TaskStateDead, err)
		return
	}
	e.logger.Info("initNatsPubClient")
	if err := e.initNatsPubClient(e.natsAddr); err != nil {
		e.onError(TaskStateDead, err)
		return
	}
	e.logger.Info("initDBConnections")
	if err := e.initDBConnections(); err != nil {
		e.logger.Error("initiateInspector error", "err", err)
		e.onError(TaskStateDead, err)
		return
	}

	fullCopy := true

	if e.mysqlContext.Gtid == "" {
		if e.mysqlContext.AutoGtid {
			e.logger.Info("using AutoGtid (latest position)")
			coord, err := base.GetSelfBinlogCoordinates(e.db)
			if err != nil {
				e.onError(TaskStateDead, err)
				return
			}
			e.mysqlContext.Gtid = coord.GtidSet
			e.logger.Debug("use auto gtid", "gtidset", coord.GtidSet)
			fullCopy = false
		}

		if e.mysqlContext.GtidStart != "" {
			e.logger.Info("calculating Gtid from GtidStart")
			coord, err := base.GetSelfBinlogCoordinates(e.db)
			if err != nil {
				e.onError(TaskStateDead, err)
				return
			}
			e.logger.Info("got mysql gtidset", "gtidset", coord.GtidSet)

			e.mysqlContext.Gtid, err = base.GtidSetDiff(coord.GtidSet, e.mysqlContext.GtidStart)
			if err != nil {
				e.onError(TaskStateDead, err)
				return
			}
			e.logger.Info("got Gtid", "Gtid", e.mysqlContext.Gtid)
			fullCopy = false
		}

		if e.mysqlContext.BinlogFile != "" {
			fullCopy = false
		}
	} else {
		fullCopy = false
		if e.mysqlContext.BinlogRelay {
			if e.mysqlContext.BinlogFile == "" {
				err := fmt.Errorf("the a job is incr-only (with GTID) and has BinlogRelay enabled," +
					" but BinlogFile,Pos is not provided")
				e.logger.Error("job config error")
				e.onError(TaskStateDead, err)
				return
			}
		}
	}

	if err := e.sendSysVarAndSqlMode(); err != nil {
		e.onError(TaskStateDead, err)
		return
	}
	e.logger.Debug("sendSysVarAndSqlMode. after")

	go e.timestampCtx.Handle()
	go func() {
		<-e.gotCoordinateCh

		if e.mysqlContext.SkipIncrementalCopy {
			e.logger.Warn("SkipIncrementalCopy is true. Make sure it is intended.")
			// empty
		} else {
			if !e.mysqlContext.BinlogRelay {
				// This must be after `<-e.gotCoordinateCh` or there will be a deadlock
				<-e.fullCopyDone
			}
			e.logger.Info("initBinlogReader")
			e.initBinlogReader(e.initialBinlogCoordinates)
		}
	}()

	if fullCopy {
		e.logger.Debug("mysqlDump. before")
		e.mysqlContext.MarkRowCopyStartTime()
		if err := e.mysqlDump(); err != nil {
			e.onError(TaskStateDead, err)
			return
		}
		err = e.sendFullComplete()
		if err != nil {
			e.onError(TaskStateDead, errors.Wrap(err, "sendFullComplete"))
			return
		}
	} else { // no full copy
		// Will not get consistent table meta-info for an incremental only job.
		// https://github.com/actiontech/dtle/issues/321#issuecomment-441191534
		if err := e.getSchemaTablesAndMeta(); err != nil {
			e.onError(TaskStateDead, err)
			return
		}
		if err := e.setInitialBinlogCoordinates(); err != nil {
			e.onError(TaskStateDead, err)
			return
		}
		err = e.sendFullComplete()
		if err != nil {
			e.onError(TaskStateDead, errors.Wrap(err, "sendFullComplete"))
			return
		}
		e.gotCoordinateCh <- struct{}{}
	}
	if !e.mysqlContext.BinlogRelay {
		e.fullCopyDone <- struct{}{}
	}

	if e.mysqlContext.SkipIncrementalCopy {
		e.logger.Info("SkipIncrementalCopy")
	} else {
		err := <-e.streamerReadyCh
		if err != nil {
			e.logger.Error("error after streamerReadyCh", "err", err)
			e.onError(TaskStateDead, err)
			return
		}
		if err := e.initiateStreaming(); err != nil {
			e.logger.Error("error at initiateStreaming", "err", err)
			e.onError(TaskStateDead, err)
			return
		}
	}
}

// initiateInspector connects, validates and inspects the "inspector" server.
// The "inspector" server is typically a replica; it is where we issue some
// queries such as:
// - table row count
// - schema validation
func (e *Extractor) initiateInspector() (err error) {
	e.inspector = NewInspector(e.mysqlContext, e.logger.ResetNamed("inspector"))
	if err := e.inspector.InitDBConnections(); err != nil {
		return err
	}

	return nil
}

func (e *Extractor) inspectTables() (err error) {
	// Creates a MYSQL Dump based on the options supplied through the dumper.
	dbsExisted, err := sql.ShowDatabases(e.db)
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

			existedTables, err := sql.ShowTables(e.db, doDb.TableSchema, e.mysqlContext.ExpandSyntaxSupport)
			if err != nil {
				return err
			}
			tbsFiltered := []*common.Table{}
			for _, tb := range existedTables {
				if len(e.mysqlContext.ReplicateIgnoreDb) > 0 && common.IgnoreTbByReplicateIgnoreDb(e.mysqlContext.ReplicateIgnoreDb, db.TableSchema, tb.TableName) {
					continue
				}
				tbsFiltered = append(tbsFiltered, tb)
			}

			if len(doDb.Tables) == 0 { // replicate all tables
				for _, doTb := range tbsFiltered {
					doTb.TableSchema = doDb.TableSchema
					doTb.TableSchemaRename = doDb.TableSchemaRename
					if err := e.inspector.ValidateOriginalTable(doDb.TableSchema, doTb.TableName, doTb); err != nil {
						e.logger.Warn("ValidateOriginalTable error", "err", err,
							"schema", doDb.TableSchema, "table", doTb.TableName)
						continue
					}
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
							if err := e.inspector.ValidateOriginalTable(doDb.TableSchema, table.TableName, newTable); err != nil {
								e.logger.Warn("ValidateOriginalTable error", "TableSchema", doDb.TableSchema, "TableName", doTb.TableName, "err", err)
								continue
							}
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
							if err := e.inspector.ValidateOriginalTable(doDb.TableSchema, doTb.TableName, doTb); err != nil {
								e.logger.Warn("ValidateOriginalTable error", "TableSchema", doDb.TableSchema, "TableName", doTb.TableName, "err", err)
								continue
							}
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

			tbs, err := sql.ShowTables(e.db, dbName, e.mysqlContext.ExpandSyntaxSupport)
			if err != nil {
				return err
			}

			for _, tb := range tbs {
				if len(e.mysqlContext.ReplicateIgnoreDb) > 0 && common.IgnoreTbByReplicateIgnoreDb(e.mysqlContext.ReplicateIgnoreDb, dbName, tb.TableName) {
					continue
				}
				if err := e.inspector.ValidateOriginalTable(dbName, tb.TableName, tb); err != nil {
					e.logger.Warn("ValidateOriginalTable error", "TableSchema", dbName, "TableName", tb.TableName, "err", err)
					continue
				}

				ds.Tables = append(ds.Tables, tb)
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

// readTableColumns reads table columns on applier
func (e *Extractor) readTableColumns() (err error) {
	e.logger.Info("Examining table structure on extractor")
	for _, doDb := range e.replicateDoDb {
		for _, doTb := range doDb.Tables {
			doTb.OriginalTableColumns, err = base.GetTableColumnsSqle(e.context, doTb.TableSchema, doTb.TableName)
			if err != nil {
				return err
			}
			doTb.ColumnMap = mysqlconfig.BuildColumnMapIndex(doTb.ColumnMapFrom, doTb.OriginalTableColumns.Ordinals)

		}
	}
	return nil
}

func (e *Extractor) initNatsPubClient(natsAddr string) (err error) {
	e.logger.Debug("begin Connect nats server", "NatAddr", natsAddr)
	sc, err := gonats.Connect(natsAddr)
	if err != nil {
		e.logger.Error("cannot connect nats server", "natsAddr", natsAddr, "err", err)
		return err
	}
	e.logger.Info("Connect nats server", "natsAddr", natsAddr)
	e.natsConn = sc

	_, err = e.natsConn.Subscribe(fmt.Sprintf("%s_restart", e.subject), func(m *gonats.Msg) {
		e.onError(TaskStateRestart, fmt.Errorf("applier restart: %v", string(m.Data)))
	})
	if err != nil {
		e.onError(TaskStateDead, errors.Wrap(err, "Subscribe restart"))
		return
	}
	_, err = e.natsConn.Subscribe(fmt.Sprintf("%s_error", e.subject), func(m *gonats.Msg) {
		e.onError(TaskStateDead, fmt.Errorf("applier error: %v", string(m.Data)))
	})
	if err != nil {
		e.onError(TaskStateDead, errors.Wrap(err, "Subscribe error"))
		return
	}

	_, err = e.natsConn.Subscribe(fmt.Sprintf("%s_progress", e.subject), func(m *gonats.Msg) {
		binlogFile := string(m.Data)
		e.logger.Debug("progress", "binlogFile", binlogFile)
		err := e.natsConn.Publish(m.Reply, nil)
		if err != nil {
			e.logger.Error("progress reply error", "err", err)
		}
		if e.binlogReader != nil {
			e.binlogReader.OnApplierRotate(binlogFile)
		} else {
			e.logger.Warn("DTLE_BUG. recv progress msg, but e.binlogReader is nil")
		}
	})
	if err != nil {
		e.onError(TaskStateDead, errors.Wrap(err, "Subscribe progress"))
		return
	}

	return nil
}

// initiateStreaming begins treaming of binary log events and registers listeners for such events
func (e *Extractor) initiateStreaming() error {
	go func() {
		e.logger.Info("Beginning streaming")
		err := e.StreamEvents()
		if err != nil {
			e.onError(TaskStateDead, err)
		}
	}()

	return nil
}

//--EventsStreamer--
func (e *Extractor) initDBConnections() (err error) {
	eventsStreamerUri := e.mysqlContext.ConnectionConfig.GetDBUri()
	if e.db, err = sql.CreateDB(eventsStreamerUri); err != nil {
		return err
	}

	if err := e.validateConnectionAndGetVersion(); err != nil {
		return err
	}

	{
		getTxIsolationVarName := func(mysqlVersionDigit int) string {
			if mysqlVersionDigit >= 50720 {
				return "transaction_isolation"
			} else {
				return "tx_isolation" // deprecated and removed in MySQL 8.0
			}
		}
		// https://github.com/go-sql-driver/mysql#system-variables
		dumpUri := fmt.Sprintf("%s&%s='REPEATABLE-READ'", e.mysqlContext.ConnectionConfig.GetSingletonDBUri(),
			getTxIsolationVarName(e.mysqlVersionDigit))
		if e.singletonDB, err = sql.CreateDB(dumpUri); err != nil {
			return err
		}
	}

	if timezone, err := base.ValidateAndReadTimeZone(e.db); err != nil {
		return err
	} else {
		e.logger.Info("got timezone", "timezone", timezone)
	}

	return nil
}

func (e *Extractor) getSchemaTablesAndMeta() error {
	if err := e.inspectTables(); err != nil {
		return err
	}

	for _, db := range e.replicateDoDb {
		e.context.AddSchema(db.TableSchema)
		e.context.LoadTables(db.TableSchema, nil)

		if strings.ToLower(db.TableSchema) == "mysql" {
			continue
		}
		e.context.UseSchema(db.TableSchema)

		for _, tb := range db.Tables {
			if strings.ToLower(tb.TableType) == "view" {
				// TODO what to do?
				continue
			}

			stmts, err := base.ShowCreateTable(e.db, db.TableSchema, tb.TableName, false, false)
			if err != nil {
				e.logger.Error("error at ShowCreateTable.", "err", err)
				return err
			}
			stmt := stmts[0]
			ast, err := sqle.ParseCreateTableStmt("mysql", stmt)
			if err != nil {
				e.logger.Error("error at ParseCreateTableStmt.", "err", err)
				return err
			}
			e.context.UpdateContext(ast, "mysql")
			if !e.context.HasTable(tb.TableSchema, tb.TableName) {
				err := fmt.Errorf("failed to add table to sqle context. table: %v.%v", db.TableSchema, tb.TableName)
				e.logger.Error(err.Error())
				return err
			}
		}
	}

	if err := e.readTableColumns(); err != nil {
		return err
	}
	return nil
}

// initBinlogReader creates and connects the reader: we hook up to a MySQL server as a replica
// Cooperate with `initiateStreaming()` using `e.streamerReadyCh`. Any err will be sent thru the chan.
func (e *Extractor) initBinlogReader(binlogCoordinates *base.BinlogCoordinatesX) {
	binlogReader, err := binlog.NewMySQLReader(e.execCtx, e.mysqlContext, e.logger.ResetNamed("reader"),
		e.replicateDoDb, e.context, e.memory2, e.db)
	if err != nil {
		e.logger.Error("err at initBinlogReader: NewMySQLReader", "err", err)
		e.streamerReadyCh <- err
		return
	}

	e.binlogReader = binlogReader

	go func() {
		err = binlogReader.ConnectBinlogStreamer(*binlogCoordinates)
		if err != nil {
			e.streamerReadyCh <- err
			return
		}
		e.streamerReadyCh <- nil
	}()
}

// validateConnection issues a simple can-connect to MySQL
func (e *Extractor) validateConnectionAndGetVersion() error {
	query := `select @@version`
	if err := e.db.QueryRow(query).Scan(&e.MySQLVersion); err != nil {
		return err
	}
	e.mysqlVersionDigit = common.MysqlVersionInDigit(e.MySQLVersion)
	if e.mysqlVersionDigit == 0 {
		return fmt.Errorf("cannot parse mysql version string to digit. string %v", e.MySQLVersion)
	}
	e.logger.Info("Connection validated", "on",
		hclog.Fmt("%s:%d", e.mysqlContext.ConnectionConfig.Host, e.mysqlContext.ConnectionConfig.Port))
	return nil
}

func (e *Extractor) selectSqlMode() error {
	query := `select @@sql_mode`
	if err := e.db.QueryRow(query).Scan(&e.sqlMode); err != nil {
		return err
	}
	return nil
}

func (e *Extractor) setInitialBinlogCoordinates() error {
	if e.mysqlContext.Gtid != "" {
		gtidSet, err := gomysql.ParseMysqlGTIDSet(e.mysqlContext.Gtid)
		if err != nil {
			return err
		}
		e.initialBinlogCoordinates = &base.BinlogCoordinatesX{
			GtidSet: gtidSet.String(),
			LogFile: e.mysqlContext.BinlogFile,
			LogPos:  e.mysqlContext.BinlogPos,
		}
	} else if e.mysqlContext.BinlogFile != "" {
		e.initialBinlogCoordinates = &base.BinlogCoordinatesX{
			LogFile: e.mysqlContext.BinlogFile,
			LogPos:  e.mysqlContext.BinlogPos,
		}
	} else {
		return fmt.Errorf("neither Gtid nor BinlogFile is assigned")
	}

	return nil
}

// CountTableRows counts exact number of rows on the original table
func (e *Extractor) CountTableRows(table *common.Table) (int64, error) {
	//e.logger.Debug("As instructed, I'm issuing a SELECT COUNT(*) on the table. This may take a while")

	var query string
	// It only requires select privilege on target table to select its information_schema item.
	query = fmt.Sprintf(`select table_rows from information_schema.tables where table_schema = '%s' and table_name = '%s'`,
		sql.EscapeValue(table.TableSchema), sql.EscapeValue(table.TableName))
	var rowsEstimate int64
	err := e.db.QueryRow(query).Scan(&rowsEstimate)
	if err != nil {
		e.logger.Error("error when getting estimated row number (using information_schema)", "err", err,
			"schema", table.TableSchema, "table", table.TableName)
		rowsEstimate = 0
	}
	atomic.AddInt64(&e.mysqlContext.RowsEstimate, rowsEstimate)

	e.mysqlContext.Stage = common.StageSearchingRowsForUpdate
	e.logger.Debug("Estimated number of rows", "schema", table.TableSchema, "table", table.TableName, "n", rowsEstimate)
	return rowsEstimate, nil
}

// Read the MySQL charset-related system variables.
func (e *Extractor) readMySqlCharsetSystemVariables() error {
	query := `show variables where Variable_name IN ('character_set_server','collation_server')`
	rows, err := e.db.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	// Show an example.
	/*
		mysql> SHOW VARIABLES WHERE Variable_name IN ('character_set_server','collation_server');
		+----------------------+-----------------+
		| Variable_name        | Value           |
		+----------------------+-----------------+
		| character_set_server | utf8            |
		| collation_server     | utf8_general_ci |
		+----------------------+-----------------+
	*/
	e.systemVariables = make(map[string]string)
	for rows.Next() {
		var (
			variable string
			value    string
		)

		err = rows.Scan(&variable, &value)

		if err != nil {
			return err
		}
		e.systemVariables[variable] = value
	}

	if rows.Err() != nil {
		return rows.Err()
	}

	e.logger.Info("Reading MySQL charset-related system variables before parsing DDL history.")
	return nil
}

func (e *Extractor) setStatementFor() string {
	var buffer bytes.Buffer
	first := true
	buffer.WriteString("SET ")
	for valName, value := range e.systemVariables {
		if first {
			first = false
		} else {
			buffer.WriteString(", ")
		}
		buffer.WriteString(valName + " = ")
		if strings.Contains(value, ",") || strings.Contains(value, ";") {
			value = "'" + value + "'"
		}
		buffer.WriteString(value)
	}
	return buffer.String()
}

type TimestampContext struct {
	stopCh         chan struct{}
	// Do not pass 0 to the chan.
	TimestampCh    chan uint32
	logger         hclog.Logger
	emptyQueueFunc func() bool
	delay          int64
}

func NewTimestampContext(stopCh chan struct{}, logger hclog.Logger, emptyQueueFunc func() bool) *TimestampContext {
	return &TimestampContext{
		stopCh:         stopCh,
		logger:         logger,
		emptyQueueFunc: emptyQueueFunc,

		TimestampCh: make(chan uint32, 16),
	}
}
func (tsc *TimestampContext) GetDelay() (d int64) {
	tsc.logger.Debug("TimestampContext.GetDelay", "delay", tsc.delay)
	return tsc.delay
}
func (tsc *TimestampContext) Handle() {
	interval := 15 * time.Second
	t := time.NewTicker(interval)
	hasTs := false
	for {
		select {
		case <-tsc.stopCh:
			t.Stop()
			return
		case ts := <-tsc.TimestampCh:
			tsc.logger.Debug("TimestampContext.Handle: got", "timestamp", ts)
			tsc.delay = time.Now().Unix() - int64(ts)
			hasTs = true
		case <-t.C:
			if hasTs {
				hasTs = false
			} else if tsc.delay != 0 {
				if tsc.emptyQueueFunc() {
					tsc.logger.Debug("delay: resetting timestamp")
					tsc.delay = 0
				}
			}
		}
	}
}

// StreamEvents will begin streaming events. It will be blocking, so should be
// executed by a goroutine
func (e *Extractor) StreamEvents() error {
	go func() {
		defer e.logger.Debug("StreamEvents goroutine exited")
		entries := common.BinlogEntries{}
		entriesSize := 0
		sendEntriesAndClear := func() error {
			var gno int64 = 0
			if len(entries.Entries) > 0 {
				theEntries := entries.Entries[0]
				gno = theEntries.Coordinates.GNO
				if theEntries.Events != nil && len(theEntries.Events) > 0 {
					e.timestampCtx.TimestampCh <- theEntries.Events[0].Timestamp
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

				if entriesSize >= e.mysqlContext.GroupMaxSize ||
					int64(len(entries.Entries)) == e.mysqlContext.ReplChanBufferSize {

					e.logger.Debug("incr. send by GroupLimit",
						"entriesSize", entriesSize,
						"groupMaxSize", e.mysqlContext.GroupMaxSize,
						"Entries.len", len(entries.Entries))

					err := sendEntriesAndClear()
					if err != nil {
						e.onError(TaskStateDead, err)
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
					err := sendEntriesAndClear()
					if err != nil {
						e.onError(TaskStateDead, err)
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
	if err := e.binlogReader.DataStreamEvents(e.dataChannel); err != nil {
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
func (e *Extractor) publish(subject string, txMsg []byte, gno int64) (err error) {
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
		_, err := e.natsConn.Request(subject, part, 24 * time.Hour)
		if err != nil {
			e.logger.Error("unexpected error on publish", "err", err)
			return err
		}
	}
	return nil
}

func (e *Extractor) testStub1() {
	if e.testStub1Delay > 0 {
		e.logger.Info("teststub1 delay start")
		time.Sleep(time.Duration(e.testStub1Delay) * time.Millisecond)
		e.logger.Info("teststub1 delay end")
	}
}

func (e *Extractor) sendSysVarAndSqlMode() error {
	// Generate the DDL statements that set the charset-related system variables ...
	if err := e.readMySqlCharsetSystemVariables(); err != nil {
		return err
	}
	setSystemVariablesStatement := e.setStatementFor()
	e.logger.Debug("set sysvar query", "query", setSystemVariablesStatement)
	if err := e.selectSqlMode(); err != nil {
		return err
	}
	setSqlMode := fmt.Sprintf("SET @@session.sql_mode = '%s'", e.sqlMode)

	entry := &common.DumpEntry{
		SystemVariablesStatement: setSystemVariablesStatement,
		SqlMode:                  setSqlMode,
	}
	if err := e.encodeAndSendDumpEntry(entry); err != nil {
		e.onError(TaskStateRestart, err)
	}

	return nil
}

//Perform the snapshot using the same logic as the "mysqldump" utility.
func (e *Extractor) mysqlDump() error {
	defer e.singletonDB.Close()
	var tx sql.QueryAble
	var err error
	step := 0
	// ------
	// STEP 0
	// ------
	// Set the transaction isolation level to REPEATABLE READ. This is the default, but the default can be changed
	// which is why we explicitly set it here.
	//
	// With REPEATABLE READ, all SELECT queries within the scope of a transaction (which we don't yet have) will read
	// from the same MVCC snapshot. Thus each plain (non-locking) SELECT statements within the same transaction are
	// consistent also with respect to each other.
	//
	// See: https://dev.mysql.com/doc/refman/5.7/en/set-transaction.html
	// See: https://dev.mysql.com/doc/refman/5.7/en/innodb-transaction-isolation-levels.html
	// See: https://dev.mysql.com/doc/refman/5.7/en/innodb-consistent-read.html
	e.logger.Info("Step: disabling autocommit and enabling repeatable read transactions", "n", step)

	step++

	// ------
	// STEP ?
	// ------
	// Obtain read lock on all tables. This statement closes all open tables and locks all tables
	// for all databases with a global read lock, and it prevents ALL updates while we have this lock.
	// It also ensures that everything we do while we have this lock will be consistent.
	/*e.logger.Info("Step %d: flush and obtain global read lock (preventing writes to database)", step)
	query := "FLUSH TABLES WITH READ LOCK"
	_, err = tx.Exec(query)
	if err != nil {
		e.logger.Info("exec %+v, error: %v", query, err)
		return err
	}
	step++*/

	// ------
	// STEP 1
	// ------
	// First, start a transaction and request that a consistent MVCC snapshot is obtained immediately.
	// See http://dev.mysql.com/doc/refman/5.7/en/commit.html

	var needConsistentSnapshot = true // TODO determine by table characteristic (has-PK or not)
	if needConsistentSnapshot {
		e.logger.Info("Step: start transaction with consistent snapshot", "n", step)
		gtidMatch := false
		gtidMatchRound := 0
		delayBetweenRetries := 200 * time.Millisecond
		for !gtidMatch {
			gtidMatchRound += 1

			// 1
			rows1, err := e.singletonDB.Query("show master status")
			if err != nil {
				e.logger.Error("get gtid 1 error", "round", gtidMatchRound, "err", err)
				return err
			}

			e.testStub1()

			// 2
			// TODO it seems that two 'start transaction' will be sent.
			// https://github.com/golang/go/issues/19981
			realTx, err := e.singletonDB.Begin()
			tx = realTx
			if err != nil {
				return err
			}
			query := "START TRANSACTION WITH CONSISTENT SNAPSHOT"
			_, err = realTx.Exec(query)
			if err != nil {
				e.logger.Error("realTx.Exec error", "query", query, "err", err)
				return err
			}

			e.testStub1()

			// 3
			rows2, err := realTx.Query("show master status")

			// 4
			binlogCoordinates1, err := base.ParseBinlogCoordinatesFromRows(rows1)
			if err != nil {
				return err
			}
			binlogCoordinates2, err := base.ParseBinlogCoordinatesFromRows(rows2)
			if err != nil {
				return err
			}
			e.logger.Debug("binlog coordinates 1", "coordinate", hclog.Fmt("%+v", binlogCoordinates1))
			e.logger.Debug("binlog coordinates 2", "coordinate", hclog.Fmt("%+v", binlogCoordinates2))

			gs1, err := gomysql.ParseMysqlGTIDSet(binlogCoordinates1.GtidSet)
			if err != nil {
				return err
			}
			gs2, err := gomysql.ParseMysqlGTIDSet(binlogCoordinates2.GtidSet)
			if err != nil {
				return err
			}

			if gs1.Contain(gs2) && gs2.Contain(gs1) {
				gtidMatch = true
				e.logger.Info("Got gtid", "gtidMatchRound", gtidMatchRound)

				// Obtain the binlog position and update the SourceInfo in the context. This means that all source records generated
				// as part of the snapshot will contain the binlog position of the snapshot.
				//binlogCoordinates, err := base.GetSelfBinlogCoordinatesWithTx(tx)

				e.initialBinlogCoordinates = binlogCoordinates2
				e.logger.Info("Step: read binlog coordinates of MySQL master", "n", step,
					"coordinate", hclog.Fmt("%+v", *e.initialBinlogCoordinates))

				defer func() {
					/*e.logger.Info("Step %d: releasing global read lock to enable MySQL writes", step)
					query := "UNLOCK TABLES"
					_, err := tx.Exec(query)
					if err != nil {
						e.logger.Info("exec %+v, error: %v", query, err)
					}
					step++*/
					e.logger.Info("Step: committing transaction", "n", step)
					if err := realTx.Commit(); err != nil {
						e.onError(TaskStateDead, err)
					}
				}()
			} else {
				e.logger.Warn("Failed to get a consistenct TX with GTID. Will retry.", "gtidMatchRound", gtidMatchRound)
				err = realTx.Rollback()
				if err != nil {
					return err
				}
				time.Sleep(delayBetweenRetries)
			}
		}
	} else {
		e.logger.Debug("no need to get consistent snapshot")
		tx = e.singletonDB
		rows1, err := tx.Query("show master status")
		if err != nil {
			return err
		}
		e.initialBinlogCoordinates, err = base.ParseBinlogCoordinatesFromRows(rows1)
		if err != nil {
			return err
		}
		e.logger.Debug("got gtid")
	}
	step++

	// ------
	// STEP 4
	// ------
	// Get the list of table IDs for each database. We can't use a prepared statement with MySQL, so we have to
	// build the SQL statement each time. Although in other cases this might lead to SQL injection, in our case
	// we are reading the database names from the database and not taking them from the user ...
	e.logger.Info("Step: read list of available tables in each database", "n", step)

	err = e.getSchemaTablesAndMeta()
	if err != nil {
		return err
	}

	e.gotCoordinateCh <- struct{}{}

	// Transform the current schema so that it reflects the *current* state of the MySQL server's contents.
	// First, get the DROP TABLE and CREATE TABLE statement (with keys and constraint definitions) for our tables ...
	if !e.mysqlContext.SkipCreateDbTable {
		e.logger.Info("generating DROP and CREATE statements to reflect current database schemas", "replicateDoDb", e.replicateDoDb)

		for _, db := range e.replicateDoDb {
			var dbSQL string
			if strings.ToLower(db.TableSchema) != "mysql" {
				if db.TableSchemaRename != "" {
					dbSQL = fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", mysqlconfig.EscapeName(db.TableSchemaRename))
				} else {
					dbSQL = fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", mysqlconfig.EscapeName(db.TableSchema))
				}

			}
			entry := &common.DumpEntry{
				DbSQL:      dbSQL,
			}
			atomic.AddInt64(&e.mysqlContext.RowsEstimate, 1)
			atomic.AddInt64(&e.TotalRowsCopied, 1)
			if err := e.encodeAndSendDumpEntry(entry); err != nil {
				e.onError(TaskStateRestart, err)
			}

			for _, tb := range db.Tables {
				if tb.TableSchema != db.TableSchema {
					continue
				}
				total, err := e.CountTableRows(tb)
				if err != nil {
					return err
				}
				tb.Counter = total
				var tbSQL []string
				if strings.ToLower(tb.TableType) == "view" {
					/*tbSQL, err = base.ShowCreateView(e.singletonDB, tb.TableSchema, tb.TableName, e.mysqlContext.DropTableIfExists)
					if err != nil {
						return err
					}*/
				} else if strings.ToLower(tb.TableSchema) != "mysql" {
					tbSQL, err = base.ShowCreateTable(e.singletonDB, tb.TableSchema, tb.TableName, e.mysqlContext.DropTableIfExists, true)
					for num, sql := range tbSQL {
						if db.TableSchemaRename != "" && strings.Contains(sql, fmt.Sprintf("USE %s", mysqlconfig.EscapeName(tb.TableSchema))) {
							tbSQL[num] = strings.Replace(sql, tb.TableSchema, db.TableSchemaRename, 1)
						}
						if tb.TableRename != "" && (strings.Contains(sql, fmt.Sprintf("DROP TABLE IF EXISTS %s", mysqlconfig.EscapeName(tb.TableName))) || strings.Contains(sql, "CREATE TABLE")) {
							tbSQL[num] = strings.Replace(sql, mysqlconfig.EscapeName(tb.TableName), tb.TableRename, 1)
						}
					}
					if err != nil {
						return err
					}
				}
				entry := &common.DumpEntry{
					TbSQL:      tbSQL,
					TotalCount: tb.Counter,
				}
				atomic.AddInt64(&e.mysqlContext.RowsEstimate, 1)
				atomic.AddInt64(&e.TotalRowsCopied, 1)
				if err := e.encodeAndSendDumpEntry(entry); err != nil {
					e.onError(TaskStateRestart, err)
				}
			}
			e.tableCount += len(db.Tables)
		}
	}
	step++

	// ------
	// STEP 5
	// ------
	// Dump all of the tables and generate source records ...
	e.logger.Info("Step: scanning contents of x tables", "n", step, "x", e.tableCount)
	startScan := g.CurrentTimeMillis()
	counter := 0
	//pool := models.NewPool(10)
	for _, db := range e.replicateDoDb {
		for _, t := range db.Tables {
			//pool.Add(1)
			//go func(t *config.Table) {
			counter++
			// Obtain a record maker for this table, which knows about the schema ...
			// Choose how we create statements based on the # of rows ...
			e.logger.Info("Step n: - scanning table (i of N tables)",
				"n", step, "schema", t.TableSchema, "table", t.TableName, "i", counter, "N", e.tableCount)

			d := NewDumper(tx, t, e.mysqlContext.ChunkSize, e.logger.ResetNamed("dumper"), e.memory1)
			if err := d.Dump(); err != nil {
				e.onError(TaskStateDead, err)
			}
			e.dumpers = append(e.dumpers, d)
			// Scan the rows in the table ...
			for entry := range d.resultsChannel {
				if entry.Err != "" {
					e.onError(TaskStateDead, fmt.Errorf(entry.Err))
				} else {
					memSize := int64(entry.Size())
					if !d.sentTableDef {
						tableBs, err := common.EncodeTable(d.table)
						if err != nil {
							err = errors.Wrap(err, "full copy: EncodeTable")
							e.onError(TaskStateDead, err)
							return err
						} else {
							entry.Table = tableBs
							d.sentTableDef = true
						}
					}
					if err = e.encodeAndSendDumpEntry(entry); err != nil {
						e.onError(TaskStateRestart, err)
					}
					atomic.AddInt64(&e.TotalRowsCopied, int64(len(entry.ValuesX)))
					atomic.AddInt64(d.memory, -memSize)
				}
			}

			//pool.Done()
			//}(tb)
		}
	}
	//pool.Wait()
	step++

	// We've copied all of the tables, but our buffer holds onto the very last record.
	// First mark the snapshot as complete and then apply the updated offset to the buffered record ...
	stop := g.CurrentTimeMillis()
	e.logger.Info("Step: scanned x rows in y tables in t",
		"n", step, "x", e.TotalRowsCopied, "y", e.tableCount, "t", time.Duration(stop-startScan))
	step++

	return nil
}
func (e *Extractor) encodeAndSendDumpEntry(entry *common.DumpEntry) error {
	bs, err := entry.Marshal(nil)
	if err != nil {
		return err
	}
	txMsg, err := common.Compress(bs)
	if err != nil {
		return errors.Wrap(err, "common.Compress")
	}
	if err := e.publish(fmt.Sprintf("%s_full", e.subject), txMsg, 0); err != nil {
		return err
	}
	e.mysqlContext.Stage = common.StageSendingData
	return nil
}

func (e *Extractor) Stats() (*common.TaskStatistics, error) {
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

	extractedTxCount := e.binlogReader.GetExtractedTxCount()
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
			Num:  0,
			Time: e.timestampCtx.GetDelay(),
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
			e.onError(TaskStateDead, fmt.Errorf("traffic limit exceeded : %d/%d", e.mysqlContext.TrafficAgainstLimits, int(taskResUsage.MsgStat.OutBytes)/1024/1024/1024))
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

func (e *Extractor) onError(state int, err error) {
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

// Shutdown is used to tear down the extractor
func (e *Extractor) Shutdown() error {
	e.logger.Debug("extractor shutdown")
	e.shutdownLock.Lock()
	defer e.shutdownLock.Unlock()

	if e.shutdown {
		return nil
	}
	e.logger.Info("extractor shutdown")

	e.shutdown = true
	close(e.shutdownCh)

	if e.natsConn != nil {
		e.natsConn.Close()
	}

	for _, d := range e.dumpers {
		d.Close()
	}

	if err := sql.CloseDB(e.singletonDB); err != nil {
		e.logger.Error("Shutdown error close singletonDB.", "err", err)
	}

	if e.inspector != nil {
		e.inspector.Close()
	}

	if e.binlogReader != nil {
		if err := e.binlogReader.Close(); err != nil {
			e.logger.Error("Shutdown error close binlogReader.", "err", err)
		}
	}

	if err := sql.CloseDB(e.db); err != nil {
		e.logger.Error("Shutdown error close e.db.", "err", err)
	}

	//close(e.binlogChannel)
	e.logger.Info("Shutting down")
	return nil
}

func (e *Extractor) Pause() error {
	return e.Shutdown()
}

func (e *Extractor) sendFullComplete() (err error) {
	dumpMsg, err := common.Encode(&common.DumpStatResult{
		Gtid:       e.initialBinlogCoordinates.GtidSet,
		LogFile:    e.initialBinlogCoordinates.LogFile,
		LogPos:     e.initialBinlogCoordinates.LogPos,
	})
	if err != nil {
		return err
	}
	if err := e.publish(fmt.Sprintf("%s_full_complete", e.subject), dumpMsg, 0); err != nil {
		return err
	}
	return nil
}
