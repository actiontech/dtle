/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package mysql

import (
	gosql "database/sql"
	"encoding/json"
	"fmt"

	"github.com/actiontech/dtle/internal/g"

	//"math"
	"bytes"
	"encoding/gob"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/snappy"
	gonats "github.com/nats-io/go-nats"
	gomysql "github.com/siddontang/go-mysql/mysql"

	"os"

	"regexp"

	"github.com/actiontech/dtle/internal/client/driver/mysql/base"
	"github.com/actiontech/dtle/internal/client/driver/mysql/binlog"
	"github.com/actiontech/dtle/internal/client/driver/mysql/sql"
	sqle "github.com/actiontech/dtle/internal/client/driver/mysql/sqle/inspector"
	"github.com/actiontech/dtle/internal/config"
	log "github.com/actiontech/dtle/internal/logger"
	"github.com/actiontech/dtle/internal/models"
	"github.com/actiontech/dtle/utils"
)

const (
	// DefaultConnectWait is the default timeout used for the connect operation
	DefaultConnectWaitSecond      = 10
	DefaultConnectWait            = DefaultConnectWaitSecond * time.Second
	ReconnectStreamerSleepSeconds = 5
	SCHEMA                        = "schema"
	TABLES                        = "tables"
	TABLE                         = "table"
)

// Extractor is the main schema extract flow manager.
type Extractor struct {
	logger       *log.Entry
	subject      string
	tp           string
	maxPayload   int
	mysqlContext *config.MySQLDriverConfig
	db           *gosql.DB
	singletonDB  *gosql.DB
	dumpers      []*dumper
	// db.tb exists when creating the job, for full-copy.
	// vs e.mysqlContext.ReplicateDoDb: all user assigned db.tb
	replicateDoDb            []*config.DataSource
	binlogChannel            chan *binlog.BinlogTx
	dataChannel              chan *binlog.BinlogEntry
	inspector                *Inspector
	binlogReader             *binlog.BinlogReader
	initialBinlogCoordinates *base.BinlogCoordinatesX
	currentBinlogCoordinates *base.BinlogCoordinateTx
	rowCopyComplete          chan bool
	rowCopyCompleteFlag      int64
	tableCount               int

	sendByTimeoutCounter  int
	sendBySizeFullCounter int

	natsConn *gonats.Conn
	waitCh   chan *models.WaitResult

	shutdown     bool
	shutdownCh   chan struct{}
	shutdownLock sync.Mutex

	testStub1Delay int64

	context *sqle.Context
}

func NewExtractor(subject, tp string, maxPayload int, cfg *config.MySQLDriverConfig, logger *log.Logger) (*Extractor, error) {

	cfg = cfg.SetDefault()
	entry := log.NewEntry(logger).WithFields(log.Fields{
		"job": subject,
	})
	e := &Extractor{
		logger:          entry,
		subject:         subject,
		tp:              tp,
		maxPayload:      maxPayload,
		mysqlContext:    cfg,
		binlogChannel:   make(chan *binlog.BinlogTx, cfg.ReplChanBufferSize),
		dataChannel:     make(chan *binlog.BinlogEntry, cfg.ReplChanBufferSize),
		rowCopyComplete: make(chan bool),
		waitCh:          make(chan *models.WaitResult, 1),
		shutdownCh:      make(chan struct{}),
		testStub1Delay:  0,
		context:         sqle.NewContext(nil),
	}
	e.context.LoadSchemas(nil)

	if delay, err := strconv.ParseInt(os.Getenv(g.ENV_TESTSTUB1_DELAY), 10, 64); err == nil {
		e.logger.Infof("%v = %v", g.ENV_TESTSTUB1_DELAY, delay)
		e.testStub1Delay = delay
	}

	return e, nil
}

// sleepWhileTrue sleeps indefinitely until the given function returns 'false'
// (or fails with error)
func (e *Extractor) sleepWhileTrue(operation func() (bool, error)) error {
	for {
		shouldSleep, err := operation()
		if err != nil {
			return err
		}
		if !shouldSleep {
			return nil
		}
		time.Sleep(time.Second)
	}
}

// retryOperation attempts up to `count` attempts at running given function,
// exiting as soon as it returns with non-error.
func (e *Extractor) retryOperation(operation func() error, notFatalHint ...bool) (err error) {
	for i := 0; i < int(e.mysqlContext.MaxRetries); i++ {
		if i != 0 {
			// sleep after previous iteration
			time.Sleep(1 * time.Second)
		}
		err = operation()
		if err == nil {
			return nil
		}
		// there's an error. Let's try again.
	}
	if len(notFatalHint) == 0 {
		return err
	}
	return err
}

// Run executes the complete extract logic.
func (e *Extractor) Run() {
	e.logger.Printf("mysql.extractor: Extract binlog events from %s.%d", e.mysqlContext.ConnectionConfig.Host, e.mysqlContext.ConnectionConfig.Port)
	e.mysqlContext.StartTime = time.Now()

	// Validate job arguments
	{
		if e.mysqlContext.SkipCreateDbTable && e.mysqlContext.DropTableIfExists {
			e.onError(TaskStateDead,
				fmt.Errorf("conflicting job argument: SkipCreateDbTable=true and DropTableIfExists=true"))
			return
		}
	}

	if err := e.initiateInspector(); err != nil {
		e.onError(TaskStateDead, err)
		return
	}
	if err := e.initNatsPubClient(); err != nil {
		e.onError(TaskStateDead, err)
		return
	}
	if err := e.initDBConnections(); err != nil {
		e.onError(TaskStateDead, err)
		return
	}

	fullCopy := true

	if e.mysqlContext.Gtid == "" {
		if e.mysqlContext.AutoGtid {
			coord, err := base.GetSelfBinlogCoordinates(e.db)
			if err != nil {
				e.onError(TaskStateDead, err)
				return
			}
			e.mysqlContext.Gtid = coord.GtidSet
			e.logger.Debugf("mysql.extractor: use auto gtid: %v", coord.GtidSet)
			fullCopy = false
		}

		if e.mysqlContext.GtidStart != "" {
			coord, err := base.GetSelfBinlogCoordinates(e.db)
			if err != nil {
				e.onError(TaskStateDead, err)
				return
			}

			e.mysqlContext.Gtid, err = base.GtidSetDiff(coord.GtidSet, e.mysqlContext.GtidStart)
			if err != nil {
				e.onError(TaskStateDead, err)
				return
			}
			fullCopy = false
		}
	} else {
		fullCopy = false
	}

	if fullCopy {
		e.mysqlContext.MarkRowCopyStartTime()
		if err := e.mysqlDump(); err != nil {
			e.onError(TaskStateDead, err)
			return
		}
		dumpMsg, err := Encode(&dumpStatResult{Gtid: e.initialBinlogCoordinates.GtidSet, TotalCount: e.mysqlContext.RowsEstimate})
		if err != nil {
			e.onError(TaskStateDead, err)
		}
		if err := e.publish(fmt.Sprintf("%s_full_complete", e.subject), "", dumpMsg); err != nil {
			e.onError(TaskStateDead, err)
		}
	} else {
		// Will not get consistent table meta-info for an incremental only job.
		// https://github.com/actiontech/dtle/issues/321#issuecomment-441191534
		if err := e.getSchemaTablesAndMeta(); err != nil {
			e.onError(TaskStateDead, err)
			return
		}
		if err := e.readCurrentBinlogCoordinates(); err != nil {
			e.onError(TaskStateDead, err)
			return
		}
	}

	if e.mysqlContext.SkipIncrementalCopy {
		e.logger.Infof("mysql.extractor. SkipIncrementalCopy")
	} else {
		if err := e.initBinlogReader(e.initialBinlogCoordinates); err != nil {
			e.logger.Debugf("mysql.extractor error at initBinlogReader: %v", err.Error())
			e.onError(TaskStateDead, err)
			return
		}

		if err := e.initiateStreaming(); err != nil {
			e.logger.Debugf("mysql.extractor error at initiateStreaming: %v", err.Error())
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
	e.inspector = NewInspector(e.mysqlContext, e.logger)
	if err := e.inspector.InitDBConnections(); err != nil {
		return err
	}

	return nil
}

func (e *Extractor) inspectTables() (err error) {
	// Creates a MYSQL Dump based on the options supplied through the dumper.
	if len(e.mysqlContext.ReplicateDoDb) > 0 {
		var doDbs []*config.DataSource
		// Get all db from  TableSchemaRegex regex and get all tableSchemaRename
		for _, doDb := range e.mysqlContext.ReplicateDoDb {
			if doDb.TableSchema == "" && doDb.TableSchemaRegex == "" {
				return fmt.Errorf("TableSchema or TableSchemaRegex can not both be blank. ")
			}
			var regex string
			if doDb.TableSchemaRegex != "" && doDb.TableSchemaRename != "" && doDb.TableSchema == "" {
				regex = doDb.TableSchemaRegex
			} else if doDb.TableSchemaRegex == "" && doDb.TableSchema != "" {
				regex = "^" + doDb.TableSchema + "$"
			} else {
				return fmt.Errorf("TableSchema  configuration error. ")
			}
			if regex != "" {
				dbs, err := sql.ShowDatabases(e.db)
				if err != nil {
					return err
				}
				schemaRenameRegex := doDb.TableSchemaRename

				for _, db := range dbs {
					newdb := &config.DataSource{}
					reg := regexp.MustCompile(regex)
					if !reg.MatchString(db) {
						continue
					}
					doDb.TableSchema = db
					if schemaRenameRegex != "" {
						match := reg.FindStringSubmatchIndex(db)
						doDb.TableSchemaRename = string(reg.ExpandString(nil, schemaRenameRegex, db, match))
					}
					*newdb = *doDb
					doDbs = append(doDbs, newdb)
				}
			} else {
				doDbs = append(doDbs, doDb)
			}

		}
		for _, doDb := range doDbs {
			db := &config.DataSource{
				TableSchema:       doDb.TableSchema,
				TableSchemaRename: doDb.TableSchemaRename,
			}

			if len(doDb.Tables) == 0 {
				db.TableSchemaScope = SCHEMA
				tbs, err := sql.ShowTables(e.db, doDb.TableSchema, e.mysqlContext.ExpandSyntaxSupport)
				if err != nil {
					return err
				}
				for _, doTb := range tbs {
					doTb.TableSchema = doDb.TableSchema
					doTb.TableSchemaRename = doDb.TableSchemaRename
					if err := e.inspector.ValidateOriginalTable(doDb.TableSchema, doTb.TableName, doTb); err != nil {
						e.logger.Warnf("mysql.extractor: %v", err)
						continue
					}
					db.Tables = append(db.Tables, doTb)
				}
			} else {
				for _, doTb := range doDb.Tables {
					doTb.TableSchema = doDb.TableSchema
					doTb.TableSchemaRename = doDb.TableSchemaRename
					var regex string
					if doTb.TableRegex != "" && doTb.TableName == "" && doTb.TableRename != "" {
						regex = doTb.TableRegex
						db.TableSchemaScope = TABLES
					} else if doTb.TableRegex == "" && doTb.TableName != "" {
						regex = "^" + doTb.TableName + "$"
						db.TableSchemaScope = TABLE
					} else {
						return fmt.Errorf("Table  configuration error. ")
					}
					tables, err := sql.ShowTables(e.db, doDb.TableSchema, e.mysqlContext.ExpandSyntaxSupport)
					if err != nil {
						return err
					}
					tableRenameRegex := doTb.TableRename
					for _, table := range tables {
						reg := regexp.MustCompile(regex)
						if !reg.MatchString(table.TableName) {
							continue
						}
						doTb.TableName = table.TableName

						if tableRenameRegex != "" {
							match := reg.FindStringSubmatchIndex(table.TableName)
							doTb.TableRename = string(reg.ExpandString(nil, tableRenameRegex, table.TableName, match))
						}
						if err := e.inspector.ValidateOriginalTable(doDb.TableSchema, table.TableName, doTb); err != nil {
							e.logger.Warnf("mysql.extractor: %v", err)
							continue
						}
						*table = *doTb
						db.Tables = append(db.Tables, table)
					}

				}
			}
			e.replicateDoDb = append(e.replicateDoDb, db)
		}
		e.mysqlContext.ReplicateDoDb = e.replicateDoDb
	} else {
		dbs, err := sql.ShowDatabases(e.db)
		if err != nil {
			return err
		}
		for _, dbName := range dbs {
			ds := &config.DataSource{
				TableSchema:      dbName,
				TableSchemaScope: SCHEMA,
			}
			if len(e.mysqlContext.ReplicateIgnoreDb) > 0 && e.ignoreDb(dbName) {
				continue
			}

			tbs, err := sql.ShowTables(e.db, dbName, e.mysqlContext.ExpandSyntaxSupport)
			if err != nil {
				return err
			}

			for _, tb := range tbs {
				if len(e.mysqlContext.ReplicateIgnoreDb) > 0 && e.ignoreTb(dbName, tb.TableName) {
					continue
				}
				if err := e.inspector.ValidateOriginalTable(dbName, tb.TableName, tb); err != nil {
					e.logger.Warnf("mysql.extractor: %v", err)
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
func (e *Extractor) ignoreDb(dbName string) bool {
	for _, ignoreDb := range e.mysqlContext.ReplicateIgnoreDb {
		if ignoreDb.TableSchema == dbName && len(ignoreDb.Tables) == 0 {
			return true
		}
	}
	return false
}

func (e *Extractor) ignoreTb(dbName, tbName string) bool {
	for _, ignoreDb := range e.mysqlContext.ReplicateIgnoreDb {
		if ignoreDb.TableSchema == dbName {
			for _, ignoreTb := range ignoreDb.Tables {
				if ignoreTb.TableName == tbName {
					return true
				}
			}
		}
	}
	return false
}

// readTableColumns reads table columns on applier
func (e *Extractor) readTableColumns() (err error) {
	e.logger.Printf("mysql.extractor: Examining table structure on extractor")
	for _, doDb := range e.replicateDoDb {
		for _, doTb := range doDb.Tables {
			doTb.OriginalTableColumns, err = base.GetTableColumnsSqle(e.context, doTb.TableSchema, doTb.TableName)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *Extractor) initNatsPubClient() (err error) {
	natsAddr := fmt.Sprintf("nats://%s", e.mysqlContext.NatsAddr)
	sc, err := gonats.Connect(natsAddr)
	if err != nil {
		e.logger.Errorf("mysql.extractor: Can't connect nats server %v. make sure a nats streaming server is running.%v", natsAddr, err)
		return err
	}
	e.logger.Debugf("mysql.extractor: Connect nats server %v", natsAddr)
	e.natsConn = sc

	return nil
}

// initiateStreaming begins treaming of binary log events and registers listeners for such events
func (e *Extractor) initiateStreaming() error {
	go func() {
		e.logger.Printf("mysql.extractor: Beginning streaming")
		err := e.StreamEvents()
		if err != nil {
			e.onError(TaskStateDead, err)
		}
	}()

	go func() {
		_, err := e.natsConn.Subscribe(fmt.Sprintf("%s_restart", e.subject), func(m *gonats.Msg) {
			e.mysqlContext.Gtid = string(m.Data)
			e.onError(TaskStateRestart, fmt.Errorf("restart"))
		})
		if err != nil {
			e.onError(TaskStateRestart, err)
		}

		_, err = e.natsConn.Subscribe(fmt.Sprintf("%s_error", e.subject), func(m *gonats.Msg) {
			e.mysqlContext.Gtid = string(m.Data)
			e.onError(TaskStateDead, fmt.Errorf("applier"))
		})
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
	//https://github.com/go-sql-driver/mysql#system-variables
	dumpUri := fmt.Sprintf("%s&tx_isolation='REPEATABLE-READ'", e.mysqlContext.ConnectionConfig.GetSingletonDBUri())
	if e.singletonDB, err = sql.CreateDB(dumpUri); err != nil {
		return err
	}
	if err := e.validateConnection(); err != nil {
		return err
	}
	if err := e.validateAndReadTimeZone(); err != nil {
		return err
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
				e.logger.Errorf("error at ShowCreateTable. err: %v", err)
				return err
			}
			stmt := stmts[0]
			ast, err := sqle.ParseCreateTableStmt("mysql", stmt)
			if err != nil {
				e.logger.Errorf("error at ParseCreateTableStmt. err: %v", err)
				return err
			}
			e.context.UpdateContext(ast, "mysql")
			if !e.context.HasTable(tb.TableSchema, tb.TableName) {
				err := fmt.Errorf("failed to add table to sqle context. table: %v.%v", db.TableSchema, tb.TableName)
				e.logger.Errorf(err.Error())
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
func (e *Extractor) initBinlogReader(binlogCoordinates *base.BinlogCoordinatesX) error {
	binlogReader, err := binlog.NewMySQLReader(e.mysqlContext, e.logger, e.replicateDoDb, e.context)
	if err != nil {
		e.logger.Debugf("mysql.extractor: err at initBinlogReader: NewMySQLReader: %v", err.Error())
		return err
	}

	if err := binlogReader.ConnectBinlogStreamer(*binlogCoordinates); err != nil {
		e.logger.Debugf("mysql.extractor: err at initBinlogReader: ConnectBinlogStreamer: %v", err.Error())
		return err
	}
	e.binlogReader = binlogReader
	return nil
}

// validateConnection issues a simple can-connect to MySQL
func (e *Extractor) validateConnection() error {
	query := `select @@global.version`
	if err := e.db.QueryRow(query).Scan(&e.mysqlContext.MySQLVersion); err != nil {
		return err
	}
	e.logger.Printf("mysql.extractor: Connection validated on %s:%d", e.mysqlContext.ConnectionConfig.Host, e.mysqlContext.ConnectionConfig.Port)
	return nil
}

func (e *Extractor) selectSqlMode() error {
	query := `select @@global.sql_mode`
	if err := e.db.QueryRow(query).Scan(&e.mysqlContext.SqlMode); err != nil {
		return err
	}
	return nil
}

// readCurrentBinlogCoordinates reads master status from hooked server
func (e *Extractor) readCurrentBinlogCoordinates() error {
	if e.mysqlContext.Gtid != "" {
		gtidSet, err := gomysql.ParseMysqlGTIDSet(e.mysqlContext.Gtid)
		if err != nil {
			return err
		}
		e.initialBinlogCoordinates = &base.BinlogCoordinatesX{
			GtidSet: gtidSet.String(),
		}
	} else {
		binlogCoordinates, err := base.GetSelfBinlogCoordinates(e.db)
		if err != nil {
			return err
		}
		e.initialBinlogCoordinates = binlogCoordinates
	}

	return nil
}

func (e *Extractor) validateAndReadTimeZone() error {
	query := `select @@global.time_zone`
	if err := e.db.QueryRow(query).Scan(&e.mysqlContext.TimeZone); err != nil {
		return err
	}

	e.logger.Printf("mysql.extractor: Will use time_zone='%s' on extractor", e.mysqlContext.TimeZone)
	return nil
}

// CountTableRows counts exact number of rows on the original table
func (e *Extractor) CountTableRows(table *config.Table) (int64, error) {
	atomic.StoreInt64(&e.mysqlContext.CountingRowsFlag, 1)
	defer atomic.StoreInt64(&e.mysqlContext.CountingRowsFlag, 0)
	//e.logger.Debugf("mysql.extractor: As instructed, I'm issuing a SELECT COUNT(*) on the table. This may take a while")

	var query string
	var method string
	if os.Getenv(g.ENV_COUNT_INFO_SCHEMA) != "" {
		method = "information_schema"
		query = fmt.Sprintf(`select table_rows from information_schema.tables where table_schema = '%s' and table_name = '%s'`,
			table.TableSchema, table.TableName)
	} else {
		method = "COUNT"
		query = fmt.Sprintf(`select count(*) as rows from %s.%s where (%s)`,
			sql.EscapeName(table.TableSchema), sql.EscapeName(table.TableName), table.Where)
	}
	var rowsEstimate int64
	if err := e.db.QueryRow(query).Scan(&rowsEstimate); err != nil {
		return 0, err
	}
	atomic.AddInt64(&e.mysqlContext.RowsEstimate, rowsEstimate)

	e.mysqlContext.Stage = models.StageSearchingRowsForUpdate
	e.logger.Debugf("mysql.extractor: Exact number of rows(%s.%s) via %v: %d", table.TableSchema, table.TableName, method, rowsEstimate)
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
	e.mysqlContext.SystemVariables = make(map[string]string)
	for rows.Next() {
		var (
			variable string
			value    string
		)

		err = rows.Scan(&variable, &value)

		if err != nil {
			return err
		}
		e.mysqlContext.SystemVariables[variable] = value
	}

	if rows.Err() != nil {
		return rows.Err()
	}

	e.logger.Printf("mysql.extractor: Reading MySQL charset-related system variables before parsing DDL history.")
	return nil
}

func (e *Extractor) setStatementFor() string {
	var buffer bytes.Buffer
	first := true
	buffer.WriteString("SET ")
	for valName, value := range e.mysqlContext.SystemVariables {
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

// Encode
func Encode(v interface{}) ([]byte, error) {
	b := new(bytes.Buffer)
	if err := gob.NewEncoder(b).Encode(v); err != nil {
		return nil, err
	}
	return snappy.Encode(nil, b.Bytes()), nil
	//return b.Bytes(), nil
}

// StreamEvents will begin streaming events. It will be blocking, so should be
// executed by a goroutine
func (e *Extractor) StreamEvents() error {
	if e.mysqlContext.ApproveHeterogeneous {
		go func() {
			defer e.logger.Debugf("extractor. StreamEvents goroutine exited")

			entries := binlog.BinlogEntries{}
			entriesSize := 0

			sendEntries := func() error {
				var gno int64 = 0
				if len(entries.Entries) > 0 {
					gno = entries.Entries[0].Coordinates.GNO
				}

				txMsg, err := Encode(entries)
				if err != nil {
					return err
				}

				e.logger.Debugf("mysql.extractor: sending gno: %v, n: %v", gno, len(entries.Entries))
				if err = e.publish(fmt.Sprintf("%s_incr_hete", e.subject), "", txMsg); err != nil {
					return err
				}
				e.logger.Debugf("mysql.extractor: send acked gno: %v, n: %v", gno, len(entries.Entries))

				entries.Entries = nil
				entriesSize = 0

				return nil
			}

			keepGoing := true

			groupTimeoutDuration := time.Duration(e.mysqlContext.GroupTimeout) * time.Millisecond
			timer := time.NewTimer(groupTimeoutDuration)
			defer timer.Stop()

			for keepGoing && !e.shutdown {
				var err error
				select {
				case binlogEntry := <-e.dataChannel:
					entries.Entries = append(entries.Entries, binlogEntry)
					entriesSize += binlogEntry.OriginalSize

					if entriesSize >= e.mysqlContext.GroupMaxSize ||
						int64(len(entries.Entries)) == e.mysqlContext.ReplChanBufferSize {
						e.logger.Debugf("extractor. incr. send by GroupLimit. entriesSize: %v", entriesSize)
						err = sendEntries()
						if !timer.Stop() {
							<-timer.C
						}
						timer.Reset(groupTimeoutDuration)
					}
				case <-timer.C:
					nEntries := len(entries.Entries)
					if nEntries > 0 {
						e.logger.Debugf("extractor. incr. send by timeout. entriesSize: %v", entriesSize)
						err = sendEntries()
					}
					timer.Reset(groupTimeoutDuration)
				}
				if err != nil {
					e.onError(TaskStateDead, err)
					keepGoing = false
				} else {
					e.mysqlContext.Stage = models.StageSendingBinlogEventToSlave
					atomic.AddInt64(&e.mysqlContext.DeltaEstimate, 1)
				}
			}
		}()
		// region commented out
		/*entryArray := make([]*binlog.BinlogEntry, 0)
		subject := fmt.Sprintf("%s_incr_hete", e.subject)

		go func() {
		L:
			for {
				select {
				case binlogEntry := <-e.dataChannel:
					{
						if nil == binlogEntry {
							continue
						}
						entryArray = append(entryArray, binlogEntry)
						txMsg, err := Encode(&entryArray)
						if err != nil {
							e.onError(TaskStateDead, err)
							break L
						}
						if len(txMsg) > e.mysqlContext.MsgBytesLimit {
							if len(txMsg) > e.maxPayload {
								e.onError(TaskStateDead, gonats.ErrMaxPayload)
							}
							if err = e.publish(subject, fmt.Sprintf("%s:1-%d", binlogEntry.Coordinates.SID, binlogEntry.Coordinates.GNO), txMsg); err != nil {
								e.onError(TaskStateDead, err)
								break L
							}
							//send_by_size_full
							e.sendBySizeFullCounter += len(entryArray)
							entryArray = []*binlog.BinlogEntry{}
						}
					}
				case <-time.After(100 * time.Millisecond):
					{
						if len(entryArray) != 0 {
							txMsg, err := Encode(&entryArray)
							if err != nil {
								e.onError(TaskStateDead, err)
								break L
							}
							if len(txMsg) > e.maxPayload {
								e.onError(TaskStateDead, gonats.ErrMaxPayload)
							}
							if err = e.publish(subject,
								fmt.Sprintf("%s:1-%d",
									entryArray[len(entryArray)-1].Coordinates.SID,
									entryArray[len(entryArray)-1].Coordinates.GNO),
								txMsg); err != nil {
								e.onError(TaskStateDead, err)
								break L
							}
							//send_by_timeout
							e.sendByTimeoutCounter += len(entryArray)
							entryArray = []*binlog.BinlogEntry{}
						}
					}
				case <-e.shutdownCh:
					break L
				}
			}
		}()*/
		// endregion
		// The next should block and execute forever, unless there's a serious error
		if err := e.binlogReader.DataStreamEvents(e.dataChannel); err != nil {
			if e.shutdown {
				return nil
			}
			return fmt.Errorf("mysql.extractor: StreamEvents encountered unexpected error: %+v", err)
		}
	} else {
		// region homogeneous
		//timeout := time.NewTimer(100 * time.Millisecond)
		txArray := make([]*binlog.BinlogTx, 0)
		txBytes := 0
		subject := fmt.Sprintf("%s_incr", e.subject)

		go func() {
		L:
			for {
				select {
				case binlogTx := <-e.binlogChannel:
					{
						if nil == binlogTx {
							continue
						}
						txArray = append(txArray, binlogTx)
						txBytes += len([]byte(binlogTx.Query))
						if txBytes > e.mysqlContext.MsgBytesLimit {
							txMsg, err := Encode(&txArray)
							if err != nil {
								e.onError(TaskStateDead, err)
								break L
							}
							if len(txMsg) > e.maxPayload {
								e.onError(TaskStateDead, gonats.ErrMaxPayload)
							}
							if err = e.publish(subject, fmt.Sprintf("%s:1-%d", binlogTx.SID, binlogTx.GNO), txMsg); err != nil {
								e.onError(TaskStateDead, err)
								break L
							}
							//send_by_size_full
							e.sendBySizeFullCounter += len(txArray)
							txArray = []*binlog.BinlogTx{}
							txBytes = 0
						}
					}
				case <-time.After(100 * time.Millisecond):
					{
						if len(txArray) != 0 {
							txMsg, err := Encode(&txArray)
							if err != nil {
								e.onError(TaskStateDead, err)
								break L
							}
							if len(txMsg) > e.maxPayload {
								e.onError(TaskStateDead, gonats.ErrMaxPayload)
							}
							if err = e.publish(subject,
								fmt.Sprintf("%s:1-%d",
									txArray[len(txArray)-1].SID,
									txArray[len(txArray)-1].GNO),
								txMsg); err != nil {
								e.onError(TaskStateDead, err)
								break L
							}
							//send_by_timeout
							e.sendByTimeoutCounter += len(txArray)
							txArray = []*binlog.BinlogTx{}
							txBytes = 0
						}
					}
				case <-e.shutdownCh:
					break L
				}
			}
		}()
		// The next should block and execute forever, unless there's a serious error
		if err := e.binlogReader.BinlogStreamEvents(e.binlogChannel); err != nil {
			if e.shutdown {
				return nil
			}
			return fmt.Errorf("mysql.extractor: StreamEvents encountered unexpected error: %+v", err)
		}
		// endregion
	}

	return nil
}

// retryOperation attempts up to `count` attempts at running given function,
// exiting as soon as it returns with non-error.
func (e *Extractor) publish(subject, gtid string, txMsg []byte) (err error) {
	for {
		e.logger.Debugf("mysql.extractor: publish. gtid: %v, msg_len: %v", gtid, len(txMsg))
		_, err = e.natsConn.Request(subject, txMsg, DefaultConnectWait)
		if err == nil {
			if gtid != "" {
				e.mysqlContext.Gtid = gtid
			}
			break
		} else if err == gonats.ErrTimeout {
			e.logger.Debugf("mysql.extractor: publish timeout, got %v", err)
			continue
		} else {
			e.logger.Errorf("mysql.extractor: unexpected error on publish, got %v", err)
			break
		}
		// there's an error. Let's try again.
		e.logger.Debugf(fmt.Sprintf("mysql.extractor: there's an error [%v]. Let's try again", err))
		time.Sleep(1 * time.Second)
	}
	return err
}

func (e *Extractor) testStub1() {
	if e.testStub1Delay > 0 {
		e.logger.Info("teststub1 delay start")
		time.Sleep(time.Duration(e.testStub1Delay) * time.Millisecond)
		e.logger.Info("teststub1 delay end")
	}
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
	e.logger.Printf("mysql.extractor: Step %d: disabling autocommit and enabling repeatable read transactions", step)

	// Generate the DDL statements that set the charset-related system variables ...
	if err := e.readMySqlCharsetSystemVariables(); err != nil {
		return err
	}
	setSystemVariablesStatement := e.setStatementFor()
	if err := e.selectSqlMode(); err != nil {
		return err
	}
	setSqlMode := fmt.Sprintf("SET @@session.sql_mode = '%s'", e.mysqlContext.SqlMode)
	step++

	// ------
	// STEP ?
	// ------
	// Obtain read lock on all tables. This statement closes all open tables and locks all tables
	// for all databases with a global read lock, and it prevents ALL updates while we have this lock.
	// It also ensures that everything we do while we have this lock will be consistent.
	/*e.logger.Printf("mysql.extractor: Step %d: flush and obtain global read lock (preventing writes to database)", step)
	query := "FLUSH TABLES WITH READ LOCK"
	_, err = tx.Exec(query)
	if err != nil {
		e.logger.Printf("[ERR] mysql.extractor: exec %+v, error: %v", query, err)
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
		e.logger.Printf("mysql.extractor: Step %d: start transaction with consistent snapshot", step)
		gtidMatch := false
		gtidMatchRound := 0
		delayBetweenRetries := 200 * time.Millisecond
		for !gtidMatch {
			gtidMatchRound += 1

			// 1
			rows1, err := e.singletonDB.Query("show master status")
			if err != nil {
				e.logger.Errorf("mysql.extractor: get gtid, round: %v, phase 1, err: %v", gtidMatchRound, err)
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
				e.logger.Printf("[ERR] mysql.extractor: exec %+v, error: %v", query, err)
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
			e.logger.Debugf("mysql.extractor: binlog coordinates 1: %+v", binlogCoordinates1)
			e.logger.Debugf("mysql.extractor: binlog coordinates 2: %+v", binlogCoordinates2)

			if binlogCoordinates1.GtidSet == binlogCoordinates2.GtidSet {
				gtidMatch = true
				e.logger.Infof("Got gtid after %v rounds", gtidMatchRound)

				// Obtain the binlog position and update the SourceInfo in the context. This means that all source records generated
				// as part of the snapshot will contain the binlog position of the snapshot.
				//binlogCoordinates, err := base.GetSelfBinlogCoordinatesWithTx(tx)

				e.initialBinlogCoordinates = binlogCoordinates2
				e.logger.Printf("mysql.extractor: Step %d: read binlog coordinates of MySQL master: %+v", step, *e.initialBinlogCoordinates)

				defer func() {
					/*e.logger.Printf("mysql.extractor: Step %d: releasing global read lock to enable MySQL writes", step)
					query := "UNLOCK TABLES"
					_, err := tx.Exec(query)
					if err != nil {
						e.logger.Printf("[ERR] mysql.extractor: exec %+v, error: %v", query, err)
					}
					step++*/
					e.logger.Printf("mysql.extractor: Step %d: committing transaction", step)
					if err := realTx.Commit(); err != nil {
						e.onError(TaskStateDead, err)
					}
				}()
			} else {
				e.logger.Warningf("Failed got a consistenct TX with GTID in %v rounds. Will retry.", gtidMatchRound)
				err = realTx.Rollback()
				if err != nil {
					return err
				}
				time.Sleep(delayBetweenRetries)
			}
		}
	} else {
		e.logger.Debugf("mysql.extractor: no need to get consistent snapshot")
		tx = e.singletonDB
		rows1, err := tx.Query("show master status")
		if err != nil {
			return err
		}
		e.initialBinlogCoordinates, err = base.ParseBinlogCoordinatesFromRows(rows1)
		if err != nil {
			return err
		}
		e.logger.Debugf("mysql.extractor: got gtid")
	}
	step++

	// ------
	// STEP 4
	// ------
	// Get the list of table IDs for each database. We can't use a prepared statement with MySQL, so we have to
	// build the SQL statement each time. Although in other cases this might lead to SQL injection, in our case
	// we are reading the database names from the database and not taking them from the user ...
	e.logger.Printf("mysql.extractor: Step %d: read list of available tables in each database", step)

	err = e.getSchemaTablesAndMeta()
	if err != nil {
		return err
	}

	// Transform the current schema so that it reflects the *current* state of the MySQL server's contents.
	// First, get the DROP TABLE and CREATE TABLE statement (with keys and constraint definitions) for our tables ...
	if !e.mysqlContext.SkipCreateDbTable {
		e.logger.Printf("mysql.extractor: Step %d: - generating DROP and CREATE statements to reflect current database schemas:%v", step, e.replicateDoDb)
	}
	for _, db := range e.replicateDoDb {
		if len(db.Tables) > 0 {
			for _, tb := range db.Tables {
				if tb.TableSchema != db.TableSchema {
					continue
				}
				total, err := e.CountTableRows(tb)
				if err != nil {
					return err
				}
				tb.Counter = total
				var dbSQL string
				var tbSQL []string
				if !e.mysqlContext.SkipCreateDbTable {
					var err error
					if strings.ToLower(tb.TableSchema) != "mysql" {
						if db.TableSchemaRename != "" {
							dbSQL = fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", tb.TableSchemaRename)
						} else {
							dbSQL = fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", tb.TableSchema)
						}
					}
					if strings.ToLower(tb.TableType) == "view" {
						/*tbSQL, err = base.ShowCreateView(e.singletonDB, tb.TableSchema, tb.TableName, e.mysqlContext.DropTableIfExists)
						if err != nil {
							return err
						}*/
					} else if strings.ToLower(tb.TableSchema) != "mysql" {
						tbSQL, err = base.ShowCreateTable(e.singletonDB, tb.TableSchema, tb.TableName, e.mysqlContext.DropTableIfExists, true)
						for num, sql := range tbSQL {
							if db.TableSchemaRename != "" && strings.Contains(sql, fmt.Sprintf("USE %s", tb.TableSchema)) {
								tbSQL[num] = strings.Replace(sql, tb.TableSchema, db.TableSchemaRename, 1)
							}
							if tb.TableRename != "" && (strings.Contains(sql, fmt.Sprintf("DROP TABLE IF EXISTS `%s`", tb.TableName)) || strings.Contains(sql, "CREATE TABLE")) {
								tbSQL[num] = strings.Replace(sql, tb.TableName, tb.TableRename, 1)
							}
						}
						if err != nil {
							return err
						}
					}
				}
				entry := &DumpEntry{
					SystemVariablesStatement: setSystemVariablesStatement,
					SqlMode:                  setSqlMode,
					DbSQL:                    dbSQL,
					TbSQL:                    tbSQL,
					TotalCount:               tb.Counter + 1,
					RowsCount:                1,
				}
				atomic.AddInt64(&e.mysqlContext.RowsEstimate, 1)
				atomic.AddInt64(&e.mysqlContext.TotalRowsCopied, 1)
				if err := e.encodeDumpEntry(entry); err != nil {
					e.onError(TaskStateRestart, err)
				}
			}
			e.tableCount += len(db.Tables)
		} else {
			var dbSQL string
			if !e.mysqlContext.SkipCreateDbTable {
				if strings.ToLower(db.TableSchema) != "mysql" {
					if db.TableSchemaRename != "" {
						dbSQL = fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", db.TableSchemaRename)
					} else {
						dbSQL = fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", db.TableSchema)
					}

				}
			}
			entry := &DumpEntry{
				SystemVariablesStatement: setSystemVariablesStatement,
				SqlMode:                  setSqlMode,
				DbSQL:                    dbSQL,
				TotalCount:               1,
				RowsCount:                1,
			}
			atomic.AddInt64(&e.mysqlContext.RowsEstimate, 1)
			atomic.AddInt64(&e.mysqlContext.TotalRowsCopied, 1)
			if err := e.encodeDumpEntry(entry); err != nil {
				e.onError(TaskStateRestart, err)
			}
		}
	}
	step++

	// ------
	// STEP 5
	// ------
	// Dump all of the tables and generate source records ...
	e.logger.Printf("mysql.extractor: Step %d: scanning contents of %d tables", step, e.tableCount)
	startScan := utils.CurrentTimeMillis()
	counter := 0
	//pool := models.NewPool(10)
	for _, db := range e.replicateDoDb {
		for _, t := range db.Tables {
			//pool.Add(1)
			//go func(t *config.Table) {
			counter++
			// Obtain a record maker for this table, which knows about the schema ...
			// Choose how we create statements based on the # of rows ...
			e.logger.Printf("mysql.extractor: Step %d: - scanning table '%s.%s' (%d of %d tables)", step, t.TableSchema, t.TableName, counter, e.tableCount)

			d := NewDumper(tx, t, e.mysqlContext.ChunkSize, e.logger)
			if err := d.Dump(); err != nil {
				e.onError(TaskStateDead, err)
			}
			e.dumpers = append(e.dumpers, d)
			// Scan the rows in the table ...
			for entry := range d.resultsChannel {
				if entry.err != nil {
					e.onError(TaskStateDead, entry.err)
				} else {
					entry.SystemVariablesStatement = setSystemVariablesStatement
					entry.SqlMode = setSqlMode

					if e.needToSendTabelDef() {
						entry.Table = d.table
					}
					if err = e.encodeDumpEntry(entry); err != nil {
						e.onError(TaskStateRestart, err)
					}
					atomic.AddInt64(&e.mysqlContext.TotalRowsCopied, entry.RowsCount)
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
	stop := utils.CurrentTimeMillis()
	e.logger.Printf("mysql.extractor: Step %d: scanned %d rows in %d tables in %s",
		step, e.mysqlContext.TotalRowsCopied, e.tableCount, time.Duration(stop-startScan))
	step++

	return nil
}
func (e *Extractor) encodeDumpEntry(entry *DumpEntry) error {
	txMsg, err := Encode(entry)
	if err != nil {
		return err
	}
	if err := e.publish(fmt.Sprintf("%s_full", e.subject), "", txMsg); err != nil {
		return err
	}
	e.mysqlContext.Stage = models.StageSendingData
	return nil
}

func (e *Extractor) Stats() (*models.TaskStatistics, error) {
	totalRowsCopied := e.mysqlContext.GetTotalRowsCopied()
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
		e.mysqlContext.Stage = models.StageMasterHasSentAllBinlogToSlave
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

	taskResUsage := models.TaskStatistics{
		ExecMasterRowCount: totalRowsCopied,
		ExecMasterTxCount:  deltaEstimate,
		ReadMasterRowCount: rowsEstimate,
		ReadMasterTxCount:  deltaEstimate,
		ProgressPct:        strconv.FormatFloat(progressPct, 'f', 1, 64),
		ETA:                eta,
		Backlog:            fmt.Sprintf("%d/%d", len(e.dataChannel), cap(e.dataChannel)),
		Stage:              e.mysqlContext.Stage,
		BufferStat: models.BufferStat{
			ExtractorTxQueueSize: len(e.binlogChannel),
			SendByTimeout:        e.sendByTimeoutCounter,
			SendBySizeFull:       e.sendBySizeFullCounter,
		},
		Timestamp: time.Now().UTC().UnixNano(),
	}
	if e.natsConn != nil {
		taskResUsage.MsgStat = e.natsConn.Statistics
		e.mysqlContext.TotalTransferredBytes = int(taskResUsage.MsgStat.OutBytes)
		if e.mysqlContext.TrafficAgainstLimits > 0 && int(taskResUsage.MsgStat.OutBytes)/1024/1024/1024 >= e.mysqlContext.TrafficAgainstLimits {
			e.onError(TaskStateDead, fmt.Errorf("traffic limit exceeded : %d/%d", e.mysqlContext.TrafficAgainstLimits, int(taskResUsage.MsgStat.OutBytes)/1024/1024/1024))
		}
	}

	currentBinlogCoordinates := &base.BinlogCoordinateTx{}
	if e.binlogReader != nil {
		currentBinlogCoordinates = e.binlogReader.GetCurrentBinlogCoordinates()
		taskResUsage.CurrentCoordinates = &models.CurrentCoordinates{
			File:     currentBinlogCoordinates.LogFile,
			Position: currentBinlogCoordinates.LogPos,
			GtidSet:  fmt.Sprintf("%s:%d", currentBinlogCoordinates.GetSid(), currentBinlogCoordinates.GNO),
		}
	} else {
		taskResUsage.CurrentCoordinates = &models.CurrentCoordinates{
			File:     "",
			Position: 0,
			GtidSet:  "",
		}
	}

	return &taskResUsage, nil
}

func (e *Extractor) ID() string {
	id := config.DriverCtx{
		DriverConfig: &config.MySQLDriverConfig{
			TotalTransferredBytes: e.mysqlContext.TotalTransferredBytes,
			ReplicateDoDb:         e.mysqlContext.ReplicateDoDb,
			ReplicateIgnoreDb:     e.mysqlContext.ReplicateIgnoreDb,
			Gtid:                  e.mysqlContext.Gtid,
			NatsAddr:              e.mysqlContext.NatsAddr,
			ConnectionConfig:      e.mysqlContext.ConnectionConfig,
		},
	}

	data, err := json.Marshal(id)
	if err != nil {
		e.logger.Errorf("mysql.extractor: Failed to marshal ID to JSON: %s", err)
	}
	return string(data)
}

func (e *Extractor) onError(state int, err error) {
	e.logger.Errorf("mysql.extractor. error: %v", err.Error())
	if e.shutdown {
		return
	}
	e.waitCh <- models.NewWaitResult(state, err)
	e.Shutdown()
}

func (e *Extractor) WaitCh() chan *models.WaitResult {
	return e.waitCh
}

// Shutdown is used to tear down the extractor
func (e *Extractor) Shutdown() error {
	e.shutdownLock.Lock()
	defer e.shutdownLock.Unlock()

	if e.shutdown {
		return nil
	}
	e.shutdown = true
	close(e.shutdownCh)

	if e.natsConn != nil {
		e.natsConn.Close()
	}

	for _, d := range e.dumpers {
		d.Close()
	}

	if err := sql.CloseDB(e.singletonDB); err != nil {
		return err
	}

	if e.binlogReader != nil {
		if err := e.binlogReader.Close(); err != nil {
			return err
		}
	}

	if err := sql.CloseDB(e.db); err != nil {
		return err
	}

	//close(e.binlogChannel)
	e.logger.Printf("mysql.extractor: Shutting down")
	return nil
}

func (e *Extractor) needToSendTabelDef() bool {
	return true
}
