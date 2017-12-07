package mysql

import (
	gosql "database/sql"
	"encoding/json"
	"fmt"
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

	"udup/internal/client/driver/mysql/base"
	"udup/internal/client/driver/mysql/binlog"
	"udup/internal/client/driver/mysql/sql"
	"udup/internal/config"
	log "udup/internal/logger"
	"udup/internal/models"
	"os"
)

const (
	// DefaultConnectWait is the default timeout used for the connect operation
	DefaultConnectWait            = 10 * time.Second
	AllEventsUpToLockProcessed    = "AllEventsUpToLockProcessed"
	ReconnectStreamerSleepSeconds = 5
)

// Extractor is the main schema extract flow manager.
type Extractor struct {
	logger                     *log.Entry
	subject                    string
	tp                         string
	maxPayload                 int
	mysqlContext               *config.MySQLDriverConfig
	db                         *gosql.DB
	singletonDB                *gosql.DB
	dumpers                    []*dumper
	replicateDoDb              []*config.DataSource
	binlogChannel              chan *binlog.BinlogTx
	dataChannel                chan *binlog.BinlogEntry
	parser                     *sql.Parser
	inspector                  *Inspector
	binlogReader               *binlog.BinlogReader
	initialBinlogCoordinates   *base.BinlogCoordinates
	currentBinlogCoordinates   *base.BinlogCoordinates
	rowCopyComplete            chan bool
	allEventsUpToLockProcessed chan string
	rowCopyCompleteFlag        int64
	tableCount                 int

	sendByTimeoutCounter  int
	sendBySizeFullCounter int

	natsConn *gonats.Conn
	waitCh   chan *models.WaitResult

	shutdown     bool
	shutdownCh   chan struct{}
	shutdownLock sync.Mutex

	testStub1Delay int64
}

func NewExtractor(subject, tp string, maxPayload int, cfg *config.MySQLDriverConfig, logger *log.Logger) *Extractor {
	cfg = cfg.SetDefault()
	entry := log.NewEntry(logger).WithFields(log.Fields{
		"job": subject,
	})
	e := &Extractor{
		logger:                     entry,
		subject:                    subject,
		tp:                         tp,
		maxPayload:                 maxPayload,
		mysqlContext:               cfg,
		binlogChannel:              make(chan *binlog.BinlogTx, cfg.ReplChanBufferSize),
		dataChannel:                make(chan *binlog.BinlogEntry, cfg.ReplChanBufferSize),
		parser:                     sql.NewParser(),
		rowCopyComplete:            make(chan bool),
		allEventsUpToLockProcessed: make(chan string),
		waitCh:                     make(chan *models.WaitResult, 1),
		shutdownCh:                 make(chan struct{}),
		testStub1Delay:             0,
	}

	if delay, err := strconv.ParseInt(os.Getenv("UDUP_TESTSTUB1_DELAY"), 10, 64); err == nil {
		e.logger.Infof("UDUP_TESTSTUB1_DELAY = %v", delay)
		e.testStub1Delay = delay
	}

	return e
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

// consumeRowCopyComplete blocks on the rowCopyComplete channel once, and then
// consumes and drops any further incoming events that may be left hanging.
func (e *Extractor) consumeRowCopyComplete() {
	<-e.rowCopyComplete
	atomic.StoreInt64(&e.rowCopyCompleteFlag, 1)
	e.mysqlContext.MarkRowCopyEndTime()
	/*go func() {
		for <-e.rowCopyComplete {
		}
	}()*/
}

func (e *Extractor) canStopStreaming() bool {
	return atomic.LoadInt64(&e.mysqlContext.CutOverCompleteFlag) != 0
}

// Run executes the complete extract logic.
func (e *Extractor) Run() {
	e.logger.Printf("mysql.extractor: Extract binlog events from %s.%d", e.mysqlContext.ConnectionConfig.Host, e.mysqlContext.ConnectionConfig.Port)
	e.mysqlContext.StartTime = time.Now()
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
	if e.mysqlContext.Gtid == "" {
		e.mysqlContext.MarkRowCopyStartTime()
		if err := e.mysqlDump(); err != nil {
			e.onError(TaskStateDead, err)
			return
		}
		dumpMsg, err := Encode(&dumpStatResult{TotalCount: e.mysqlContext.RowsEstimate})
		if err != nil {
			e.onError(TaskStateDead, err)
		}
		if err := e.requestMsg(fmt.Sprintf("%s_full_complete", e.subject), "", dumpMsg); err != nil {
			e.onError(TaskStateDead, err)
		}
	} else {
		if err := e.readCurrentBinlogCoordinates(); err != nil {
			e.onError(TaskStateDead, err)
			return
		}
	}

	if err := e.initBinlogReader(e.initialBinlogCoordinates); err != nil {
		e.onError(TaskStateDead, err)
		return
	}

	if err := e.initiateStreaming(); err != nil {
		e.onError(TaskStateDead, err)
		return
	}

	/*if e.tp == models.JobTypeMig {
		for {
			binlogCoordinates, err := base.GetSelfBinlogCoordinates(e.db)
			if err != nil {
				e.onError(TaskStateDead, err)
				break
			}

			if e.initialBinlogCoordinates.DisplayString() == binlogCoordinates.DisplayString() {
				if err := e.requestMsg(fmt.Sprintf("%s_incr_complete", e.subject), "", []byte("0")); err != nil {
					e.onError(TaskStateDead, err)
					return
				}
				e.onDone()
				break
			}

			if e.mysqlContext.Gtid != "" && binlogCoordinates.DisplayString() != "" {
				equals, err := base.ContrastGtidSet(e.mysqlContext.Gtid, binlogCoordinates.DisplayString())
				if err != nil {
					e.onError(TaskStateDead, err)
					break
				}

				if equals {
					if err := e.requestMsg(fmt.Sprintf("%s_incr_complete", e.subject), "", []byte("1")); err != nil {
						e.onError(TaskStateDead, err)
						return
					}
					e.onDone()
					break
				}
			}
			time.Sleep(time.Second)
		}
	}*/
}

// cutOver performs the final step of migration, based on migration
// type (on replica? atomic? safe?)
func (e *Extractor) cutOver() (err error) {
	e.logger.Debugf("mysql.extractor: Checking for cut-over postpone")
	e.sleepWhileTrue(
		func() (bool, error) {
			if e.mysqlContext.PostponeCutOverFlagFile == "" {
				return false, nil
			}
			if atomic.LoadInt64(&e.mysqlContext.UserCommandedUnpostponeFlag) > 0 {
				atomic.StoreInt64(&e.mysqlContext.UserCommandedUnpostponeFlag, 0)
				return false, nil
			}
			if base.FileExists(e.mysqlContext.PostponeCutOverFlagFile) {
				atomic.StoreInt64(&e.mysqlContext.IsPostponingCutOver, 1)
				return true, nil
			}
			return false, nil
		},
	)
	atomic.StoreInt64(&e.mysqlContext.IsPostponingCutOver, 0)
	e.logger.Printf("mysql.extractor: Checking for cut-over postpone: complete")

	if e.mysqlContext.CutOverType == config.CutOverAtomic {
		// Atomic solution: we use low timeout and multiple attempts. But for
		// each failed attempt, we throttle until replication lag is back to normal
		for _, doDb := range e.mysqlContext.ReplicateDoDb {
			for _, doTb := range doDb.Tables {
				err := e.atomicCutOver(doTb.TableName)
				return err
			}
		}
		return nil
	}
	if e.mysqlContext.CutOverType == config.CutOverTwoStep {
		err := e.cutOverTwoStep()
		return err
	}
	return fmt.Errorf("Unknown cut-over type: %d; should never get here!", e.mysqlContext.CutOverType)
}

// Inject the "AllEventsUpToLockProcessed" state hint, wait for it to appear in the binary logs,
// make sure the queue is drained.
func (e *Extractor) waitForEventsUpToLock() (err error) {
	timeout := time.NewTimer(time.Second * time.Duration(e.mysqlContext.CutOverLockTimeoutSeconds))

	waitForEventsUpToLockStartTime := time.Now()

	allEventsUpToLockProcessedChallenge := fmt.Sprintf("%s:%d", string(AllEventsUpToLockProcessed), waitForEventsUpToLockStartTime.UnixNano())
	e.logger.Printf("mysql.extractor: Writing changelog state: %+v", allEventsUpToLockProcessedChallenge)

	e.logger.Printf("Waiting for events up to lock")
	atomic.StoreInt64(&e.mysqlContext.AllEventsUpToLockProcessedInjectedFlag, 1)
	for found := false; !found; {
		select {
		case <-timeout.C:
			{
				return fmt.Errorf("Timeout while waiting for events up to lock")
			}
		case state := <-e.allEventsUpToLockProcessed:
			{
				if state == allEventsUpToLockProcessedChallenge {
					e.logger.Printf("mysql.extractor: Waiting for events up to lock: got %s", state)
					found = true
				} else {
					e.logger.Printf("mysql.extractor: Waiting for events up to lock: skipping %s", state)
				}
			}
		}
	}
	waitForEventsUpToLockDuration := time.Since(waitForEventsUpToLockStartTime)

	e.logger.Printf("mysql.extractor: Done waiting for events up to lock; duration=%+v", waitForEventsUpToLockDuration)

	return nil
}

// cutOverTwoStep will lock down the original table, execute
// what's left of last DML entries, and **non-atomically** swap original->old, then new->original.
// There is a point in time where the "original" table does not exist and queries are non-blocked
// and failing.
func (e *Extractor) cutOverTwoStep() (err error) {
	atomic.StoreInt64(&e.mysqlContext.InCutOverCriticalSectionFlag, 1)
	defer atomic.StoreInt64(&e.mysqlContext.InCutOverCriticalSectionFlag, 0)
	atomic.StoreInt64(&e.mysqlContext.AllEventsUpToLockProcessedInjectedFlag, 0)

	/*if err := e.retryOperation(e.applier.LockOriginalTable); err != nil {
		return err
	}*/

	if err := e.retryOperation(e.waitForEventsUpToLock); err != nil {
		return err
	}
	/*if err := e.retryOperation(e.applier.UnlockTables); err != nil {
		return err
	}*/

	//lockAndRenameDuration := e.mysqlContext.RenameTablesEndTime.Sub(e.mysqlContext.LockTablesStartTime)
	//renameDuration := e.mysqlContext.RenameTablesEndTime.Sub(e.mysqlContext.RenameTablesStartTime)
	//e.logger.Debugf("mysql.extractor: Lock & rename duration: %s (rename only: %s). During this time, queries on %s were locked or failing", lockAndRenameDuration, renameDuration, sql.EscapeName(e.mysqlContext.OriginalTableName))
	return nil
}

// atomicCutOver
func (e *Extractor) atomicCutOver(tableName string) (err error) {
	atomic.StoreInt64(&e.mysqlContext.InCutOverCriticalSectionFlag, 1)
	defer atomic.StoreInt64(&e.mysqlContext.InCutOverCriticalSectionFlag, 0)

	okToUnlockTable := make(chan bool, 4)
	defer func() {
		okToUnlockTable <- true
		//e.applier.DropAtomicCutOverSentryTableIfExists()
	}()

	atomic.StoreInt64(&e.mysqlContext.AllEventsUpToLockProcessedInjectedFlag, 0)

	lockOriginalSessionIdChan := make(chan int64, 2)
	tableLocked := make(chan error, 2)
	tableUnlocked := make(chan error, 2)
	/*go func() {
		if err := e.applier.AtomicCutOverMagicLock(lockOriginalSessionIdChan, tableLocked, okToUnlockTable, tableUnlocked); err != nil {
			e.logger.Errorf("%v",err)
		}
	}()*/
	if err := <-tableLocked; err != nil {
		return err
	}
	lockOriginalSessionId := <-lockOriginalSessionIdChan
	e.logger.Printf("mysql.extractor: Session locking original & magic tables is %+v", lockOriginalSessionId)
	// At this point we know the original table is locked.
	// We know any newly incoming DML on original table is blocked.
	if err := e.waitForEventsUpToLock(); err != nil {
		return err
	}

	// Step 2
	// We now attempt an atomic RENAME on original & ghost tables, and expect it to block.
	e.mysqlContext.RenameTablesStartTime = time.Now()

	var tableRenameKnownToHaveFailed int64
	renameSessionIdChan := make(chan int64, 2)
	tablesRenamed := make(chan error, 2)
	/*go func() {
		if err := e.applier.AtomicCutoverRename(renameSessionIdChan, tablesRenamed); err != nil {
			// Abort! Release the lock
			atomic.StoreInt64(&tableRenameKnownToHaveFailed, 1)
			okToUnlockTable <- true
		}
	}()*/
	renameSessionId := <-renameSessionIdChan
	e.logger.Printf("mysql.extractor: Session renaming tables is %+v", renameSessionId)

	waitForRename := func() error {
		if atomic.LoadInt64(&tableRenameKnownToHaveFailed) == 1 {
			// We return `nil` here so as to avoid the `retry`. The RENAME has failed,
			// it won't show up in PROCESSLIST, no point in waiting
			return nil
		}
		//return e.applier.ExpectProcess(renameSessionId, "metadata lock", "rename")
		return nil
	}
	// Wait for the RENAME to appear in PROCESSLIST
	if err := e.retryOperation(waitForRename, true); err != nil {
		// Abort! Release the lock
		okToUnlockTable <- true
		return err
	}
	if atomic.LoadInt64(&tableRenameKnownToHaveFailed) == 0 {
		e.logger.Printf("mysql.extractor: Found atomic RENAME to be blocking, as expected. Double checking the lock is still in place (though I don't strictly have to)")
	}
	/*if err := e.applier.ExpectUsedLock(lockOriginalSessionId); err != nil {
		// Abort operation. Just make sure to drop the magic table.
		return err
	}*/
	e.logger.Printf("mysql.extractor: Connection holding lock on original table still exists")

	// Now that we've found the RENAME blocking, AND the locking connection still alive,
	// we know it is safe to proceed to release the lock

	okToUnlockTable <- true
	// BAM! magic table dropped, original table lock is released
	// -> RENAME released -> queries on original are unblocked.
	if err := <-tableUnlocked; err != nil {
		return err
	}
	if err := <-tablesRenamed; err != nil {
		return err
	}
	e.mysqlContext.RenameTablesEndTime = time.Now()

	lockAndRenameDuration := e.mysqlContext.RenameTablesEndTime.Sub(e.mysqlContext.LockTablesStartTime)
	e.logger.Printf("mysql.extractor: Lock & rename duration: %s. During this time, queries on %s were blocked", lockAndRenameDuration, sql.EscapeName(tableName))
	return nil
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

	if err := e.inspector.validateLogSlaveUpdates(); err != nil {
		return err
	}

	return nil
}

func (e *Extractor) inspectTables() (err error) {
	// Creates a MYSQL Dump based on the options supplied through the dumper.
	if len(e.mysqlContext.ReplicateDoDb) > 0 {
		for _, doDb := range e.mysqlContext.ReplicateDoDb {
			if doDb.TableSchema == "" {
				continue
			}
			db := &config.DataSource{
				TableSchema: doDb.TableSchema,
			}

			if len(doDb.Tables) == 0 {
				tbs, err := sql.ShowTables(e.db, doDb.TableSchema)
				if err != nil {
					return err
				}
				db.Tables = tbs
			} else {
				db.Tables = doDb.Tables
			}
			e.replicateDoDb = append(e.replicateDoDb, db)
			for _, doTb := range db.Tables {
				doTb.TableSchema = doDb.TableSchema
				if err := e.inspector.ValidateOriginalTable(doDb.TableSchema, doTb.TableName, doTb); err != nil {
					e.logger.Warnf("mysql.extractor: %v", err)
					continue
				}
			}
		}
	} else {
		dbs, err := sql.ShowDatabases(e.db)
		if err != nil {
			return err
		}
		for _, dbName := range dbs {
			ds := &config.DataSource{
				TableSchema: dbName,
			}
			if len(e.mysqlContext.ReplicateIgnoreDb) > 0 && e.ignoreDb(dbName) {
				continue
			}

			tbs, err := sql.ShowTables(e.db, dbName)
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
			doTb.OriginalTableColumns, err = base.GetTableColumns(e.db, doTb.TableSchema, doTb.TableName)
			if err != nil {
				e.logger.Errorf("mysql.extractor: Unexpected error on readTableColumns, got %v", err)
				return err
			}
			if err := base.InspectTables(e.db, doTb.TableSchema, doTb, e.mysqlContext.TimeZone); err != nil {
				e.logger.Errorf("mysql.extractor: unexpected error on inspectTables, got %v", err)
				return err
			}
		}
	}
	return nil
}

// printMigrationStatusHint prints a detailed configuration dump, that is useful
// to keep in mind; such as the name of migrated table, throttle params etc.
// This gets printed at beginning and end of migration, every 10 minutes throughout
// migration, and as reponse to the "status" interactive command.
func (e *Extractor) printMigrationStatusHint(databaseName, tableName string) {
	e.logger.Printf("mysql.extractor # Migrating %s.%s",
		sql.EscapeName(databaseName),
		sql.EscapeName(tableName),
	)
	e.logger.Printf("mysql.extractor # Migration started at %+v",
		e.mysqlContext.StartTime.Format(time.RubyDate),
	)
	maxLoad := e.mysqlContext.GetMaxLoad()
	criticalLoad := e.mysqlContext.GetCriticalLoad()
	e.logger.Printf("mysql.extractor # chunk-size: %+v; max-lag-millis: %+vms; max-load: %s; critical-load: %s; nice-ratio: %f",
		atomic.LoadInt64(&e.mysqlContext.ChunkSize),
		atomic.LoadInt64(&e.mysqlContext.MaxLagMillisecondsThrottleThreshold),
		maxLoad.String(),
		criticalLoad.String(),
		e.mysqlContext.GetNiceRatio(),
	)

	if e.mysqlContext.PostponeCutOverFlagFile != "" {
		setIndicator := ""
		if base.FileExists(e.mysqlContext.PostponeCutOverFlagFile) {
			setIndicator = "[set]"
		}
		e.logger.Printf("mysql.extractor # postpone-cut-over-flag-file: %+v %+v",
			e.mysqlContext.PostponeCutOverFlagFile, setIndicator,
		)
	}
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
		e.logger.Debugf("mysql.extractor: Beginning streaming")
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
	if err := e.inspectTables(); err != nil {
		return err
	}
	if err := e.readTableColumns(); err != nil {
		return err
	}

	return nil
}

// initBinlogReader creates and connects the reader: we hook up to a MySQL server as a replica
func (e *Extractor) initBinlogReader(binlogCoordinates *base.BinlogCoordinates) error {
	binlogReader, err := binlog.NewMySQLReader(e.mysqlContext, e.logger)
	if err != nil {
		return err
	}
	if err := binlogReader.ConnectBinlogStreamer(*binlogCoordinates); err != nil {
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

func (e *Extractor) GetCurrentBinlogCoordinates() *base.BinlogCoordinates {
	return e.binlogReader.GetCurrentBinlogCoordinates()
}

func (e *Extractor) GetReconnectBinlogCoordinates() *base.BinlogCoordinates {
	return &base.BinlogCoordinates{LogFile: e.GetCurrentBinlogCoordinates().LogFile, LogPos: 4}
}

// readCurrentBinlogCoordinates reads master status from hooked server
func (e *Extractor) readCurrentBinlogCoordinates() error {
	if e.mysqlContext.Gtid != "" {
		gtidSet, err := gomysql.ParseMysqlGTIDSet(e.mysqlContext.Gtid)
		if err != nil {
			return err
		}
		e.initialBinlogCoordinates = &base.BinlogCoordinates{
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
func (e *Extractor) CountTableRows(tableSchema, tableName string) (int64, error) {
	atomic.StoreInt64(&e.mysqlContext.CountingRowsFlag, 1)
	defer atomic.StoreInt64(&e.mysqlContext.CountingRowsFlag, 0)
	//e.logger.Debugf("mysql.extractor: As instructed, I'm issuing a SELECT COUNT(*) on the table. This may take a while")

	query := fmt.Sprintf(`select count(*) as rows from %s.%s`, sql.EscapeName(tableSchema), sql.EscapeName(tableName))
	var rowsEstimate int64
	if err := e.db.QueryRow(query).Scan(&rowsEstimate); err != nil {
		return 0, err
	}
	atomic.AddInt64(&e.mysqlContext.RowsEstimate, rowsEstimate)

	e.mysqlContext.Stage = models.StageSearchingRowsForUpdate
	e.logger.Debugf("mysql.extractor: Exact number of rows(%s.%s) via COUNT: %d", tableSchema, tableName, rowsEstimate)
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

func (e *Extractor) validateServerUUID() string {
	query := `SELECT @@SERVER_UUID`
	var server_uuid string
	if err := e.db.QueryRow(query).Scan(&server_uuid); err != nil {
		return ""
	}
	return server_uuid
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
			for binlogEntry := range e.dataChannel {
				if nil != binlogEntry {
					txMsg, err := Encode(binlogEntry)
					if err != nil {
						e.onError(TaskStateDead, err)
						break
					}

					if err = e.requestMsg(fmt.Sprintf("%s_incr_hete", e.subject), "", txMsg); err != nil {
						e.onError(TaskStateDead, err)
						break
					}
					e.mysqlContext.Stage = models.StageSendingBinlogEventToSlave
					atomic.AddInt64(&e.mysqlContext.DeltaEstimate, 1)
				}
			}
		}()
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
							if err = e.requestMsg(subject, fmt.Sprintf("%s:1-%d", binlogEntry.Coordinates.SID, binlogEntry.Coordinates.GNO), txMsg); err != nil {
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
							if err = e.requestMsg(subject,
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
		// The next should block and execute forever, unless there's a serious error
		if err := e.binlogReader.DataStreamEvents(e.dataChannel); err != nil {
			if e.shutdown {
				return nil
			}
			return fmt.Errorf("mysql.extractor: StreamEvents encountered unexpected error: %+v", err)
		}
	} else {
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
							if err = e.requestMsg(subject, fmt.Sprintf("%s:1-%d", binlogTx.SID, binlogTx.GNO), txMsg); err != nil {
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
							if err = e.requestMsg(subject,
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
	}

	return nil
}

// retryOperation attempts up to `count` attempts at running given function,
// exiting as soon as it returns with non-error.
func (e *Extractor) requestMsg(subject, gtid string, txMsg []byte) (err error) {
	for i := 0; i < int(e.mysqlContext.MaxRetries); i++ {
		if i != 0 {
			// sleep after previous iteration
			time.Sleep(1 * time.Second)
		}

		_, err = e.natsConn.Request(subject, txMsg, DefaultConnectWait)
		if err == nil {
			if gtid != "" {
				e.mysqlContext.Gtid = gtid
			}
			return nil
		}
		if err == gonats.ErrTimeout {
			continue
		}
		// there's an error. Let's try again.
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
	var tx *gosql.Tx
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
		};

		e.testStub1()

		// 2
		// TODO it seems that two 'start transaction' will be sent.
		// https://github.com/golang/go/issues/19981
		tx, err = e.singletonDB.Begin()
		if err != nil {
			return err
		}
		query := "START TRANSACTION WITH CONSISTENT SNAPSHOT"
		_, err = tx.Exec(query)
		if err != nil {
			e.logger.Printf("[ERR] mysql.extractor: exec %+v, error: %v", query, err)
			return err
		}

		e.testStub1()

		// 3
		rows2, err := tx.Query("show master status")

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
		} else {
			e.logger.Warningf("Failed got a consistenct TX with GTID in %v rounds. Will retry.", gtidMatchRound)
			err = tx.Rollback()
			if err != nil {
				return err
			}
			time.Sleep(delayBetweenRetries)
		}
	}
	step++

	defer func() {
		/*e.logger.Printf("mysql.extractor: Step %d: releasing global read lock to enable MySQL writes", step)
		query := "UNLOCK TABLES"
		_, err := tx.Exec(query)
		if err != nil {
			e.logger.Printf("[ERR] mysql.extractor: exec %+v, error: %v", query, err)
		}
		step++*/
		e.logger.Printf("mysql.extractor: Step %d: committing transaction", step)
		if err := tx.Commit(); err != nil {
			e.onError(TaskStateDead, err)
		}
	}()

	// ------
	// STEP 4
	// ------
	// Get the list of table IDs for each database. We can't use a prepared statement with MySQL, so we have to
	// build the SQL statement each time. Although in other cases this might lead to SQL injection, in our case
	// we are reading the database names from the database and not taking them from the user ...
	e.logger.Printf("mysql.extractor: Step %d: read list of available tables in each database", step)

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
				total, err := e.CountTableRows(tb.TableSchema, tb.TableName)
				if err != nil {
					return err
				}
				tb.Counter = total
			}
			for _, tb := range db.Tables {
				var dbSQL, tbSQL string
				if !e.mysqlContext.SkipCreateDbTable {
					var err error
					dbSQL = fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", tb.TableSchema)
					tbSQL, err = base.ShowCreateTable(e.singletonDB, tb.TableSchema, tb.TableName, e.mysqlContext.DropTableIfExists)
					if err != nil {
						return err
					}
				}
				entry := &dumpEntry{
					SystemVariablesStatement: setSystemVariablesStatement,
					SqlMode:                  setSqlMode,
					DbSQL:                    dbSQL,
					TbSQL:                    tbSQL,
					TotalCount:               tb.Counter,
				}
				if err := e.encodeDumpEntry(entry); err != nil {
					return err
				}
			}
			e.tableCount += len(db.Tables)
		} else {
			var dbSQL string
			if !e.mysqlContext.SkipCreateDbTable {
				dbSQL = fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", db)
			}
			entry := &dumpEntry{
				SystemVariablesStatement: setSystemVariablesStatement,
				SqlMode:                  setSqlMode,
				DbSQL:                    dbSQL,
			}
			if err := e.encodeDumpEntry(entry); err != nil {
				return err
			}
		}
	}
	step++

	// ------
	// STEP 5
	// ------
	// Dump all of the tables and generate source records ...
	e.logger.Printf("mysql.extractor: Step %d: scanning contents of %d tables", step, e.tableCount)
	startScan := currentTimeMillis()
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

			d := NewDumper(tx, t, t.Counter, e.mysqlContext.ChunkSize, e.logger)
			if err := d.Dump(1); err != nil {
				e.onError(TaskStateDead, err)
			}
			e.dumpers = append(e.dumpers, d)
			// Scan the rows in the table ...
			for i := 0; i < d.entriesCount; i++ {
				entry := <-d.resultsChannel
				if entry.err != nil {
					e.onError(TaskStateDead, entry.err)
				}
				// TODO: entry values may be empty. skip the entry after removing 'start transaction'.
				entry.SystemVariablesStatement = setSystemVariablesStatement
				entry.SqlMode = setSqlMode
				if err = e.encodeDumpEntry(entry); err != nil {
					e.onError(TaskStateDead, err)
				}
				atomic.AddInt64(&e.mysqlContext.TotalRowsCopied, entry.RowsCount)
			}

			close(d.resultsChannel)
			//pool.Done()
			//}(tb)
		}
	}
	//pool.Wait()
	step++

	// We've copied all of the tables, but our buffer holds onto the very last record.
	// First mark the snapshot as complete and then apply the updated offset to the buffered record ...
	stop := currentTimeMillis()
	e.logger.Printf("mysql.extractor: Step %d: scanned %d rows in %d tables in %s",
		step, e.mysqlContext.TotalRowsCopied, e.tableCount, time.Duration(stop-startScan))
	step++

	return nil
}
func (e *Extractor) encodeDumpEntry(entry *dumpEntry) error {
	txMsg, err := Encode(entry)
	if err != nil {
		return err
	}
	if err := e.requestMsg(fmt.Sprintf("%s_full", e.subject), "", txMsg); err != nil {
		return err
	}
	e.mysqlContext.Stage = models.StageSendingData
	return nil
}

func currentTimeMillis() int64 {
	return time.Now().UnixNano() / 1000000
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
		progressPct = 100.0
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
	}

	currentBinlogCoordinates := &base.BinlogCoordinates{}
	if e.binlogReader != nil {
		currentBinlogCoordinates = e.binlogReader.GetCurrentBinlogCoordinates()
		taskResUsage.CurrentCoordinates = &models.CurrentCoordinates{
			File:     currentBinlogCoordinates.LogFile,
			Position: currentBinlogCoordinates.LogPos,
			GtidSet:  fmt.Sprintf("%s:%d", currentBinlogCoordinates.SID, currentBinlogCoordinates.GNO),
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
			ReplicateDoDb:     e.mysqlContext.ReplicateDoDb,
			ReplicateIgnoreDb: e.mysqlContext.ReplicateIgnoreDb,
			Gtid:              e.mysqlContext.Gtid,
			NatsAddr:          e.mysqlContext.NatsAddr,
			ConnectionConfig:  e.mysqlContext.ConnectionConfig,
		},
	}

	data, err := json.Marshal(id)
	if err != nil {
		e.logger.Errorf("mysql.extractor: Failed to marshal ID to JSON: %s", err)
	}
	return string(data)
}

func (e *Extractor) onError(state int, err error) {
	if e.shutdown {
		return
	}
	e.waitCh <- models.NewWaitResult(state, err)
	e.Shutdown()
}

func (e *Extractor) onDone() {
	if e.shutdown {
		return
	}
	e.logger.Printf("mysql.extractor: Row copy complete")
	e.waitCh <- models.NewWaitResult(0, nil)
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
