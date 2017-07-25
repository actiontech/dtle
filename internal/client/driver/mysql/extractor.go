package mysql

import (
	gosql "database/sql"
	"encoding/json"
	"fmt"
	//"math"
	"bytes"
	"encoding/gob"
	"strings"
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
)

const (
	// DefaultConnectWait is the default timeout used for the connect operation
	DefaultConnectWait            = 30 * time.Minute
	AllEventsUpToLockProcessed    = "AllEventsUpToLockProcessed"
	ReconnectStreamerSleepSeconds = 5
)

// Extractor is the main schema extract flow manager.
type Extractor struct {
	logger                     *log.Entry
	subject                    string
	tp                         string
	mysqlContext               *config.MySQLDriverConfig
	db                         *gosql.DB
	databases                  []string
	tables                     []*config.Table
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

	sendByTimeoutCounter  int
	sendBySizeFullCounter int

	natsConn *gonats.Conn
	waitCh   chan *models.WaitResult
}

func NewExtractor(subject, tp string, cfg *config.MySQLDriverConfig, logger *log.Logger) *Extractor {
	cfg = cfg.SetDefault()
	entry := log.NewEntry(logger).WithFields(log.Fields{
		"job": subject,
	})
	extractor := &Extractor{
		logger:                     entry,
		subject:                    subject,
		tp:                         tp,
		mysqlContext:               cfg,
		tables:                     make([]*config.Table, 0),
		binlogChannel:              make(chan *binlog.BinlogTx, cfg.ReplChanBufferSize),
		dataChannel:                make(chan *binlog.BinlogEntry, cfg.ReplChanBufferSize),
		parser:                     sql.NewParser(),
		rowCopyComplete:            make(chan bool),
		allEventsUpToLockProcessed: make(chan string),
		waitCh: make(chan *models.WaitResult, 1),
	}
	return extractor
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

// validateStatement validates the `alter` statement meets criteria.
// At this time this means:
// - column renames are approved
func (e *Extractor) validateStatement(doTb *config.Table) (err error) {
	if e.parser.HasNonTrivialRenames() && !e.mysqlContext.SkipRenamedColumns {
		doTb.ColumnRenameMap = e.parser.GetNonTrivialRenames()
		e.logger.Printf("mysql.extractor: Alter statement has column(s) renamed. udup finds the following renames: %v.", e.parser.GetNonTrivialRenames())
	}
	doTb.DroppedColumnsMap = e.parser.DroppedColumnsMap()
	return nil
}

// Run executes the complete extract logic.
func (e *Extractor) Run() {
	e.logger.Printf("mysql.extractor: extract binlog events from %s.%d", e.mysqlContext.ConnectionConfig.Host, e.mysqlContext.ConnectionConfig.Port)
	e.mysqlContext.StartTime = time.Now()
	if err := e.initiateInspector(); err != nil {
		e.onError(err)
		return
	}
	for _, doDb := range e.mysqlContext.ReplicateDoDb {
		for _, doTb := range doDb.Tables {
			if err := e.parser.ParseAlterStatement(doTb.AlterStatement); err != nil {
				e.onError(err)
				return
			}
			if err := e.validateStatement(doTb); err != nil {
				e.onError(err)
				return
			}
			/*if err := e.inspector.inspectTables(doDb.Database, doTb); err != nil {
				e.logger.Errorf("mysql.extractor: unexpected error on inspectOriginalAndGhostTables, got %v", err)
				return err
			}*/
			/*if err := e.ReadMigrationRangeValues(doDb.Database, doTb); err != nil {
				e.logger.Errorf("mysql.extractor: unexpected error on ReadMigrationRangeValues, got %v", err)
				return err
			}*/
		}
	}
	if err := e.initNatsPubClient(); err != nil {
		e.onError(err)
		return
	}
	if err := e.initDBConnections(); err != nil {
		e.onError(err)
		return
	}
	if e.mysqlContext.Gtid == "" {
		e.mysqlContext.RowCopyStartTime = time.Now()
		if err := e.mysqlDump(); err != nil {
			e.onError(err)
			return
		}
		e.logger.Printf("mysql.extractor: Row copy complete")
	} else {
		if err := e.readCurrentBinlogCoordinates(); err != nil {
			e.onError(err)
			return
		}
	}

	if err := e.initBinlogReader(e.initialBinlogCoordinates); err != nil {
		e.onError(err)
		return
	}

	if err := e.initiateStreaming(); err != nil {
		e.onError(err)
		return
	}

	if e.tp == models.JobTypeMig {
		for {
			binlogCoordinates, err := base.GetSelfBinlogCoordinates(e.db)
			if err != nil {
				e.onError(err)
				return
			}
			if e.mysqlContext.Gtid == binlogCoordinates.DisplayString() {
				e.waitCh <- models.NewWaitResult(0, nil)
			}
		}
	}

	/*if err := e.retryOperation(e.cutOver); err != nil {
		e.onError(err)
		return
	}*/
}

// cutOver performs the final step of migration, based on migration
// type (on replica? atomic? safe?)
func (e *Extractor) cutOver() (err error) {
	e.mysqlContext.MarkPointOfInterest()
	e.logger.Debugf("mysql.extractor: checking for cut-over postpone")
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
	e.mysqlContext.MarkPointOfInterest()
	e.logger.Printf("mysql.extractor: checking for cut-over postpone: complete")

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

	e.mysqlContext.MarkPointOfInterest()
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
	pc, err := gonats.Connect(natsAddr)
	if err != nil {
		e.logger.Errorf("mysql.extractor: can't connect nats server %v. make sure a nats streaming server is running.%v", natsAddr, err)
		return err
	}
	e.logger.Debugf("mysql.extractor: connect nats server %v", natsAddr)
	e.natsConn = pc

	return nil
}

// initiateStreaming begins treaming of binary log events and registers listeners for such events
func (e *Extractor) initiateStreaming() error {
	go func() {
		e.logger.Debugf("mysql.extractor: beginning streaming")
		err := e.StreamEvents(e.mysqlContext.ApproveHeterogeneous, e.canStopStreaming)
		if err != nil {
			e.onError(err)
		}
	}()

	go func() {
		_, err := e.natsConn.Subscribe(fmt.Sprintf("%s_restart", e.subject), func(m *gonats.Msg) {
			e.mysqlContext.Gtid = string(m.Data)
			e.onError(fmt.Errorf("restart"))
		})
		if err != nil {
			e.onError(err)
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
	if err := e.validateConnection(); err != nil {
		return err
	}
	if err := e.readCurrentBinlogCoordinates(); err != nil {
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
	e.logger.Printf("mysql.extractor: connection validated on %s:%d", e.mysqlContext.ConnectionConfig.Host,e.mysqlContext.ConnectionConfig.Port)
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
		sid := e.validateServerUUID()
		for _, gtid := range strings.Split(e.mysqlContext.Gtid, ",") {
			id := strings.Split(gtid, ":")[0]
			if id == sid {
				gtidSet, err := gomysql.ParseMysqlGTIDSet(gtid)
				if err != nil {
					return err
				}
				e.initialBinlogCoordinates = &base.BinlogCoordinates{
					GtidSet: gtidSet.String(),
				}
			} else {
				gtidSet, err := gomysql.ParseMysqlGTIDSet(e.mysqlContext.Gtid)
				if err != nil {
					return err
				}
				e.initialBinlogCoordinates = &base.BinlogCoordinates{
					GtidSet: gtidSet.String(),
				}
			}
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

// Read the MySQL charset-related system variables.
func (e *Extractor) readMySqlCharsetSystemVariables() error {
	query := `SHOW VARIABLES WHERE Variable_name IN ('character_set_server','collation_server')`
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
func (e *Extractor) StreamEvents(approveHeterogeneous bool, canStopStreaming func() bool) error {
	if approveHeterogeneous {
		go func() {
			for binlogEntry := range e.dataChannel {
				if binlogEntry.Events != nil {
					/*if err := e.jsonEncodedConn.Publish(fmt.Sprintf("%s_incr_heterogeneous", e.subject), binlogEntry); err != nil {
						e.logger.Errorf("mysql.extractor: unexpected error on publish, got %v", err)
						e.onError(err)
					}*/
				}
			}
		}()
		// The next should block and execute forever, unless there's a serious error
		var successiveFailures int64
		var lastAppliedRowsEventHint base.BinlogCoordinates
	OUTER_DS:
		for {
			if err := e.binlogReader.DataStreamEvents(canStopStreaming, e.dataChannel); err != nil {
				if atomic.LoadInt64(&e.mysqlContext.ShutdownFlag) > 0 {
					break OUTER_DS
				}
				e.logger.Errorf("mysql.extractor: streamEvents encountered unexpected error: %+v", err)
				e.mysqlContext.MarkPointOfInterest()
				time.Sleep(ReconnectStreamerSleepSeconds * time.Second)

				// See if there's retry overflow
				if e.binlogReader.LastAppliedRowsEventHint.Equals(&lastAppliedRowsEventHint) {
					successiveFailures += 1
				} else {
					successiveFailures = 0
				}
				if successiveFailures > e.mysqlContext.MaxRetries {
					return fmt.Errorf("%d successive failures in streamer reconnect at coordinates %+v", successiveFailures, e.GetReconnectBinlogCoordinates())
				}

				// Reposition at same binlog file.
				lastAppliedRowsEventHint = e.binlogReader.LastAppliedRowsEventHint
				e.logger.Printf("mysql.extractor: reconnecting... Will resume at %+v", lastAppliedRowsEventHint)
				if err := e.initBinlogReader(e.GetReconnectBinlogCoordinates()); err != nil {
					return err
				}
				e.binlogReader.LastAppliedRowsEventHint = lastAppliedRowsEventHint
			}
		}
	} else {
		go func() {
			txArray := []*binlog.BinlogTx{}
			subject := fmt.Sprintf("%s_incr", e.subject)
		OUTER:
			for {
				select {
				case <-time.After(100 * time.Millisecond):
					{
						if len(txArray) != 0 {
							txMsg, err := Encode(&txArray)
							if err != nil {
								e.onError(err)
								break OUTER
							}
							if uint64(len(txMsg)) > 100*1024*1024 {
								e.onError(gonats.ErrMaxPayload)
							}
							if err := e.requestMsg(subject,
								fmt.Sprintf("%s:1-%d",
									txArray[len(txArray)-1].SID,
									txArray[len(txArray)-1].GNO),
								txMsg); err != nil {
								e.onError(err)
								break OUTER
							}
							//send_by_timeout
							e.sendByTimeoutCounter += len(txArray)
							txArray = []*binlog.BinlogTx{}
						}
					}
				case binlogTx := <-e.binlogChannel:
					{
						txArray = append(txArray, binlogTx)
						txMsg, err := Encode(&txArray)
						if err != nil {
							e.onError(err)
							break OUTER
						}
						if uint64(len(txMsg)) > 100*1024*1024 {
							e.onError(gonats.ErrMaxPayload)
							break OUTER
						}
						if uint64(len(txMsg)) > e.mysqlContext.MsgBytesLimit {
							if err := e.requestMsg(subject, fmt.Sprintf("%s:1-%d", binlogTx.SID, binlogTx.GNO), txMsg); err != nil {
								e.onError(err)
								break OUTER
							}
							//send_by_size_full
							e.sendBySizeFullCounter += len(txArray)
							txArray = []*binlog.BinlogTx{}
						}
					}
				}
			}
		}()
		// The next should block and execute forever, unless there's a serious error
		var successiveFailures int64
		var lastAppliedRowsEventHint base.BinlogCoordinates
	OUTER_BS:
		for {
			if err := e.binlogReader.BinlogStreamEvents(e.binlogChannel); err != nil {
				if atomic.LoadInt64(&e.mysqlContext.ShutdownFlag) > 0 {
					break OUTER_BS
				}
				e.logger.Printf("mysql.extractor: streamEvents encountered unexpected error: %+v", err)
				e.mysqlContext.MarkPointOfInterest()
				time.Sleep(ReconnectStreamerSleepSeconds * time.Second)

				// See if there's retry overflow
				if e.binlogReader.LastAppliedRowsEventHint.Equals(&lastAppliedRowsEventHint) {
					successiveFailures += 1
				} else {
					successiveFailures = 0
				}
				if successiveFailures > e.mysqlContext.MaxRetries {
					return fmt.Errorf("%d successive failures in streamer reconnect at coordinates %+v", successiveFailures, e.GetReconnectBinlogCoordinates())
				}

				// Reposition at same binlog file.
				lastAppliedRowsEventHint = e.binlogReader.LastAppliedRowsEventHint
				e.logger.Printf("mysql.extractor: reconnecting... Will resume at %+v", lastAppliedRowsEventHint)
				if err := e.initBinlogReader(e.GetReconnectBinlogCoordinates()); err != nil {
					return err
				}
				e.binlogReader.LastAppliedRowsEventHint = lastAppliedRowsEventHint
			}
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

		_, err := e.natsConn.Request(subject, txMsg, DefaultConnectWait)
		if err == nil {
			if gtid != "" {
				e.mysqlContext.Gtid = gtid
			}
			return nil
		}
		// there's an error. Let's try again.
	}
	return err
}

//Perform the snapshot using the same logic as the "mysqldump" utility.
func (e *Extractor) mysqlDump() error {
	defer func() {
		// ------
		// STEP 7
		// ------
		// Release the read lock if we have not yet done so ...
		/*e.logger.Printf("mysql.extractor: Step 7: releasing global read lock to enable MySQL writes)")
		query := "UNLOCK TABLES"
		_, err := sql.ExecNoPrepare(e.db, query)
		if err != nil {
			e.logger.Errorf("mysql.extractor: exec %+v, error: %v", query, err)
		}*/

		// ------
		// STEP 6
		// ------
		e.logger.Printf("mysql.extractor: Step 6: committing transaction")
		query := "COMMIT"
		_, err := sql.ExecNoPrepare(e.db, query)
		if err != nil {
			e.logger.Errorf("mysql.extractor: exec %+v, error: %v", query, err)
		}
	}()
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
	e.logger.Printf("mysql.extractor: Step 0: disabling autocommit and enabling repeatable read transactions")
	query := "SET AUTOCOMMIT=0"
	_, err := sql.ExecNoPrepare(e.db, query)
	if err != nil {
		e.logger.Errorf("mysql.extractor: exec %+v, error: %v", query, err)
		return err
	}
	query = "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"
	_, err = sql.ExecNoPrepare(e.db, query)
	if err != nil {
		e.logger.Errorf("mysql.extractor: exec %+v, error: %v", query, err)
		return err
	}

	// Generate the DDL statements that set the charset-related system variables ...
	if err := e.readMySqlCharsetSystemVariables(); err != nil {
		return err
	}
	setSystemVariablesStatement := e.setStatementFor()

	// ------
	// STEP 1
	// ------
	// First, start a transaction and request that a consistent MVCC snapshot is obtained immediately.
	// See http://dev.mysql.com/doc/refman/5.7/en/commit.html
	e.logger.Printf("mysql.extractor: Step 1: start transaction with consistent snapshot")
	query = "START TRANSACTION WITH CONSISTENT SNAPSHOT"
	_, err = sql.ExecNoPrepare(e.db, query)
	if err != nil {
		e.logger.Errorf("mysql.extractor: exec %+v, error: %v", query, err)
		return err
	}

	// ------
	// STEP 2
	// ------
	// Obtain read lock on all tables. This statement closes all open tables and locks all tables
	// for all databases with a global read lock, and it prevents ALL updates while we have this lock.
	// It also ensures that everything we do while we have this lock will be consistent.
	/*e.logger.Printf("mysql.extractor: Step 2: flush and obtain global read lock (preventing writes to database)")
	query = "FLUSH TABLES WITH READ LOCK"
	_, err = sql.ExecNoPrepare(e.db, query)
	if err != nil {
		e.logger.Errorf("mysql.extractor: exec %+v, error: %v", query, err)
		return err
	}*/

	// ------
	// STEP 2
	// ------
	// Obtain the binlog position and update the SourceInfo in the context. This means that all source records generated
	// as part of the snapshot will contain the binlog position of the snapshot.
	e.logger.Printf("mysql.extractor: Step 2: read binlog coordinates of MySQL master: %+v", *e.initialBinlogCoordinates)
	if err := e.readCurrentBinlogCoordinates(); err != nil {
		return err
	}

	// ------
	// STEP 3
	// ------
	// Get the list of table IDs for each database. We can't use a prepared statement with MySQL, so we have to
	// build the SQL statement each time. Although in other cases this might lead to SQL injection, in our case
	// we are reading the database names from the database and not taking them from the user ...
	e.logger.Printf("mysql.extractor: Step 3: read list of available tables in each database")

	// Creates a MYSQL Dump based on the options supplied through the dumper.
	if len(e.mysqlContext.ReplicateDoDb) > 0 {
		// ------
		// STEP 4
		// ------
		// Transform the current schema so that it reflects the *current* state of the MySQL server's contents.
		// First, get the DROP TABLE and CREATE TABLE statement (with keys and constraint definitions) for our tables ...
		e.logger.Printf("mysql.extractor: Step 4: generating DROP and CREATE statements to reflect current database schemas")

		for _, doDb := range e.mysqlContext.ReplicateDoDb {
			if doDb.TableSchema == "" {
				continue
			}
			e.databases = append(e.databases, doDb.TableSchema)
			if len(doDb.Tables) == 0 {
				tbs, err := showTables(e.db, doDb.TableSchema)
				if err != nil {
					return err
				}
				doDb.Tables = tbs
			}
			for _, doTb := range doDb.Tables {
				doTb.TableSchema = doDb.TableSchema
				if err := e.inspector.ValidateOriginalTable(doDb.TableSchema, doTb.TableName); err != nil {
					e.logger.Warnf("mysql.extractor: %v", err)
					continue
				}
				e.tables = append(e.tables, doTb)
			}
		}
	} else {
		dbs, err := showDatabases(e.db)
		if err != nil {
			return err
		}
		e.databases = dbs
		// ------
		// STEP 4
		// ------
		// Transform the current schema so that it reflects the *current* state of the MySQL server's contents.
		// First, get the DROP TABLE and CREATE TABLE statement (with keys and constraint definitions) for our tables ...
		e.logger.Printf("mysql.extractor: Step 4: generating DROP and CREATE statements to reflect current database schemas: %v", e.databases)
		for _, dbName := range e.databases {
			tbs, err := showTables(e.db, dbName)
			if err != nil {
				return err
			}
			for _, tb := range tbs {
				if err := e.inspector.ValidateOriginalTable(dbName, tb.TableName); err != nil {
					e.logger.Warnf("mysql.extractor: %v", err)
					continue
				}
				e.tables = append(e.tables, tb)
			}
		}
	}

	// ------
	// STEP 5
	// ------
	// Dump all of the tables and generate source records ...
	e.logger.Printf("mysql.extractor: Step 5: scanning contents of %d tables", len(e.tables))
	startScan := currentTimeMillis()
	totalRowCount := 0
	counter := 0
	pool := models.NewPool(3)
	for _, tb := range e.tables {
		pool.Add(1)
		go func(t *config.Table) {
			// Obtain a record maker for this table, which knows about the schema ...
			// Choose how we create statements based on the # of rows ...
			counter++
			e.logger.Printf("mysql.extractor: Step 5: - scanning table '%s.%s' (%d of %d tables)", t.TableSchema, t.TableName, counter, len(e.tables))

			d := NewDumper(e.db, t.TableSchema, t.TableName, e.logger)
			if err := d.Dump(setSystemVariablesStatement, e.mysqlContext.ParallelWorkers); err != nil {
				e.onError(err)
			}

			// Scan the rows in the table ...
			for i := 0; i < d.entriesCount; i++ {
				entry := <-d.resultsChannel
				if entry.err != nil {
					e.onError(entry.err)
				}
				txMsg, err := Encode(entry)
				if err != nil {
					e.onError(err)
				}
				if err := e.requestMsg(fmt.Sprintf("%s_full", e.subject), "", txMsg); err != nil {
					e.onError(err)
				}
				totalRowCount += int(entry.Counter)
			}
			close(d.resultsChannel)
			pool.Done()
		}(tb)
	}
	pool.Wait()

	time.Sleep(5 * time.Second)
	// We've copied all of the tables, but our buffer holds onto the very last record.
	// First mark the snapshot as complete and then apply the updated offset to the buffered record ...
	stop := currentTimeMillis()
	e.logger.Printf("mysql.extractor: Step 5: scanned %d rows in %d tables in %s",
		totalRowCount, len(e.tables), time.Duration(stop-startScan))

	if len(e.tables) == 0 {
		for _, db := range e.databases {
			dbSQL := fmt.Sprintf("CREATE DATABASE %s", db)
			entry := &dumpEntry{
				SystemVariablesStatement: setSystemVariablesStatement,
				DbSQL: dbSQL,
			}
			txMsg, err := Encode(entry)
			if err != nil {
				e.onError(err)
			}
			if err := e.requestMsg(fmt.Sprintf("%s_full", e.subject), "", txMsg); err != nil {
				e.onError(err)
			}
		}
	}

	return nil
}

func currentTimeMillis() int64 {
	return time.Now().UnixNano() / 1000000
}

func (e *Extractor) Stats() (*models.TaskStatistics, error) {
	taskResUsage := models.TaskStatistics{
		Status: "",
		BufferStat: &models.BufferStat{
			ExtractorTxQueueSize: len(e.binlogChannel),
			SendByTimeout:        e.sendByTimeoutCounter,
			SendBySizeFull:       e.sendBySizeFullCounter,
		},
		Timestamp: time.Now().UTC().UnixNano(),
	}
	if e.natsConn != nil {
		taskResUsage.MsgStat = e.natsConn.Statistics
	}
	/*elapsedTime := e.mysqlContext.ElapsedTime()
	elapsedSeconds := int64(elapsedTime.Seconds())
	totalRowsCopied := e.mysqlContext.GetTotalRowsCopied()
	rowsEstimate := atomic.LoadInt64(&e.mysqlContext.RowsDeltaEstimate */ /*RowsEstimate*/ /*) + atomic.LoadInt64(&e.mysqlContext.RowsDeltaEstimate)
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
	// Before status, let's see if we should print a nice reminder for what exactly we're doing here.
	shouldPrintMigrationStatusHint := (elapsedSeconds%600 == 0)

	if shouldPrintMigrationStatusHint {
		//e.printMigrationStatusHint(writers...)
	}

	var etaSeconds float64 = math.MaxFloat64
	eta := "N/A"
	if progressPct >= 100.0 {
		eta = "due"
	} else if progressPct >= 1.0 {
		elapsedRowCopySeconds := e.mysqlContext.ElapsedRowCopyTime().Seconds()
		totalExpectedSeconds := elapsedRowCopySeconds * float64(rowsEstimate) / float64(totalRowsCopied)
		etaSeconds = totalExpectedSeconds - elapsedRowCopySeconds
		if etaSeconds >= 0 {
			etaDuration := time.Duration(etaSeconds) * time.Second
			eta = base.PrettifyDurationOutput(etaDuration)
		} else {
			eta = "due"
		}
	}

	state := "migrating"
	if atomic.LoadInt64(&e.mysqlContext.CountingRowsFlag) > 0 && !e.mysqlContext.ConcurrentCountTableRows {
		state = "counting rows"
	} else if atomic.LoadInt64(&e.mysqlContext.IsPostponingCutOver) > 0 {
		eta = "due"
		state = "postponing cut-over"
	} */ /*else if isThrottled, throttleReason, _ := e.mysqlContext.IsThrottled(); isThrottled {
		state = fmt.Sprintf("throttled, %s", throttleReason)
	}*/ /*

		shouldPrintStatus := false
		if elapsedSeconds <= 60 {
			shouldPrintStatus = true
		} else if etaSeconds <= 60 {
			shouldPrintStatus = true
		} else if etaSeconds <= 180 {
			shouldPrintStatus = (elapsedSeconds%5 == 0)
		} else if elapsedSeconds <= 180 {
			shouldPrintStatus = (elapsedSeconds%5 == 0)
		} else if e.mysqlContext.TimeSincePointOfInterest().Seconds() <= 60 {
			shouldPrintStatus = (elapsedSeconds%5 == 0)
		} else {
			shouldPrintStatus = (elapsedSeconds%30 == 0)
		}
		if !shouldPrintStatus {
			return nil, nil
		}

		status := fmt.Sprintf("Copy: %d/%d %.1f%%; Applied: %d; Time: %+v(total), %+v(copy); streamer: %+v; State: %s; ETA: %s",
			totalRowsCopied, rowsEstimate, progressPct,
			atomic.LoadInt64(&e.mysqlContext.TotalDMLEventsApplied),
			base.PrettifyDurationOutput(elapsedTime), base.PrettifyDurationOutput(e.mysqlContext.ElapsedRowCopyTime()),
			e.currentBinlogCoordinates,
			state,
			eta,
		)
		//e.logger.Printf("mysql.extractor: copy iteration %d at %d,status:%v", e.mysqlContext.GetIteration(), time.Now().Unix(), status)

		if elapsedSeconds%60 == 0 {
			//e.hooksExecutor.onStatus(status)
		}
		taskResUsage := models.TaskStatistics{
			Stats: &models.Stats{
				Status: status,
			},
			Timestamp: time.Now().UTC().UnixNano(),
		}*/
	return &taskResUsage, nil
}

func (e *Extractor) ID() string {
	id := config.DriverCtx{
		DriverConfig: &config.MySQLDriverConfig{
			ReplicateDoDb:    e.mysqlContext.ReplicateDoDb,
			Gtid:             e.mysqlContext.Gtid,
			NatsAddr:         e.mysqlContext.NatsAddr,
			ConnectionConfig: e.mysqlContext.ConnectionConfig,
		},
	}

	data, err := json.Marshal(id)
	if err != nil {
		e.logger.Errorf("mysql.extractor: failed to marshal ID to JSON: %s", err)
	}
	return string(data)
}

func (e *Extractor) onError(err error) {
	e.logger.Errorf("mysql.extractor: %v", err)
	if atomic.LoadInt64(&e.mysqlContext.ShutdownFlag) > 0 {
		return
	}
	e.waitCh <- models.NewWaitResult(1, err)
	//close(e.waitCh)
	e.Shutdown()
}

func (e *Extractor) WaitCh() chan *models.WaitResult {
	return e.waitCh
}

func (e *Extractor) Shutdown() error {
	atomic.StoreInt64(&e.mysqlContext.ShutdownFlag, 1)
	if e.natsConn != nil {
		e.natsConn.Close()
	}

	if e.binlogReader != nil {
		if err := e.binlogReader.Close(); err != nil {
			return err
		}
	}

	if err := sql.CloseDB(e.db); err != nil {
		return err
	}

	e.logger.Printf("mysql.extractor: closed streamer connection.")
	return nil
}
