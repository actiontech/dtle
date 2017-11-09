package mysql

import (
	gosql "database/sql"
	"encoding/json"
	"fmt"
	//"math"
	"bytes"
	"encoding/gob"
	//"encoding/base64"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"strconv"

	"github.com/golang/snappy"
	gonats "github.com/nats-io/go-nats"
	gomysql "github.com/siddontang/go-mysql/mysql"

	"udup/internal/client/driver/mysql/base"
	"udup/internal/client/driver/mysql/binlog"
	"udup/internal/client/driver/mysql/sql"
	"udup/internal/config"
	umconf "udup/internal/config/mysql"
	log "udup/internal/logger"
	"udup/internal/models"
)

const (
	TaskStateComplete int = iota
	TaskStateRestart
	TaskStateDead
)

// Applier connects and writes the the applier-server, which is the server where
// write row data and apply binlog events onto the dest table.
type Applier struct {
	logger             *log.Entry
	subject            string
	tp                 string
	mysqlContext       *config.MySQLDriverConfig
	dbs                []*sql.DB
	db                 *gosql.DB
	parser             *sql.Parser
	retrievedGtidSet   string
	currentCoordinates *models.CurrentCoordinates

	rowCopyComplete            chan bool
	allEventsUpToLockProcessed chan string
	rowCopyCompleteFlag        int64
	// copyRowsQueue should not be buffered; if buffered some non-damaging but
	//  excessive work happens at the end of the iteration as new copy-jobs arrive befroe realizing the copy is complete
	copyRowsQueue            chan *dumpEntry
	applyDataEntryQueue      chan *binlog.BinlogEntry
	applyGrouDataEntrypQueue chan []*binlog.BinlogEntry
	applyBinlogTxQueue       chan *binlog.BinlogTx
	applyBinlogGroupTxQueue  chan []*binlog.BinlogTx
	lastAppliedBinlogTx      *binlog.BinlogTx

	natsConn *gonats.Conn
	waitCh   chan *models.WaitResult
	wg       sync.WaitGroup

	shutdown     bool
	shutdownCh   chan struct{}
	shutdownLock sync.Mutex
}

func NewApplier(subject, tp string, cfg *config.MySQLDriverConfig, logger *log.Logger) *Applier {
	cfg = cfg.SetDefault()
	entry := log.NewEntry(logger).WithFields(log.Fields{
		"job": subject,
	})
	a := &Applier{
		logger:                     entry,
		subject:                    subject,
		tp:                         tp,
		mysqlContext:               cfg,
		parser:                     sql.NewParser(),
		currentCoordinates:         &models.CurrentCoordinates{},
		rowCopyComplete:            make(chan bool, 1),
		allEventsUpToLockProcessed: make(chan string),
		copyRowsQueue:              make(chan *dumpEntry, cfg.ReplChanBufferSize),
		applyDataEntryQueue:        make(chan *binlog.BinlogEntry, cfg.ReplChanBufferSize),
		applyGrouDataEntrypQueue:   make(chan []*binlog.BinlogEntry, cfg.ReplChanBufferSize),
		applyBinlogTxQueue:         make(chan *binlog.BinlogTx, cfg.ReplChanBufferSize),
		applyBinlogGroupTxQueue:    make(chan []*binlog.BinlogTx, cfg.ReplChanBufferSize),
		waitCh:                     make(chan *models.WaitResult, 1),
		shutdownCh:                 make(chan struct{}),
	}
	return a
}

// sleepWhileTrue sleeps indefinitely until the given function returns 'false'
// (or fails with error)
func (a *Applier) sleepWhileTrue(operation func() (bool, error)) error {
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
func (a *Applier) retryOperation(operation func() error, notFatalHint ...bool) (err error) {
	maxRetries := int(a.mysqlContext.MaxRetries)
	for i := 0; i < maxRetries; i++ {
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
func (a *Applier) consumeRowCopyComplete() {
	var rowCount string
	_, err := a.natsConn.Subscribe(fmt.Sprintf("%s_full_complete", a.subject), func(m *gonats.Msg) {
		rowCount = fmt.Sprintf("%s", m.Data)
		if err := a.natsConn.Publish(m.Reply, nil); err != nil {
			a.onError(TaskStateDead, err)
		}
	})
	if err != nil {
		a.onError(TaskStateDead, err)
	}

	for {
		if rowCount == fmt.Sprintf("%v", a.mysqlContext.TotalRowsCopied) {
			a.rowCopyComplete <- true
			break
		} /*else {
			a.onError(fmt.Errorf("we might get an inconsistent data during the dump process"))
		}*/
		time.Sleep(time.Second)
	}

	<-a.rowCopyComplete
	close(a.copyRowsQueue)
	atomic.StoreInt64(&a.rowCopyCompleteFlag, 1)
	a.mysqlContext.MarkRowCopyEndTime()

	go func() {
		for <-a.rowCopyComplete {
		}
	}()
}

// validateStatement validates the `alter` statement meets criteria.
// At this time this means:
// - column renames are approved
func (a *Applier) validateStatement(doTb *config.Table) (err error) {
	if a.parser.HasNonTrivialRenames() && !a.mysqlContext.SkipRenamedColumns {
		doTb.ColumnRenameMap = a.parser.GetNonTrivialRenames()
		a.logger.Printf("mysql.applier: Alter statement has column(s) renamed. udup finds the following renames: %v.", a.parser.GetNonTrivialRenames())
	}
	doTb.DroppedColumnsMap = a.parser.DroppedColumnsMap()
	return nil
}

// Run executes the complete apply logic.
func (a *Applier) Run() {
	a.logger.Printf("mysql.applier: Apply binlog events to %s.%d", a.mysqlContext.ConnectionConfig.Host, a.mysqlContext.ConnectionConfig.Port)
	a.mysqlContext.StartTime = time.Now()
	/*for _, doDb := range a.mysqlContext.ReplicateDoDb {
		for _, doTb := range doDb.Tables {
			if err := a.parser.ParseAlterStatement(doTb.AlterStatement); err != nil {
				a.onError(TaskStateDead, err)
				return
			}
			if err := a.validateStatement(doTb); err != nil {
				a.onError(TaskStateDead, err)
				return
			}
		}
	}*/
	if err := a.initDBConnections(); err != nil {
		a.onError(TaskStateDead, err)
		return
	}
	if err := a.initNatSubClient(); err != nil {
		a.onError(TaskStateDead, err)
		return
	}

	if err := a.initiateStreaming(); err != nil {
		a.onError(TaskStateDead, err)
		return
	}

	go a.executeWriteFuncs()

	/*if a.mysqlContext.Gtid == "" {
		a.logger.Printf("mysql.applier: Operating until row copy is complete")
		a.consumeRowCopyComplete()
		a.logger.Printf("mysql.applier: Row copy complete")
	}*/

	if a.tp == models.JobTypeMig {
		var completeFlag string
		_, err := a.natsConn.Subscribe(fmt.Sprintf("%s_incr_complete", a.subject), func(m *gonats.Msg) {
			completeFlag = string(m.Data)
			if err := a.natsConn.Publish(m.Reply, nil); err != nil {
				a.onError(TaskStateDead, err)
			}
		})
		if err != nil {
			a.onError(TaskStateDead, err)
		}

		for {
			if completeFlag != "" && a.rowCopyCompleteFlag == 1 {
				switch completeFlag {
				case "0":
					a.onError(TaskStateComplete, nil)
					break
				default:
					binlogCoordinates, err := base.GetSelfBinlogCoordinates(a.db)
					if err != nil {
						a.onError(TaskStateDead, err)
						break
					}
					if a.mysqlContext.Gtid != "" && binlogCoordinates.DisplayString() != "" {
						equals, err := base.ContrastGtidSet(a.mysqlContext.Gtid, binlogCoordinates.DisplayString())
						if err != nil {
							a.onError(TaskStateDead, err)
							break
						}
						if equals {
							a.onError(TaskStateComplete, nil)
							break
						}
					}
				}
			}
			time.Sleep(time.Second)
		}
	}
}

// readCurrentBinlogCoordinates reads master status from hooked server
func (a *Applier) readCurrentBinlogCoordinates() error {
	query := `show master status`
	foundMasterStatus := false
	err := sql.QueryRowsMap(a.db, query, func(m sql.RowMap) error {
		if m.GetString("Executed_Gtid_Set") != "" {
			gtidSet, err := gomysql.ParseMysqlGTIDSet(m.GetString("Executed_Gtid_Set"))
			if err != nil {
				return err
			}

			a.mysqlContext.Gtid = gtidSet.String()
		}
		foundMasterStatus = true

		return nil
	})
	if err != nil {
		return err
	}
	if !foundMasterStatus {
		return fmt.Errorf("Got no results from SHOW MASTER STATUS. Bailing out")
	}

	return nil
}

// cutOver performs the final step of migration, based on migration
// type (on replica? atomic? safe?)
func (a *Applier) cutOver() (err error) {
	//a.mysqlContext.MarkPointOfInterest()
	/*a.throttler.throttle(func() {
		a.logger.Printf("mysql.applier: throttling before swapping tables")
	})*/

	//a.mysqlContext.MarkPointOfInterest()
	a.logger.Debugf("mysql.applier: checking for cut-over postpone")
	a.sleepWhileTrue(
		func() (bool, error) {
			if a.mysqlContext.PostponeCutOverFlagFile == "" {
				return false, nil
			}
			if atomic.LoadInt64(&a.mysqlContext.UserCommandedUnpostponeFlag) > 0 {
				atomic.StoreInt64(&a.mysqlContext.UserCommandedUnpostponeFlag, 0)
				return false, nil
			}
			if base.FileExists(a.mysqlContext.PostponeCutOverFlagFile) {
				// Postpone file defined and exists!
				/*if atomic.LoadInt64(&a.mysqlContext.IsPostponingCutOver) == 0 {
					if err := a.hooksExecutor.onBeginPostponed(); err != nil {
						return true, err
					}
				}*/
				atomic.StoreInt64(&a.mysqlContext.IsPostponingCutOver, 1)
				return true, nil
			}
			return false, nil
		},
	)
	atomic.StoreInt64(&a.mysqlContext.IsPostponingCutOver, 0)
	a.logger.Printf("mysql.applier: checking for cut-over postpone: complete")

	if a.mysqlContext.CutOverType == config.CutOverAtomic {
		// Atomic solution: we use low timeout and multiple attempts. But for
		// each failed attempt, we throttle until replication lag is back to normal
		err := a.atomicCutOver()
		return err
	}
	if a.mysqlContext.CutOverType == config.CutOverTwoStep {
		err := a.cutOverTwoStep()
		return err
	}
	return fmt.Errorf("Unknown cut-over type: %d; should never get here!", a.mysqlContext.CutOverType)
}

// Inject the "AllEventsUpToLockProcessed" state hint, wait for it to appear in the binary logs,
// make sure the queue is drained.
func (a *Applier) waitForEventsUpToLock() (err error) {
	timeout := time.NewTimer(time.Second * time.Duration(a.mysqlContext.CutOverLockTimeoutSeconds))

	waitForEventsUpToLockStartTime := time.Now()

	allEventsUpToLockProcessedChallenge := fmt.Sprintf("%s:%d", string(AllEventsUpToLockProcessed), waitForEventsUpToLockStartTime.UnixNano())
	a.logger.Printf("mysql.applier: writing changelog state: %+v", allEventsUpToLockProcessedChallenge)

	a.logger.Printf("mysql.applier: waiting for events up to lock")
	atomic.StoreInt64(&a.mysqlContext.AllEventsUpToLockProcessedInjectedFlag, 1)
	for found := false; !found; {
		select {
		case <-timeout.C:
			{
				return fmt.Errorf("Timeout while waiting for events up to lock")
			}
		case state := <-a.allEventsUpToLockProcessed:
			{
				if state == allEventsUpToLockProcessedChallenge {
					a.logger.Printf("mysql.applier: waiting for events up to lock: got %s", state)
					found = true
				} else {
					a.logger.Printf("mysql.applier: waiting for events up to lock: skipping %s", state)
				}
			}
		}
	}
	waitForEventsUpToLockDuration := time.Since(waitForEventsUpToLockStartTime)

	a.logger.Printf("mysql.applier: done waiting for events up to lock; duration=%+v", waitForEventsUpToLockDuration)

	return nil
}

// cutOverTwoStep will lock down the original table, execute
// what's left of last DML entries, and **non-atomically** swap original->old, then new->original.
// There is a point in time where the "original" table does not exist and queries are non-blocked
// and failing.
func (a *Applier) cutOverTwoStep() (err error) {
	atomic.StoreInt64(&a.mysqlContext.InCutOverCriticalSectionFlag, 1)
	defer atomic.StoreInt64(&a.mysqlContext.InCutOverCriticalSectionFlag, 0)
	atomic.StoreInt64(&a.mysqlContext.AllEventsUpToLockProcessedInjectedFlag, 0)

	if err := a.retryOperation(a.LockOriginalTable); err != nil {
		return err
	}

	if err := a.retryOperation(a.waitForEventsUpToLock); err != nil {
		return err
	}
	if err := a.retryOperation(a.UnlockTables); err != nil {
		return err
	}

	//lockAndRenameDuration := a.mysqlContext.RenameTablesEndTime.Sub(a.mysqlContext.LockTablesStartTime)
	//renameDuration := a.mysqlContext.RenameTablesEndTime.Sub(a.mysqlContext.RenameTablesStartTime)
	//a.logger.Debugf("mysql.applier: lock & rename duration: %s (rename only: %s). During this time, queries on %s were locked or failing", lockAndRenameDuration, renameDuration, sql.EscapeName(a.mysqlContext.OriginalTableName))
	return nil
}

// atomicCutOver
func (a *Applier) atomicCutOver() (err error) {
	atomic.StoreInt64(&a.mysqlContext.InCutOverCriticalSectionFlag, 1)
	defer atomic.StoreInt64(&a.mysqlContext.InCutOverCriticalSectionFlag, 0)

	okToUnlockTable := make(chan bool, 4)
	defer func() {
		okToUnlockTable <- true
		//a.DropAtomicCutOverSentryTableIfExists()
	}()

	atomic.StoreInt64(&a.mysqlContext.AllEventsUpToLockProcessedInjectedFlag, 0)

	lockOriginalSessionIdChan := make(chan int64, 2)
	//tableLocked := make(chan error, 2)
	tableUnlocked := make(chan error, 2)
	/*go func() {
		if err := a.AtomicCutOverMagicLock(lockOriginalSessionIdChan, tableLocked, okToUnlockTable, tableUnlocked); err != nil {
			a.logger.Errorf("%v", err)
		}
	}()
	if err := <-tableLocked; err != nil {
		return err
	}*/
	lockOriginalSessionId := <-lockOriginalSessionIdChan
	a.logger.Printf("mysql.applier: session locking original & magic tables is %+v", lockOriginalSessionId)
	// At this point we know the original table is locked.
	// We know any newly incoming DML on original table is blocked.
	if err := a.waitForEventsUpToLock(); err != nil {
		return err
	}

	// Step 2
	// We now attempt an atomic RENAME on original & ghost tables, and expect it to block.
	a.mysqlContext.RenameTablesStartTime = time.Now()

	var tableRenameKnownToHaveFailed int64
	renameSessionIdChan := make(chan int64, 2)
	tablesRenamed := make(chan error, 2)
	/*go func() {
		if err := a.AtomicCutoverRename(renameSessionIdChan, tablesRenamed); err != nil {
			// Abort! Release the lock
			atomic.StoreInt64(&tableRenameKnownToHaveFailed, 1)
			okToUnlockTable <- true
		}
	}()*/
	renameSessionId := <-renameSessionIdChan
	a.logger.Printf("mysql.applier: session renaming tables is %+v", renameSessionId)

	waitForRename := func() error {
		if atomic.LoadInt64(&tableRenameKnownToHaveFailed) == 1 {
			// We return `nil` here so as to avoid the `retry`. The RENAME has failed,
			// it won't show up in PROCESSLIST, no point in waiting
			return nil
		}
		return a.ExpectProcess(renameSessionId, "metadata lock", "rename")
	}
	// Wait for the RENAME to appear in PROCESSLIST
	if err := a.retryOperation(waitForRename, true); err != nil {
		// Abort! Release the lock
		okToUnlockTable <- true
		return err
	}
	if atomic.LoadInt64(&tableRenameKnownToHaveFailed) == 0 {
		a.logger.Printf("mysql.applier: found atomic RENAME to be blocking, as expected. Double checking the lock is still in place (though I don't strictly have to)")
	}
	if err := a.ExpectUsedLock(lockOriginalSessionId); err != nil {
		// Abort operation. Just make sure to drop the magic tabla.
		return err
	}
	a.logger.Printf("mysql.applier: connection holding lock on original table still exists")

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
	a.mysqlContext.RenameTablesEndTime = time.Now()

	// ooh nice! We're actually truly and thankfully done
	//lockAndRenameDuration := a.mysqlContext.RenameTablesEndTime.Sub(a.mysqlContext.LockTablesStartTime)
	//a.logger.Printf("mysql.applier: lock & rename duration: %s. During this time, queries on %s were blocked", lockAndRenameDuration, sql.EscapeName(a.mysqlContext.OriginalTableName))
	return nil
}

// printMigrationStatusHint prints a detailed configuration dump, that is useful
// to keep in mind; such as the name of migrated table, throttle params etc.
// This gets printed at beginning and end of migration, every 10 minutes throughout
// migration, and as reponse to the "status" interactive command.
func (a *Applier) printMigrationStatusHint(databaseName, tableName string) {
	a.logger.Printf("mysql.applier # Applying %s.%s",
		sql.EscapeName(databaseName),
		sql.EscapeName(tableName),
	)
	a.logger.Printf("mysql.applier # Migration started at %+v",
		a.mysqlContext.StartTime.Format(time.RubyDate),
	)
	maxLoad := a.mysqlContext.GetMaxLoad()
	criticalLoad := a.mysqlContext.GetCriticalLoad()
	a.logger.Printf("mysql.applier # chunk-size: %+v; max-lag-millis: %+vms; max-load: %s; critical-load: %s; nice-ratio: %f",
		atomic.LoadInt64(&a.mysqlContext.ChunkSize),
		atomic.LoadInt64(&a.mysqlContext.MaxLagMillisecondsThrottleThreshold),
		maxLoad.String(),
		criticalLoad.String(),
		a.mysqlContext.GetNiceRatio(),
	)

	if a.mysqlContext.PostponeCutOverFlagFile != "" {
		setIndicator := ""
		if base.FileExists(a.mysqlContext.PostponeCutOverFlagFile) {
			setIndicator = "[set]"
		}
		a.logger.Printf("mysql.applier # postpone-cut-over-flag-file: %+v %+v",
			a.mysqlContext.PostponeCutOverFlagFile, setIndicator,
		)
	}
}

func (a *Applier) onApplyTxStructWithSuper(dbApplier *sql.DB, binlogTx *binlog.BinlogTx) error {
	dbApplier.DbMutex.Lock()
	defer func() {
		_, err := sql.ExecNoPrepare(dbApplier.Db, `commit;set gtid_next='automatic'`)
		if err != nil {
			a.onError(TaskStateDead, err)
		}
		dbApplier.DbMutex.Unlock()
	}()

	if binlogTx.Fde != "" && dbApplier.Fde != binlogTx.Fde {
		dbApplier.Fde = binlogTx.Fde // IMO it would comare the internal pointer first
		_, err := sql.ExecNoPrepare(dbApplier.Db, binlogTx.Fde)
		if err != nil {
			return err
		}
	}

	_, err := sql.ExecNoPrepare(dbApplier.Db, fmt.Sprintf(`set gtid_next='%s:%d'`, binlogTx.SID, binlogTx.GNO))
	if err != nil {
		return err
	}
	var ignoreError error
	if binlogTx.Query == "" {
		_, err = sql.ExecNoPrepare(dbApplier.Db, `begin;commit`)
		if err != nil {
			return err
		}
	} else {
		_, err := sql.ExecNoPrepare(dbApplier.Db, binlogTx.Query)
		if err != nil {
			if !sql.IgnoreError(err) {
				//SELECT FROM_BASE64('')
				a.logger.Errorf("mysql.applier: exec gtid:[%s:%d] error: %v", binlogTx.SID, binlogTx.GNO, err)
				return err
			}
			a.logger.Warnf("mysql.applier: exec gtid:[%s:%d],ignore error: %v", binlogTx.SID, binlogTx.GNO, err)
			ignoreError = err
		}
	}

	if ignoreError != nil {
		_, err := sql.ExecNoPrepare(dbApplier.Db, fmt.Sprintf(`commit;set gtid_next='%s:%d'`, binlogTx.SID, binlogTx.GNO))
		if err != nil {
			return err
		}
		_, err = sql.ExecNoPrepare(dbApplier.Db, `begin;commit`)
		if err != nil {
			return err
		}
	}
	return nil
}

// executeWriteFuncs writes data via applier: both the rowcopy and the events backlog.
// This is where the ghost table gets the data. The function fills the data single-threaded.
// Both event backlog and rowcopy events are polled; the backlog events have precedence.
func (a *Applier) executeWriteFuncs() {
	go func() {
	L:
		for {
			select {
			case copyRows := <-a.copyRowsQueue:
				{
					//copyRowsStartTime := time.Now()
					// Retries are handled within the copyRowsFunc
					/*if err := copyRowsFunc(); err != nil {
						return err
					}*/
					if nil == copyRows {
						continue
					}
					if copyRows.DbSQL != "" || copyRows.TbSQL != "" {
						if err := a.ApplyEventQueries(a.db, copyRows); err != nil {
							a.onError(TaskStateDead, err)
						}
						atomic.AddInt64(&a.mysqlContext.RowsEstimate, copyRows.TotalCount)
					} else {
						go func() {
							if err := a.ApplyEventQueries(a.db, copyRows); err != nil {
								a.onError(TaskStateDead, err)
							}
							atomic.AddInt64(&a.mysqlContext.RowsEstimate, copyRows.TotalCount)
						}()
					}
					atomic.AddInt64(&a.mysqlContext.ExecQueries, 1)

					/*a.logger.Printf("mysql.applier: operating until row copy is complete")
					a.consumeRowCopyComplete()
					a.logger.Printf("mysql.applier: row copy complete")
					if niceRatio := a.mysqlContext.GetNiceRatio(); niceRatio > 0 {
						copyRowsDuration := time.Since(copyRowsStartTime)
						sleepTimeNanosecondFloat64 := niceRatio * float64(copyRowsDuration.Nanoseconds())
						sleepTime := time.Duration(time.Duration(int64(sleepTimeNanosecondFloat64)) * time.Nanosecond)
						time.Sleep(sleepTime)
					}*/
				}
			case <-a.rowCopyComplete:
				break L
			case <-a.shutdownCh:
				break L
			default:
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()

	if a.mysqlContext.Gtid == "" {
		a.logger.Printf("mysql.applier: Operating until row copy is complete")
		a.mysqlContext.Stage = models.StageSlaveWaitingForWorkersToProcessQueue
		for {
			if a.mysqlContext.ReceQueries != 0 && a.mysqlContext.ReceQueries == a.mysqlContext.ExecQueries && a.mysqlContext.TotalRowsCopied == a.mysqlContext.RowsEstimate {
				a.logger.Printf("mysql.applier: Rows copy complete.number of rows:%d", a.mysqlContext.RowsEstimate)
				break
			}
			if a.shutdown {
				break
			}
			time.Sleep(time.Second)
		}
	}
	a.rowCopyComplete <- true
	atomic.StoreInt64(&a.rowCopyCompleteFlag, 1)
	a.mysqlContext.MarkRowCopyStartTime()

	var dbApplier *sql.DB
OUTER:
	for {
		select {
		case groupEntry := <-a.applyGrouDataEntrypQueue:
			{
				if len(groupEntry) == 0 {
					continue
				}
				/*if a.mysqlContext.MySQLServerUuid == binlogEntry.Coordinates.OSID {
					continue
				}*/

				for idx, binlogEntry := range groupEntry {
					dbApplier = a.dbs[idx%a.mysqlContext.ParallelWorkers]
					//go func(entry *binlog.BinlogEntry) {
					//a.wg.Add(1)
					if err := a.ApplyBinlogEvent(dbApplier, binlogEntry); err != nil {
						a.onError(TaskStateDead, err)
					}
					//a.wg.Done()
					//}(binlogEntry)
				}
				//a.wg.Wait() // Waiting for all goroutines to finish

				//a.logger.Debugf("mysql.applier: apply binlogEntry: %+v", groupEntry[len(groupEntry)-1].Coordinates.GNO)

				if !a.shutdown {
					a.mysqlContext.Gtid = fmt.Sprintf("%s:1-%d", groupEntry[len(groupEntry)-1].Coordinates.SID, groupEntry[len(groupEntry)-1].Coordinates.GNO)
				}
			}
		case groupTx := <-a.applyBinlogGroupTxQueue:
			{
				if len(groupTx) == 0 {
					continue
				}
				for idx, binlogTx := range groupTx {
					dbApplier = a.dbs[idx%a.mysqlContext.ParallelWorkers]
					go func(tx *binlog.BinlogTx) {
						a.wg.Add(1)
						if err := a.onApplyTxStructWithSuper(dbApplier, tx); err != nil {
							a.onError(TaskStateDead, err)
						}
						a.wg.Done()
					}(binlogTx)
				}
				a.wg.Wait() // Waiting for all goroutines to finish

				if !a.shutdown {
					a.lastAppliedBinlogTx = groupTx[len(groupTx)-1]
					a.mysqlContext.Gtid = fmt.Sprintf("%s:1-%d", a.lastAppliedBinlogTx.SID, a.lastAppliedBinlogTx.GNO)
				}
			}
		case <-a.shutdownCh:
			break OUTER
		default:
			time.Sleep(time.Second)
		}
	}
}

func (a *Applier) initNatSubClient() (err error) {
	natsAddr := fmt.Sprintf("nats://%s", a.mysqlContext.NatsAddr)
	sc, err := gonats.Connect(natsAddr)
	if err != nil {
		a.logger.Errorf("mysql.applier: Can't connect nats server %v. make sure a nats streaming server is running.%v", natsAddr, err)
		return err
	}
	a.logger.Debugf("mysql.applier: Connect nats server %v", natsAddr)
	a.natsConn = sc
	return nil
}

// Decode
func Decode(data []byte, vPtr interface{}) (err error) {
	msg, err := snappy.Decode(nil, data)
	if err != nil {
		return err
	}

	return gob.NewDecoder(bytes.NewBuffer(msg)).Decode(vPtr)
}

// initiateStreaming begins treaming of binary log events and registers listeners for such events
func (a *Applier) initiateStreaming() error {
	if a.mysqlContext.Gtid == "" {
		a.mysqlContext.MarkRowCopyStartTime()
		_, err := a.natsConn.Subscribe(fmt.Sprintf("%s_full", a.subject), func(m *gonats.Msg) {
			dumpData := &dumpEntry{}
			if err := Decode(m.Data, dumpData); err != nil {
				a.onError(TaskStateDead, err)
			}
			a.copyRowsQueue <- dumpData
			a.mysqlContext.Stage = models.StageSlaveWaitingForWorkersToProcessQueue
			if err := a.natsConn.Publish(m.Reply, nil); err != nil {
				a.onError(TaskStateDead, err)
			}
			atomic.AddInt64(&a.mysqlContext.ReceQueries, 1)
		})
		if err != nil {
			return err
		}
		/*if err := sub.SetPendingLimits(a.mysqlContext.MsgsLimit, a.mysqlContext.BytesLimit); err != nil {
			return err
		}*/
	}

	if a.mysqlContext.ApproveHeterogeneous {
		_, err := a.natsConn.Subscribe(fmt.Sprintf("%s_incr_hete", a.subject), func(m *gonats.Msg) {
			var binlogEntry *binlog.BinlogEntry
			if err := Decode(m.Data, &binlogEntry); err != nil {
				a.onError(TaskStateDead, err)
			}
			a.logger.Debugf("mysql.applier: received binlogEntry GNO: %+v,LastCommitted:%+v", binlogEntry.Coordinates.GNO, binlogEntry.Coordinates.LastCommitted)
			//for _, entry := range binlogEntry {
			a.applyDataEntryQueue <- binlogEntry
			a.currentCoordinates.RetrievedGtidSet = fmt.Sprintf("%s:%d", binlogEntry.Coordinates.SID, binlogEntry.Coordinates.GNO)
			//}
			a.mysqlContext.Stage = models.StageWaitingForMasterToSendEvent
			atomic.AddInt64(&a.mysqlContext.DeltaEstimate, 1)

			if err := a.natsConn.Publish(m.Reply, nil); err != nil {
				a.onError(TaskStateDead, err)
			}
		})
		if err != nil {
			return err
		}

		go func() {
			var lastCommitted int64
			//timeout := time.After(100 * time.Millisecond)
			groupEntry := []*binlog.BinlogEntry{}
		OUTER:
			for {
				select {
				case binlogEntry := <-a.applyDataEntryQueue:
					if nil == binlogEntry {
						continue
					}
					/*if a.mysqlContext.MySQLServerUuid == binlogTx.SID {
						continue
					}*/
					if a.mysqlContext.ParallelWorkers <= 1 {
						if err := a.ApplyBinlogEvent(a.dbs[0], binlogEntry); err != nil {
							a.onError(TaskStateDead, err)
							break OUTER
						}
					} else {
						if binlogEntry.Coordinates.LastCommitted == lastCommitted {
							groupEntry = append(groupEntry, binlogEntry)
						} else {
							if len(groupEntry) != 0 {
								a.applyGrouDataEntrypQueue <- groupEntry
								groupEntry = []*binlog.BinlogEntry{}
							}
							groupEntry = append(groupEntry, binlogEntry)
						}
						lastCommitted = binlogEntry.Coordinates.LastCommitted
					}
				case <-time.After(100 * time.Millisecond):
					if len(groupEntry) != 0 {
						a.applyGrouDataEntrypQueue <- groupEntry
						groupEntry = []*binlog.BinlogEntry{}
					}
				case <-a.shutdownCh:
					break OUTER
				}
			}
		}()
	} else {
		_, err := a.natsConn.Subscribe(fmt.Sprintf("%s_incr", a.subject), func(m *gonats.Msg) {
			var binlogTx []*binlog.BinlogTx
			if err := Decode(m.Data, &binlogTx); err != nil {
				a.onError(TaskStateDead, err)
			}
			for _, tx := range binlogTx {
				a.applyBinlogTxQueue <- tx
			}
			if err := a.natsConn.Publish(m.Reply, nil); err != nil {
				a.onError(TaskStateDead, err)
			}
		})
		if err != nil {
			return err
		}
		/*if err := sub.SetPendingLimits(a.mysqlContext.MsgsLimit, a.mysqlContext.BytesLimit); err != nil {
			return err
		}*/
	}

	go func() {
		var lastCommitted int64
		var err error
		//timeout := time.After(100 * time.Millisecond)
		groupTx := []*binlog.BinlogTx{}
	OUTER:
		for {
			select {
			case binlogTx := <-a.applyBinlogTxQueue:
				if nil == binlogTx {
					continue
				}
				if a.mysqlContext.MySQLServerUuid == binlogTx.SID {
					continue
				}
				if a.mysqlContext.ParallelWorkers <= 1 {
					if err = a.onApplyTxStructWithSuper(a.dbs[0], binlogTx); err != nil {
						a.onError(TaskStateDead, err)
						break OUTER
					}

					if !a.shutdown {
						a.lastAppliedBinlogTx = binlogTx
						a.mysqlContext.Gtid = fmt.Sprintf("%s:1-%d", a.lastAppliedBinlogTx.SID, a.lastAppliedBinlogTx.GNO)
					}
				} else {
					if binlogTx.LastCommitted == lastCommitted {
						groupTx = append(groupTx, binlogTx)
					} else {
						if len(groupTx) != 0 {
							a.applyBinlogGroupTxQueue <- groupTx
							groupTx = []*binlog.BinlogTx{}
						}
						groupTx = append(groupTx, binlogTx)
					}
					lastCommitted = binlogTx.LastCommitted
				}
			case <-time.After(100 * time.Millisecond):
				if len(groupTx) != 0 {
					a.applyBinlogGroupTxQueue <- groupTx
					groupTx = []*binlog.BinlogTx{}
				}
			case <-a.shutdownCh:
				break OUTER
			}
		}
	}()

	return nil
}

func (a *Applier) initDBConnections() (err error) {
	applierUri := a.mysqlContext.ConnectionConfig.GetDBUri()
	if a.dbs, err = sql.CreateDBs(applierUri, a.mysqlContext.ParallelWorkers); err != nil {
		return err
	}
	if a.db, err = sql.CreateDB(applierUri); err != nil {
		return err
	}
	a.db.SetMaxOpenConns(10)
	if err := a.validateConnection(a.db); err != nil {
		return err
	}
	if err := a.validateServerUUID(); err != nil {
		return err
	}
	/*if err := a.validateTableForeignKeys(); err != nil {
		return err
	}*/
	if err := a.validateGrants(); err != nil {
		a.logger.Errorf("mysql.applier: Unexpected error on validateGrants, got %v", err)
		return err
	}
	if err := a.validateAndReadTimeZone(); err != nil {
		return err
	}
	if a.mysqlContext.ApproveHeterogeneous {
		if err := a.createTableGtidExecuted(); err != nil {
			return err
		}
	}
	/*if err := a.readCurrentBinlogCoordinates(); err != nil {
		return err
	}*/
	/*if err := a.readTableColumns(); err != nil {
		return err
	}*/
	a.logger.Printf("mysql.applier: Initiated on %s:%d, version %+v", a.mysqlContext.ConnectionConfig.Host, a.mysqlContext.ConnectionConfig.Port, a.mysqlContext.MySQLVersion)
	return nil
}

func (a *Applier) validateServerUUID() error {
	query := `SELECT @@SERVER_UUID`
	if err := a.db.QueryRow(query).Scan(&a.mysqlContext.MySQLServerUuid); err != nil {
		return err
	}
	return nil
}

// validateConnection issues a simple can-connect to MySQL
func (a *Applier) validateConnection(db *gosql.DB) error {
	query := `select @@global.version`
	if err := db.QueryRow(query).Scan(&a.mysqlContext.MySQLVersion); err != nil {
		return err
	}
	// Match the version string (from SELECT VERSION()).
	if strings.HasPrefix(a.mysqlContext.MySQLVersion, "5.6") {
		a.mysqlContext.ParallelWorkers = 1
	}
	a.logger.Debugf("mysql.applier: Connection validated on %s:%d", a.mysqlContext.ConnectionConfig.Host, a.mysqlContext.ConnectionConfig.Port)
	return nil
}

// validateGrants verifies the user by which we're executing has necessary grants
// to do its thang.
func (a *Applier) validateGrants() error {
	query := `show grants for current_user()`
	foundAll := false
	foundSuper := false
	foundDBAll := false

	err := sql.QueryRowsMap(a.db, query, func(rowMap sql.RowMap) error {
		for _, grantData := range rowMap {
			grant := grantData.String
			if strings.Contains(grant, `GRANT ALL PRIVILEGES ON`) {
				foundAll = true
			}
			if strings.Contains(grant, `SUPER`) && strings.Contains(grant, ` ON *.*`) {
				foundSuper = true
			}
			if strings.Contains(grant, "GRANT ALL PRIVILEGES ON `actiontech_udup`.`gtid_executed`") {
				foundDBAll = true
			}
			if base.StringContainsAll(grant, `ALTER`, `CREATE`, `DELETE`, `DROP`, `INDEX`, `INSERT`, `LOCK TABLES`, `SELECT`, `TRIGGER`, `UPDATE`, ` ON`) {
				foundDBAll = true
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	a.mysqlContext.HasSuperPrivilege = foundSuper

	if foundAll {
		a.logger.Printf("mysql.applier: User has ALL privileges")
		return nil
	}
	if foundSuper {
		a.logger.Printf("mysql.applier: User has SUPER privileges")
		return nil
	}
	if foundDBAll {
		a.logger.Printf("User has ALL privileges on *.*")
		return nil
	}
	a.logger.Debugf("mysql.applier: Privileges: super: %t, ALL on *.*: %t", foundSuper, foundAll)
	//return fmt.Errorf("user has insufficient privileges for applier. Needed: SUPER|ALL on *.*")
	return nil
}

// validateTableForeignKeys checks that binary log foreign_key_checks is set.
func (a *Applier) validateTableForeignKeys() error {
	query := `select @@global.foreign_key_checks`
	var foreignKeyChecks bool
	if err := a.db.QueryRow(query).Scan(&foreignKeyChecks); err != nil {
		return err
	}

	if !foreignKeyChecks {
		a.logger.Printf("mysql.applier: foreign_key_checks validated on %s:%d", a.mysqlContext.ConnectionConfig.Host, a.mysqlContext.ConnectionConfig.Port)
		return nil
	}

	//SET @@global.foreign_key_checks = 0;
	return fmt.Errorf("%s:%d must have foreign_key_checks disabled for executing", a.mysqlContext.ConnectionConfig.Host, a.mysqlContext.ConnectionConfig.Port)
}

// validateAndReadTimeZone potentially reads server time-zone
func (a *Applier) validateAndReadTimeZone() error {
	query := `select @@global.time_zone`
	if err := a.db.QueryRow(query).Scan(&a.mysqlContext.TimeZone); err != nil {
		return err
	}

	a.logger.Printf("mysql.applier: Will use time_zone='%s' on applier", a.mysqlContext.TimeZone)
	return nil
}

func (a *Applier) createTableGtidExecuted() error {
	query := fmt.Sprintf(`
			CREATE DATABASE IF NOT EXISTS actiontech_udup;
			CREATE TABLE IF NOT EXISTS actiontech_udup.gtid_executed (
  				source_uuid char(36) NOT NULL COMMENT 'uuid of the source where the transaction was originally executed.',
  				interval_gtid text NOT NULL COMMENT 'number of interval.'
			)
		`)
	if _, err := sql.ExecNoPrepare(a.db, query); err != nil {
		return err
	}
	return nil
}

// readTableColumns reads table columns on applier
func (a *Applier) readTableColumns() (err error) {
	a.logger.Printf("mysql.applier: Examining table structure on applier")
	for _, doDb := range a.mysqlContext.ReplicateDoDb {
		for _, doTb := range doDb.Tables {
			doTb.OriginalTableColumnsOnApplier, err = base.GetTableColumns(a.db, doDb.TableSchema, doTb.TableName)
			if err != nil {
				a.logger.Errorf("mysql.applier: Unexpected error on readTableColumns, got %v", err)
				return err
			}
		}
	}
	return nil
}

// showTableStatus returns the output of `show table status like '...'` command
/*func (a *Applier) showTableStatus(tableName string) (rowMap sql.RowMap) {
	rowMap = nil
	query := fmt.Sprintf(`show table status from %s like '%s'`, sql.EscapeName(a.mysqlContext.DatabaseName), tableName)
	sql.QueryRowsMap(a.db, query, func(m sql.RowMap) error {
		rowMap = m
		return nil
	})
	return rowMap
}*/

// CalculateNextIterationRangeEndValues reads the next-iteration-range-end unique key values,
// which will be used for copying the next chunk of rows. Ir returns "false" if there is
// no further chunk to work through, i.e. we're past the last chunk and are done with
// itrating the range (and this done with copying row chunks)
/*func (a *Applier) CalculateNextIterationRangeEndValues() (hasFurtherRange bool, err error) {
	a.mysqlContext.MigrationIterationRangeMinValues = a.mysqlContext.MigrationIterationRangeMaxValues
	if a.mysqlContext.MigrationIterationRangeMinValues == nil {
		a.mysqlContext.MigrationIterationRangeMinValues = a.mysqlContext.MigrationRangeMinValues
	}
	query, explodedArgs, err := sql.BuildUniqueKeyRangeEndPreparedQuery(
		a.mysqlContext.DatabaseName,
		a.mysqlContext.OriginalTableName,
		&a.mysqlContext.UniqueKey.Columns,
		a.mysqlContext.MigrationIterationRangeMinValues.AbstractValues(),
		a.mysqlContext.MigrationRangeMaxValues.AbstractValues(),
		atomic.LoadInt64(&a.mysqlContext.ChunkSize),
		a.mysqlContext.GetIteration() == 0,
		fmt.Sprintf("iteration:%d", a.mysqlContext.GetIteration()),
	)
	if err != nil {
		return hasFurtherRange, err
	}
	rows, err := a.db.Query(query, explodedArgs...)
	if err != nil {
		return hasFurtherRange, err
	}
	iterationRangeMaxValues := sql.NewColumnValues(a.mysqlContext.UniqueKey.Len())
	for rows.Next() {
		if err = rows.Scan(iterationRangeMaxValues.ValuesPointers...); err != nil {
			return hasFurtherRange, err
		}
		hasFurtherRange = true
	}
	if !hasFurtherRange {
		a.logger.Debugf("mysql.applier: Iteration complete: no further range to iterate")
		return hasFurtherRange, nil
	}
	a.mysqlContext.MigrationIterationRangeMaxValues = iterationRangeMaxValues
	return hasFurtherRange, nil
}

// ApplyIterationInsertQuery issues a chunk-INSERT query on the ghost table. It is where
// data actually gets copied from original table.
func (a *Applier) ApplyIterationInsertQuery() (chunkSize int64, rowsAffected int64, duration time.Duration, err error) {
	startTime := time.Now()
	chunkSize = atomic.LoadInt64(&a.mysqlContext.ChunkSize)

	query, explodedArgs, err := sql.BuildRangeInsertPreparedQuery(
		a.mysqlContext.DatabaseName,
		a.mysqlContext.OriginalTableName,
		a.mysqlContext.OriginalTableName, //GetGhostTableName()
		a.mysqlContext.SharedColumns.Names(),
		a.mysqlContext.MappedSharedColumns.Names(),
		a.mysqlContext.UniqueKey.Name,
		&a.mysqlContext.UniqueKey.Columns,
		a.mysqlContext.MigrationIterationRangeMinValues.AbstractValues(),
		a.mysqlContext.MigrationIterationRangeMaxValues.AbstractValues(),
		a.mysqlContext.GetIteration() == 0,
		a.mysqlContext.IsTransactionalTable(),
	)
	if err != nil {
		return chunkSize, rowsAffected, duration, err
	}

	sqlResult, err := func() (gosql.Result, error) {
		tx, err := a.db.Begin()
		if err != nil {
			return nil, err
		}
		sessionQuery := fmt.Sprintf(`SET
			SESSION time_zone = '%s',
			sql_mode = CONCAT(@@session.sql_mode, ',STRICT_ALL_TABLES')
			`, a.mysqlContext.ApplierTimeZone)
		if _, err := tx.Exec(sessionQuery); err != nil {
			return nil, err
		}
		result, err := tx.Exec(query, explodedArgs...)
		if err != nil {
			return nil, err
		}
		if err := tx.Commit(); err != nil {
			return nil, err
		}
		return result, nil
	}()

	if err != nil {
		return chunkSize, rowsAffected, duration, err
	}
	rowsAffected, _ = sqlResult.RowsAffected()
	duration = time.Since(startTime)
	a.logger.Printf(
		"[DEBUG] mysql.applier: Issued INSERT on range: [%s]..[%s]; iteration: %d; chunk-size: %d",
		a.mysqlContext.MigrationIterationRangeMinValues,
		a.mysqlContext.MigrationIterationRangeMaxValues,
		a.mysqlContext.GetIteration(),
		chunkSize)
	return chunkSize, rowsAffected, duration, nil
}*/

// LockOriginalTable places a write lock on the original table
func (a *Applier) LockOriginalTable() error {
	for _, doDb := range a.mysqlContext.ReplicateDoDb {
		for _, doTb := range doDb.Tables {
			query := fmt.Sprintf(`lock tables %s.%s write`,
				sql.EscapeName(doDb.TableSchema),
				sql.EscapeName(doTb.TableName),
			)
			a.logger.Printf("mysql.applier: Locking %s.%s",
				sql.EscapeName(doDb.TableSchema),
				sql.EscapeName(doTb.TableName),
			)
			a.mysqlContext.LockTablesStartTime = time.Now()
			if _, err := sql.ExecNoPrepare(a.db, query); err != nil {
				return err
			}
		}
	}

	a.logger.Printf("mysql.applier: Table locked")
	return nil
}

// UnlockTables makes tea. No wait, it unlocks tables.
func (a *Applier) UnlockTables() error {
	query := `unlock tables`
	a.logger.Printf("Unlocking tables")
	if _, err := sql.ExecNoPrepare(a.db, query); err != nil {
		return err
	}
	a.logger.Printf("Tables unlocked")
	return nil
}

// GetSessionLockName returns a name for the special hint session voluntary lock
func (a *Applier) GetSessionLockName(sessionId int64) string {
	return fmt.Sprintf("udup.%d.lock", sessionId)
}

// ExpectUsedLock expects the special hint voluntary lock to exist on given session
func (a *Applier) ExpectUsedLock(sessionId int64) error {
	var result int64
	query := `select is_used_lock(?)`
	lockName := a.GetSessionLockName(sessionId)
	a.logger.Printf("mysql.applier: Checking session lock: %s", lockName)
	if err := a.db.QueryRow(query, lockName).Scan(&result); err != nil || result != sessionId {
		return fmt.Errorf("Session lock %s expected to be found but wasn't", lockName)
	}
	return nil
}

// ExpectProcess expects a process to show up in `SHOW PROCESSLIST` that has given characteristics
func (a *Applier) ExpectProcess(sessionId int64, stateHint, infoHint string) error {
	found := false
	query := `
		select id
			from information_schema.processlist
			where
				id != connection_id()
				and ? in (0, id)
				and state like concat('%', ?, '%')
				and info  like concat('%', ?, '%')
	`
	err := sql.QueryRowsMap(a.db, query, func(m sql.RowMap) error {
		found = true
		return nil
	}, sessionId, stateHint, infoHint)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("Cannot find process. Hints: %s, %s", stateHint, infoHint)
	}
	return nil
}

// CreateAtomicCutOverSentryTable
/*func (a *Applier) CreateAtomicCutOverSentryTable() error {
	//DropAtomicCutOverSentryTableIfExists
	tableName := a.mysqlContext.OriginalTableName

	query := fmt.Sprintf(`create table %s.%s (
			id int auto_increment primary key
		) engine=%s comment='%s'
		`,
		sql.EscapeName(a.mysqlContext.DatabaseName),
		sql.EscapeName(tableName),
		a.mysqlContext.TableEngine,
		atomicCutOverMagicHint,
	)
	a.logger.Printf("mysql.applier: Creating magic cut-over table %s.%s",
		sql.EscapeName(a.mysqlContext.DatabaseName),
		sql.EscapeName(tableName),
	)
	if _, err := sql.ExecNoPrepare(a.db, query); err != nil {
		return err
	}
	a.logger.Printf("mysql.applier: Magic cut-over table created")

	return nil
}*/

// AtomicCutOverMagicLock
/*func (a *Applier) AtomicCutOverMagicLock(sessionIdChan chan int64, tableLocked chan<- error, okToUnlockTable <-chan bool, tableUnlocked chan<- error) error {
tx, err := a.db.Begin()
if err != nil {
	tableLocked <- err
	return err
}
defer func() {
	sessionIdChan <- -1
	tableLocked <- fmt.Errorf("Unexpected error in AtomicCutOverMagicLock(), injected to release blocking channel reads")
	tableUnlocked <- fmt.Errorf("Unexpected error in AtomicCutOverMagicLock(), injected to release blocking channel reads")
	tx.Rollback()
}()

var sessionId int64
if err := tx.QueryRow(`select connection_id()`).Scan(&sessionId); err != nil {
	tableLocked <- err
	return err
}
sessionIdChan <- sessionId

lockResult := 0
query := `select get_lock(?, 0)`
lockName := a.GetSessionLockName(sessionId)
a.logger.Printf("mysql.applier: Grabbing voluntary lock: %s", lockName)
if err := tx.QueryRow(query, lockName).Scan(&lockResult); err != nil || lockResult != 1 {
	err := fmt.Errorf("Unable to acquire lock %s", lockName)
	tableLocked <- err
	return err
}

tableLockTimeoutSeconds := a.mysqlContext.CutOverLockTimeoutSeconds * 2
a.logger.Printf("mysql.applier: Setting LOCK timeout as %d seconds", tableLockTimeoutSeconds)
query = fmt.Sprintf(`set session lock_wait_timeout:=%d`, tableLockTimeoutSeconds)
if _, err := tx.Exec(query); err != nil {
	tableLocked <- err
	return err
}

*/ /*if err := a.CreateAtomicCutOverSentryTable(); err != nil {
	tableLocked <- err
	return err
}*/ /*

	query = fmt.Sprintf(`lock tables %s.%s write`,
		sql.EscapeName(a.mysqlContext.DatabaseName),
		sql.EscapeName(a.mysqlContext.OriginalTableName),
	)
	a.logger.Printf("mysql.applier: Locking %s.%s",
		sql.EscapeName(a.mysqlContext.DatabaseName),
		sql.EscapeName(a.mysqlContext.OriginalTableName),
	)
	a.mysqlContext.LockTablesStartTime = time.Now()
	if _, err := tx.Exec(query); err != nil {
		tableLocked <- err
		return err
	}
	a.logger.Printf("mysql.applier: Tables locked")
	tableLocked <- nil // No error.

	// From this point on, we are committed to UNLOCK TABLES. No matter what happens,
	// the UNLOCK must execute (or, alternatively, this connection dies, which gets the same impact)

	// The cut-over phase will proceed to apply remaining backlog onto ghost table,
	// and issue RENAME. We wait here until told to proceed.
	<-okToUnlockTable
	a.logger.Printf("mysql.applier: Will now proceed to drop magic table and unlock tables")

	// Tables still locked
	a.logger.Printf("mysql.applier: Releasing lock from %s.%s",
		sql.EscapeName(a.mysqlContext.DatabaseName),
		sql.EscapeName(a.mysqlContext.OriginalTableName),
	)
	query = `unlock tables`
	if _, err := tx.Exec(query); err != nil {
		tableUnlocked <- err
		return err
	}
	a.logger.Printf("mysql.applier: Tables unlocked")
	tableUnlocked <- nil
	return nil
}

// AtomicCutoverRename
func (a *Applier) AtomicCutoverRename(sessionIdChan chan int64, tablesRenamed chan<- error) error {
	tx, err := a.db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		tx.Rollback()
		sessionIdChan <- -1
		tablesRenamed <- fmt.Errorf("Unexpected error in AtomicCutoverRename(), injected to release blocking channel reads")
	}()
	var sessionId int64
	if err := tx.QueryRow(`select connection_id()`).Scan(&sessionId); err != nil {
		return err
	}
	sessionIdChan <- sessionId

	a.logger.Printf("mysql.applier: Setting RENAME timeout as %d seconds", a.mysqlContext.CutOverLockTimeoutSeconds)
	query := fmt.Sprintf(`set session lock_wait_timeout:=%d`, a.mysqlContext.CutOverLockTimeoutSeconds)
	if _, err := tx.Exec(query); err != nil {
		return err
	}

	query = fmt.Sprintf(`rename table %s.%s to %s.%s, %s.%s to %s.%s`,
		sql.EscapeName(a.mysqlContext.DatabaseName),
		sql.EscapeName(a.mysqlContext.OriginalTableName),
*/ /*sql.EscapeName(a.mysqlContext.DatabaseName),
sql.EscapeName(a.mysqlContext.GetOldTableName()),
sql.EscapeName(a.mysqlContext.DatabaseName),
sql.EscapeName(a.mysqlContext.GetGhostTableName()),*/ /*
		sql.EscapeName(a.mysqlContext.DatabaseName),
		sql.EscapeName(a.mysqlContext.OriginalTableName),
	)
	a.logger.Printf("mysql.applier: Issuing and expecting this to block: %s", query)
	if _, err := tx.Exec(query); err != nil {
		tablesRenamed <- err
		return err
	}
	tablesRenamed <- nil
	a.logger.Printf("mysql.applier: Tables renamed")
	return nil
}*/

func (a *Applier) ShowStatusVariable(variableName string) (result int64, err error) {
	query := fmt.Sprintf(`show global status like '%s'`, variableName)
	if err := a.db.QueryRow(query).Scan(&variableName, &result); err != nil {
		return 0, err
	}
	return result, nil
}

// getCandidateUniqueKeys investigates a table and returns the list of unique keys
// candidate for chunking
func (a *Applier) getCandidateUniqueKeys(databaseName, tableName string) (uniqueKeys [](*umconf.UniqueKey), err error) {
	query := `
    SELECT
      COLUMNS.TABLE_SCHEMA,
      COLUMNS.TABLE_NAME,
      COLUMNS.COLUMN_NAME,
      UNIQUES.INDEX_NAME,
      UNIQUES.COLUMN_NAMES,
      UNIQUES.COUNT_COLUMN_IN_INDEX,
      COLUMNS.DATA_TYPE,
      COLUMNS.CHARACTER_SET_NAME,
			LOCATE('auto_increment', EXTRA) > 0 as is_auto_increment,
      has_nullable
    FROM INFORMATION_SCHEMA.COLUMNS INNER JOIN (
      SELECT
        TABLE_SCHEMA,
        TABLE_NAME,
        INDEX_NAME,
        COUNT(*) AS COUNT_COLUMN_IN_INDEX,
        GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX ASC) AS COLUMN_NAMES,
        SUBSTRING_INDEX(GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX ASC), ',', 1) AS FIRST_COLUMN_NAME,
        SUM(NULLABLE='YES') > 0 AS has_nullable
      FROM INFORMATION_SCHEMA.STATISTICS
      WHERE
				NON_UNIQUE=0
				AND TABLE_SCHEMA = ?
      	AND TABLE_NAME = ?
      GROUP BY TABLE_SCHEMA, TABLE_NAME, INDEX_NAME
    ) AS UNIQUES
    ON (
      COLUMNS.TABLE_SCHEMA = UNIQUES.TABLE_SCHEMA AND
      COLUMNS.TABLE_NAME = UNIQUES.TABLE_NAME AND
      COLUMNS.COLUMN_NAME = UNIQUES.FIRST_COLUMN_NAME
    )
    WHERE
      COLUMNS.TABLE_SCHEMA = ?
      AND COLUMNS.TABLE_NAME = ?
    ORDER BY
      COLUMNS.TABLE_SCHEMA, COLUMNS.TABLE_NAME,
      CASE UNIQUES.INDEX_NAME
        WHEN 'PRIMARY' THEN 0
        ELSE 1
      END,
      CASE has_nullable
        WHEN 0 THEN 0
        ELSE 1
      END,
      CASE IFNULL(CHARACTER_SET_NAME, '')
          WHEN '' THEN 0
          ELSE 1
      END,
      CASE DATA_TYPE
        WHEN 'tinyint' THEN 0
        WHEN 'smallint' THEN 1
        WHEN 'int' THEN 2
        WHEN 'bigint' THEN 3
        ELSE 100
      END,
      COUNT_COLUMN_IN_INDEX
  `
	err = sql.QueryRowsMap(a.db, query, func(m sql.RowMap) error {
		uniqueKey := &umconf.UniqueKey{
			Name:            m.GetString("INDEX_NAME"),
			Columns:         *umconf.ParseColumnList(m.GetString("COLUMN_NAMES")),
			HasNullable:     m.GetBool("has_nullable"),
			IsAutoIncrement: m.GetBool("is_auto_increment"),
		}
		uniqueKeys = append(uniqueKeys, uniqueKey)
		return nil
	}, databaseName, tableName, databaseName, tableName)
	if err != nil {
		return uniqueKeys, err
	}
	//a.logger.Debugf("mysql.applier: potential unique keys in %+v.%+v: %+v", databaseName, tableName, uniqueKeys)
	return uniqueKeys, nil
}

func (a *Applier) InspectTableColumnsAndUniqueKeys(databaseName, tableName string) (columns *umconf.ColumnList, uniqueKeys [](*umconf.UniqueKey), err error) {
	uniqueKeys, err = a.getCandidateUniqueKeys(databaseName, tableName)
	if err != nil {
		return columns, uniqueKeys, err
	}
	/*if len(uniqueKeys) == 0 {
		return columns, uniqueKeys, fmt.Errorf("No PRIMARY nor UNIQUE key found in table! Bailing out")
	}*/
	columns, err = base.GetTableColumns(a.db, databaseName, tableName)
	if err != nil {
		return columns, uniqueKeys, err
	}
	t := &config.Table{
		TableName:            tableName,
		OriginalTableColumns: columns,
	}
	if err := base.InspectTables(a.db, databaseName, t, a.mysqlContext.TimeZone); err != nil {
		return columns, uniqueKeys, err
	}
	columns = t.OriginalTableColumns
	uniqueKeys = t.OriginalTableUniqueKeys

	return columns, uniqueKeys, nil
}

// getSharedColumns returns the intersection of two lists of columns in same order as the first list
func (a *Applier) getSharedColumns(originalColumns, columns *umconf.ColumnList, columnRenameMap map[string]string) (*umconf.ColumnList, *umconf.ColumnList) {
	columnsInGhost := make(map[string]bool)
	for _, column := range columns.Names() {
		columnsInGhost[column] = true
	}
	sharedColumnNames := []string{}
	for _, originalColumn := range originalColumns.Names() {
		isSharedColumn := false
		if columnsInGhost[originalColumn] || columnsInGhost[columnRenameMap[originalColumn]] {
			isSharedColumn = true
		}
		/*if a.mysqlContext.DroppedColumnsMap[originalColumn] {
			isSharedColumn = false
		}*/
		if isSharedColumn {
			sharedColumnNames = append(sharedColumnNames, originalColumn)
		}
	}
	mappedSharedColumnNames := []string{}
	for _, columnName := range sharedColumnNames {
		if mapped, ok := columnRenameMap[columnName]; ok {
			mappedSharedColumnNames = append(mappedSharedColumnNames, mapped)
		} else {
			mappedSharedColumnNames = append(mappedSharedColumnNames, columnName)
		}
	}
	return umconf.NewColumnList(sharedColumnNames), umconf.NewColumnList(mappedSharedColumnNames)
}

// buildDMLEventQuery creates a query to operate on the ghost table, based on an intercepted binlog
// event entry on the original table.
func (a *Applier) buildDMLEventQuery(dmlEvent binlog.DataEvent) (query string, args []interface{}, rowsDelta int64, err error) {
	/*var destTableColumns *umconf.ColumnList
	if len(a.mysqlContext.ReplicateDoDb) ==0 {
		tableColumns, _, err := a.InspectTableColumnsAndUniqueKeys(dmlEvent.DatabaseName, dmlEvent.TableName)
		if err != nil {
			return "", args, 0, err
		}
		tb:=&config.Table{
			TableSchema:dmlEvent.DatabaseName,
			TableName:dmlEvent.TableName,
			OriginalTableColumns:tableColumns,
		}
		db:= &config.DataSource{
			TableSchema:dmlEvent.DatabaseName,
			Tables:[]*config.Table{tb},
		}
		a.mysqlContext.ReplicateDoDb = append(a.mysqlContext.ReplicateDoDb,db)
	}else{
	L:
		for _,db:=range a.mysqlContext.ReplicateDoDb{
			if db.TableSchema != dmlEvent.DatabaseName {
				continue
			}
			for _,tb:=range db.Tables {
				if tb.TableName == dmlEvent.TableName && tb.OriginalTableColumns!=nil{
					break L
				}
			}
			tableColumns, _, err := a.InspectTableColumnsAndUniqueKeys(dmlEvent.DatabaseName, dmlEvent.TableName)
			if err != nil {
				return "", args, 0, err
			}
			tb:=&config.Table{
				TableSchema:dmlEvent.DatabaseName,
				TableName:dmlEvent.TableName,
				OriginalTableColumns:tableColumns,
			}
			db.Tables = append(db.Tables,tb)
		}
	}
	for _,db:=range a.mysqlContext.ReplicateDoDb{
		if db.TableSchema == dmlEvent.DatabaseName {
			for _,tb:=range db.Tables {
				if tb.TableName == dmlEvent.TableName {
					destTableColumns = tb.OriginalTableColumns
				}
			}
		}
	}
	if destTableColumns ==nil{
		destTableColumns, _, err = a.InspectTableColumnsAndUniqueKeys(dmlEvent.DatabaseName, dmlEvent.TableName)
		if err != nil {
			return "", args, 0, err
		}
	}*/

	tableColumns, err := base.GetTableColumns(a.db, dmlEvent.DatabaseName, dmlEvent.TableName)
	switch dmlEvent.DML {
	case binlog.DeleteDML:
		{
			query, uniqueKeyArgs, err := sql.BuildDMLDeleteQuery(dmlEvent.DatabaseName, dmlEvent.TableName, tableColumns, dmlEvent.WhereColumnValues.GetAbstractValues())
			return query, uniqueKeyArgs, -1, err
		}
	case binlog.InsertDML:
		{
			//query, sharedArgs,err := sql.BuildDMLInsertQuery(dmlEvent.DatabaseName, dmlEvent.TableName,dmlEvent.ColumnCount,dmlEvent.NewColumnValues)
			query, sharedArgs, err := sql.BuildDMLInsertQuery(dmlEvent.DatabaseName, dmlEvent.TableName, tableColumns, tableColumns, tableColumns, dmlEvent.NewColumnValues)
			return query, sharedArgs, 1, err
		}
	case binlog.UpdateDML:
		{
			query, sharedArgs, uniqueKeyArgs, err := sql.BuildDMLUpdateQuery(dmlEvent.DatabaseName, dmlEvent.TableName, tableColumns, tableColumns, tableColumns, tableColumns, dmlEvent.NewColumnValues[0].GetAbstractValues(), dmlEvent.WhereColumnValues.GetAbstractValues())
			args = append(args, sharedArgs...)
			args = append(args, uniqueKeyArgs...)
			return query, args, 0, err
		}
	}
	return "", args, 0, fmt.Errorf("Unknown dml event type: %+v", dmlEvent.DML)
}

// ApplyEventQueries applies multiple DML queries onto the dest table
func (a *Applier) ApplyBinlogEvent(dbApplier *sql.DB, binlogEntry *binlog.BinlogEntry) error {
	var totalDelta int64

	interval, err := base.SelectGtidExecuted(dbApplier.Db, binlogEntry.Coordinates.SID, binlogEntry.Coordinates.GNO)
	if err != nil {
		return err
	}
	if interval == "" {
		return nil
	}

	dbApplier.DbMutex.Lock()
	tx, err := dbApplier.Db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err := tx.Commit(); err != nil {
			a.onError(TaskStateDead, err)
		}
		if !a.shutdown {
			a.currentCoordinates.RelayMasterLogFile = binlogEntry.Coordinates.LogFile
			a.currentCoordinates.ReadMasterLogPos = binlogEntry.Coordinates.LogPos
			a.currentCoordinates.ExecutedGtidSet = fmt.Sprintf("%s:%d", binlogEntry.Coordinates.SID, binlogEntry.Coordinates.GNO)
			a.mysqlContext.Gtid = fmt.Sprintf("%s:1-%d", binlogEntry.Coordinates.SID, binlogEntry.Coordinates.GNO)
		}
		dbApplier.DbMutex.Unlock()
	}()
	sessionQuery := `SET @@session.foreign_key_checks = 0`
	if _, err := tx.Exec(sessionQuery); err != nil {
		return err
	}
	/*sessionQuery = `SET
			SESSION time_zone = '+00:00',
			sql_mode = CONCAT(@@session.sql_mode, ',STRICT_ALL_TABLES')
			`
	if _, err := tx.Exec(sessionQuery); err != nil {
		return err
	}*/
	for _, event := range binlogEntry.Events {
		if event.DatabaseName != "" {
			_, err := tx.Exec(fmt.Sprintf("USE %s", event.DatabaseName))
			if err != nil {
				if !sql.IgnoreError(err) {
					a.logger.Errorf("mysql.applier: Exec sql error: %v", err)
					return err
				} else {
					a.logger.Warnf("mysql.applier: Ignore error: %v", err)
				}
			}
		}
		switch event.DML {
		case binlog.NotDML:
			_, err := tx.Exec(event.Query)
			if err != nil {
				if !sql.IgnoreError(err) {
					a.logger.Errorf("mysql.applier: Exec sql error: %v", err)
					return err
				} else {
					a.logger.Warnf("mysql.applier: Ignore error: %v", err)
				}
			}
			/*for _,db:=range a.mysqlContext.ReplicateDoDb{
				for _,tb:=range db.Tables {
					tableColumns, _, err := a.InspectTableColumnsAndUniqueKeys(tb.TableSchema, tb.TableName)
					if err != nil {
						return err
					}
					tb.OriginalTableColumns = tableColumns
				}
			}*/
		default:
			query, args, rowDelta, err := a.buildDMLEventQuery(event)
			if err != nil {
				a.logger.Errorf("mysql.applier: Build dml query error: %v", err)
				return err
			}
			_, err = tx.Exec(query, args...)
			if err != nil {
				a.logger.Errorf("mysql.applier: Exec %+v,args: %v,gtid: %s:%d, error: %v", query, args, binlogEntry.Coordinates.SID, binlogEntry.Coordinates.GNO, err)
				return err
			}
			totalDelta += rowDelta
		}
	}
	query := fmt.Sprintf(`
		delete from
			actiontech_udup.gtid_executed
	 	where
	 		source_uuid = '%s'
		`,
		binlogEntry.Coordinates.SID,
	)
	if _, err := tx.Exec(query); err != nil {
		return err
	}
	query = fmt.Sprintf(`
			insert into actiontech_udup.gtid_executed
  				(source_uuid,interval_gtid)
  			values
  				('%s','%s')
		`,
		binlogEntry.Coordinates.SID,
		interval,
	)
	if _, err := tx.Exec(query); err != nil {
		return err
	}

	// no error
	a.mysqlContext.Stage = models.StageWaitingForGtidToBeCommitted
	atomic.AddInt64(&a.mysqlContext.TotalDeltaCopied, 1)
	return nil
}

func (a *Applier) ApplyEventQueries(db *gosql.DB, entry *dumpEntry) error {
	queries := []string{}
	queries = append(queries, entry.SystemVariablesStatement, entry.SqlMode, entry.DbSQL, entry.TbSQL)
	if len(entry.Values) > 0 {
		for _, e := range entry.Values {
			queries = append(queries, fmt.Sprintf(`replace into %s.%s values %s`, entry.TableSchema, entry.TableName, strings.Join(e, ",")))
		}
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err := tx.Commit(); err != nil {
			a.onError(TaskStateDead, err)
		}
	}()
	sessionQuery := `SET @@session.foreign_key_checks = 0`
	if _, err := tx.Exec(sessionQuery); err != nil {
		return err
	}
	for _, query := range queries {
		if query == "" {
			continue
		}
		_, err := tx.Exec(query)
		if err != nil {
			if !sql.IgnoreError(err) {
				a.logger.Errorf("mysql.applier: Exec [%s] error: %v", query, err)
				return err
			}
			if !sql.IgnoreExistsError(err) {
				a.logger.Warnf("mysql.applier: Ignore error: %v", err)
			}
		}
	}
	atomic.AddInt64(&a.mysqlContext.TotalRowsCopied, entry.RowsCount)
	return nil
}

func (a *Applier) Stats() (*models.TaskStatistics, error) {
	totalRowsCopied := a.mysqlContext.GetTotalRowsCopied()
	rowsEstimate := atomic.LoadInt64(&a.mysqlContext.RowsEstimate)
	totalDeltaCopied := a.mysqlContext.GetTotalDeltaCopied()
	deltaEstimate := atomic.LoadInt64(&a.mysqlContext.DeltaEstimate)

	var progressPct float64
	var backlog, eta string
	if rowsEstimate == 0 && deltaEstimate == 0 {
		progressPct = 100.0
	} else {
		progressPct = 100.0 * float64(totalDeltaCopied+totalRowsCopied) / float64(deltaEstimate+rowsEstimate)
		if atomic.LoadInt64(&a.rowCopyCompleteFlag) == 1 {
			// Done copying rows. The totalRowsCopied value is the de-facto number of rows,
			// and there is no further need to keep updating the value.
			backlog = fmt.Sprintf("%d/%d", len(a.applyDataEntryQueue), cap(a.applyDataEntryQueue))
		} else {
			backlog = fmt.Sprintf("%d/%d", len(a.copyRowsQueue), cap(a.copyRowsQueue))
		}
	}

	var etaSeconds float64 = math.MaxFloat64
	eta = "N/A"
	if progressPct >= 100.0 {
		eta = "0s"
		a.mysqlContext.Stage = models.StageSlaveHasReadAllRelayLog
	} else if progressPct >= 1.0 {
		elapsedRowCopySeconds := a.mysqlContext.ElapsedRowCopyTime().Seconds()
		totalExpectedSeconds := elapsedRowCopySeconds * float64(rowsEstimate) / float64(totalRowsCopied)
		if atomic.LoadInt64(&a.rowCopyCompleteFlag) == 1 {
			totalExpectedSeconds = elapsedRowCopySeconds * float64(deltaEstimate) / float64(totalDeltaCopied)
		}
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
		ExecMasterTxCount:  totalDeltaCopied,
		ReadMasterRowCount: rowsEstimate,
		ReadMasterTxCount:  deltaEstimate,
		ProgressPct:        strconv.FormatFloat(progressPct,'f',1,64),
		ETA:                eta,
		Backlog:            backlog,
		Stage:              a.mysqlContext.Stage,
		CurrentCoordinates: a.currentCoordinates,
		BufferStat: models.BufferStat{
			ApplierTxQueueSize:      len(a.applyBinlogTxQueue),
			ApplierGroupTxQueueSize: len(a.applyBinlogGroupTxQueue),
		},
		Timestamp: time.Now().UTC().UnixNano(),
	}
	if a.natsConn != nil {
		taskResUsage.MsgStat = a.natsConn.Statistics
	}

	return &taskResUsage, nil
}

func (a *Applier) ID() string {
	id := config.DriverCtx{
		DriverConfig: &config.MySQLDriverConfig{
			ReplicateDoDb:     a.mysqlContext.ReplicateDoDb,
			ReplicateIgnoreDb: a.mysqlContext.ReplicateIgnoreDb,
			Gtid:              a.mysqlContext.Gtid,
			NatsAddr:          a.mysqlContext.NatsAddr,
			ParallelWorkers:   a.mysqlContext.ParallelWorkers,
			ConnectionConfig:  a.mysqlContext.ConnectionConfig,
		},
	}

	data, err := json.Marshal(id)
	if err != nil {
		a.logger.Errorf("mysql.applier: Failed to marshal ID to JSON: %s", err)
	}
	return string(data)
}

func (a *Applier) onError(state int, err error) {
	if a.shutdown {
		return
	}
	switch state {
	case TaskStateComplete:
		a.logger.Printf("mysql.applier: Done migrating")
	case TaskStateRestart:
		if a.natsConn != nil {
			if err := a.natsConn.Publish(fmt.Sprintf("%s_restart", a.subject), []byte(a.mysqlContext.Gtid)); err != nil {
				a.logger.Errorf("mysql.applier: Trigger restart extractor : %v", err)
			}
		}
	default:
		if a.natsConn != nil {
			if err := a.natsConn.Publish(fmt.Sprintf("%s_error", a.subject), []byte(a.mysqlContext.Gtid)); err != nil {
				a.logger.Errorf("mysql.applier: Trigger extractor shutdown: %v", err)
			}
		}
	}

	a.waitCh <- models.NewWaitResult(state, err)
	a.Shutdown()
}

func (a *Applier) WaitCh() chan *models.WaitResult {
	return a.waitCh
}

func (a *Applier) Shutdown() error {
	a.shutdownLock.Lock()
	defer a.shutdownLock.Unlock()
	if a.shutdown {
		return nil
	}

	if a.natsConn != nil {
		a.natsConn.Close()
	}

	a.shutdown = true
	close(a.shutdownCh)

	if err := sql.CloseDB(a.db); err != nil {
		return err
	}
	if err := sql.CloseDBs(a.dbs...); err != nil {
		return err
	}

	//close(a.applyBinlogTxQueue)
	//close(a.applyBinlogGroupTxQueue)
	a.logger.Printf("mysql.applier: Shutting down")
	return nil
}
