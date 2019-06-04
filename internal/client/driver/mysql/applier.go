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

	//"encoding/base64"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/snappy"
	gonats "github.com/nats-io/go-nats"
	gomysql "github.com/siddontang/go-mysql/mysql"

	"container/heap"
	"context"
	"encoding/hex"
	"os"

	"github.com/actiontech/dtle/internal/client/driver/mysql/base"
	"github.com/actiontech/dtle/internal/client/driver/mysql/binlog"
	"github.com/actiontech/dtle/internal/client/driver/mysql/sql"
	"github.com/actiontech/dtle/internal/config"
	umconf "github.com/actiontech/dtle/internal/config/mysql"
	log "github.com/actiontech/dtle/internal/logger"
	"github.com/actiontech/dtle/internal/models"
	"github.com/actiontech/dtle/utils"

	"github.com/satori/go.uuid"
)

const (
	cleanupGtidExecutedLimit = 4096
	pingInterval             = 10 * time.Second
)
const (
	TaskStateComplete int = iota
	TaskStateRestart
	TaskStateDead
)

// from container/heap/example_intheap_test.go
type Int64PriQueue []int64

func (q Int64PriQueue) Len() int           { return len(q) }
func (q Int64PriQueue) Less(i, j int) bool { return q[i] < q[j] }
func (q Int64PriQueue) Swap(i, j int)      { q[i], q[j] = q[j], q[i] }
func (q *Int64PriQueue) Push(x interface{}) {
	*q = append(*q, x.(int64))
}
func (q *Int64PriQueue) Pop() interface{} {
	old := *q
	n := len(old)
	x := old[n-1]
	*q = old[0 : n-1]
	return x
}

type applierTableItem struct {
	columns  *umconf.ColumnList
	psInsert []*gosql.Stmt
	psDelete []*gosql.Stmt
	psUpdate []*gosql.Stmt
}

func newApplierTableItem(parallelWorkers int) *applierTableItem {
	return &applierTableItem{
		columns:  nil,
		psInsert: make([]*gosql.Stmt, parallelWorkers),
		psDelete: make([]*gosql.Stmt, parallelWorkers),
		psUpdate: make([]*gosql.Stmt, parallelWorkers),
	}
}
func (ait *applierTableItem) Reset() {
	// TODO handle err of `.Close()`?
	closeStmts := func(stmts []*gosql.Stmt) {
		for i := range stmts {
			if stmts[i] != nil {
				stmts[i].Close()
				stmts[i] = nil
			}
		}
	}
	closeStmts(ait.psInsert)
	closeStmts(ait.psDelete)
	closeStmts(ait.psUpdate)

	ait.columns = nil
}

type mapSchemaTableItems map[string](map[string](*applierTableItem))

// Applier connects and writes the the applier-server, which is the server where
// write row data and apply binlog events onto the dest table.

type MtsManager struct {
	// If you set lastCommit = N, all tx with seqNum <= N must have been executed
	lastCommitted int64
	lastEnqueue   int64
	updated       chan struct{}
	shutdownCh    chan struct{}
	// SeqNum executed but not added to LC
	m          Int64PriQueue
	chExecuted chan int64
}

//  shutdownCh: close to indicate a shutdown
func NewMtsManager(shutdownCh chan struct{}) *MtsManager {
	return &MtsManager{
		lastCommitted: 0,
		updated:       make(chan struct{}, 1), // 1-buffered, see #211-7.1
		shutdownCh:    shutdownCh,
		m:             nil,
		chExecuted:    make(chan int64),
	}
}

//  This function must be called sequentially.
func (mm *MtsManager) WaitForAllCommitted() bool {
	for {
		if mm.lastCommitted == mm.lastEnqueue {
			return true
		}

		select {
		case <-mm.updated:
			// continue
		case <-mm.shutdownCh:
			return false
		}
	}
}

// block for waiting. return true for can_execute, false for abortion.
//  This function must be called sequentially.
func (mm *MtsManager) WaitForExecution(binlogEntry *binlog.BinlogEntry) bool {
	mm.lastEnqueue = binlogEntry.Coordinates.SeqenceNumber

	for {
		currentLC := atomic.LoadInt64(&mm.lastCommitted)
		if currentLC >= binlogEntry.Coordinates.LastCommitted {
			return true
		}

		// block until lastCommitted updated
		select {
		case <-mm.updated:
			// continue
		case <-mm.shutdownCh:
			return false
		}
	}
}

func (mm *MtsManager) LcUpdater() {
	for {
		select {
		case seqNum := <-mm.chExecuted:
			if seqNum <= mm.lastCommitted {
				// ignore it
			} else {
				heap.Push(&mm.m, seqNum)

				for mm.m.Len() > 0 {
					least := mm.m[0]
					if least == mm.lastCommitted+1 {
						heap.Pop(&mm.m)
						atomic.AddInt64(&mm.lastCommitted, 1)
						select {
						case mm.updated <- struct{}{}:
						default: // non-blocking
						}
					} else {
						break
					}
				}
			}

		case <-mm.shutdownCh:
			return
		}
	}
}

func (mm *MtsManager) Executed(binlogEntry *binlog.BinlogEntry) {
	mm.chExecuted <- binlogEntry.Coordinates.SeqenceNumber
}

type Applier struct {
	logger             *log.Entry
	subject            string
	subjectUUID        uuid.UUID
	tp                 string
	mysqlContext       *config.MySQLDriverConfig
	dbs                []*sql.Conn
	db                 *gosql.DB
	gtidExecuted       base.GtidSet
	currentCoordinates *models.CurrentCoordinates
	tableItems         mapSchemaTableItems

	rowCopyComplete     chan bool
	rowCopyCompleteFlag int64
	// copyRowsQueue should not be buffered; if buffered some non-damaging but
	//  excessive work happens at the end of the iteration as new copy-jobs arrive befroe realizing the copy is complete
	copyRowsQueue           chan *DumpEntry
	applyDataEntryQueue     chan *binlog.BinlogEntry
	applyBinlogTxQueue      chan *binlog.BinlogTx
	applyBinlogGroupTxQueue chan []*binlog.BinlogTx
	// only TX can be executed should be put into this chan
	applyBinlogMtsTxQueue chan *binlog.BinlogEntry
	lastAppliedBinlogTx   *binlog.BinlogTx

	natsConn *gonats.Conn
	waitCh   chan *models.WaitResult
	wg       sync.WaitGroup

	shutdown     bool
	shutdownCh   chan struct{}
	shutdownLock sync.Mutex

	mtsManager     *MtsManager
	printTps       bool
	txLastNSeconds uint32
	nDumpEntry     int64

	stubFullApplyDelay bool

	// TODO we might need to save these from DumpEntry for reconnecting.
	//SystemVariablesStatement string
	//SqlMode                  string
}

func NewApplier(subject, tp string, cfg *config.MySQLDriverConfig, logger *log.Logger) (*Applier, error) {
	cfg = cfg.SetDefault()
	entry := log.NewEntry(logger).WithFields(log.Fields{
		"job": subject,
	})
	subjectUUID, err := uuid.FromString(subject)
	if err != nil {
		logger.Errorf("job id is not a valid UUID: %v", err.Error())
		return nil, err
	}

	a := &Applier{
		logger:                  entry,
		subject:                 subject,
		subjectUUID:             subjectUUID,
		tp:                      tp,
		mysqlContext:            cfg,
		currentCoordinates:      &models.CurrentCoordinates{},
		tableItems:              make(mapSchemaTableItems),
		rowCopyComplete:         make(chan bool, 1),
		copyRowsQueue:           make(chan *DumpEntry, 24),
		applyDataEntryQueue:     make(chan *binlog.BinlogEntry, cfg.ReplChanBufferSize*2),
		applyBinlogMtsTxQueue:   make(chan *binlog.BinlogEntry, cfg.ReplChanBufferSize*2),
		applyBinlogTxQueue:      make(chan *binlog.BinlogTx, cfg.ReplChanBufferSize*2),
		applyBinlogGroupTxQueue: make(chan []*binlog.BinlogTx, cfg.ReplChanBufferSize*2),
		waitCh:                  make(chan *models.WaitResult, 1),
		shutdownCh:              make(chan struct{}),
		printTps:                os.Getenv(g.ENV_PRINT_TPS) != "",
		stubFullApplyDelay:      os.Getenv(g.ENV_FULL_APPLY_DELAY) != "",
	}
	a.mtsManager = NewMtsManager(a.shutdownCh)
	go a.mtsManager.LcUpdater()
	return a, nil
}

func (a *Applier) MtsWorker(workerIndex int) {
	keepLoop := true

	for keepLoop {
		timer := time.NewTimer(pingInterval)
		select {
		case tx := <-a.applyBinlogMtsTxQueue:
			a.logger.Debugf("mysql.applier: a binlogEntry MTS dequeue, worker: %v. GNO: %v",
				workerIndex, tx.Coordinates.GNO)
			if err := a.ApplyBinlogEvent(workerIndex, tx); err != nil {
				a.onError(TaskStateDead, err) // TODO coordinate with other goroutine
				keepLoop = false
			} else {
				// do nothing
			}
			a.logger.Debugf("mysql.applier: worker: %v. after ApplyBinlogEvent. GNO: %v",
				workerIndex, tx.Coordinates.GNO)
		case <-a.shutdownCh:
			keepLoop = false
		case <-timer.C:
			err := a.dbs[workerIndex].Db.PingContext(context.Background())
			if err != nil {
				a.logger.Errorf("mysql.applier. bad connection for mts worker. workerIndex: %v, err: %v",
					workerIndex, err)
			}
		}
		timer.Stop()
	}
}

// Run executes the complete apply logic.
func (a *Applier) Run() {
	if a.printTps {
		go func() {
			for {
				time.Sleep(5 * time.Second)
				n := atomic.SwapUint32(&a.txLastNSeconds, 0)
				a.logger.Infof("mysql.applier: txLastNSeconds: %v", n)
			}
		}()
	}

	a.logger.Printf("mysql.applier: Apply binlog events to %s.%d", a.mysqlContext.ConnectionConfig.Host, a.mysqlContext.ConnectionConfig.Port)
	a.mysqlContext.StartTime = time.Now()
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

	for i := 0; i < a.mysqlContext.ParallelWorkers; i++ {
		go a.MtsWorker(i)
	}

	go a.executeWriteFuncs()
}

func (a *Applier) onApplyTxStructWithSuper(dbApplier *sql.Conn, binlogTx *binlog.BinlogTx) error {
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
	if a.mysqlContext.Gtid == "" {
		go func() {
			var stopLoop = false
			for !stopLoop {
				select {
				case copyRows := <-a.copyRowsQueue:
					if nil != copyRows {
						//time.Sleep(20 * time.Second) // #348 stub
						if err := a.ApplyEventQueries(a.db, copyRows); err != nil {
							a.onError(TaskStateDead, err)
						}
					}
					if atomic.LoadInt64(&a.nDumpEntry) < 0 {
						a.onError(TaskStateDead, fmt.Errorf("DTLE_BUG"))
					} else {
						atomic.AddInt64(&a.nDumpEntry, -1)
					}
				case <-a.rowCopyComplete:
					stopLoop = true
				case <-a.shutdownCh:
					stopLoop = true
				case <-time.After(10 * time.Second):
					a.logger.Debugf("mysql.applier: no copyRows for 10s.")
				}
			}
		}()
	}

	if a.mysqlContext.Gtid == "" {
		a.logger.Printf("mysql.applier: Operating until row copy is complete")
		a.mysqlContext.Stage = models.StageSlaveWaitingForWorkersToProcessQueue
		for {
			if atomic.LoadInt64(&a.rowCopyCompleteFlag) == 1 && a.mysqlContext.TotalRowsCopied == a.mysqlContext.TotalRowsReplay {
				a.rowCopyComplete <- true
				a.logger.Printf("mysql.applier: Rows copy complete.number of rows:%d", a.mysqlContext.TotalRowsReplay)
				a.mysqlContext.Gtid = a.currentCoordinates.RetrievedGtidSet
				break
			}
			if a.shutdown {
				break
			}
			time.Sleep(time.Second)
		}
	}

	var dbApplier *sql.Conn

	stopMTSIncrLoop := false
	for !stopMTSIncrLoop {
		select {
		case <-a.shutdownCh:
			stopMTSIncrLoop = true
		case groupTx := <-a.applyBinlogGroupTxQueue:
			// this chan is used for homogeneous
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
		case <-time.After(1 * time.Second):
			// do nothing
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

func (a *Applier) setTableItemForBinlogEntry(binlogEntry *binlog.BinlogEntry) error {
	var err error
	for i := range binlogEntry.Events {
		dmlEvent := &binlogEntry.Events[i]
		switch dmlEvent.DML {
		case binlog.NotDML:
			// do nothing
		default:
			tableItem := a.getTableItem(dmlEvent.DatabaseName, dmlEvent.TableName)
			if tableItem.columns == nil {
				a.logger.Debugf("mysql.applier: get tableColumns %v.%v", dmlEvent.DatabaseName, dmlEvent.TableName)
				tableItem.columns, err = base.GetTableColumns(a.db, dmlEvent.DatabaseName, dmlEvent.TableName)
				if err != nil {
					a.logger.Errorf("mysql.applier. GetTableColumns error. err: %v", err)
					return err
				}
				// Review: column types is not applied or used. Only
			} else {
				a.logger.Debugf("mysql.applier: reuse tableColumns %v.%v", dmlEvent.DatabaseName, dmlEvent.TableName)
			}
			dmlEvent.TableItem = tableItem
		}
	}
	return nil
}

func (a *Applier) cleanGtidExecuted(sid uuid.UUID, intervalStr string) error {
	a.logger.Debugf("mysql.applier. incr. cleanup before WaitForExecution")
	if !a.mtsManager.WaitForAllCommitted() {
		return nil // shutdown
	}
	a.logger.Debugf("mysql.applier. incr. cleanup after WaitForExecution")

	// The TX is unnecessary if we first insert and then delete.
	// However, consider `binlog_group_commit_sync_delay > 0`,
	// `begin; delete; insert; commit;` (1 TX) is faster than `insert; delete;` (2 TX)
	dbApplier := a.dbs[0]
	tx, err := dbApplier.Db.BeginTx(context.Background(), &gosql.TxOptions{})
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			tx.Rollback()
		} else {
			err = tx.Commit()
		}
	}()

	_, err = dbApplier.PsDeleteExecutedGtid.Exec(sid.Bytes())
	if err != nil {
		return err
	}

	a.logger.Debugf("mysql.applier: compactation gtid. new interval: %v", intervalStr)
	_, err = dbApplier.PsInsertExecutedGtid.Exec(sid.Bytes(), intervalStr)
	if err != nil {
		return err
	}
	return nil
}

func (a *Applier) heterogeneousReplay() {
	var err error
	stopSomeLoop := false
	prevDDL := false
	for !stopSomeLoop {
		select {
		case binlogEntry := <-a.applyDataEntryQueue:
			if nil == binlogEntry {
				continue
			}

			a.logger.Debugf("mysql.applier: a binlogEntry. remaining: %v. gno: %v, lc: %v, seq: %v",
				len(a.applyDataEntryQueue), binlogEntry.Coordinates.GNO,
				binlogEntry.Coordinates.LastCommitted, binlogEntry.Coordinates.SeqenceNumber)

			if binlogEntry.Coordinates.OSID == a.mysqlContext.MySQLServerUuid {
				a.logger.Debugf("mysql.applier: skipping a dtle tx. osid: %v", binlogEntry.Coordinates.OSID)
				continue
			}

			// region TestIfExecuted
			if a.gtidExecuted == nil {
				// udup crash recovery or never executed
				a.gtidExecuted, err = base.SelectAllGtidExecuted(a.db, a.subjectUUID)
				if err != nil {
					a.onError(TaskStateDead, err)
					return
				}
			}

			txSid := binlogEntry.Coordinates.GetSid()

			gtidSetItem, hasSid := a.gtidExecuted[binlogEntry.Coordinates.SID]
			if !hasSid {
				gtidSetItem = &base.GtidExecutedItem{}
				a.gtidExecuted[binlogEntry.Coordinates.SID] = gtidSetItem
			}
			if base.IntervalSlicesContainOne(gtidSetItem.Intervals, binlogEntry.Coordinates.GNO) {
				// entry executed
				a.logger.Debugf("mysql.applier: skip an executed tx: %v:%v", txSid, binlogEntry.Coordinates.GNO)
				continue
			}
			// endregion

			// this must be after duplication check
			var rotated bool
			if a.currentCoordinates.File == binlogEntry.Coordinates.LogFile {
				rotated = false
			} else {
				rotated = true
				a.currentCoordinates.File = binlogEntry.Coordinates.LogFile
			}

			a.logger.Debugf("mysql.applier. gtidSetItem.NRow: %v", gtidSetItem.NRow)
			if gtidSetItem.NRow >= cleanupGtidExecutedLimit {
				err = a.cleanGtidExecuted(binlogEntry.Coordinates.SID, base.StringInterval(gtidSetItem.Intervals))
				if err != nil {
					a.onError(TaskStateDead, err)
					return
				}
				gtidSetItem.NRow = 1
			}

			thisInterval := gomysql.Interval{Start: binlogEntry.Coordinates.GNO, Stop: binlogEntry.Coordinates.GNO + 1}

			gtidSetItem.NRow += 1
			// TODO normalize may affect oringinal intervals
			newInterval := append(gtidSetItem.Intervals, thisInterval).Normalize()
			// TODO this is assigned before real execution
			gtidSetItem.Intervals = newInterval

			if binlogEntry.Coordinates.SeqenceNumber == 0 {
				// MySQL 5.6: non mts
				err := a.setTableItemForBinlogEntry(binlogEntry)
				if err != nil {
					a.onError(TaskStateDead, err)
					return
				}
				if err := a.ApplyBinlogEvent(0, binlogEntry); err != nil {
					a.onError(TaskStateDead, err)
					return
				}
			} else {
				if rotated {
					a.logger.Debugf("mysql.applier: binlog rotated to %v", a.currentCoordinates.File)
					if !a.mtsManager.WaitForAllCommitted() {
						return // shutdown
					}
					a.mtsManager.lastCommitted = 0
					a.mtsManager.lastEnqueue = 0
					if len(a.mtsManager.m) != 0 {
						a.logger.Warnf("DTLE_BUG: len(a.mtsManager.m) should be 0")
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
						case binlog.NotDML:
							return true
						default:
						}
					}
					return false
				}()

				// DDL must be executed separatedly
				if hasDDL || prevDDL {
					a.logger.Debugf("mysql.applier: gno: %v MTS found DDL(%v,%v). WaitForAllCommitted",
						binlogEntry.Coordinates.GNO, hasDDL, prevDDL)
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

				a.logger.Debugf("mysql.applier: a binlogEntry MTS enqueue. gno: %v", binlogEntry.Coordinates.GNO)
				err = a.setTableItemForBinlogEntry(binlogEntry)
				if err != nil {
					a.onError(TaskStateDead, err)
					return
				}
				a.applyBinlogMtsTxQueue <- binlogEntry
			}
			if !a.shutdown {
				// TODO what is this used for?
				a.mysqlContext.Gtid = fmt.Sprintf("%s:1-%d", txSid, binlogEntry.Coordinates.GNO)
			}
		case <-time.After(10 * time.Second):
			a.logger.Debugf("mysql.applier: no binlogEntry for 10s")
		case <-a.shutdownCh:
			stopSomeLoop = true
		}
	}
}
func (a *Applier) homogeneousReplay() {
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
}

// initiateStreaming begins treaming of binary log events and registers listeners for such events
func (a *Applier) initiateStreaming() error {
	a.mysqlContext.MarkRowCopyStartTime()
	a.logger.Debugf("mysql.applier: nats subscribe")
	_, err := a.natsConn.Subscribe(fmt.Sprintf("%s_full", a.subject), func(m *gonats.Msg) {
		a.logger.Debugf("mysql.applier: full. recv a msg. copyRowsQueue: %v", len(a.copyRowsQueue))

		dumpData := &DumpEntry{}
		data2, err := snappy.Decode(nil, m.Data)
		if err != nil {
			a.onError(TaskStateDead, err)
			// TODO return?
		}
		_, err = dumpData.Unmarshal(data2)
		if err != nil {
			a.onError(TaskStateDead, err)
			//return
		}

		timer := time.NewTimer(DefaultConnectWait / 2)
		atomic.AddInt64(&a.nDumpEntry, 1) // this must be increased before enqueuing
		select {
		case a.copyRowsQueue <- dumpData:
			a.logger.Debugf("mysql.applier: full. enqueue")
			timer.Stop()
			a.mysqlContext.Stage = models.StageSlaveWaitingForWorkersToProcessQueue
			if err := a.natsConn.Publish(m.Reply, nil); err != nil {
				a.onError(TaskStateDead, err)
			}
			a.logger.Debugf("mysql.applier. full. after publish nats reply")
			atomic.AddInt64(&a.mysqlContext.RowsEstimate, dumpData.TotalCount)
		case <-timer.C:
			atomic.AddInt64(&a.nDumpEntry, -1)

			a.logger.Debugf("mysql.applier. full. discarding entries")
			a.mysqlContext.Stage = models.StageSlaveWaitingForWorkersToProcessQueue
		}
	})
	/*if err := sub.SetPendingLimits(a.mysqlContext.MsgsLimit, a.mysqlContext.BytesLimit); err != nil {
		return err
	}*/

	_, err = a.natsConn.Subscribe(fmt.Sprintf("%s_full_complete", a.subject), func(m *gonats.Msg) {
		dumpData := &dumpStatResult{}
		if err := Decode(m.Data, dumpData); err != nil {
			a.onError(TaskStateDead, err)
		}
		a.currentCoordinates.RetrievedGtidSet = dumpData.Gtid
		a.mysqlContext.Stage = models.StageSlaveWaitingForWorkersToProcessQueue

		for atomic.LoadInt64(&a.nDumpEntry) != 0 {
			a.logger.Debugf("mysql.applier. nDumpEntry is not zero, waiting. %v", a.nDumpEntry)
			time.Sleep(1 * time.Second)
			if a.shutdown {
				return
			}
		}

		a.logger.Debugf("mysql.applier. ack full_complete")
		if err := a.natsConn.Publish(m.Reply, nil); err != nil {
			a.onError(TaskStateDead, err)
		}
		atomic.AddInt64(&a.mysqlContext.TotalRowsCopied, dumpData.TotalCount)
		atomic.StoreInt64(&a.rowCopyCompleteFlag, 1)
	})
	if err != nil {
		return err
	}

	if a.mysqlContext.ApproveHeterogeneous {
		_, err := a.natsConn.Subscribe(fmt.Sprintf("%s_incr_hete", a.subject), func(m *gonats.Msg) {
			var binlogEntries binlog.BinlogEntries
			if err := Decode(m.Data, &binlogEntries); err != nil {
				a.onError(TaskStateDead, err)
			}

			nEntries := len(binlogEntries.Entries)

			handled := false
			for i := 0; !handled && (i < DefaultConnectWaitSecond/2); i++ {
				vacancy := cap(a.applyDataEntryQueue) - len(a.applyDataEntryQueue)
				a.logger.Debugf("applier. incr. nEntries: %v, vacancy: %v", nEntries, vacancy)
				if vacancy < nEntries {
					a.logger.Debugf("applier. incr. wait 1s for applyDataEntryQueue")
					time.Sleep(1 * time.Second) // It will wait an second at the end, but seems no hurt.
				} else {
					a.logger.Debugf("applier. incr. applyDataEntryQueue enqueue")
					for _, binlogEntry := range binlogEntries.Entries {
						a.applyDataEntryQueue <- binlogEntry
						a.currentCoordinates.RetrievedGtidSet = binlogEntry.Coordinates.GetGtidForThisTx()
						atomic.AddInt64(&a.mysqlContext.DeltaEstimate, 1)
					}
					a.mysqlContext.Stage = models.StageWaitingForMasterToSendEvent

					if err := a.natsConn.Publish(m.Reply, nil); err != nil {
						a.onError(TaskStateDead, err)
					}
					a.logger.Debugf("applier. incr. ack-recv. nEntries: %v", nEntries)

					handled = true
				}
			}
			if !handled {
				// discard these entries
				a.logger.Debugf("applier. incr. discarding entries")
				a.mysqlContext.Stage = models.StageWaitingForMasterToSendEvent
			}
		})
		if err != nil {
			return err
		}

		go a.heterogeneousReplay()
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
		go a.homogeneousReplay()
	}

	return nil
}

func (a *Applier) initDBConnections() (err error) {
	applierUri := a.mysqlContext.ConnectionConfig.GetDBUri()
	if a.db, err = sql.CreateDB(applierUri); err != nil {
		return err
	}
	a.db.SetMaxOpenConns(10 + a.mysqlContext.ParallelWorkers)

	if a.dbs, err = sql.CreateConns(a.db, a.mysqlContext.ParallelWorkers); err != nil {
		return err
	}

	if err := a.validateConnection(a.db); err != nil {
		return err
	}
	if err := a.validateServerUUID(); err != nil {
		return err
	}
	if err := a.validateGrants(); err != nil {
		a.logger.Errorf("mysql.applier: Unexpected error on validateGrants, got %v", err)
		return err
	}
	a.logger.Debugf("mysql.applier. after validateGrants")
	if err := a.validateAndReadTimeZone(); err != nil {
		return err
	}
	a.logger.Debugf("mysql.applier. after validateAndReadTimeZone")

	if a.mysqlContext.ApproveHeterogeneous {
		if err := a.createTableGtidExecutedV3(); err != nil {
			return err
		}
		a.logger.Debugf("mysql.applier. after createTableGtidExecutedV2")

		for i := range a.dbs {
			a.dbs[i].PsDeleteExecutedGtid, err = a.dbs[i].Db.PrepareContext(context.Background(), fmt.Sprintf("delete from %v.%v where job_uuid = unhex('%s') and source_uuid = ?",
				g.DtleSchemaName, g.GtidExecutedTableV3, hex.EncodeToString(a.subjectUUID.Bytes())))
			if err != nil {
				return err
			}
			a.dbs[i].PsInsertExecutedGtid, err = a.dbs[i].Db.PrepareContext(context.Background(), fmt.Sprintf("replace into %v.%v "+
				"(job_uuid,source_uuid,interval_gtid) "+
				"values (unhex('%s'), ?, ?)",
				g.DtleSchemaName, g.GtidExecutedTableV3,
				hex.EncodeToString(a.subjectUUID.Bytes())))
			if err != nil {
				return err
			}

		}
		a.logger.Debugf("mysql.applier. after prepare stmt for gtid_executed table")
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
	if a.mysqlContext.SkipPrivilegeCheck {
		a.logger.Debugf("mysql.applier: skipping priv check")
		return nil
	}
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
			if strings.Contains(grant, fmt.Sprintf("GRANT ALL PRIVILEGES ON `%v`.`%v`",
				g.DtleSchemaName, g.GtidExecutedTableV3)) {
				foundDBAll = true
			}
			if base.StringContainsAll(grant, `ALTER`, `CREATE`, `DELETE`, `DROP`, `INDEX`, `INSERT`, `SELECT`, `TRIGGER`, `UPDATE`, ` ON`) {
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

// validateAndReadTimeZone potentially reads server time-zone
func (a *Applier) validateAndReadTimeZone() error {
	query := `select @@global.time_zone`
	if err := a.db.QueryRow(query).Scan(&a.mysqlContext.TimeZone); err != nil {
		return err
	}

	a.logger.Printf("mysql.applier: Will use time_zone='%s' on applier", a.mysqlContext.TimeZone)
	return nil
}
func (a *Applier) migrateGtidExecutedV2toV3() error {
	a.logger.Infof(`migrateGtidExecutedV2toV3 starting`)

	var err error
	var query string

	logErr := func(query string, err error) {
		a.logger.Errorf(`migrateGtidExecutedV2toV3 failed. manual intervention might be required. query: %v. err: %v`,
			query, err)
	}

	query = fmt.Sprintf("alter table %v.%v rename to %v.%v",
		g.DtleSchemaName, g.GtidExecutedTableV2, g.DtleSchemaName, g.GtidExecutedTempTable2To3)
	_, err = a.db.Exec(query)
	if err != nil {
		logErr(query, err)
		return err
	}

	query = fmt.Sprintf("alter table %v.%v modify column interval_gtid longtext",
		g.DtleSchemaName, g.GtidExecutedTempTable2To3)
	_, err = a.db.Exec(query)
	if err != nil {
		logErr(query, err)
		return err
	}

	query = fmt.Sprintf("alter table %v.%v rename to %v.%v",
		g.DtleSchemaName, g.GtidExecutedTempTable2To3, g.DtleSchemaName, g.GtidExecutedTableV3)
	_, err = a.db.Exec(query)
	if err != nil {
		logErr(query, err)
		return err
	}

	a.logger.Infof(`migrateGtidExecutedV2toV3 done`)

	return nil
}
func (a *Applier) createTableGtidExecutedV3() error {
	if result, err := sql.QueryResultData(a.db, fmt.Sprintf("SHOW TABLES FROM %v LIKE '%v%%'",
		g.DtleSchemaName, g.GtidExecutedTempTablePrefix)); nil == err && len(result) > 0 {
		return fmt.Errorf("GtidExecutedTempTable exists. require manual intervention")
	}

	if result, err := sql.QueryResultData(a.db, fmt.Sprintf("SHOW TABLES FROM %v LIKE '%v%%'",
		g.DtleSchemaName, g.GtidExecutedTablePrefix)); nil == err && len(result) > 0 {
		if len(result) > 1 {
			return fmt.Errorf("multiple GtidExecutedTable exists, while at most one is allowed. require manual intervention")
		} else {
			if len(result[0]) < 1 {
				return fmt.Errorf("mysql error: expect 1 column for 'SHOW TABLES' query")
			}
			switch result[0][0].String {
			case g.GtidExecutedTableV2:
				err = a.migrateGtidExecutedV2toV3()
				if err != nil {
					return err
				}
			case g.GtidExecutedTableV3:
				return nil
			default:
				return fmt.Errorf("newer GtidExecutedTable exists, which is unrecognized by this verion. require manual intervention")
			}
		}
	}

	a.logger.Debugf("mysql.applier. after show gtid_executed table")

	query := fmt.Sprintf(`
			CREATE DATABASE IF NOT EXISTS %v;
		`, g.DtleSchemaName)
	if _, err := a.db.Exec(query); err != nil {
		return err
	}
	a.logger.Debugf("mysql.applier. after create dtle schema")

	query = fmt.Sprintf(`
			CREATE TABLE IF NOT EXISTS %v.%v (
				job_uuid binary(16) NOT NULL COMMENT 'unique identifier of job',
				source_uuid binary(16) NOT NULL COMMENT 'uuid of the source where the transaction was originally executed.',
				interval_gtid longtext NOT NULL COMMENT 'number of interval.'
			);
		`, g.DtleSchemaName, g.GtidExecutedTableV3)
	if _, err := a.db.Exec(query); err != nil {
		return err
	}
	a.logger.Debugf("mysql.applier. after create gtid_executed table")

	return nil
}

func (a *Applier) getTableItem(schema string, table string) *applierTableItem {
	schemaItem, ok := a.tableItems[schema]
	if !ok {
		schemaItem = make(map[string]*applierTableItem)
		a.tableItems[schema] = schemaItem
	}

	tableItem, ok := schemaItem[table]
	if !ok {
		tableItem = newApplierTableItem(a.mysqlContext.ParallelWorkers)
		schemaItem[table] = tableItem
	}

	return tableItem
}

// buildDMLEventQuery creates a query to operate on the ghost table, based on an intercepted binlog
// event entry on the original table.
func (a *Applier) buildDMLEventQuery(dmlEvent binlog.DataEvent, workerIdx int) (query *gosql.Stmt, args []interface{}, rowsDelta int64, err error) {
	// Large piece of code deleted here. See git annotate.
	tableItem := dmlEvent.TableItem.(*applierTableItem)
	var tableColumns = tableItem.columns

	doPrepareIfNil := func(stmts []*gosql.Stmt, query string) (*gosql.Stmt, error) {
		var err error
		if stmts[workerIdx] == nil {
			a.logger.Debugf("mysql.applier buildDMLEventQuery prepare query %v", query)
			stmts[workerIdx], err = a.dbs[workerIdx].Db.PrepareContext(context.Background(), query)
			if err != nil {
				a.logger.Errorf("mysql.applier buildDMLEventQuery prepare query %v err %v", query, err)
			}
		}
		return stmts[workerIdx], err
	}

	switch dmlEvent.DML {
	case binlog.DeleteDML:
		{
			query, uniqueKeyArgs, err := sql.BuildDMLDeleteQuery(dmlEvent.DatabaseName, dmlEvent.TableName, tableColumns, dmlEvent.WhereColumnValues.GetAbstractValues())
			if err != nil {
				return nil, nil, -1, err
			}
			stmt, err := doPrepareIfNil(tableItem.psDelete, query)
			if err != nil {
				return nil, nil, -1, err
			}
			return stmt, uniqueKeyArgs, -1, err
		}
	case binlog.InsertDML:
		{
			// TODO no need to generate query string every time
			query, sharedArgs, err := sql.BuildDMLInsertQuery(dmlEvent.DatabaseName, dmlEvent.TableName, tableColumns, tableColumns, tableColumns, dmlEvent.NewColumnValues.GetAbstractValues())
			if err != nil {
				return nil, nil, -1, err
			}
			stmt, err := doPrepareIfNil(tableItem.psInsert, query)
			if err != nil {
				return nil, nil, -1, err
			}
			return stmt, sharedArgs, 1, err
		}
	case binlog.UpdateDML:
		{
			query, sharedArgs, uniqueKeyArgs, err := sql.BuildDMLUpdateQuery(dmlEvent.DatabaseName, dmlEvent.TableName, tableColumns, tableColumns, tableColumns, tableColumns, dmlEvent.NewColumnValues.GetAbstractValues(), dmlEvent.WhereColumnValues.GetAbstractValues())
			if err != nil {
				return nil, nil, -1, err
			}
			args = append(args, sharedArgs...)
			args = append(args, uniqueKeyArgs...)

			stmt, err := doPrepareIfNil(tableItem.psUpdate, query)
			if err != nil {
				return nil, nil, -1, err
			}

			return stmt, args, 0, err
		}
	}
	return nil, args, 0, fmt.Errorf("Unknown dml event type: %+v", dmlEvent.DML)
}

// ApplyEventQueries applies multiple DML queries onto the dest table
func (a *Applier) ApplyBinlogEvent(workerIdx int, binlogEntry *binlog.BinlogEntry) error {
	dbApplier := a.dbs[workerIdx]

	var totalDelta int64
	var err error

	txSid := binlogEntry.Coordinates.GetSid()

	dbApplier.DbMutex.Lock()
	tx, err := dbApplier.Db.BeginTx(context.Background(), &gosql.TxOptions{})
	if err != nil {
		return err
	}
	defer func() {
		if err := tx.Commit(); err != nil {
			a.onError(TaskStateDead, err)
		} else {
			a.mtsManager.Executed(binlogEntry)
		}
		if a.printTps {
			atomic.AddUint32(&a.txLastNSeconds, 1)
		}

		dbApplier.DbMutex.Unlock()
	}()

	for i, event := range binlogEntry.Events {
		a.logger.Debugf("mysql.applier: ApplyBinlogEvent. gno: %v, event: %v",
			binlogEntry.Coordinates.GNO, i)
		switch event.DML {
		case binlog.NotDML:
			var err error
			a.logger.Debugf("mysql.applier: ApplyBinlogEvent: not dml: %v", event.Query)

			if event.CurrentSchema != "" {
				// TODO escape schema name?
				query := fmt.Sprintf("USE %s", event.CurrentSchema)
				a.logger.Debugf("mysql.applier: query: %v", query)
				_, err = tx.Exec(query)
				if err != nil {
					if !sql.IgnoreError(err) {
						a.logger.Errorf("mysql.applier: Exec sql error: %v", err)
						return err
					} else {
						a.logger.Warnf("mysql.applier: Ignore error: %v", err)
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
				a.logger.Debugf("mysql.applier: reset tableItem %v.%v", schema, event.TableName)
				a.getTableItem(schema, event.TableName).Reset()
			} else { // TableName == ""
				if event.DatabaseName != "" {
					if schemaItem, ok := a.tableItems[event.DatabaseName]; ok {
						for tableName, v := range schemaItem {
							a.logger.Debugf("mysql.applier: reset tableItem %v.%v", event.DatabaseName, tableName)
							v.Reset()
						}
					}
					delete(a.tableItems, event.DatabaseName)
				}
			}

			_, err = tx.Exec(event.Query)
			if err != nil {
				if !sql.IgnoreError(err) {
					a.logger.Errorf("mysql.applier: Exec sql error: %v", err)
					return err
				} else {
					a.logger.Warnf("mysql.applier: Ignore error: %v", err)
				}
			}
			a.logger.Debugf("mysql.applier: Exec [%s]", event.Query)
		default:
			a.logger.Debugf("mysql.applier: ApplyBinlogEvent: a dml event")
			stmt, args, rowDelta, err := a.buildDMLEventQuery(event, workerIdx)
			if err != nil {
				a.logger.Errorf("mysql.applier: Build dml query error: %v", err)
				return err
			}

			a.logger.Debugf("ApplyBinlogEvent. args: %v", args)

			var r gosql.Result
			r, err = stmt.Exec(args...)
			if err != nil {
				a.logger.Errorf("mysql.applier: gtid: %s:%d, error: %v", txSid, binlogEntry.Coordinates.GNO, err)
				return err
			}
			nr, err := r.RowsAffected()
			if err != nil {
				a.logger.Debugf("ApplyBinlogEvent executed gno %v event %v rows_affected_err %v schema", binlogEntry.Coordinates.GNO, i, err)
			} else {
				a.logger.Debugf("ApplyBinlogEvent executed gno %v event %v rows_affected %v", binlogEntry.Coordinates.GNO, i, nr)
			}
			totalDelta += rowDelta
		}
	}

	a.logger.Debugf("ApplyBinlogEvent. insert gno: %v", binlogEntry.Coordinates.GNO)
	_, err = dbApplier.PsInsertExecutedGtid.Exec(binlogEntry.Coordinates.SID.Bytes(), binlogEntry.Coordinates.GNO)
	if err != nil {
		return err
	}

	// no error
	a.mysqlContext.Stage = models.StageWaitingForGtidToBeCommitted
	atomic.AddInt64(&a.mysqlContext.TotalDeltaCopied, 1)
	return nil
}

func (a *Applier) ApplyEventQueries(db *gosql.DB, entry *DumpEntry) error {
	if a.stubFullApplyDelay {
		a.logger.Debugf("mysql.applier: stubFullApplyDelay start sleep")
		time.Sleep(20 * time.Second)
		a.logger.Debugf("mysql.applier: stubFullApplyDelay end sleep")
	}

	if entry.SystemVariablesStatement != "" {
		for i := range a.dbs {
			a.logger.Debugf("mysql.applier: exec sysvar query: %v", entry.SystemVariablesStatement)
			_, err := a.dbs[i].Db.ExecContext(context.Background(), entry.SystemVariablesStatement)
			if err != nil {
				a.logger.Errorf("mysql.applier: err exec sysvar query. err: %v", err)
				return err
			}
		}
	}
	if entry.SqlMode != "" {
		for i := range a.dbs {
			a.logger.Debugf("mysql.applier: exec sqlmode query: %v", entry.SqlMode)
			_, err := a.dbs[i].Db.ExecContext(context.Background(), entry.SqlMode)
			if err != nil {
				a.logger.Errorf("mysql.applier: err exec sysvar query. err: %v", err)
				return err
			}
		}
	}

	queries := []string{}
	queries = append(queries, entry.SystemVariablesStatement, entry.SqlMode, entry.DbSQL)
	queries = append(queries, entry.TbSQL...)
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err := tx.Commit(); err != nil {
			a.onError(TaskStateDead, err)
		}
		atomic.AddInt64(&a.mysqlContext.TotalRowsReplay, entry.RowsCount)
	}()
	sessionQuery := `SET @@session.foreign_key_checks = 0`
	if _, err := tx.Exec(sessionQuery); err != nil {
		return err
	}
	execQuery := func(query string) error {
		a.logger.Debugf("mysql.applier: Exec [%s]", utils.StrLim(query, 256))
		_, err := tx.Exec(query)
		if err != nil {
			if !sql.IgnoreError(err) {
				a.logger.Errorf("mysql.applier: Exec [%s] error: %v", utils.StrLim(query, 10), err)
				return err
			}
			if !sql.IgnoreExistsError(err) {
				a.logger.Warnf("mysql.applier: Ignore error: %v", err)
			}
		}
		return nil
	}

	for _, query := range queries {
		if query == "" {
			continue
		}
		err := execQuery(query)
		if err != nil {
			return err
		}
	}

	var buf bytes.Buffer
	BufSizeLimit := 1 * 1024 * 1024 // 1MB. TODO parameterize it
	BufSizeLimitDelta := 1024
	buf.Grow(BufSizeLimit + BufSizeLimitDelta)
	for i, _ := range entry.ValuesX {
		if buf.Len() == 0 {
			buf.WriteString(fmt.Sprintf(`replace into %s.%s values (`, entry.TableSchema, entry.TableName))
		} else {
			buf.WriteString(",(")
		}

		firstCol := true
		for j := range entry.ValuesX[i] {
			if firstCol {
				firstCol = false
			} else {
				buf.WriteByte(',')
			}

			colData := entry.ValuesX[i][j]
			if colData != nil {
				buf.WriteByte('\'')
				buf.WriteString(sql.EscapeValue(string(*colData)))
				buf.WriteByte('\'')
			} else {
				buf.WriteString("NULL")
			}
		}
		buf.WriteByte(')')

		needInsert := (i == len(entry.ValuesX)-1) || (buf.Len() >= BufSizeLimit)
		// last rows or sql too large

		if needInsert {
			err := execQuery(buf.String())
			buf.Reset()
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (a *Applier) Stats() (*models.TaskStatistics, error) {
	totalRowsReplay := a.mysqlContext.GetTotalRowsReplay()
	rowsEstimate := atomic.LoadInt64(&a.mysqlContext.RowsEstimate)
	totalDeltaCopied := a.mysqlContext.GetTotalDeltaCopied()
	deltaEstimate := atomic.LoadInt64(&a.mysqlContext.DeltaEstimate)

	var progressPct float64
	var backlog, eta string
	if rowsEstimate == 0 && deltaEstimate == 0 {
		progressPct = 0.0
	} else {
		progressPct = 100.0 * float64(totalDeltaCopied+totalRowsReplay) / float64(deltaEstimate+rowsEstimate)
		if a.mysqlContext.Gtid != "" {
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
		totalExpectedSeconds := elapsedRowCopySeconds * float64(rowsEstimate) / float64(totalRowsReplay)
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
		ExecMasterRowCount: totalRowsReplay,
		ExecMasterTxCount:  totalDeltaCopied,
		ReadMasterRowCount: rowsEstimate,
		ReadMasterTxCount:  deltaEstimate,
		ProgressPct:        strconv.FormatFloat(progressPct, 'f', 1, 64),
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
	if err := sql.CloseConns(a.dbs...); err != nil {
		return err
	}

	//close(a.applyBinlogTxQueue)
	//close(a.applyBinlogGroupTxQueue)
	a.logger.Printf("mysql.applier: Shutting down")
	return nil
}
