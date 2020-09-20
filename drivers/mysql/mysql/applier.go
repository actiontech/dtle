/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package mysql

import (
	gosql "database/sql"
	"fmt"
	"github.com/actiontech/dtle/drivers/mysql/common"
	"github.com/actiontech/dtle/drivers/mysql/config"
	"github.com/hashicorp/nomad/plugins/drivers"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"

	"bytes"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	gonats "github.com/nats-io/go-nats"
	gomysql "github.com/siddontang/go-mysql/mysql"

	"context"
	"os"

	"github.com/actiontech/dtle/drivers/mysql/mysql/base"
	umconf "github.com/actiontech/dtle/drivers/mysql/mysql/mysqlconfig"
	"github.com/actiontech/dtle/drivers/mysql/mysql/sql"
	"github.com/actiontech/dtle/g"

	hclog "github.com/hashicorp/go-hclog"
	not "github.com/nats-io/not.go"
)

const (
	cleanupGtidExecutedLimit = 2048
	pingInterval             = 10 * time.Second
)
const (
	TaskStateComplete int = iota
	TaskStateRestart
	TaskStateDead
)

type mapSchemaTableItems map[string](map[string](*common.ApplierTableItem))

// Applier connects and writes the the applier-server, which is the server where
// write row data and apply binlog events onto the dest table.

type Applier struct {
	logger       hclog.Logger
	subject      string
	mysqlContext *config.MySQLDriverConfig

	NatsAddr          string
	MySQLVersion      string
	MySQLServerUuid   string
	TotalRowsReplayed int64
	TotalDeltaCopied  int64

	dbs         []*sql.Conn
	db          *gosql.DB
	gtidItemMap base.GtidItemMap
	tableItems  mapSchemaTableItems

	rowCopyComplete chan struct{}
	// copyRowsQueue should not be buffered; if buffered some non-damaging but
	//  excessive work happens at the end of the iteration as new copy-jobs arrive befroe realizing the copy is complete
	copyRowsQueue       chan *common.DumpEntry
	applyDataEntryQueue chan *common.BinlogEntryContext
	// only TX can be executed should be put into this chan
	applyBinlogMtsTxQueue chan *common.BinlogEntryContext

	natsConn *gonats.Conn
	waitCh   chan *drivers.ExitResult
	wg       sync.WaitGroup

	shutdown     bool
	shutdownCh   chan struct{}
	shutdownLock sync.Mutex

	mtsManager     *MtsManager
	printTps       bool
	txLastNSeconds uint32
	nDumpEntry     int64

	stubFullApplyDelay time.Duration

	gtidSet *gomysql.MysqlGTIDSet

	storeManager *common.StoreManager
	gtidCh       chan *common.BinlogCoordinateTx

	timestampCtx       *TimestampContext
}

func NewApplier(
	ctx *common.ExecContext, cfg *config.MySQLDriverConfig, logger hclog.Logger,
	storeManager *common.StoreManager, natsAddr string, waitCh chan *drivers.ExitResult) (a *Applier, err error) {

	logger.Info("NewApplier", "job", ctx.Subject)

	a = &Applier{
		logger:          logger.Named("applier").With("job", ctx.Subject),
		subject:         ctx.Subject,
		mysqlContext:    cfg,
		NatsAddr:        natsAddr,
		tableItems:      make(mapSchemaTableItems),
		rowCopyComplete: make(chan struct{}),
		copyRowsQueue:   make(chan *common.DumpEntry, 24),
		waitCh:          waitCh,
		shutdownCh:      make(chan struct{}),
		printTps:        os.Getenv(g.ENV_PRINT_TPS) != "",
		storeManager:    storeManager,
		gtidCh:          make(chan *common.BinlogCoordinateTx, 4096),
	}

	a.timestampCtx = NewTimestampContext(a.shutdownCh, a.logger, func() bool {
		return len(a.applyDataEntryQueue) == 0 && len(a.applyBinlogMtsTxQueue) == 0
		// TODO need a more reliable method to determine queue.empty.
	})

	stubFullApplyDelayStr := os.Getenv(g.ENV_FULL_APPLY_DELAY)
	if stubFullApplyDelayStr == "" {
		a.stubFullApplyDelay = 0
	} else {
		delay, parseIntErr := strconv.ParseInt(stubFullApplyDelayStr, 10, 0)
		if parseIntErr != nil { // backward compatibility
			delay = 20
		}
		a.stubFullApplyDelay = time.Duration(delay) * time.Second
	}

	a.mtsManager = NewMtsManager(a.shutdownCh)
	go a.mtsManager.LcUpdater()
	return a, nil
}

func (a *Applier) updateGtidLoop() {
	updateGtidInterval := 15 * time.Second
	t0 := time.NewTicker(2 * time.Second)
	t := time.NewTicker(updateGtidInterval)

	var file string
	var pos  int64

	updated := false
	doUpdate := func() {
		if updated { // catch by reference
			updated = false
			a.mysqlContext.Gtid = a.gtidSet.String()
			if file != "" {
				if a.mysqlContext.BinlogFile != file {
					a.mysqlContext.BinlogFile = file
					a.publishProgress()
				}
				a.mysqlContext.BinlogPos = pos
			}
		}
	}

	needUpload := true
	doUpload := func() {
		if needUpload {
			needUpload = false
			a.logger.Debug("SaveGtidForJob", "job", a.subject, "gtid", a.mysqlContext.Gtid)
			err := a.storeManager.SaveGtidForJob(a.subject, a.mysqlContext.Gtid)
			if err != nil {
				a.onError(TaskStateDead, errors.Wrap(err, "SaveGtidForJob"))
				return
			}

			err = a.storeManager.SaveBinlogFilePosForJob(a.subject,
				a.mysqlContext.BinlogFile, int(a.mysqlContext.BinlogPos))
			if err != nil {
				a.onError(TaskStateDead, errors.Wrap(err, "SaveBinlogFilePosForJob"))
				return
			}
		}
	}
	for !a.shutdown {
		select {
		case <-a.shutdownCh:
			return
		case <-t.C: // this must be prior to gtidCh
			doUpdate()
			doUpload()
		case <-t0.C:
			doUpdate()
		case coord := <-a.gtidCh:
			updated = true
			needUpload = true
			if coord == nil {
				// coord == nil is a flag for update/upload gtid
				doUpdate()
				doUpload()
			} else {
				common.UpdateGtidSet(a.gtidSet, coord.SID, coord.GNO)
				file = coord.LogFile
				pos = coord.LogPos
			}
		}
	}
}

func (a *Applier) MtsWorker(workerIndex int) {
	keepLoop := true

	logger := a.logger.With("worker", workerIndex)
	for keepLoop {
		timer := time.NewTimer(pingInterval)
		select {
		case entryContext := <-a.applyBinlogMtsTxQueue:
			logger.Debug("a binlogEntry MTS dequeue", "gno", entryContext.Entry.Coordinates.GNO)
			if err := a.ApplyBinlogEvent(nil, workerIndex, entryContext); err != nil {
				a.onError(TaskStateDead, err) // TODO coordinate with other goroutine
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

// Run executes the complete apply logic.
func (a *Applier) Run() {
	var err error

	{
		v, err := a.storeManager.WaitKv(a.subject, "ReplChanBufferSize", a.shutdownCh)
		if err != nil {
			a.onError(TaskStateDead, errors.Wrap(err, "WaitKv ReplChanBufferSize"))
			return
		}
		i, err := strconv.Atoi(string(v))
		if err != nil {
			a.onError(TaskStateDead, errors.Wrap(err, "Atoi ReplChanBufferSize"))
			return
		}
		a.logger.Debug("got ReplChanBufferSize from consul", "i", i)
		if i > 0 {
			a.logger.Debug("use ReplChanBufferSize from consul", "i", i)
			a.mysqlContext.ReplChanBufferSize = int64(i)
		}
		a.applyDataEntryQueue = make(chan *common.BinlogEntryContext, a.mysqlContext.ReplChanBufferSize * 2)
		a.applyBinlogMtsTxQueue = make(chan *common.BinlogEntryContext, a.mysqlContext.ReplChanBufferSize * 2)
	}

	err = common.GetGtidFromConsul(a.storeManager, a.subject, a.logger, a.mysqlContext)
	if err != nil {
		a.onError(TaskStateDead, errors.Wrap(err, "GetGtidFromConsul"))
		return
	}

	a.gtidSet, err = common.DtleParseMysqlGTIDSet(a.mysqlContext.Gtid)
	if err != nil {
		a.onError(TaskStateDead, errors.Wrap(err, "DtleParseMysqlGTIDSet"))
		return
	}

	if a.printTps {
		go func() {
			for !a.shutdown {
				time.Sleep(5 * time.Second)
				n := atomic.SwapUint32(&a.txLastNSeconds, 0)
				a.logger.Info("txLastNSeconds", "n", n)
			}
		}()
	}
	//a.logger.Debug("the connectionconfi host is ",a.mysqlContext.ConnectionConfig.Host)
//	a.logger.Info("Apply binlog events to %s.%d", a.mysqlContext.ConnectionConfig.Host, a.mysqlContext.ConnectionConfig.Port)
	if err := a.initDBConnections(); err != nil {
		a.onError(TaskStateDead, err)
		return
	}

	a.gtidItemMap, err = SelectAllGtidExecuted(a.db, a.subject, a.gtidSet)
	if err != nil {
		a.onError(TaskStateDead, err)
		return
	}

	a.logger.Debug("initNatSubClient")
	if err := a.initNatSubClient(); err != nil {
		a.onError(TaskStateDead, err)
		return
	}
	a.logger.Debug("subscribeNats")
	if err := a.subscribeNats(); err != nil {
		a.onError(TaskStateDead, err)
		return
	}
	a.logger.Info("go WatchAndPutNats")
	go a.storeManager.WatchAndPutNats(a.subject, a.NatsAddr, a.shutdownCh, func(err error) {
		a.onError(TaskStateDead, errors.Wrap(err, "WatchAndPutNats"))
	})

	go a.timestampCtx.Handle()
	go a.updateGtidLoop()

	for i := 0; i < a.mysqlContext.ParallelWorkers; i++ {
		go a.MtsWorker(i)
	}

	go a.doFullCopy()
	go a.heterogeneousReplay()
}

func (a *Applier) doFullCopy() {
	a.mysqlContext.Stage = common.StageSlaveWaitingForWorkersToProcessQueue
	a.logger.Info("Operating until row copy is complete")

	var stopLoop = false
	for !stopLoop && !a.shutdown {
		t10 := time.NewTimer(10 * time.Second)

		select {
		case copyRows := <-a.copyRowsQueue:
			if nil != copyRows {
				//time.Sleep(20 * time.Second) // #348 stub
				if err := a.ApplyEventQueries(a.db, copyRows); err != nil {
					a.onError(TaskStateDead, err)
				}
			}
			if atomic.LoadInt64(&a.nDumpEntry) <= 0 {
				err := fmt.Errorf("DTLE_BUG a.nDumpEntry <= 0")
				a.logger.Error(err.Error())
				a.onError(TaskStateDead, err)
			} else {
				atomic.AddInt64(&a.nDumpEntry, -1)
				a.logger.Debug("ApplyEventQueries. after", "nDumpEntry", a.nDumpEntry)
			}
		case <-a.rowCopyComplete:
			a.logger.Info("doFullCopy: loop: rowCopyComplete")
			stopLoop = true
		case <-a.shutdownCh:
			stopLoop = true
		case <-t10.C:
			a.logger.Debug("no copyRows for 10s.")
		}
		t10.Stop()
	}
}

func (a *Applier) initNatSubClient() (err error) {
	sc, err := gonats.Connect(a.NatsAddr)
	if err != nil {
		a.logger.Error("cannot connect to nats server", "natsAddr", a.NatsAddr, "err", err)
		return err
	}
	a.logger.Debug("Connect nats server", "natsAddr", a.NatsAddr)
	a.natsConn = sc
	return nil
}

func (a *Applier) setTableItemForBinlogEntry(binlogEntry *common.BinlogEntryContext) error {
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

func (a *Applier) heterogeneousReplay() {
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
			intervals := base.GetIntervals(a.gtidSet, binlogEntry.Coordinates.SID)
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
					a.onError(TaskStateDead, err)
					return
				}
				gtidSetItem.NRow = 1
			}

			gtidSetItem.NRow += 1
			if binlogEntry.Coordinates.SeqenceNumber == 0 {
				// MySQL 5.6: non mts
				err := a.setTableItemForBinlogEntry(entryCtx)
				if err != nil {
					a.onError(TaskStateDead, err)
					return
				}
				if err := a.ApplyBinlogEvent(ctx, 0, entryCtx); err != nil {
					a.onError(TaskStateDead, err)
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
					a.onError(TaskStateDead, err)
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

// initiateStreaming begins treaming of binary log events and registers listeners for such events
func (a *Applier) subscribeNats() error {
	a.mysqlContext.MarkRowCopyStartTime()
	a.logger.Debug("nats subscribe")
	tracer := opentracing.GlobalTracer()
	_, err := a.natsConn.Subscribe(fmt.Sprintf("%s_full", a.subject), func(m *gonats.Msg) {
		a.logger.Debug("full. recv a msg.", "copyRowsQueue", len(a.copyRowsQueue))

		select {
		case <-a.rowCopyComplete: // full complete. Maybe src task restart.
			if err := a.natsConn.Publish(m.Reply, nil); err != nil {
				a.onError(TaskStateDead, err)
			}
			return
		default:
		}

		t := not.NewTraceMsg(m)
		// Extract the span context from the request message.
		sc, err := tracer.Extract(opentracing.Binary, t)
		if err != nil {
			a.logger.Debug("get data")
		}
		// Setup a span referring to the span context of the incoming NATS message.
		replySpan := tracer.StartSpan("Service Responder", ext.SpanKindRPCServer, ext.RPCServerOption(sc))
		ext.MessageBusDestination.Set(replySpan, m.Subject)
		defer replySpan.Finish()
		dumpData, err := common.DecodeDumpEntry(t.Bytes())
		if err != nil {
			a.onError(TaskStateDead, errors.Wrap(err, "DecodeDumpEntry"))
			return
		}

		timer := time.NewTimer(common.DefaultConnectWait / 2)
		atomic.AddInt64(&a.nDumpEntry, 1) // this must be increased before enqueuing
		select {
		case a.copyRowsQueue <- dumpData:
			a.logger.Debug("full. enqueue", "nDumpEntry", a.nDumpEntry)
			timer.Stop()
			a.mysqlContext.Stage = common.StageSlaveWaitingForWorkersToProcessQueue
			if err := a.natsConn.Publish(m.Reply, nil); err != nil {
				a.onError(TaskStateDead, err)
			}
			a.logger.Debug("full. after publish nats reply")
			atomic.AddInt64(&a.mysqlContext.RowsEstimate, dumpData.TotalCount)
		case <-timer.C:
			atomic.AddInt64(&a.nDumpEntry, -1)
			a.logger.Debug("full. discarding entries", "nDumpEntry", a.nDumpEntry)

			a.mysqlContext.Stage = common.StageSlaveWaitingForWorkersToProcessQueue
		}
	})
	/*if err := sub.SetPendingLimits(a.mysqlContext.MsgsLimit, a.mysqlContext.BytesLimit); err != nil {
		return err
	}*/

	_, err = a.natsConn.Subscribe(fmt.Sprintf("%s_full_complete", a.subject), func(m *gonats.Msg) {
		a.logger.Debug("recv _full_complete.")

		select {
		case <-a.rowCopyComplete: // already full complete. Maybe src task restart.
			if err := a.natsConn.Publish(m.Reply, nil); err != nil {
				a.onError(TaskStateDead, err)
			}
			return
		default:
		}

		dumpData := &DumpStatResult{}
		t := not.NewTraceMsg(m)
		// Extract the span context from the request message.
		sc, err := tracer.Extract(opentracing.Binary, t)
		if err != nil {
			a.logger.Debug("tracer.Extract error", "err", err)
		}
		// Setup a span referring to the span context of the incoming NATS message.
		replySpan := tracer.StartSpan("Service Responder", ext.SpanKindRPCServer, ext.RPCServerOption(sc))
		ext.MessageBusDestination.Set(replySpan, m.Subject)
		defer replySpan.Finish()
		if err := common.Decode(t.Bytes(), dumpData); err != nil {
			a.onError(TaskStateDead, errors.Wrap(err, "Decode"))
			return
		}
		for atomic.LoadInt64(&a.nDumpEntry) != 0 {
			a.logger.Debug("nDumpEntry is not zero, waiting", "nDumpEntry", a.nDumpEntry)
			time.Sleep(1 * time.Second)
			if a.shutdown {
				return
			}
		}
		a.logger.Info("Rows copy complete.", "TotalRowsReplayed", a.TotalRowsReplayed)

		a.logger.Info("got gtid from extractor", "gtid", dumpData.Gtid)
		a.gtidSet, err = common.DtleParseMysqlGTIDSet(dumpData.Gtid)
		if err != nil {
			a.onError(TaskStateDead, errors.Wrap(err, "DtleParseMysqlGTIDSet"))
			return
		}
		a.mysqlContext.Gtid = dumpData.Gtid
		a.mysqlContext.BinlogFile = dumpData.LogFile
		a.mysqlContext.BinlogPos = dumpData.LogPos
		a.gtidCh <- nil // coord == nil is a flag for update/upload gtid

		a.mysqlContext.Stage = common.StageSlaveWaitingForWorkersToProcessQueue

		close(a.rowCopyComplete)

		a.logger.Debug("ack _full_complete")
		if err := a.natsConn.Publish(m.Reply, nil); err != nil {
			a.onError(TaskStateDead, errors.Wrap(err, "Publish"))
			return
		}
	})
	if err != nil {
		return err
	}

	var bigEntries common.BinlogEntries
	_, err = a.natsConn.Subscribe(fmt.Sprintf("%s_incr_hete", a.subject), func(m *gonats.Msg) {
		var binlogEntries common.BinlogEntries
		t := not.NewTraceMsg(m)
		// Extract the span context from the request message.
		spanContext, err := tracer.Extract(opentracing.Binary, t)
		if err != nil {
			a.logger.Debug("get data")
		}
		// Setup a span referring to the span context of the incoming NATS message.
		replySpan := tracer.StartSpan("nast : dest to get data  ", ext.SpanKindRPCServer, ext.RPCServerOption(spanContext))
		ext.MessageBusDestination.Set(replySpan, m.Subject)
		defer replySpan.Finish()
		if err := common.Decode(t.Bytes(), &binlogEntries); err != nil {
			a.onError(TaskStateDead, err)
		}

		nEntries := len(binlogEntries.Entries)

		handled := false
		if binlogEntries.BigTx {
			if binlogEntries.TxNum == 1 {
				bigEntries = binlogEntries
			} else if bigEntries.Entries != nil {
				bigEntries.Entries[0].Events = append(bigEntries.Entries[0].Events, binlogEntries.Entries[0].Events...)
				bigEntries.TxNum = binlogEntries.TxNum
				a.logger.Debug("tx get the n package", "n", binlogEntries.TxNum)
				binlogEntries.Entries = nil
			}
			if bigEntries.TxNum == bigEntries.TxLen {
				binlogEntries = bigEntries
				bigEntries.Entries = nil
			}
		}
		for i := 0; !handled && (i < common.DefaultConnectWaitSecond / 2); i++ {
			if binlogEntries.BigTx && binlogEntries.TxNum < binlogEntries.TxLen {
				handled = true
				if err := a.natsConn.Publish(m.Reply, nil); err != nil {
					a.onError(TaskStateDead, err)
				}
				continue
			}
			binlogEntries.BigTx=false
			binlogEntries.TxNum = 0
			binlogEntries.TxLen = 0
			vacancy := cap(a.applyDataEntryQueue) - len(a.applyDataEntryQueue)
			a.logger.Debug("incr.", "nEntries", nEntries, "vacancy", vacancy)
			if vacancy < nEntries {
				a.logger.Debug("incr. wait 1s for applyDataEntryQueue")
				time.Sleep(1 * time.Second) // It will wait an second at the end, but seems no hurt.
			} else {
				a.logger.Debug("incr. applyDataEntryQueue enqueue")
				for _, binlogEntry := range binlogEntries.Entries {
					a.applyDataEntryQueue <- &common.BinlogEntryContext{
						Entry:       binlogEntry,
						SpanContext: replySpan.Context(),
						TableItems:  nil,
					}
					//a.retrievedGtidSet = ""
					// It costs quite a lot to maintain the set, and retrievedGtidSet is not
					// as necessary as executedGtidSet. So I removed it.
					// Union incoming TX gtid with current set if you want to set it.
					atomic.AddInt64(&a.mysqlContext.DeltaEstimate, 1)
				}
				a.mysqlContext.Stage = common.StageWaitingForMasterToSendEvent

				if err := a.natsConn.Publish(m.Reply, nil); err != nil {
					a.onError(TaskStateDead, err)
				}
				a.logger.Debug("incr. ack-recv.", "nEntries", nEntries)

				handled = true
			}
		}
		if !handled {
			// discard these entries
			a.logger.Debug("incr. discarding entries")
			a.mysqlContext.Stage = common.StageWaitingForMasterToSendEvent
		}
	})
	if err != nil {
		return err
	}

	return nil
}

func (a *Applier) publishProgress() {
	logger := a.logger.Named("publishProgress")
	retry := 0
	keep := true
	for keep {
		logger.Debug("Request.before", "retry", retry, "file", a.mysqlContext.BinlogFile)
		_, err := a.natsConn.Request(fmt.Sprintf("%s_progress", a.subject), []byte(a.mysqlContext.BinlogFile), 10*time.Second)
		if err == nil {
			keep = false
		} else {
			if err == gonats.ErrTimeout {
				logger.Debug("timeout", "retry", retry, "file", a.mysqlContext.BinlogFile)
				break
			} else {
				a.logger.Debug("unknown error", "retry", retry, "file", a.mysqlContext.BinlogFile, "err", err)

			}
			retry += 1
			if retry < 5 {
				time.Sleep(1 * time.Second)
			} else {
				keep = false
			}
		}
	}
}

func (a *Applier) initDBConnections() (err error) {
	applierUri := a.mysqlContext.ConnectionConfig.GetDBUri()
	if a.db, err = sql.CreateDB(applierUri); err != nil {
		return err
	}
	a.db.SetMaxOpenConns(10 + a.mysqlContext.ParallelWorkers)
	if a.dbs, err = sql.CreateConns(a.db, a.mysqlContext.ParallelWorkers); err != nil {
		a.logger.Debug("beging connetion mysql 2 create conns err")
		return err
	}

	if err := a.validateConnection(a.db); err != nil {
		return err
	}
	a.logger.Debug("beging connetion mysql 4 validate  serverid")
	if err := a.validateServerUUID(); err != nil {
		return err
	}
	a.logger.Debug("beging connetion mysql 5 validate  grants")
	if err := a.validateGrants(); err != nil {
		a.logger.Error("Unexpected error on validateGrants", "err", err)
		return err
	}
	a.logger.Debug("after validateGrants")

	if timezone, err := base.ValidateAndReadTimeZone(a.db); err != nil {
		return err
	} else {
		a.logger.Info("got timezone", "timezone", timezone)
	}

	if err := a.createTableGtidExecutedV4(); err != nil {
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

	a.logger.Info("Initiated", "mysql", a.mysqlContext.ConnectionConfig.GetAddr(), "version", a.MySQLVersion)
	return nil
}

func (a *Applier) validateServerUUID() error {
	query := `SELECT @@SERVER_UUID`
	if err := a.db.QueryRow(query).Scan(&a.MySQLServerUuid); err != nil {
		return err
	}
	return nil
}

// validateConnection issues a simple can-connect to MySQL
func (a *Applier) validateConnection(db *gosql.DB) error {
	query := `select @@version`
	if err := db.QueryRow(query).Scan(&a.MySQLVersion); err != nil {
		return err
	}
	// Match the version string (from SELECT VERSION()).
	if strings.HasPrefix(a.MySQLVersion, "5.6") {
		a.mysqlContext.ParallelWorkers = 1
	}
	a.logger.Debug("Connection validated", "on",
		hclog.Fmt("%s:%d", a.mysqlContext.ConnectionConfig.Host, a.mysqlContext.ConnectionConfig.Port))
	return nil
}

// validateGrants verifies the user by which we're executing has necessary grants
// to do its thang.
func (a *Applier) validateGrants() error {
	if a.mysqlContext.SkipPrivilegeCheck {
		a.logger.Debug("skipping priv check")
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
				g.DtleSchemaName, g.GtidExecutedTableV4)) {
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

	if foundAll {
		a.logger.Info("User has ALL privileges")
		return nil
	}
	if foundSuper {
		a.logger.Info("User has SUPER privileges")
		return nil
	}
	if foundDBAll {
		a.logger.Info("User has ALL privileges on *.*")
		return nil
	}
	a.logger.Debug("Privileges", "Super", foundSuper, "All", foundAll)
	//return fmt.Error("user has insufficient privileges for applier. Needed: SUPER|ALL on *.*")
	return nil
}

func (a *Applier) getTableItem(schema string, table string) *common.ApplierTableItem {
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

// buildDMLEventQuery creates a query to operate on the ghost table, based on an intercepted binlog
// event entry on the original table.
func (a *Applier) buildDMLEventQuery(dmlEvent common.DataEvent, workerIdx int, spanContext opentracing.SpanContext,
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
func (a *Applier) ApplyBinlogEvent(ctx context.Context, workerIdx int, binlogEntryCtx *common.BinlogEntryContext) error {
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
			a.onError(TaskStateDead, err)
		} else {
			a.mtsManager.Executed(binlogEntry)
			a.gtidCh <- &binlogEntry.Coordinates
		}
		if a.printTps {
			atomic.AddUint32(&a.txLastNSeconds, 1)
		}
		span.SetTag("after  commit sql ", time.Now().UnixNano()/1e6)

		dbApplier.DbMutex.Unlock()
	}()
	span.SetTag("begin transform binlogEvent to sql time  ", time.Now().UnixNano()/1e6)
	for i, event := range binlogEntry.Events {
		logger.Debug("binlogEntry.Events", "gno", binlogEntry.Coordinates.GNO, "event", i)
		switch event.DML {
		case common.NotDML:
			var err error
			logger.Debug("not dml", "query", event.Query)

			if event.CurrentSchema != "" {
				query := fmt.Sprintf("USE %s", umconf.EscapeName(event.CurrentSchema))
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

func (a *Applier) ApplyEventQueries(db *gosql.DB, entry *common.DumpEntry) error {
	if a.stubFullApplyDelay != 0 {
		a.logger.Debug("stubFullApplyDelay start sleep")
		time.Sleep(a.stubFullApplyDelay)
		a.logger.Debug("stubFullApplyDelay end sleep")
	}

	if entry.SystemVariablesStatement != "" {
		for i := range a.dbs {
			a.logger.Debug("exec sysvar query", "query", entry.SystemVariablesStatement)
			_, err := a.dbs[i].Db.ExecContext(context.Background(), entry.SystemVariablesStatement)
			if err != nil {
				a.logger.Error("err exec sysvar query.", "err", err)
				return err
			}
		}
	}
	if entry.SqlMode != "" {
		for i := range a.dbs {
			a.logger.Debug("exec sqlmode query", "query", entry.SqlMode)
			_, err := a.dbs[i].Db.ExecContext(context.Background(), entry.SqlMode)
			if err != nil {
				a.logger.Error("err exec sysvar query.", "err", err)
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
	nRows := int64(len(entry.ValuesX))
	defer func() {
		if err := tx.Commit(); err != nil {
			a.onError(TaskStateDead, err)
		}
		atomic.AddInt64(&a.TotalRowsReplayed, nRows)
	}()
	sessionQuery := `SET @@session.foreign_key_checks = 0`
	if _, err := tx.Exec(sessionQuery); err != nil {
		return err
	}
	execQuery := func(query string) error {
		a.logger.Debug("Exec", "query", common.StrLim(query, 256))
		_, err := tx.Exec(query)
		if err != nil {
			if !sql.IgnoreError(err) {
				a.logger.Error("Exec error", "query", common.StrLim(query, 10), "err", err)
				return err
			}
			if !sql.IgnoreExistsError(err) {
				a.logger.Warn("Ignore error", "err", err)
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
			buf.WriteString(fmt.Sprintf(`replace into %s.%s values (`,
				umconf.EscapeName(entry.TableSchema), umconf.EscapeName(entry.TableName)))
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

func (a *Applier) Stats() (*common.TaskStatistics, error) {
	a.logger.Debug("Stats")
	totalRowsReplay := a.TotalRowsReplayed
	rowsEstimate := atomic.LoadInt64(&a.mysqlContext.RowsEstimate)
	totalDeltaCopied := a.TotalDeltaCopied
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
		a.mysqlContext.Stage = common.StageSlaveHasReadAllRelayLog
	} else if progressPct >= 1.0 {
		elapsedRowCopySeconds := a.mysqlContext.ElapsedRowCopyTime().Seconds()
		totalExpectedSeconds := elapsedRowCopySeconds * float64(rowsEstimate) / float64(totalRowsReplay)
		if a.mysqlContext.Gtid != "" {
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

	taskResUsage :=  common.TaskStatistics{
		ExecMasterRowCount: totalRowsReplay,
		ExecMasterTxCount:  totalDeltaCopied,
		ReadMasterRowCount: rowsEstimate,
		ReadMasterTxCount:  deltaEstimate,
		ProgressPct:        strconv.FormatFloat(progressPct, 'f', 1, 64),
		ETA:                eta,
		Backlog:            backlog,
		Stage:              a.mysqlContext.Stage,
		CurrentCoordinates: &common.CurrentCoordinates{
			File:               a.mysqlContext.BinlogFile,
			Position:           a.mysqlContext.BinlogPos,
			GtidSet:            a.mysqlContext.Gtid, // TODO
			RelayMasterLogFile: "",
			ReadMasterLogPos:   0,
			RetrievedGtidSet:   "",
		},
		BufferStat:  common.BufferStat{
			ApplierTxQueueSize:      len(a.applyDataEntryQueue),
		},
		Timestamp: time.Now().UTC().UnixNano(),
		DelayCount: &common.DelayCount{
			Num:  0,
			Time: a.timestampCtx.GetDelay(),
		},
	}
	if a.natsConn != nil {
		taskResUsage.MsgStat = a.natsConn.Statistics
	}

	return &taskResUsage, nil
}

func (a *Applier) onError(state int, err error) {
	a.logger.Error("onError", "err", err)
	if a.shutdown {
		return
	}

	bs := []byte(err.Error())

	switch state {
	case TaskStateComplete:
		a.logger.Info("Done migrating")
	case TaskStateRestart:
		if a.natsConn != nil {
			if err := a.natsConn.Publish(fmt.Sprintf("%s_restart", a.subject), bs); err != nil {
				a.logger.Error("Trigger restart extractor", "err", err)
			}
		}
	default:
		if a.natsConn != nil {
			if err := a.natsConn.Publish(fmt.Sprintf("%s_error", a.subject), bs); err != nil {
				a.logger.Error("Trigger extractor shutdown", "err", err)
			}
		}
	}

	a.waitCh <- &drivers.ExitResult{
		ExitCode:  state,
		Signal:    0,
		OOMKilled: false,
		Err:       err,
	}
	a.Shutdown()
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
	a.logger.Info("Shutting down")
	return nil
}
