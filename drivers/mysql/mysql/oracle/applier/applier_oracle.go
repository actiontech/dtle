/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package applier

import (
	gosql "database/sql"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/actiontech/dtle/drivers/mysql/common"
	"github.com/hashicorp/nomad/plugins/drivers"
	"github.com/pkg/errors"

	gonats "github.com/nats-io/go-nats"
	gomysql "github.com/go-mysql-org/go-mysql/mysql"

	"context"

	"github.com/actiontech/dtle/drivers/mysql/mysql/base"
	umconf "github.com/actiontech/dtle/drivers/mysql/mysql/mysqlconfig"
	"github.com/actiontech/dtle/drivers/mysql/mysql/sql"
	"github.com/actiontech/dtle/g"

	hclog "github.com/hashicorp/go-hclog"
	"github.com/hashicorp/nomad/drivers/shared/eventer"
)

// ApplierOracle connects and writes the the ApplierOracle-server, which is the server where
// write row data and apply binlog events onto the dest table.

type ApplierOracle struct {
	logger       g.LoggerType
	subject      string
	mysqlContext *common.MySQLDriverConfig

	NatsAddr            string
	MySQLVersion        string
	lowerCaseTableNames umconf.LowerCaseTableNamesValue
	TotalRowsReplayed   int64

	dbs []*sql.Conn
	db  *gosql.DB

	rowCopyComplete chan struct{}
	fullBytesQueue  chan []byte
	dumpEntryQueue  chan *common.DumpEntry
	ai              *ApplierOracleIncr

	natsConn *gonats.Conn
	waitCh   chan *drivers.ExitResult
	// we need to close all data channel while pausing task runner. and these data channel will be recreate when restart the runner.
	// to avoid writing closed channel, we need to wait for all goroutines that deal with data channels finishing. wg is used for the waiting.
	wg sync.WaitGroup

	shutdown     bool
	shutdownCh   chan struct{}
	shutdownLock sync.Mutex
	ctx          context.Context
	cancelFunc   context.CancelFunc

	nDumpEntry int64

	stubFullApplyDelay time.Duration

	gtidSet     *gomysql.MysqlGTIDSet
	gtidSetLock *sync.RWMutex

	storeManager *common.StoreManager
	gtidCh       chan *common.BinlogCoordinateTx

	stage      string
	memory1    *int64
	memory2    *int64
	event      *eventer.Eventer
	taskConfig *drivers.TaskConfig

	targetGtid gomysql.GTIDSet
}

func (a *ApplierOracle) Finish1() error {
	return nil
}

func NewApplierOracle(
	execCtx *common.ExecContext, cfg *common.MySQLDriverConfig, logger g.LoggerType,
	storeManager *common.StoreManager, natsAddr string, waitCh chan *drivers.ExitResult, event *eventer.Eventer, taskConfig *drivers.TaskConfig) (a *ApplierOracle, err error) {

	logger.Info("NewApplierOracle", "job", execCtx.Subject)

	a = &ApplierOracle{
		logger:          logger.Named("ApplierOracle").With("job", execCtx.Subject),
		subject:         execCtx.Subject,
		mysqlContext:    cfg,
		NatsAddr:        natsAddr,
		rowCopyComplete: make(chan struct{}),
		fullBytesQueue:  make(chan []byte, 16),
		dumpEntryQueue:  make(chan *common.DumpEntry, 8),
		waitCh:          waitCh,
		gtidSetLock:     &sync.RWMutex{},
		shutdownCh:      make(chan struct{}),
		storeManager:    storeManager,
		gtidCh:          make(chan *common.BinlogCoordinateTx, 4096),
		memory1:         new(int64),
		memory2:         new(int64),
		event:           event,
		taskConfig:      taskConfig,
	}
	a.ctx, a.cancelFunc = context.WithCancel(context.TODO())

	return a, nil
}

// Run executes the complete apply logic.
func (a *ApplierOracle) Run() {
	var err error

	a.checkJobFinish()

	a.logger.Debug("initNatSubClient")
	if err := a.initNatSubClient(); err != nil {
		a.onError(common.TaskStateDead, err)
		return
	}
	a.logger.Debug("subscribeNats")
	if err := a.subscribeNats(); err != nil {
		a.onError(common.TaskStateDead, err)
		return
	}
	err = a.storeManager.DstPutNats(a.subject, a.NatsAddr, a.shutdownCh, func(err error) {
		a.onError(common.TaskStateDead, errors.Wrap(err, "DstPutNats"))
	})
	if err != nil {
		a.onError(common.TaskStateDead, errors.Wrap(err, "DstPutNats"))
		return
	}
	//a.logger.Debug("the connectionconfi host is ",a.mysqlContext.ConnectionConfig.Host)
	//	a.logger.Info("Apply binlog events to %s.%d", a.mysqlContext.ConnectionConfig.Host, a.mysqlContext.ConnectionConfig.Port)
	if err := a.initDBConnections(); err != nil {
		a.onError(common.TaskStateDead, err)
		return
	}

	a.ai, err = NewApplierOracleIncr(a.ctx, a.subject, a.mysqlContext, a.logger, a.gtidSet, a.memory2,
		a.db, a.dbs, a.shutdownCh, a.gtidSetLock)
	if err != nil {
		a.onError(common.TaskStateDead, errors.Wrap(err, "NewApplierOracleIncr"))
		return
	}
	a.ai.OnError = a.onError

	a.ai.EntryExecutedHook = func(entry *common.BinlogEntry) {
		err = a.storeManager.SaveOracleSCNPos(a.subject, entry.Coordinates.LogPos, entry.Coordinates.LastCommitted)
		if err != nil {
			a.onError(common.TaskStateDead, errors.Wrap(err, "SaveOracleSCNPos"))
			return
		}
	}

	go func() {
		err := a.ai.Run()
		if err != nil {
			a.onError(common.TaskStateDead, err)
		}
	}()
}

func (a *ApplierOracle) initNatSubClient() (err error) {
	sc, err := gonats.Connect(a.NatsAddr)
	if err != nil {
		a.logger.Error("cannot connect to nats server", "natsAddr", a.NatsAddr, "err", err)
		return err
	}
	a.logger.Debug("Connect nats server", "natsAddr", a.NatsAddr)
	a.natsConn = sc
	return nil
}

// initiateStreaming begins treaming of binary log events and registers listeners for such events
func (a *ApplierOracle) subscribeNats() (err error) {
	a.mysqlContext.MarkRowCopyStartTime()
	a.logger.Debug("nats subscribe")

	////fullNMM := common.NewNatsMsgMerger(a.logger.With("nmm", "full"))
	//_, err = a.natsConn.Subscribe(fmt.Sprintf("%s_full", a.subject), func(m *gonats.Msg) {
	//	a.wg.Add(1)
	//	defer a.wg.Done()
	//	if err := a.natsConn.Publish(m.Reply, nil); err != nil {
	//		a.onError(common.TaskStateDead, err)
	//	}
	//	a.logger.Debug("full. after publish nats reply")
	//})
	//
	//_, err = a.natsConn.Subscribe(fmt.Sprintf("%s_full_complete", a.subject), func(m *gonats.Msg) {
	//	a.logger.Debug("recv _full_complete.")
	//
	//	if err := a.natsConn.Publish(m.Reply, nil); err != nil {
	//		a.onError(common.TaskStateDead, errors.Wrap(err, "Publish"))
	//		return
	//	}
	a.logger.Debug("ack _full_complete END")
	//})
	//if err != nil {
	//	return err
	//}

	incrNMM := common.NewNatsMsgMerger(a.logger.With("nmm", "incr"))
	_, err = a.natsConn.Subscribe(fmt.Sprintf("%s_incr_hete", a.subject), func(m *gonats.Msg) {
		a.logger.Debug("incr. recv a msg.")

		segmentFinished, err := incrNMM.Handle(m.Data)
		if err != nil {
			a.onError(common.TaskStateDead, errors.Wrap(err, "incrNMM.Handle"))
			return
		}

		if !segmentFinished {
			if err := a.natsConn.Publish(m.Reply, nil); err != nil {
				a.onError(common.TaskStateDead, err)
				return
			}
			a.logger.Debug("incr. after publish nats reply.")
		} else {
			bs := incrNMM.GetBytes()
			select {
			case <-a.shutdownCh:
				return
			case a.ai.incrBytesQueue <- bs:
				atomic.AddInt64(a.memory2, int64(len(bs)))
				incrNMM.Reset()

				a.logger.Debug("incr. incrBytesQueue enqueued", "vacancy", cap(a.ai.incrBytesQueue)-len(a.ai.incrBytesQueue))

				if err := a.natsConn.Publish(m.Reply, nil); err != nil {
					a.onError(common.TaskStateDead, err)
					return
				}
				a.logger.Debug("incr. after publish nats reply.")

				a.mysqlContext.Stage = common.StageWaitingForMasterToSendEvent
			}
		}
	})
	if err != nil {
		return err
	}

	return nil
}

func (a *ApplierOracle) InitDB() (err error) {
	ApplierOracleUri := a.mysqlContext.ConnectionConfig.GetDBUri()
	if a.db, err = sql.CreateDB(ApplierOracleUri); err != nil {
		return err
	}
	return nil
}

func (a *ApplierOracle) initDBConnections() (err error) {
	if err := a.InitDB(); nil != err {
		return err
	}
	a.db.SetMaxOpenConns(10 + a.mysqlContext.ParallelWorkers)
	a.logger.Debug("CreateConns", "ParallelWorkers", a.mysqlContext.ParallelWorkers)
	if a.dbs, err = sql.CreateConns(a.db, a.mysqlContext.ParallelWorkers); err != nil {
		a.logger.Debug("beging connetion mysql 2 create conns err")
		return err
	}

	someSysVars := base.GetSomeSysVars(a.db, a.logger)
	if someSysVars.Err != nil {
		return someSysVars.Err
	}
	a.logger.Debug("Connection validated", "on",
		hclog.Fmt("%s:%d", a.mysqlContext.ConnectionConfig.Host, a.mysqlContext.ConnectionConfig.Port))

	a.MySQLVersion = someSysVars.Version
	a.lowerCaseTableNames = someSysVars.LowerCaseTableNames

	if strings.HasPrefix(a.MySQLVersion, "5.6") {
		a.mysqlContext.ParallelWorkers = 1
	}

	a.logger.Debug("beging connetion mysql 5 validate  grants")
	if err := a.ValidateGrants(); err != nil {
		a.logger.Error("Unexpected error on ValidateGrants", "err", err)
		return err
	}
	a.logger.Debug("after ValidateGrants")

	a.logger.Info("Initiated", "mysql", a.mysqlContext.ConnectionConfig.GetAddr(), "version", a.MySQLVersion)

	return nil
}

// ValidateGrants verifies the user by which we're executing has necessary grants
// to do its thang.
func (a *ApplierOracle) ValidateGrants() error {
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

	if a.mysqlContext.ExpandSyntaxSupport {
		if _, err := a.db.ExecContext(a.ctx, `use mysql`); err != nil {
			msg := fmt.Sprintf(`"mysql" schema is expected to be access when ExpandSyntaxSupport=true`)
			a.logger.Info(msg, "error", err)
			return fmt.Errorf("%v. error: %v", msg, err)
		}
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
	return fmt.Errorf("user has insufficient privileges for ApplierOracle. Needed:ALTER, CREATE, DROP, INDEX, REFERENCES, INSERT, DELETE, UPDATE, SELECT, TRIGGER ON *.*")
}

func (a *ApplierOracle) Stats() (*common.TaskStatistics, error) {
	a.logger.Debug("Stats")
	var totalDeltaCopied int64
	var lenApplierOracleMsgQueue int
	var capApplierOracleMsgQueue int
	var lenApplierOracleTxQueue int
	var capApplierOracleTxQueue int
	var delay int64
	if a.ai != nil {
		totalDeltaCopied = a.ai.TotalDeltaCopied
		lenApplierOracleMsgQueue = len(a.ai.incrBytesQueue)
		capApplierOracleMsgQueue = cap(a.ai.incrBytesQueue)
		lenApplierOracleTxQueue = len(a.ai.binlogEntryQueue)
		capApplierOracleTxQueue = cap(a.ai.binlogEntryQueue)
		//delay = a.ai.timestampCtx.GetDelay()
	}
	totalRowsReplay := a.TotalRowsReplayed
	rowsEstimate := atomic.LoadInt64(&a.mysqlContext.RowsEstimate)
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
			backlog = fmt.Sprintf("%d/%d", lenApplierOracleMsgQueue+lenApplierOracleTxQueue,
				capApplierOracleMsgQueue+capApplierOracleTxQueue)
		} else {
			backlog = fmt.Sprintf("%d/%d", len(a.fullBytesQueue), cap(a.fullBytesQueue))
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

	var txCount uint32
	if a.ai != nil {
		txCount = a.ai.appliedTxCount
	}
	taskResUsage := common.TaskStatistics{
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
		BufferStat: common.BufferStat{
			//ApplierOracleMsgQueueSize: lenApplierOracleMsgQueue,
			//ApplierOracleTxQueueSize:  lenApplierOracleTxQueue,
		},
		Timestamp: time.Now().UTC().UnixNano(),
		DelayCount: &common.DelayCount{
			Num:  0,
			Time: delay,
		},
		MemoryStat: common.MemoryStat{
			Full: *a.memory1,
			Incr: *a.memory2,
		},
		HandledTxCount: common.TxCount{
			AppliedTxCount: &txCount,
		},
	}
	if a.natsConn != nil {
		taskResUsage.MsgStat = a.natsConn.Statistics
	}

	return &taskResUsage, nil
}

func (a *ApplierOracle) onError(state int, err error) {
	a.logger.Error("onError", "err", err)
	if a.shutdown {
		return
	}

	switch state {
	case common.TaskStateComplete:
		a.logger.Info("Done migrating")
	case common.TaskStateRestart, common.TaskStateDead:
		msg := &common.ControlMsg{
			Msg:  err.Error(),
			Type: common.ControlMsgError,
		}

		bs, err1 := msg.Marshal(nil)
		if err1 != nil {
			bs = nil // send zero bytes
			a.logger.Error("onError. Marshal", "err", err1)
		}

		if a.natsConn != nil {
			if err := a.natsConn.Publish(fmt.Sprintf("%s_control2", a.subject), bs); err != nil {
				a.logger.Error("when sending control2 msg", "err", err, "state", state, "type", msg.Type)
			}
		}
	}

	a.logger.Debug("onError. nats published")
	// Do not send ExitResult in Shutdown().
	// pause API will call Shutdown and the task should not exit.
	a.waitCh <- &drivers.ExitResult{
		ExitCode:  state,
		Signal:    0,
		OOMKilled: false,
		Err:       err,
	}
	_ = a.Shutdown()
}

func (a *ApplierOracle) Shutdown() error {
	a.logger.Info("Shutting down")

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

	if a.ai != nil {
		a.ai.wg.Wait()
	}
	a.logger.Debug("Shutdown. a.ai.wg.Wait. after")
	a.wg.Wait()
	a.logger.Debug("Shutdown. a.wg.Wait. after")

	a.cancelFunc()
	_ = sql.CloseDB(a.db)
	a.logger.Debug("Shutdown. CloseDB. after")
	_ = sql.CloseConns(a.dbs...)
	a.logger.Debug("Shutdown. CloseConns. after")

	a.logger.Info("Shutdown")
	return nil
}

func (a *ApplierOracle) watchTargetGtid() {

	target, err := a.storeManager.WatchTargetGtid(a.subject, a.shutdownCh)
	if err != nil {
		a.onError(common.TaskStateDead, err)
	}
	a.logger.Info("got target GTIDSet", "gs", target)

	gs, err := gomysql.ParseMysqlGTIDSet(target)
	if err != nil {
		a.onError(common.TaskStateDead, errors.Wrap(err, "CommandTypeJobFinish. ParseMysqlGTIDSet"))
	}
	a.targetGtid = gs
	a.gtidCh <- nil
}

func (a *ApplierOracle) checkJobFinish() {
	jobStatus, err := a.storeManager.GetJobStatus(a.subject)
	if err != nil {
		a.onError(common.TaskStateDead, err)
	}
	if jobStatus == common.TargetGtidFinished {
		a.logger.Info("job finish. shutting down")
		_ = a.Shutdown()
	}
}

func (a *ApplierOracle) enableForeignKeyChecks() error {
	query := "set @@session.foreign_key_checks = 1"
	_, err := a.db.ExecContext(a.ctx, query)
	if err != nil {
		return err
	}
	for _, conn := range a.dbs {
		_, err = conn.Db.ExecContext(a.ctx, query)
		if err != nil {
			return err
		}
	}
	return nil
}
