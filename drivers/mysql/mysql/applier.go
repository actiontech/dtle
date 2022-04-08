/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package mysql

import (
	"bytes"
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
	uuid "github.com/satori/go.uuid"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	gonats "github.com/nats-io/go-nats"

	"context"
	"os"

	"github.com/actiontech/dtle/drivers/mysql/mysql/base"
	umconf "github.com/actiontech/dtle/drivers/mysql/mysql/mysqlconfig"
	"github.com/actiontech/dtle/drivers/mysql/mysql/sql"
	"github.com/actiontech/dtle/g"

	hclog "github.com/hashicorp/go-hclog"
	"github.com/hashicorp/nomad/drivers/shared/eventer"
)

const (
	cleanupGtidExecutedLimit = 2048
	pingInterval             = 10 * time.Second
	JobIncrCopy              = "job_stage_incr"
	JobFullCopy              = "job_stage_full"
)

// Applier connects and writes the the applier-server, which is the server where
// write row data and apply binlog events onto the dest table.

type Applier struct {
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
	ai              *ApplierIncr

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
	gtidCh       chan common.CoordinatesI

	stage      string
	memory1    *int64
	memory2    *int64
	event      *eventer.Eventer
	taskConfig *drivers.TaskConfig

	targetGtid gomysql.GTIDSet
}

func (a *Applier) Finish1() error {
	return nil
}

func NewApplier(
	execCtx *common.ExecContext, cfg *common.MySQLDriverConfig, logger g.LoggerType,
	storeManager *common.StoreManager, natsAddr string, waitCh chan *drivers.ExitResult, event *eventer.Eventer, taskConfig *drivers.TaskConfig, ctx context.Context) (a *Applier, err error) {

	logger.Info("NewApplier", "job", execCtx.Subject)

	a = &Applier{
		ctx:             ctx,
		logger:          logger.Named("applier").With("job", execCtx.Subject),
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
		gtidCh:          make(chan common.CoordinatesI, 4096),
		memory1:         new(int64),
		memory2:         new(int64),
		event:           event,
		taskConfig:      taskConfig,
	}

	a.ctx, a.cancelFunc = context.WithCancel(context.TODO())

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

	return a, nil
}

func (a *Applier) updateGtidLoop() {
	a.wg.Add(1)
	defer a.wg.Done()
	updateGtidInterval := 15 * time.Second
	t0 := time.NewTicker(2 * time.Second)
	t := time.NewTicker(updateGtidInterval)

	var file string
	var pos int64

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
				a.onError(common.TaskStateDead, errors.Wrap(err, "SaveGtidForJob"))
				return
			}

			err = a.storeManager.SaveBinlogFilePosForJob(a.subject,
				a.mysqlContext.BinlogFile, int(a.mysqlContext.BinlogPos))
			if err != nil {
				a.onError(common.TaskStateDead, errors.Wrap(err, "SaveBinlogFilePosForJob"))
				return
			}
		}
	}

	testTargetGtid := func() {
		if a.gtidSet.Contain(a.targetGtid) {
			a.logger.Info("meet target gtid , update job status", "gtidSet", a.targetGtid.String())
			jobInfo, err := a.storeManager.GetJobInfo(a.subject)
			if err != nil {
				a.onError(common.TaskStateDead, errors.Wrap(err, "GetJobInfo"))
			}
			jobInfo.JobStatus = common.TargetGtidFinished
			err = a.storeManager.SaveJobInfo(*jobInfo)
			if err != nil {
				a.onError(common.TaskStateDead, errors.Wrap(err, "SaveJobInfo"))
			}
			_ = a.Shutdown()
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
				if a.targetGtid != nil {
					a.gtidSetLock.RLock()
					testTargetGtid()
					a.gtidSetLock.RUnlock()
				}
			} else {
				a.gtidSetLock.Lock()
				common.UpdateGtidSet(a.gtidSet, coord.GetSid().(uuid.UUID), coord.GetGNO())
				if a.targetGtid != nil {
					testTargetGtid()
				}
				a.gtidSetLock.Unlock()
				file = coord.GetLogFile()
				pos = coord.GetLogPos()
			}
		}
	}
}

// Run executes the complete apply logic.
func (a *Applier) Run() {
	var err error

	a.checkJobFinish()
	go a.watchTargetGtid()

	err = common.GetGtidFromConsul(a.storeManager, a.subject, a.logger, a.mysqlContext)
	if err != nil {
		a.onError(common.TaskStateDead, errors.Wrap(err, "GetGtidFromConsul"))
		return
	}

	a.gtidSet, err = common.DtleParseMysqlGTIDSet(a.mysqlContext.Gtid)
	if err != nil {
		a.onError(common.TaskStateDead, errors.Wrap(err, "DtleParseMysqlGTIDSet"))
		return
	}

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

	a.ai, err = NewApplierIncr(a.ctx, a.subject, a.mysqlContext, a.logger, a.gtidSet, a.memory2,
		a.db, a.dbs, a.shutdownCh, a.gtidSetLock)
	if err != nil {
		a.onError(common.TaskStateDead, errors.Wrap(err, "NewApplierIncr"))
		return
	}
	a.ai.EntryExecutedHook = func(entry *common.DataEntry) {
		err = a.storeManager.SaveOracleSCNPos(a.subject, entry.Coordinates.GetLogPos(), entry.Coordinates.GetLastCommit())
		if err != nil {
			a.onError(common.TaskStateDead, errors.Wrap(err, "SaveOracleSCNPos"))
			return
		}

		if entry.Final {
			a.gtidCh <- entry.Coordinates
		}
		if entry.IsPartOfBigTx() {
			bs, err := (&common.BigTxAck{
				GNO:   entry.Coordinates.GetGNO(),
				Index: entry.Index,
			}).Marshal(nil)
			if err != nil {
				a.onError(common.TaskStateDead, errors.Wrap(err, "bigtx_ack. Marshal"))
			}
			_, err = a.natsConn.Request(fmt.Sprintf("%s_bigtx_ack", a.subject),
				bs, 1*time.Minute)
			if err != nil {
				a.onError(common.TaskStateDead, errors.Wrap(err, "bigtx_ack. Request"))
			}
		}
	}
	a.ai.OnError = a.onError

	go a.updateGtidLoop()

	if a.stage != JobFullCopy {
		a.stage = JobFullCopy
		a.sendEvent(JobFullCopy)
	}

	go a.doFullCopy()
	go func() {
		err := a.ai.Run()
		if err != nil {
			a.onError(common.TaskStateDead, err)
		}
	}()
}

func (a *Applier) doFullCopy() {
	a.wg.Add(1)
	defer a.wg.Done()

	a.mysqlContext.Stage = common.StageSlaveWaitingForWorkersToProcessQueue
	a.logger.Info("Operating until row copy is complete")

	a.wg.Add(1)
	go func() {
		var err error
		defer func() {
			a.wg.Done()
			if err != nil {
				a.onError(common.TaskStateDead, err)
			}
		}()
		for {
			select {
			case <-a.shutdownCh:
				return
			case copyRows := <-a.dumpEntryQueue:
				//time.Sleep(20 * time.Second) // #348 stub
				if err = a.ApplyEventQueries(a.db, copyRows); err != nil {
					return
				}
				atomic.AddInt64(a.memory1, -int64(copyRows.Size()))
				if atomic.LoadInt64(&a.nDumpEntry) <= 0 {
					err = fmt.Errorf("DTLE_BUG a.nDumpEntry <= 0")
					a.logger.Error(err.Error())
					return
				} else {
					atomic.AddInt64(&a.nDumpEntry, -1)
					a.logger.Debug("ApplyEventQueries. after", "nDumpEntry", a.nDumpEntry)
				}
			}
		}
	}()

	var stopLoop = false
	t10 := time.NewTicker(10 * time.Second)
	defer t10.Stop()
	hasEntry := false
	for !stopLoop && !a.shutdown {
		select {
		case <-a.shutdownCh:
			stopLoop = true
		case bs := <-a.fullBytesQueue:
			hasEntry = true
			atomic.AddInt64(a.memory1, -int64(len(bs)))

			copyRows := &common.DumpEntry{}
			err := common.Decode(bs, copyRows) // TODO decode once for discarded-resent msg
			if err != nil {
				a.onError(common.TaskStateDead, errors.Wrap(err, "DecodeDumpEntry"))
				return
			}
			select {
			case <-a.shutdownCh:
				stopLoop = true
			case a.dumpEntryQueue <- copyRows:
				atomic.AddInt64(a.memory1, int64(copyRows.Size()))
				atomic.AddInt64(&a.mysqlContext.RowsEstimate, copyRows.TotalCount)
			}
		case <-a.rowCopyComplete:
			a.logger.Info("doFullCopy: loop: rowCopyComplete")
			stopLoop = true
		case <-t10.C:
			if !hasEntry {
				a.logger.Debug("no copyRows for 10s.")
			}
			hasEntry = false
		}
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

func (a *Applier) sendEvent(status string) {
	err := a.event.EmitEvent(&drivers.TaskEvent{
		TaskID:      a.taskConfig.ID,
		TaskName:    a.taskConfig.Name,
		AllocID:     a.taskConfig.AllocID,
		Timestamp:   time.Now(),
		Message:     status,
		Annotations: nil,
		Err:         nil,
	})
	if err != nil {
		a.logger.Error("error at sending task event", "err", err, "status", status)
	}
}

// initiateStreaming begins treaming of binary log events and registers listeners for such events
func (a *Applier) subscribeNats() (err error) {
	a.mysqlContext.MarkRowCopyStartTime()
	a.logger.Debug("nats subscribe")

	fullNMM := common.NewNatsMsgMerger(a.logger.With("nmm", "full"))
	_, err = a.natsConn.Subscribe(fmt.Sprintf("%s_full", a.subject), func(m *gonats.Msg) {
		a.wg.Add(1)
		defer a.wg.Done()

		a.logger.Debug("full. recv a msg.", "len", len(m.Data), "fullBytesQueue", len(a.fullBytesQueue))

		select {
		case <-a.rowCopyComplete: // full complete. Maybe src task restart.
			if err := a.natsConn.Publish(m.Reply, nil); err != nil {
				a.onError(common.TaskStateDead, err)
			}
			a.logger.Debug("full. after publish nats reply")
			return
		default:
		}

		segmentFinished, err := fullNMM.Handle(m.Data)
		if err != nil {
			a.onError(common.TaskStateDead, errors.Wrap(err, "fullNMM.Handle"))
			return
		}

		if !segmentFinished {
			if err := a.natsConn.Publish(m.Reply, nil); err != nil {
				a.onError(common.TaskStateDead, err)
			}
			a.logger.Debug("full. after publish nats reply")
		} else {
			atomic.AddInt64(&a.nDumpEntry, 1) // this must be increased before enqueuing
			bs := fullNMM.GetBytes()
			select {
			case <-a.shutdownCh:
				return
			case a.fullBytesQueue <- bs:
				atomic.AddInt64(a.memory1, int64(len(bs)))
				a.logger.Debug("full. enqueue", "nDumpEntry", a.nDumpEntry)
				a.mysqlContext.Stage = common.StageSlaveWaitingForWorkersToProcessQueue
				fullNMM.Reset()

				vacancy := cap(a.fullBytesQueue) - len(a.fullBytesQueue)
				err := a.natsConn.Publish(m.Reply, nil)
				if err != nil {
					a.onError(common.TaskStateDead, err)
					return
				}
				a.logger.Debug("full. after publish nats reply", "vacancy", vacancy)
			}
		}
	})

	_, err = a.natsConn.Subscribe(fmt.Sprintf("%s_full_complete", a.subject), func(m *gonats.Msg) {
		a.logger.Debug("recv _full_complete.")

		dumpData := &common.DumpStatResult{}
		if err := common.Decode(m.Data, dumpData); err != nil {
			a.onError(common.TaskStateDead, errors.Wrap(err, "Decode"))
			return
		}

		select {
		case <-a.rowCopyComplete: // already full complete. Maybe src task restart.
			if err := a.natsConn.Publish(m.Reply, nil); err != nil {
				a.onError(common.TaskStateDead, err)
			}

			return
		default:
		}

		for atomic.LoadInt64(&a.nDumpEntry) != 0 {
			a.logger.Debug("nDumpEntry is not zero, waiting", "nDumpEntry", a.nDumpEntry)
			time.Sleep(1 * time.Second)
			if a.shutdown {
				return
			}
		}
		a.logger.Info("Rows copy complete.", "TotalRowsReplayed", a.TotalRowsReplayed)

		if a.mysqlContext.ForeignKeyChecks {
			err = a.enableForeignKeyChecks()
			if err != nil {
				a.onError(common.TaskStateDead, errors.Wrap(err, "enableForeignKeyChecks"))
				return
			}
		} else {
			a.logger.Warn("ParallelWorkers > 1 and UseMySQLDependency = false. disabling MySQL session.foreign_key_checks")
		}

		a.logger.Info("got gtid from extractor", "gtid", dumpData.Coord.GetTxSet())
		// Do not re-assign a.gtidSet (#538). Update it.
		gs0, err := gomysql.ParseMysqlGTIDSet(dumpData.Coord.GetTxSet())
		if err != nil {
			a.onError(common.TaskStateDead, errors.Wrap(err, "ParseMysqlGTIDSet"))
			return
		}
		gs := gs0.(*gomysql.MysqlGTIDSet)
		for _, uuidSet := range gs.Sets {
			a.gtidSet.AddSet(uuidSet)
		}
		a.mysqlContext.Gtid = dumpData.Coord.GetTxSet()
		a.mysqlContext.BinlogFile = dumpData.Coord.GetLogFile()
		a.mysqlContext.BinlogPos = dumpData.Coord.GetLogPos()
		a.gtidCh <- nil // coord == nil is a flag for update/upload gtid

		a.mysqlContext.Stage = common.StageSlaveWaitingForWorkersToProcessQueue
		if a.stage != JobIncrCopy {
			a.stage = JobIncrCopy
			a.sendEvent(JobIncrCopy)
		}
		close(a.rowCopyComplete)

		a.logger.Debug("ack _full_complete")
		if err := a.natsConn.Publish(m.Reply, nil); err != nil {
			a.onError(common.TaskStateDead, errors.Wrap(err, "Publish"))
			return
		}
	})
	if err != nil {
		return err
	}

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

func (a *Applier) InitDB() (err error) {
	applierUri := a.mysqlContext.ConnectionConfig.GetDBUri()
	if a.db, err = sql.CreateDB(applierUri); err != nil {
		return err
	}
	return nil
}

func (a *Applier) initDBConnections() (err error) {
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

// for compatibility
func (a *Applier) ValidateConnection() error {
	r := base.GetSomeSysVars(a.db, a.logger)
	if r.Err != nil {
		return r.Err
	}
	return nil
}

// ValidateGrants verifies the user by which we're executing has necessary grants
// to do its thang.
func (a *Applier) ValidateGrants() error {
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
	return fmt.Errorf("user has insufficient privileges for applier. Needed:ALTER, CREATE, DROP, INDEX, REFERENCES, INSERT, DELETE, UPDATE, SELECT, TRIGGER ON *.*")
}

func (a *Applier) ApplyEventQueries(db *gosql.DB, entry *common.DumpEntry) (err error) {
	a.logger.Debug("ApplyEventQueries", "schema", entry.TableSchema, "table", entry.TableName,
		"rows", len(entry.ValuesX))

	if a.stubFullApplyDelay != 0 {
		a.logger.Debug("stubFullApplyDelay start sleep")
		time.Sleep(a.stubFullApplyDelay)
		a.logger.Debug("stubFullApplyDelay end sleep")
	}

	if entry.SystemVariablesStatement != "" {
		for i := range a.dbs {
			a.logger.Debug("exec sysvar query", "query", entry.SystemVariablesStatement)
			_, err := a.dbs[i].Db.ExecContext(a.ctx, entry.SystemVariablesStatement)
			if err != nil {
				a.logger.Error("err exec sysvar query.", "err", err)
				return err
			}
		}
	}
	if entry.SqlMode != "" {
		for i := range a.dbs {
			a.logger.Debug("exec sqlmode query", "query", entry.SqlMode)
			_, err := a.dbs[i].Db.ExecContext(a.ctx, entry.SqlMode)
			if err != nil {
				a.logger.Error("err exec sysvar query.", "err", err)
				return err
			}
		}
	}

	queries := []string{}
	queries = append(queries, entry.SystemVariablesStatement, entry.SqlMode, entry.DbSQL)
	queries = append(queries, entry.TbSQL...)
	tx, err := db.BeginTx(a.ctx, &gosql.TxOptions{})
	if err != nil {
		return err
	}
	nRows := int64(len(entry.ValuesX))
	defer func() {
		if err != nil {
			return
		}
		err = tx.Commit()
		if err == nil {
			atomic.AddInt64(&a.TotalRowsReplayed, nRows)
		}
	}()
	if _, err := tx.ExecContext(a.ctx, querySetFKChecksOff); err != nil {
		return err
	}
	execQuery := func(query string) error {
		a.logger.Debug("ApplyEventQueries. exec", "query", g.StrLim(query, 256))
		_, err := tx.ExecContext(a.ctx, query)
		if err != nil {
			queryStart := g.StrLim(query, 10) // avoid printing sensitive information
			errCtx := errors.Wrapf(err, "tx.Exec. queryStart %v seq", queryStart)
			if !sql.IgnoreError(err) {
				a.logger.Error("ApplyEventQueries. exec error", "err", errCtx)
				return errCtx
			}
			if !sql.IgnoreExistsError(err) {
				a.logger.Warn("ApplyEventQueries. ignore error", "err", errCtx)
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
	var totalDeltaCopied int64
	var lenApplierMsgQueue int
	var capApplierMsgQueue int
	var lenApplierTxQueue int
	var capApplierTxQueue int
	var delay int64
	if a.ai != nil {
		totalDeltaCopied = a.ai.TotalDeltaCopied
		lenApplierMsgQueue = len(a.ai.incrBytesQueue)
		capApplierMsgQueue = cap(a.ai.incrBytesQueue)
		lenApplierTxQueue = len(a.ai.binlogEntryQueue)
		capApplierTxQueue = cap(a.ai.binlogEntryQueue)
		delay = a.ai.timestampCtx.GetDelay()
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
			backlog = fmt.Sprintf("%d/%d", lenApplierMsgQueue+lenApplierTxQueue,
				capApplierMsgQueue+capApplierTxQueue)
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
			ApplierMsgQueueSize: lenApplierMsgQueue,
			ApplierTxQueueSize:  lenApplierTxQueue,
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

func (a *Applier) onError(state int, err error) {
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

func (a *Applier) Shutdown() error {
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

func (a *Applier) watchTargetGtid() {

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

func (a *Applier) checkJobFinish() {
	jobStatus, err := a.storeManager.GetJobStatus(a.subject)
	if err != nil {
		a.onError(common.TaskStateDead, err)
	}
	if jobStatus == common.TargetGtidFinished {
		a.logger.Info("job finish. shutting down")
		_ = a.Shutdown()
	}
}

func (a *Applier) enableForeignKeyChecks() error {
	_, err := a.db.ExecContext(a.ctx, querySetFKChecksOn)
	if err != nil {
		return err
	}
	for _, conn := range a.dbs {
		_, err = conn.Db.ExecContext(a.ctx, querySetFKChecksOn)
		if err != nil {
			return err
		}
	}
	return nil
}
