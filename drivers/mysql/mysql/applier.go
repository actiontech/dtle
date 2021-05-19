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
	"github.com/actiontech/dtle/drivers/mysql/common"
	"github.com/hashicorp/nomad/plugins/drivers"
	"github.com/pkg/errors"
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
	"github.com/hashicorp/nomad/drivers/shared/eventer"
)

const (
	cleanupGtidExecutedLimit = 2048
	pingInterval             = 10 * time.Second
	jobIncrCopy              = "job_stage_incr"
	jobFullCopy              = "job_stage_full"
)
const (
	TaskStateComplete int = iota
	TaskStateRestart
	TaskStateDead
)

// Applier connects and writes the the applier-server, which is the server where
// write row data and apply binlog events onto the dest table.

type Applier struct {
	logger       hclog.Logger
	subject      string
	mysqlContext *common.MySQLDriverConfig

	NatsAddr          string
	MySQLVersion      string
	TotalRowsReplayed int64

	dbs []*sql.Conn
	db  *gosql.DB

	rowCopyComplete chan struct{}
	fullBytesQueue  chan []byte
	dumpEntryQueue  chan *common.DumpEntry
	ai              *ApplierIncr

	natsConn *gonats.Conn
	waitCh   chan *drivers.ExitResult
	wg       sync.WaitGroup

	shutdown     bool
	shutdownCh   chan struct{}
	shutdownLock sync.Mutex

	nDumpEntry int64

	stubFullApplyDelay time.Duration

	gtidSet     *gomysql.MysqlGTIDSet
	gtidSetLock *sync.RWMutex

	storeManager *common.StoreManager
	gtidCh       chan *common.BinlogCoordinateTx

	stage          string
	memory1        *int64
	memory2        *int64
	event          *eventer.Eventer
	taskConfig     *drivers.TaskConfig
}

func NewApplier(
	ctx *common.ExecContext, cfg *common.MySQLDriverConfig, logger hclog.Logger,
	storeManager *common.StoreManager, natsAddr string, waitCh chan *drivers.ExitResult, event *eventer.Eventer, taskConfig *drivers.TaskConfig) (a *Applier, err error) {

	logger.Info("NewApplier", "job", ctx.Subject)

	a = &Applier{
		logger:          logger.Named("applier").With("job", ctx.Subject),
		subject:         ctx.Subject,
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
				a.gtidSetLock.Lock()
				common.UpdateGtidSet(a.gtidSet, coord.SID, coord.GNO)
				a.gtidSetLock.Unlock()
				file = coord.LogFile
				pos = coord.LogPos
			}
		}
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

	//a.logger.Debug("the connectionconfi host is ",a.mysqlContext.ConnectionConfig.Host)
//	a.logger.Info("Apply binlog events to %s.%d", a.mysqlContext.ConnectionConfig.Host, a.mysqlContext.ConnectionConfig.Port)
	if err := a.initDBConnections(); err != nil {
		a.onError(TaskStateDead, err)
		return
	}

	a.ai, err = NewApplierIncr(a.subject, a.mysqlContext, a.logger, a.gtidSet, a.memory2,
		a.db, a.dbs, a.shutdownCh, a.gtidSetLock)
	if err != nil {
		a.onError(TaskStateDead, errors.Wrap(err, "NewApplierIncr"))
		return
	}
	a.ai.GtidUpdateHook = func(coord *common.BinlogCoordinateTx) {
		a.gtidCh <- coord
	}
	a.ai.OnError = a.onError

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
	err = a.storeManager.DstPutNats(a.subject, a.NatsAddr, a.shutdownCh, func(err error) {
		a.onError(TaskStateDead, errors.Wrap(err, "DstPutNats"))
	})
	if err != nil {
		a.onError(TaskStateDead, errors.Wrap(err, "DstPutNats"))
		return
	}

	go a.updateGtidLoop()

	if a.stage != jobFullCopy {
		a.stage = jobFullCopy
		a.sendEvent(jobFullCopy)
	}

	go a.doFullCopy()
	go func() {
		err := a.ai.Run()
		if err != nil {
			a.onError(TaskStateDead, err)
		}
	}()
}

func (a *Applier) doFullCopy() {
	a.mysqlContext.Stage = common.StageSlaveWaitingForWorkersToProcessQueue
	a.logger.Info("Operating until row copy is complete")

	go func() {
		for {
			select {
			case <-a.shutdownCh:
				return
			case copyRows := <-a.dumpEntryQueue:
				//time.Sleep(20 * time.Second) // #348 stub
				if err := a.ApplyEventQueries(a.db, copyRows); err != nil {
					a.onError(TaskStateDead, err)
					return
				}
				atomic.AddInt64(a.memory1, -int64(copyRows.Size()))
				if atomic.LoadInt64(&a.nDumpEntry) <= 0 {
					err := fmt.Errorf("DTLE_BUG a.nDumpEntry <= 0")
					a.logger.Error(err.Error())
					a.onError(TaskStateDead, err)
					return
				} else {
					atomic.AddInt64(&a.nDumpEntry, -1)
					a.logger.Debug("ApplyEventQueries. after", "nDumpEntry", a.nDumpEntry)
				}
			}
		}
	}()

	var stopLoop = false
	for !stopLoop && !a.shutdown {
		t10 := time.NewTimer(10 * time.Second)

		select {
		case <-a.shutdownCh:
			stopLoop = true

		case bs := <-a.fullBytesQueue:
			atomic.AddInt64(a.memory1, -int64(len(bs)))

			copyRows := &common.DumpEntry{}
			err := common.Decode(bs, copyRows) // TODO decode once for discarded-resent msg
			if err != nil {
				a.onError(TaskStateDead, errors.Wrap(err, "DecodeDumpEntry"))
				return
			}
			a.dumpEntryQueue <- copyRows
			atomic.AddInt64(a.memory1, int64(copyRows.Size()))

			atomic.AddInt64(&a.mysqlContext.RowsEstimate, copyRows.TotalCount)

		case <-a.rowCopyComplete:
			a.logger.Info("doFullCopy: loop: rowCopyComplete")
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
		a.logger.Debug("full. recv a msg.", "len", len(m.Data), "fullBytesQueue", len(a.fullBytesQueue))

		select {
		case <-a.rowCopyComplete: // full complete. Maybe src task restart.
			if err := a.natsConn.Publish(m.Reply, nil); err != nil {
				a.onError(TaskStateDead, err)
			}
			a.logger.Debug("full. after publish nats reply")
			return
		default:
		}

		segmentFinished, err := fullNMM.Handle(m.Data)
		if err != nil {
			a.onError(TaskStateDead, errors.Wrap(err, "fullNMM.Handle"))
			return
		}

		if !segmentFinished {
			if err := a.natsConn.Publish(m.Reply, nil); err != nil {
				a.onError(TaskStateDead, err)
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
					a.onError(TaskStateDead, err)
					return
				}
				a.logger.Debug("full. after publish nats reply", "vacancy", vacancy)
			}
		}
	})

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

		dumpData := &common.DumpStatResult{}
		if err := common.Decode(m.Data, dumpData); err != nil {
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
		// Do not re-assign a.gtidSet (#538). Update it.
		gs0, err := gomysql.ParseMysqlGTIDSet(dumpData.Gtid)
		if err != nil {
			a.onError(TaskStateDead, errors.Wrap(err, "ParseMysqlGTIDSet"))
			return
		}
		gs := gs0.(*gomysql.MysqlGTIDSet)
		for _, uuidSet := range gs.Sets {
			a.gtidSet.AddSet(uuidSet)
		}
		a.mysqlContext.Gtid = dumpData.Gtid
		a.mysqlContext.BinlogFile = dumpData.LogFile
		a.mysqlContext.BinlogPos = dumpData.LogPos
		a.gtidCh <- nil // coord == nil is a flag for update/upload gtid

		a.mysqlContext.Stage = common.StageSlaveWaitingForWorkersToProcessQueue
		if a.stage != jobIncrCopy {
			a.stage = jobIncrCopy
			a.sendEvent(jobIncrCopy)
		}
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

	incrNMM := common.NewNatsMsgMerger(a.logger.With("nmm", "incr"))
	_, err = a.natsConn.Subscribe(fmt.Sprintf("%s_incr_hete", a.subject), func(m *gonats.Msg) {
		a.logger.Debug("incr. recv a msg.")

		segmentFinished, err := incrNMM.Handle(m.Data)
		if err != nil {
			a.onError(TaskStateDead, errors.Wrap(err, "incrNMM.Handle"))
			return
		}

		if !segmentFinished {
			if err := a.natsConn.Publish(m.Reply, nil); err != nil {
				a.onError(TaskStateDead, err)
				return
			}
			a.logger.Debug("incr. after publish nats reply.")
		} else {
			select {
			case <-a.shutdownCh:
				return
			case a.ai.incrBytesQueue <- incrNMM.GetBytes():
				incrNMM.Reset()
				a.logger.Debug("incr. incrBytesQueue enqueued", "vacancy", cap(a.ai.incrBytesQueue) - len(a.ai.incrBytesQueue))

				if err := a.natsConn.Publish(m.Reply, nil); err != nil {
					a.onError(TaskStateDead, err)
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
	if a.dbs, err = sql.CreateConns(a.db, a.mysqlContext.ParallelWorkers); err != nil {
		a.logger.Debug("beging connetion mysql 2 create conns err")
		return err
	}

	if err := a.ValidateConnection(); err != nil {
		return err
	}
	a.logger.Debug("beging connetion mysql 5 validate  grants")
	if err := a.ValidateGrants(); err != nil {
		a.logger.Error("Unexpected error on ValidateGrants", "err", err)
		return err
	}
	a.logger.Debug("after ValidateGrants")

	if timezone, err := base.ValidateAndReadTimeZone(a.db); err != nil {
		return err
	} else {
		a.logger.Info("got timezone", "timezone", timezone)
	}

	a.logger.Info("Initiated", "mysql", a.mysqlContext.ConnectionConfig.GetAddr(), "version", a.MySQLVersion)
	return nil
}

// ValidateConnection issues a simple can-connect to MySQL
func (a *Applier) ValidateConnection() error {
	query := `select @@version`
	if err := a.db.QueryRow(query).Scan(&a.MySQLVersion); err != nil {
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
		if _, err := a.db.Query(`use mysql`); err != nil {
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
	return fmt.Errorf("user has insufficient privileges for applier. Needed: SUPER|ALL on *.*")
}

func (a *Applier) ApplyEventQueries(db *gosql.DB, entry *common.DumpEntry) error {
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
		a.logger.Debug("ApplyEventQueries. exec", "query", g.StrLim(query, 256))
		_, err := tx.Exec(query)
		if err != nil {
			queryStart := g.StrLim(query, 10) // avoid printing sensitive information
			errCtx := errors.Wrapf(err, "tx.Exec. queryStart %v seq %v", queryStart, entry.Seq)
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
	var lenApplyDataEntryQueue int
	var capApplyDataEntryQueue int
	var delay int64
	if a.ai != nil {
		totalDeltaCopied = a.ai.TotalDeltaCopied
		lenApplyDataEntryQueue = len(a.ai.incrBytesQueue)
		capApplyDataEntryQueue = cap(a.ai.incrBytesQueue)
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
			backlog = fmt.Sprintf("%d/%d", lenApplyDataEntryQueue, capApplyDataEntryQueue)
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
			ApplierTxQueueSize: lenApplyDataEntryQueue,
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

	bs := []byte(err.Error())

	switch state {
	case TaskStateComplete:
		a.logger.Info("Done migrating")
	case TaskStateRestart:
		if a.natsConn != nil {
			if err := a.natsConn.Publish(fmt.Sprintf("%s_restart", a.subject), bs); err != nil {
				a.logger.Error("when triggering extractor restart", "err", err, "state", state)
			}
		}
	default:
		if a.natsConn != nil {
			if err := a.natsConn.Publish(fmt.Sprintf("%s_error", a.subject), bs); err != nil {
				a.logger.Error("when triggering extractor shutdown", "err", err, "state", state)
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

func (a *Applier) Pause() error {
	return a.Shutdown()
}
