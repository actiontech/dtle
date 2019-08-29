// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package relay

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"github.com/siddontang/go/sync2"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/dm/unit"
	"github.com/pingcap/dm/pkg/binlog"
	fr "github.com/pingcap/dm/pkg/func-rollback"
	"github.com/pingcap/dm/pkg/log"
	pkgstreamer "github.com/pingcap/dm/pkg/streamer"
	"github.com/pingcap/dm/pkg/utils"
	"github.com/pingcap/dm/relay/reader"
	"github.com/pingcap/dm/relay/transformer"
	"github.com/pingcap/dm/relay/writer"
)

var (
	// used to fill RelayLogInfo
	fakeTaskName = "relay"
)

const (
	slaveReadTimeout            = 1 * time.Minute  // slave read binlog data timeout, ref: https://dev.mysql.com/doc/refman/8.0/en/replication-options-slave.html#sysvar_slave_net_timeout
	masterHeartbeatPeriod       = 30 * time.Second // master server send heartbeat period: ref: `MASTER_HEARTBEAT_PERIOD` in https://dev.mysql.com/doc/refman/8.0/en/change-master-to.html
	flushMetaInterval           = 30 * time.Second
	getMasterStatusInterval     = 30 * time.Second
	trimUUIDsInterval           = 1 * time.Hour
	showStatusConnectionTimeout = "1m"

	// dumpFlagSendAnnotateRowsEvent (BINLOG_SEND_ANNOTATE_ROWS_EVENT) request the MariaDB master to send Annotate_rows_log_event back.
	dumpFlagSendAnnotateRowsEvent uint16 = 0x02
)

// NewRelay creates an instance of Relay.
var NewRelay = NewRealRelay

// Process defines mysql-like relay log process unit
type Process interface {
	// Init initial relat log unit
	Init() (err error)
	// Process run background logic of relay log unit
	Process(ctx context.Context, pr chan pb.ProcessResult)
	// SwitchMaster switches relay's master server
	SwitchMaster(ctx context.Context, req *pb.SwitchRelayMasterRequest) error
	// Migrate  resets  binlog position
	Migrate(ctx context.Context, binlogName string, binlogPos uint32) error
	// ActiveRelayLog returns the earliest active relay log info in this operator
	ActiveRelayLog() *pkgstreamer.RelayLogInfo
	// Reload reloads config
	Reload(newCfg *Config) error
	// Update updates config
	Update(cfg *config.SubTaskConfig) error
	// Resume resumes paused relay log process unit
	Resume(ctx context.Context, pr chan pb.ProcessResult)
	// Pause pauses a running relay log process unit
	Pause()
	// Error returns error message if having one
	Error() interface{}
	// Status returns status of relay log process unit
	Status() interface{}
	// Close does some clean works
	Close()
	// IsClosed returns whether relay log process unit was closed
	IsClosed() bool

	GetMeta() Meta
}

// Relay relays mysql binlog to local file.
type Relay struct {
	db        *sql.DB
	cfg       *Config
	syncerCfg replication.BinlogSyncerConfig

	meta   Meta
	closed sync2.AtomicBool
	sync.RWMutex

	activeRelayLog struct {
		sync.RWMutex
		info *pkgstreamer.RelayLogInfo
	}
}

func (r *Relay) GetMeta() Meta {
	return r.meta
}

// NewRealRelay creates an instance of Relay.
func NewRealRelay(cfg *Config) Process {
	syncerCfg := replication.BinlogSyncerConfig{
		ServerID:        uint32(cfg.ServerID),
		Flavor:          cfg.Flavor,
		Host:            cfg.From.Host,
		Port:            uint16(cfg.From.Port),
		User:            cfg.From.User,
		Password:        cfg.From.Password,
		Charset:         cfg.Charset,
		UseDecimal:      true, // must set true. ref: https://github.com/pingcap/tidb-enterprise-tools/pull/272
		ReadTimeout:     slaveReadTimeout,
		HeartbeatPeriod: masterHeartbeatPeriod,
		VerifyChecksum:  true,
	}

	if !cfg.EnableGTID {
		// for rawMode(true), we only parse FormatDescriptionEvent and RotateEvent
		// if not need to support GTID mode, we can enable rawMode
		syncerCfg.RawModeEnabled = true
	}

	if cfg.Flavor == mysql.MariaDBFlavor {
		// ref: https://mariadb.com/kb/en/library/annotate_rows_log_event/#slave-option-replicate-annotate-row-events
		// ref: https://github.com/MariaDB/server/blob/bf71d263621c90cbddc7bde9bf071dae503f333f/sql/sql_repl.cc#L1809
		syncerCfg.DumpCommandFlag |= dumpFlagSendAnnotateRowsEvent
	}

	return &Relay{
		cfg:       cfg,
		syncerCfg: syncerCfg,
		meta:      NewLocalMeta(cfg.Flavor, cfg.RelayDir),
	}
}

// Init implements the dm.Unit interface.
func (r *Relay) Init() (err error) {
	rollbackHolder := fr.NewRollbackHolder("relay")
	defer func() {
		if err != nil {
			rollbackHolder.RollbackReverseOrder()
		}
	}()

	cfg := r.cfg.From
	dbDSN := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8mb4&interpolateParams=true&readTimeout=%s", cfg.User, cfg.Password, cfg.Host, cfg.Port, showStatusConnectionTimeout)
	db, err := sql.Open("mysql", dbDSN)
	if err != nil {
		return errors.Trace(err)
	}
	r.db = db
	rollbackHolder.Add(fr.FuncRollback{Name: "close-DB", Fn: r.closeDB})

	if err2 := os.MkdirAll(r.cfg.RelayDir, 0755); err2 != nil {
		return errors.Trace(err2)
	}

	err = r.meta.Load()
	if err != nil {
		return errors.Trace(err)
	}

	if err := reportRelayLogSpaceInBackground(r.cfg.RelayDir); err != nil {
		return errors.Trace(err)
	}

	return nil
}

// Process implements the dm.Unit interface.
func (r *Relay) Process(ctx context.Context, pr chan pb.ProcessResult) {
	errs := make([]*pb.ProcessError, 0, 1)
	err := r.process(ctx)
	if err != nil && errors.Cause(err) != replication.ErrSyncClosed {
		relayExitWithErrorCounter.Inc()
		log.Errorf("[relay] process exit with error %v", errors.ErrorStack(err))
		// TODO: add specified error type instead of pb.ErrorType_UnknownError
		errs = append(errs, unit.NewProcessError(pb.ErrorType_UnknownError, errors.ErrorStack(err)))
	}

	isCanceled := false
	if len(errs) == 0 {
		select {
		case <-ctx.Done():
			isCanceled = true
		default:
		}
	}
	pr <- pb.ProcessResult{
		IsCanceled: isCanceled,
		Errors:     errs,
	}
}

// SwitchMaster switches relay's master server
// before call this from dmctl, you must ensure that relay catches up previous master
// we can not check this automatically in this func because master already changed
// switch master server steps:
//   1. use dmctl to pause relay
//   2. ensure relay catching up current master server (use `query-status`)
//   3. switch master server for upstream
//      * change relay's master config, TODO
//      * change master behind VIP
//   4. use dmctl to switch relay's master server (use `switch-relay-master`)
//   5. use dmctl to resume relay
func (r *Relay) SwitchMaster(ctx context.Context, req *pb.SwitchRelayMasterRequest) error {
	if !r.cfg.EnableGTID {
		return errors.New("can only switch relay's master server when GTID enabled")
	}
	err := r.reSetupMeta()
	return errors.Trace(err)
}

func (r *Relay) process(parentCtx context.Context) error {
	parser2, err := utils.GetParser(r.db, false) // refine to use user config later
	if err != nil {
		return errors.Trace(err)
	}

	isNew, err := isNewServer(r.meta.UUID(), r.db, r.cfg.Flavor)
	if err != nil {
		return errors.Trace(err)
	}
	if isNew {
		// re-setup meta for new server
		err = r.reSetupMeta()
		if err != nil {
			return errors.Trace(err)
		}
	} else {
		r.updateMetricsRelaySubDirIndex()
		// if not a new server, try to recover the latest relay log file.
		err = r.tryRecoverLatestFile(parser2)
		if err != nil {
			return errors.Trace(err)
		}
	}

	reader2, err := r.setUpReader()
	if err != nil {
		return errors.Trace(err)
	}
	defer func() {
		err = reader2.Close()
		if err != nil {
			log.Errorf("[relay] close binlog event reader error %v", err)
		}
	}()

	writer2, err := r.setUpWriter(parser2)
	if err != nil {
		return errors.Trace(err)
	}
	defer func() {
		err = writer2.Close()
		if err != nil {
			log.Errorf("[relay] close binlog event writer error %v", err)
		}
	}()

	transformer2 := transformer.NewTransformer(parser2)

	go r.doIntervalOps(parentCtx)

	return errors.Trace(r.handleEvents(parentCtx, reader2, transformer2, writer2))
}

// tryRecoverLatestFile tries to recover latest relay log file with corrupt/incomplete binlog events/transactions.
func (r *Relay) tryRecoverLatestFile(parser2 *parser.Parser) error {
	var (
		uuid, latestPos = r.meta.Pos()
		_, latestGTID   = r.meta.GTID()
	)

	if latestPos.Compare(minCheckpoint) <= 0 {
		log.Warnf("[relay] no relay log file need to recover, position %s, GTID sets %v", latestPos, latestGTID)
		return nil
	}

	// setup a special writer to do the recovering
	cfg := &writer.FileConfig{
		RelayDir: r.meta.Dir(),
		Filename: latestPos.Name,
	}
	writer2 := writer.NewFileWriter(cfg, parser2)
	err := writer2.Start()
	if err != nil {
		return errors.Annotatef(err, "start recover writer for UUID %s with config %+v", uuid, cfg)
	}
	defer func() {
		err2 := writer2.Close()
		if err2 != nil {
			log.Errorf("[relay] close recover writer for UUID %s with config %+v error %v", uuid, cfg, err2)
		}
	}()
	log.Infof("[relay] started recover writer for UUID %s with config %+v", uuid, cfg)

	result, err := writer2.Recover()
	if err == nil {
		if result.Recovered {
			log.Warnf("[relay] relay log file recovered from position %s to %s, GTID sets %v to %v",
				latestPos, result.LatestPos, latestGTID, result.LatestGTIDs)
			err = r.meta.Save(result.LatestPos, result.LatestGTIDs)
			if err != nil {
				return errors.Annotatef(err, "save position %s, GTID sets %v after recovered", result.LatestPos, result.LatestGTIDs)
			}
		} else if result.LatestPos.Compare(latestPos) > 0 ||
			(result.LatestGTIDs != nil && !result.LatestGTIDs.Equal(latestGTID) && result.LatestGTIDs.Contain(latestGTID)) {
			log.Warnf("[relay] relay log file have more events after position %s (until %s), GTID sets %v (until %v)",
				latestPos, result.LatestPos, latestGTID, result.LatestGTIDs)
		}

	}
	return errors.Annotatef(err, "recover for UUID %s with config %+v", uuid, cfg)
}

// handleEvents handles binlog events, including:
//   1. read events from upstream
//   2. transform events
//   3. write events into relay log files
//   4. update metadata if needed
func (r *Relay) handleEvents(ctx context.Context, reader2 reader.Reader, transformer2 transformer.Transformer, writer2 writer.Writer) error {
	var (
		_, lastPos  = r.meta.Pos()
		_, lastGTID = r.meta.GTID()
	)

	for {
		// 1. read events from upstream server
		readTimer := time.Now()
		rResult, err := reader2.GetEvent(ctx)
		binlogReadDurationHistogram.Observe(time.Since(readTimer).Seconds())

		if err != nil {
			switch errors.Cause(err) {
			case context.Canceled:
				return nil
			case replication.ErrChecksumMismatch:
				relayLogDataCorruptionCounter.Inc()
			case replication.ErrSyncClosed, replication.ErrNeedSyncAgain:
				// do nothing, but the error will be returned
			default:
				if utils.IsErrBinlogPurged(err) {
					// TODO: try auto fix GTID, and can support auto switching between upstream server later.
					cfg := r.cfg.From
					log.Errorf("[relay] the requested binlog files have purged in the master server or the master server behind %s:%d have switched, currently DM do no support to handle this error %v",
						cfg.Host, cfg.Port, err)
				}
				binlogReadErrorCounter.Inc()
			}
			return errors.Trace(err)
		}
		e := rResult.Event
		log.Debugf("[relay] receive binlog event with header %+v", e.Header)

		// 2. transform events
		tResult := transformer2.Transform(e)
		if len(tResult.NextLogName) > 0 && tResult.NextLogName > lastPos.Name {
			lastPos = mysql.Position{
				Name: string(tResult.NextLogName),
				Pos:  uint32(tResult.LogPos),
			}
			log.Infof("[relay] rotate to %s", lastPos.String())
		}
		if tResult.Ignore {
			log.Infof("[relay] ignore event %+v by transformer", e.Header)
			continue
		}

		// 3. save events into file
		writeTimer := time.Now()
		log.Debugf("[relay] writing binlog event with header %+v", e.Header)
		wResult, err := writer2.WriteEvent(e)
		if err != nil {
			relayLogWriteErrorCounter.Inc()
			return errors.Trace(err)
		} else if wResult.Ignore {
			log.Infof("[relay] ignore event %+v by writer", e.Header)
			r.tryUpdateActiveRelayLog(e, lastPos.Name) // even the event ignored we still need to try this update.
			continue
		}
		relayLogWriteDurationHistogram.Observe(time.Since(writeTimer).Seconds())
		r.tryUpdateActiveRelayLog(e, lastPos.Name) // wrote a event, try update the current active relay log.

		// 4. update meta and metrics
		needSavePos := tResult.CanSaveGTID
		lastPos.Pos = tResult.LogPos
		err = lastGTID.Set(tResult.GTIDSet)
		if err != nil {
			return errors.Annotatef(err, "update last GTID set to %v", tResult.GTIDSet)
		}
		if !r.cfg.EnableGTID {
			// if go-mysql set RawModeEnabled to true
			// then it will only parse FormatDescriptionEvent and RotateEvent
			// then check `e.Event.(type)` for `QueryEvent` and `XIDEvent` will never be true
			// so we need to update pos for all events
			// and also save pos for all events
			if e.Header.EventType != replication.ROTATE_EVENT {
				lastPos.Pos = e.Header.LogPos // for RotateEvent, lastPos updated to the next binlog file's position.
			}
			needSavePos = true
		}

		relayLogWriteSizeHistogram.Observe(float64(e.Header.EventSize))
		relayLogPosGauge.WithLabelValues("relay").Set(float64(lastPos.Pos))
		if index, err2 := binlog.GetFilenameIndex(lastPos.Name); err2 != nil {
			log.Errorf("[relay] parse binlog file name %s err %v", lastPos.Name, err2)
		} else {
			relayLogFileGauge.WithLabelValues("relay").Set(float64(index))
		}

		if needSavePos {
			err = r.meta.Save(lastPos, lastGTID)
			if err != nil {
				return errors.Annotatef(err, "save position %s, GTID sets %v into meta", lastPos, lastGTID)
			}
		}
	}
}

// tryUpdateActiveRelayLog tries to update current active relay log file.
// we should to update after received/wrote a FormatDescriptionEvent because it means switched to a new relay log file.
// NOTE: we can refactor active (writer/read) relay log mechanism later.
func (r *Relay) tryUpdateActiveRelayLog(e *replication.BinlogEvent, filename string) {
	if e.Header.EventType == replication.FORMAT_DESCRIPTION_EVENT {
		r.setActiveRelayLog(filename)
		log.Infof("[relay] the active relay log file change to %s", filename)
	}
}

// reSetupMeta re-setup the metadata when switching to a new upstream master server.
func (r *Relay) reSetupMeta() error {
	uuid, err := utils.GetServerUUID(r.db, r.cfg.Flavor)
	if err != nil {
		return errors.Trace(err)
	}
	err = r.meta.AddDir(uuid, nil, nil)
	if err != nil {
		return errors.Trace(err)
	}
	err = r.meta.Load()
	if err != nil {
		return errors.Trace(err)
	}

	// try adjust meta with start pos from config
	if (r.cfg.EnableGTID && len(r.cfg.BinlogGTID) > 0) || len(r.cfg.BinLogName) > 0 {
		adjusted, err := r.meta.AdjustWithStartPos(r.cfg.BinLogName, r.cfg.BinlogGTID, r.cfg.EnableGTID)
		if err != nil {
			return errors.Trace(err)
		} else if adjusted {
			log.Infof("[relay] adjusted meta to start pos with binlog-name (%s), binlog-gtid (%s)", r.cfg.BinLogName, r.cfg.BinlogGTID)
		}
	}

	r.updateMetricsRelaySubDirIndex()

	return nil
}

func (r *Relay) updateMetricsRelaySubDirIndex() {
	// when switching master server, update sub dir index metrics
	node := r.masterNode()
	uuidWithSuffix := r.meta.UUID() // only change after switch
	_, suffix, err := utils.ParseSuffixForUUID(uuidWithSuffix)
	if err != nil {
		log.Errorf("parse suffix for UUID %s error %v", uuidWithSuffix, errors.Trace(err))
		return
	}
	relaySubDirIndex.WithLabelValues(node, uuidWithSuffix).Set(float64(suffix))
}

func (r *Relay) doIntervalOps(ctx context.Context) {
	flushTicker := time.NewTicker(flushMetaInterval)
	defer flushTicker.Stop()
	masterStatusTicker := time.NewTicker(getMasterStatusInterval)
	defer masterStatusTicker.Stop()
	trimUUIDsTicker := time.NewTicker(trimUUIDsInterval)
	defer trimUUIDsTicker.Stop()

	for {
		select {
		case <-flushTicker.C:
			if r.meta.Dirty() {
				err := r.meta.Flush()
				if err != nil {
					log.Errorf("[relay] flush meta error %v", errors.ErrorStack(err))
				} else {
					log.Infof("[relay] flush meta finished, %s", r.meta.String())
				}
			}
		case <-masterStatusTicker.C:
			pos, _, err := utils.GetMasterStatus(r.db, r.cfg.Flavor)
			if err != nil {
				log.Warnf("[relay] get master status error %v", errors.ErrorStack(err))
				continue
			}
			index, err := binlog.GetFilenameIndex(pos.Name)
			if err != nil {
				log.Errorf("[relay] parse binlog file name %s error %v", pos.Name, err)
				continue
			}
			relayLogFileGauge.WithLabelValues("master").Set(float64(index))
			relayLogPosGauge.WithLabelValues("master").Set(float64(pos.Pos))
		case <-trimUUIDsTicker.C:
			trimmed, err := r.meta.TrimUUIDs()
			if err != nil {
				log.Errorf("[relay] trim UUIDs error %s", errors.ErrorStack(err))
			} else if len(trimmed) > 0 {
				log.Infof("[relay] trim UUIDs %s", strings.Join(trimmed, ";"))
			}
		case <-ctx.Done():
			return
		}
	}
}

// setUpReader setups the underlying reader used to read binlog events from the upstream master server.
func (r *Relay) setUpReader() (reader.Reader, error) {
	uuid, pos := r.meta.Pos()
	_, gs := r.meta.GTID()
	cfg := &reader.Config{
		SyncConfig: r.syncerCfg,
		Pos:        pos,
		GTIDs:      gs,
		MasterID:   r.masterNode(),
		EnableGTID: r.cfg.EnableGTID,
	}

	reader2 := reader.NewReader(cfg)
	err := reader2.Start()
	if err != nil {
		// do not log the whole config to protect the password in `SyncConfig`.
		// and other config items should already logged before or included in `err`.
		return nil, errors.Annotatef(err, "start reader for UUID %s", uuid)
	}

	log.Infof("[relay] started underlying reader for UUID %s ", uuid)
	return reader2, nil
}

// setUpWriter setups the underlying writer used to writer binlog events into file or other places.
func (r *Relay) setUpWriter(parser2 *parser.Parser) (writer.Writer, error) {
	uuid, pos := r.meta.Pos()
	cfg := &writer.FileConfig{
		RelayDir: r.meta.Dir(),
		Filename: pos.Name,
	}
	writer2 := writer.NewFileWriter(cfg, parser2)
	err := writer2.Start()
	if err != nil {
		return nil, errors.Annotatef(err, "start writer for UUID %s with config %+v", uuid, cfg)
	}

	log.Infof("[relay] started underlying writer for UUID %s with config %+v", uuid, cfg)
	return writer2, nil
}

func (r *Relay) masterNode() string {
	return fmt.Sprintf("%s:%d", r.cfg.From.Host, r.cfg.From.Port)
}

// IsClosed tells whether Relay unit is closed or not.
func (r *Relay) IsClosed() bool {
	return r.closed.Get()
}

// stopSync stops syncing, now it used by Close and Pause
func (r *Relay) stopSync() {
	if err := r.meta.Flush(); err != nil {
		log.Errorf("[relay] flush checkpoint error %v", errors.ErrorStack(err))
	}
}

func (r *Relay) closeDB() {
	if r.db != nil {
		r.db.Close()
		r.db = nil
	}
}

// Close implements the dm.Unit interface.
func (r *Relay) Close() {
	r.Lock()
	defer r.Unlock()
	if r.closed.Get() {
		return
	}
	log.Info("[relay] relay unit is closing")

	r.stopSync()

	r.closeDB()

	r.closed.Set(true)
	log.Info("[relay] relay unit closed")
}

// Status implements the dm.Unit interface.
func (r *Relay) Status() interface{} {
	masterPos, masterGTID, err := utils.GetMasterStatus(r.db, r.cfg.Flavor)
	if err != nil {
		log.Warnf("[relay] get master status %v", errors.ErrorStack(err))
	}

	uuid, relayPos := r.meta.Pos()
	_, relayGTIDSet := r.meta.GTID()
	rs := &pb.RelayStatus{
		MasterBinlog: masterPos.String(),
		RelaySubDir:  uuid,
		RelayBinlog:  relayPos.String(),
	}
	if masterGTID != nil { // masterGTID maybe a nil interface
		rs.MasterBinlogGtid = masterGTID.String()
	}
	if relayGTIDSet != nil {
		rs.RelayBinlogGtid = relayGTIDSet.String()
	}
	if r.cfg.EnableGTID {
		if masterGTID != nil && relayGTIDSet != nil && relayGTIDSet.Equal(masterGTID) {
			rs.RelayCatchUpMaster = true
		}
	} else {
		rs.RelayCatchUpMaster = masterPos.Compare(relayPos) == 0
	}
	return rs
}

// Error implements the dm.Unit interface.
func (r *Relay) Error() interface{} {
	return &pb.RelayError{}
}

// Type implements the dm.Unit interface.
func (r *Relay) Type() pb.UnitType {
	return pb.UnitType_Relay
}

// IsFreshTask implements Unit.IsFreshTask
func (r *Relay) IsFreshTask() (bool, error) {
	return true, nil
}

// Pause pauses the process, it can be resumed later
func (r *Relay) Pause() {
	if r.IsClosed() {
		log.Warn("[relay] try to pause, but already closed")
		return
	}

	r.stopSync()
}

// Resume resumes the paused process
func (r *Relay) Resume(ctx context.Context, pr chan pb.ProcessResult) {
	// do nothing now, re-process called `Process` from outer directly
}

// Update implements Unit.Update
func (r *Relay) Update(cfg *config.SubTaskConfig) error {
	// not support update configuration now
	return nil
}

// Reload updates config
func (r *Relay) Reload(newCfg *Config) error {
	r.Lock()
	defer r.Unlock()
	log.Info("[relay] relay unit is updating")

	// Update From
	r.cfg.From = newCfg.From

	// Update AutoFixGTID
	r.cfg.AutoFixGTID = newCfg.AutoFixGTID

	// Update Charset
	r.cfg.Charset = newCfg.Charset

	r.closeDB()
	cfg := r.cfg.From
	dbDSN := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8mb4&interpolateParams=true&readTimeout=%s", cfg.User, cfg.Password, cfg.Host, cfg.Port, showStatusConnectionTimeout)
	db, err := sql.Open("mysql", dbDSN)
	if err != nil {
		return errors.Trace(err)
	}
	r.db = db

	syncerCfg := replication.BinlogSyncerConfig{
		ServerID:        uint32(r.cfg.ServerID),
		Flavor:          r.cfg.Flavor,
		Host:            newCfg.From.Host,
		Port:            uint16(newCfg.From.Port),
		User:            newCfg.From.User,
		Password:        newCfg.From.Password,
		Charset:         newCfg.Charset,
		UseDecimal:      true, // must set true. ref: https://github.com/pingcap/dm/pull/272
		ReadTimeout:     slaveReadTimeout,
		HeartbeatPeriod: masterHeartbeatPeriod,
		VerifyChecksum:  true,
	}

	if !newCfg.EnableGTID {
		// for rawMode(true), we only parse FormatDescriptionEvent and RotateEvent
		// if not need to support GTID mode, we can enable rawMode
		syncerCfg.RawModeEnabled = true
	}

	r.syncerCfg = syncerCfg

	log.Info("[relay] relay unit is updated")

	return nil
}

// setActiveRelayLog sets or updates the current active relay log to file
func (r *Relay) setActiveRelayLog(filename string) {
	uuid := r.meta.UUID()
	_, suffix, _ := utils.ParseSuffixForUUID(uuid)
	rli := &pkgstreamer.RelayLogInfo{
		TaskName:   fakeTaskName,
		UUID:       uuid,
		UUIDSuffix: suffix,
		Filename:   filename,
	}
	r.activeRelayLog.Lock()
	r.activeRelayLog.info = rli
	r.activeRelayLog.Unlock()
}

// ActiveRelayLog returns the current active RelayLogInfo
func (r *Relay) ActiveRelayLog() *pkgstreamer.RelayLogInfo {
	r.activeRelayLog.RLock()
	defer r.activeRelayLog.RUnlock()
	return r.activeRelayLog.info
}

// Migrate reset binlog pos and name, create sub dir
func (r *Relay) Migrate(ctx context.Context, binlogName string, binlogPos uint32) error {
	r.Lock()
	defer r.Unlock()
	uuid, err := utils.GetServerUUID(r.db, r.cfg.Flavor)
	if err != nil {
		return errors.Trace(err)
	}
	err = r.meta.AddDir(uuid, &mysql.Position{Name: binlogName, Pos: binlogPos}, nil)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}
