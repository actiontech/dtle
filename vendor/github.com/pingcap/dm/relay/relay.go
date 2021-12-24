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
	"crypto/tls"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	toolutils "github.com/pingcap/tidb-tools/pkg/utils"
	"github.com/pingcap/tidb/parser"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/dm/unit"
	"github.com/pingcap/dm/pkg/binlog"
	"github.com/pingcap/dm/pkg/binlog/common"
	binlogReader "github.com/pingcap/dm/pkg/binlog/reader"
	"github.com/pingcap/dm/pkg/conn"
	"github.com/pingcap/dm/pkg/gtid"
	"github.com/pingcap/dm/pkg/log"
	pkgstreamer "github.com/pingcap/dm/pkg/streamer"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/utils"
	"github.com/pingcap/dm/relay/reader"
	"github.com/pingcap/dm/relay/retry"
	"github.com/pingcap/dm/relay/transformer"
	"github.com/pingcap/dm/relay/writer"
)

// used to fill RelayLogInfo.
var fakeTaskName = "relay"

const (
	flushMetaInterval           = 30 * time.Second
	getMasterStatusInterval     = 30 * time.Second
	trimUUIDsInterval           = 1 * time.Hour
	showStatusConnectionTimeout = "1m"

	// dumpFlagSendAnnotateRowsEvent (BINLOG_SEND_ANNOTATE_ROWS_EVENT) request the MariaDB master to send Annotate_rows_log_event back.
	dumpFlagSendAnnotateRowsEvent uint16 = 0x02
)

// NewRelay creates an instance of Relay.
var NewRelay = NewRealRelay

var _ Process = &Relay{}

// Process defines mysql-like relay log process unit.
type Process interface {
	// Init initial relat log unit
	Init(ctx context.Context) (err error)
	// Process run background logic of relay log unit
	Process(ctx context.Context) pb.ProcessResult
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
	// Status returns status of relay log process unit.
	Status(sourceStatus *binlog.SourceStatus) interface{}
	// Close does some clean works
	Close()
	// IsClosed returns whether relay log process unit was closed
	IsClosed() bool
	// SaveMeta save relay meta
	SaveMeta(pos mysql.Position, gset gtid.Set) error
	// ResetMeta reset relay meta
	ResetMeta()
	// PurgeRelayDir will clear all contents under w.cfg.RelayDir
	PurgeRelayDir() error

	GetMeta() Meta
}

// Relay relays mysql binlog to local file.
type Relay struct {
	db        *conn.BaseDB
	cfg       *Config
	syncerCfg replication.BinlogSyncerConfig

	meta   Meta
	closed atomic.Bool
	sync.RWMutex

	logger log.Logger

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
	return &Relay{
		cfg:    cfg,
		meta:   NewLocalMeta(cfg.Flavor, cfg.RelayDir),
		logger: log.With(zap.String("component", "relay log")),
	}
}

// Init implements the dm.Unit interface.
// NOTE when Init encounters an error, it will make DM-worker exit when it boots up and assigned relay.
func (r *Relay) Init(ctx context.Context) (err error) {
	return reportRelayLogSpaceInBackground(ctx, r.cfg.RelayDir)
}

// Process implements the dm.Unit interface.
func (r *Relay) Process(ctx context.Context) pb.ProcessResult {
	errs := make([]*pb.ProcessError, 0, 1)
	err := r.process(ctx)
	if err != nil && errors.Cause(err) != replication.ErrSyncClosed {
		relayExitWithErrorCounter.Inc()
		r.logger.Error("process exit", zap.Error(err))
		// TODO: add specified error type instead of pb.ErrorType_UnknownError
		errs = append(errs, unit.NewProcessError(err))
	}

	isCanceled := false
	if len(errs) == 0 {
		select {
		case <-ctx.Done():
			isCanceled = true
		default:
		}
	}
	return pb.ProcessResult{
		IsCanceled: isCanceled,
		Errors:     errs,
	}
}

func (r *Relay) process(ctx context.Context) error {
	err := r.setSyncConfig()
	if err != nil {
		return err
	}

	db, err := conn.DefaultDBProvider.Apply(r.cfg.From)
	if err != nil {
		return terror.WithScope(err, terror.ScopeUpstream)
	}
	r.db = db

	if err2 := os.MkdirAll(r.cfg.RelayDir, 0o755); err2 != nil {
		return terror.ErrRelayMkdir.Delegate(err2)
	}

	err = r.meta.Load()
	if err != nil {
		return err
	}

	parser2, err := utils.GetParser(ctx, r.db.DB) // refine to use user config later
	if err != nil {
		return err
	}

	isNew, err := isNewServer(ctx, r.meta.UUID(), r.db.DB, r.cfg.Flavor)
	if err != nil {
		return err
	}

	failpoint.Inject("NewUpstreamServer", func(_ failpoint.Value) {
		// test a bug which caused by upstream switching
		isNew = true
	})

	if isNew {
		// re-setup meta for new server or new source
		err = r.reSetupMeta(ctx)
		if err != nil {
			return err
		}
	} else {
		// connected to last source
		r.updateMetricsRelaySubDirIndex()
		// if not a new server, try to recover the latest relay log file.
		err = r.tryRecoverLatestFile(ctx, parser2)
		if err != nil {
			return err
		}

		// resuming will take the risk that upstream has purge the binlog relay is needed.
		// when this worker is down, HA may schedule the source to other workers and forward the sync progress,
		// and then when the source is scheduled back to this worker, we could start relay from sync checkpoint's
		// location which is newer, and now could purge the outdated relay logs.
		//
		// locations in `r.cfg` is set to min needed location of subtasks (higher priority) or source config specified
		isRelayMetaOutdated := false
		neededBinlogName := r.cfg.BinLogName
		neededBinlogGset, err2 := gtid.ParserGTID(r.cfg.Flavor, r.cfg.BinlogGTID)
		if err2 != nil {
			return err2
		}
		if r.cfg.EnableGTID {
			_, metaGset := r.meta.GTID()
			if neededBinlogGset.Contain(metaGset) && !neededBinlogGset.Equal(metaGset) {
				isRelayMetaOutdated = true
			}
		} else {
			_, metaPos := r.meta.Pos()
			if neededBinlogName > metaPos.Name {
				isRelayMetaOutdated = true
			}
		}

		if isRelayMetaOutdated {
			uuidWithSuffix := r.meta.UUID() // only change after switch
			err2 = r.PurgeRelayDir()
			if err2 != nil {
				return err2
			}
			r.ResetMeta()

			uuid, _, err3 := utils.ParseSuffixForUUID(uuidWithSuffix)
			if err3 != nil {
				r.logger.Error("parse suffix for UUID when relay meta outdated", zap.String("UUID", uuidWithSuffix), zap.Error(err))
				return err3
			}

			pos := &mysql.Position{Name: neededBinlogName, Pos: binlog.MinPosition.Pos}
			err = r.meta.AddDir(uuid, pos, neededBinlogGset, r.cfg.UUIDSuffix)
			if err != nil {
				return err
			}
			err = r.meta.Load()
			if err != nil {
				return err
			}
		}
	}

	reader2, err := r.setUpReader(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if reader2 != nil {
			err = reader2.Close()
			if err != nil {
				r.logger.Error("fail to close binlog event reader", zap.Error(err))
			}
		}
	}()

	writer2, err := r.setUpWriter(parser2)
	if err != nil {
		return err
	}
	defer func() {
		err = writer2.Close()
		if err != nil {
			r.logger.Error("fail to close binlog event writer", zap.Error(err))
		}
	}()

	readerRetry, err := retry.NewReaderRetry(r.cfg.ReaderRetry)
	if err != nil {
		return err
	}

	transformer2 := transformer.NewTransformer(parser2)

	go r.doIntervalOps(ctx)

	// handles binlog events with retry mechanism.
	// it only do the retry for some binlog reader error now.
	for {
		eventIdx, err := r.handleEvents(ctx, reader2, transformer2, writer2)
	checkError:
		if err == nil {
			return nil
		} else if !readerRetry.Check(ctx, err) {
			return err
		}

		r.logger.Warn("receive retryable error for binlog reader", log.ShortError(err))
		err = reader2.Close() // close the previous reader
		if err != nil {
			r.logger.Error("fail to close binlog event reader", zap.Error(err))
		}
		reader2, err = r.setUpReader(ctx) // setup a new one
		if err != nil {
			return err
		}
		r.logger.Info("retrying to read binlog")
		if r.cfg.EnableGTID && eventIdx > 0 {
			// check if server has switched
			isNew, err2 := isNewServer(ctx, r.meta.UUID(), r.db.DB, r.cfg.Flavor)
			// should start from the transaction beginning when switch to a new server
			if err2 != nil {
				r.logger.Warn("check new server failed, continue outer loop", log.ShortError(err2))
				err = err2
				goto checkError
			}
			if !isNew {
				for i := 0; i < eventIdx; {
					res, err2 := reader2.GetEvent(ctx)
					if err2 != nil {
						err = err2
						goto checkError
					}
					tResult := transformer2.Transform(res.Event)
					// do not count skip event
					if !tResult.Ignore {
						i++
					}
				}
				if eventIdx > 0 {
					r.logger.Info("discard duplicate event", zap.Int("count", eventIdx))
				}
			}
		}
	}
}

// PurgeRelayDir implements the dm.Unit interface.
func (r *Relay) PurgeRelayDir() error {
	dir := r.cfg.RelayDir
	d, err := os.Open(dir)
	r.logger.Info("will try purge whole relay dir for new relay log", zap.String("relayDir", dir))
	// fail to open dir, return directly
	if err != nil {
		if err == os.ErrNotExist {
			return nil
		}
		return err
	}
	defer d.Close()
	names, err := d.Readdirnames(-1)
	if err != nil {
		return err
	}
	for _, name := range names {
		err = os.RemoveAll(filepath.Join(dir, name))
		if err != nil {
			return err
		}
	}
	r.logger.Info("relay dir is purged to be ready for new relay log", zap.String("relayDir", dir))
	return nil
}

// tryRecoverLatestFile tries to recover latest relay log file with corrupt/incomplete binlog events/transactions.
func (r *Relay) tryRecoverLatestFile(ctx context.Context, parser2 *parser.Parser) error {
	var (
		uuid, latestPos = r.meta.Pos()
		_, latestGTID   = r.meta.GTID()
	)

	if latestPos.Compare(minCheckpoint) <= 0 {
		r.logger.Warn("no relay log file need to recover", zap.Stringer("position", latestPos), log.WrapStringerField("gtid set", latestGTID))
		return nil
	}

	// setup a special writer to do the recovering
	cfg := &writer.FileConfig{
		RelayDir: r.meta.Dir(),
		Filename: latestPos.Name,
	}
	writer2 := writer.NewFileWriter(r.logger, cfg, parser2)
	err := writer2.Start()
	if err != nil {
		return terror.Annotatef(err, "start recover writer for UUID %s with config %+v", uuid, cfg)
	}
	defer func() {
		err2 := writer2.Close()
		if err2 != nil {
			r.logger.Error("fail to close recover writer", zap.String("UUID", uuid), zap.Reflect("config", cfg), log.ShortError(err2))
		}
	}()
	r.logger.Info("started recover writer", zap.String("UUID", uuid), zap.Reflect("config", cfg))

	// NOTE: recover a relay log file with too many binlog events may take a little long time.
	result, err := writer2.Recover(ctx)
	if err == nil {
		relayLogHasMore := result.LatestPos.Compare(latestPos) > 0 ||
			(result.LatestGTIDs != nil && !result.LatestGTIDs.Equal(latestGTID) && result.LatestGTIDs.Contain(latestGTID))

		if result.Truncated || relayLogHasMore {
			r.logger.Warn("relay log file recovered",
				zap.Stringer("from position", latestPos), zap.Stringer("to position", result.LatestPos), log.WrapStringerField("from GTID set", latestGTID), log.WrapStringerField("to GTID set", result.LatestGTIDs))

			if result.LatestGTIDs != nil {
				dbConn, err2 := r.db.DB.Conn(ctx)
				if err2 != nil {
					return err2
				}
				defer dbConn.Close()
				result.LatestGTIDs, err2 = utils.AddGSetWithPurged(ctx, result.LatestGTIDs, dbConn)
				if err2 != nil {
					return err2
				}
				latestGTID, err2 = utils.AddGSetWithPurged(ctx, latestGTID, dbConn)
				if err2 != nil {
					return err2
				}
			}

			if result.LatestGTIDs != nil && !result.LatestGTIDs.Equal(latestGTID) && result.LatestGTIDs.Contain(latestGTID) {
				r.logger.Warn("some GTIDs are missing in the meta data, this is usually due to the process was interrupted while writing the meta data. force to update GTIDs",
					log.WrapStringerField("from GTID set", latestGTID), log.WrapStringerField("to GTID set", result.LatestGTIDs))
				latestGTID = result.LatestGTIDs.Clone()
			} else if err = latestGTID.Truncate(result.LatestGTIDs); err != nil {
				return err
			}
			err = r.SaveMeta(result.LatestPos, latestGTID)
			if err != nil {
				return terror.Annotatef(err, "save position %s, GTID sets %v after recovered", result.LatestPos, result.LatestGTIDs)
			}
		}
	}
	return terror.Annotatef(err, "recover for UUID %s with config %+v", uuid, cfg)
}

// handleEvents handles binlog events, including:
//   1. read events from upstream
//   2. transform events
//   3. write events into relay log files
//   4. update metadata if needed
// the first return value is the index of last read rows event if the transaction is not finished.
func (r *Relay) handleEvents(
	ctx context.Context,
	reader2 reader.Reader,
	transformer2 transformer.Transformer,
	writer2 writer.Writer,
) (int, error) {
	var (
		_, lastPos  = r.meta.Pos()
		_, lastGTID = r.meta.GTID()
		err         error
		eventIndex  int
	)
	if lastGTID == nil {
		if lastGTID, err = gtid.ParserGTID(r.cfg.Flavor, ""); err != nil {
			return 0, err
		}
	}

	for {
		// 1. read events from upstream server
		readTimer := time.Now()
		rResult, err := reader2.GetEvent(ctx)
		failpoint.Inject("RelayGetEventFailed", func(v failpoint.Value) {
			if intVal, ok := v.(int); ok && intVal == eventIndex {
				err = errors.New("fail point triggered")
				_, gtid := r.meta.GTID()
				r.logger.Warn("failed to get event", zap.Int("event_index", eventIndex),
					zap.Any("gtid", gtid), log.ShortError(err))
				// wait backoff retry interval
				time.Sleep(1 * time.Second)
			}
		})
		if err != nil {
			switch errors.Cause(err) {
			case context.Canceled:
				return 0, nil
			case replication.ErrChecksumMismatch:
				relayLogDataCorruptionCounter.Inc()
			case replication.ErrSyncClosed, replication.ErrNeedSyncAgain:
				// do nothing, but the error will be returned
			default:
				if utils.IsErrBinlogPurged(err) {
					// TODO: try auto fix GTID, and can support auto switching between upstream server later.
					cfg := r.cfg.From
					r.logger.Error("the requested binlog files have purged in the master server or the master server have switched, currently DM do no support to handle this error",
						zap.String("db host", cfg.Host), zap.Int("db port", cfg.Port), zap.Stringer("last pos", lastPos), log.ShortError(err))
					// log the status for debug
					pos, gs, err2 := utils.GetMasterStatus(ctx, r.db.DB, r.cfg.Flavor)
					if err2 == nil {
						r.logger.Info("current master status", zap.Stringer("position", pos), log.WrapStringerField("GTID sets", gs))
					}
				}
				binlogReadErrorCounter.Inc()
			}
			return eventIndex, err
		}

		binlogReadDurationHistogram.Observe(time.Since(readTimer).Seconds())
		failpoint.Inject("BlackholeReadBinlog", func(_ failpoint.Value) {
			// r.logger.Info("back hole read binlog takes effects")
			failpoint.Continue()
		})

		e := rResult.Event
		r.logger.Debug("receive binlog event with header", zap.Reflect("header", e.Header))

		// 2. transform events
		transformTimer := time.Now()
		tResult := transformer2.Transform(e)
		binlogTransformDurationHistogram.Observe(time.Since(transformTimer).Seconds())
		if len(tResult.NextLogName) > 0 && tResult.NextLogName > lastPos.Name {
			lastPos = mysql.Position{
				Name: tResult.NextLogName,
				Pos:  tResult.LogPos,
			}
			r.logger.Info("rotate event", zap.Stringer("position", lastPos))
		}
		if tResult.Ignore {
			r.logger.Info("ignore event by transformer",
				zap.Reflect("header", e.Header),
				zap.String("reason", tResult.IgnoreReason))
			continue
		}

		if _, ok := e.Event.(*replication.RotateEvent); ok && utils.IsFakeRotateEvent(e.Header) {
			isNew, err2 := isNewServer(ctx, r.meta.UUID(), r.db.DB, r.cfg.Flavor)
			// should start from the transaction beginning when switch to a new server
			if err2 != nil {
				return 0, err2
			}
			// upstream database switch
			// report an error, let outer logic handle it
			// should start from the transaction beginning when switch to a new server
			if isNew {
				return 0, terror.ErrRotateEventWithDifferentServerID.Generate()
			}
		}

		// 3. save events into file
		writeTimer := time.Now()
		r.logger.Debug("writing binlog event", zap.Reflect("header", e.Header))
		wResult, err := writer2.WriteEvent(e)
		if err != nil {
			relayLogWriteErrorCounter.Inc()
			return eventIndex, err
		} else if wResult.Ignore {
			r.logger.Info("ignore event by writer",
				zap.Reflect("header", e.Header),
				zap.String("reason", wResult.IgnoreReason))
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
			return 0, terror.ErrRelayUpdateGTID.Delegate(err, lastGTID, tResult.GTIDSet)
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
			r.logger.Error("parse binlog file name", zap.String("file name", lastPos.Name), log.ShortError(err2))
		} else {
			relayLogFileGauge.WithLabelValues("relay").Set(float64(index))
		}

		if needSavePos {
			err = r.SaveMeta(lastPos, lastGTID)
			if err != nil {
				return 0, terror.Annotatef(err, "save position %s, GTID sets %v into meta", lastPos, lastGTID)
			}
			eventIndex = 0
		} else {
			eventIndex++
		}
		if tResult.NextLogName != "" && !utils.IsFakeRotateEvent(e.Header) {
			// if the binlog is rotated, we need to save and flush the next binlog filename to meta
			lastPos.Name = tResult.NextLogName
			if err := r.SaveMeta(lastPos, lastGTID); err != nil {
				return 0, terror.Annotatef(err, "save position %s, GTID sets %v into meta", lastPos, lastGTID)
			}
			if err := r.FlushMeta(); err != nil {
				return 0, err
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
		r.logger.Info("change the active relay log file", zap.String("file name", filename))
	}
}

// reSetupMeta re-setup the metadata when switching to a new upstream master server.
func (r *Relay) reSetupMeta(ctx context.Context) error {
	uuid, err := utils.GetServerUUID(ctx, r.db.DB, r.cfg.Flavor)
	if err != nil {
		return err
	}

	var newPos *mysql.Position
	var newGset gtid.Set
	var newUUIDSuffix int
	if r.cfg.UUIDSuffix > 0 {
		// if bound or rebound to a source, clear all relay log and meta
		if err = r.PurgeRelayDir(); err != nil {
			return err
		}
		r.ResetMeta()

		newUUIDSuffix = r.cfg.UUIDSuffix
		// reset the UUIDSuffix
		r.cfg.UUIDSuffix = 0

		if len(r.cfg.BinLogName) != 0 {
			newPos = &mysql.Position{Name: r.cfg.BinLogName, Pos: binlog.MinPosition.Pos}
		}
		if len(r.cfg.BinlogGTID) != 0 {
			newGset, err = gtid.ParserGTID(r.cfg.Flavor, r.cfg.BinlogGTID)
			if err != nil {
				return err
			}
		}
	}
	err = r.meta.AddDir(uuid, newPos, newGset, newUUIDSuffix)
	if err != nil {
		return err
	}
	err = r.meta.Load()
	if err != nil {
		return err
	}

	var latestPosName, latestGTIDStr string
	if (r.cfg.EnableGTID && len(r.cfg.BinlogGTID) == 0) || (!r.cfg.EnableGTID && len(r.cfg.BinLogName) == 0) {
		latestPos, latestGTID, err2 := utils.GetMasterStatus(ctx, r.db.DB, r.cfg.Flavor)
		if err2 != nil {
			return err2
		}
		latestPosName = latestPos.Name
		latestGTIDStr = latestGTID.String()
	}

	// try adjust meta with start pos from config
	_, err = r.meta.AdjustWithStartPos(r.cfg.BinLogName, r.cfg.BinlogGTID, r.cfg.EnableGTID, latestPosName, latestGTIDStr)
	if err != nil {
		return err
	}

	_, pos := r.meta.Pos()
	_, gs := r.meta.GTID()
	if r.cfg.EnableGTID {
		// Adjust given gtid
		// This means we always pull the binlog from the beginning of file.
		gs, err = r.adjustGTID(ctx, gs)
		if err != nil {
			return terror.Annotate(err, "fail to adjust gtid for relay")
		}
		err = r.SaveMeta(pos, gs)
		if err != nil {
			return err
		}
	}

	r.logger.Info("adjusted meta to start pos", zap.Any("start pos", pos), zap.Stringer("start pos's binlog gtid", gs))
	r.updateMetricsRelaySubDirIndex()
	r.logger.Info("resetup meta", zap.String("uuid", uuid))

	return nil
}

func (r *Relay) updateMetricsRelaySubDirIndex() {
	// when switching master server, update sub dir index metrics
	node := r.masterNode()
	uuidWithSuffix := r.meta.UUID() // only change after switch
	_, suffix, err := utils.ParseSuffixForUUID(uuidWithSuffix)
	if err != nil {
		r.logger.Error("parse suffix for UUID", zap.String("UUID", uuidWithSuffix), zap.Error(err))
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
			r.RLock()
			if r.closed.Load() {
				r.RUnlock()
				return
			}
			if r.meta.Dirty() {
				err := r.FlushMeta()
				if err != nil {
					r.logger.Error("flush meta", zap.Error(err))
				} else {
					r.logger.Info("flush meta finished", zap.Stringer("meta", r.meta))
				}
			}
			r.RUnlock()
		case <-masterStatusTicker.C:
			r.RLock()
			if r.closed.Load() {
				r.RUnlock()
				return
			}
			ctx2, cancel2 := context.WithTimeout(ctx, utils.DefaultDBTimeout)
			pos, _, err := utils.GetMasterStatus(ctx2, r.db.DB, r.cfg.Flavor)
			cancel2()
			if err != nil {
				r.logger.Warn("get master status", zap.Error(err))
				r.RUnlock()
				continue
			}
			index, err := binlog.GetFilenameIndex(pos.Name)
			if err != nil {
				r.logger.Error("parse binlog file name", zap.String("file name", pos.Name), log.ShortError(err))
				r.RUnlock()
				continue
			}
			relayLogFileGauge.WithLabelValues("master").Set(float64(index))
			relayLogPosGauge.WithLabelValues("master").Set(float64(pos.Pos))
			r.RUnlock()
		case <-trimUUIDsTicker.C:
			r.RLock()
			if r.closed.Load() {
				r.RUnlock()
				return
			}
			trimmed, err := r.meta.TrimUUIDs()
			if err != nil {
				r.logger.Error("trim UUIDs", zap.Error(err))
			} else if len(trimmed) > 0 {
				r.logger.Info("trim UUIDs", zap.String("UUIDs", strings.Join(trimmed, ";")))
			}
			r.RUnlock()
		case <-ctx.Done():
			return
		}
	}
}

// setUpReader setups the underlying reader used to read binlog events from the upstream master server.
func (r *Relay) setUpReader(ctx context.Context) (reader.Reader, error) {
	ctx2, cancel := context.WithTimeout(ctx, utils.DefaultDBTimeout)
	defer cancel()

	// always use a new random serverID
	randomServerID, err := utils.GetRandomServerID(ctx2, r.db.DB)
	if err != nil {
		// should never happened unless the master has too many slave
		return nil, terror.Annotate(err, "fail to get random server id for relay reader")
	}
	r.syncerCfg.ServerID = randomServerID
	r.cfg.ServerID = randomServerID

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
	err = reader2.Start()
	if err != nil {
		// do not log the whole config to protect the password in `SyncConfig`.
		// and other config items should already logged before or included in `err`.
		return nil, terror.Annotatef(err, "start reader for UUID %s", uuid)
	}

	r.logger.Info("started underlying reader", zap.String("UUID", uuid))
	return reader2, nil
}

// setUpWriter setups the underlying writer used to writer binlog events into file or other places.
func (r *Relay) setUpWriter(parser2 *parser.Parser) (writer.Writer, error) {
	uuid, pos := r.meta.Pos()
	cfg := &writer.FileConfig{
		RelayDir: r.meta.Dir(),
		Filename: pos.Name,
	}
	writer2 := writer.NewFileWriter(r.logger, cfg, parser2)
	if err := writer2.Start(); err != nil {
		return nil, terror.Annotatef(err, "start writer for UUID %s with config %+v", uuid, cfg)
	}

	r.logger.Info("started underlying writer", zap.String("UUID", uuid), zap.Reflect("config", cfg))
	return writer2, nil
}

func (r *Relay) masterNode() string {
	return fmt.Sprintf("%s:%d", r.cfg.From.Host, r.cfg.From.Port)
}

// IsClosed tells whether Relay unit is closed or not.
func (r *Relay) IsClosed() bool {
	return r.closed.Load()
}

// SaveMeta save relay meta and update meta in RelayLogInfo.
func (r *Relay) SaveMeta(pos mysql.Position, gset gtid.Set) error {
	return r.meta.Save(pos, gset)
}

// ResetMeta reset relay meta.
func (r *Relay) ResetMeta() {
	r.Lock()
	defer r.Unlock()
	r.meta = NewLocalMeta(r.cfg.Flavor, r.cfg.RelayDir)
}

// FlushMeta flush relay meta.
func (r *Relay) FlushMeta() error {
	return r.meta.Flush()
}

// stopSync stops syncing, now it used by Close and Pause.
func (r *Relay) stopSync() {
	if err := r.FlushMeta(); err != nil {
		r.logger.Error("flush checkpoint", zap.Error(err))
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
	if r.closed.Load() {
		return
	}
	r.logger.Info("relay unit is closing")

	r.stopSync()

	r.closeDB()

	r.closed.Store(true)
	r.logger.Info("relay unit closed")
}

// Status implements the dm.Unit interface.
func (r *Relay) Status(sourceStatus *binlog.SourceStatus) interface{} {
	r.RLock()
	defer r.RUnlock()
	uuid, relayPos := r.meta.Pos()

	rs := &pb.RelayStatus{
		RelaySubDir: uuid,
		RelayBinlog: relayPos.String(),
	}
	if _, relayGTIDSet := r.meta.GTID(); relayGTIDSet != nil {
		rs.RelayBinlogGtid = relayGTIDSet.String()
	}

	if sourceStatus != nil {
		masterPos, masterGTID := sourceStatus.Location.Position, sourceStatus.Location.GetGTID()
		rs.MasterBinlog = masterPos.String()
		if masterGTID != nil { // masterGTID maybe a nil interface
			rs.MasterBinlogGtid = masterGTID.String()
		}

		if r.cfg.EnableGTID {
			// rely on sorted GTID set when String()
			rs.RelayCatchUpMaster = rs.MasterBinlogGtid == rs.RelayBinlogGtid
		} else {
			rs.RelayCatchUpMaster = masterPos.Compare(relayPos) == 0
		}
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

// IsFreshTask implements Unit.IsFreshTask.
func (r *Relay) IsFreshTask() (bool, error) {
	return true, nil
}

// Pause pauses the process, it can be resumed later.
func (r *Relay) Pause() {
	if r.IsClosed() {
		r.logger.Warn("try to pause, but already closed")
		return
	}

	r.stopSync()
}

// Resume resumes the paused process.
func (r *Relay) Resume(ctx context.Context, pr chan pb.ProcessResult) {
	// do nothing now, re-process called `Process` from outer directly
}

// Update implements Unit.Update.
func (r *Relay) Update(cfg *config.SubTaskConfig) error {
	// not support update configuration now
	return nil
}

// Reload updates config.
func (r *Relay) Reload(newCfg *Config) error {
	r.Lock()
	defer r.Unlock()
	r.logger.Info("relay unit is updating")

	// Update From
	r.cfg.From = newCfg.From

	// Update AutoFixGTID
	r.cfg.AutoFixGTID = newCfg.AutoFixGTID

	// Update Charset
	r.cfg.Charset = newCfg.Charset

	r.closeDB()
	if r.cfg.From.RawDBCfg == nil {
		r.cfg.From.RawDBCfg = config.DefaultRawDBConfig()
	}
	r.cfg.From.RawDBCfg.ReadTimeout = showStatusConnectionTimeout
	db, err := conn.DefaultDBProvider.Apply(r.cfg.From)
	if err != nil {
		return terror.WithScope(terror.DBErrorAdapt(err, terror.ErrDBDriverError), terror.ScopeUpstream)
	}
	r.db = db

	if err := r.setSyncConfig(); err != nil {
		return err
	}

	r.logger.Info("relay unit is updated")

	return nil
}

// setActiveRelayLog sets or updates the current active relay log to file.
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

// ActiveRelayLog returns the current active RelayLogInfo.
func (r *Relay) ActiveRelayLog() *pkgstreamer.RelayLogInfo {
	r.activeRelayLog.RLock()
	defer r.activeRelayLog.RUnlock()
	return r.activeRelayLog.info
}

func (r *Relay) setSyncConfig() error {
	var tlsConfig *tls.Config
	var err error
	if r.cfg.From.Security != nil {
		if loadErr := r.cfg.From.Security.LoadTLSContent(); loadErr != nil {
			return terror.ErrCtlLoadTLSCfg.Delegate(loadErr)
		}
		tlsConfig, err = toolutils.ToTLSConfigWithVerifyByRawbytes(r.cfg.From.Security.SSLCABytes,
			r.cfg.From.Security.SSLCertBytes, r.cfg.From.Security.SSLKEYBytes, r.cfg.From.Security.CertAllowedCN)
		if err != nil {
			return terror.ErrConnInvalidTLSConfig.Delegate(err)
		}
		if tlsConfig != nil {
			tlsConfig.InsecureSkipVerify = true
		}
	}

	syncerCfg := replication.BinlogSyncerConfig{
		ServerID:  r.cfg.ServerID,
		Flavor:    r.cfg.Flavor,
		Host:      r.cfg.From.Host,
		Port:      uint16(r.cfg.From.Port),
		User:      r.cfg.From.User,
		Password:  r.cfg.From.Password,
		Charset:   r.cfg.Charset,
		TLSConfig: tlsConfig,
	}
	common.SetDefaultReplicationCfg(&syncerCfg, common.MaxBinlogSyncerReconnect)

	if !r.cfg.EnableGTID {
		syncerCfg.RawModeEnabled = true
	}

	if r.cfg.Flavor == mysql.MariaDBFlavor {
		syncerCfg.DumpCommandFlag |= dumpFlagSendAnnotateRowsEvent
	}

	r.syncerCfg = syncerCfg
	return nil
}

// AdjustGTID implements Relay.AdjustGTID
// starting sync at returned gset will wholly fetch a binlog from beginning of the file.
func (r *Relay) adjustGTID(ctx context.Context, gset gtid.Set) (gtid.Set, error) {
	// setup a TCP binlog reader (because no relay can be used when upgrading).
	syncCfg := r.syncerCfg
	// always use a new random serverID
	randomServerID, err := utils.GetRandomServerID(ctx, r.db.DB)
	if err != nil {
		return nil, terror.Annotate(err, "fail to get random server id when relay adjust gtid")
	}
	syncCfg.ServerID = randomServerID
	tcpReader := binlogReader.NewTCPReader(syncCfg)
	resultGs, err := binlogReader.GetPreviousGTIDFromGTIDSet(ctx, tcpReader, gset)
	if err != nil {
		return nil, err
	}

	dbConn, err2 := r.db.DB.Conn(ctx)
	if err2 != nil {
		return nil, err2
	}
	defer dbConn.Close()
	return utils.AddGSetWithPurged(ctx, resultGs, dbConn)
}
