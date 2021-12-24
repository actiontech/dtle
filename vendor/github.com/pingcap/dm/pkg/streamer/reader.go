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

package streamer

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"go.uber.org/zap"

	"github.com/pingcap/dm/pkg/binlog"
	"github.com/pingcap/dm/pkg/binlog/event"
	"github.com/pingcap/dm/pkg/binlog/reader"
	tcontext "github.com/pingcap/dm/pkg/context"
	"github.com/pingcap/dm/pkg/gtid"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/utils"
)

// ErrorMaybeDuplicateEvent indicates that there may be duplicate event in next binlog file
// this is mainly happened when upstream master changed when relay log not finish reading a transaction.
var ErrorMaybeDuplicateEvent = errors.New("truncate binlog file found, event may be duplicated")

// Meta represents binlog meta information in relay.meta.
type Meta struct {
	BinLogName string `toml:"binlog-name" json:"binlog-name"`
	BinLogPos  uint32 `toml:"binlog-pos" json:"binlog-pos"`
	BinlogGTID string `toml:"binlog-gtid" json:"binlog-gtid"`
	UUID       string `toml:"-" json:"-"`
}

// polling interval for watcher.
var watcherInterval = 100 * time.Millisecond

// BinlogReaderConfig is the configuration for BinlogReader.
type BinlogReaderConfig struct {
	RelayDir string
	Timezone *time.Location
	Flavor   string
}

// BinlogReader is a binlog reader.
type BinlogReader struct {
	cfg    *BinlogReaderConfig
	parser *replication.BinlogParser

	indexPath string   // relay server-uuid index file path
	uuids     []string // master UUIDs (relay sub dir)

	latestServerID uint32 // latest server ID, got from relay log

	running bool
	wg      sync.WaitGroup
	cancel  context.CancelFunc

	tctx *tcontext.Context

	usingGTID          bool
	prevGset, currGset mysql.GTIDSet
}

// NewBinlogReader creates a new BinlogReader.
func NewBinlogReader(logger log.Logger, cfg *BinlogReaderConfig) *BinlogReader {
	ctx, cancel := context.WithCancel(context.Background()) // only can be canceled in `Close`
	parser := replication.NewBinlogParser()
	parser.SetVerifyChecksum(true)
	// use string representation of decimal, to replicate the exact value
	parser.SetUseDecimal(false)
	if cfg.Timezone != nil {
		parser.SetTimestampStringLocation(cfg.Timezone)
	}

	newtctx := tcontext.NewContext(ctx, logger.WithFields(zap.String("component", "binlog reader")))

	return &BinlogReader{
		cfg:       cfg,
		parser:    parser,
		indexPath: path.Join(cfg.RelayDir, utils.UUIDIndexFilename),
		cancel:    cancel,
		tctx:      newtctx,
	}
}

// checkRelayPos will check whether the given relay pos is too big.
func (r *BinlogReader) checkRelayPos(pos mysql.Position) error {
	currentUUID, _, realPos, err := binlog.ExtractPos(pos, r.uuids)
	if err != nil {
		return terror.Annotatef(err, "parse relay dir with pos %s", pos)
	}
	pos = realPos
	relayFilepath := path.Join(r.cfg.RelayDir, currentUUID, pos.Name)
	r.tctx.L().Info("start to check relay log file", zap.String("path", relayFilepath), zap.Stringer("position", pos))
	fi, err := os.Stat(relayFilepath)
	if err != nil {
		return terror.ErrGetRelayLogStat.Delegate(err, relayFilepath)
	}
	if fi.Size() < int64(pos.Pos) {
		return terror.ErrRelayLogGivenPosTooBig.Generate(pos)
	}
	return nil
}

// IsGTIDCoverPreviousFiles check whether gset contains file's previous_gset.
func (r *BinlogReader) IsGTIDCoverPreviousFiles(ctx context.Context, filePath string, gset mysql.GTIDSet) (bool, error) {
	fileReader := reader.NewFileReader(&reader.FileReaderConfig{Timezone: r.cfg.Timezone})
	defer fileReader.Close()
	err := fileReader.StartSyncByPos(mysql.Position{Name: filePath, Pos: 4})
	if err != nil {
		return false, err
	}

	var gs gtid.Set

	for {
		select {
		case <-ctx.Done():
			return false, nil
		default:
		}

		ctx2, cancel := context.WithTimeout(ctx, time.Second)
		e, err := fileReader.GetEvent(ctx2)
		cancel()
		if err != nil {
			// reach end of file
			// Maybe we can only Parse the first three fakeRotate, Format_desc and Previous_gtids events.
			if terror.ErrReaderReachEndOfFile.Equal(err) {
				return false, terror.ErrPreviousGTIDNotExist.Generate(filePath)
			}
			return false, err
		}

		switch {
		case e.Header.EventType == replication.PREVIOUS_GTIDS_EVENT:
			gs, err = event.GTIDsFromPreviousGTIDsEvent(e)
		case e.Header.EventType == replication.MARIADB_GTID_LIST_EVENT:
			gs, err = event.GTIDsFromMariaDBGTIDListEvent(e)
		default:
			continue
		}

		if err != nil {
			return false, err
		}
		return gset.Contain(gs.Origin()), nil
	}
}

// getPosByGTID gets file position by gtid, result should be (filename, 4).
func (r *BinlogReader) getPosByGTID(gset mysql.GTIDSet) (*mysql.Position, error) {
	// start from newest uuid dir
	for i := len(r.uuids) - 1; i >= 0; i-- {
		uuid := r.uuids[i]
		_, suffix, err := utils.ParseSuffixForUUID(uuid)
		if err != nil {
			return nil, err
		}

		uuidDir := path.Join(r.cfg.RelayDir, uuid)
		allFiles, err := CollectAllBinlogFiles(uuidDir)
		if err != nil {
			return nil, err
		}

		// iterate files from the newest one
		for i := len(allFiles) - 1; i >= 0; i-- {
			file := allFiles[i]
			filePath := path.Join(uuidDir, file)
			// if input `gset` not contain previous_gtids_event's gset (complementary set of `gset` overlap with
			// previous_gtids_event), that means there're some needed events in previous files.
			// so we go to previous one
			contain, err := r.IsGTIDCoverPreviousFiles(r.tctx.Ctx, filePath, gset)
			if err != nil {
				return nil, err
			}
			if contain {
				fileName, err := binlog.ParseFilename(file)
				if err != nil {
					return nil, err
				}
				// Start at the beginning of the file
				return &mysql.Position{
					Name: binlog.ConstructFilenameWithUUIDSuffix(fileName, utils.SuffixIntToStr(suffix)),
					Pos:  4,
				}, nil
			}
		}
	}
	return nil, terror.ErrNoRelayPosMatchGTID.Generate(gset.String())
}

// StartSyncByPos start sync by pos
// TODO:  thread-safe?
func (r *BinlogReader) StartSyncByPos(pos mysql.Position) (Streamer, error) {
	if pos.Name == "" {
		return nil, terror.ErrBinlogFileNotSpecified.Generate()
	}
	if r.running {
		return nil, terror.ErrReaderAlreadyRunning.Generate()
	}

	// load and update UUID list
	// NOTE: if want to support auto master-slave switching, then needing to re-load UUIDs when parsing.
	err := r.updateUUIDs()
	if err != nil {
		return nil, err
	}
	err = r.checkRelayPos(pos)
	if err != nil {
		return nil, err
	}

	r.latestServerID = 0
	r.running = true
	s := newLocalStreamer()

	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		r.tctx.L().Info("start reading", zap.Stringer("position", pos))
		err = r.parseRelay(r.tctx.Context(), s, pos)
		if errors.Cause(err) == r.tctx.Context().Err() {
			r.tctx.L().Warn("parse relay finished", log.ShortError(err))
		} else if err != nil {
			s.closeWithError(err)
			r.tctx.L().Error("parse relay stopped", zap.Error(err))
		}
	}()

	return s, nil
}

// StartSyncByGTID start sync by gtid.
func (r *BinlogReader) StartSyncByGTID(gset mysql.GTIDSet) (Streamer, error) {
	r.tctx.L().Info("begin to sync binlog", zap.Stringer("GTID Set", gset))
	r.usingGTID = true

	if r.running {
		return nil, terror.ErrReaderAlreadyRunning.Generate()
	}

	if err := r.updateUUIDs(); err != nil {
		return nil, err
	}

	pos, err := r.getPosByGTID(gset)
	if err != nil {
		return nil, err
	}
	r.tctx.L().Info("get pos by gtid", zap.Stringer("GTID Set", gset), zap.Stringer("Position", pos))

	r.prevGset = gset
	r.currGset = nil

	r.latestServerID = 0
	r.running = true
	s := newLocalStreamer()

	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		r.tctx.L().Info("start reading", zap.Stringer("position", pos))
		err = r.parseRelay(r.tctx.Context(), s, *pos)
		if errors.Cause(err) == r.tctx.Context().Err() {
			r.tctx.L().Warn("parse relay finished", log.ShortError(err))
		} else if err != nil {
			s.closeWithError(err)
			r.tctx.L().Error("parse relay stopped", zap.Error(err))
		}
	}()

	return s, nil
}

// parseRelay parses relay root directory, it support master-slave switch (switching to next sub directory).
func (r *BinlogReader) parseRelay(ctx context.Context, s *LocalStreamer, pos mysql.Position) error {
	var (
		needSwitch     bool
		nextUUID       string
		nextBinlogName string
		err            error
	)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		needSwitch, nextUUID, nextBinlogName, err = r.parseDirAsPossible(ctx, s, pos)
		if err != nil {
			return err
		}
		if !needSwitch {
			return terror.ErrNoSubdirToSwitch.Generate()
		}

		_, suffixInt, err2 := utils.ParseSuffixForUUID(nextUUID)
		if err2 != nil {
			return terror.Annotatef(err2, "parse suffix for UUID %s", nextUUID)
		}
		uuidSuffix := utils.SuffixIntToStr(suffixInt)

		parsed, err2 := binlog.ParseFilename(nextBinlogName)
		if err2 != nil {
			return terror.Annotatef(err2, "parse binlog file name %s", nextBinlogName)
		}

		// update pos, so can switch to next sub directory
		pos.Name = binlog.ConstructFilenameWithUUIDSuffix(parsed, uuidSuffix)
		pos.Pos = 4 // start from pos 4 for next sub directory / file
		r.tctx.L().Info("switching to next ready sub directory", zap.String("next uuid", nextUUID), zap.Stringer("position", pos))
	}
}

// parseDirAsPossible parses relay sub directory as far as possible.
func (r *BinlogReader) parseDirAsPossible(ctx context.Context, s *LocalStreamer, pos mysql.Position) (needSwitch bool, nextUUID string, nextBinlogName string, err error) {
	currentUUID, _, realPos, err := binlog.ExtractPos(pos, r.uuids)
	if err != nil {
		return false, "", "", terror.Annotatef(err, "parse relay dir with pos %v", pos)
	}
	pos = realPos      // use realPos to do syncing
	firstParse := true // the first parse time for the relay log file
	dir := path.Join(r.cfg.RelayDir, currentUUID)
	r.tctx.L().Info("start to parse relay log files in sub directory", zap.String("directory", dir), zap.Stringer("position", pos))

	for {
		select {
		case <-ctx.Done():
			return false, "", "", ctx.Err()
		default:
		}
		files, err := CollectBinlogFilesCmp(dir, pos.Name, FileCmpBiggerEqual)
		if err != nil {
			return false, "", "", terror.Annotatef(err, "parse relay dir %s with pos %s", dir, pos)
		} else if len(files) == 0 {
			return false, "", "", terror.ErrNoRelayLogMatchPos.Generate(dir, pos)
		}

		r.tctx.L().Debug("start read relay log files", zap.Strings("files", files), zap.String("directory", dir), zap.Stringer("position", pos))

		var (
			latestPos  int64
			latestName string
			offset     = int64(pos.Pos)
		)
		for i, relayLogFile := range files {
			select {
			case <-ctx.Done():
				return false, "", "", ctx.Err()
			default:
			}
			if i == 0 {
				if !strings.HasSuffix(relayLogFile, pos.Name) {
					return false, "", "", terror.ErrFirstRelayLogNotMatchPos.Generate(relayLogFile, pos)
				}
			} else {
				offset = 4        // for other relay log file, start parse from 4
				firstParse = true // new relay log file need to parse
			}
			needSwitch, latestPos, nextUUID, nextBinlogName, err = r.parseFileAsPossible(ctx, s, relayLogFile, offset, dir, firstParse, currentUUID, i == len(files)-1)
			if err != nil {
				return false, "", "", terror.Annotatef(err, "parse relay log file %s from offset %d in dir %s", relayLogFile, offset, dir)
			}
			firstParse = false // already parsed
			if needSwitch {
				// need switch to next relay sub directory
				return true, nextUUID, nextBinlogName, nil
			}
			latestName = relayLogFile // record the latest file name
		}

		// update pos, so can re-collect files from the latest file and re start parse from latest pos
		pos.Pos = uint32(latestPos)
		pos.Name = latestName
	}
}

// parseFileAsPossible parses single relay log file as far as possible.
func (r *BinlogReader) parseFileAsPossible(ctx context.Context, s *LocalStreamer, relayLogFile string, offset int64, relayLogDir string, firstParse bool, currentUUID string, possibleLast bool) (needSwitch bool, latestPos int64, nextUUID string, nextBinlogName string, err error) {
	var needReParse bool
	latestPos = offset
	replaceWithHeartbeat := false
	r.tctx.L().Debug("start to parse relay log file", zap.String("file", relayLogFile), zap.Int64("position", latestPos), zap.String("directory", relayLogDir))

	for {
		select {
		case <-ctx.Done():
			return false, 0, "", "", ctx.Err()
		default:
		}
		needSwitch, needReParse, latestPos, nextUUID, nextBinlogName, replaceWithHeartbeat, err = r.parseFile(ctx, s, relayLogFile, latestPos, relayLogDir, firstParse, currentUUID, possibleLast, replaceWithHeartbeat)
		if err != nil {
			return false, 0, "", "", terror.Annotatef(err, "parse relay log file %s from offset %d in dir %s", relayLogFile, latestPos, relayLogDir)
		}
		firstParse = false // set to false to handle the `continue` below
		if needReParse {
			r.tctx.L().Debug("continue to re-parse relay log file", zap.String("file", relayLogFile), zap.String("directory", relayLogDir))
			continue // should continue to parse this file
		}
		return needSwitch, latestPos, nextUUID, nextBinlogName, nil
	}
}

// parseFile parses single relay log file from specified offset
// TODO: move all stateful variables into a class, such as r.fileParser.
func (r *BinlogReader) parseFile(
	ctx context.Context,
	s *LocalStreamer,
	relayLogFile string,
	offset int64,
	relayLogDir string,
	firstParse bool,
	currentUUID string,
	possibleLast bool,
	replaceWithHeartbeat bool,
) (needSwitch, needReParse bool, latestPos int64, nextUUID, nextBinlogName string, currentReplaceFlag bool, err error) {
	_, suffixInt, err := utils.ParseSuffixForUUID(currentUUID)
	if err != nil {
		return false, false, 0, "", "", false, err
	}

	uuidSuffix := utils.SuffixIntToStr(suffixInt) // current UUID's suffix, which will be added to binlog name
	latestPos = offset                            // set to argument passed in

	onEventFunc := func(e *replication.BinlogEvent) error {
		r.tctx.L().Debug("read event", zap.Reflect("header", e.Header))
		r.latestServerID = e.Header.ServerID // record server_id

		switch ev := e.Event.(type) {
		case *replication.FormatDescriptionEvent:
			// ignore FORMAT_DESCRIPTION event, because go-mysql will send this fake event
		case *replication.RotateEvent:
			// add master UUID suffix to pos.Name
			parsed, _ := binlog.ParseFilename(string(ev.NextLogName))
			nameWithSuffix := binlog.ConstructFilenameWithUUIDSuffix(parsed, uuidSuffix)
			ev.NextLogName = []byte(nameWithSuffix)

			if e.Header.Timestamp != 0 && e.Header.LogPos != 0 {
				// not fake rotate event, update file pos
				latestPos = int64(e.Header.LogPos)
			} else {
				r.tctx.L().Debug("skip fake rotate event", zap.Reflect("header", e.Header))
			}

			// currently, we do not switch to the next relay log file when we receive the RotateEvent,
			// because that next relay log file may not exists at this time,
			// and we *try* to switch to the next when `needReParse` is false.
			// so this `currentPos` only used for log now.
			currentPos := mysql.Position{
				Name: string(ev.NextLogName),
				Pos:  uint32(ev.Position),
			}
			r.tctx.L().Info("rotate binlog", zap.Stringer("position", currentPos))
		case *replication.GTIDEvent:
			if r.prevGset == nil {
				latestPos = int64(e.Header.LogPos)
				break
			}
			u, _ := uuid.FromBytes(ev.SID)
			replaceWithHeartbeat, err = r.advanceCurrentGtidSet(fmt.Sprintf("%s:%d", u.String(), ev.GNO))
			if err != nil {
				return errors.Trace(err)
			}
			latestPos = int64(e.Header.LogPos)
		case *replication.MariadbGTIDEvent:
			if r.prevGset == nil {
				latestPos = int64(e.Header.LogPos)
				break
			}
			GTID := ev.GTID
			replaceWithHeartbeat, err = r.advanceCurrentGtidSet(fmt.Sprintf("%d-%d-%d", GTID.DomainID, GTID.ServerID, GTID.SequenceNumber))
			if err != nil {
				return errors.Trace(err)
			}
			latestPos = int64(e.Header.LogPos)
		case *replication.XIDEvent:
			ev.GSet = r.getCurrentGtidSet()
			latestPos = int64(e.Header.LogPos)
		case *replication.QueryEvent:
			ev.GSet = r.getCurrentGtidSet()
			latestPos = int64(e.Header.LogPos)
		default:
			// update file pos
			latestPos = int64(e.Header.LogPos)
		}

		// align with MySQL
		// if an event's gtid has been contained by given gset
		// replace it with HEARTBEAT event
		// for Mariadb, it will bee replaced with MARIADB_GTID_LIST_EVENT
		// In DM, we replace both of them with HEARTBEAT event
		if replaceWithHeartbeat {
			switch e.Event.(type) {
			// Only replace transaction event
			// Other events such as FormatDescriptionEvent, RotateEvent, etc. should be the same as before
			case *replication.RowsEvent, *replication.QueryEvent, *replication.GTIDEvent, *replication.XIDEvent, *replication.TableMapEvent:
				// replace with heartbeat event
				e = event.GenHeartbeatEvent(e.Header)
			default:
			}
		}

		select {
		case s.ch <- e:
		case <-ctx.Done():
		}
		return nil
	}

	fullPath := filepath.Join(relayLogDir, relayLogFile)

	if firstParse {
		// if the file is the first time to parse, send a fake ROTATE_EVENT before parse binlog file
		// ref: https://github.com/mysql/mysql-server/blob/4f1d7cf5fcb11a3f84cff27e37100d7295e7d5ca/sql/rpl_binlog_sender.cc#L248
		e, err2 := utils.GenFakeRotateEvent(relayLogFile, uint64(offset), r.latestServerID)
		if err2 != nil {
			return false, false, 0, "", "", false, terror.Annotatef(err2, "generate fake RotateEvent for (%s: %d)", relayLogFile, offset)
		}
		err2 = onEventFunc(e)
		if err2 != nil {
			return false, false, 0, "", "", false, terror.Annotatef(err2, "send event %+v", e.Header)
		}
		r.tctx.L().Info("start parse relay log file", zap.String("file", fullPath), zap.Int64("offset", offset))
	} else {
		r.tctx.L().Debug("start parse relay log file", zap.String("file", fullPath), zap.Int64("offset", offset))
	}

	// use parser.ParseFile directly now, if needed we can change to use FileReader.
	err = r.parser.ParseFile(fullPath, offset, onEventFunc)
	if err != nil {
		if possibleLast && isIgnorableParseError(err) {
			r.tctx.L().Warn("fail to parse relay log file, meet some ignorable error", zap.String("file", fullPath), zap.Int64("offset", offset), zap.Error(err))
			// the file is truncated, we send a mock event with `IGNORABLE_EVENT` to notify the the consumer
			// TODO: should add a integration test for this
			e := &replication.BinlogEvent{
				RawData: []byte(ErrorMaybeDuplicateEvent.Error()),
				Header: &replication.EventHeader{
					EventType: replication.IGNORABLE_EVENT,
				},
			}
			s.ch <- e
		} else {
			r.tctx.L().Error("parse relay log file", zap.String("file", fullPath), zap.Int64("offset", offset), zap.Error(err))
			return false, false, 0, "", "", false, terror.ErrParserParseRelayLog.Delegate(err, fullPath)
		}
	}
	r.tctx.L().Debug("parse relay log file", zap.String("file", fullPath), zap.Int64("offset", latestPos))

	if !possibleLast {
		// there are more relay log files in current sub directory, continue to re-collect them
		r.tctx.L().Info("more relay log files need to parse", zap.String("directory", relayLogDir))
		return false, false, latestPos, "", "", false, nil
	}

	switchCh := make(chan SwitchPath, 1)
	switchErrCh := make(chan error, 1)
	updatePathCh := make(chan string, 1)
	updateErrCh := make(chan error, 1)
	newCtx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	defer func() {
		cancel()
		wg.Wait()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		needSwitchSubDir(newCtx, r.cfg.RelayDir, currentUUID, switchCh, switchErrCh)
	}()

	wg.Add(1)
	go func(latestPos int64) {
		defer wg.Done()
		relayLogUpdatedOrNewCreated(newCtx, watcherInterval, relayLogDir, fullPath, relayLogFile, latestPos, updatePathCh, updateErrCh)
	}(latestPos)

	select {
	case <-ctx.Done():
		return false, false, 0, "", "", false, nil
	case switchResp := <-switchCh:
		// wait to ensure old file not updated
		pathUpdated := utils.WaitSomething(3, watcherInterval, func() bool { return len(updatePathCh) > 0 })
		if pathUpdated {
			// re-parse it
			return false, true, latestPos, "", "", replaceWithHeartbeat, nil
		}
		// update new uuid
		if err = r.updateUUIDs(); err != nil {
			return false, false, 0, "", "", false, nil
		}
		return true, false, 0, switchResp.nextUUID, switchResp.nextBinlogName, false, nil
	case updatePath := <-updatePathCh:
		if strings.HasSuffix(updatePath, relayLogFile) {
			// current relay log file updated, need to re-parse it
			return false, true, latestPos, "", "", replaceWithHeartbeat, nil
		}
		// need parse next relay log file or re-collect files
		return false, false, latestPos, "", "", false, nil
	case err := <-switchErrCh:
		return false, false, 0, "", "", false, err
	case err := <-updateErrCh:
		return false, false, 0, "", "", false, err
	}
}

// updateUUIDs re-parses UUID index file and updates UUID list.
func (r *BinlogReader) updateUUIDs() error {
	uuids, err := utils.ParseUUIDIndex(r.indexPath)
	if err != nil {
		return terror.Annotatef(err, "index file path %s", r.indexPath)
	}
	oldUUIDs := r.uuids
	r.uuids = uuids
	r.tctx.L().Info("update relay UUIDs", zap.Strings("old uuids", oldUUIDs), zap.Strings("uuids", uuids))
	return nil
}

// Close closes BinlogReader.
func (r *BinlogReader) Close() {
	r.tctx.L().Info("binlog reader closing")
	r.running = false
	r.cancel()
	r.parser.Stop()
	r.wg.Wait()
	r.tctx.L().Info("binlog reader closed")
}

// GetUUIDs returns binlog reader's uuids.
func (r *BinlogReader) GetUUIDs() []string {
	uuids := make([]string, 0, len(r.uuids))
	uuids = append(uuids, r.uuids...)
	return uuids
}

func (r *BinlogReader) getCurrentGtidSet() mysql.GTIDSet {
	if r.currGset == nil {
		return nil
	}
	return r.currGset.Clone()
}

// advanceCurrentGtidSet advance gtid set and return whether currGset not updated.
func (r *BinlogReader) advanceCurrentGtidSet(gtid string) (bool, error) {
	if r.currGset == nil {
		r.currGset = r.prevGset.Clone()
	}
	// Special treatment for Maridb
	// MaridbGTIDSet.Update(gtid) will replace gset with given gtid
	// ref https://github.com/go-mysql-org/go-mysql/blob/0c5789dd0bd378b4b84f99b320a2d35a80d8858f/mysql/mariadb_gtid.go#L96
	if r.cfg.Flavor == mysql.MariaDBFlavor {
		gset, err := mysql.ParseMariadbGTIDSet(gtid)
		if err != nil {
			return false, err
		}
		if r.currGset.Contain(gset) {
			return true, nil
		}
	}
	prev := r.currGset.Clone()
	err := r.currGset.Update(gtid)
	if err == nil {
		if !r.currGset.Equal(prev) {
			r.prevGset = prev
			return false, nil
		}
		return true, nil
	}
	return false, err
}
