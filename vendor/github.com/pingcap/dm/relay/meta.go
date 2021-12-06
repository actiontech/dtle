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
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/BurntSushi/toml"
	"github.com/go-mysql-org/go-mysql/mysql"

	"github.com/pingcap/dm/pkg/binlog"
	"github.com/pingcap/dm/pkg/gtid"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/utils"
)

var (
	minUUIDSufix  = 1
	minCheckpoint = mysql.Position{Pos: 4}
)

// Meta represents binlog meta information for sync source
// when re-syncing, we should reload meta info to guarantee continuous transmission
// in order to support master-slave switching, Meta should support switching binlog meta info to newer master
// should support the case, where switching from A to B, then switching from B back to A.
type Meta interface {
	// Load loads meta information for the recently active server
	Load() error

	// AdjustWithStartPos adjusts current pos / GTID with start pos
	// if current pos / GTID is meaningless, update to start pos or last pos when start pos is meaningless
	// else do nothing
	AdjustWithStartPos(binlogName string, binlogGTID string, enableGTID bool, latestBinlogName string, latestBinlogGTID string) (bool, error)

	// Save saves meta information
	Save(pos mysql.Position, gset gtid.Set) error

	// Flush flushes meta information
	Flush() error

	// Dirty checks whether meta in memory is dirty (need to Flush)
	Dirty() bool

	// AddDir adds sub relay directory for server UUID (without suffix)
	// if uuidSuffix is not zero value, add sub relay directory with uuidSuffix (bound to a new source)
	// otherwise the added sub relay directory's suffix is incremented (master/slave switch)
	// after sub relay directory added, the internal binlog pos should be reset
	// and binlog pos will be set again when new binlog events received
	// @serverUUID should be a server_uuid for MySQL or MariaDB
	// if set @newPos / @newGTID, old value will be replaced
	AddDir(serverUUID string, newPos *mysql.Position, newGTID gtid.Set, uuidSuffix int) error

	// Pos returns current (UUID with suffix, Position) pair
	Pos() (string, mysql.Position)

	// GTID returns current (UUID with suffix, GTID) pair
	GTID() (string, gtid.Set)

	// UUID returns current UUID (with suffix)
	UUID() string

	// TrimUUIDs trim invalid UUIDs from memory and update the server-uuid.index file
	// return trimmed UUIDs
	TrimUUIDs() ([]string, error)

	// Dir returns current relay log (sub) directory
	Dir() string

	// String returns string representation of current meta info
	String() string
}

// LocalMeta implements Meta by save info in local.
type LocalMeta struct {
	sync.RWMutex
	flavor        string
	baseDir       string
	uuidIndexPath string
	currentUUID   string   // current UUID with suffix
	uuids         []string // all valid UUIDs
	gset          gtid.Set
	emptyGSet     gtid.Set
	dirty         bool

	BinLogName string `toml:"binlog-name" json:"binlog-name"`
	BinLogPos  uint32 `toml:"binlog-pos" json:"binlog-pos"`
	BinlogGTID string `toml:"binlog-gtid" json:"binlog-gtid"`
}

// NewLocalMeta creates a new LocalMeta.
func NewLocalMeta(flavor, baseDir string) Meta {
	lm := &LocalMeta{
		flavor:        flavor,
		baseDir:       baseDir,
		uuidIndexPath: filepath.Join(baseDir, utils.UUIDIndexFilename),
		currentUUID:   "",
		uuids:         make([]string, 0),
		dirty:         false,
		BinLogName:    minCheckpoint.Name,
		BinLogPos:     minCheckpoint.Pos,
		BinlogGTID:    "",
	}
	lm.emptyGSet, _ = gtid.ParserGTID(flavor, "")
	return lm
}

// Load implements Meta.Load.
func (lm *LocalMeta) Load() error {
	lm.Lock()
	defer lm.Unlock()

	uuids, err := utils.ParseUUIDIndex(lm.uuidIndexPath)
	if err != nil {
		return err
	}

	err = lm.verifyUUIDs(uuids)
	if err != nil {
		return err
	}

	if len(uuids) > 0 {
		// update to the latest
		err = lm.updateCurrentUUID(uuids[len(uuids)-1])
		if err != nil {
			return err
		}
	}
	lm.uuids = uuids

	err = lm.loadMetaData()
	if err != nil {
		return err
	}

	return nil
}

// AdjustWithStartPos implements Meta.AdjustWithStartPos, return whether adjusted.
func (lm *LocalMeta) AdjustWithStartPos(binlogName string, binlogGTID string, enableGTID bool, latestBinlogName string, latestBinlogGTID string) (bool, error) {
	lm.Lock()
	defer lm.Unlock()

	// check whether already have meaningful pos
	if len(lm.currentUUID) > 0 {
		_, suffix, err := utils.ParseSuffixForUUID(lm.currentUUID)
		if err != nil {
			return false, err
		}
		currPos := mysql.Position{Name: lm.BinLogName, Pos: lm.BinLogPos}
		if suffix != minUUIDSufix || currPos.Compare(minCheckpoint) > 0 || len(lm.BinlogGTID) > 0 {
			return false, nil // current pos is meaningful, do nothing
		}
	}

	gset := lm.emptyGSet.Clone()
	var err error

	if enableGTID {
		if len(binlogGTID) == 0 {
			binlogGTID = latestBinlogGTID
			binlogName = latestBinlogName
		}
		gset, err = gtid.ParserGTID(lm.flavor, binlogGTID)
		if err != nil {
			return false, terror.Annotatef(err, "relay-binlog-gtid %s", binlogGTID)
		}
	} else {
		if len(binlogName) == 0 { // no meaningful start pos specified
			binlogGTID = latestBinlogGTID
			binlogName = latestBinlogName
		} else if !binlog.VerifyFilename(binlogName) {
			return false, terror.ErrRelayBinlogNameNotValid.Generate(binlogName)
		}
	}

	lm.BinLogName = binlogName
	lm.BinLogPos = minCheckpoint.Pos // always set pos to 4
	lm.BinlogGTID = binlogGTID
	lm.gset = gset

	return true, lm.doFlush()
}

// Save implements Meta.Save.
func (lm *LocalMeta) Save(pos mysql.Position, gset gtid.Set) error {
	lm.Lock()
	defer lm.Unlock()

	if len(lm.currentUUID) == 0 {
		return terror.ErrRelayNoCurrentUUID.Generate()
	}

	lm.BinLogName = pos.Name
	lm.BinLogPos = pos.Pos
	if gset == nil {
		lm.BinlogGTID = ""
	} else {
		lm.BinlogGTID = gset.String()
		lm.gset = gset
	}

	lm.dirty = true

	return nil
}

// Flush implements Meta.Flush.
func (lm *LocalMeta) Flush() error {
	lm.Lock()
	defer lm.Unlock()

	return lm.doFlush()
}

// doFlush does the real flushing.
func (lm *LocalMeta) doFlush() error {
	if len(lm.currentUUID) == 0 {
		return terror.ErrRelayNoCurrentUUID.Generate()
	}

	var buf bytes.Buffer
	enc := toml.NewEncoder(&buf)
	err := enc.Encode(lm)
	if err != nil {
		return terror.ErrRelayFlushLocalMeta.Delegate(err)
	}

	filename := filepath.Join(lm.baseDir, lm.currentUUID, utils.MetaFilename)
	err = utils.WriteFileAtomic(filename, buf.Bytes(), 0o644)
	if err != nil {
		return terror.ErrRelayFlushLocalMeta.Delegate(err)
	}

	lm.dirty = false

	return nil
}

// Dirty implements Meta.Dirty.
func (lm *LocalMeta) Dirty() bool {
	lm.RLock()
	defer lm.RUnlock()

	return lm.dirty
}

// Dir implements Meta.Dir.
func (lm *LocalMeta) Dir() string {
	lm.RLock()
	defer lm.RUnlock()

	return filepath.Join(lm.baseDir, lm.currentUUID)
}

// AddDir implements Meta.AddDir.
func (lm *LocalMeta) AddDir(serverUUID string, newPos *mysql.Position, newGTID gtid.Set, uuidSuffix int) error {
	lm.Lock()
	defer lm.Unlock()

	var newUUID string

	if len(lm.currentUUID) == 0 {
		// no UUID exists yet, simply add it
		if uuidSuffix == 0 {
			newUUID = utils.AddSuffixForUUID(serverUUID, minUUIDSufix)
		} else {
			newUUID = utils.AddSuffixForUUID(serverUUID, uuidSuffix)
		}
	} else {
		_, suffix, err := utils.ParseSuffixForUUID(lm.currentUUID)
		if err != nil {
			return err
		}
		// even newUUID == currentUUID, we still append it (for some cases, like `RESET MASTER`)
		newUUID = utils.AddSuffixForUUID(serverUUID, suffix+1)
	}

	// flush previous meta
	if lm.dirty {
		err := lm.doFlush()
		if err != nil {
			return err
		}
	}

	// make sub dir for UUID
	err := os.Mkdir(filepath.Join(lm.baseDir, newUUID), 0o744)
	if err != nil {
		return terror.ErrRelayMkdir.Delegate(err)
	}

	// update UUID index file
	uuids := lm.uuids
	uuids = append(uuids, newUUID)
	err = lm.updateIndexFile(uuids)
	if err != nil {
		return err
	}

	// update current UUID
	lm.currentUUID = newUUID
	lm.uuids = uuids

	if newPos != nil {
		lm.BinLogName = newPos.Name
		lm.BinLogPos = newPos.Pos
	} else {
		// reset binlog pos, will be set again when new binlog events received from master
		// not reset GTID, it will be used to continue the syncing
		lm.BinLogName = minCheckpoint.Name
		lm.BinLogPos = minCheckpoint.Pos
	}

	if newGTID != nil {
		lm.gset = newGTID
		lm.BinlogGTID = newGTID.String()
	} // if newGTID == nil, keep GTID not changed

	// flush new meta to file
	return lm.doFlush()
}

// Pos implements Meta.Pos.
func (lm *LocalMeta) Pos() (string, mysql.Position) {
	lm.RLock()
	defer lm.RUnlock()

	return lm.currentUUID, mysql.Position{Name: lm.BinLogName, Pos: lm.BinLogPos}
}

// GTID implements Meta.GTID.
func (lm *LocalMeta) GTID() (string, gtid.Set) {
	lm.RLock()
	defer lm.RUnlock()

	if lm.gset != nil {
		return lm.currentUUID, lm.gset.Clone()
	}
	return lm.currentUUID, nil
}

// UUID implements Meta.UUID.
func (lm *LocalMeta) UUID() string {
	lm.RLock()
	defer lm.RUnlock()
	return lm.currentUUID
}

// TrimUUIDs implements Meta.TrimUUIDs.
func (lm *LocalMeta) TrimUUIDs() ([]string, error) {
	lm.Lock()
	defer lm.Unlock()

	kept := make([]string, 0, len(lm.uuids))
	trimmed := make([]string, 0)
	for _, uuid := range lm.uuids {
		// now, only check if the sub dir exists
		fp := filepath.Join(lm.baseDir, uuid)
		if utils.IsDirExists(fp) {
			kept = append(kept, uuid)
		} else {
			trimmed = append(trimmed, uuid)
		}
	}

	if len(trimmed) == 0 {
		return nil, nil
	}

	err := lm.updateIndexFile(kept)
	if err != nil {
		return nil, err
	}

	// currentUUID should be not changed
	lm.uuids = kept
	return trimmed, nil
}

// String implements Meta.String.
func (lm *LocalMeta) String() string {
	uuid, pos := lm.Pos()
	_, gs := lm.GTID()
	return fmt.Sprintf("master-uuid = %s, relay-binlog = %v, relay-binlog-gtid = %v", uuid, pos, gs)
}

// updateIndexFile updates the content of server-uuid.index file.
func (lm *LocalMeta) updateIndexFile(uuids []string) error {
	var buf bytes.Buffer
	for _, uuid := range uuids {
		buf.WriteString(uuid)
		buf.WriteString("\n")
	}

	err := utils.WriteFileAtomic(lm.uuidIndexPath, buf.Bytes(), 0o644)
	return terror.ErrRelayUpdateIndexFile.Delegate(err, lm.uuidIndexPath)
}

func (lm *LocalMeta) verifyUUIDs(uuids []string) error {
	previousSuffix := 0
	for _, uuid := range uuids {
		_, suffix, err := utils.ParseSuffixForUUID(uuid)
		if err != nil {
			return terror.Annotatef(err, "UUID %s", uuid)
		}
		if previousSuffix > 0 {
			if previousSuffix+1 != suffix {
				return terror.ErrRelayUUIDSuffixNotValid.Generate(uuid, suffix, previousSuffix)
			}
		}
		previousSuffix = suffix
	}

	return nil
}

// updateCurrentUUID updates current UUID.
func (lm *LocalMeta) updateCurrentUUID(uuid string) error {
	_, suffix, err := utils.ParseSuffixForUUID(uuid)
	if err != nil {
		return err
	}

	if len(lm.currentUUID) > 0 {
		_, previousSuffix, err := utils.ParseSuffixForUUID(lm.currentUUID)
		if err != nil {
			return err // should not happen
		}
		if previousSuffix > suffix {
			return terror.ErrRelayUUIDSuffixLessThanPrev.Generate(lm.currentUUID, uuid)
		}
	}

	lm.currentUUID = uuid
	return nil
}

// loadMetaData loads meta information from meta data file.
func (lm *LocalMeta) loadMetaData() error {
	lm.gset = lm.emptyGSet.Clone()

	if len(lm.currentUUID) == 0 {
		return nil
	}

	filename := filepath.Join(lm.baseDir, lm.currentUUID, utils.MetaFilename)

	fd, err := os.Open(filename)
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return terror.ErrRelayLoadMetaData.Delegate(err)
	}
	defer fd.Close()

	_, err = toml.DecodeReader(fd, lm)
	if err != nil {
		return terror.ErrRelayLoadMetaData.Delegate(err)
	}

	if len(lm.BinlogGTID) != 0 {
		gset, err := gtid.ParserGTID("", lm.BinlogGTID)
		if err != nil {
			return terror.ErrRelayLoadMetaData.Delegate(err)
		}
		lm.gset = gset
	}

	return nil
}
