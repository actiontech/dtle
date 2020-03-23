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

package reader

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/pingcap/errors"
	gmysql "github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"

	"github.com/pingcap/dm/pkg/binlog/common"
	"github.com/pingcap/dm/pkg/gtid"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/utils"
)

// TCPReader is a binlog event reader which read binlog events from a TCP stream.
type TCPReader struct {
	syncerCfg replication.BinlogSyncerConfig

	mu    sync.RWMutex
	stage common.Stage

	syncer   *replication.BinlogSyncer
	streamer *replication.BinlogStreamer
}

// TCPReaderStatus represents the status of a TCPReader.
type TCPReaderStatus struct {
	Stage  string `json:"stage"`
	ConnID uint32 `json:"connection"`
}

// String implements Stringer.String.
func (s *TCPReaderStatus) String() string {
	data, err := json.Marshal(s)
	if err != nil {
		log.Errorf("[TCPReaderStatus] marshal status to json error %v", err)
	}
	return string(data)
}

// NewTCPReader creates a TCPReader instance.
func NewTCPReader(syncerCfg replication.BinlogSyncerConfig) Reader {
	return &TCPReader{
		syncerCfg: syncerCfg,
		syncer:    replication.NewBinlogSyncer(syncerCfg),
	}
}

// StartSyncByPos implements Reader.StartSyncByPos.
func (r *TCPReader) StartSyncByPos(pos gmysql.Position) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.stage != common.StageNew {
		return errors.Errorf("stage %s, expect %s, already started", r.stage, common.StageNew)
	}

	streamer, err := r.syncer.StartSync(pos)
	if err != nil {
		return errors.Annotatef(err, "start sync from position %s", pos)
	}

	r.streamer = streamer
	r.stage = common.StagePrepared
	return nil
}

// StartSyncByGTID implements Reader.StartSyncByGTID.
func (r *TCPReader) StartSyncByGTID(gSet gtid.Set) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.stage != common.StageNew {
		return errors.Errorf("stage %s, expect %s, already started", r.stage, common.StageNew)
	}

	if gSet == nil {
		return errors.NotValidf("nil GTID set")
	}

	streamer, err := r.syncer.StartSyncGTID(gSet.Origin())
	if err != nil {
		return errors.Annotatef(err, "start sync from GTID set %s", gSet)
	}

	r.streamer = streamer
	r.stage = common.StagePrepared
	return nil
}

// Close implements Reader.Close.
func (r *TCPReader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.stage != common.StagePrepared {
		return errors.Errorf("stage %s, expect %s, can not close", r.stage, common.StagePrepared)
	}

	defer r.syncer.Close()
	connID := r.syncer.LastConnectionID()
	if connID > 0 {
		dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8mb4",
			r.syncerCfg.User, r.syncerCfg.Password, r.syncerCfg.Host, r.syncerCfg.Port)
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			return errors.Annotatef(err, "open connection to the master %s:%d", r.syncerCfg.Host, r.syncerCfg.Port)
		}
		defer db.Close()
		err = utils.KillConn(db, connID)
		if err != nil {
			return errors.Annotatef(err, "kill connection %d for master %s:%d", connID, r.syncerCfg.Host, r.syncerCfg.Port)
		}
	}

	r.stage = common.StageClosed
	return nil
}

// GetEvent implements Reader.GetEvent.
func (r *TCPReader) GetEvent(ctx context.Context) (*replication.BinlogEvent, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.stage != common.StagePrepared {
		return nil, errors.Errorf("stage %s, expect %s, please start sync first", r.stage, common.StagePrepared)
	}

	return r.streamer.GetEvent(ctx)
}

// Status implements Reader.Status.
func (r *TCPReader) Status() interface{} {
	r.mu.RLock()
	stage := r.stage
	r.mu.RUnlock()

	var connID uint32
	if stage != common.StageNew {
		connID = r.syncer.LastConnectionID()
	}
	return &TCPReaderStatus{
		Stage:  stage.String(),
		ConnID: connID,
	}
}
