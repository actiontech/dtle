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
	"strconv"
	"sync"
	"sync/atomic"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-sql-driver/mysql"
	"go.uber.org/zap"

	"github.com/pingcap/failpoint"

	"github.com/pingcap/dm/pkg/binlog/common"
	"github.com/pingcap/dm/pkg/gtid"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/utils"
)

var customID int64

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
		log.L().Error("fail to marshal status to json", zap.Reflect("tcp reader status", s), log.ShortError(err))
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
		return terror.ErrRelayReaderNotStateNew.Generate(r.stage, common.StageNew)
	}

	failpoint.Inject("MockTCPReaderStartSyncByPos", func() {
		r.stage = common.StagePrepared
		failpoint.Return(nil)
	})

	streamer, err := r.syncer.StartSync(pos)
	if err != nil {
		return terror.ErrRelayTCPReaderStartSync.Delegate(err, pos)
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
		return terror.ErrRelayReaderNotStateNew.Generate(r.stage, common.StageNew)
	}

	if gSet == nil {
		return terror.ErrRelayTCPReaderNilGTID.Generate()
	}

	failpoint.Inject("MockTCPReaderStartSyncByGTID", func() {
		r.stage = common.StagePrepared
		failpoint.Return(nil)
	})

	streamer, err := r.syncer.StartSyncGTID(gSet.Origin())
	if err != nil {
		return terror.ErrRelayTCPReaderStartSyncGTID.Delegate(err, gSet)
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
		return terror.ErrRelayReaderStateCannotClose.Generate(r.stage, common.StagePrepared)
	}

	failpoint.Inject("MockTCPReaderClose", func() {
		r.stage = common.StageClosed
		failpoint.Return(nil)
	})

	defer r.syncer.Close()
	connID := r.syncer.LastConnectionID()
	if connID > 0 {
		dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8mb4",
			r.syncerCfg.User, r.syncerCfg.Password, r.syncerCfg.Host, r.syncerCfg.Port)
		if r.syncerCfg.TLSConfig != nil {
			tlsName := "replicate" + strconv.FormatInt(atomic.AddInt64(&customID, 1), 10)
			err := mysql.RegisterTLSConfig(tlsName, r.syncerCfg.TLSConfig)
			if err != nil {
				return terror.WithScope(
					terror.Annotatef(terror.DBErrorAdapt(err, terror.ErrDBDriverError),
						"fail to register tls config for master %s:%d", r.syncerCfg.Host, r.syncerCfg.Port), terror.ScopeUpstream)
			}
			dsn += "&tls=" + tlsName
			defer mysql.DeregisterTLSConfig(tlsName)
		}
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			return terror.WithScope(
				terror.Annotatef(terror.DBErrorAdapt(err, terror.ErrDBDriverError),
					"open connection to the master %s:%d", r.syncerCfg.Host, r.syncerCfg.Port), terror.ScopeUpstream)
		}
		defer db.Close()

		// try to KILL the conn in default timeout.
		ctx, cancel := context.WithTimeout(context.Background(), utils.DefaultDBTimeout)
		defer cancel()
		err = utils.KillConn(ctx, db, connID)
		if err != nil {
			return terror.WithScope(terror.Annotatef(err, "kill connection %d for master %s:%d", connID, r.syncerCfg.Host, r.syncerCfg.Port), terror.ScopeUpstream)
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
		return nil, terror.ErrRelayReaderNeedStart.Generate(r.stage, common.StagePrepared)
	}

	failpoint.Inject("MockTCPReaderGetEvent", func() {
		failpoint.Return(nil, nil)
	})

	ev, err := r.streamer.GetEvent(ctx)
	return ev, terror.ErrRelayTCPReaderGetEvent.Delegate(err)
}

// Status implements Reader.Status.
func (r *TCPReader) Status() interface{} {
	r.mu.RLock()
	stage := r.stage
	r.mu.RUnlock()

	failpoint.Inject("MockTCPReaderStatus", func() {
		status := &TCPReaderStatus{
			Stage:  stage.String(),
			ConnID: uint32(1),
		}
		failpoint.Return(status)
	})
	var connID uint32
	if stage != common.StageNew {
		connID = r.syncer.LastConnectionID()
	}
	return &TCPReaderStatus{
		Stage:  stage.String(),
		ConnID: connID,
	}
}
