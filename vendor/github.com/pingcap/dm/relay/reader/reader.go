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
	"sync"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"go.uber.org/zap"

	"github.com/pingcap/dm/pkg/binlog/common"
	br "github.com/pingcap/dm/pkg/binlog/reader"
	"github.com/pingcap/dm/pkg/gtid"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"
)

// Result represents a read operation result.
type Result struct {
	Event *replication.BinlogEvent
}

// Reader reads binlog events from a upstream master server.
// The read binlog events should be send to a transformer.
// The reader should support:
//   1. handle expected errors
//   2. do retry if possible
// NOTE: some errors still need to be handled in the outer caller.
type Reader interface {
	// Start starts the reading process.
	Start() error

	// Close closes the reader and release the resource.
	Close() error

	// GetEvent gets the binlog event one by one, it will block if no event can be read.
	// You can pass a context (like Cancel) to break the block.
	GetEvent(ctx context.Context) (Result, error)
}

// Config is the configuration used by the Reader.
type Config struct {
	SyncConfig replication.BinlogSyncerConfig
	Pos        mysql.Position
	GTIDs      gtid.Set
	EnableGTID bool
	MasterID   string // the identifier for the master, used when logging.
}

// reader implements Reader interface.
type reader struct {
	cfg *Config

	mu    sync.RWMutex
	stage common.Stage

	in  br.Reader // the underlying reader used to read binlog events.
	out chan *replication.BinlogEvent

	logger log.Logger
}

// NewReader creates a Reader instance.
func NewReader(cfg *Config) Reader {
	return &reader{
		cfg:    cfg,
		in:     br.NewTCPReader(cfg.SyncConfig),
		out:    make(chan *replication.BinlogEvent),
		logger: log.With(zap.String("component", "relay reader")),
	}
}

// Start implements Reader.Start.
func (r *reader) Start() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.stage != common.StageNew {
		return terror.ErrRelayReaderNotStateNew.Generate(r.stage, common.StageNew)
	}
	r.stage = common.StagePrepared

	defer func() {
		status := r.in.Status()
		r.logger.Info("set up binlog reader", zap.String("master", r.cfg.MasterID), zap.Reflect("status", status))
	}()

	var err error
	if r.cfg.EnableGTID {
		err = r.setUpReaderByGTID()
	} else {
		err = r.setUpReaderByPos()
	}

	return err
}

// Close implements Reader.Close.
func (r *reader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.stage != common.StagePrepared {
		return terror.ErrRelayReaderStateCannotClose.Generate(r.stage, common.StagePrepared)
	}

	err := r.in.Close()
	r.stage = common.StageClosed
	return err
}

// GetEvent implements Reader.GetEvent.
// NOTE: can only close the reader after this returned.
func (r *reader) GetEvent(ctx context.Context) (Result, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var result Result
	if r.stage != common.StagePrepared {
		return result, terror.ErrRelayReaderNeedStart.Generate(r.stage, common.StagePrepared)
	}

	for {
		ctx2, cancel2 := context.WithTimeout(ctx, common.SlaveReadTimeout)
		ev, err := r.in.GetEvent(ctx2)
		cancel2()

		if err == nil {
			result.Event = ev
		} else if isRetryableError(err) {
			r.logger.Info("get retryable error when reading binlog event", log.ShortError(err))
			continue
		}
		return result, err
	}
}

func (r *reader) setUpReaderByGTID() error {
	gs := r.cfg.GTIDs
	r.logger.Info("start sync", zap.String("master", r.cfg.MasterID), zap.Stringer("from GTID set", gs))
	return r.in.StartSyncByGTID(gs)
}

func (r *reader) setUpReaderByPos() error {
	pos := r.cfg.Pos
	r.logger.Info("start sync", zap.String("master", r.cfg.MasterID), zap.Stringer("from position", pos))
	return r.in.StartSyncByPos(pos)
}
