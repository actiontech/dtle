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

// binlog events generator for MySQL used to generate some binlog events for tests.
// Readability takes precedence over performance.

package reader

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pingcap/errors"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/pingcap/dm/pkg/binlog/common"
	"github.com/pingcap/dm/pkg/gtid"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"
)

// FileReader is a binlog event reader which reads binlog events from a file.
type FileReader struct {
	mu sync.RWMutex
	wg sync.WaitGroup

	stage      common.Stage
	readOffset atomic.Uint32
	sendOffset atomic.Uint32

	parser *replication.BinlogParser
	ch     chan *replication.BinlogEvent
	ech    chan error
	endCh  chan struct{}

	logger log.Logger

	ctx    context.Context
	cancel context.CancelFunc
}

// FileReaderConfig is the configuration used by a FileReader.
type FileReaderConfig struct {
	EnableRawMode bool
	Timezone      *time.Location
	ChBufferSize  int // event channel's buffer size
	EchBufferSize int // error channel's buffer size
}

// FileReaderStatus represents the status of a FileReader.
type FileReaderStatus struct {
	Stage      string `json:"stage"`
	ReadOffset uint32 `json:"read-offset"` // read event's offset in the file
	SendOffset uint32 `json:"send-offset"` // sent event's offset in the file
}

// String implements Stringer.String.
func (s *FileReaderStatus) String() string {
	data, err := json.Marshal(s)
	if err != nil {
		log.L().Error("fail to marshal status to json", zap.Reflect("file reader status", s), log.ShortError(err))
	}
	return string(data)
}

// NewFileReader creates a FileReader instance.
func NewFileReader(cfg *FileReaderConfig) Reader {
	parser := replication.NewBinlogParser()
	parser.SetVerifyChecksum(true)
	parser.SetUseDecimal(false)
	parser.SetRawMode(cfg.EnableRawMode)
	if cfg.Timezone != nil {
		parser.SetTimestampStringLocation(cfg.Timezone)
	}
	return &FileReader{
		parser: parser,
		ch:     make(chan *replication.BinlogEvent, cfg.ChBufferSize),
		ech:    make(chan error, cfg.EchBufferSize),
		endCh:  make(chan struct{}),
		logger: log.With(zap.String("component", "binlog file reader")),
	}
}

// StartSyncByPos implements Reader.StartSyncByPos.
// TODO: support heartbeat event.
func (r *FileReader) StartSyncByPos(pos gmysql.Position) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.stage != common.StageNew {
		return terror.ErrReaderAlreadyStarted.Generate(r.stage, common.StageNew)
	}

	// keep running until canceled in `Close`.
	r.ctx, r.cancel = context.WithCancel(context.Background())
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		err := r.parser.ParseFile(pos.Name, int64(pos.Pos), r.onEvent)
		if err != nil {
			if errors.Cause(err) != context.Canceled {
				r.logger.Error("fail to parse binlog file", zap.Error(err))
			}
			select {
			case r.ech <- err:
			case <-r.ctx.Done():
			}
		} else {
			r.logger.Info("parse end of binlog file", zap.Uint32("pos", r.readOffset.Load()))
			close(r.endCh)
		}
	}()

	r.stage = common.StagePrepared
	return nil
}

// StartSyncByGTID implements Reader.StartSyncByGTID.
func (r *FileReader) StartSyncByGTID(gSet gtid.Set) error {
	// NOTE: may be supported later.
	return terror.ErrBinlogReadFileByGTID.Generate()
}

// Close implements Reader.Close.
func (r *FileReader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.stage != common.StagePrepared {
		return terror.ErrReaderStateCannotClose.Generate(r.stage, common.StagePrepared)
	}

	r.cancel()
	r.wg.Wait()
	r.parser.Stop()
	r.stage = common.StageClosed
	return nil
}

// GetEvent implements Reader.GetEvent.
func (r *FileReader) GetEvent(ctx context.Context) (*replication.BinlogEvent, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.stage != common.StagePrepared {
		return nil, terror.ErrReaderShouldStartSync.Generate(r.stage, common.StagePrepared)
	}

	select {
	case ev := <-r.ch:
		r.sendOffset.Store(ev.Header.LogPos)
		return ev, nil
	case err := <-r.ech:
		return nil, err
	case <-r.endCh:
		return nil, terror.ErrReaderReachEndOfFile.Generate()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Status implements Reader.Status.
func (r *FileReader) Status() interface{} {
	r.mu.RLock()
	stage := r.stage
	r.mu.RUnlock()

	return &FileReaderStatus{
		Stage:      stage.String(),
		ReadOffset: r.readOffset.Load(),
		SendOffset: r.sendOffset.Load(),
	}
}

func (r *FileReader) onEvent(ev *replication.BinlogEvent) error {
	select {
	case r.ch <- ev:
		r.readOffset.Store(ev.Header.LogPos)
		return nil
	case <-r.ctx.Done():
		return r.ctx.Err()
	}
}
