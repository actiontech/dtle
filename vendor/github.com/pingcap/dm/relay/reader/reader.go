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
	"time"

	"github.com/pingcap/errors"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"

	"github.com/pingcap/dm/pkg/binlog/common"
	br "github.com/pingcap/dm/pkg/binlog/reader"
	"github.com/pingcap/dm/pkg/gtid"
	"github.com/pingcap/dm/pkg/log"
)

const (
	// event timeout when trying to read events from upstream master server.
	eventTimeout = 10 * time.Minute
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
}

// NewReader creates a Reader instance.
func NewReader(cfg *Config) Reader {
	return &reader{
		cfg: cfg,
		in:  br.NewTCPReader(cfg.SyncConfig),
		out: make(chan *replication.BinlogEvent),
	}
}

// Start implements Reader.Start.
func (r *reader) Start() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.stage != common.StageNew {
		return errors.Errorf("stage %s, expect %s, already started", r.stage, common.StageNew)
	}
	r.stage = common.StagePrepared

	defer func() {
		status := r.in.Status()
		log.Infof("[relay] set up binlog reader for master %s with status %s", r.cfg.MasterID, status)
	}()

	var err error
	if r.cfg.EnableGTID {
		err = r.setUpReaderByGTID()
	} else {
		err = r.setUpReaderByPos()
	}

	return errors.Trace(err)
}

// Close implements Reader.Close.
func (r *reader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.stage != common.StagePrepared {
		return errors.Errorf("stage %s, expect %s, can not close", r.stage, common.StagePrepared)
	}

	err := r.in.Close()
	r.stage = common.StageClosed
	return errors.Trace(err)
}

// GetEvent implements Reader.GetEvent.
// NOTE: can only close the reader after this returned.
func (r *reader) GetEvent(ctx context.Context) (Result, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var result Result
	if r.stage != common.StagePrepared {
		return result, errors.Errorf("stage %s, expect %s, please start the reader first", r.stage, common.StagePrepared)
	}

	for {
		ctx2, cancel2 := context.WithTimeout(ctx, eventTimeout)
		ev, err := r.in.GetEvent(ctx2)
		cancel2()

		if err == nil {
			result.Event = ev
		} else if isRetryableError(err) {
			log.Infof("[relay] get retryable error %v when reading binlog event", err)
			continue
		}
		return result, errors.Trace(err)
	}
}

func (r *reader) setUpReaderByGTID() error {
	gs := r.cfg.GTIDs
	log.Infof("[relay] start sync for master %s from GTID set %s", r.cfg.MasterID, gs)
	return r.in.StartSyncByGTID(gs)
}

func (r *reader) setUpReaderByPos() error {
	pos := r.cfg.Pos
	log.Infof("[relay] start sync for master %s from position %s", r.cfg.MasterID, pos)
	return r.in.StartSyncByPos(pos)
}
