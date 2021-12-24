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

	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/pingcap/dm/pkg/gtid"
)

// MockReader is a binlog event reader which read binlog events from an input channel.
type MockReader struct {
	ch  chan *replication.BinlogEvent
	ech chan error

	// returned error for methods
	ErrStartByPos  error
	ErrStartByGTID error
	ErrClose       error
}

// NewMockReader creates a MockReader instance.
func NewMockReader() Reader {
	return &MockReader{
		ch:  make(chan *replication.BinlogEvent),
		ech: make(chan error),
	}
}

// StartSyncByPos implements Reader.StartSyncByPos.
func (r *MockReader) StartSyncByPos(pos gmysql.Position) error {
	return r.ErrStartByPos
}

// StartSyncByGTID implements Reader.StartSyncByGTID.
func (r *MockReader) StartSyncByGTID(gSet gtid.Set) error {
	return r.ErrStartByGTID
}

// Close implements Reader.Close.
func (r *MockReader) Close() error {
	return r.ErrClose
}

// GetEvent implements Reader.GetEvent.
func (r *MockReader) GetEvent(ctx context.Context) (*replication.BinlogEvent, error) {
	select {
	case ev := <-r.ch:
		return ev, nil
	case err := <-r.ech:
		return nil, err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Status implements Reader.Status.
func (r *MockReader) Status() interface{} {
	return nil
}

// PushEvent pushes an event into the reader.
func (r *MockReader) PushEvent(ctx context.Context, ev *replication.BinlogEvent) error {
	select {
	case r.ch <- ev:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// PushError pushes an error into the reader.
func (r *MockReader) PushError(ctx context.Context, err error) error {
	select {
	case r.ech <- err:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
