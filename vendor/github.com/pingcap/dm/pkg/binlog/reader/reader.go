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

	gmysql "github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"

	"github.com/pingcap/dm/pkg/gtid"
)

// Reader is a binlog event reader, it may read binlog events from a TCP stream, binlog files or any other in-memory buffer.
// One reader should read binlog events either through position mode or GTID mode.
type Reader interface {
	// StartSyncByPos prepares the reader for reading binlog from the specified position.
	StartSyncByPos(pos gmysql.Position) error

	// StartSyncByGTID prepares the reader for reading binlog from the specified GTID set.
	StartSyncByGTID(gSet gtid.Set) error

	// Close closes the reader and release the resource.
	// Close will be blocked if `GetEvent` has not returned.
	Close() error

	// GetEvent gets the binlog event one by one, it will block if no event can be read.
	// You can pass a context (like Cancel or Timeout) to break the block.
	GetEvent(ctx context.Context) (*replication.BinlogEvent, error)

	// Status returns the status of the reader.
	Status() interface{}
}
