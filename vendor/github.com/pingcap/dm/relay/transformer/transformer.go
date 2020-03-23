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

package transformer

import (
	"github.com/pingcap/parser"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"

	"github.com/pingcap/dm/relay/common"
)

// Result represents a transform result.
type Result struct {
	Ignore      bool          // whether the event should be ignored
	LogPos      uint32        // binlog event's End_log_pos or Position in RotateEvent
	NextLogName string        // next binlog filename, only valid for RotateEvent
	GTIDSet     mysql.GTIDSet // GTIDSet got from QueryEvent and XIDEvent when RawModeEnabled not true
	CanSaveGTID bool          // whether can save GTID into meta, true for DDL query and XIDEvent
}

// Transformer receives binlog events from a reader and transforms them.
// The transformed binlog events should be send to one or more writers.
// The transformer should support:
//   1. extract binlog position, GTID info from the event.
//   2. decide the event whether needed by a downstream writer.
//     - the downstream writer may also drop some events according to its strategy.
// NOTE: more features maybe moved from outer into Transformer later.
type Transformer interface {
	// Transform transforms a binlog event.
	Transform(e *replication.BinlogEvent) Result
}

// transformer implements Transformer interface.
type transformer struct {
	parser2 *parser.Parser // used to parse query statement
}

// NewTransformer creates a Transformer instance.
func NewTransformer(parser2 *parser.Parser) Transformer {
	return &transformer{
		parser2: parser2,
	}
}

// Transform implements Transformer.Transform.
func (t *transformer) Transform(e *replication.BinlogEvent) Result {
	result := Result{
		LogPos: e.Header.LogPos,
	}

	switch ev := e.Event.(type) {
	case *replication.RotateEvent:
		result.LogPos = uint32(ev.Position)         // next event's position
		result.NextLogName = string(ev.NextLogName) // for RotateEvent, update binlog name
		// NOTE: we need to get the first binlog filename from fake RotateEvent when using auto position
	case *replication.QueryEvent:
		// when RawModeEnabled not true, QueryEvent will be parsed.
		// even for `BEGIN`, we still update pos/GTID, but only save GTID for DDL.
		result.GTIDSet = ev.GSet
		isDDL := common.CheckIsDDL(string(ev.Query), t.parser2)
		if isDDL {
			result.CanSaveGTID = true
		}
	case *replication.XIDEvent:
		// when RawModeEnabled not true, XIDEvent will be parsed.
		result.GTIDSet = ev.GSet
		result.CanSaveGTID = true // need save GTID for XID
	case *replication.GenericEvent:
		// handle some un-parsed events
		switch e.Header.EventType {
		case replication.HEARTBEAT_EVENT:
			// ignore artificial heartbeat event
			// ref: https://dev.mysql.com/doc/internals/en/heartbeat-event.html
			result.Ignore = true
		}
	default:
		if e.Header.Flags&replication.LOG_EVENT_ARTIFICIAL_F != 0 {
			// ignore events with LOG_EVENT_ARTIFICIAL_F flag(0x0020) set
			// ref: https://dev.mysql.com/doc/internals/en/binlog-event-flag.html
			result.Ignore = true
		}
	}
	return result
}
