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

package event

import (
	"bytes"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/pingcap/dm/pkg/gtid"
	"github.com/pingcap/dm/pkg/terror"
)

// DMLData represents data used to generate events for DML statements.
type DMLData struct {
	TableID    uint64
	Schema     string
	Table      string
	ColumnType []byte
	Rows       [][]interface{}
}

// GenDMLEvents generates binlog events for `INSERT`/`UPDATE`/`DELETE`.
// events: [GTIDEvent, QueryEvent, TableMapEvent, RowsEvent, ..., XIDEvent]
// NOTE: multi <TableMapEvent, RowsEvent> pairs can be in events.
func GenDMLEvents(flavor string, serverID uint32, latestPos uint32, latestGTID gtid.Set, eventType replication.EventType, xid uint64, dmlData []*DMLData) (*DDLDMLResult, error) {
	if len(dmlData) == 0 {
		return nil, terror.ErrBinlogDMLEmptyData.Generate()
	}

	// GTIDEvent, increase GTID first.
	latestGTID, err := GTIDIncrease(flavor, latestGTID)
	if err != nil {
		return nil, terror.Annotatef(err, "increase GTID %s", latestGTID)
	}
	gtidEv, err := GenCommonGTIDEvent(flavor, serverID, latestPos, latestGTID)
	if err != nil {
		return nil, terror.Annotate(err, "generate GTIDEvent")
	}
	latestPos = gtidEv.Header.LogPos

	// QueryEvent, `BEGIN`
	header := &replication.EventHeader{
		Timestamp: uint32(time.Now().Unix()),
		ServerID:  serverID,
		Flags:     defaultHeaderFlags,
	}
	query := []byte("BEGIN")
	queryEv, err := GenQueryEvent(header, latestPos, defaultSlaveProxyID, defaultExecutionTime, defaultErrorCode, defaultStatusVars, nil, query)
	if err != nil {
		return nil, terror.Annotate(err, "generate QueryEvent for `BEGIN` statement")
	}
	latestPos = queryEv.Header.LogPos

	// all events
	events := make([]*replication.BinlogEvent, 0, 5)
	events = append(events, gtidEv)
	events = append(events, queryEv)

	// <TableMapEvent, RowsEvent> pairs
	for _, data := range dmlData {
		// TableMapEvent
		tableMapEv, err2 := GenTableMapEvent(header, latestPos, data.TableID, []byte(data.Schema), []byte(data.Table), data.ColumnType)
		if err2 != nil {
			return nil, terror.Annotatef(err2, "generate TableMapEvent for `%s`.`%s`", data.Schema, data.Table)
		}
		latestPos = tableMapEv.Header.LogPos
		events = append(events, tableMapEv)

		// RowsEvent
		rowsEv, err2 := GenRowsEvent(header, latestPos, eventType, data.TableID, defaultRowsFlag, data.Rows, data.ColumnType, tableMapEv)
		if err2 != nil {
			return nil, terror.Annotatef(err2, "generate RowsEvent for `%s`.`%s`", data.Schema, data.Table)
		}
		latestPos = rowsEv.Header.LogPos
		events = append(events, rowsEv)
	}

	// XIDEvent
	xidEv, err := GenXIDEvent(header, latestPos, xid)
	if err != nil {
		return nil, terror.Annotatef(err, "generate XIDEvent for %d", xid)
	}
	latestPos = xidEv.Header.LogPos
	events = append(events, xidEv)

	var buf bytes.Buffer
	for _, ev := range events {
		_, err = buf.Write(ev.RawData)
		if err != nil {
			return nil, terror.ErrBinlogWriteDataToBuffer.AnnotateDelegate(err, "write %d data % X", ev.Header.EventType, ev.RawData)
		}
	}

	return &DDLDMLResult{
		Events:     events,
		Data:       buf.Bytes(),
		LatestPos:  latestPos,
		LatestGTID: latestGTID,
	}, nil
}
