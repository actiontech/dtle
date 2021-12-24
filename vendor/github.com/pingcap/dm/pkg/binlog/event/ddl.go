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
	"fmt"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/pingcap/dm/pkg/gtid"
	"github.com/pingcap/dm/pkg/terror"
)

// GenCreateDatabaseEvents generates binlog events for `CREATE DATABASE`.
// events: [GTIDEvent, QueryEvent]
func GenCreateDatabaseEvents(flavor string, serverID uint32, latestPos uint32, latestGTID gtid.Set, schema string) (*DDLDMLResult, error) {
	query := fmt.Sprintf("CREATE DATABASE `%s`", schema)
	return GenDDLEvents(flavor, serverID, latestPos, latestGTID, schema, query)
}

// GenDropDatabaseEvents generates binlog events for `DROP DATABASE`.
// events: [GTIDEvent, QueryEvent]
func GenDropDatabaseEvents(flavor string, serverID uint32, latestPos uint32, latestGTID gtid.Set, schema string) (*DDLDMLResult, error) {
	query := fmt.Sprintf("DROP DATABASE `%s`", schema)
	return GenDDLEvents(flavor, serverID, latestPos, latestGTID, schema, query)
}

// GenCreateTableEvents generates binlog events for `CREATE TABLE`.
// events: [GTIDEvent, QueryEvent]
// NOTE: we do not support all `column type` and `column meta` for DML now, so the caller should restrict the `query` statement.
func GenCreateTableEvents(flavor string, serverID uint32, latestPos uint32, latestGTID gtid.Set, schema string, query string) (*DDLDMLResult, error) {
	return GenDDLEvents(flavor, serverID, latestPos, latestGTID, schema, query)
}

// GenDropTableEvents generates binlog events for `DROP TABLE`.
// events: [GTIDEvent, QueryEvent]
func GenDropTableEvents(flavor string, serverID uint32, latestPos uint32, latestGTID gtid.Set, schema string, table string) (*DDLDMLResult, error) {
	query := fmt.Sprintf("DROP TABLE `%s`.`%s`", schema, table)
	return GenDDLEvents(flavor, serverID, latestPos, latestGTID, schema, query)
}

// GenDDLEvents generates binlog events for DDL statements.
// events: [GTIDEvent, QueryEvent]
func GenDDLEvents(flavor string, serverID uint32, latestPos uint32, latestGTID gtid.Set, schema string, query string) (*DDLDMLResult, error) {
	// GTIDEvent, increase GTID first
	latestGTID, err := GTIDIncrease(flavor, latestGTID)
	if err != nil {
		return nil, terror.Annotatef(err, "increase GTID %s", latestGTID)
	}
	gtidEv, err := GenCommonGTIDEvent(flavor, serverID, latestPos, latestGTID)
	if err != nil {
		return nil, terror.Annotate(err, "generate GTIDEvent")
	}
	latestPos = gtidEv.Header.LogPos

	// QueryEvent
	header := &replication.EventHeader{
		Timestamp: uint32(time.Now().Unix()),
		ServerID:  serverID,
		Flags:     defaultHeaderFlags,
	}
	queryEv, err := GenQueryEvent(header, latestPos, defaultSlaveProxyID, defaultExecutionTime, defaultErrorCode, defaultStatusVars, []byte(schema), []byte(query))
	if err != nil {
		return nil, terror.Annotatef(err, "generate QueryEvent for schema %s, query %s", schema, query)
	}
	latestPos = queryEv.Header.LogPos

	var buf bytes.Buffer
	_, err = buf.Write(gtidEv.RawData)
	if err != nil {
		return nil, terror.ErrBinlogWriteDataToBuffer.AnnotateDelegate(err, "write GTIDEvent data % X", gtidEv.RawData)
	}
	_, err = buf.Write(queryEv.RawData)
	if err != nil {
		return nil, terror.ErrBinlogWriteDataToBuffer.AnnotateDelegate(err, "write QueryEvent data % X", queryEv.RawData)
	}

	return &DDLDMLResult{
		Events:     []*replication.BinlogEvent{gtidEv, queryEv},
		Data:       buf.Bytes(),
		LatestPos:  latestPos,
		LatestGTID: latestGTID,
	}, nil
}
