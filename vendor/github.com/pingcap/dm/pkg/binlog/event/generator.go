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
	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/pingcap/dm/pkg/gtid"
	"github.com/pingcap/dm/pkg/terror"
)

// Generator represents a binlog events generator.
type Generator struct {
	Flavor        string
	ServerID      uint32
	LatestPos     uint32
	LatestGTID    gtid.Set
	PreviousGTIDs gtid.Set
	LatestXID     uint64
}

// NewGenerator creates a new instance of Generator.
func NewGenerator(flavor string, serverID uint32, latestPos uint32, latestGTID gtid.Set, previousGTIDs gtid.Set, latestXID uint64) (*Generator, error) {
	prevOrigin := previousGTIDs.Origin()
	if prevOrigin == nil {
		return nil, terror.ErrPreviousGTIDsNotValid.Generate(previousGTIDs)
	}

	singleGTID, err := verifySingleGTID(flavor, latestGTID)
	if err != nil {
		return nil, terror.Annotate(err, "verify single latest GTID in set")
	}
	switch flavor {
	case gmysql.MySQLFlavor:
		uuidSet := singleGTID.(*gmysql.UUIDSet)
		prevGSet, ok := prevOrigin.(*gmysql.MysqlGTIDSet)
		if !ok || prevGSet == nil {
			return nil, terror.ErrBinlogGTIDMySQLNotValid.Generate(previousGTIDs)
		}
		// latestGTID should be one of the latest previousGTIDs
		prevGTID, ok := prevGSet.Sets[uuidSet.SID.String()]
		if !ok || prevGTID.Intervals.Len() != 1 || prevGTID.Intervals[0].Stop != uuidSet.Intervals[0].Stop {
			return nil, terror.ErrBinlogLatestGTIDNotInPrev.Generate(latestGTID, previousGTIDs)
		}

	case gmysql.MariaDBFlavor:
		mariaGTID := singleGTID.(*gmysql.MariadbGTID)
		if mariaGTID.ServerID != serverID {
			return nil, terror.ErrBinlogMariaDBServerIDMismatch.Generate(mariaGTID.ServerID, serverID)
		}
		// latestGTID should be one of previousGTIDs
		prevGSet, ok := prevOrigin.(*gmysql.MariadbGTIDSet)
		if !ok || prevGSet == nil {
			return nil, terror.ErrBinlogGTIDMariaDBNotValid.Generate(previousGTIDs)
		}
		prevGTID, ok := prevGSet.Sets[mariaGTID.DomainID]
		if !ok || prevGTID.ServerID != mariaGTID.ServerID || prevGTID.SequenceNumber != mariaGTID.SequenceNumber {
			return nil, terror.ErrBinlogLatestGTIDNotInPrev.Generate(latestGTID, previousGTIDs)
		}
	default:
		return nil, terror.ErrBinlogFlavorNotSupport.Generate(flavor)
	}

	return &Generator{
		Flavor:        flavor,
		ServerID:      serverID,
		LatestPos:     latestPos,
		LatestGTID:    latestGTID,
		PreviousGTIDs: previousGTIDs,
		LatestXID:     latestXID,
	}, nil
}

// GenFileHeader generates a binlog file header, including to PreviousGTIDsEvent/MariadbGTIDListEvent.
// for MySQL:
//   1. BinLogFileHeader, [ fe `bin` ]
//   2. FormatDescriptionEvent
//   3. PreviousGTIDsEvent
// for MariaDB:
//   1. BinLogFileHeader, [ fe `bin` ]
//   2. FormatDescriptionEvent
//   3. MariadbGTIDListEvent
func (g *Generator) GenFileHeader() ([]*replication.BinlogEvent, []byte, error) {
	events, data, err := GenCommonFileHeader(g.Flavor, g.ServerID, g.PreviousGTIDs)
	if err != nil {
		return nil, nil, err
	}
	g.LatestPos = uint32(len(data)) // if generate a binlog file header then reset latest pos
	return events, data, nil
}

// GenCreateDatabaseEvents generates binlog events for `CREATE DATABASE`.
// events: [GTIDEvent, QueryEvent]
func (g *Generator) GenCreateDatabaseEvents(schema string) ([]*replication.BinlogEvent, []byte, error) {
	result, err := GenCreateDatabaseEvents(g.Flavor, g.ServerID, g.LatestPos, g.LatestGTID, schema)
	if err != nil {
		return nil, nil, err
	}
	g.updateLatestPosGTID(result.LatestPos, result.LatestGTID)
	return result.Events, result.Data, nil
}

// GenDropDatabaseEvents generates binlog events for `DROP DATABASE`.
// events: [GTIDEvent, QueryEvent]
func (g *Generator) GenDropDatabaseEvents(schema string) ([]*replication.BinlogEvent, []byte, error) {
	result, err := GenDropDatabaseEvents(g.Flavor, g.ServerID, g.LatestPos, g.LatestGTID, schema)
	if err != nil {
		return nil, nil, err
	}
	g.updateLatestPosGTID(result.LatestPos, result.LatestGTID)
	return result.Events, result.Data, nil
}

// GenCreateTableEvents generates binlog events for `CREATE TABLE`.
// events: [GTIDEvent, QueryEvent]
func (g *Generator) GenCreateTableEvents(schema string, query string) ([]*replication.BinlogEvent, []byte, error) {
	result, err := GenCreateTableEvents(g.Flavor, g.ServerID, g.LatestPos, g.LatestGTID, schema, query)
	if err != nil {
		return nil, nil, err
	}
	g.updateLatestPosGTID(result.LatestPos, result.LatestGTID)
	return result.Events, result.Data, nil
}

// GenDropTableEvents generates binlog events for `DROP TABLE`.
// events: [GTIDEvent, QueryEvent]
func (g *Generator) GenDropTableEvents(schema string, table string) ([]*replication.BinlogEvent, []byte, error) {
	result, err := GenDropTableEvents(g.Flavor, g.ServerID, g.LatestPos, g.LatestGTID, schema, table)
	if err != nil {
		return nil, nil, err
	}
	g.updateLatestPosGTID(result.LatestPos, result.LatestGTID)
	return result.Events, result.Data, nil
}

// GenDDLEvents generates binlog events for DDL statements.
// events: [GTIDEvent, QueryEvent]
func (g *Generator) GenDDLEvents(schema string, query string) ([]*replication.BinlogEvent, []byte, error) {
	result, err := GenDDLEvents(g.Flavor, g.ServerID, g.LatestPos, g.LatestGTID, schema, query)
	if err != nil {
		return nil, nil, err
	}
	g.updateLatestPosGTID(result.LatestPos, result.LatestGTID)
	return result.Events, result.Data, nil
}

// GenDMLEvents generates binlog events for `INSERT`/`UPDATE`/`DELETE`.
// events: [GTIDEvent, QueryEvent, TableMapEvent, RowsEvent, ..., XIDEvent]
// NOTE: multi <TableMapEvent, RowsEvent> pairs can be in events.
func (g *Generator) GenDMLEvents(eventType replication.EventType, dmlData []*DMLData) ([]*replication.BinlogEvent, []byte, error) {
	result, err := GenDMLEvents(g.Flavor, g.ServerID, g.LatestPos, g.LatestGTID, eventType, g.LatestXID+1, dmlData)
	if err != nil {
		return nil, nil, err
	}
	g.updateLatestPosGTID(result.LatestPos, result.LatestGTID)
	g.LatestXID++ // increase XID
	return result.Events, result.Data, nil
}

func (g *Generator) updateLatestPosGTID(latestPos uint32, latestGTID gtid.Set) {
	g.LatestPos = latestPos
	g.LatestGTID = latestGTID
}
