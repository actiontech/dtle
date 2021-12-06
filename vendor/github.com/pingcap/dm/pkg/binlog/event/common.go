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

	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/pingcap/dm/pkg/gtid"
	"github.com/pingcap/dm/pkg/terror"
)

// DDLDMLResult represents a binlog event result for generated DDL/DML.
type DDLDMLResult struct {
	Events     []*replication.BinlogEvent
	Data       []byte // data contain all events
	LatestPos  uint32
	LatestGTID gtid.Set
}

// GenCommonFileHeader generates a common binlog file header.
// for MySQL:
//   1. BinLogFileHeader, [ fe `bin` ]
//   2. FormatDescriptionEvent
//   3. PreviousGTIDsEvent
// for MariaDB:
//   1. BinLogFileHeader, [ fe `bin` ]
//   2. FormatDescriptionEvent
//   3. MariadbGTIDListEvent
//   -. MariadbBinlogCheckPointEvent, not added yet
func GenCommonFileHeader(flavor string, serverID uint32, gSet gtid.Set) ([]*replication.BinlogEvent, []byte, error) {
	var (
		header = &replication.EventHeader{
			Timestamp: uint32(time.Now().Unix()),
			ServerID:  serverID,
			Flags:     defaultHeaderFlags,
		}
		latestPos   = uint32(len(replication.BinLogFileHeader))
		prevGTIDsEv *replication.BinlogEvent
	)

	formatDescEv, err := GenFormatDescriptionEvent(header, latestPos)
	if err != nil {
		return nil, nil, terror.Annotate(err, "generate FormatDescriptionEvent")
	}
	latestPos += uint32(len(formatDescEv.RawData)) // update latestPos

	switch flavor {
	case gmysql.MySQLFlavor:
		prevGTIDsEv, err = GenPreviousGTIDsEvent(header, latestPos, gSet)
	case gmysql.MariaDBFlavor:
		prevGTIDsEv, err = GenMariaDBGTIDListEvent(header, latestPos, gSet)
	default:
		return nil, nil, terror.ErrBinlogFlavorNotSupport.Generate(flavor)
	}
	if err != nil {
		return nil, nil, terror.Annotate(err, "generate PreviousGTIDsEvent/MariadbGTIDListEvent")
	}

	var buf bytes.Buffer
	_, err = buf.Write(replication.BinLogFileHeader)
	if err != nil {
		return nil, nil, terror.ErrBinlogWriteDataToBuffer.AnnotateDelegate(err, "write binlog file header % X", replication.BinLogFileHeader)
	}
	_, err = buf.Write(formatDescEv.RawData)
	if err != nil {
		return nil, nil, terror.ErrBinlogWriteDataToBuffer.AnnotateDelegate(err, "write FormatDescriptionEvent % X", formatDescEv.RawData)
	}
	_, err = buf.Write(prevGTIDsEv.RawData)
	if err != nil {
		return nil, nil, terror.ErrBinlogWriteDataToBuffer.AnnotateDelegate(err, "write PreviousGTIDsEvent/MariadbGTIDListEvent % X", prevGTIDsEv.RawData)
	}

	events := []*replication.BinlogEvent{formatDescEv, prevGTIDsEv}
	return events, buf.Bytes(), nil
}

// GenCommonGTIDEvent generates a common GTID event.
func GenCommonGTIDEvent(flavor string, serverID uint32, latestPos uint32, gSet gtid.Set) (*replication.BinlogEvent, error) {
	singleGTID, err := verifySingleGTID(flavor, gSet)
	if err != nil {
		return nil, terror.Annotate(err, "verify single GTID in set")
	}

	var (
		header = &replication.EventHeader{
			Timestamp: uint32(time.Now().Unix()),
			ServerID:  serverID,
			Flags:     defaultHeaderFlags,
		}
		gtidEv *replication.BinlogEvent
	)

	switch flavor {
	case gmysql.MySQLFlavor:
		uuidSet := singleGTID.(*gmysql.UUIDSet)
		interval := uuidSet.Intervals[0]
		gtidEv, err = GenGTIDEvent(header, latestPos, defaultGTIDFlags, uuidSet.SID.String(), interval.Start, defaultLastCommitted, defaultSequenceNumber)
	case gmysql.MariaDBFlavor:
		mariaGTID := singleGTID.(*gmysql.MariadbGTID)
		if mariaGTID.ServerID != header.ServerID {
			return nil, terror.ErrBinlogMariaDBServerIDMismatch.Generate(mariaGTID.ServerID, header.ServerID)
		}
		gtidEv, err = GenMariaDBGTIDEvent(header, latestPos, mariaGTID.SequenceNumber, mariaGTID.DomainID)
		if err != nil {
			return gtidEv, err
		}
		// in go-mysql, set ServerID in parseEvent. we try to set it directly
		gtidEvBody := gtidEv.Event.(*replication.MariadbGTIDEvent)
		gtidEvBody.GTID.ServerID = header.ServerID
	default:
		err = terror.ErrBinlogGTIDSetNotValid.Generate(gSet, flavor)
	}
	return gtidEv, err
}

// GTIDIncrease returns a new GTID with GNO/SequenceNumber +1.
func GTIDIncrease(flavor string, gSet gtid.Set) (gtid.Set, error) {
	singleGTID, err := verifySingleGTID(flavor, gSet)
	if err != nil {
		return nil, terror.Annotate(err, "verify single GTID in set")
	}
	clone := gSet.Clone()

	switch flavor {
	case gmysql.MySQLFlavor:
		uuidSet := singleGTID.(*gmysql.UUIDSet)
		uuidSet.Intervals[0].Start++
		uuidSet.Intervals[0].Stop++
		gtidSet := new(gmysql.MysqlGTIDSet)
		gtidSet.Sets = map[string]*gmysql.UUIDSet{uuidSet.SID.String(): uuidSet}
		err = clone.Set(gtidSet)
	case gmysql.MariaDBFlavor:
		mariaGTID := singleGTID.(*gmysql.MariadbGTID)
		mariaGTID.SequenceNumber++
		gtidSet := new(gmysql.MariadbGTIDSet)
		gtidSet.Sets = map[uint32]*gmysql.MariadbGTID{mariaGTID.DomainID: mariaGTID}
		err = clone.Set(gtidSet)
	default:
		err = terror.ErrBinlogGTIDSetNotValid.Generate(gSet, flavor)
	}
	return clone, err
}

// verifySingleGTID verifies gSet whether only containing a single valid GTID.
func verifySingleGTID(flavor string, gSet gtid.Set) (interface{}, error) {
	if gSet == nil || len(gSet.String()) == 0 {
		return nil, terror.ErrBinlogEmptyGTID.Generate()
	}
	origin := gSet.Origin()
	if origin == nil {
		return nil, terror.ErrBinlogGTIDMySQLNotValid.Generate(gSet)
	}

	switch flavor {
	case gmysql.MySQLFlavor:
		mysqlGTIDs, ok := origin.(*gmysql.MysqlGTIDSet)
		if !ok {
			return nil, terror.ErrBinlogGTIDMySQLNotValid.Generate(gSet)
		}
		if len(mysqlGTIDs.Sets) != 1 {
			return nil, terror.ErrBinlogOnlyOneGTIDSupport.Generate(len(mysqlGTIDs.Sets), gSet)
		}
		var uuidSet *gmysql.UUIDSet
		for _, uuidSet = range mysqlGTIDs.Sets {
		}
		intervals := uuidSet.Intervals
		if intervals.Len() != 1 {
			return nil, terror.ErrBinlogOnlyOneIntervalInUUID.Generate(intervals.Len(), gSet)
		}
		interval := intervals[0]
		if interval.Stop != interval.Start+1 {
			return nil, terror.ErrBinlogIntervalValueNotValid.Generate(interval, gSet)
		}
		return uuidSet, nil
	case gmysql.MariaDBFlavor:
		mariaGTIDs, ok := origin.(*gmysql.MariadbGTIDSet)
		if !ok {
			return nil, terror.ErrBinlogGTIDMariaDBNotValid.Generate(gSet)
		}
		if len(mariaGTIDs.Sets) != 1 {
			return nil, terror.ErrBinlogOnlyOneGTIDSupport.Generate(len(mariaGTIDs.Sets), gSet)
		}
		var mariaGTID *gmysql.MariadbGTID
		for _, mariaGTID = range mariaGTIDs.Sets {
		}
		return mariaGTID, nil
	default:
		return nil, terror.ErrBinlogGTIDSetNotValid.Generate(gSet, flavor)
	}
}
