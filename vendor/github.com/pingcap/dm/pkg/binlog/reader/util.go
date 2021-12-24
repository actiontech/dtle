// Copyright 2020 PingCAP, Inc.
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
	"fmt"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"go.uber.org/zap"

	"github.com/pingcap/dm/pkg/binlog/event"
	"github.com/pingcap/dm/pkg/gtid"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/utils"
	"github.com/pingcap/dm/relay/common"
)

// GetGTIDsForPosFromStreamer tries to get GTID sets for the specified binlog position (for the corresponding txn) from a Streamer.
func GetGTIDsForPosFromStreamer(ctx context.Context, r Streamer, endPos gmysql.Position) (gtid.Set, error) {
	var (
		flavor      string
		latestPos   uint32
		latestGSet  gmysql.GTIDSet
		nextGTIDStr string // can be recorded if the coming transaction completed
		err         error
	)
	for {
		var e *replication.BinlogEvent
		e, err = r.GetEvent(ctx)
		if err != nil {
			return nil, err
		}

		// NOTE: only update endPos/GTIDs for DDL/XID to get an complete transaction.
		switch ev := e.Event.(type) {
		case *replication.QueryEvent:
			parser2, err2 := event.GetParserForStatusVars(ev.StatusVars)
			if err2 != nil {
				log.L().Warn("found error when get sql_mode from binlog status_vars", zap.Error(err2))
			}

			isDDL := common.CheckIsDDL(string(ev.Query), parser2)
			if isDDL {
				if latestGSet == nil {
					// GTID not enabled, can't get GTIDs for the position.
					return nil, errors.Errorf("should have a GTIDEvent before the DDL QueryEvent %+v", e.Header)
				}
				err2 = latestGSet.Update(nextGTIDStr)
				if err2 != nil {
					return nil, terror.Annotatef(err2, "update GTID set %v with GTID %s", latestGSet, nextGTIDStr)
				}
				latestPos = e.Header.LogPos
			}
		case *replication.XIDEvent:
			if latestGSet == nil {
				// GTID not enabled, can't get GTIDs for the position.
				return nil, errors.Errorf("should have a GTIDEvent before the XIDEvent %+v", e.Header)
			}
			err = latestGSet.Update(nextGTIDStr)
			if err != nil {
				return nil, terror.Annotatef(err, "update GTID set %v with GTID %s", latestGSet, nextGTIDStr)
			}
			latestPos = e.Header.LogPos
		case *replication.GTIDEvent:
			if latestGSet == nil {
				return nil, errors.Errorf("should have a PreviousGTIDsEvent before the GTIDEvent %+v", e.Header)
			}
			// learn from: https://github.com/go-mysql-org/go-mysql/blob/c6ab05a85eb86dc51a27ceed6d2f366a32874a24/replication/binlogsyncer.go#L736
			u, _ := uuid.FromBytes(ev.SID)
			nextGTIDStr = fmt.Sprintf("%s:%d", u.String(), ev.GNO)
		case *replication.MariadbGTIDEvent:
			if latestGSet == nil {
				return nil, errors.Errorf("should have a MariadbGTIDListEvent before the MariadbGTIDEvent %+v", e.Header)
			}
			// learn from: https://github.com/go-mysql-org/go-mysql/blob/c6ab05a85eb86dc51a27ceed6d2f366a32874a24/replication/binlogsyncer.go#L745
			GTID := ev.GTID
			nextGTIDStr = fmt.Sprintf("%d-%d-%d", GTID.DomainID, GTID.ServerID, GTID.SequenceNumber)
		case *replication.PreviousGTIDsEvent:
			// if GTID enabled, we can get a PreviousGTIDEvent after the FormatDescriptionEvent
			// ref: https://github.com/mysql/mysql-server/blob/8cc757da3d87bf4a1f07dcfb2d3c96fed3806870/sql/binlog.cc#L4549
			// ref: https://github.com/mysql/mysql-server/blob/8cc757da3d87bf4a1f07dcfb2d3c96fed3806870/sql/binlog.cc#L5161
			var gSet gtid.Set
			gSet, err = gtid.ParserGTID(gmysql.MySQLFlavor, ev.GTIDSets)
			if err != nil {
				return nil, err
			}
			latestGSet = gSet.Origin()
			flavor = gmysql.MySQLFlavor
		case *replication.MariadbGTIDListEvent:
			// a MariadbGTIDListEvent logged in every binlog to record the current replication state if GTID enabled
			// ref: https://mariadb.com/kb/en/library/gtid_list_event/
			gSet, err2 := event.GTIDsFromMariaDBGTIDListEvent(e)
			if err2 != nil {
				return nil, terror.Annotatef(err2, "get GTID set from MariadbGTIDListEvent %+v", e.Header)
			}
			latestGSet = gSet.Origin()
			flavor = gmysql.MariaDBFlavor
		}

		if latestPos == endPos.Pos {
			// reach the end position, return the GTID sets.
			if latestGSet == nil {
				return nil, errors.Errorf("no GTIDs get for position %s", endPos)
			}
			var latestGTIDs gtid.Set
			latestGTIDs, err = gtid.ParserGTID(flavor, latestGSet.String())
			if err != nil {
				return nil, terror.Annotatef(err, "parse GTID set %s with flavor %s", latestGSet.String(), flavor)
			}
			return latestGTIDs, nil
		} else if latestPos > endPos.Pos {
			return nil, errors.Errorf("invalid position %s or GTID not enabled in upstream", endPos)
		}
	}
}

// GetGTIDsForPos tries to get GTID sets for the specified binlog position (for the corresponding txn).
// NOTE: this method is very similar with `relay/writer/file_util.go/getTxnPosGTIDs`, unify them if needed later.
// NOTE: this method is not well tested directly, but more tests have already been done for `relay/writer/file_util.go/getTxnPosGTIDs`.
func GetGTIDsForPos(ctx context.Context, r Reader, endPos gmysql.Position) (gtid.Set, error) {
	// start to get and parse binlog event from the beginning of the file.
	startPos := gmysql.Position{
		Name: endPos.Name,
		Pos:  0,
	}
	err := r.StartSyncByPos(startPos)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	return GetGTIDsForPosFromStreamer(ctx, r, endPos)
}

// GetPreviousGTIDFromGTIDSet tries to get previous GTID sets from Previous_GTID_EVENT GTID for the specified GITD Set.
// events should be [fake_rotate_event,format_description_event,previous_gtids_event/mariadb_gtid_list_event].
func GetPreviousGTIDFromGTIDSet(ctx context.Context, r Reader, gset gtid.Set) (gtid.Set, error) {
	failpoint.Inject("MockGetEmptyPreviousGTIDFromGTIDSet", func(_ failpoint.Value) {
		gset, _ = gtid.ParserGTID("mysql", "")
		failpoint.Return(gset, nil)
	})

	err := r.StartSyncByGTID(gset)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	for {
		var e *replication.BinlogEvent
		e, err = r.GetEvent(ctx)
		if err != nil {
			return nil, err
		}

		switch e.Header.EventType {
		case replication.ROTATE_EVENT:
			if utils.IsFakeRotateEvent(e.Header) {
				continue
			}
			return nil, terror.ErrPreviousGTIDNotExist.Generate(gset.String())
		case replication.FORMAT_DESCRIPTION_EVENT:
			continue
		case replication.PREVIOUS_GTIDS_EVENT:
			previousGset, err := event.GTIDsFromPreviousGTIDsEvent(e)
			return previousGset, err
		case replication.MARIADB_GTID_LIST_EVENT:
			previousGset, err := event.GTIDsFromMariaDBGTIDListEvent(e)
			return previousGset, err
		default:
			return nil, terror.ErrPreviousGTIDNotExist.Generate(gset.String())
		}
	}
}
