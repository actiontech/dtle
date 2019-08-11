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

package writer

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
	uuid "github.com/satori/go.uuid"
	gmysql "github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"

	"github.com/pingcap/dm/pkg/binlog/event"
	"github.com/pingcap/dm/pkg/binlog/reader"
	"github.com/pingcap/dm/pkg/gtid"
	"github.com/pingcap/dm/relay/common"
)

// checkBinlogHeaderExist checks if the file has a binlog file header.
// It is not safe if there other routine is writing the file.
func checkBinlogHeaderExist(filename string) (bool, error) {
	f, err := os.Open(filename)
	if err != nil {
		return false, errors.Annotatef(err, "open file %s", filename)
	}
	defer f.Close()

	return checkBinlogHeaderExistFd(f)
}

// checkBinlogHeaderExistFd checks if the file has a binlog file header.
// It is not safe if there other routine is writing the file.
func checkBinlogHeaderExistFd(fd *os.File) (bool, error) {
	fileHeaderLen := len(replication.BinLogFileHeader)
	buff := make([]byte, fileHeaderLen)
	n, err := fd.Read(buff)
	if err != nil {
		if n == 0 && err == io.EOF {
			return false, nil // empty file
		}
		return false, errors.Annotate(err, "read binlog header")
	} else if n != fileHeaderLen {
		return false, errors.Errorf("binlog file %s has no enough data, only got % X", fd.Name(), buff[:n])
	}

	if !bytes.Equal(buff, replication.BinLogFileHeader) {
		return false, errors.Errorf("binlog file %s header not valid, got % X, expect % X", fd.Name(), buff, replication.BinLogFileHeader)
	}
	return true, nil
}

// checkFormatDescriptionEventExist checks if the file has a valid FormatDescriptionEvent.
// It is not safe if there other routine is writing the file.
func checkFormatDescriptionEventExist(filename string) (bool, error) {
	f, err := os.Open(filename)
	if err != nil {
		return false, errors.Annotatef(err, "open file %s", filename)
	}
	defer f.Close()

	// FormatDescriptionEvent always follows the binlog file header
	exist, err := checkBinlogHeaderExistFd(f)
	if err != nil {
		return false, errors.Annotatef(err, "check binlog file header for %s", filename)
	} else if !exist {
		return false, errors.Errorf("no binlog file header at the beginning for %s", filename)
	}

	// check whether only the file header
	fileHeaderLen := len(replication.BinLogFileHeader)
	fs, err := f.Stat()
	if err != nil {
		return false, errors.Errorf("get stat for %s", filename)
	} else if fs.Size() == int64(fileHeaderLen) {
		return false, nil // only the file header
	}

	// seek to the beginning of the FormatDescriptionEvent
	_, err = f.Seek(int64(fileHeaderLen), io.SeekStart)
	if err != nil {
		return false, errors.Annotatef(err, "seek to %d for %s", fileHeaderLen, filename)
	}

	// parse a FormatDescriptionEvent
	var found bool
	onEventFunc := func(e *replication.BinlogEvent) error {
		if e.Header.EventType != replication.FORMAT_DESCRIPTION_EVENT {
			return errors.Errorf("got %+v, expect FormatDescriptionEvent", e.Header)
		} else if (e.Header.LogPos - e.Header.EventSize) != uint32(fileHeaderLen) {
			return errors.Errorf("wrong offset %d for FormatDescriptionEvent, should be %d", e.Header.LogPos, fileHeaderLen)
		}
		found = true
		return nil
	}

	// only parse single event
	eof, err := replication.NewBinlogParser().ParseSingleEvent(f, onEventFunc)
	if found {
		return found, nil // if found is true, we return `true` even meet an error, because FormatDescriptionEvent exists.
	} else if err != nil {
		return false, errors.Annotatef(err, "parse %s", filename)
	} else if eof {
		return false, errors.Annotatef(io.EOF, "parse %s", filename)
	}
	return found, nil
}

// checkIsDuplicateEvent checks if the event is a duplicate event in the file.
// It is not safe if there other routine is writing the file.
// NOTE: handle cases when file size > 4GB
func checkIsDuplicateEvent(filename string, ev *replication.BinlogEvent) (bool, error) {
	// 1. check event start/end pos with the file size, and it's enough for most cases
	fs, err := os.Stat(filename)
	if err != nil {
		return false, errors.Annotatef(err, "get stat for %s", filename)
	}
	evStartPos := int64(ev.Header.LogPos - ev.Header.EventSize)
	evEndPos := int64(ev.Header.LogPos)
	if fs.Size() <= evStartPos {
		return false, nil // the event not in the file
	} else if fs.Size() < evEndPos {
		// the file can not hold the whole event, often because the file is corrupt
		return false, errors.Errorf("file size %d is between event's start pos (%d) and end pos (%d)",
			fs.Size(), evStartPos, evEndPos)
	}

	// 2. compare the file data with the raw data of the event
	f, err := os.Open(filename)
	if err != nil {
		return false, errors.Annotate(err, "open binlog file")
	}
	defer f.Close()
	buf := make([]byte, ev.Header.EventSize)
	_, err = f.ReadAt(buf, evStartPos)
	if err != nil {
		return false, errors.Annotatef(err, "read data from %d in %s with length %d", evStartPos, filename, len(buf))
		// } else if bytes.Compare(buf, ev.RawData) != 0 {
	} else if !bytes.Equal(buf, ev.RawData) {
		return false, errors.Errorf("event from %d in %s diff from passed-in event %+v", evStartPos, filename, ev.Header)
	}

	// duplicate in the file
	return true, nil
}

// getTxnPosGTIDs gets position/GTID set for all completed transactions from a binlog file.
// It is not safe if there other routine is writing the file.
// NOTE: we use a int64 rather than a uint32 to represent the latest transaction's end log pos.
func getTxnPosGTIDs(filename string, p *parser.Parser) (int64, gtid.Set, error) {
	// use a FileReader to parse the binlog file.
	rCfg := &reader.FileReaderConfig{
		EnableRawMode: false, // in order to get GTID set, we always disable RawMode.
	}
	startPos := gmysql.Position{Name: filename, Pos: 0} // always start from the file header
	r := reader.NewFileReader(rCfg)
	defer r.Close()
	err := r.StartSyncByPos(startPos) // we always parse the file by pos
	if err != nil {
		return 0, nil, errors.Annotatef(err, "start sync by pos %s for %s", startPos, filename)
	}

	var (
		latestPos   int64
		latestGSet  gmysql.GTIDSet
		nextGTIDStr string // can be recorded if the coming transaction completed
		flavor      string
	)
	for {
		var e *replication.BinlogEvent
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		e, err = r.GetEvent(ctx)
		cancel()
		if err != nil {
			break // now, we stop to parse for any errors
		}

		// NOTE: only update pos/GTID set for DDL/XID to get an complete transaction.
		switch ev := e.Event.(type) {
		case *replication.QueryEvent:
			isDDL := common.CheckIsDDL(string(ev.Query), p)
			if isDDL {
				if latestGSet != nil { // GTID may not be enabled in the binlog
					err = latestGSet.Update(nextGTIDStr)
					if err != nil {
						return 0, nil, errors.Annotatef(err, "update GTID set %v with GTID %s", latestGSet, nextGTIDStr)
					}
				}
				latestPos = int64(e.Header.LogPos)
			}
		case *replication.XIDEvent:
			if latestGSet != nil { // GTID may not be enabled in the binlog
				err = latestGSet.Update(nextGTIDStr)
				if err != nil {
					return 0, nil, errors.Annotatef(err, "update GTID set %v with GTID %s", latestGSet, nextGTIDStr)
				}
			}
			latestPos = int64(e.Header.LogPos)
		case *replication.GTIDEvent:
			if latestGSet == nil {
				return 0, nil, errors.Errorf("should have a PreviousGTIDsEvent before the GTIDEvent %+v", e.Header)
			}
			// learn from: https://github.com/siddontang/go-mysql/blob/c6ab05a85eb86dc51a27ceed6d2f366a32874a24/replication/binlogsyncer.go#L736
			u, _ := uuid.FromBytes(ev.SID)
			nextGTIDStr = fmt.Sprintf("%s:%d", u.String(), ev.GNO)
		case *replication.MariadbGTIDEvent:
			if latestGSet == nil {
				return 0, nil, errors.Errorf("should have a MariadbGTIDListEvent before the MariadbGTIDEvent %+v", e.Header)
			}
			// learn from: https://github.com/siddontang/go-mysql/blob/c6ab05a85eb86dc51a27ceed6d2f366a32874a24/replication/binlogsyncer.go#L745
			GTID := ev.GTID
			nextGTIDStr = fmt.Sprintf("%d-%d-%d", GTID.DomainID, GTID.ServerID, GTID.SequenceNumber)
		case *replication.MariadbGTIDListEvent:
			// a MariadbGTIDListEvent logged in every binlog to record the current replication state if GTID enabled
			// ref: https://mariadb.com/kb/en/library/gtid_list_event/
			gSet, err2 := event.GTIDsFromMariaDBGTIDListEvent(e)
			if err2 != nil {
				return 0, nil, errors.Annotatef(err2, "get GTID set from MariadbGTIDListEvent %+v", e.Header)
			}
			latestGSet = gSet.Origin()
			flavor = gmysql.MariaDBFlavor
		case *replication.GenericEvent:
			if e.Header.EventType == replication.PREVIOUS_GTIDS_EVENT {
				// if GTID enabled, we can get a PreviousGTIDEvent after the FormatDescriptionEvent
				// ref: https://github.com/mysql/mysql-server/blob/8cc757da3d87bf4a1f07dcfb2d3c96fed3806870/sql/binlog.cc#L4549
				// ref: https://github.com/mysql/mysql-server/blob/8cc757da3d87bf4a1f07dcfb2d3c96fed3806870/sql/binlog.cc#L5161
				gSet, err2 := event.GTIDsFromPreviousGTIDsEvent(e)
				if err2 != nil {
					return 0, nil, errors.Annotatef(err2, "get GTID set from PreviousGTIDsEvent %+v", e.Header)
				}
				latestGSet = gSet.Origin()
				flavor = gmysql.MySQLFlavor
			}
		}
	}

	var latestGTIDs gtid.Set
	if latestGSet != nil {
		latestGTIDs, err = gtid.ParserGTID(flavor, latestGSet.String())
		if err != nil {
			return 0, nil, errors.Annotatef(err, "parse GTID set %s with flavor %s", latestGSet.String(), flavor)
		}
	}

	return latestPos, latestGTIDs, nil
}
