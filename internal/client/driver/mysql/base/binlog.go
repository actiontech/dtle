/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package base

import (
	"fmt"

	"github.com/satori/go.uuid"
	gomysql "github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
)

type BinlogEvent struct {
	BinlogFile string
	RealPos    uint32
	Header     *replication.EventHeader
	Evt        replication.Event
	RawBs      []byte
	Query      []string //[]StreamEvent

	Err error
}

type BinlogCoordinateTx struct {
	LogFile string
	LogPos  int64
	// Original SID -- a cycle prevention mechanism
	OSID          string
	SID           uuid.UUID
	GNO           int64
	LastCommitted int64
	SeqenceNumber int64
}

// Do not call this frequently. Cache your result.
func (b *BinlogCoordinateTx) GetSid() string {
	return b.SID.String()
}

// BinlogCoordinates described binary log coordinates in the form of log file & log position.
type BinlogCoordinatesX struct {
	LogFile string
	LogPos  int64
	GtidSet string
}

// String returns a user-friendly string representation of these coordinates
func (b BinlogCoordinatesX) String() string {
	return fmt.Sprintf("%v", b.GtidSet)
}

// Equals tests equality of this corrdinate and another one.
func (b *BinlogCoordinateTx) Equals(other *BinlogCoordinateTx) bool {
	if other == nil {
		return false
	}
	return b.LogFile == other.LogFile && b.LogPos == other.LogPos
}

// IsEmpty returns true if the log file is empty, unnamed
func (b *BinlogCoordinatesX) IsEmpty() bool {
	return b.GtidSet == "" && b.LogFile == ""
}

// SmallerThan returns true if this coordinate is strictly smaller than the other.
func (b *BinlogCoordinateTx) SmallerThan(other *BinlogCoordinateTx) bool {
	if b.LogFile < other.LogFile {
		return true
	}
	if b.LogFile == other.LogFile && b.LogPos < other.LogPos {
		return true
	}
	return false
}

// SmallerThanOrEquals returns true if this coordinate is the same or equal to the other one.
// We do NOT compare the type so we can not use b.Equals()
func (b *BinlogCoordinateTx) SmallerThanOrEquals(other *BinlogCoordinateTx) bool {
	if b.SmallerThan(other) {
		return true
	}
	return b.LogFile == other.LogFile && b.LogPos == other.LogPos // No Type comparison
}

func (b *BinlogCoordinateTx) GetGtidForThisTx() string {
	return fmt.Sprintf("%s:%d", b.GetSid(), b.GNO)
}

type GtidItemMap map[uuid.UUID]*GtidItem
type GtidItem struct {
	NRow      int
}

func IntervalSlicesContainOne(intervals gomysql.IntervalSlice, gno int64) bool {
	for i := range intervals {
		if gno >= intervals[i].Start && gno < intervals[i].Stop {
			return true
		}
	}
	return false
}
