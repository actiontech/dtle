package base

import (
	"fmt"
	"regexp"
	"github.com/siddontang/go-mysql/replication"
)

var detachPattern *regexp.Regexp

func init() {
	detachPattern, _ = regexp.Compile(`//([^/:]+):([\d]+)`) // e.g. `//binlog.01234:567890`
}

type BinlogType int

const (
	BinaryLog BinlogType = iota
	RelayLog
)

type OpType byte

const (
	insert = iota + 1
	update
	del
	ddl
	xid
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

// BinlogCoordinates described binary log coordinates in the form of log file & log position.
type BinlogCoordinates struct {
	LogFile       string
	LogPos        int64
	OSID          string
	SID           string
	GNO           int64
	LastCommitted int64
	Type          BinlogType
	GtidSet       string
}

// DisplayString returns a user-friendly string representation of these coordinates
func (b *BinlogCoordinates) DisplayString() string {
	return fmt.Sprintf("%v", b.GtidSet)
}

// String returns a user-friendly string representation of these coordinates
func (b BinlogCoordinates) String() string {
	return b.DisplayString()
}

// Equals tests equality of this corrdinate and another one.
func (b *BinlogCoordinates) Equals(other *BinlogCoordinates) bool {
	if other == nil {
		return false
	}
	return b.LogFile == other.LogFile && b.LogPos == other.LogPos && b.Type == other.Type
}

// IsEmpty returns true if the log file is empty, unnamed
func (b *BinlogCoordinates) IsEmpty() bool {
	return b.GtidSet == ""
}

// SmallerThan returns true if this coordinate is strictly smaller than the other.
func (b *BinlogCoordinates) SmallerThan(other *BinlogCoordinates) bool {
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
func (b *BinlogCoordinates) SmallerThanOrEquals(other *BinlogCoordinates) bool {
	if b.SmallerThan(other) {
		return true
	}
	return b.LogFile == other.LogFile && b.LogPos == other.LogPos // No Type comparison
}

func (b *BinlogCoordinates) GetGtidForThisTx() string {
	return fmt.Sprintf("%s:%d", b.SID, b.GNO)
}
