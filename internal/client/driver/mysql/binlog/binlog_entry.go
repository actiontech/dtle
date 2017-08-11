package binlog

import (
	"fmt"

	"udup/internal/client/driver/mysql/base"
)

// BinlogEntry describes an entry in the binary log
type BinlogEntry struct {
	Coordinates base.BinlogCoordinates
	EndLogPos   uint64

	Events []DataEvent
}

// NewBinlogEntry creates an empty, ready to go BinlogEntry object
func NewBinlogEntry(logFile string, logPos uint64) *BinlogEntry {
	binlogEntry := &BinlogEntry{
		Coordinates: base.BinlogCoordinates{LogFile: logFile, LogPos: int64(logPos)},
	}
	return binlogEntry
}

// NewBinlogEntry creates an empty, ready to go BinlogEntry object
func NewBinlogEntryAt(coordinates base.BinlogCoordinates) *BinlogEntry {
	binlogEntry := &BinlogEntry{
		Coordinates: coordinates,
		Events:      make([]DataEvent, 0),
	}
	return binlogEntry
}

// Duplicate creates and returns a new binlog entry, with some of the attributes pre-assigned
func (b *BinlogEntry) Duplicate() *BinlogEntry {
	binlogEntry := NewBinlogEntry(b.Coordinates.LogFile, uint64(b.Coordinates.LogPos))
	binlogEntry.EndLogPos = b.EndLogPos
	return binlogEntry
}

// Duplicate creates and returns a new binlog entry, with some of the attributes pre-assigned
func (b *BinlogEntry) String() string {
	return fmt.Sprintf("[BinlogEntry at %+v]", b.Coordinates)
}
