package binlog

import (
	"fmt"

	"udup/internal/client/driver/mysql/base"
)

type BinlogEntries struct {
	Entries []*BinlogEntry
}

// BinlogEntry describes an entry in the binary log
type BinlogEntry struct {
	hasBeginQuery bool
	Coordinates   base.BinlogCoordinates

	Events []DataEvent
	OriginalSize         int // size of binlog entry
}

// NewBinlogEntry creates an empty, ready to go BinlogEntry object
func NewBinlogEntryAt(coordinates base.BinlogCoordinates) *BinlogEntry {
	binlogEntry := &BinlogEntry{
		Coordinates: coordinates,
		Events:      make([]DataEvent, 0),
		OriginalSize: 0,
	}
	return binlogEntry
}

// Duplicate creates and returns a new binlog entry, with some of the attributes pre-assigned
func (b *BinlogEntry) String() string {
	return fmt.Sprintf("[BinlogEntry at %+v]", b.Coordinates)
}
