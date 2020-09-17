package common

import (
	"fmt"
	"github.com/satori/go.uuid"
)

// Do not call this frequently. Cache your result.
func (b *BinlogCoordinateTx) GetSid() string {
	return uuid.UUID(b.SID).String()
}

// Equals tests equality of this corrdinate and another one.
func (b *BinlogCoordinateTx) Equals(other *BinlogCoordinateTx) bool {
	if other == nil {
		return false
	}
	return b.LogFile == other.LogFile && b.LogPos == other.LogPos
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

type BinlogCoordinateTx struct {
	LogFile       string
	LogPos        int64
	OSID          string
	SID           [16]byte
	GNO           int64
	LastCommitted int64
	SeqenceNumber int64
}
