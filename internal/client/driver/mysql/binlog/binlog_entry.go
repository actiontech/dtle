/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

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
	Coordinates   base.BinlogCoordinateTx

	Events       []DataEvent
	OriginalSize int // size of binlog entry
}

// NewBinlogEntry creates an empty, ready to go BinlogEntry object
func NewBinlogEntryAt(coordinates base.BinlogCoordinateTx) *BinlogEntry {
	binlogEntry := &BinlogEntry{
		Coordinates:  coordinates,
		Events:       make([]DataEvent, 0),
		OriginalSize: 0,
	}
	return binlogEntry
}

// Duplicate creates and returns a new binlog entry, with some of the attributes pre-assigned
func (b *BinlogEntry) String() string {
	return fmt.Sprintf("[BinlogEntry at %+v]", b.Coordinates)
}
