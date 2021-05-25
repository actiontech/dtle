/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package base

import (
	"testing"

	test "github.com/outbrain/golib/tests"
)

func TestBinlogCoordinates(t *testing.T) {
	c1 := BinlogCoordinatesX{LogFile: "mysql-bin.00017", LogPos: 104}
	c2 := BinlogCoordinatesX{LogFile: "mysql-bin.00017", LogPos: 104}
	c3 := BinlogCoordinatesX{LogFile: "mysql-bin.00017", LogPos: 5000}
	c4 := BinlogCoordinatesX{LogFile: "mysql-bin.00099", LogPos: 104}
	c5 := BinlogCoordinatesX{LogFile: "mysql-bin.00012", LogPos: 5000}
	c6 := BinlogCoordinatesX{LogFile: "mysql-bin.00012", LogPos: 104}

	// equal

	test.S(t).ExpectEquals(c1.CompareFilePos(&c2), 0)    // file = && pos =
	test.S(t).ExpectNotEquals(c1.CompareFilePos(&c3), 0) // file = && pos !=
	test.S(t).ExpectNotEquals(c1.CompareFilePos(&c4), 0) // file != && pos =
	test.S(t).ExpectNotEquals(c3.CompareFilePos(&c4), 0) // file != && pos !=

	// smaller

	test.S(t).ExpectEquals(c1.CompareFilePos(&c3), -1) // file = && pos <
	test.S(t).ExpectEquals(c6.CompareFilePos(&c3), -1) // file < && pos <
	test.S(t).ExpectEquals(c5.CompareFilePos(&c3), -1) // file < && pos =
	test.S(t).ExpectEquals(c5.CompareFilePos(&c1), -1) // file < && pos >

	//// bigger
	test.S(t).ExpectEquals(c3.CompareFilePos(&c1), 1) // file = && pos >
	test.S(t).ExpectEquals(c4.CompareFilePos(&c3), 1) // file > && pos <
	test.S(t).ExpectEquals(c4.CompareFilePos(&c1), 1) // file > && pos =
	test.S(t).ExpectEquals(c3.CompareFilePos(&c6), 1) // file > && pos >
}

func TestBinlogCoordinatesAsKey(t *testing.T) {
	m := make(map[BinlogCoordinatesX]bool)

	c1 := BinlogCoordinatesX{LogFile: "mysql-bin.00017", LogPos: 104}
	c2 := BinlogCoordinatesX{LogFile: "mysql-bin.00022", LogPos: 104}
	c3 := BinlogCoordinatesX{LogFile: "mysql-bin.00017", LogPos: 104}
	c4 := BinlogCoordinatesX{LogFile: "mysql-bin.00017", LogPos: 222}

	m[c1] = true
	m[c2] = true
	m[c3] = true
	m[c4] = true

	test.S(t).ExpectEquals(len(m), 3)
}

func TestBinlogCoordinates_String(t *testing.T) {
	type fields struct {
		LogFile string
		LogPos  int64
		GtidSet string
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{"emptyLog", fields{}, ""},
		{"emptyGtid", fields{LogFile: "mysql-bin.00017", LogPos: 104}, ""},
		{"gtid1", fields{GtidSet: "server_id:121"}, "server_id:121"},
		{"gtid2", fields{GtidSet: "108cc4a4-0d40-11ea-9598-2016d8c96b66:1-5,\nc42216ad-0d37-11ea-b163-2016d8c96b56:1-9,\nceabbacf-0c77-11ea-b49f-2016d8c96b46:1-1662590"}, "108cc4a4-0d40-11ea-9598-2016d8c96b66:1-5,\nc42216ad-0d37-11ea-b163-2016d8c96b56:1-9,\nceabbacf-0c77-11ea-b49f-2016d8c96b46:1-1662590"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := BinlogCoordinatesX{
				LogFile: tt.fields.LogFile,
				LogPos:  tt.fields.LogPos,
				GtidSet: tt.fields.GtidSet,
			}
			if got := b.String(); got != tt.want {
				t.Errorf("BinlogCoordinates.String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBinlogCoordinates_IsEmpty(t *testing.T) {
	type fields struct {
		LogFile string
		LogPos  int64
		GtidSet string
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		{"emptyTrue", fields{}, true},
		{"allEmpty", fields{LogFile: "", LogPos: 0}, true},
		{"gtidEmpty", fields{LogFile: "mysql-bin.00017", LogPos: 104}, false},
		{"logFileEmpty", fields{GtidSet: "server_id:121"}, false},
		{"gtidEmpty", fields{LogFile: "mysql-bin.00017"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &BinlogCoordinatesX{
				LogFile: tt.fields.LogFile,
				LogPos:  tt.fields.LogPos,
				GtidSet: tt.fields.GtidSet,
			}
			if got := b.IsEmpty(); got != tt.want {
				t.Errorf("BinlogCoordinates.IsEmpty() = %v, want %v", got, tt.want)
			}
		})
	}
}
