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

// binlog events generator for MySQL used to generate some binlog events for tests.
// Readability takes precedence over performance.

package event

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"math"
	"reflect"

	"github.com/pingcap/errors"
	gmysql "github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
)

var (
	columnTypeMismatchErrFmt = "value %+v (type %v) with column type %v"
)

// encodeTableMapColumnMeta generates the column_meta_def according to the column_type_def.
// NOTE: we should pass more arguments for some type def later, now simply hard-code them.
// ref: https://dev.mysql.com/doc/internals/en/table-map-event.html
// ref: https://github.com/siddontang/go-mysql/blob/88e9cd7f6643b246b4dcc0e3206e9a169dd0ac96/replication/row_event.go#L100
func encodeTableMapColumnMeta(columnType []byte) ([]byte, error) {
	buf := new(bytes.Buffer)
	for _, t := range columnType {
		switch t {
		case gmysql.MYSQL_TYPE_STRING:
			buf.WriteByte(0xfe) // real type
			buf.WriteByte(0xff) // pack or field length
		case gmysql.MYSQL_TYPE_NEWDECIMAL:
			buf.WriteByte(0x12) // precision, 18
			buf.WriteByte(0x09) // decimals, 9
		case gmysql.MYSQL_TYPE_VAR_STRING, gmysql.MYSQL_TYPE_VARCHAR, gmysql.MYSQL_TYPE_BIT:
			buf.WriteByte(0xff)
			buf.WriteByte(0xff)
		case gmysql.MYSQL_TYPE_BLOB, gmysql.MYSQL_TYPE_DOUBLE, gmysql.MYSQL_TYPE_FLOAT, gmysql.MYSQL_TYPE_GEOMETRY, gmysql.MYSQL_TYPE_JSON,
			gmysql.MYSQL_TYPE_TIME2, gmysql.MYSQL_TYPE_DATETIME2, gmysql.MYSQL_TYPE_TIMESTAMP2:
			buf.WriteByte(0xff)
		case gmysql.MYSQL_TYPE_NEWDATE, gmysql.MYSQL_TYPE_ENUM, gmysql.MYSQL_TYPE_SET, gmysql.MYSQL_TYPE_TINY_BLOB, gmysql.MYSQL_TYPE_MEDIUM_BLOB, gmysql.MYSQL_TYPE_LONG_BLOB:
			return nil, errors.NotSupportedf("column type %d in binlog", t)
		}
	}
	return gmysql.PutLengthEncodedString(buf.Bytes()), nil
}

// decodeTableMapColumnMeta generates the column_meta_def to uint16 slices.
// ref: https://github.com/siddontang/go-mysql/blob/88e9cd7f6643b246b4dcc0e3206e9a169dd0ac96/replication/row_event.go#L100
func decodeTableMapColumnMeta(data []byte, columnType []byte) ([]uint16, error) {
	pos := 0
	columnMeta := make([]uint16, len(columnType))
	for i, t := range columnType {
		switch t {
		case gmysql.MYSQL_TYPE_STRING:
			var x = uint16(data[pos]) << 8 //real type
			x += uint16(data[pos+1])       //pack or field length
			columnMeta[i] = x
			pos += 2
		case gmysql.MYSQL_TYPE_NEWDECIMAL:
			var x = uint16(data[pos]) << 8 //precision
			x += uint16(data[pos+1])       //decimals
			columnMeta[i] = x
			pos += 2
		case gmysql.MYSQL_TYPE_VAR_STRING, gmysql.MYSQL_TYPE_VARCHAR, gmysql.MYSQL_TYPE_BIT:
			columnMeta[i] = binary.LittleEndian.Uint16(data[pos:])
			pos += 2
		case gmysql.MYSQL_TYPE_BLOB, gmysql.MYSQL_TYPE_DOUBLE, gmysql.MYSQL_TYPE_FLOAT, gmysql.MYSQL_TYPE_GEOMETRY, gmysql.MYSQL_TYPE_JSON,
			gmysql.MYSQL_TYPE_TIME2, gmysql.MYSQL_TYPE_DATETIME2, gmysql.MYSQL_TYPE_TIMESTAMP2:
			columnMeta[i] = uint16(data[pos])
			pos++
		case gmysql.MYSQL_TYPE_NEWDATE, gmysql.MYSQL_TYPE_ENUM, gmysql.MYSQL_TYPE_SET, gmysql.MYSQL_TYPE_TINY_BLOB, gmysql.MYSQL_TYPE_MEDIUM_BLOB, gmysql.MYSQL_TYPE_LONG_BLOB:
			return nil, errors.NotSupportedf("column type %d in binlog", t)
		default:
			columnMeta[i] = 0
		}
	}

	return columnMeta, nil
}

// bitmapByteSize returns the byte length of bitmap for columnCount.
func bitmapByteSize(columnCount int) int {
	return (columnCount + 7) / 8
}

// nullBytes returns a n-length null bytes slice
func nullBytes(n int) []byte {
	return make([]byte, n)
}

// fullBytes returns a n-length full bytes slice (all bits are set)
func fullBytes(n int) []byte {
	buf := new(bytes.Buffer)
	for i := 0; i < n; i++ {
		buf.WriteByte(0xff)
	}
	return buf.Bytes()
}

// assembleEvent assembles header fields, postHeader and payload together to an event.
// header: pass as a struct to make a copy
func assembleEvent(buf *bytes.Buffer, event replication.Event, decodeWithChecksum bool, header replication.EventHeader, eventType replication.EventType, latestPos uint32, postHeader, payload []byte) (*replication.BinlogEvent, error) {
	eventSize := uint32(eventHeaderLen) + uint32(len(postHeader)) + uint32(len(payload)) + crc32Len
	// update some fields in header
	header.EventSize = eventSize
	header.LogPos = latestPos + eventSize
	header.EventType = eventType
	headerData, err := GenEventHeader(&header)
	if err != nil {
		return nil, errors.Annotate(err, "generate event header")
	}

	err = combineHeaderPayload(buf, headerData, postHeader, payload)
	if err != nil {
		return nil, errors.Annotate(err, "combine header, post-header and payload")
	}

	// CRC32 checksum, 4 bytes
	checksum := crc32.ChecksumIEEE(buf.Bytes())
	err = binary.Write(buf, binary.LittleEndian, checksum)
	if err != nil {
		return nil, errors.Annotatef(err, "write CRC32 % X", checksum)
	}

	if event == nil {
		return nil, nil // not need to decode the event
	}

	// decode event, some implementations of `Decode` also need checksum
	var endIdx = buf.Len()
	if !decodeWithChecksum {
		endIdx -= int(crc32Len)
	}
	err = event.Decode(buf.Bytes()[eventHeaderLen:endIdx])
	if err != nil {
		return nil, errors.Annotatef(err, "decode % X", buf.Bytes())
	}

	return &replication.BinlogEvent{RawData: buf.Bytes(), Header: &header, Event: event}, nil
}

// combineHeaderPayload combines header, postHeader and payload together.
func combineHeaderPayload(buf *bytes.Buffer, header, postHeader, payload []byte) error {
	if len(header) != int(eventHeaderLen) {
		return errors.NotValidf("header length should be %d, but got %d", eventHeaderLen, len(header))
	}

	err := binary.Write(buf, binary.LittleEndian, header)
	if err != nil {
		return errors.Annotatef(err, "write event header % X", header)
	}

	if len(postHeader) > 0 { // postHeader maybe empty
		err = binary.Write(buf, binary.LittleEndian, postHeader)
		if err != nil {
			return errors.Annotatef(err, "write event post-header % X", postHeader)
		}
	}

	err = binary.Write(buf, binary.LittleEndian, payload)
	if err != nil {
		return errors.Annotatef(err, "write event payload % X", payload)
	}

	return nil
}

// encodeColumnValue encodes value to bytes
// ref: https://github.com/siddontang/go-mysql/blob/88e9cd7f6643b246b4dcc0e3206e9a169dd0ac96/replication/row_event.go#L368
// NOTE: we do not generate meaningful `meta` yet.
func encodeColumnValue(v interface{}, tp byte, meta uint16) ([]byte, error) {
	var (
		buf = new(bytes.Buffer)
		err error
	)
	switch tp {
	case gmysql.MYSQL_TYPE_NULL:
		return nil, nil
	case gmysql.MYSQL_TYPE_LONG:
		err = writeIntegerColumnValue(buf, v, reflect.TypeOf(int32(0)))
	case gmysql.MYSQL_TYPE_TINY:
		err = writeIntegerColumnValue(buf, v, reflect.TypeOf(int8(0)))
	case gmysql.MYSQL_TYPE_SHORT:
		err = writeIntegerColumnValue(buf, v, reflect.TypeOf(int16(0)))
	case gmysql.MYSQL_TYPE_INT24:
		err = writeIntegerColumnValue(buf, v, reflect.TypeOf(int32(0)))
		if err == nil {
			buf.Truncate(3)
		}
	case gmysql.MYSQL_TYPE_LONGLONG:
		err = writeIntegerColumnValue(buf, v, reflect.TypeOf(int64(0)))
	case gmysql.MYSQL_TYPE_FLOAT:
		value, ok := v.(float32)
		if !ok {
			err = errors.NotValidf(columnTypeMismatchErrFmt, v, reflect.TypeOf(v), reflect.TypeOf(float32(0)))
		} else {
			bits := math.Float32bits(value)
			err = writeIntegerColumnValue(buf, bits, reflect.TypeOf(uint32(0)))
		}
	case gmysql.MYSQL_TYPE_DOUBLE:
		value, ok := v.(float64)
		if !ok {
			err = errors.NotValidf(columnTypeMismatchErrFmt, v, reflect.TypeOf(v), reflect.TypeOf(float64(0)))
		} else {
			bits := math.Float64bits(value)
			err = writeIntegerColumnValue(buf, bits, reflect.TypeOf(uint64(0)))
		}
	case gmysql.MYSQL_TYPE_STRING:
		err = writeStringColumnValue(buf, v)
	case gmysql.MYSQL_TYPE_VARCHAR, gmysql.MYSQL_TYPE_VAR_STRING,
		gmysql.MYSQL_TYPE_NEWDECIMAL, gmysql.MYSQL_TYPE_BIT,
		gmysql.MYSQL_TYPE_TIMESTAMP, gmysql.MYSQL_TYPE_TIMESTAMP2,
		gmysql.MYSQL_TYPE_DATETIME, gmysql.MYSQL_TYPE_DATETIME2,
		gmysql.MYSQL_TYPE_TIME, gmysql.MYSQL_TYPE_TIME2,
		gmysql.MYSQL_TYPE_YEAR, gmysql.MYSQL_TYPE_ENUM, gmysql.MYSQL_TYPE_SET,
		gmysql.MYSQL_TYPE_BLOB, gmysql.MYSQL_TYPE_JSON, gmysql.MYSQL_TYPE_GEOMETRY:
		// this generator is used for testing, so some types supporting can be added later.
		err = errors.NotSupportedf("go-mysql type %d in event generator", tp)
	default:
		err = errors.NotValidf("go-mysql type %d in event generator", tp)
	}
	return buf.Bytes(), errors.Annotatef(err, "go-mysql type %d", tp)
}

// writeIntegerColumnValue writes integer value to bytes buffer.
func writeIntegerColumnValue(buf *bytes.Buffer, value interface{}, valueType reflect.Type) error {
	if reflect.TypeOf(value) != valueType {
		return errors.NotValidf(columnTypeMismatchErrFmt, value, reflect.TypeOf(value), valueType)
	}
	return errors.Trace(binary.Write(buf, binary.LittleEndian, value))
}

// writeStringColumnValue writes string value to bytes buffer.
func writeStringColumnValue(buf *bytes.Buffer, value interface{}) error {
	str, ok := value.(string)
	if !ok {
		return errors.NotValidf(columnTypeMismatchErrFmt, value, reflect.TypeOf(value), reflect.TypeOf(""))
	}
	var (
		err    error
		length = len(str)
	)
	if length < 256 {
		err = binary.Write(buf, binary.LittleEndian, uint8(length))
		if err == nil {
			err = binary.Write(buf, binary.LittleEndian, []byte(str))
		}
	} else {
		err = binary.Write(buf, binary.LittleEndian, uint16(length))
		if err != nil {
			err = binary.Write(buf, binary.LittleEndian, []byte(str))
		}
	}
	return errors.Trace(err)
}
