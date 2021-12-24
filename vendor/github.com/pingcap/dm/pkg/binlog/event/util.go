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
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"reflect"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/mysql"

	"github.com/pingcap/dm/pkg/terror"
)

// encodeTableMapColumnMeta generates the column_meta_def according to the column_type_def.
// NOTE: we should pass more arguments for some type def later, now simply hard-code them.
// ref: https://dev.mysql.com/doc/internals/en/table-map-event.html
// ref: https://github.com/go-mysql-org/go-mysql/blob/88e9cd7f6643b246b4dcc0e3206e9a169dd0ac96/replication/row_event.go#L100
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
			return nil, terror.ErrBinlogColumnTypeNotSupport.Generate(t)
		}
	}
	return gmysql.PutLengthEncodedString(buf.Bytes()), nil
}

// decodeTableMapColumnMeta generates the column_meta_def to uint16 slices.
// ref: https://github.com/go-mysql-org/go-mysql/blob/88e9cd7f6643b246b4dcc0e3206e9a169dd0ac96/replication/row_event.go#L100
func decodeTableMapColumnMeta(data []byte, columnType []byte) ([]uint16, error) {
	pos := 0
	columnMeta := make([]uint16, len(columnType))
	for i, t := range columnType {
		switch t {
		case gmysql.MYSQL_TYPE_STRING:
			x := uint16(data[pos]) << 8 // real type
			x += uint16(data[pos+1])    // pack or field length
			columnMeta[i] = x
			pos += 2
		case gmysql.MYSQL_TYPE_NEWDECIMAL:
			x := uint16(data[pos]) << 8 // precision
			x += uint16(data[pos+1])    // decimals
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
			return nil, terror.ErrBinlogColumnTypeNotSupport.Generate(t)
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

// nullBytes returns a n-length null bytes slice.
func nullBytes(n int) []byte {
	return make([]byte, n)
}

// fullBytes returns a n-length full bytes slice (all bits are set).
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
		return nil, terror.Annotate(err, "generate event header")
	}

	err = combineHeaderPayload(buf, headerData, postHeader, payload)
	if err != nil {
		return nil, terror.Annotate(err, "combine header, post-header and payload")
	}

	// CRC32 checksum, 4 bytes
	checksum := crc32.ChecksumIEEE(buf.Bytes())
	err = binary.Write(buf, binary.LittleEndian, checksum)
	if err != nil {
		return nil, terror.ErrBinlogWriteBinaryData.AnnotateDelegate(err, "write CRC32 % X", checksum)
	}

	if event == nil {
		return nil, nil // not need to decode the event
	}

	// decode event, some implementations of `Decode` also need checksum
	endIdx := buf.Len()
	if !decodeWithChecksum {
		endIdx -= int(crc32Len)
	}
	err = event.Decode(buf.Bytes()[eventHeaderLen:endIdx])
	if err != nil {
		return nil, terror.ErrBinlogEventDecode.Delegate(err, buf.Bytes())
	}

	return &replication.BinlogEvent{RawData: buf.Bytes(), Header: &header, Event: event}, nil
}

// combineHeaderPayload combines header, postHeader and payload together.
func combineHeaderPayload(buf *bytes.Buffer, header, postHeader, payload []byte) error {
	if len(header) != int(eventHeaderLen) {
		return terror.ErrBinlogHeaderLengthNotValid.Generate(eventHeaderLen, len(header))
	}

	err := binary.Write(buf, binary.LittleEndian, header)
	if err != nil {
		return terror.ErrBinlogWriteBinaryData.AnnotateDelegate(err, "write event header % X", header)
	}

	if len(postHeader) > 0 { // postHeader maybe empty
		err = binary.Write(buf, binary.LittleEndian, postHeader)
		if err != nil {
			return terror.ErrBinlogWriteBinaryData.AnnotateDelegate(err, "write event post-header % X", postHeader)
		}
	}

	err = binary.Write(buf, binary.LittleEndian, payload)
	if err != nil {
		return terror.ErrBinlogWriteBinaryData.AnnotateDelegate(err, "write event payload % X", payload)
	}

	return nil
}

// encodeColumnValue encodes value to bytes
// ref: https://github.com/go-mysql-org/go-mysql/blob/88e9cd7f6643b246b4dcc0e3206e9a169dd0ac96/replication/row_event.go#L368
// NOTE: we do not generate meaningful `meta` yet.
// nolint:unparam
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
			err = terror.ErrBinlogColumnTypeMisMatch.Generate(v, reflect.TypeOf(v), reflect.TypeOf(float32(0)))
		} else {
			bits := math.Float32bits(value)
			err = writeIntegerColumnValue(buf, bits, reflect.TypeOf(uint32(0)))
		}
	case gmysql.MYSQL_TYPE_DOUBLE:
		value, ok := v.(float64)
		if !ok {
			err = terror.ErrBinlogColumnTypeMisMatch.Generate(v, reflect.TypeOf(v), reflect.TypeOf(float64(0)))
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
		err = terror.ErrBinlogGoMySQLTypeNotSupport.Generate(tp)
	default:
		err = terror.ErrBinlogGoMySQLTypeNotSupport.Generate(tp)
	}
	return buf.Bytes(), terror.Annotatef(err, "go-mysql type %d", tp)
}

// writeIntegerColumnValue writes integer value to bytes buffer.
func writeIntegerColumnValue(buf *bytes.Buffer, value interface{}, valueType reflect.Type) error {
	if reflect.TypeOf(value) != valueType {
		return terror.ErrBinlogColumnTypeMisMatch.Generate(value, reflect.TypeOf(value), valueType)
	}
	return terror.ErrBinlogWriteBinaryData.Delegate(binary.Write(buf, binary.LittleEndian, value))
}

// writeStringColumnValue writes string value to bytes buffer.
func writeStringColumnValue(buf *bytes.Buffer, value interface{}) error {
	str, ok := value.(string)
	if !ok {
		return terror.ErrBinlogColumnTypeMisMatch.Generate(value, reflect.TypeOf(value), reflect.TypeOf(""))
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
	return terror.ErrBinlogWriteBinaryData.Delegate(err)
}

// https://dev.mysql.com/doc/internals/en/query-event.html
// and after Q_COMMIT_TS could be found in
// https://github.com/mysql/mysql-server/blob/124c7ab1d6f914637521fd4463a993aa73403513/libbinlogevents/include/statement_events.h#L500
// Q_HRNOW (MariaDB) could be found in
// https://github.com/MariaDB/server/blob/09a1f0075a8d5752dd7b2940a20d86a040af1741/sql/log_event.h#L321
const (
	QFlags2Code = iota
	QSqlModeCode
	QCatalog
	QAutoIncrement
	QCharsetCode
	QTimeZoneCode
	QCatalogNzCode
	QLcTimeNamesCode
	QCharsetDatabaseCode
	QTableMapForUpdateCode
	QMasterDataWrittenCode
	QInvokers
	QUpdatedDBNames
	QMicroseconds
	QCommitTS
	QCommitTS2
	QExplicitDefaultsForTimestamp
	QDdlLoggedWithXid
	QDefaultCollationForUtf8mb4
	QSqlRequirePrimaryKey
	QDefaultTableEncryption
	QHrnow = 128
)

// https://dev.mysql.com/doc/internals/en/query-event.html
var statusVarsFixedLength = map[byte]int{
	QFlags2Code:            4,
	QSqlModeCode:           8,
	QAutoIncrement:         2 + 2,
	QCharsetCode:           2 + 2 + 2,
	QLcTimeNamesCode:       2,
	QCharsetDatabaseCode:   2,
	QTableMapForUpdateCode: 8,
	QMasterDataWrittenCode: 4,
	QMicroseconds:          3,
	QCommitTS:              0, // unused now
	QCommitTS2:             0, // unused now
	// below variables could be find in
	// https://github.com/mysql/mysql-server/blob/7d10c82196c8e45554f27c00681474a9fb86d137/libbinlogevents/src/statement_events.cpp#L312
	QExplicitDefaultsForTimestamp: 1,
	QDdlLoggedWithXid:             8,
	QDefaultCollationForUtf8mb4:   2,
	QSqlRequirePrimaryKey:         1,
	QDefaultTableEncryption:       1,
	// https://github.com/MariaDB/server/blob/94b45787045677c106a25ebb5aaf1273040b2ff6/sql/log_event.cc#L1619
	QHrnow: 3,
}

// getSQLMode gets SQL mode from binlog statusVars, still could return a reasonable value if found error.
func getSQLMode(statusVars []byte) (mysql.SQLMode, error) {
	vars, err := statusVarsToKV(statusVars)
	b, ok := vars[QSqlModeCode]

	if !ok {
		if err == nil {
			// only happen when this is a dummy event generated by DM
			err = fmt.Errorf("Q_SQL_MODE_CODE not found in status_vars %v", statusVars)
		}
		return mysql.ModeNone, err
	}

	r := bytes.NewReader(b)
	var v int64
	_ = binary.Read(r, binary.LittleEndian, &v)

	return mysql.SQLMode(v), err
}

// GetParserForStatusVars gets a parser for binlog which is suitable for its sql_mode in statusVars.
func GetParserForStatusVars(statusVars []byte) (*parser.Parser, error) {
	parser2 := parser.New()
	mode, err := getSQLMode(statusVars)
	parser2.SetSQLMode(mode)
	return parser2, err
}

// if returned error is `io.EOF`, it means UnexpectedEOF because we handled expected `io.EOF` as success
// returned map should not be nil for other usage.
func statusVarsToKV(statusVars []byte) (map[byte][]byte, error) {
	r := bytes.NewReader(statusVars)
	vars := make(map[byte][]byte)
	var value []byte

	// NOTE: this closure modifies variable `value`
	appendLengthThenCharsToValue := func() error {
		length, err := r.ReadByte()
		if err != nil {
			return err
		}
		value = append(value, length)

		buf := make([]byte, length)
		n, err := r.Read(buf)
		if err != nil {
			return err
		}
		if n != int(length) {
			return io.EOF
		}
		value = append(value, buf...)
		return nil
	}

	generateError := func(err error) (map[byte][]byte, error) {
		offset, _ := r.Seek(0, io.SeekCurrent)
		return vars, terror.ErrBinlogStatusVarsParse.Delegate(err, statusVars, offset)
	}

	for {
		// reset value
		value = make([]byte, 0)
		key, err := r.ReadByte()
		if err == io.EOF {
			break
		}
		if err != nil {
			return generateError(err)
		}

		if _, ok := vars[key]; ok {
			return generateError(errors.New("duplicate key"))
		}

		if length, ok := statusVarsFixedLength[key]; ok {
			value = make([]byte, length)
			n, err2 := r.Read(value)
			if err2 != nil || n != length {
				return generateError(io.EOF)
			}

			vars[key] = value
			continue
		}

		// get variable-length value of according key and save it in `value`
		switch key {
		// 1-byte length + <length> chars of the catalog + '0'-char
		case QCatalog:
			if err = appendLengthThenCharsToValue(); err != nil {
				return generateError(err)
			}

			b, err2 := r.ReadByte()
			if err2 != nil {
				return generateError(err)
			}
			// nolint:makezero
			value = append(value, b)
		// 1-byte length + <length> chars of the timezone/catalog
		case QTimeZoneCode, QCatalogNzCode:
			if err = appendLengthThenCharsToValue(); err != nil {
				return generateError(err)
			}
		// 1-byte length + <length> bytes username and 1-byte length + <length> bytes hostname
		case QInvokers:
			if err = appendLengthThenCharsToValue(); err != nil {
				return generateError(err)
			}
			if err = appendLengthThenCharsToValue(); err != nil {
				return generateError(err)
			}
		// 1-byte count + <count> \0 terminated string
		case QUpdatedDBNames:
			count, err := r.ReadByte()
			if err != nil {
				return generateError(err)
			}
			// nolint:makezero
			value = append(value, count)
			// if count is 254 (OVER_MAX_DBS_IN_EVENT_MTS), there's no following DB names
			// https://github.com/mysql/mysql-server/blob/ee4455a33b10f1b1886044322e4893f587b319ed/libbinlogevents/include/binlog_event.h#L107
			if count == 254 {
				break
			}

			buf := make([]byte, 0, 128)
			b := byte(1) // initialize to any non-zero value
			for ; count > 0; count-- {
				// read one zero-terminated string
				for b != 0 {
					b, err = r.ReadByte()
					if err != nil {
						return generateError(err)
					}
					buf = append(buf, b)
				}
				b = byte(1) // reset to any non-zero value
			}
			// nolint:makezero
			value = append(value, buf...)
		default:
			return generateError(errors.New("unrecognized key"))
		}
		vars[key] = value
	}

	return vars, nil
}
