/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package mysqlconfig

import (
	"strings"

	"golang.org/x/text/transform"
)

type ColumnType int

const (
	UnknownColumnType ColumnType = iota
	TimestampColumnType
	DateTimeColumnType
	EnumColumnType
	MediumIntColumnType
	BigIntColumnType
	FloatColumnType
	DoubleColumnType
	DecimalColumnType
	BinaryColumnType
	TextColumnType // 10
	JSONColumnType

	DateColumnType
	TimeColumnType
	YearColumnType

	VarbinaryColumnType

	BitColumnType
	TinytextColumnType
	TinyintColumnType
	SmallintColumnType
	IntColumnType // 20
	SetColumnType
	CharColumnType
	VarcharColumnType
	BlobColumnType
	BooleanColumnType
	// TODO: more type
)

const maxMediumintUnsigned int32 = 16777215

type TimezoneConvertion struct {
	ToTimezone string
}

type Column struct {
	// Every time you set this, you must also set `EscapedName`.
	RawName            string
	EscapedName        string
	IsUnsigned         bool
	Charset            string
	Type               ColumnType
	Default            interface{}
	ColumnType         string
	Key                string
	TimezoneConversion *TimezoneConvertion
	Nullable           bool
	Precision          int // for decimal, time or datetime
	Scale              int // for decimal
	// somehow ugly. A better solution might be MetaInfo with subtypes
}

func (c *Column) IsPk() bool {
	return c.Key == "PRI"
}
// type of arg: see type.schema
func (c *Column) ConvertArg(arg interface{}) interface{} {
	switch v := arg.(type) {
	case []byte:
		if strings.Contains(c.ColumnType, "text") { // TODO make a flag
			if encoding, ok := charsetEncodingMap[c.Charset]; ok {
				arg, _, _ = transform.Bytes(encoding.NewDecoder(), v)
			}
			return arg
		}
	case string:
		if v == "" {
			return ""
		} else {
			// string, charset conversion
			if encoding, ok := charsetEncodingMap[c.Charset]; ok {
				arg, _, _ = transform.String(encoding.NewDecoder(), v)
			}
			return arg
		}
	}

	if c.IsUnsigned {
		switch i := arg.(type) {
		case int8:
			return uint8(i)
		case int16:
			return uint16(i)
		case int32:
			if c.Type == MediumIntColumnType {
				return uint32(i) & 0x00FFFFFF
			} else {
				return uint32(i)
			}
		case int64:
			return uint64(i)
		case int:
			return uint(i)
		default:
			return arg
		}
	}

	return arg
}

func NewColumns(names []string) []Column {
	result := make([]Column, len(names))
	for i := range names {
		result[i].RawName = names[i]
		result[i].EscapedName = EscapeName(names[i])
	}
	return result
}

func ParseColumns(names string) []Column {
	namesArray := strings.Split(names, ",")
	return NewColumns(namesArray)
}

// ColumnsMap maps a column name onto its ordinal position
type ColumnsMap map[string]int

func NewEmptyColumnsMap() ColumnsMap {
	columnsMap := make(map[string]int)
	return ColumnsMap(columnsMap)
}

func NewColumnsMap(orderedColumns []Column) ColumnsMap {
	columnsMap := NewEmptyColumnsMap()
	for i, column := range orderedColumns {
		columnsMap[column.RawName] = i
	}
	return columnsMap
}

// NewColumnList creates an object given ordered list of column names
/*func NewColumnList(names []string) *ColumnList {
	result := &ColumnList{
		Columns: NewColumns(names),
	}
	result.Ordinals = NewColumnsMap(result.Columns)
	return result
}*/

func EscapeName(name string) string {
	sb := strings.Builder{}
	sb.WriteByte('`')
	for i := range name {
		if name[i] == '`' {
			sb.WriteByte('`')
			sb.WriteByte('`')
		} else {
			sb.WriteByte(name[i])
		}
	}
	sb.WriteByte('`')

	return sb.String()
}

func BuildColumnMapIndex(from []string, ordinals ColumnsMap) (mapIndex []int) {
	mapIndex = make([]int, len(from))
	for i, colName := range from {
		idxFrom := ordinals[colName]
		mapIndex[i] = idxFrom
	}
	return mapIndex
}
