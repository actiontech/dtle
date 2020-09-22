/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package mysqlconfig

import (
	"fmt"
	"strconv"
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
func (c *Column) ConvertArg(arg interface{}) interface{} {
	if fmt.Sprintf("%s", arg) == "" {
		return ""
	}

	if strings.Contains(c.ColumnType, "text") {
		if encoding, ok := charsetEncodingMap[c.Charset]; ok {
			arg, _, _ = transform.String(encoding.NewDecoder(), fmt.Sprintf("%s", arg))
		}
		return arg
	}
	if s, ok := arg.(string); ok {
		// string, charset conversion
		if encoding, ok := charsetEncodingMap[c.Charset]; ok {
			arg, _, _ = transform.String(encoding.NewDecoder(), s)
		}
		return arg
	}

	if c.IsUnsigned {
		if i, ok := arg.(int8); ok {
			return uint8(i)
		}
		if i, ok := arg.(int16); ok {
			return uint16(i)
		}
		if i, ok := arg.(int32); ok {
			if c.Type == MediumIntColumnType {
				// problem with mediumint is that it's a 3-byte type. There is no compatible golang type to match that.
				// So to convert from negative to positive we'd need to convert the value manually
				if i >= 0 {
					return i
				}
				return uint32(maxMediumintUnsigned + i + 1)
			}
			return uint32(i)
		}
		if i, ok := arg.(int64); ok {
			return strconv.FormatUint(uint64(i), 10)
		}
		if i, ok := arg.(int); ok {
			return uint(i)
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
