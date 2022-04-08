package common

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/actiontech/dtle/drivers/driver/mysql/mysqlconfig"
)

func RowColumnIsNull(row []interface{}, index int) bool {
	return row[index] == nil
}
func RowGetBytesColumn(row []interface{}, index int) []byte {
	val := row[index]
	switch v := val.(type) {
	case []byte:
		return v
	case string:
		return []byte(v)
	default:
		buf := bytes.NewBuffer(nil)
		_ = binary.Write(buf, binary.LittleEndian, v)
		return buf.Bytes()
	}
}

// ColumnList makes for a named list of columns
type ColumnList struct {
	Columns  []mysqlconfig.Column
	Ordinals mysqlconfig.ColumnsMap
	UniqueKeys []*UniqueKey
}

// NewColumnList creates an object given ordered list of column names
func NewColumnList(columns []mysqlconfig.Column) *ColumnList {
	result := &ColumnList{
		Columns: columns,
	}
	result.Ordinals = mysqlconfig.NewColumnsMap(result.Columns)
	return result
}

// ParseColumnList parses a comma delimited list of column names
func ParseColumnList(names string, tableColumns *ColumnList) *ColumnList {
	r := &ColumnList{
		Columns: mysqlconfig.ParseColumns(names),
	}
	r.Ordinals = make(mysqlconfig.ColumnsMap)
	for i := range r.Columns {
		colName := r.Columns[i].RawName
		r.Ordinals[colName] = tableColumns.Ordinals[colName]
	}
	return r
}

func (c *ColumnList) ColumnList() []mysqlconfig.Column {
	return c.Columns
}

func (c *ColumnList) Names() []string {
	names := make([]string, len(c.Columns))
	for i := range c.Columns {
		names[i] = c.Columns[i].RawName
	}
	return names
}
func (c *ColumnList) EscapedNames() []string {
	names := make([]string, len(c.Columns))
	for i := range c.Columns {
		names[i] = c.Columns[i].EscapedName
	}
	return names
}

// TODO caller doesn't handle nil.
func (c *ColumnList) GetColumn(columnName string) *mysqlconfig.Column {
	if ordinal, ok := c.Ordinals[columnName]; ok {
		return &c.Columns[ordinal]
	}
	return nil
}

func (c *ColumnList) SetUnsigned(columnName string) {
	c.GetColumn(columnName).IsUnsigned = true
}

func (c *ColumnList) IsUnsigned(columnName string) bool {
	return c.GetColumn(columnName).IsUnsigned
}

func (c *ColumnList) SetCharset(columnName string, charset string) {
	c.GetColumn(columnName).Charset = charset
}

func (c *ColumnList) GetCharset(columnName string) string {
	return c.GetColumn(columnName).Charset
}

func (c *ColumnList) SetColumnType(columnName string, columnType mysqlconfig.ColumnType) {
	c.GetColumn(columnName).Type = columnType
}

func (c *ColumnList) GetColumnType(columnName string) mysqlconfig.ColumnType {
	return c.GetColumn(columnName).Type
}

func (c *ColumnList) SetConvertDatetimeToTimestamp(columnName string, toTimezone string) {
	c.GetColumn(columnName).TimezoneConversion = &mysqlconfig.TimezoneConvertion{ToTimezone: toTimezone}
}

func (c *ColumnList) HasTimezoneConversion(columnName string) bool {
	return c.GetColumn(columnName).TimezoneConversion != nil
}

func (c *ColumnList) String() string {
	return strings.Join(c.Names(), ",")
}

// IsSubsetOf returns 'true' when column names of this list are a subset of
// another list, in arbitrary order (order agnostic)
func (c *ColumnList) IsSubsetOf(other *ColumnList) bool {
	for _, column := range c.Columns {
		if _, exists := other.Ordinals[column.RawName]; !exists {
			return false
		}
	}
	return true
}

func (c *ColumnList) Len() int {
	return len(c.Columns)
}

// UniqueKey is the combination of a key's name and columns
type UniqueKey struct {
	Name            string
	Columns         ColumnList
	HasNullable     bool
	IsAutoIncrement bool
	LastMaxVals     []string
}

// IsPrimary checks if this unique key is primary
func (c *UniqueKey) IsPrimary() bool {
	return c.Name == "PRIMARY"
}

func (c *UniqueKey) Len() int {
	return c.Columns.Len()
}

func (c *UniqueKey) String() string {
	description := c.Name
	if c.IsAutoIncrement {
		description = fmt.Sprintf("%s (auto_increment)", description)
	}
	return fmt.Sprintf("%s: %s; has nullable: %+v", description, c.Columns.Names(), c.HasNullable)
}
