package mysql

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"golang.org/x/text/transform"
	"sort"
)

type ColumnType int

const (
	UnknownColumnType   ColumnType = iota
	TimestampColumnType            = iota
	DateTimeColumnType             = iota
	EnumColumnType                 = iota
	MediumIntColumnType            = iota
	BigIntColumnType               = iota
	FloatColumnType                = iota
	DoubleColumnType               = iota
	DecimalColumnType              = iota
)

const maxMediumintUnsigned int32 = 16777215

type TimezoneConvertion struct {
	ToTimezone string
}

type Column struct {
	Idx                int
	Name               string
	IsUnsigned         bool
	Charset            string
	Type               ColumnType
	TimezoneConversion *TimezoneConvertion
}

func (c *Column) ConvertArg(arg interface{}) interface{} {
	if s, ok := arg.(string); ok {
		// string, charset conversion
		if encoding, ok := charsetEncodingMap[c.Charset]; ok {
			arg, _, _ = transform.String(encoding.NewDecoder(), s)
			fmt.Println(s)
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
		result[i].Name = names[i]
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
		columnsMap[column.Name] = i
	}
	return columnsMap
}

// ColumnList makes for a named list of columns
type ColumnList struct {
	Columns  []Column
	Ordinals ColumnsMap
}

// NewColumnList creates an object given ordered list of column names
func NewColumnList(names []string) *ColumnList {
	result := &ColumnList{
		Columns: NewColumns(names),
	}
	result.Ordinals = NewColumnsMap(result.Columns)
	return result
}

// ParseColumnList parses a comma delimited list of column names
func ParseColumnList(names string) *ColumnList {
	result := &ColumnList{
		Columns: ParseColumns(names),
	}
	result.Ordinals = NewColumnsMap(result.Columns)
	return result
}

func (c *ColumnList) ColumnList() []Column {
	return c.Columns
}

func (c *ColumnList) Names() []string {
	names := make([]string, len(c.Columns))
	for i := range c.Columns {
		names[i] = c.Columns[i].Name
	}
	return names
}

func (c *ColumnList) GetColumn(columnName string) *Column {
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

func (c *ColumnList) SetColumnType(columnName string, columnType ColumnType) {
	c.GetColumn(columnName).Type = columnType
}

func (c *ColumnList) GetColumnType(columnName string) ColumnType {
	return c.GetColumn(columnName).Type
}

func (c *ColumnList) SetConvertDatetimeToTimestamp(columnName string, toTimezone string) {
	c.GetColumn(columnName).TimezoneConversion = &TimezoneConvertion{ToTimezone: toTimezone}
}

func (c *ColumnList) HasTimezoneConversion(columnName string) bool {
	return c.GetColumn(columnName).TimezoneConversion != nil
}

func (c *ColumnList) String() string {
	return strings.Join(c.Names(), ",")
}

func (c *ColumnList) Equals(other *ColumnList) bool {
	return reflect.DeepEqual(c.Columns, other.Columns)
}

func (c *ColumnList) EqualsByNames(other *ColumnList) bool {
	return reflect.DeepEqual(c.Names(), other.Names())
}

// IsSubsetOf returns 'true' when column names of this list are a subset of
// another list, in arbitrary order (order agnostic)
func (c *ColumnList) IsSubsetOf(other *ColumnList) bool {
	for _, column := range c.Columns {
		if _, exists := other.Ordinals[column.Name]; !exists {
			return false
		}
	}
	return true
}

func (c *ColumnList) Len() int {
	return len(c.Columns)
}

type TableWithForeignKey struct {
	ReferencedTableSchema string
	ReferencedTableName   string
	TableSchema           string
	TableName             string
	Index                 int
}

type TableWrapper struct {
	Table []TableWithForeignKey
	By    func(p, q *TableWithForeignKey) bool
}

type SortBy func(p, q *TableWithForeignKey) bool

func (pw TableWrapper) Len() int { // Len() Method overriding
	return len(pw.Table)
}
func (pw TableWrapper) Swap(i, j int) { // Swap() Method overriding
	pw.Table[i], pw.Table[j] = pw.Table[j], pw.Table[i]
}
func (pw TableWrapper) Less(i, j int) bool { // Less() Method overriding
	return pw.By(&pw.Table[i], &pw.Table[j])
}

func SortTable(table []TableWithForeignKey, by SortBy) { // SortTable method
	sort.Sort(TableWrapper{table, by})
}

// UniqueKey is the combination of a key's name and columns
type UniqueKey struct {
	Name            string
	Columns         ColumnList
	HasNullable     bool
	IsAutoIncrement bool
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

type ColumnValues struct {
	AbstractValues []interface{}
	ValuesPointers []interface{}
}

func NewColumnValues(length int) *ColumnValues {
	result := &ColumnValues{
		AbstractValues: make([]interface{}, length),
		ValuesPointers: make([]interface{}, length),
	}
	for i := 0; i < length; i++ {
		result.ValuesPointers[i] = &result.AbstractValues[i]
	}

	return result
}

func ToColumnValues(abstractValues []interface{}) *ColumnValues {
	result := &ColumnValues{
		AbstractValues: abstractValues,
		ValuesPointers: make([]interface{}, len(abstractValues)),
	}
	for i := 0; i < len(abstractValues); i++ {
		result.AbstractValues[i] = result.StringColumn(i)
		result.ValuesPointers[i] = &result.AbstractValues[i]
	}

	return result
}

func (c *ColumnValues) GetAbstractValues() []interface{} {
	return c.AbstractValues
}

func (c *ColumnValues) StringColumn(index int) string {
	val := c.GetAbstractValues()[index]
	if ints, ok := val.([]uint8); ok {
		return string(ints)
	}
	return fmt.Sprintf("%+v", val)
}

func (c *ColumnValues) String() string {
	stringValues := []string{}
	for i := range c.GetAbstractValues() {
		stringValues = append(stringValues, c.StringColumn(i))
	}
	return strings.Join(stringValues, ",")
}
