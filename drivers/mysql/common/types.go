package common

import (
	"fmt"
	"strings"
)

func (c *ColumnValues) GetAbstractValues() []*interface{} {
	return c.AbstractValues
}

func (c *ColumnValues) StringColumn(index int) string {
	val := *c.GetAbstractValues()[index]
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

