package gou

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCoerce(t *testing.T) {

	data := map[string]interface{}{
		"int":     4,
		"float":   45.3,
		"string":  "22",
		"stringf": "22.2",
	}
	assert.True(t, CoerceStringShort(data["int"]) == "4", "get int as string")
	assert.True(t, CoerceStringShort(data["float"]) == "45.3", "get float as string: %v", data["float"])
	assert.True(t, CoerceStringShort(data["string"]) == "22", "get string as string: %v", data["string"])
	assert.True(t, CoerceStringShort(data["stringf"]) == "22.2", "get stringf as string: %v", data["stringf"])

	assert.True(t, CoerceIntShort(data["int"]) == 4, "get int as int: %v", data["int"])
	assert.True(t, CoerceIntShort(data["float"]) == 45, "get float as int: %v", data["float"])
	assert.True(t, CoerceIntShort(data["string"]) == 22, "get string as int: %v", data["string"])
	assert.True(t, CoerceIntShort(data["stringf"]) == 22, "get stringf as int: %v", data["stringf"])
}
