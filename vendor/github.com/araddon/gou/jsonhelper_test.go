package gou

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

//  go test -bench=".*"
//  go test -run="(Util)"

var (
	jh JsonHelper
)

func init() {
	SetupLogging("debug")
	SetColorOutput()
	//SetLogger(log.New(os.Stderr, "", log.Ltime|log.Lshortfile), "debug")
	// create test data
	json.Unmarshal([]byte(`{
		"name":"aaron",
		"nullstring":null,
		"ints":[1,2,3,4],
		"int":1,
		"intstr":"1",
		"int64":1234567890,
		"float64":123.456,
		"float64str":"123.456",
		"float64null": null,
		"MaxSize" : 1048576,
		"strings":["string1"],
		"stringscsv":"string1,string2",
		"nested":{
			"nest":"string2",
			"strings":["string1"],
			"int":2,
			"list":["value"],
			"nest2":{
				"test":"good"
			}
		},
		"nested2":[
			{"sub":2}
		],
		"period.name":"value"
	}`), &jh)
}

func TestJsonRawWriter(t *testing.T) {
	var buf bytes.Buffer
	buf.WriteString(`"hello"`)
	raw := json.RawMessage(buf.Bytes())
	bya, _ := json.Marshal(&buf)
	Debug(string(bya))
	bya, _ = json.Marshal(&raw)
	Debug(string(bya))

	/*
		bya, err := json.Marshal(buf)
		assert.True(t,string(bya) == `"hello"`, t, "Should be hello but was %s", string(bya))
		Debug(string(buf.Bytes()), err)
		var jrw JsonRawWriter
		jrw.WriteString(`"hello"`)
		Debug(jrw.Raw())
		bya, err = json.Marshal(jrw.Raw())
		assert.True(t,string(bya) == `"hello"`, t, "Should be hello but was %s", string(bya))
		Debug(string(jrw.Bytes()), err)
	*/
}

func TestJsonHelper(t *testing.T) {

	assert.True(t, jh.String("name") == "aaron", "should get 'aaron' %s", jh.String("name"))
	assert.True(t, jh.String("nullstring") == "", "should get '' %s", jh.String("nullstring"))

	assert.True(t, jh.Int("int") == 1, "get int ")
	assert.True(t, jh.Int("ints[0]") == 1, "get int from array %d", jh.Int("ints[0]"))
	assert.True(t, jh.Int("ints[2]") == 3, "get int from array %d", jh.Int("ints[0]"))
	assert.True(t, len(jh.Ints("ints")) == 4, "get int array %v", jh.Ints("ints"))
	assert.True(t, jh.Int64("int64") == 1234567890, "get int")
	assert.True(t, jh.Int("nested.int") == 2, "get int")
	assert.True(t, jh.String("nested.nest") == "string2", "should get string %s", jh.String("nested.nest"))
	assert.True(t, jh.String("nested.nest2.test") == "good", "should get string %s", jh.String("nested.nest2.test"))
	assert.True(t, jh.String("nested.list[0]") == "value", "get string from array")
	assert.True(t, jh.Int("nested2[0].sub") == 2, "get int from obj in array %d", jh.Int("nested2[0].sub"))

	assert.True(t, jh.Int("MaxSize") == 1048576, "get int, test capitalization? ")
	sl := jh.Strings("strings")
	assert.True(t, len(sl) == 1 && sl[0] == "string1", "get strings ")
	sl = jh.Strings("stringscsv")
	assert.True(t, len(sl) == 2 && sl[0] == "string1", "get strings ")

	i64, ok := jh.Int64Safe("int64")
	assert.True(t, ok, t, "int64safe ok")
	assert.True(t, i64 == 1234567890, "int64safe value")

	u64, ok := jh.Uint64Safe("int64")
	assert.True(t, ok, "uint64safe ok")
	assert.True(t, u64 == 1234567890, "int64safe value")
	_, ok = jh.Uint64Safe("notexistent")
	assert.True(t, !ok, "should not be ok")
	_, ok = jh.Uint64Safe("name")
	assert.True(t, !ok, "should not be ok")

	i, ok := jh.IntSafe("int")
	assert.True(t, ok, "intsafe ok")
	assert.True(t, i == 1, "intsafe value")

	l := jh.List("nested2")
	assert.True(t, len(l) == 1, "get list")

	fv, ok := jh.Float64Safe("name")
	assert.True(t, !ok, "floatsafe not ok")
	fv, ok = jh.Float64Safe("float64")
	assert.True(t, ok, "floatsafe ok")
	assert.True(t, CloseEnuf(fv, 123.456), "floatsafe value %v", fv)
	fv = jh.Float64("float64")
	assert.True(t, CloseEnuf(fv, 123.456), "floatsafe value %v", fv)
	fv, ok = jh.Float64Safe("float64str")
	assert.True(t, ok, "floatsafe ok")
	assert.True(t, CloseEnuf(fv, 123.456), "floatsafe value %v", fv)
	fv = jh.Float64("float64str")
	assert.True(t, CloseEnuf(fv, 123.456), "floatsafe value %v", fv)
	fv, ok = jh.Float64Safe("float64null")
	assert.True(t, ok, "float64null ok")
	assert.True(t, math.IsNaN(fv), "float64null expected Nan but got %v", fv)
	fv = jh.Float64("float64null")
	assert.True(t, math.IsNaN(fv), "float64null expected Nan but got %v", fv)

	jhm := jh.Helpers("nested2")
	assert.True(t, len(jhm) == 1, "get list of helpers")
	assert.True(t, jhm[0].Int("sub") == 2, "Should get list of helpers")
}

func TestJsonInterface(t *testing.T) {

	var jim map[string]JsonInterface
	err := json.Unmarshal([]byte(`{
		"nullstring":null,
		"string":"string",
		"int":22,
		"float":22.2,
		"floatstr":"22.2",
		"intstr":"22"
	}`), &jim)
	assert.True(t, err == nil, t, "no error:%v ", err)
	assert.True(t, jim["nullstring"].StringSh() == "", "nullstring: %v", jim["nullstring"])
	assert.True(t, jim["string"].StringSh() == "string", "nullstring: %v", jim["string"])
	assert.True(t, jim["int"].IntSh() == 22, "int: %v", jim["int"])
	assert.True(t, jim["int"].StringSh() == "22", "int->string: %v", jim["int"])
	assert.True(t, jim["int"].FloatSh() == float32(22), "int->float: %v", jim["int"])
	assert.True(t, jim["float"].FloatSh() == 22.2, "float: %v", jim["float"])
	assert.True(t, jim["float"].StringSh() == "22.2", "float->string: %v", jim["float"])
	assert.True(t, jim["float"].IntSh() == 22, "float->int: %v", jim["float"])
	assert.True(t, jim["intstr"].IntSh() == 22, "intstr: %v", jim["intstr"])
	assert.True(t, jim["intstr"].FloatSh() == float32(22), "intstr->float: %v", jim["intstr"])
}

func TestJsonCoercion(t *testing.T) {
	assert.True(t, jh.Int("intstr") == 1, "get string as int %s", jh.String("intstr"))
	assert.True(t, jh.String("int") == "1", "get int as string %s", jh.String("int"))
	assert.True(t, jh.Int("notint") == -1, "get non existent int = 0??? ")
}

func TestJsonPathNotation(t *testing.T) {
	// Now lets test xpath type syntax
	assert.True(t, jh.Int("/MaxSize") == 1048576, "get int, test capitalization? ")
	assert.True(t, jh.String("/nested/nest") == "string2", "should get string %s", jh.String("/nested/nest"))
	assert.True(t, jh.String("/nested/list[0]") == "value", "get string from array")
	// note this one has period in name
	assert.True(t, jh.String("/period.name") == "value", "test period in name ")
}

func TestFromReader(t *testing.T) {
	raw := `{"testing": 123}`
	reader := strings.NewReader(raw)
	jh, err := NewJsonHelperReader(reader)
	assert.True(t, err == nil, "Unexpected error decoding json: %s", err)
	assert.True(t, jh.Int("testing") == 123, "Unexpected value in json: %d", jh.Int("testing"))
}

func TestJsonHelperGobEncoding(t *testing.T) {
	raw := `{"testing": 123,"name":"bob & more"}`
	reader := strings.NewReader(raw)
	jh, err := NewJsonHelperReader(reader)
	assert.True(t, err == nil, "Unexpected error decoding gob: %s", err)
	assert.True(t, jh.Int("testing") == 123, "Unexpected value in gob: %d", jh.Int("testing"))
	var buf bytes.Buffer
	err = gob.NewEncoder(&buf).Encode(&jh)
	assert.True(t, err == nil, err)

	var jhNew JsonHelper
	err = gob.NewDecoder(&buf).Decode(&jhNew)
	assert.True(t, err == nil, err)
	assert.True(t, jhNew.Int("testing") == 123, "Unexpected value in gob: %d", jhNew.Int("testing"))
	assert.True(t, jhNew.String("name") == "bob & more", "Unexpected value in gob: %d", jhNew.String("name"))

	buf2 := bytes.Buffer{}
	gt := GobTest{"Hello", jh}
	err = gob.NewEncoder(&buf2).Encode(&gt)
	assert.True(t, err == nil, err)

	var gt2 GobTest
	err = gob.NewDecoder(&buf2).Decode(&gt2)
	assert.True(t, err == nil, err)
	assert.True(t, gt2.Name == "Hello", "Unexpected value in gob: %d", gt2.Name)
	assert.True(t, gt2.Data.Int("testing") == 123, "Unexpected value in gob: %d", gt2.Data.Int("testing"))
	assert.True(t, gt2.Data.String("name") == "bob & more", "Unexpected value in gob: %d", gt2.Data.String("name"))
}

type GobTest struct {
	Name string
	Data JsonHelper
}
