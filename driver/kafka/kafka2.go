/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package kafka

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"

	"github.com/actiontech/dtle/driver/common"
	"github.com/actiontech/dtle/g"

	"strconv"

	"time"

	"github.com/Shopify/sarama"
)

type SchemaType string

const (
	CONVERTER_JSON = "json"
	CONVERTER_AVRO = "avro"

	SCHEMA_TYPE_STRUCT    = "struct"
	SCHEMA_TYPE_STRING    = "string"
	SCHEMA_TYPE_INT64     = "int64"
	SCHEMA_TYPE_INT32     = "int32"
	SCHEMA_TYPE_INT16     = "int16"
	SCHEMA_TYPE_INT8      = "int8"
	SCHEMA_TYPE_BYTES     = "bytes"
	SCHEMA_TYPE_FLOAT64   = "float64"
	SCHEMA_TYPE_TIMESTAMP = "timestamp"
	SCHEMA_TYPE_DOUBLE    = "float64"
	SCHEMA_TYPE_FLOAT32   = "float32"
	SCHEMA_TYPE_BOOLEAN   = "boolean"

	RECORD_OP_INSERT = "c"
	RECORD_OP_UPDATE = "u"
	RECORD_OP_DELETE = "d"
	RECORD_OP_READ   = "r"
)

const (
	MySQLDateTimeFormat = "2006-01-02 15:04:05"
	MySQLDateFormat     = "2006-01-02"
	dbzTimestampFormat  = "2006-01-02T15:04:05Z"
)

var (
	jsonNullBs []byte
)

type ColDefs []*Schema

type KafkaManager struct {
	Cfg      *common.KafkaConfig
	producer sarama.SyncProducer
}

func NewKafkaManager(kcfg *common.KafkaConfig) (*KafkaManager, error) {
	var err error
	k := &KafkaManager{
		Cfg: kcfg,
	}
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Net.SASL.User = kcfg.User
	config.Net.SASL.Password = kcfg.Password

	k.producer, err = sarama.NewSyncProducer(kcfg.Brokers, config)
	if err != nil {
		return nil, err
	}
	return k, nil
}

func (k *KafkaManager) SendMessages(logger g.LoggerType, topics []string, keys [][]byte, values [][]byte) error {
	if !(len(topics) == len(keys) && len(values) == len(keys)) {
		return fmt.Errorf("length of topics, keys and values must be equal")
	}
	logger.Debug("SendMessages", "n", len(keys))
	msgs := make([]*sarama.ProducerMessage, len(topics))
	for i, topic := range topics {
		msg := &sarama.ProducerMessage{
			Topic:     topic,
			Partition: int32(-1),
			Key:       sarama.ByteEncoder(keys[i]),
			Value:     sarama.ByteEncoder(values[i]),
		}
		msgs[i] = msg
	}
	if errs := k.producer.SendMessages(msgs); errs != nil {
		for _, err := range errs.(sarama.ProducerErrors) {
			logger.Error("SendMessages failed", "err", err)
		}
		return errs
	}

	return nil
}

var (
	SourceSchema = &Schema{
		Fields: []*Schema{
			NewSimpleSchemaField(SCHEMA_TYPE_STRING, true, "version"),
			NewSimpleSchemaField(SCHEMA_TYPE_STRING, false, "name"),
			NewSimpleSchemaField(SCHEMA_TYPE_INT64, false, "server_id"),
			NewSimpleSchemaField(SCHEMA_TYPE_INT64, false, "ts_sec"),
			NewSimpleSchemaField(SCHEMA_TYPE_STRING, true, "gtid"),
			NewSimpleSchemaField(SCHEMA_TYPE_STRING, false, "file"),
			NewSimpleSchemaField(SCHEMA_TYPE_INT64, false, "pos"),
			NewSimpleSchemaField(SCHEMA_TYPE_INT32, false, "row"),
			NewSimpleSchemaWithDefaultField(SCHEMA_TYPE_BOOLEAN, true, "snapshot", false),
			NewSimpleSchemaField(SCHEMA_TYPE_INT64, true, "thread"),
			NewSimpleSchemaField(SCHEMA_TYPE_STRING, true, "db"),
			NewSimpleSchemaField(SCHEMA_TYPE_STRING, true, "table"),
			NewSimpleSchemaField(SCHEMA_TYPE_STRING, true, "query"),
		},
		Optional: false,
		Name:     "io.debezium.connector.mysql.Source",
		Field:    "source",
		Type:     SCHEMA_TYPE_STRUCT,
	}
)

func NewKeySchema(tableIdent string, fields ColDefs) *Schema {
	return &Schema{
		Type:     SCHEMA_TYPE_STRUCT,
		Name:     fmt.Sprintf("%v.Key", tableIdent),
		Optional: false,
		Fields:   fields,
	}
}

func NewColDefSchema(tableIdent string, field string) *Schema {
	return &Schema{
		Type:     SCHEMA_TYPE_STRUCT,
		Fields:   nil, // TODO
		Optional: true,
		Name:     fmt.Sprintf("%v.Value", tableIdent),
		Field:    field,
	}
}
func NewBeforeAfter(tableIdent string, fields []*Schema) (*Schema, *Schema) {
	before := NewColDefSchema(tableIdent, "before")
	after := NewColDefSchema(tableIdent, "after")
	before.Fields = fields
	after.Fields = fields
	return before, after
}
func NewEnvelopeSchema(tableIdent string, colDefs ColDefs) *Schema {
	before, after := NewBeforeAfter(tableIdent, colDefs)
	return &Schema{
		Type: SCHEMA_TYPE_STRUCT,
		Fields: []*Schema{
			before,
			after,
			SourceSchema,
			NewSimpleSchemaField(SCHEMA_TYPE_STRING, false, "op"),
			NewSimpleSchemaField(SCHEMA_TYPE_INT64, true, "ts_ms"),
		},
		Optional: false,
		Name:     fmt.Sprintf("%v.Envelope", tableIdent),
		Version:  1,
	}
}

type DbzOutput struct {
	Schema *SchemaJson `json:"schema"`
	// ValuePayload or Row
	Payload interface{} `json:"payload"`
}

type ValuePayload struct {
	Before *Row           `json:"before"`
	After  *Row           `json:"after"`
	Source *SourcePayload `json:"source"`
	Op     string         `json:"op"`
	TsMs   int64          `json:"ts_ms"`
}

func NewValuePayload() *ValuePayload { // TODO source
	return &ValuePayload{
		Source: &SourcePayload{},
	}
}

type SourcePayload struct {
	// we use 'interface{}' to represent an optional field
	Version  string      `json:"version"`
	Name     string      `json:"name"`
	ServerID int         `json:"server_id"`
	TsSec    int64       `json:"ts_sec"`
	Gtid     interface{} `json:"gtid"` // real type: optional<string>
	File     string      `json:"file"`
	Pos      int64       `json:"pos"`
	Query    interface{} `json:"query"`
	Row      int         `json:"row"`
	Snapshot bool        `json:"snapshot"`
	Thread   interface{} `json:"thread"` // real type: optional<int64>
	Db       string      `json:"db"`
	Table    string      `json:"table"`
}

type Schema struct {
	Type       SchemaType             `json:"type"`
	Optional   bool                   `json:"optional"`
	Default    interface{}            `json:"default,omitempty"`
	Field      string                 `json:"field,omitempty"` // field name in outer struct
	Fields     []*Schema              `json:"fields,omitempty"`
	Name       string                 `json:"name,omitempty"`
	Version    int                    `json:"version,omitempty"`
	Parameters map[string]interface{} `json:"parameters,omitempty"`
}

type SchemaJson struct {
	schema *Schema
	cache  []byte
}

func (sj *SchemaJson) MarshalJSON() ([]byte, error) {
	if sj.cache != nil {
		return sj.cache, nil
	} else {
		return json.Marshal(sj.schema)
	}
}

type Row struct {
	ColNames []string
	Values   []interface{}
}

func NewRow() *Row {
	return &Row{}
}
func (r *Row) AddField(key string, value interface{}) {
	r.ColNames = append(r.ColNames, key)
	r.Values = append(r.Values, value)
}
func (r *Row) MarshalJSON() ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	buf.WriteString("{")
	first := true
	for i, _ := range r.ColNames {
		if first {
			first = false
		} else {
			buf.WriteString(",")
		}
		bsKey, err := json.Marshal(r.ColNames[i])
		if err != nil {
			return nil, err
		}
		buf.Write(bsKey)
		buf.WriteByte(byte(':'))
		bsValue, err := json.Marshal(r.Values[i])
		if err != nil {
			return nil, err
		}
		buf.Write(bsValue)
	}
	buf.WriteString("}")
	return buf.Bytes(), nil
}

func NewSimpleSchemaField(theType SchemaType, optional bool, field string) *Schema {
	return &Schema{
		Type:     theType,
		Optional: optional,
		Field:    field,
	}
}
func NewSimpleSchemaWithDefaultField(theType SchemaType, optional bool, field string, defaultValue interface{}) *Schema {
	if defaultValue != nil {
		switch theType {
		case SCHEMA_TYPE_BYTES:
			defaultValue = base64.StdEncoding.EncodeToString([]byte(defaultValue.(string)))
		case SCHEMA_TYPE_INT16:
			val16, _ := strconv.ParseInt(defaultValue.(string), 10, 16)
			defaultValue = val16
		case SCHEMA_TYPE_INT32:
			val32, _ := strconv.ParseInt(defaultValue.(string), 10, 32)
			defaultValue = val32
		case SCHEMA_TYPE_INT64:
			val64, _ := strconv.ParseInt(defaultValue.(string), 10, 64)
			defaultValue = val64
		case SCHEMA_TYPE_FLOAT64:
			floatValue, _ := strconv.ParseFloat(defaultValue.(string), 64)
			defaultValue = floatValue
		}
	}
	return &Schema{
		Default:  defaultValue,
		Type:     theType,
		Optional: optional,
		Field:    field,
	}
}
func NewDecimalField(precision int, scale int, optional bool, field string, defaultValue interface{}) *Schema {
	if defaultValue != nil {
		defaultValue = DecimalValueFromStringMysql(defaultValue.(string))
	}
	return &Schema{
		Field:    field,
		Default:  defaultValue,
		Optional: optional,
		Name:     "org.apache.kafka.connect.data.Decimal",
		Parameters: map[string]interface{}{
			"connect.decimal.precision": strconv.Itoa(precision),
			"scale":                     strconv.Itoa(scale),
		},
		Type:    SCHEMA_TYPE_BYTES,
		Version: 1,
	}
}

var (
	decimalNums [11]*big.Int
)

func init() {
	for i := 0; i <= 10; i++ {
		decimalNums[i] = big.NewInt(int64(i))
	}
	jsonNullBs, _ = json.Marshal(nil)
}

// value: e.g. decimal(11,5), 123.45 will be 123.45000
func DecimalValueFromStringMysql(value string) string {
	sum := big.NewInt(0)

	isNeg := false
	if value[0] == '-' {
		value = value[1:]
		isNeg = true
	}

	for i := range value {
		if '0' <= value[i] && value[i] <= '9' {
			sum.Mul(sum, decimalNums[10])
			offset := value[i] - '0'
			if offset != 0 { // add 0 = do nothing
				sum.Add(sum, decimalNums[offset])
			}
		}
	}

	bs := sum.Bytes()
	if len(bs) == 0 {
		bs = []byte{0}
	}

	if isNeg {
		for i := len(bs) - 1; i >= 0; i-- {
			bs[i] = ^bs[i]
		}
		for i := len(bs) - 1; i >= 0; i-- {
			bs[i] += 1
			if bs[i] != 0x00 {
				break
			}
		}
	} else if bs[0] > 0x7f {
		bs2 := make([]byte, len(bs)+1)
		bs2[0] = 0x00
		copy(bs2[1:], bs)
		bs = bs2
	}

	return base64.StdEncoding.EncodeToString(bs)
}

func NewTimeField(optional bool, field string, defaultValue interface{}) *Schema {
	if defaultValue != nil {
		defaultValue = TimeValue(defaultValue.(string))
	}
	return &Schema{
		Default:  defaultValue,
		Field:    field,
		Optional: optional,
		Type:     SCHEMA_TYPE_INT64,
		Name:     "io.debezium.time.MicroTime",
		Version:  1,
	}
}

func timeValueHelper(h, m, s, microsec int64, isNeg bool) int64 {
	r := (h*3600+m*60+s)*1000000 + microsec
	if isNeg {
		return -r
	} else {
		return r
	}
}

// precision make no difference
func TimeValue(value string) int64 {
	var err error

	if len(value) == 0 {
		return 0
	}
	isNeg := false
	if value[0] == '-' {
		isNeg = true
		value = value[1:]
	}

	ss := strings.Split(value, ":")
	if len(ss) != 3 {
		// TODO report err, as well the followings.
		return 0
	}
	var h, m, s, microsec int64
	h, err = strconv.ParseInt(ss[0], 10, 64)
	if err != nil {
		return 0
	}
	m, err = strconv.ParseInt(ss[1], 10, 64)
	if err != nil {
		return 0
	}
	ssms := strings.Split(ss[2], ".")
	switch len(ssms) {
	case 1:
		s, err = strconv.ParseInt(ss[2], 10, 64)
		if err != nil {
			return 0
		}
		microsec = 0
	case 2:
		s, err = strconv.ParseInt(ssms[0], 10, 64)
		if err != nil {
			return 0
		}
		microsec, err = strconv.ParseInt(ssms[1], 10, 64)
		if err != nil {
			return 0
		}
	default:
		return 0
	}

	return timeValueHelper(h, m, s, microsec, isNeg)
}
func NewDateTimeField(optional bool, field string, defaultValue interface{}, loc *time.Location) *Schema {
	if defaultValue != nil {
		defaultValue = DateTimeValue(defaultValue.(string), loc)
	}
	return &Schema{
		Default:  defaultValue,
		Field:    field,
		Optional: optional,
		Type:     SCHEMA_TYPE_INT64,
		Name:     "io.debezium.time.Timestamp",
		Version:  1,
	}
}
func DateTimeValue(dateTime string, loc *time.Location) int64 {
	tm2, err := time.ParseInLocation(MySQLDateTimeFormat, dateTime, loc)
	if err != nil {
		return 0
	}
	timeSlice := strings.Split(tm2.Format("15:04:05.000"), ".")
	if len(timeSlice) != 2 {
		return 0
	}
	ms, err := strconv.ParseInt(timeSlice[1], 10, 64)
	return tm2.Unix()*1000 + ms
}
func DateValue(date string) int64 {
	tm2, err := time.Parse(MySQLDateFormat, date)
	if err != nil {
		return 0
	}
	return tm2.Unix() / 60 / 60 / 24
}
func TimeStamp(timestamp string) string {
	// dtle always read timestamp in UTC
	tm2, _ := time.ParseInLocation(MySQLDateTimeFormat, timestamp, time.UTC)
	return tm2.In(time.UTC).Format(dbzTimestampFormat)
}

func NewJsonField(optional bool, field string) *Schema {
	return &Schema{
		Field:    field,
		Optional: optional,
		Type:     SCHEMA_TYPE_STRING,
		Name:     "io.debezium.data.Json",
	}
}

func NewBitsField(optional bool, field string, length string, defaultValue interface{}) *Schema {
	if defaultValue != nil {
		defaultValue = base64.StdEncoding.EncodeToString(defaultValue.([]byte))
	}
	return &Schema{
		Field:    field,
		Optional: optional,
		Default:  defaultValue,
		Parameters: map[string]interface{}{
			"length": length,
		},
		Type:    SCHEMA_TYPE_BYTES,
		Name:    "io.debezium.data.Bits",
		Version: 1,
	}
}
func NewDateField(theType SchemaType, optional bool, field string, defaultValue interface{}) *Schema {
	if defaultValue != nil {
		defaultValue = DateValue(defaultValue.(string))
	}
	return &Schema{
		Field:    field,
		Default:  defaultValue,
		Optional: optional,
		Type:     theType,
		Name:     "io.debezium.time.Date",
		Version:  1,
	}
}
func NewEnumField(theType SchemaType, optional bool, field string, allowed string, defaultValue interface{}) *Schema {
	allowed = strings.Replace(allowed[5:len(allowed)-1], "'", "", -1)
	return &Schema{
		Default:  defaultValue,
		Field:    field,
		Optional: optional,
		Parameters: map[string]interface{}{
			"allowed": allowed,
		},
		Type:    theType,
		Name:    "io.debezium.data.Enum",
		Version: 1,
	}
}
func NewSetField(theType SchemaType, optional bool, field string, allowed string, defaultValue interface{}) *Schema {
	allowed = strings.Replace(allowed[4:len(allowed)-1], "'", "", -1)
	return &Schema{
		Field:    field,
		Optional: optional,
		Default:  defaultValue,
		Parameters: map[string]interface{}{
			"allowed": allowed,
		},
		Type:    theType,
		Name:    "io.debezium.data.EnumSet",
		Version: 1,
	}
}
func NewTimeStampField(optional bool, field string, defaultValue interface{}) *Schema {
	if defaultValue == "CURRENT_TIMESTAMP" {
		defaultValue = "1970-01-01T00:00:00Z"
	} else if defaultValue != nil {
		tm2, _ := time.ParseInLocation(MySQLDateTimeFormat, defaultValue.(string), time.UTC)
		defaultValue = tm2.In(time.UTC).Format(dbzTimestampFormat)
	}
	return &Schema{
		Field:    field,
		Optional: optional,
		Default:  defaultValue,
		Type:     SCHEMA_TYPE_STRING,
		Name:     "io.debezium.time.ZonedTimestamp",
		Version:  1,
	}
}

func NewYearField(theType SchemaType, optional bool, field string, defaultValue interface{}) *Schema {
	return &Schema{
		Field:    field,
		Default:  defaultValue,
		Optional: optional,
		Type:     theType,
		Name:     "io.debezium.time.Year",
		Version:  1,
	}
}

func YearValue(year string) int {
	int, err := strconv.Atoi(year)
	if err != nil {
		return 0
	}
	yearValue := int - 1900
	if 0 < yearValue && yearValue <= 69 {
		yearValue += 2000
	} else if 70 <= yearValue && yearValue <= 99 {
		yearValue += 1900
	}
	return yearValue

}

func BinaryStringToBytes(s string) (bs []byte) {
	uint8arr := [8]uint8{128, 64, 32, 16, 8, 4, 2, 1}
	l := len(s)
	mo := l % 8
	l /= 8
	if mo != 0 {
		l++
	}
	bs = make([]byte, 0, l)
	mo = 8 - mo
	var n uint8
	for i, b := range []byte(s) {
		m := (i + mo) % 8
		switch b {
		case byte('1'):
			n += uint8arr[m]
		}
		if m == 7 {
			bs = append(bs, n)
			n = 0
		}
	}
	return bs
}
