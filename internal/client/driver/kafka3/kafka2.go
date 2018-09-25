/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package kafka3

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"

	"strconv"

	"github.com/Shopify/sarama"
)

type SchemaType string

const (
	CONVERTER_JSON = "json"
	CONVERTER_AVRO = "avro"

	SCHEMA_TYPE_STRUCT  = "struct"
	SCHEMA_TYPE_STRING  = "string"
	SCHEMA_TYPE_INT64   = "int64"
	SCHEMA_TYPE_INT32   = "int32"
	SCHEMA_TYPE_INT16   = "int16"
	SCHEMA_TYPE_INT8    = "int8"
	SCHEMA_TYPE_BYTES   = "bytes"
	SCHEMA_TYPE_FLOAT64 = "float64"
	SCHEMA_TYPE_FLOAT32 = "float32"
	SCHEMA_TYPE_BOOLEAN = "boolean"

	RECORD_OP_INSERT = "c"
	RECORD_OP_UPDATE = "u"
	RECORD_OP_DELETE = "d"
	RECORD_OP_READ   = "r"
)

type ColDefs []*Schema

type KafkaConfig struct {
	Brokers   []string
	Topic     string
	Converter string
	NatsAddr  string
	Gtid      string // TODO remove?
}

type KafkaManager struct {
	Cfg      *KafkaConfig
	producer sarama.SyncProducer
}

func NewKafkaManager(kcfg *KafkaConfig) (*KafkaManager, error) {
	var err error
	k := &KafkaManager{
		Cfg: kcfg,
	}
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true

	k.producer, err = sarama.NewSyncProducer(kcfg.Brokers, config)
	if err != nil {
		return nil, err
	}
	return k, nil
}

func (k *KafkaManager) Send(topic string, key []byte, value []byte) error {
	msg := &sarama.ProducerMessage{
		Topic:     topic,
		Partition: int32(-1),
		Key:       sarama.ByteEncoder(key),
		Value:     sarama.ByteEncoder(value),
	}

	_, _, err := k.producer.SendMessage(msg)
	if err != nil {
		return err
	}

	// TODO partition? offset?
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
			NewSimpleSchemaField(SCHEMA_TYPE_BOOLEAN, true, "snapshot"),
			NewSimpleSchemaField(SCHEMA_TYPE_INT64, true, "thread"),
			NewSimpleSchemaField(SCHEMA_TYPE_STRING, true, "db"),
			NewSimpleSchemaField(SCHEMA_TYPE_STRING, true, "table"),
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
	Schema *Schema `json:"schema"`
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
	Row      int         `json:"row"`
	Snapshot bool        `json:"snapshot"`
	Thread   interface{} `json:"thread"` // real type: optional<int64>
	Db       string      `json:"db"`
	Table    string      `json:"table"`
}

type Schema struct {
	Type       SchemaType             `json:"type"`
	Optional   bool                   `json:"optional"`
	Field      string                 `json:"field,omitempty"` // field name in outer struct
	Fields     []*Schema              `json:"fields,omitempty"`
	Name       string                 `json:"name,omitempty"`
	Version    int                    `json:"version,omitempty"`
	Parameters map[string]interface{} `json:"parameters,omitempty"`
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
func NewDecimalField(precision int, scale int, optional bool, field string) *Schema {
	return &Schema{
		Field:    field,
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

func NewTimeField(optional bool, field string) *Schema {
	return &Schema{
		Field:    field,
		Optional: optional,
		Type:     SCHEMA_TYPE_INT64,
		Name:     "io.debezium.time.MicroTime",
		Version:  1,
	}
}

// precision make no difference
func TimeValue(timestamp int64) int64 {
	// TODO
	return 0
}
func NewDateTimeField(optional bool, field string) *Schema {
	return &Schema{
		Field:    field,
		Optional: optional,
		Type:     SCHEMA_TYPE_INT64,
		Name:     "io.debezium.time.MicroTimestamp",
		Version:  1,
	}
}
func DateTimeValue(timestamp int64) int64 {
	// TODO
	return 0
	// precision <= 3: 1534932206000
	// precision >  3: 1534931868000000
}
func NewJsonField(optional bool, field string) *Schema {
	return &Schema{
		Field:    field,
		Optional: optional,
		Type:     SCHEMA_TYPE_STRING,
		Name:     "io.debezium.data.Json",
	}
}
