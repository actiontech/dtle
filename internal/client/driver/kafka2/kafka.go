package kafka2

import (
	"github.com/Shopify/sarama"
	"bytes"
	"encoding/json"
	"fmt"
)

const (
	CONVERTER_JSON = "json"
	CONVERTER_AVRO = "avro"

	SCHEMA_TYPE_STRUCT = "struct"
	SCHEMA_TYPE_STRING = "string"
	SCHEMA_TYPE_INT64 = "int64"

	RECORD_OP_INSERT = "c"
	RECORD_OP_UPDATE = "u"
	RECORD_OP_DELETE = "d"
	RECORD_OP_READ   = "r"
)

type ColDefs []*Schema

type KafkaConfig struct {
	Broker string
	Topic  string
	Converter string
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

	k.producer, err = sarama.NewSyncProducer([]string{kcfg.Broker}, config)
	if err != nil {
		return nil, err
	}
	return k, nil
}

func (k *KafkaManager) Send(key []byte, value []byte) error {
	msg := &sarama.ProducerMessage{
		Topic:     k.Cfg.Topic,
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

type SchemaType string

var (
	SourceSchema = &Schema{
		Fields: []*Schema{
			NewSimpleSchemaField(SCHEMA_TYPE_STRING, true, "version"),
			NewSimpleSchemaField(SCHEMA_TYPE_STRING, false, "name"),
			NewSimpleSchemaField(SCHEMA_TYPE_INT64, false, "server_id"),
			NewSimpleSchemaField(SCHEMA_TYPE_INT64, false, "ts_sec"),
			// TODO
		},
		Optional: false,
		Name: "io.debezium.connector.mysql.Source",
		Field: "source",
	}
)

func NewKeySchema(tableIdent string, ) *Schema {
	return &Schema{
		Type:SCHEMA_TYPE_STRUCT,
		Name: fmt.Sprintf("%v.Key", tableIdent),
		Optional:false,
		Fields:nil,
	}
}

func NewColDefSchema(tableIdent string, field string) *Schema {
	return &Schema{
		Type:SCHEMA_TYPE_STRUCT,
		Fields: nil, // TODO
		Optional: true,
		Name: fmt.Sprintf("%v.Value", tableIdent),
		Field: field,
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
			NewSimpleSchemaField(SCHEMA_TYPE_INT64, false, "ts_ms"),
		},
		Optional: false,
		Name: fmt.Sprintf("%v.Envelope", tableIdent),
		Version: 1,
	}
}

type DbzOutput struct {
	Schema  *Schema       `json:"schema"`
	// ValuePayload or Row
	Payload interface{} `json:"payload"`
}

type ValuePayload struct {
	Before *Row           `json:"before"`
	After *Row            `json:"after"`
	Source *SourcePayload `json:"source"`
	Op string             `json:"op"`
	TsMs int64            `json:"ts_ms"`
}
func NewValuePayload(op string) *ValuePayload {
	return &ValuePayload{
		Op: op,
	}
}
type SourcePayload struct {
	// we use 'interface{}' to represent an optional field
	Version string `json:"version"`
	Name string `json:"name"`
	ServerID int `json:"server_id"`
	TsSec int64 `json:"ts_sec"`
	Gtid interface{} `json:"gtid"` // real type: optional<string>
	File string `json:"file"`
	Pos int64 `json:"pos"`
	Row int `json:"row"`
	Snapshot bool `json:"snapshot"`
	Thread interface{} `json:"thread"` // real type: optional<int64>
	Db string `json:"db"`
	Table string `json:"table"`
}

type Schema struct {
	Type SchemaType `json:"type"`
	Optional bool `json:"optional"`
	Field string `json:"field,omitempty"` // field name in outer struct
	Fields []*Schema `json:"fields,omitempty"`
	Name string `json:"name,omitempty"`
	Version int `json:"version,omitempty"`
}
type Row struct {
	ColNames []string
	Values []interface{}
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
		Type: theType,
		Optional: optional,
		Field: field,
	}
}
