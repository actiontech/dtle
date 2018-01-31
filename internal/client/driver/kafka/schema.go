package kafka

import (
	"fmt"
)

// internal error, it's a bug if you see one
var (
	SCHEMA_UNKNOWN_FIELD_ERROR = "schema unknown field"
)

// TODO: concurrency safe
type Schema struct {
	Name           string
	Field          string // need to be unique across Schema
	FieldType      SchemaType
	Optional       bool
	Fields         []*Schema
	DefaultPayload interface{}
	Payload        interface{}
}

// TODO: unit test
// field need to be unique across Schema
func (s *Schema) put(field string, value interface{}) error {
	for _, f := range s.Fields {
		if field == f.Field {
			f.Payload = value
			return nil
		}
		if nil != f.Fields {
			return f.put(field, value)
		}
	}
	return fmt.Errorf("schema.put error: field(%s) %s", field, SCHEMA_UNKNOWN_FIELD_ERROR)
}

func (s *Schema) printFields() []string {
	ret := make([]string, len(s.Fields))
	for i, f := range s.Fields {
		ret[i] = f.Field
	}
	return ret
}

type SchemaBuilder struct {
	schemaType SchemaType
	name       string
	field      string
	optional   bool
	fields     []*Schema
}

func newSchemaBuilder() *SchemaBuilder {
	return &SchemaBuilder{}
}

func (s *SchemaBuilder) setType(t SchemaType) *SchemaBuilder {
	s.schemaType = t
	return s
}

func (s *SchemaBuilder) setName(name string) *SchemaBuilder {
	s.name = name
	return s
}

func (s *SchemaBuilder) setOptional(optional bool) *SchemaBuilder {
	s.optional = optional
	return s
}

func (s *SchemaBuilder) setFieldName(field string) *SchemaBuilder {
	s.field = field
	return s
}

func (s *SchemaBuilder) setField(schema *Schema) *SchemaBuilder {
	s.fields = append(s.fields, schema)
	return s
}

func (s *SchemaBuilder) build() *Schema {
	return &Schema{
		Field:     s.field,
		Name:      s.name,
		Optional:  s.optional,
		FieldType: s.schemaType,
		Fields:    s.fields,
	}
}

type SchemaType string

const (
	UNKNOWN SchemaType = "unknown"     // TODO: type should not exist in kafka message
	STRUCT             = "innerStruct" // TODO: type should not exist in kafka message

	SCHEMA_STRING           = "string"
	OPTIONAL_SCHEMA_STRING  = "optionalString"
	SCHEMA_INT64            = "int64"
	OPTIONAL_SCHEMA_INT64   = "optionalInt64"
	SCHEMA_INT32            = "int32"
	OPTIONAL_SCHEMA_BOOLEAN = "optionalBoolean"
	SCHEMA_STRUCT           = "struct"
	SCHEMA_BYTES            = "bytes"
	SCHEMA_INT16            = "int16"
)
