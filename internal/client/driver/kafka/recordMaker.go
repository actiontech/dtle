package kafka

import (
	"encoding/json"
	"udup/internal/config"
	"fmt"
	"udup/internal/config/mysql"
	"time"
)

const (
	RECORD_FIELD_DATABASE_NAME  = "databaseName"
	RECORD_FIELD_SOURCE         = "source"
	RECORD_FIELD_DDL_STATEMENTS = "ddl"
	RECORD_FIELD_BEFORE         = "before"
	RECORD_FIELD_AFTER          = "after"
	RECORD_FIELD_ID             = "id"
	RECORD_FIELD_OP             = "op"
	RECORD_FIELD_TS_MS          = "ts_ms"
)

type opValue string

const (
	RECORD_OP_INSERT opValue = "c"
	RECORD_OP_UPDATE opValue = "u"
	RECORD_OP_DELETE opValue = "d"
	RECORD_OP_READ   opValue = "r"
)

//record name
const (
	RECORD_NAME_SOURCE = "mysql.Source"
)

type recordMakers struct {
	Key   *Schema
	Value *Schema
}

func newRecordMakers(key, value *Schema) *recordMakers {
	return &recordMakers{
		Key:   key,
		Value: value,
	}
}

// TODO: avro. now use json for demo
func (r *recordMakers) serialization() (key, value []byte, err error) {
	key, err = json.Marshal(r.Key)
	if nil != err {
		return key, value, err
	}
	value, err = json.Marshal(r.Value)
	if nil != err {
		return key, value, err
	}
	return key, value, nil
}

type schemaChangeRecord struct {
	*recordMakers
}

func newSchemaChangeRecord() *schemaChangeRecord {
	record := &schemaChangeRecord{}
	record.recordMakers = newRecordMakers(record.keySchema(), record.valueSchema())
	return record
}

func (r *schemaChangeRecord) GenerateKey(databaseName string) {
	r.Key.put(RECORD_FIELD_DATABASE_NAME, databaseName)
}

func (r *schemaChangeRecord) GenerateValue(databaseName, ddl string, source sourceInfo) {
	r.Value.put(RECORD_FIELD_SOURCE, source)
	r.Value.put(RECORD_FIELD_DATABASE_NAME, databaseName)
	r.Value.put(RECORD_FIELD_DDL_STATEMENTS, ddl)
}

func (r *schemaChangeRecord) keySchema() *Schema {
	return newSchemaBuilder().setType(STRUCT).setName("udup.client.driver.kafka.schemaChangeKey").setOptional(false).
		setField(newSchemaBuilder().setType(SCHEMA_STRING).setFieldName("databaseName").setOptional(false).build()).
		build()
}

func (r *schemaChangeRecord) valueSchema() *Schema {
	return newSchemaBuilder().setType(STRUCT).setName("udup.client.driver.kafka.schemaChangeValue").setOptional(false).
		setField(newSchemaBuilder().setType(SCHEMA_STRUCT).setFieldName(RECORD_FIELD_SOURCE).setOptional(false).build()).
		setField(newSchemaBuilder().setType(SCHEMA_STRING).setFieldName(RECORD_FIELD_DATABASE_NAME).setOptional(false).build()).
		setField(newSchemaBuilder().setType(SCHEMA_STRING).setFieldName(RECORD_FIELD_DDL_STATEMENTS).setOptional(false).build()).
		build()
}

type sourceInfo *Schema

func sourceInfoSchemaBuilder() *SchemaBuilder {
	return newSchemaBuilder().setType(STRUCT).setName("udup.client.driver.kafka.source").setFieldName("source").
		setField(newSchemaBuilder().setType(SCHEMA_STRING).setFieldName("name").setOptional(false).build()).
		setField(newSchemaBuilder().setType(SCHEMA_INT64).setFieldName("server_id").setOptional(false).build()).
		setField(newSchemaBuilder().setType(SCHEMA_INT64).setFieldName("ts_sec").setOptional(false).build()).
		setField(newSchemaBuilder().setType(OPTIONAL_SCHEMA_STRING).setFieldName("gtid").setOptional(true).build()).
		setField(newSchemaBuilder().setType(SCHEMA_STRING).setFieldName("file").setOptional(false).build()).
		setField(newSchemaBuilder().setType(SCHEMA_INT64).setFieldName("pos").setOptional(false).build()).
		setField(newSchemaBuilder().setType(SCHEMA_INT32).setFieldName("row").setOptional(false).build()).
		setField(newSchemaBuilder().setType(OPTIONAL_SCHEMA_BOOLEAN).setFieldName("snapshot").setOptional(false).build()).
		setField(newSchemaBuilder().setType(OPTIONAL_SCHEMA_INT64).setFieldName("thread").setOptional(true).build()).
		setField(newSchemaBuilder().setType(OPTIONAL_SCHEMA_STRING).setFieldName("db").setOptional(true).build()).
		setField(newSchemaBuilder().setType(OPTIONAL_SCHEMA_STRING).setFieldName("table").setOptional(true).build())
}

func newSourceInfoRecord(name string, serverId int64, tsSec int64, gtid string, logFile string, logPos int64, row int64, snapshot bool, thread string, db string, table string) sourceInfo {
	source := sourceInfoSchemaBuilder().build()
	source.put("name", name)
	source.put("server_id", serverId)
	source.put("ts_sec", tsSec)
	source.put("gtid", gtid)
	source.put("file", logFile)
	source.put("pos", logPos)
	source.put("row", row)
	source.put("snapshot", snapshot)
	source.put("thread", thread)
	source.put("db", db)
	source.put("table", table)
	return source
}

type tableDMLRecord struct {
	name         recordName
	table        *config.Table
	columnSchema *Schema
	source       *Schema

	*recordMakers
}

func newTableDMLRecord(serverName string, table *config.Table, source *Schema) *tableDMLRecord {
	record := &tableDMLRecord{
		name:   recordName{serverName: serverName, databaseName: table.TableSchema, tableName: table.TableName},
		table:  table,
		source: source,
	}
	record.columnSchema = tableColumnsSchema(record.table, record.name)
	record.recordMakers = newRecordMakers(record.keySchema(), record.valueSchema())
	return record
}

func (t *tableDMLRecord) GenerateKey(value columnValue) error {
	if nil == t.table.UseUniqueKey {
		return nil
	} else {
		for _, col := range t.table.UseUniqueKey.Columns.ColumnList() {
			if _, ok := value[col.Name]; !ok {
				return fmt.Errorf("tableDMLRecord.GenerateKey error: missing unique key (%v) value", col.Name)
			}
			t.Key.put(col.Name, value[col.Name])
		}
		return nil
	}
}

func (t *tableDMLRecord) GenerateValue(before *Schema, after *Schema, op opValue) {
	t.Value.put(RECORD_FIELD_BEFORE, before)
	t.Value.put(RECORD_FIELD_AFTER, after)
	t.Value.put(RECORD_FIELD_SOURCE, t.source)
	t.Value.put(RECORD_FIELD_OP, op)
	t.Value.put(RECORD_FIELD_TS_MS, time.Now().UnixNano()/1000000)
}

func (t *tableDMLRecord) keySchema() *Schema {
	if nil == t.table.UseUniqueKey {
		// TODO: need test
		return newSchemaBuilder().build()
	} else {
		builder := newSchemaBuilder().setType(STRUCT).setName(t.name.keyStr()).setOptional(false)
		for _, col := range t.table.UseUniqueKey.Columns.ColumnList() {
			builder.setField(newSchemaBuilder().setType(typeConvert[col.Type]).setFieldName(col.Name).setName("").build())
		}
		return builder.build()
	}

}

func (t *tableDMLRecord) valueSchema() *Schema {
	return newSchemaBuilder().setType(STRUCT).setName(t.name.envelopeStr()).setOptional(false).
		setField(newSchemaBuilder().setType(SCHEMA_STRUCT).setFieldName(RECORD_FIELD_BEFORE).setName(t.name.valueStr()).setOptional(true).build()).
		setField(newSchemaBuilder().setType(SCHEMA_STRUCT).setFieldName(RECORD_FIELD_AFTER).setName(t.name.valueStr()).setOptional(true).build()).
		setField(newSchemaBuilder().setType(SCHEMA_STRUCT).setFieldName(RECORD_FIELD_SOURCE).setName(RECORD_NAME_SOURCE).setOptional(false).build()).
		setField(newSchemaBuilder().setType(SCHEMA_STRING).setFieldName(RECORD_FIELD_OP).setOptional(false).build()).
		setField(newSchemaBuilder().setType(SCHEMA_INT64).setFieldName(RECORD_FIELD_TS_MS).setOptional(true).build()).
		build()
}

func (t *tableDMLRecord) printKeyColumnsName() []string {
	return t.Key.printFields()
}

func (t *tableDMLRecord) printValueColumnsName() []string {
	return t.Key.printFields()
}

type columnValue map[string /*column name*/ ]interface{}

var (
	typeConvert = map[mysql.ColumnType]SchemaType{
		mysql.UnknownColumnType: UNKNOWN,

		mysql.DateColumnType:      SCHEMA_INT32,
		mysql.DateTimeColumnType:  SCHEMA_INT64,
		mysql.TimeColumnType:      SCHEMA_INT64,
		mysql.YearColumnType:      SCHEMA_INT32,
		mysql.TimestampColumnType: SCHEMA_STRING,

		mysql.TinyintColumnType:   SCHEMA_INT16,
		mysql.SmallintColumnType:  SCHEMA_INT16,
		mysql.IntColumnType:       SCHEMA_INT32,
		mysql.MediumIntColumnType: SCHEMA_INT32,
		mysql.BigIntColumnType:    SCHEMA_INT64,

		mysql.FloatColumnType:   SCHEMA_BYTES,
		mysql.DoubleColumnType:  SCHEMA_BYTES,
		mysql.DecimalColumnType: SCHEMA_BYTES,

		mysql.BinaryColumnType:    SCHEMA_BYTES,
		mysql.VarbinaryColumnType: SCHEMA_BYTES,
		mysql.JSONColumnType:      SCHEMA_STRING,
		mysql.EnumColumnType:      SCHEMA_STRING,

		mysql.BitColumnType:     SCHEMA_BYTES,
		mysql.CharColumnType:    SCHEMA_STRING,
		mysql.VarcharColumnType: SCHEMA_STRING,
		mysql.TextColumnType:    SCHEMA_STRING,
		mysql.BlobColumnType:    SCHEMA_BYTES,
		// TODO: more type
	}
)

func tableColumnsSchema(table *config.Table, name recordName) *Schema {
	builder := newSchemaBuilder().setType(STRUCT).setName(name.valueStr()).setOptional(true)
	for _, col := range table.OriginalTableColumns.ColumnList() {
		builder.setField(newSchemaBuilder().setType(typeConvert[col.Type]).setFieldName(col.Name).setName("").build())
	}
	return builder.build()
}

func tableColumnsSchemaFill(columnSchema *Schema, value columnValue) (*Schema, error) {
	for columnName, v := range value {
		if err := columnSchema.put(columnName, v); nil != err {
			return nil, err
		}
	}
	return columnSchema, nil
}

type recordName struct {
	serverName   string
	databaseName string
	tableName    string
}

func (n *recordName) topicStr() string {
	return fmt.Sprintf("%s.%s.%s", n.serverName, n.databaseName, n.tableName)
}

func (n *recordName) envelopeStr() string {
	return fmt.Sprintf("%s.%s.%s.Envelope", n.serverName, n.databaseName, n.tableName)
}

func (n *recordName) keyStr() string {
	return fmt.Sprintf("%s.%s.%s.Key", n.serverName, n.databaseName, n.tableName)
}

func (n *recordName) valueStr() string {
	return fmt.Sprintf("%s.%s.%s.Value", n.serverName, n.databaseName, n.tableName)
}
