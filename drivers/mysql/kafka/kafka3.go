package kafka

/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/actiontech/kafkas, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"github.com/hashicorp/nomad/plugins/drivers"
	"strconv"

	"github.com/actiontech/dtle/drivers/mysql/common"
	mysqlDriver "github.com/actiontech/dtle/drivers/mysql/mysql"

	"encoding/base64"
	"encoding/binary"
	"strings"
	"time"

	//"github.com/actiontech/dtle/drivers/kafka"
	"github.com/actiontech/dtle/drivers/mysql/mysql/binlog"
	config "github.com/actiontech/dtle/drivers/mysql/mysql/config"
	"github.com/golang/snappy"

	hclog "github.com/hashicorp/go-hclog"
	gonats "github.com/nats-io/go-nats"
	uuid "github.com/satori/go.uuid"
)

const (
	TaskStateComplete int = iota
	TaskStateRestart
	TaskStateDead
)

type KafkaRunner struct {
	logger      hclog.Logger
	subject     string
	subjectUUID uuid.UUID
	natsConn    *gonats.Conn
	waitCh      chan *drivers.ExitResult

	shutdown   bool
	shutdownCh chan struct{}

	kafkaConfig *KafkaConfig
	kafkaMgr    *KafkaManager

	tables map[string](map[string]*config.Table)
}

func NewKafkaRunner(execCtx *common.ExecContext, cfg *KafkaConfig, logger hclog.Logger) *KafkaRunner {
	/*	entry := logger.WithFields(logrus.Fields{
		"job": execCtx.Subject,
	})*/
	return &KafkaRunner{
		subject:     execCtx.Subject,
		kafkaConfig: cfg,
		logger:      logger,
		waitCh:      make(chan *drivers.ExitResult, 1),
		shutdownCh:  make(chan struct{}),
		tables:      make(map[string](map[string]*config.Table)),
	}
}
func (kr *KafkaRunner) ID() string {
	id := config.DriverCtx{
		// TODO
		DriverConfig: &config.MySQLDriverConfig{
		//ReplicateDoDb:     a.mysqlContext.ReplicateDoDb,
		//ReplicateIgnoreDb: a.mysqlContext.ReplicateIgnoreDb,
		//Gtid:              a.mysqlContext.Gtid,
		//NatsAddr:          a.mysqlContext.NatsAddr,
		//ParallelWorkers:   a.mysqlContext.ParallelWorkers,
		//ConnectionConfig:  a.mysqlContext.ConnectionConfig,
		},
	}

	data, err := json.Marshal(id)
	if err != nil {
		kr.logger.Error("kafkas: Failed to marshal ID to JSON: %s", err)
	}
	return string(data)
}

func (kr *KafkaRunner) WaitCh() chan *drivers.ExitResult {
	return kr.waitCh
}

func (kr *KafkaRunner) Shutdown() error {
	if kr.shutdown {
		return nil
	}
	if kr.natsConn != nil {
		kr.natsConn.Close()
	}
	kr.shutdown = true
	close(kr.shutdownCh)

	kr.logger.Info("kafkas: Shutting down")
	return nil
}

func (kr *KafkaRunner) Stats() (*common.TaskStatistics, error) {
	taskResUsage := &common.TaskStatistics{}
	return taskResUsage, nil
}
func (kr *KafkaRunner) initNatSubClient() (err error) {
	natsAddr := fmt.Sprintf("nats://%s", kr.kafkaConfig.NatsAddr)
	sc, err := gonats.Connect(natsAddr)
	if err != nil {
		kr.logger.Error("err: %v ,natsServer: %v kafkas: Can't connect nats server. make sure a nats streaming server is running", err, natsAddr)
		return err
	}
	kr.logger.Debug("natsServer: %v ,natsServer: %vkafkas: Connect nats server %v", natsAddr, natsAddr)

	kr.natsConn = sc
	return nil
}
func (kr *KafkaRunner) Run() {
	kr.logger.Debug("kafkas. broker %v", kr.kafkaConfig.Brokers)
	var err error
	kr.kafkaMgr, err = NewKafkaManager(kr.kafkaConfig)
	if err != nil {
		kr.logger.Error("failed to initialize kafkas err: %v", err.Error())
		kr.onError(TaskStateDead, err)
		return
	}

	err = kr.initNatSubClient()
	if err != nil {
		kr.logger.Error("initNatSubClient error: %v", err.Error())

		kr.onError(TaskStateDead, err)
		return
	}

	err = kr.initiateStreaming()
	if err != nil {
		kr.onError(TaskStateDead, err)
		return
	}
}

func (kr *KafkaRunner) getOrSetTable(schemaName string, tableName string, table *config.Table) (*config.Table, error) {
	a, ok := kr.tables[schemaName]
	if !ok {
		a = make(map[string]*config.Table)
		kr.tables[schemaName] = a
	}

	if table == nil {
		b, ok := a[tableName]
		if ok {
			kr.logger.Debug("schemaName: %v,tableName: %v,kafkas: reuse table info", schemaName, tableName)
			return b, nil
		} else {
			return nil, fmt.Errorf("DTLE_BUG kafkas: unknown table structure")
		}
	} else {
		kr.logger.Debug("schemaName: %v,tableName: %v,kafkas: new table info", schemaName, tableName)
		a[tableName] = table
		return table, nil
	}
}

func (kr *KafkaRunner) initiateStreaming() error {
	var err error

	_, err = kr.natsConn.Subscribe(fmt.Sprintf("%s_full", kr.subject), func(m *gonats.Msg) {
		kr.logger.Debug("kafkas: recv a msg")
		dumpData, err := mysqlDriver.DecodeDumpEntry(m.Data)
		if err != nil {
			kr.onError(TaskStateDead, err)
			return
		}

		if dumpData.DbSQL != "" || len(dumpData.TbSQL) > 0 {
			kr.logger.Debug("kafkas. a sql dumpEntry")
		} else if dumpData.TableSchema == "" && dumpData.TableName == "" {
			kr.logger.Debug("kafkas.  skip apply sqlMode and SystemVariablesStatement")
			if err := kr.natsConn.Publish(m.Reply, nil); err != nil {
				kr.onError(TaskStateDead, err)
				return
			}
			return
		} else {
			var tableFromDumpData *config.Table = nil
			if len(dumpData.Table) > 0 {
				tableFromDumpData = &config.Table{}
				err = DecodeGob(dumpData.Table, tableFromDumpData)
				if err != nil {
					kr.onError(TaskStateDead, err)
					return
				}
			}
			table, err := kr.getOrSetTable(dumpData.TableSchema, dumpData.TableName, tableFromDumpData)
			if err != nil {
				kr.onError(TaskStateDead, err)
				return
			}

			err = kr.kafkaTransformSnapshotData(table, dumpData)
			if err != nil {
				kr.onError(TaskStateDead, err)
				return
			}
		}

		if err := kr.natsConn.Publish(m.Reply, nil); err != nil {
			kr.onError(TaskStateDead, err)
			return
		}
		kr.logger.Debug("kafkas: after publish nats reply")
	})
	if err != nil {
		return err
	}

	_, err = kr.natsConn.Subscribe(fmt.Sprintf("%s_full_complete", kr.subject), func(m *gonats.Msg) {
		if err := kr.natsConn.Publish(m.Reply, nil); err != nil {
			kr.onError(TaskStateDead, err)
		}
	})
	if err != nil {
		return err
	}

	_, err = kr.natsConn.Subscribe(fmt.Sprintf("%s_incr_hete", kr.subject), func(m *gonats.Msg) {
		var binlogEntries binlog.BinlogEntries
		if err := Decode(m.Data, &binlogEntries); err != nil {
			kr.onError(TaskStateDead, err)
		}

		for _, binlogEntry := range binlogEntries.Entries {
			err = kr.kafkaTransformDMLEventQuery(binlogEntry)
		}

		if err := kr.natsConn.Publish(m.Reply, nil); err != nil {
			kr.onError(TaskStateDead, err)
		}
		kr.logger.Debug("applier. incr. ack-recv. nEntries: %v", binlogEntries.Entries)
	})
	if err != nil {
		return err
	}

	return nil
}

// TODO move to one place
func Decode(data []byte, vPtr interface{}) (err error) {
	msg, err := snappy.Decode(nil, data)
	if err != nil {
		return err
	}

	return gob.NewDecoder(bytes.NewBuffer(msg)).Decode(vPtr)
}
func DecodeGob(data []byte, vPtr interface{}) (err error) {
	return gob.NewDecoder(bytes.NewBuffer(data)).Decode(vPtr)
}

func (kr *KafkaRunner) onError(state int, err error) {
	if kr.shutdown {
		return
	}
	switch state {
	case TaskStateComplete:
		kr.logger.Info("kafkas: Done migrating")
	case TaskStateRestart:
		if kr.natsConn != nil {
			if err := kr.natsConn.Publish(fmt.Sprintf("%s_restart", kr.subject), []byte(kr.kafkaConfig.Gtid)); err != nil {
				kr.logger.Error("kafkas: Trigger restart err: %v", err)
			}
		}
	default:
		if kr.natsConn != nil {
			if err := kr.natsConn.Publish(fmt.Sprintf("%s_error", kr.subject), []byte(kr.kafkaConfig.Gtid)); err != nil {
				kr.logger.Error("kafkas: Trigger shutdown, err: %v", err)
			}
		}
	}

	kr.waitCh <- &drivers.ExitResult{
		ExitCode:  state,
		Signal:    0,
		OOMKilled: false,
		Err:       err,
	}
	kr.Shutdown()
}

func (kr *KafkaRunner) kafkaTransformSnapshotData(table *config.Table, value *mysqlDriver.DumpEntry) error {
	var err error

	tableIdent := fmt.Sprintf("%v.%v.%v", kr.kafkaMgr.Cfg.Topic, table.TableSchema, table.TableName)
	kr.logger.Debug("kafkas: kafkaTransformSnapshotData value: %v", value.ValuesX)
	for _, rowValues := range value.ValuesX {
		keyPayload := NewRow()
		valuePayload := NewValuePayload()
		valuePayload.Source.Version = "0.0.1"
		valuePayload.Source.Name = kr.kafkaMgr.Cfg.Topic
		valuePayload.Source.ServerID = 0 // TODO
		valuePayload.Source.TsSec = 0    // TODO the timestamp in seconds
		valuePayload.Source.Gtid = nil
		valuePayload.Source.File = ""
		valuePayload.Source.Pos = 0
		valuePayload.Source.Row = 0 // TODO "the row within the event (if there is more than one)".
		valuePayload.Source.Snapshot = true
		valuePayload.Source.Thread = nil // TODO
		valuePayload.Source.Db = table.TableSchema
		valuePayload.Source.Table = table.TableName
		valuePayload.Op = RECORD_OP_INSERT
		valuePayload.Source.Query = nil
		valuePayload.TsMs = common.CurrentTimeMillis()

		valuePayload.Before = nil
		valuePayload.After = NewRow()

		columnList := table.OriginalTableColumns.ColumnList()
		valueColDef, keyColDef := kafkaColumnListToColDefs(table.OriginalTableColumns)
		keySchema := NewKeySchema(tableIdent, keyColDef)

		for i, _ := range columnList {
			var value interface{}

			if rowValues[i] != nil {
				valueStr := string(*rowValues[i])
				switch columnList[i].Type {
				case config.TinyintColumnType, config.SmallintColumnType, config.MediumIntColumnType, config.IntColumnType:
					value, err = strconv.ParseInt(valueStr, 10, 64)
					if err != nil {
						return err
					}
				case config.BigIntColumnType:
					if columnList[i].IsUnsigned {
						valueUint64, err := strconv.ParseUint(valueStr, 10, 64)
						if err != nil {
							return err
						}
						value = int64(valueUint64)
					} else {
						value, err = strconv.ParseInt(valueStr, 10, 64)
						if err != nil {
							return err
						}
					}
				case config.DoubleColumnType:
					value, err = strconv.ParseFloat(valueStr, 64)
					if err != nil {
						return err
					}
				case config.FloatColumnType:
					value, err = strconv.ParseFloat(valueStr, 64)
					if err != nil {
						return err
					}
				case config.DecimalColumnType:
					value = DecimalValueFromStringMysql(valueStr)
				case config.TimeColumnType:
					if valueStr != "" && columnList[i].ColumnType == "timestamp" {
						value = valueStr[:10] + "T" + valueStr[11:] + "Z"
					} else {
						value = TimeValue(valueStr)
					}

				case config.BinaryColumnType:
					value = base64.StdEncoding.EncodeToString([]byte(valueStr))
				case config.BitColumnType:
					value = base64.StdEncoding.EncodeToString([]byte(valueStr))
				case config.BlobColumnType:
					value = base64.StdEncoding.EncodeToString([]byte(valueStr))
				case config.VarbinaryColumnType:
					value = base64.StdEncoding.EncodeToString([]byte(valueStr))
				case config.DateColumnType, config.DateTimeColumnType:
					if valueStr != "" && columnList[i].ColumnType == "datetime" {
						value = DateTimeValue(valueStr)
					} else if valueStr != "" {
						value = DateValue(valueStr)
					}

				case config.YearColumnType:
					if valueStr != "" {
						value = YearValue(valueStr)
					} else {
						value = valueStr
					}
				default:
					value = valueStr
				}
			} else {
				value = nil
			}

			if columnList[i].IsPk() {
				keyPayload.AddField(columnList[i].RawName, value)
			}
			kr.logger.Debug("kafkas: kafkaTransformSnapshotData rowvalue: %v", value)
			valuePayload.After.AddField(columnList[i].RawName, value)
		}

		valueSchema := NewEnvelopeSchema(tableIdent, valueColDef)

		k := DbzOutput{
			Schema:  keySchema,
			Payload: keyPayload,
		}
		v := DbzOutput{
			Schema:  valueSchema,
			Payload: valuePayload,
		}

		kBs, err := json.Marshal(k)
		if err != nil {
			return fmt.Errorf("kafkas: serialization error: %v", err)
		}
		vBs, err := json.Marshal(v)
		if err != nil {
			return fmt.Errorf("kafkas: serialization error: %v", err)
		}
		//vBs = []byte(strings.Replace(string(vBs), "\"field\":\"snapshot\"", "\"default\":false,\"field\":\"snapshot\"", -1))
		err = kr.kafkaMgr.Send(tableIdent, kBs, vBs)
		if err != nil {
			return err
		}
		kr.logger.Debug("kafkas: sent one msg")
	}
	return nil
}

func (kr *KafkaRunner) kafkaTransformDMLEventQuery(dmlEvent *binlog.BinlogEntry) (err error) {
	for i, _ := range dmlEvent.Events {
		dataEvent := &dmlEvent.Events[i]
		// this must be executed before skipping DDL
		table, err := kr.getOrSetTable(dataEvent.DatabaseName, dataEvent.TableName, dataEvent.Table)
		if err != nil {
			return err
		}

		// skipping DDL
		if dataEvent.DML == binlog.NotDML {
			continue
		}

		var op string
		var before *Row
		var after *Row

		switch dataEvent.DML {
		case binlog.InsertDML:
			op = RECORD_OP_INSERT
			before = nil
			after = NewRow()
		case binlog.DeleteDML:
			op = RECORD_OP_DELETE
			before = NewRow()
			after = nil
		case binlog.UpdateDML:
			op = RECORD_OP_UPDATE
			before = NewRow()
			after = NewRow()
		}

		tableIdent := fmt.Sprintf("%v.%v.%v", kr.kafkaMgr.Cfg.Topic, table.TableSchema, table.TableName)

		keyPayload := NewRow()
		colList := table.OriginalTableColumns.ColumnList()
		colDefs, keyColDefs := kafkaColumnListToColDefs(table.OriginalTableColumns)

		for i, _ := range colList {
			colName := colList[i].RawName

			var beforeValue interface{}
			var afterValue interface{}

			if before != nil {
				beforeValue = *dataEvent.WhereColumnValues.AbstractValues[i]
			}
			if after != nil {
				afterValue = *dataEvent.NewColumnValues.AbstractValues[i]
			}

			switch colList[i].Type {
			case config.DecimalColumnType:
				// nil: either entire row does not exist or this field is NULL
				if beforeValue != nil {
					beforeValue = DecimalValueFromStringMysql(beforeValue.(string))
				}
				if afterValue != nil {
					afterValue = DecimalValueFromStringMysql(afterValue.(string))
				}
			case config.BigIntColumnType:
				if colList[i].IsUnsigned {
					if beforeValue != nil {
						beforeValue = int64(beforeValue.(uint64))
					}
					if afterValue != nil {
						afterValue = int64(afterValue.(uint64))
					}
				}
			case config.TimeColumnType, config.TimestampColumnType:
				if beforeValue != nil && colList[i].ColumnType == "timestamp" {
					beforeValue = beforeValue.(string)[:10] + "T" + beforeValue.(string)[11:] + "Z"
				} else if beforeValue != nil {
					beforeValue = TimeValue(beforeValue.(string))
				}
				if afterValue != nil && colList[i].ColumnType == "timestamp" {
					afterValue = afterValue.(string)[:10] + "T" + afterValue.(string)[11:] + "Z"
				} else if afterValue != nil {
					afterValue = TimeValue(afterValue.(string))
				}
			case config.DateColumnType, config.DateTimeColumnType:
				if beforeValue != nil && colList[i].ColumnType == "datetime" {
					beforeValue = DateTimeValue(beforeValue.(string))
				} else if beforeValue != nil {
					beforeValue = DateValue(beforeValue.(string))
				}
				if afterValue != nil && colList[i].ColumnType == "datetime" {
					afterValue = DateTimeValue(afterValue.(string))
				} else if afterValue != nil {
					afterValue = DateValue(afterValue.(string))
				}
			case config.VarbinaryColumnType:
				if beforeValue != nil {
					beforeValue = beforeValue.(string)
				}
				if afterValue != nil {
					afterValue = afterValue.(string)
				}
			case config.BinaryColumnType:

				if beforeValue != nil {
					beforeValue = getBinaryValue(colList[i].ColumnType, beforeValue.(string))
				}
				if afterValue != nil {
					afterValue = getBinaryValue(colList[i].ColumnType, afterValue.(string))
				}
			case config.TinytextColumnType:
				//println("beforeValue:",string(beforeValue.([]uint8)))
				if beforeValue != nil {
					beforeValue = string(beforeValue.([]uint8))
				}
				if afterValue != nil {
					afterValue = string(afterValue.([]uint8))
				}
			case config.FloatColumnType:
				if beforeValue != nil {
					beforeValue = beforeValue.(float32)
				}
				if afterValue != nil {
					afterValue = afterValue.(float32)
				}
			case config.EnumColumnType:
				enums := strings.Split(colList[i].ColumnType[5:len(colList[i].ColumnType)-1], ",")
				if beforeValue != nil {
					beforeValue = strings.Replace(enums[beforeValue.(int64)-1], "'", "", -1)
				}
				if afterValue != nil {
					afterValue = strings.Replace(enums[afterValue.(int64)-1], "'", "", -1)
				}
			case config.SetColumnType:
				columnType := colList[i].ColumnType
				if beforeValue != nil {
					beforeValue = getSetValue(beforeValue.(int64), columnType)
				}
				if afterValue != nil {
					afterValue = getSetValue(afterValue.(int64), columnType)
				}

			case config.BitColumnType:
				if beforeValue != nil {
					beforeValue = getBitValue(colList[i].ColumnType, beforeValue.(int64))
				}
				if afterValue != nil {
					afterValue = getBitValue(colList[i].ColumnType, afterValue.(int64))
				}
			default:
				// do nothing
			}

			if colList[i].IsPk() {
				if before != nil {
					// update/delete: use before
					keyPayload.AddField(colName, beforeValue)
				} else {
					// insert: use after
					keyPayload.AddField(colName, afterValue)
				}
			}

			if before != nil {
				kr.logger.Debug("kafkas. beforeValue: %v", beforeValue)
				before.AddField(colName, beforeValue)
			}
			if after != nil {
				kr.logger.Debug("kafkas. afterValue: %v", afterValue)
				after.AddField(colName, afterValue)
			}
		}

		valuePayload := NewValuePayload()
		valuePayload.Before = before
		valuePayload.After = after

		valuePayload.Source.Version = "0.0.1"
		valuePayload.Source.Name = kr.kafkaMgr.Cfg.Topic
		valuePayload.Source.ServerID = 1 // TODO
		valuePayload.Source.TsSec = time.Now().Unix()
		valuePayload.Source.Gtid = dmlEvent.Coordinates.GetGtidForThisTx()
		valuePayload.Source.File = dmlEvent.Coordinates.LogFile
		valuePayload.Source.Pos = dataEvent.LogPos
		valuePayload.Source.Row = 0          // TODO "the row within the event (if there is more than one)".
		valuePayload.Source.Snapshot = false // TODO "whether this event was part of a snapshot"

		valuePayload.Source.Query = nil
		// My guess: for full range, snapshot=true, else false
		valuePayload.Source.Thread = nil // TODO
		valuePayload.Source.Db = dataEvent.DatabaseName
		valuePayload.Source.Table = dataEvent.TableName
		valuePayload.Op = op
		valuePayload.TsMs = common.CurrentTimeMillis()

		valueSchema := NewEnvelopeSchema(tableIdent, colDefs)

		keySchema := NewKeySchema(tableIdent, keyColDefs)
		k := DbzOutput{
			Schema:  keySchema,
			Payload: keyPayload,
		}
		v := DbzOutput{
			Schema:  valueSchema,
			Payload: valuePayload,
		}
		kBs, err := json.Marshal(k)
		if err != nil {
			return err
		}
		vBs, err := json.Marshal(v)
		if err != nil {
			return err
		}
		//	vBs = []byte(strings.Replace(string(vBs), "\"field\":\"snapshot\"", "\"default\":false,\"field\":\"snapshot\"", -1))
		err = kr.kafkaMgr.Send(tableIdent, kBs, vBs)
		if err != nil {
			return err
		}
		kr.logger.Debug("kafkas: sent one msg")

		// tombstone event for DELETE
		if dataEvent.DML == binlog.DeleteDML {
			v2 := DbzOutput{
				Schema:  nil,
				Payload: nil,
			}
			v2Bs, err := json.Marshal(v2)
			if err != nil {
				return err
			}
			err = kr.kafkaMgr.Send(tableIdent, kBs, v2Bs)
			if err != nil {
				return err
			}
			kr.logger.Debug("kafkas: sent one msg")
		}
	}

	return nil
}

func getSetValue(num int64, set string) string {
	if num == 0 {
		return ""
	}
	value := ""
	sets := strings.Split(set[5:len(set)-1], ",")
	for i := 0; i < len(sets); i++ {
		a := uint(len(sets) - 1 - i)
		val := num / (1 << a)
		num = num % (1 << a)
		if val == 1 {
			value = strings.Replace(sets[a], "'", "", -1) + "," + value
		}
	}
	return value[0 : len(value)-1]
}

func getBinaryValue(binary string, value string) string {
	binaryLen := binary[7 : len(binary)-1]
	lens, err := strconv.Atoi(binaryLen)
	if err != nil {
		return ""
	}
	valueLen := len(value)
	var buffer bytes.Buffer
	buffer.Write([]byte(value))
	if lens-valueLen > 0 {
		buffer.Write(make([]byte, lens-valueLen))
	}
	return base64.StdEncoding.EncodeToString(buffer.Bytes())
}
func getBitValue(bit string, value int64) string {
	bitLen := bit[4 : len(bit)-1]
	lens, _ := strconv.Atoi(bitLen)
	bitNumber := lens / 8
	if lens%8 != 0 {
		bitNumber = bitNumber + 1
	}
	var buf = make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(value))

	return base64.StdEncoding.EncodeToString(buf[8-bitNumber:])
}

func kafkaColumnListToColDefs(colList *config.ColumnList) (valColDefs ColDefs, keyColDefs ColDefs) {
	cols := colList.ColumnList()
	for i, _ := range cols {
		var field *Schema
		defaultValue := cols[i].Default
		if defaultValue == "" {
			defaultValue = nil
		}
		optional := cols[i].Nullable
		fieldName := cols[i].RawName
		switch cols[i].Type {
		case config.UnknownColumnType:
			// TODO warning
			field = NewSimpleSchemaWithDefaultField("", optional, fieldName, defaultValue)

		case config.BitColumnType:
			field = NewBitsField(optional, fieldName, cols[i].ColumnType[4:len(cols[i].ColumnType)-1], defaultValue)
		case config.BlobColumnType:
			field = NewSimpleSchemaWithDefaultField(SCHEMA_TYPE_BYTES, optional, fieldName, defaultValue)
		case config.BinaryColumnType:
			field = NewSimpleSchemaWithDefaultField(SCHEMA_TYPE_BYTES, optional, fieldName, defaultValue)
		case config.VarbinaryColumnType:
			field = NewSimpleSchemaWithDefaultField(SCHEMA_TYPE_BYTES, optional, fieldName, defaultValue)
		case config.TextColumnType:
			fallthrough
		case config.CharColumnType:
			fallthrough
		case config.VarcharColumnType:
			field = NewSimpleSchemaWithDefaultField(SCHEMA_TYPE_STRING, optional, fieldName, defaultValue)
		case config.EnumColumnType:
			field = NewEnumField(SCHEMA_TYPE_STRING, optional, fieldName, cols[i].ColumnType, defaultValue)
		case config.SetColumnType:
			field = NewSetField(SCHEMA_TYPE_STRING, optional, fieldName, cols[i].ColumnType, defaultValue)
		case config.TinyintColumnType:
			field = NewSimpleSchemaWithDefaultField(SCHEMA_TYPE_INT16, optional, fieldName, defaultValue)
		case config.TinytextColumnType:
			field = NewSimpleSchemaWithDefaultField(SCHEMA_TYPE_STRING, optional, fieldName, defaultValue)
		case config.SmallintColumnType:
			if cols[i].IsUnsigned {
				field = NewSimpleSchemaWithDefaultField(SCHEMA_TYPE_INT32, optional, fieldName, defaultValue)
			} else {
				field = NewSimpleSchemaWithDefaultField(SCHEMA_TYPE_INT16, optional, fieldName, defaultValue)
			}
		case config.MediumIntColumnType: // 24 bit in config
			field = NewSimpleSchemaWithDefaultField(SCHEMA_TYPE_INT32, optional, fieldName, defaultValue)
		case config.IntColumnType:
			if cols[i].IsUnsigned {
				field = NewSimpleSchemaWithDefaultField(SCHEMA_TYPE_INT64, optional, fieldName, defaultValue)
			} else {
				field = NewSimpleSchemaWithDefaultField(SCHEMA_TYPE_INT32, optional, fieldName, defaultValue)
			}
		case config.BigIntColumnType:
			field = NewSimpleSchemaWithDefaultField(SCHEMA_TYPE_INT64, optional, fieldName, defaultValue)
		case config.FloatColumnType:
			field = NewSimpleSchemaWithDefaultField(SCHEMA_TYPE_FLOAT64, optional, fieldName, defaultValue)
		case config.DoubleColumnType:
			field = NewSimpleSchemaWithDefaultField(SCHEMA_TYPE_FLOAT64, optional, fieldName, defaultValue)
		case config.DecimalColumnType:
			field = NewDecimalField(cols[i].Precision, cols[i].Scale, optional, fieldName, defaultValue)
		case config.DateColumnType:
			if cols[i].ColumnType == "datetime" {
				field = NewDateTimeField(optional, fieldName, defaultValue)
			} else {
				field = NewDateField(SCHEMA_TYPE_INT32, optional, fieldName, defaultValue)
			}
		case config.YearColumnType:
			field = NewYearField(SCHEMA_TYPE_INT32, optional, fieldName, defaultValue)
		case config.DateTimeColumnType:
			field = NewDateTimeField(optional, fieldName, defaultValue)
		case config.TimeColumnType:
			if cols[i].ColumnType == "timestamp" {
				field = NewTimeStampField(SCHEMA_TYPE_STRING, optional, fieldName, defaultValue)
			} else {
				field = NewTimeField(optional, fieldName, defaultValue)
			}
		case config.JSONColumnType:
			field = NewJsonField(optional, fieldName)
		default:
			// TODO report a BUG
			field = NewSimpleSchemaWithDefaultField("", optional, fieldName, defaultValue)
		}

		addToKey := cols[i].IsPk()
		if addToKey {
			keyColDefs = append(keyColDefs, field)
		}

		valColDefs = append(valColDefs, field)
	}
	return valColDefs, keyColDefs
}
