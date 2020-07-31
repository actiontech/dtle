package kafka

/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/actiontech/dtle/drivers/mysql/config"
	"github.com/hashicorp/nomad/plugins/drivers"
	"github.com/pkg/errors"
	"strconv"

	"encoding/base64"
	"encoding/binary"
	"github.com/actiontech/dtle/drivers/mysql/common"
	"strings"
	"time"

	"github.com/actiontech/dtle/drivers/mysql/mysql/binlog"
	"github.com/actiontech/dtle/drivers/mysql/mysql/mysqlconfig"
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

	kafkaConfig *config.KafkaConfig
	kafkaMgr    *KafkaManager

	storeManager *common.StoreManager

	tables map[string](map[string]*mysqlconfig.Table)
}

func NewKafkaRunner(execCtx *common.ExecContext, cfg *config.KafkaConfig, logger hclog.Logger,
	storeManager *common.StoreManager) *KafkaRunner {
	/*	entry := logger.WithFields(logrus.Fields{
		"job": execCtx.Subject,
	})*/
	return &KafkaRunner{
		subject:      execCtx.Subject,
		kafkaConfig:  cfg,
		logger:       logger,
		waitCh:       make(chan *drivers.ExitResult, 1),
		shutdownCh:   make(chan struct{}),
		tables:       make(map[string](map[string]*mysqlconfig.Table)),
		storeManager: storeManager,
	}
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

	kr.logger.Info("Shutting down")
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
		kr.logger.Error("Can't connect nats server.", "err", err, "natsAddr", natsAddr)
		return err
	}
	kr.logger.Debug("kafka: Connect nats server", "natsAddr", natsAddr)

	kr.natsConn = sc
	return nil
}
func (kr *KafkaRunner) Run() {
	kr.logger.Debug("Run", "brokers", kr.kafkaConfig.Brokers)
	var err error

	kr.logger.Info("go WatchAndPutNats")
	go kr.storeManager.WatchAndPutNats(kr.subject, kr.kafkaConfig.NatsAddr, kr.shutdownCh, func(err error) {
		kr.onError(TaskStateDead, errors.Wrap(err, "WatchAndPutNats"))
	})

	{
		gtid, err := kr.storeManager.GetGtidForJob(kr.subject)
		if err != nil {
			kr.onError(TaskStateDead, errors.Wrap(err, "GetGtidForJob"))
			return
		}
		if gtid != "" {
			kr.logger.Info("Got gtid from consul", "gtid", gtid)
			kr.kafkaConfig.Gtid = gtid
		}
	}

	kr.kafkaMgr, err = NewKafkaManager(kr.kafkaConfig)
	if err != nil {
		kr.logger.Error("failed to initialize kafka", "err", err)
		kr.onError(TaskStateDead, err)
		return
	}

	err = kr.initNatSubClient()
	if err != nil {
		kr.logger.Error("initNatSubClient", "err", err)

		kr.onError(TaskStateDead, err)
		return
	}

	err = kr.initiateStreaming()
	if err != nil {
		kr.onError(TaskStateDead, err)
		return
	}
}

func (kr *KafkaRunner) getOrSetTable(schemaName string, tableName string, table *mysqlconfig.Table) (*mysqlconfig.Table, error) {
	a, ok := kr.tables[schemaName]
	if !ok {
		a = make(map[string]*mysqlconfig.Table)
		kr.tables[schemaName] = a
	}

	if table == nil {
		b, ok := a[tableName]
		if ok {
			kr.logger.Debug("reuse table info", "schemaName", schemaName, "tableName", tableName)
			return b, nil
		} else {
			return nil, fmt.Errorf("DTLE_BUG kafka: unknown table structure")
		}
	} else {
		kr.logger.Debug("new table info", "schemaName", schemaName, "tableName", tableName)
		a[tableName] = table
		return table, nil
	}
}

func (kr *KafkaRunner) initiateStreaming() error {
	var err error

	_, err = kr.natsConn.Subscribe(fmt.Sprintf("%s_full", kr.subject), func(m *gonats.Msg) {
		kr.logger.Debug("recv a msg")
		dumpData, err := common.DecodeDumpEntry(m.Data)
		if err != nil {
			kr.onError(TaskStateDead, err)
			return
		}

		if dumpData.DbSQL != "" || len(dumpData.TbSQL) > 0 {
			kr.logger.Debug("a sql dumpEntry")
		} else if dumpData.TableSchema == "" && dumpData.TableName == "" {
			kr.logger.Debug("skip apply sqlMode and SystemVariablesStatement")
			if err := kr.natsConn.Publish(m.Reply, nil); err != nil {
				kr.onError(TaskStateDead, err)
				return
			}
			return
		} else {
			var tableFromDumpData *mysqlconfig.Table = nil
			if len(dumpData.Table) > 0 {
				tableFromDumpData = &mysqlconfig.Table{}
				err = common.DecodeGob(dumpData.Table, tableFromDumpData)
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
		kr.logger.Debug("after publish nats reply")
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
		if err := common.Decode(m.Data, &binlogEntries); err != nil {
			kr.onError(TaskStateDead, err)
		}

		for _, binlogEntry := range binlogEntries.Entries {
			err = kr.kafkaTransformDMLEventQuery(binlogEntry)
		}

		if err := kr.natsConn.Publish(m.Reply, nil); err != nil {
			kr.onError(TaskStateDead, err)
		}
		kr.logger.Debug("applier. incr. ack-recv.", "nEntries", binlogEntries.Entries)
	})
	if err != nil {
		return err
	}

	return nil
}

func (kr *KafkaRunner) onError(state int, err error) {
	if kr.shutdown {
		return
	}
	switch state {
	case TaskStateComplete:
		kr.logger.Info("Done migrating")
	case TaskStateRestart:
		if kr.natsConn != nil {
			if err := kr.natsConn.Publish(fmt.Sprintf("%s_restart", kr.subject), []byte(kr.kafkaConfig.Gtid)); err != nil {
				kr.logger.Error("Trigger restart", "err", err)
			}
		}
	default:
		if kr.natsConn != nil {
			if err := kr.natsConn.Publish(fmt.Sprintf("%s_error", kr.subject), []byte(kr.kafkaConfig.Gtid)); err != nil {
				kr.logger.Error("Trigger shutdown", "err", err)
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

func (kr *KafkaRunner) kafkaTransformSnapshotData(table *mysqlconfig.Table, value *common.DumpEntry) error {
	var err error

	tableIdent := fmt.Sprintf("%v.%v.%v", kr.kafkaMgr.Cfg.Topic, table.TableSchema, table.TableName)
	kr.logger.Debug("kafkaTransformSnapshotData", "value", value.ValuesX)
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
				case mysqlconfig.TinyintColumnType, mysqlconfig.SmallintColumnType, mysqlconfig.MediumIntColumnType, mysqlconfig.IntColumnType:
					value, err = strconv.ParseInt(valueStr, 10, 64)
					if err != nil {
						return err
					}
				case mysqlconfig.BigIntColumnType:
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
				case mysqlconfig.DoubleColumnType:
					value, err = strconv.ParseFloat(valueStr, 64)
					if err != nil {
						return err
					}
				case mysqlconfig.FloatColumnType:
					value, err = strconv.ParseFloat(valueStr, 64)
					if err != nil {
						return err
					}
				case mysqlconfig.DecimalColumnType:
					value = DecimalValueFromStringMysql(valueStr)
				case mysqlconfig.TimeColumnType:
					if valueStr != "" && columnList[i].ColumnType == "timestamp" {
						value = valueStr[:10] + "T" + valueStr[11:] + "Z"
					} else {
						value = TimeValue(valueStr)
					}

				case mysqlconfig.BinaryColumnType:
					value = base64.StdEncoding.EncodeToString([]byte(valueStr))
				case mysqlconfig.BitColumnType:
					value = base64.StdEncoding.EncodeToString([]byte(valueStr))
				case mysqlconfig.BlobColumnType:
					value = base64.StdEncoding.EncodeToString([]byte(valueStr))
				case mysqlconfig.VarbinaryColumnType:
					value = base64.StdEncoding.EncodeToString([]byte(valueStr))
				case mysqlconfig.DateColumnType, mysqlconfig.DateTimeColumnType:
					if valueStr != "" && columnList[i].ColumnType == "datetime" {
						value = DateTimeValue(valueStr)
					} else if valueStr != "" {
						value = DateValue(valueStr)
					}

				case mysqlconfig.YearColumnType:
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
			kr.logger.Debug("kafkaTransformSnapshotData", "rowvalue", value)
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
			return fmt.Errorf("serialization error: %v", err)
		}
		vBs, err := json.Marshal(v)
		if err != nil {
			return fmt.Errorf("serialization error: %v", err)
		}
		//vBs = []byte(strings.Replace(string(vBs), "\"field\":\"snapshot\"", "\"default\":false,\"field\":\"snapshot\"", -1))
		err = kr.kafkaMgr.Send(tableIdent, kBs, vBs)
		if err != nil {
			return err
		}
		kr.logger.Debug("sent one msg")
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
			case mysqlconfig.DecimalColumnType:
				// nil: either entire row does not exist or this field is NULL
				if beforeValue != nil {
					beforeValue = DecimalValueFromStringMysql(beforeValue.(string))
				}
				if afterValue != nil {
					afterValue = DecimalValueFromStringMysql(afterValue.(string))
				}
			case mysqlconfig.BigIntColumnType:
				if colList[i].IsUnsigned {
					if beforeValue != nil {
						beforeValue = int64(beforeValue.(uint64))
					}
					if afterValue != nil {
						afterValue = int64(afterValue.(uint64))
					}
				}
			case mysqlconfig.TimeColumnType, mysqlconfig.TimestampColumnType:
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
			case mysqlconfig.DateColumnType, mysqlconfig.DateTimeColumnType:
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
			case mysqlconfig.VarbinaryColumnType:
				if beforeValue != nil {
					beforeValue = beforeValue.(string)
				}
				if afterValue != nil {
					afterValue = afterValue.(string)
				}
			case mysqlconfig.BinaryColumnType:

				if beforeValue != nil {
					beforeValue = getBinaryValue(colList[i].ColumnType, beforeValue.(string))
				}
				if afterValue != nil {
					afterValue = getBinaryValue(colList[i].ColumnType, afterValue.(string))
				}
			case mysqlconfig.TinytextColumnType:
				//println("beforeValue:",string(beforeValue.([]uint8)))
				if beforeValue != nil {
					beforeValue = string(beforeValue.([]uint8))
				}
				if afterValue != nil {
					afterValue = string(afterValue.([]uint8))
				}
			case mysqlconfig.FloatColumnType:
				if beforeValue != nil {
					beforeValue = beforeValue.(float32)
				}
				if afterValue != nil {
					afterValue = afterValue.(float32)
				}
			case mysqlconfig.EnumColumnType:
				enums := strings.Split(colList[i].ColumnType[5:len(colList[i].ColumnType)-1], ",")
				if beforeValue != nil {
					beforeValue = strings.Replace(enums[beforeValue.(int64)-1], "'", "", -1)
				}
				if afterValue != nil {
					afterValue = strings.Replace(enums[afterValue.(int64)-1], "'", "", -1)
				}
			case mysqlconfig.SetColumnType:
				columnType := colList[i].ColumnType
				if beforeValue != nil {
					beforeValue = getSetValue(beforeValue.(int64), columnType)
				}
				if afterValue != nil {
					afterValue = getSetValue(afterValue.(int64), columnType)
				}

			case mysqlconfig.BitColumnType:
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
				kr.logger.Debug("beforeValue", "v", beforeValue)
				before.AddField(colName, beforeValue)
			}
			if after != nil {
				kr.logger.Debug("afterValue", "v", afterValue)
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
		kr.logger.Debug("sent one msg")

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
			kr.logger.Debug("sent one msg")
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

func kafkaColumnListToColDefs(colList *mysqlconfig.ColumnList) (valColDefs ColDefs, keyColDefs ColDefs) {
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
		case mysqlconfig.UnknownColumnType:
			// TODO warning
			field = NewSimpleSchemaWithDefaultField("", optional, fieldName, defaultValue)

		case mysqlconfig.BitColumnType:
			field = NewBitsField(optional, fieldName, cols[i].ColumnType[4:len(cols[i].ColumnType)-1], defaultValue)
		case mysqlconfig.BlobColumnType:
			field = NewSimpleSchemaWithDefaultField(SCHEMA_TYPE_BYTES, optional, fieldName, defaultValue)
		case mysqlconfig.BinaryColumnType:
			field = NewSimpleSchemaWithDefaultField(SCHEMA_TYPE_BYTES, optional, fieldName, defaultValue)
		case mysqlconfig.VarbinaryColumnType:
			field = NewSimpleSchemaWithDefaultField(SCHEMA_TYPE_BYTES, optional, fieldName, defaultValue)
		case mysqlconfig.TextColumnType:
			fallthrough
		case mysqlconfig.CharColumnType:
			fallthrough
		case mysqlconfig.VarcharColumnType:
			field = NewSimpleSchemaWithDefaultField(SCHEMA_TYPE_STRING, optional, fieldName, defaultValue)
		case mysqlconfig.EnumColumnType:
			field = NewEnumField(SCHEMA_TYPE_STRING, optional, fieldName, cols[i].ColumnType, defaultValue)
		case mysqlconfig.SetColumnType:
			field = NewSetField(SCHEMA_TYPE_STRING, optional, fieldName, cols[i].ColumnType, defaultValue)
		case mysqlconfig.TinyintColumnType:
			field = NewSimpleSchemaWithDefaultField(SCHEMA_TYPE_INT16, optional, fieldName, defaultValue)
		case mysqlconfig.TinytextColumnType:
			field = NewSimpleSchemaWithDefaultField(SCHEMA_TYPE_STRING, optional, fieldName, defaultValue)
		case mysqlconfig.SmallintColumnType:
			if cols[i].IsUnsigned {
				field = NewSimpleSchemaWithDefaultField(SCHEMA_TYPE_INT32, optional, fieldName, defaultValue)
			} else {
				field = NewSimpleSchemaWithDefaultField(SCHEMA_TYPE_INT16, optional, fieldName, defaultValue)
			}
		case mysqlconfig.MediumIntColumnType: // 24 bit in config
			field = NewSimpleSchemaWithDefaultField(SCHEMA_TYPE_INT32, optional, fieldName, defaultValue)
		case mysqlconfig.IntColumnType:
			if cols[i].IsUnsigned {
				field = NewSimpleSchemaWithDefaultField(SCHEMA_TYPE_INT64, optional, fieldName, defaultValue)
			} else {
				field = NewSimpleSchemaWithDefaultField(SCHEMA_TYPE_INT32, optional, fieldName, defaultValue)
			}
		case mysqlconfig.BigIntColumnType:
			field = NewSimpleSchemaWithDefaultField(SCHEMA_TYPE_INT64, optional, fieldName, defaultValue)
		case mysqlconfig.FloatColumnType:
			field = NewSimpleSchemaWithDefaultField(SCHEMA_TYPE_FLOAT64, optional, fieldName, defaultValue)
		case mysqlconfig.DoubleColumnType:
			field = NewSimpleSchemaWithDefaultField(SCHEMA_TYPE_FLOAT64, optional, fieldName, defaultValue)
		case mysqlconfig.DecimalColumnType:
			field = NewDecimalField(cols[i].Precision, cols[i].Scale, optional, fieldName, defaultValue)
		case mysqlconfig.DateColumnType:
			if cols[i].ColumnType == "datetime" {
				field = NewDateTimeField(optional, fieldName, defaultValue)
			} else {
				field = NewDateField(SCHEMA_TYPE_INT32, optional, fieldName, defaultValue)
			}
		case mysqlconfig.YearColumnType:
			field = NewYearField(SCHEMA_TYPE_INT32, optional, fieldName, defaultValue)
		case mysqlconfig.DateTimeColumnType:
			field = NewDateTimeField(optional, fieldName, defaultValue)
		case mysqlconfig.TimeColumnType:
			if cols[i].ColumnType == "timestamp" {
				field = NewTimeStampField(SCHEMA_TYPE_STRING, optional, fieldName, defaultValue)
			} else {
				field = NewTimeField(optional, fieldName, defaultValue)
			}
		case mysqlconfig.JSONColumnType:
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
