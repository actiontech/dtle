/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package kafka3

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	gomysql "github.com/siddontang/go-mysql/mysql"
	"strconv"

	"github.com/actiontech/dtle/internal/client/driver/common"

	mysqlDriver "github.com/actiontech/dtle/internal/client/driver/mysql"
	"github.com/actiontech/dtle/internal/config/mysql"

	gonats "github.com/nats-io/go-nats"
	"github.com/satori/go.uuid"

	"encoding/base64"
	"encoding/binary"
	"strings"

	"time"

	"github.com/actiontech/dtle/internal/client/driver/mysql/binlog"
	"github.com/actiontech/dtle/internal/config"
	"github.com/actiontech/dtle/internal/models"
	"github.com/actiontech/dtle/utils"
	"github.com/sirupsen/logrus"
)

const (
	TaskStateComplete int = iota
	TaskStateRestart
	TaskStateDead
)

type KafkaRunner struct {
	logger      *logrus.Entry
	subject     string
	subjectUUID uuid.UUID
	natsConn    *gonats.Conn
	waitCh      chan *models.WaitResult

	shutdown   bool
	shutdownCh chan struct{}

	kafkaConfig *KafkaConfig
	kafkaMgr    *KafkaManager

	tables map[string](map[string]*config.Table)

	gtidSet *gomysql.MysqlGTIDSet
}

func NewKafkaRunner(execCtx *common.ExecContext, cfg *KafkaConfig, logger *logrus.Logger) *KafkaRunner {
	entry := logger.WithFields(logrus.Fields{
		"job": execCtx.Subject,
	})
	return &KafkaRunner{
		subject:     execCtx.Subject,
		kafkaConfig: cfg,
		logger:      entry,
		waitCh:      make(chan *models.WaitResult, 1),
		shutdownCh:  make(chan struct{}),
		tables:      make(map[string](map[string]*config.Table)),
	}
}

func (kr *KafkaRunner) updateGtidString() {
	kr.kafkaConfig.Gtid = kr.gtidSet.String()
	kr.logger.WithField("gtid", kr.kafkaConfig.Gtid).Debugf("kafka. updateGtidString")
}

func (kr *KafkaRunner) ID() string {
	id := config.DriverCtx{
		// TODO
		DriverConfig: &config.MySQLDriverConfig{
			BinlogPos:  kr.kafkaConfig.BinlogPos,
			BinlogFile: kr.kafkaConfig.BinlogFile,
			//ReplicateDoDb:     a.mysqlContext.ReplicateDoDb,
			//ReplicateIgnoreDb: a.mysqlContext.ReplicateIgnoreDb,
			Gtid:     kr.kafkaConfig.Gtid,
			NatsAddr: kr.kafkaConfig.NatsAddr,
			//ParallelWorkers:   a.mysqlContext.ParallelWorkers,
			//ConnectionConfig:  a.mysqlContext.ConnectionConfig,
		},
	}

	data, err := json.Marshal(id)
	if err != nil {
		kr.logger.Errorf("kafka: Failed to marshal ID to JSON: %s", err)
	}
	return string(data)
}

func (kr *KafkaRunner) WaitCh() chan *models.WaitResult {
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

	kr.logger.Printf("kafka: Shutting down")
	return nil
}

func (kr *KafkaRunner) Stats() (*models.TaskStatistics, error) {
	taskResUsage := &models.TaskStatistics{}
	return taskResUsage, nil
}
func (kr *KafkaRunner) initNatSubClient() (err error) {
	natsAddr := fmt.Sprintf("nats://%s", kr.kafkaConfig.NatsAddr)
	sc, err := gonats.Connect(natsAddr)
	if err != nil {
		kr.logger.WithFields(logrus.Fields{
			"err":        err,
			"natsServer": natsAddr,
		}).Errorf("kafka: Can't connect nats server. make sure a nats streaming server is running")
		return err
	}
	kr.logger.WithFields(logrus.Fields{
		"natsServer": natsAddr,
	}).Debugf("kafka: Connect nats server %v", natsAddr)
	kr.natsConn = sc
	return nil
}
func (kr *KafkaRunner) Run() {
	kr.logger.WithFields(logrus.Fields{
		"broker": kr.kafkaConfig.Brokers,
	}).Debugf("kafka. broker: %v", kr.kafkaConfig.Brokers)

	var err error
	kr.kafkaMgr, err = NewKafkaManager(kr.kafkaConfig)
	if err != nil {
		kr.logger.WithFields(logrus.Fields{
			"err": err.Error(),
		}).Errorf("failed to initialize kafka")
		kr.onError(TaskStateDead, err)
		return
	}

	err = kr.initNatSubClient()
	if err != nil {
		kr.logger.WithFields(logrus.Fields{
			"err": err.Error(),
		}).Errorf("initNatSubClient error")
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
			kr.logger.WithFields(logrus.Fields{
				"schemaName": schemaName,
				"tableName":  tableName,
			}).Debugf("kafka: reuse table info ")
			return b, nil
		} else {
			return nil, fmt.Errorf("DTLE_BUG kafka: unknown table structure")
		}
	} else {
		kr.logger.WithFields(logrus.Fields{
			"schemaName": schemaName,
			"tableName":  tableName,
		}).Debugf("kafka: new table info")
		a[tableName] = table
		return table, nil
	}
}

func (kr *KafkaRunner) initiateStreaming() error {
	var err error

	hasFull := kr.kafkaConfig.Gtid == ""
	afterFull := make(chan struct{})

	// TODO We subscribe _full anyway to receive sendSysVarAndSqlMode.
	//  Use a better method.
	_, err = kr.natsConn.Subscribe(fmt.Sprintf("%s_full", kr.subject), func(m *gonats.Msg) {
		kr.logger.Debugf("kafka: recv a full msg")
		if err := kr.natsConn.Publish(m.Reply, nil); err != nil {
			kr.onError(TaskStateDead, err)
			return
		}
		kr.logger.Debugf("kafka: ack a full msg")

		dumpData, err := common.DecodeDumpEntry(m.Data)
		if err != nil {
			kr.onError(TaskStateDead, err)
			return
		}

		if dumpData.DbSQL != "" || len(dumpData.TbSQL) > 0 {
			kr.logger.Debugf("kafka. a sql dumpEntry")
		} else if dumpData.TableSchema == "" && dumpData.TableName == "" {
			kr.logger.Debugf("kafka.  skip apply sqlMode and SystemVariablesStatement")
			if err := kr.natsConn.Publish(m.Reply, nil); err != nil {
				kr.onError(TaskStateDead, err)
				return
			}
			return
		} else {
			var tableFromDumpData *config.Table = nil
			if len(dumpData.Table) > 0 {
				tableFromDumpData = &config.Table{}
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
	})
	if err != nil {
		return err
	}

	_, err = kr.natsConn.Subscribe(fmt.Sprintf("%s_full_complete", kr.subject), func(m *gonats.Msg) {
		kr.logger.Debugf("kafka: recv a full_complete msg")

		if err := kr.natsConn.Publish(m.Reply, nil); err != nil {
			kr.onError(TaskStateDead, err)
		}
		kr.logger.Debugf("kafka: ack a full_complete msg")

		dumpData := &mysqlDriver.DumpStatResult{}
		if err := common.Decode(m.Data, dumpData); err != nil {
			kr.onError(TaskStateDead, err)
		}

		kr.kafkaConfig.BinlogFile = dumpData.LogFile
		kr.kafkaConfig.BinlogPos = dumpData.LogPos
		kr.kafkaConfig.Gtid = dumpData.Gtid

		if hasFull {
			afterFull <- struct{}{}
		}
	})
	if err != nil {
		return err
	}

	if hasFull {
		<-afterFull
	}
	kr.logger.WithField("gtid", kr.kafkaConfig.Gtid).Infof("kafka. starting incremental replication.")

	kr.gtidSet, err = common.DtleParseMysqlGTIDSet(kr.kafkaConfig.Gtid)
	if err != nil {
		return errors.Wrap(err, "DtleParseMysqlGTIDSet")
	}

	_, err = kr.natsConn.Subscribe(fmt.Sprintf("%s_incr_hete", kr.subject), func(m *gonats.Msg) {
		kr.logger.Debugf("kafka: recv a incr_hete msg")

		if err := kr.natsConn.Publish(m.Reply, nil); err != nil {
			kr.onError(TaskStateDead, errors.Wrap(err, "Publish"))
		}
		kr.logger.Debugf("kafka. ack a incr_hete msg")

		var bigEntries binlog.BinlogEntries
		var binlogEntries binlog.BinlogEntries
		if err := common.Decode(m.Data, &binlogEntries); err != nil {
			kr.onError(TaskStateDead, err)
		}
		if binlogEntries.BigTx {
			if binlogEntries.TxNum == 1 {
				bigEntries = binlogEntries
			} else {
				bigEntries.Entries[0].Events = append(bigEntries.Entries[0].Events, binlogEntries.Entries[0].Events...)
				bigEntries.TxNum = binlogEntries.TxNum
				binlogEntries.Entries = nil
			}
			if binlogEntries.TxNum == binlogEntries.TxLen {
				binlogEntries = bigEntries
				bigEntries.Entries = nil
			}
		}

		for _, binlogEntry := range binlogEntries.Entries {
			if binlogEntries.BigTx && binlogEntries.TxNum < binlogEntries.TxLen {
				continue
			}
			err = kr.kafkaTransformDMLEventQuery(binlogEntry)
			if err != nil {
				kr.onError(TaskStateDead, errors.Wrap(err, "kafkaTransformDMLEventQuery"))
				return
			}
			kr.logger.Debugf("kafka: after kafkaTransformDMLEventQuery")
		}
	})
	if err != nil {
		return errors.Wrap(err, "Subscribe")
	}

	return nil
}

func (kr *KafkaRunner) onError(state int, err error) {
	if kr.shutdown {
		return
	}
	switch state {
	case TaskStateComplete:
		kr.logger.Printf("kafka: Done migrating")
	case TaskStateRestart:
		if kr.natsConn != nil {
			if err := kr.natsConn.Publish(fmt.Sprintf("%s_restart", kr.subject), []byte(kr.kafkaConfig.Gtid)); err != nil {
				kr.logger.WithFields(logrus.Fields{
					"err": err,
				}).Errorf("kafka: Trigger restart")
			}
		}
	default:
		if kr.natsConn != nil {
			if err := kr.natsConn.Publish(fmt.Sprintf("%s_error", kr.subject), []byte(kr.kafkaConfig.Gtid)); err != nil {
				kr.logger.WithFields(logrus.Fields{
					"err": err,
				}).Errorf("kafka: Trigger shutdown")
			}
		}
	}

	kr.waitCh <- models.NewWaitResult(state, err)
	kr.Shutdown()
}

func (kr *KafkaRunner) kafkaTransformSnapshotData(table *config.Table, value *common.DumpEntry) error {
	var err error

	tableIdent := fmt.Sprintf("%v.%v.%v", kr.kafkaMgr.Cfg.Topic, table.TableSchema, table.TableName)
	kr.logger.WithFields(logrus.Fields{
		"value": value.ValuesX,
	}).Debugf("kafka: kafkaTransformSnapshotData value")
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
		valuePayload.TsMs = utils.CurrentTimeMillis()

		valuePayload.Before = nil
		valuePayload.After = NewRow()

		columnList := table.OriginalTableColumns.ColumnList()
		valueColDef, keyColDef := kafkaColumnListToColDefs(table.OriginalTableColumns, kr.kafkaConfig.TimeZone)
		keySchema := NewKeySchema(tableIdent, keyColDef)

		for i, _ := range columnList {
			var value interface{}

			if rowValues[i] != nil {
				valueStr := string(*rowValues[i])
				switch columnList[i].Type {
				case mysql.TinyintColumnType, mysql.SmallintColumnType, mysql.MediumIntColumnType, mysql.IntColumnType:
					value, err = strconv.ParseInt(valueStr, 10, 64)
					if err != nil {
						return err
					}
				case mysql.BigIntColumnType:
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
				case mysql.DoubleColumnType:
					value, err = strconv.ParseFloat(valueStr, 64)
					if err != nil {
						return err
					}
				case mysql.FloatColumnType:
					value, err = strconv.ParseFloat(valueStr, 64)
					if err != nil {
						return err
					}
				case mysql.DecimalColumnType:
					value = DecimalValueFromStringMysql(valueStr)
				case mysql.TimeColumnType:
					value = TimeValue(valueStr)
				case mysql.TimestampColumnType:
					if valueStr != "" {
						value = TimeStamp(valueStr, kr.kafkaConfig.TimeZone)
					} else {
						value = TimeValue(valueStr)
					}
				case mysql.BinaryColumnType:
					value = base64.StdEncoding.EncodeToString([]byte(valueStr))
				case mysql.BitColumnType:
					value = base64.StdEncoding.EncodeToString([]byte(valueStr))
				case mysql.BlobColumnType:
					if columnList[i].ColumnType == "text"||columnList[i].ColumnType == "tinytext" || columnList[i].ColumnType == "mediumtext"|| columnList[i].ColumnType == "longtext" {
						value = valueStr
					} else {
						value = base64.StdEncoding.EncodeToString([]byte(valueStr))
					}
				case mysql.VarbinaryColumnType:
					value = base64.StdEncoding.EncodeToString([]byte(valueStr))
				case mysql.DateColumnType, mysql.DateTimeColumnType:
					if valueStr != "" && columnList[i].ColumnType == "datetime" {
						value = DateTimeValue(valueStr, kr.kafkaConfig.TimeZone)
					} else if valueStr != "" {
						value = DateValue(valueStr)
					}

				case mysql.YearColumnType:
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

			kr.logger.WithFields(logrus.Fields{
				"rowvalue": value,
			}).Debugf("kafka: kafkaTransformSnapshotData rowvalue")
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
			return fmt.Errorf("kafka: serialization error: %v", err)
		}
		vBs, err := json.Marshal(v)
		if err != nil {
			return fmt.Errorf("kafka: serialization error: %v", err)
		}
		//vBs = []byte(strings.Replace(string(vBs), "\"field\":\"snapshot\"", "\"default\":false,\"field\":\"snapshot\"", -1))
		err = kr.kafkaMgr.Send(tableIdent, kBs, vBs)
		if err != nil {
			return err
		}
		kr.logger.Debugf("kafka: sent one msg")
	}
	return nil
}

func (kr *KafkaRunner) kafkaTransformDMLEventQuery(dmlEvent *binlog.BinlogEntry) (err error) {
	txSid := dmlEvent.Coordinates.GetSid()

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
		colDefs, keyColDefs := kafkaColumnListToColDefs(table.OriginalTableColumns, kr.kafkaConfig.TimeZone)

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
			case mysql.DecimalColumnType:
				// nil: either entire row does not exist or this field is NULL
				if beforeValue != nil {
					beforeValue = DecimalValueFromStringMysql(beforeValue.(string))
				}
				if afterValue != nil {
					afterValue = DecimalValueFromStringMysql(afterValue.(string))
				}
			case mysql.BigIntColumnType:
				if colList[i].IsUnsigned {
					if beforeValue != nil {
						beforeValue = int64(beforeValue.(uint64))
					}
					if afterValue != nil {
						afterValue = int64(afterValue.(uint64))
					}
				}
			case mysql.TimeColumnType, mysql.TimestampColumnType:
				if beforeValue != nil && colList[i].ColumnType == "timestamp" {
					beforeValue = TimeStamp(beforeValue.(string), kr.kafkaConfig.TimeZone)
				} else if beforeValue != nil {
					beforeValue = TimeValue(beforeValue.(string))
				}
				if afterValue != nil && colList[i].ColumnType == "timestamp" {
					afterValue = TimeStamp(afterValue.(string), kr.kafkaConfig.TimeZone)
				} else if afterValue != nil {
					afterValue = TimeValue(afterValue.(string))
				}
			case mysql.DateColumnType, mysql.DateTimeColumnType:
				if beforeValue != nil && colList[i].ColumnType == "datetime" {
					beforeValue = DateTimeValue(beforeValue.(string), kr.kafkaConfig.TimeZone)
				} else if beforeValue != nil {
					beforeValue = DateValue(beforeValue.(string))
				}
				if afterValue != nil && colList[i].ColumnType == "datetime" {
					afterValue = DateTimeValue(afterValue.(string), kr.kafkaConfig.TimeZone)
				} else if afterValue != nil {
					afterValue = DateValue(afterValue.(string))
				}
			case mysql.VarbinaryColumnType:
				if beforeValue != nil {
					beforeValue = beforeValue.(string)
				}
				if afterValue != nil {
					afterValue = afterValue.(string)
				}
			case mysql.BinaryColumnType:

				if beforeValue != nil {
					beforeValue = getBinaryValue(colList[i].ColumnType, beforeValue.(string))
				}
				if afterValue != nil {
					afterValue = getBinaryValue(colList[i].ColumnType, afterValue.(string))
				}
			case mysql.TinytextColumnType:
				//println("beforeValue:",string(beforeValue.([]uint8)))
				if beforeValue != nil {
					beforeValue = string(beforeValue.([]uint8))
				}
				if afterValue != nil {
					afterValue = string(afterValue.([]uint8))
				}
			case mysql.FloatColumnType:
				if beforeValue != nil {
					beforeValue = beforeValue.(float32)
				}
				if afterValue != nil {
					afterValue = afterValue.(float32)
				}
			case mysql.EnumColumnType:
				enums := strings.Split(colList[i].ColumnType[5:len(colList[i].ColumnType)-1], ",")
				if beforeValue != nil {
					beforeValue = strings.Replace(enums[beforeValue.(int64)-1], "'", "", -1)
				}
				if afterValue != nil {
					afterValue = strings.Replace(enums[afterValue.(int64)-1], "'", "", -1)
				}
			case mysql.SetColumnType:
				columnType := colList[i].ColumnType
				if beforeValue != nil {
					beforeValue = getSetValue(beforeValue.(int64), columnType)
				}
				if afterValue != nil {
					afterValue = getSetValue(afterValue.(int64), columnType)
				}
			case mysql.BlobColumnType:
				if colList[i].ColumnType == "text" {
					if beforeValue != nil {
						beforeValue = string(beforeValue.([]byte))
					}
					if afterValue != nil {
						afterValue = string(afterValue.([]byte))
					}
				}
			case mysql.TextColumnType:
				if beforeValue != nil {
					beforeValue = string(beforeValue.([]byte))
				}
				if afterValue != nil {
					afterValue = string(afterValue.([]byte))
				}

			case mysql.BitColumnType:
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
				kr.logger.WithFields(logrus.Fields{
					"beforeValue": beforeValue,
				}).Trace("kafka. beforeValue")
				before.AddField(colName, beforeValue)
			}
			if after != nil {
				kr.logger.WithFields(logrus.Fields{
					"afterValue": afterValue,
				}).Trace("kafka. afterValue")
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
		valuePayload.Source.Gtid = fmt.Sprintf("%s:%d", txSid, dmlEvent.Coordinates.GNO)
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
		valuePayload.TsMs = utils.CurrentTimeMillis()

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

		kr.logger.Debugf("kafka: sent one msg")

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
			kr.logger.Debugf("kafka: sent one msg")
		}
	}

	kr.kafkaConfig.BinlogFile = dmlEvent.Coordinates.LogFile
	kr.kafkaConfig.BinlogPos = dmlEvent.Coordinates.LogPos

	common.UpdateGtidSet(kr.gtidSet, txSid, dmlEvent.Coordinates.SID, dmlEvent.Coordinates.GNO)
	kr.updateGtidString()

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

func kafkaColumnListToColDefs(colList *mysql.ColumnList, timeZone string) (valColDefs ColDefs, keyColDefs ColDefs) {
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
		case mysql.UnknownColumnType:
			// TODO warning
			field = NewSimpleSchemaWithDefaultField("", optional, fieldName, defaultValue)

		case mysql.BitColumnType:
			field = NewBitsField(optional, fieldName, cols[i].ColumnType[4:len(cols[i].ColumnType)-1], defaultValue)
		case mysql.BlobColumnType:
			if cols[i].ColumnType == "text"||cols[i].ColumnType == "tinytext" || cols[i].ColumnType == "mediumtext"|| cols[i].ColumnType == "longtext"  {
				field = NewSimpleSchemaWithDefaultField(SCHEMA_TYPE_STRING, optional, fieldName, defaultValue)
			} else {
				field = NewSimpleSchemaWithDefaultField(SCHEMA_TYPE_BYTES, optional, fieldName, defaultValue)
			}
		case mysql.BinaryColumnType:
			field = NewSimpleSchemaWithDefaultField(SCHEMA_TYPE_BYTES, optional, fieldName, defaultValue)
		case mysql.VarbinaryColumnType:
			field = NewSimpleSchemaWithDefaultField(SCHEMA_TYPE_BYTES, optional, fieldName, defaultValue)
		case mysql.TextColumnType:
			fallthrough
		case mysql.CharColumnType:
			fallthrough
		case mysql.VarcharColumnType:
			field = NewSimpleSchemaWithDefaultField(SCHEMA_TYPE_STRING, optional, fieldName, defaultValue)
		case mysql.EnumColumnType:
			field = NewEnumField(SCHEMA_TYPE_STRING, optional, fieldName, cols[i].ColumnType, defaultValue)
		case mysql.SetColumnType:
			field = NewSetField(SCHEMA_TYPE_STRING, optional, fieldName, cols[i].ColumnType, defaultValue)
		case mysql.TinyintColumnType:
			field = NewSimpleSchemaWithDefaultField(SCHEMA_TYPE_INT16, optional, fieldName, defaultValue)
		case mysql.TinytextColumnType:
			field = NewSimpleSchemaWithDefaultField(SCHEMA_TYPE_STRING, optional, fieldName, defaultValue)
		case mysql.SmallintColumnType:
			if cols[i].IsUnsigned {
				field = NewSimpleSchemaWithDefaultField(SCHEMA_TYPE_INT32, optional, fieldName, defaultValue)
			} else {
				field = NewSimpleSchemaWithDefaultField(SCHEMA_TYPE_INT16, optional, fieldName, defaultValue)
			}
		case mysql.MediumIntColumnType: // 24 bit in mysql
			field = NewSimpleSchemaWithDefaultField(SCHEMA_TYPE_INT32, optional, fieldName, defaultValue)
		case mysql.IntColumnType:
			if cols[i].IsUnsigned {
				field = NewSimpleSchemaWithDefaultField(SCHEMA_TYPE_INT64, optional, fieldName, defaultValue)
			} else {
				field = NewSimpleSchemaWithDefaultField(SCHEMA_TYPE_INT32, optional, fieldName, defaultValue)
			}
		case mysql.BigIntColumnType:
			field = NewSimpleSchemaWithDefaultField(SCHEMA_TYPE_INT64, optional, fieldName, defaultValue)
		case mysql.FloatColumnType:
			field = NewSimpleSchemaWithDefaultField(SCHEMA_TYPE_FLOAT64, optional, fieldName, defaultValue)
		case mysql.DoubleColumnType:
			field = NewSimpleSchemaWithDefaultField(SCHEMA_TYPE_FLOAT64, optional, fieldName, defaultValue)
		case mysql.DecimalColumnType:
			field = NewDecimalField(cols[i].Precision, cols[i].Scale, optional, fieldName, defaultValue)
		case mysql.DateColumnType:
			if cols[i].ColumnType == "datetime" {
				field = NewDateTimeField(optional, fieldName, defaultValue, timeZone)
			} else {
				field = NewDateField(SCHEMA_TYPE_INT32, optional, fieldName, defaultValue)
			}
		case mysql.YearColumnType:
			field = NewYearField(SCHEMA_TYPE_INT32, optional, fieldName, defaultValue)
		case mysql.DateTimeColumnType:
			field = NewDateTimeField(optional, fieldName, defaultValue, timeZone)
		case mysql.TimeColumnType:
			field = NewTimeField(optional, fieldName, defaultValue)
		case mysql.TimestampColumnType:
			field = NewTimeStampField(optional, fieldName, defaultValue, timeZone)
		case mysql.JSONColumnType:
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
