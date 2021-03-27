package kafka

/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/actiontech/dtle/drivers/mysql/common"
	"github.com/actiontech/dtle/drivers/mysql/mysql"
	"github.com/actiontech/dtle/g"
	"github.com/hashicorp/nomad/plugins/drivers"
	"github.com/pkg/errors"
	gomysql "github.com/siddontang/go-mysql/mysql"

	"github.com/actiontech/dtle/drivers/mysql/mysql/mysqlconfig"
	hclog "github.com/hashicorp/go-hclog"
	gonats "github.com/nats-io/go-nats"
	"github.com/pingcap/tidb/types"
	uuid "github.com/satori/go.uuid"
)

const (
	TaskStateComplete int = iota
	TaskStateRestart
	TaskStateDead
)

type KafkaTableItem struct {
	table *common.Table
	keySchema *SchemaJson
	valueSchema *SchemaJson
}

type KafkaRunner struct {
	logger      hclog.Logger
	subject     string
	subjectUUID uuid.UUID
	natsConn    *gonats.Conn
	waitCh      chan *drivers.ExitResult

	shutdown   bool
	shutdownCh chan struct{}

	kafkaConfig *common.KafkaConfig
	kafkaMgr    *KafkaManager
	natsAddr    string

	storeManager *common.StoreManager

	tables map[string](map[string]*KafkaTableItem)

	gtidSet    *gomysql.MysqlGTIDSet
	Gtid       string // TODO remove?
	BinlogFile string
	BinlogPos  int64

	chBinlogEntries chan *common.BinlogEntries
	chDumpEntry     chan *common.DumpEntry

	lastSavedGtid string

	// _full_complete must be ack-ed after all full entries has been executed
	// (not just received). Since the applier ack _full before execution, the extractor
	// might send _full_complete before the entry has been executed.
	fullWg sync.WaitGroup

	timestampCtx *mysql.TimestampContext
	memory1      *int64
	memory2      *int64

	printTps       bool
	txLastNSeconds uint32
	appliedTxCount uint32
}

func NewKafkaRunner(execCtx *common.ExecContext, cfg *common.KafkaConfig, logger hclog.Logger,
	storeManager *common.StoreManager, natsAddr string, waitCh chan *drivers.ExitResult) *KafkaRunner {

	kr := &KafkaRunner{
		subject:      execCtx.Subject,
		kafkaConfig:  cfg,
		logger:       logger.Named("kafka").With("job", execCtx.Subject),
		natsAddr:     natsAddr,
		waitCh:       waitCh,
		shutdownCh:   make(chan struct{}),
		tables:       make(map[string](map[string]*KafkaTableItem)),
		storeManager: storeManager,

		chDumpEntry:     make(chan *common.DumpEntry, 2),
		chBinlogEntries: make(chan *common.BinlogEntries, 2),

		memory1:  new(int64),
		memory2:  new(int64),
		printTps: os.Getenv(g.ENV_PRINT_TPS) != "",
	}
	kr.timestampCtx = mysql.NewTimestampContext(kr.shutdownCh, kr.logger, func() bool {
		return len(kr.chBinlogEntries) == 0
	})
	return kr
}

func (kr *KafkaRunner) updateGtidLoop() {
	updateGtidInterval := 15 * time.Second
	for !kr.shutdown {
		time.Sleep(updateGtidInterval)
		if kr.Gtid != kr.lastSavedGtid {
			// TODO thread safety.
			kr.logger.Debug("SaveGtidForJob", "job", kr.subject, "gtid", kr.Gtid)
			err := kr.storeManager.SaveGtidForJob(kr.subject, kr.Gtid)
			if err != nil {
				kr.onError(TaskStateDead, errors.Wrap(err, "SaveGtidForJob"))
				return
			}
			kr.lastSavedGtid = kr.Gtid

			err = kr.storeManager.SaveBinlogFilePosForJob(kr.subject,
				kr.BinlogFile, int(kr.BinlogPos))
			if err != nil {
				kr.onError(TaskStateDead, errors.Wrap(err, "SaveBinlogFilePosForJob"))
				return
			}
		}
	}
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
	taskResUsage := &common.TaskStatistics{
		CurrentCoordinates: &common.CurrentCoordinates{
			File:               kr.BinlogFile,
			Position:           kr.BinlogPos,
			GtidSet:            kr.Gtid,
			RelayMasterLogFile: "",
			ReadMasterLogPos:   0,
			RetrievedGtidSet:   "",
		},
		TableStats: nil,
		DelayCount: &common.DelayCount{
			Num:  0,
			Time: kr.timestampCtx.GetDelay(),
		},
		ProgressPct:        "",
		ExecMasterRowCount: 0,
		ExecMasterTxCount:  0,
		ReadMasterRowCount: 0,
		ReadMasterTxCount:  0,
		ETA:                "",
		Backlog:            "",
		ThroughputStat:     nil,
		MsgStat:            gonats.Statistics{},
		BufferStat:         common.BufferStat{},
		Stage:              "",
		Timestamp:          time.Now().Unix(),
		MemoryStat: common.MemoryStat{
			Full: *kr.memory1,
			Incr: *kr.memory2,
		},
		HandledTxCount: common.TxCount{
			AppliedTxCount: &kr.appliedTxCount,
		},
	}
	return taskResUsage, nil
}
func (kr *KafkaRunner) initNatSubClient() (err error) {
	sc, err := gonats.Connect(kr.natsAddr)
	if err != nil {
		kr.logger.Error("Can't connect nats server.", "err", err, "natsAddr", kr.natsAddr)
		return err
	}
	kr.logger.Debug("kafka: Connect nats server", "natsAddr", kr.natsAddr)

	kr.natsConn = sc
	return nil
}
func (kr *KafkaRunner) Run() {
	kr.logger.Debug("Run", "brokers", kr.kafkaConfig.Brokers)
	var err error

	{
		gtid, err := kr.storeManager.GetGtidForJob(kr.subject)
		if err != nil {
			kr.onError(TaskStateDead, errors.Wrap(err, "GetGtidForJob"))
			return
		}
		if gtid != "" {
			kr.logger.Info("Got gtid from consul", "gtid", gtid)
			kr.gtidSet, err = common.DtleParseMysqlGTIDSet(gtid)
			if err != nil {
				kr.onError(TaskStateDead, errors.Wrap(err, "DtleParseMysqlGTIDSet"))
				return
			}
			kr.Gtid = gtid
		}

		pos, err := kr.storeManager.GetBinlogFilePosForJob(kr.subject)
		if err != nil {
			kr.onError(TaskStateDead, errors.Wrap(err, "GetBinlogFilePosForJob"))
			return
		}
		if pos.Name != "" {
			kr.BinlogFile = pos.Name
			kr.BinlogPos = int64(pos.Pos)
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

	go kr.timestampCtx.Handle()

	kr.logger.Info("PutAndWatchNats")
	kr.storeManager.PutAndWatchNats(kr.subject, kr.natsAddr, kr.shutdownCh, func(err error) {
		kr.onError(TaskStateDead, errors.Wrap(err, "PutAndWatchNats"))
	})

	err = kr.initiateStreaming()
	if err != nil {
		kr.onError(TaskStateDead, err)
		return
	}

	go kr.updateGtidLoop()
}

func (kr *KafkaRunner) getOrSetTable(schemaName string, tableName string,
	table *common.Table) (item *KafkaTableItem, err error) {
	a, ok := kr.tables[schemaName]
	if !ok {
		a = make(map[string]*KafkaTableItem)
		kr.tables[schemaName] = a
	}

	if table == nil {
		b, ok := a[tableName]
		if ok {
			kr.logger.Debug("reuse table info", "schemaName", schemaName, "tableName", tableName)
			return b, nil
		} else {
			// e.g. `drop table if exists` does not have a table structure. It will be ignored.
			kr.logger.Debug("nil table info", "schemaName", schemaName, "tableName", tableName)
			return nil, nil
		}
	} else {
		kr.logger.Debug("new table info", "schemaName", schemaName, "tableName", tableName)
		tableIdent := fmt.Sprintf("%v.%v.%v", kr.kafkaMgr.Cfg.Topic, table.TableSchema, table.TableName)
		colDefs, keyColDefs := kafkaColumnListToColDefs(table.OriginalTableColumns, kr.kafkaConfig.TimeZone)
		keySchema := &SchemaJson{
			schema: NewKeySchema(tableIdent, keyColDefs),
		}
		valueSchema := &SchemaJson{
			schema: NewEnvelopeSchema(tableIdent, colDefs),
		}

		keySchema.cache, err = json.Marshal(keySchema)
		if err != nil {
			return nil, err
		}
		valueSchema.cache, err = json.Marshal(valueSchema)
		if err != nil {
			return nil, err
		}

		item = &KafkaTableItem{
			table:       table,
			keySchema:   keySchema,
			valueSchema: valueSchema,
		}
		a[tableName] = item
		return item, nil
	}
}

func decodeMaybeTable(tableBs []byte) (*common.Table, error) {
	if len(tableBs) > 0 {
		var r *common.Table
		r = &common.Table{}
		err := common.GobDecode(tableBs, r)
		if err != nil {
			return nil, errors.Wrap(err, "GobDecode")
		}
		return r, nil
	} else {
		return nil, nil
	}
}
func (kr *KafkaRunner) handleFullCopy() {
	for !kr.shutdown {
		var dumpData *common.DumpEntry
		select {
		case <-kr.shutdownCh:
			return
		case dumpData = <-kr.chDumpEntry:
		}

		if dumpData.DbSQL != "" || len(dumpData.TbSQL) > 0 {
			kr.logger.Debug("a sql dumpEntry")
		} else if dumpData.TableSchema == "" && dumpData.TableName == "" {
			kr.logger.Debug("skip apply sqlMode and SystemVariablesStatement")
		} else {
			tableFromDumpData, err := decodeMaybeTable(dumpData.Table)
			if err != nil {
				kr.onError(TaskStateDead, errors.Wrap(err, "decodeMaybeTable"))
				return
			}
			tableItem, err := kr.getOrSetTable(dumpData.TableSchema, dumpData.TableName, tableFromDumpData)
			if err != nil {
				kr.onError(TaskStateDead, err)
				return
			}

			if tableItem == nil {
				err := fmt.Errorf("DTLE_BUG: kafkaTransformSnapshotData: tableItem is nil %v.%v TotalCount %v",
					dumpData.TableSchema, dumpData.TableName, dumpData.TotalCount)
				kr.logger.Error(err.Error())
				kr.onError(TaskStateDead, err)
				return
			}

			err = kr.kafkaTransformSnapshotData(tableItem, dumpData)
			if err != nil {
				kr.onError(TaskStateDead, err)
				return
			}
			atomic.AddInt64(kr.memory1, -int64(dumpData.Size()))
		}
		kr.fullWg.Done()
	}
}
func (kr *KafkaRunner) handleIncr() {
	kr.logger.Debug("handleIncr")
	var err error
	groupTimeoutDuration := time.Duration(kr.kafkaConfig.MessageGroupTimeout) * time.Millisecond
	var entriesSize uint64
	entriesWillBeSent := []*common.BinlogEntry{}

	sendEntriesAndClear := func() error {
		err = kr.kafkaTransformDMLEventQueries(entriesWillBeSent)
		if err != nil {
			return errors.Wrap(err, "kafkaTransformDMLEventQueries")
		}

		if kr.printTps {
			atomic.AddUint32(&kr.txLastNSeconds, uint32(len(entriesWillBeSent)))
		}
		atomic.AddUint32(&kr.appliedTxCount, 1)
		entriesSize = 0
		entriesWillBeSent = []*common.BinlogEntry{}

		return nil
	}
	if kr.printTps {
		go func() {
			for {
				select {
				case <-kr.shutdownCh:
					return
				default:
					// keep loop
				}
				time.Sleep(5 * time.Second)
				n := atomic.SwapUint32(&kr.txLastNSeconds, 0)
				kr.logger.Info("txLastNSeconds", "n", n)
			}
		}()
	}

	timer := time.NewTimer(groupTimeoutDuration)
	defer timer.Stop()
	for !kr.shutdown {
		var binlogEntries *common.BinlogEntries
		select {
		case <-kr.shutdownCh:
			return
		case binlogEntries = <-kr.chBinlogEntries:
		case <-timer.C:
			if len(entriesWillBeSent) > 0 {
				kr.logger.Debug("kafka: incr. send by timeout.",
					"timeout", kr.kafkaConfig.MessageGroupTimeout,
					"entriesSize", entriesSize,
					"Entries.len", len(entriesWillBeSent))
				if err := sendEntriesAndClear(); nil != err {
					kr.onError(TaskStateDead, err)
				}
			}

			timer.Reset(groupTimeoutDuration)
			continue
		}
		memSize := int64(binlogEntries.Size())

		for _, binlogEntry := range binlogEntries.Entries {
			entriesWillBeSent = append(entriesWillBeSent, binlogEntry)
			entriesSize = entriesSize + binlogEntry.Size()
			if entriesSize >= kr.kafkaConfig.MessageGroupMaxSize {
				kr.logger.Debug("kafka: incr. send by GroupLimit",
					"MessageGroupMaxSize", kr.kafkaConfig.MessageGroupMaxSize,
					"entriesSize", entriesSize,
					"Entries.len", len(entriesWillBeSent))
				if err := sendEntriesAndClear(); nil != err {
					kr.onError(TaskStateDead, err)
					return
				}
			}
		}
		atomic.AddInt64(kr.memory2, -memSize)
	}
}
func (kr *KafkaRunner) initiateStreaming() error {
	var err error

	go kr.handleFullCopy()
	go kr.handleIncr()

	_, err = kr.natsConn.Subscribe(fmt.Sprintf("%s_full", kr.subject), func(m *gonats.Msg) {
		kr.logger.Debug("recv a full msg")

		kr.fullWg.Add(1)
		dumpData := &common.DumpEntry{}
		err = common.Decode(m.Data, dumpData)
		if err != nil {
			kr.onError(TaskStateDead, err)
			return
		}

		t := time.NewTimer(common.DefaultConnectWait / 2)
		select {
		case kr.chDumpEntry <- dumpData:
			atomic.AddInt64(kr.memory1, int64(dumpData.Size()))
			if err := kr.natsConn.Publish(m.Reply, nil); err != nil {
				kr.onError(TaskStateDead, err)
				return
			}
			kr.logger.Debug("ack a full msg")
		case <-t.C:
			kr.fullWg.Done()
			kr.logger.Debug("discard a full msg")
		}
		t.Stop()
	})
	if err != nil {
		return err
	}

	_, err = kr.natsConn.Subscribe(fmt.Sprintf("%s_full_complete", kr.subject), func(m *gonats.Msg) {
		kr.logger.Debug("recv a full_complete msg")

		dumpData := &common.DumpStatResult{}
		if err := common.Decode(m.Data, dumpData); err != nil {
			kr.onError(TaskStateDead, err)
			return
		}

		kr.fullWg.Wait()

		kr.gtidSet, err = common.DtleParseMysqlGTIDSet(dumpData.Gtid)
		if err != nil {
			kr.onError(TaskStateDead, errors.Wrap(err, "DtleParseMysqlGTIDSet"))
			return
		}
		kr.Gtid = dumpData.Gtid
		kr.BinlogFile = dumpData.LogFile
		kr.BinlogPos = dumpData.LogPos

		if err := kr.natsConn.Publish(m.Reply, nil); err != nil {
			kr.onError(TaskStateDead, err)
			return
		}
		kr.logger.Debug("ack a full_complete msg")
	})
	if err != nil {
		return err
	}

	_, err = kr.natsConn.Subscribe(fmt.Sprintf("%s_incr_hete", kr.subject), func(m *gonats.Msg) {
		kr.logger.Debug("recv an incr_hete msg")

		var binlogEntries common.BinlogEntries
		if err := common.Decode(m.Data, &binlogEntries); err != nil {
			kr.onError(TaskStateDead, err)
			return
		}
		t := time.NewTimer(common.DefaultConnectWait / 2)
		select {
		case kr.chBinlogEntries <-&binlogEntries:
			if err := kr.natsConn.Publish(m.Reply, nil); err != nil {
				kr.onError(TaskStateDead, errors.Wrap(err, "Publish"))
				return
			}
			kr.logger.Debug("ack an incr_hete msg")
			atomic.AddInt64(kr.memory2, int64(binlogEntries.Size()))
		case <-t.C:
			kr.logger.Debug("discard an incr_hete msg")
			//kr.natsConn.Publish(m.Reply, "wait")
		}
		t.Stop()
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

	bs := []byte(err.Error())

	switch state {
	case TaskStateComplete:
		kr.logger.Info("Done migrating")
	case TaskStateRestart:
		if kr.natsConn != nil {
			if err := kr.natsConn.Publish(fmt.Sprintf("%s_restart", kr.subject), bs); err != nil {
				kr.logger.Error("Trigger restart", "err", err)
			}
		}
	default:
		if kr.natsConn != nil {
			if err := kr.natsConn.Publish(fmt.Sprintf("%s_error", kr.subject), bs); err != nil {
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

func (kr *KafkaRunner) kafkaTransformSnapshotData(
	tableItem *KafkaTableItem, value *common.DumpEntry) error {

	var err error
	table := tableItem.table

	keysBs, valuesBs := make([][]byte, 0), make([][]byte, 0)
	realTopics := []string{}

	var realTopic string
	if kr.kafkaConfig.TopicWithSchemaTable {
		realTopic = fmt.Sprintf("%v.%v.%v", kr.kafkaMgr.Cfg.Topic, table.TableSchema, table.TableName)
	} else {
		realTopic = kr.kafkaMgr.Cfg.Topic
	}
	kr.logger.Debug("kafkaTransformSnapshotData", "value", value.ValuesX)
	for iValuesX, rowValues := range value.ValuesX {
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
					value = TimeValue(valueStr)
				case mysqlconfig.TimestampColumnType:
					if valueStr != "" {
						value = TimeStamp(valueStr, kr.kafkaConfig.TimeZone)
					} else {
						value = TimeValue(valueStr)
					}
				case mysqlconfig.BinaryColumnType:
					value = base64.StdEncoding.EncodeToString([]byte(valueStr))
				case mysqlconfig.BitColumnType:
					if columnList[i].ColumnType=="bit(1)"{
						value,_ = strconv.ParseBool( base64.StdEncoding.EncodeToString([]byte(valueStr)))
					}else{
						value = base64.StdEncoding.EncodeToString([]byte(valueStr))
					}
				case mysqlconfig.BlobColumnType:
					if strings.Contains(columnList[i].ColumnType, "text") {
						value = valueStr
					} else {
						value = base64.StdEncoding.EncodeToString([]byte(valueStr))
					}
				case mysqlconfig.VarbinaryColumnType:
					value = base64.StdEncoding.EncodeToString([]byte(valueStr))
				case mysqlconfig.CharColumnType:
					if valueStr == "" {
						valueStr = "char(255)"
					}
					value = base64.StdEncoding.EncodeToString([]byte(valueStr))
				case mysqlconfig.DateColumnType, mysqlconfig.DateTimeColumnType:
					if valueStr != "" && columnList[i].ColumnType == "datetime" {
						value = DateTimeValue(valueStr, kr.kafkaConfig.TimeZone)
					} else if valueStr != "" {
						value = DateValue(valueStr)
					}

				case mysqlconfig.YearColumnType:
					if valueStr != "" {
						value = YearValue(valueStr)
					} else {
						value = valueStr
					}
				case mysqlconfig.VarcharColumnType:
					if strings.Contains(columnList[i].ColumnType, "binary") {
						value = base64.StdEncoding.EncodeToString([]byte(valueStr))
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

		k := DbzOutput{
			Schema:  tableItem.keySchema,
			Payload: keyPayload,
		}
		v := DbzOutput{
			Schema:  tableItem.valueSchema,
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

		keysBs = append(keysBs, kBs)
		valuesBs = append(valuesBs, vBs)
		realTopics = append(realTopics, realTopic)

		isLastPart := iValuesX == len(value.ValuesX) - 1
		if len(keysBs) > 100 || isLastPart {
			err := kr.kafkaMgr.SendMessages(kr.logger, realTopics, keysBs, valuesBs)
			if err != nil {
				return fmt.Errorf("send msgs failed: %v", err)
			}
			kr.logger.Debug("sent a group of msgs")
			keysBs = nil
			valuesBs = nil
			realTopics = nil
		}
	}

	return nil
}

func (kr *KafkaRunner) kafkaTransformDMLEventQueries(dmlEntries []*common.BinlogEntry) (err error) {
	if len(dmlEntries) <= 0 {
		return nil
	}

	latestTimestamp := uint32(0)
	keysBs, valuesBs := make([][]byte, 0), make([][]byte, 0)
	realTopics := []string{}

	for _, dmlEvent := range dmlEntries {
		kr.logger.Debug("kafkaTransformDMLEventQueries", "gno", dmlEvent.Coordinates.GNO)
		txSid := dmlEvent.Coordinates.GetSid()

		for i, _ := range dmlEvent.Events {
			dataEvent := &dmlEvent.Events[i]
			realSchema := common.StringElse(dataEvent.DatabaseName, dataEvent.CurrentSchema)

			var tableItem *KafkaTableItem
			if dataEvent.TableName != "" {
				// this must be executed before skipping DDL
				table, err := decodeMaybeTable(dataEvent.Table)
				tableItem, err = kr.getOrSetTable(realSchema, dataEvent.TableName, table)
				if err != nil {
					return err
				}
			} else {
				kr.logger.Debug("kafkaTransformDMLEventQueries: empty table name",
					"query", dataEvent.Query, "type", dataEvent.DML)
			}

			// skipping DDL
			if dataEvent.DML == common.NotDML {
				continue
			}

			if tableItem == nil {
				err = fmt.Errorf("DTLE_BUG: table meta is nil %v.%v gno %v", realSchema, dataEvent.TableName, dmlEvent.Coordinates.GNO)
				kr.logger.Error("table meta is nil", "err", err)
				return err
			}

			table := tableItem.table

			var op string
			var before *Row
			var after *Row

			switch dataEvent.DML {
			case common.InsertDML:
				op = RECORD_OP_INSERT
				before = nil
				after = NewRow()
			case common.DeleteDML:
				op = RECORD_OP_DELETE
				before = NewRow()
				after = nil
			case common.UpdateDML:
				op = RECORD_OP_UPDATE
				before = NewRow()
				after = NewRow()
			}

			keyPayload := NewRow()
			colList := table.OriginalTableColumns.ColumnList()

			for i, _ := range colList {
				colName := colList[i].RawName

				var beforeValue interface{}
				var afterValue interface{}

				if before != nil {
					beforeValue = colList[i].ConvertArg(dataEvent.WhereColumnValues.AbstractValues[i])
				}
				if after != nil {
					afterValue = colList[i].ConvertArg(dataEvent.NewColumnValues.AbstractValues[i])
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
						beforeValue = TimeStamp(beforeValue.(string), kr.kafkaConfig.TimeZone)
					} else if beforeValue != nil {
						beforeValue = TimeValue(beforeValue.(string))
					}
					if afterValue != nil && colList[i].ColumnType == "timestamp" {
						afterValue = TimeStamp(afterValue.(string), kr.kafkaConfig.TimeZone)
					} else if afterValue != nil {
						afterValue = TimeValue(afterValue.(string))
					}
				case mysqlconfig.DateColumnType, mysqlconfig.DateTimeColumnType:
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
				case mysqlconfig.BlobColumnType:
					if strings.Contains(colList[i].ColumnType, "text") {
						if beforeValue != nil {
							beforeValue = string(afterValue.([]byte))
						}
						if afterValue != nil {
							afterValue = string(afterValue.([]byte))
						}
					}
				case mysqlconfig.TextColumnType:
					if beforeValue != nil {
						beforeValue = string(afterValue.([]byte))
					}
					if afterValue != nil {
						afterValue = string(afterValue.([]byte))
					}

				case mysqlconfig.BitColumnType:
					if colList[i].ColumnType == "bit(1)" {
						if beforeValue != nil {
							beforeValue, _ = strconv.ParseBool(getBitValue(colList[i].ColumnType, beforeValue.(int64)))
						}
						if afterValue != nil {
							afterValue, _ = strconv.ParseBool(getBitValue(colList[i].ColumnType, afterValue.(int64)))
						}
					} else {
						if beforeValue != nil {
							beforeValue = getBitValue(colList[i].ColumnType, beforeValue.(int64))
						}
						if afterValue != nil {
							afterValue = getBitValue(colList[i].ColumnType, afterValue.(int64))
						}
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
					kr.logger.Trace("beforeValue", "v", beforeValue)
					before.AddField(colName, beforeValue)
				}
				if after != nil {
					kr.logger.Trace("afterValue", "v", afterValue)
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
			valuePayload.TsMs = common.CurrentTimeMillis()

			k := DbzOutput{
				Schema:  tableItem.keySchema,
				Payload: keyPayload,
			}
			v := DbzOutput{
				Schema:  tableItem.valueSchema,
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

			var realTopic string
			if kr.kafkaConfig.TopicWithSchemaTable {
				realTopic = fmt.Sprintf("%v.%v.%v", kr.kafkaMgr.Cfg.Topic, table.TableSchema, table.TableName)
			} else {
				realTopic = kr.kafkaMgr.Cfg.Topic
			}
			realTopics = append(realTopics, realTopic)
			keysBs = append(keysBs, kBs)
			valuesBs = append(valuesBs, vBs)
			kr.logger.Debug("appended an event", "schema", table.TableSchema, "table", table.TableName,
				"gno", dmlEvent.Coordinates.GNO)

			// tombstone event for DELETE
			if dataEvent.DML == common.DeleteDML {
				v2 := DbzOutput{
					Schema:  nil,
					Payload: nil,
				}
				v2Bs, err := json.Marshal(v2)
				if err != nil {
					return err
				}

				realTopics = append(realTopics, realTopic)
				keysBs = append(keysBs, kBs)
				valuesBs = append(valuesBs, v2Bs)
			}
			latestTimestamp = dataEvent.Timestamp
		}
	}

	err = kr.kafkaMgr.SendMessages(kr.logger, realTopics, keysBs, valuesBs)
	if err != nil {
		return fmt.Errorf("send msgs failed: %v", err)
	}
	kr.logger.Debug("sent msgs")

	if latestTimestamp != 0 {
		kr.timestampCtx.TimestampCh <- latestTimestamp
	}

	for _, entry := range dmlEntries {
		kr.BinlogFile = entry.Coordinates.LogFile
		kr.BinlogPos = entry.Coordinates.LogPos

		common.UpdateGtidSet(kr.gtidSet, entry.Coordinates.SID, entry.Coordinates.GNO)
	}
	kr.Gtid = kr.gtidSet.String()
	kr.logger.Debug("kafka. updateGtidString", "gtid", kr.Gtid)

	kr.logger.Debug("kafka: after kafkaTransformDMLEventQueries")
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

func kafkaColumnListToColDefs(colList *common.ColumnList, timeZone string) (valColDefs ColDefs, keyColDefs ColDefs) {
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
			if cols[i].ColumnType=="bit(1)"{
				value,_ := strconv.ParseBool( defaultValue.(types.BinaryLiteral).ToString())
				field = NewSimpleSchemaWithDefaultField(SCHEMA_TYPE_BOOLEAN, optional, fieldName, value)
			}else{
				field = NewBitsField(optional, fieldName, cols[i].ColumnType[4:len(cols[i].ColumnType)-1], defaultValue)
			}
		case mysqlconfig.BlobColumnType:
			if strings.Contains(cols[i].ColumnType, "text") {
				field = NewSimpleSchemaWithDefaultField(SCHEMA_TYPE_STRING, optional, fieldName, defaultValue)
			} else {
				field = NewSimpleSchemaWithDefaultField(SCHEMA_TYPE_BYTES, optional, fieldName, defaultValue)
			}
		case mysqlconfig.BinaryColumnType:
			field = NewSimpleSchemaWithDefaultField(SCHEMA_TYPE_BYTES, optional, fieldName, defaultValue)
		case mysqlconfig.VarbinaryColumnType:
			field = NewSimpleSchemaWithDefaultField(SCHEMA_TYPE_BYTES, optional, fieldName, defaultValue)
		case mysqlconfig.TextColumnType:
			fallthrough
		case mysqlconfig.CharColumnType:
			fallthrough
		case mysqlconfig.VarcharColumnType:
			if strings.Contains(cols[i].ColumnType, "binary") {
				field = NewSimpleSchemaWithDefaultField(SCHEMA_TYPE_BYTES, optional, fieldName, defaultValue)
			} else {
				field = NewSimpleSchemaWithDefaultField(SCHEMA_TYPE_STRING, optional, fieldName, defaultValue)
			}
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
				field = NewDateTimeField(optional, fieldName, defaultValue, timeZone)
			} else {
				field = NewDateField(SCHEMA_TYPE_INT32, optional, fieldName, defaultValue)
			}
		case mysqlconfig.YearColumnType:
			field = NewYearField(SCHEMA_TYPE_INT32, optional, fieldName, defaultValue)
		case mysqlconfig.DateTimeColumnType:
			field = NewDateTimeField(optional, fieldName, defaultValue, timeZone)
		case mysqlconfig.TimeColumnType:
			field = NewTimeField(optional, fieldName, defaultValue)
		case mysqlconfig.TimestampColumnType:
			field = NewTimeStampField(optional, fieldName, defaultValue, timeZone)
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
