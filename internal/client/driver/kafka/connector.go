package kafka

import (
	"sync"
	"time"
	"fmt"
	"strings"
	"sync/atomic"
	"bytes"
	"encoding/json"
	"strconv"
	gosql "database/sql"
	"udup/internal/models"
	"udup/internal/logger"
	"udup/internal/config"
	"udup/internal/client/driver/mysql/base"
	"udup/internal/client/driver/mysql/binlog"
	"udup/internal/client/driver/mysql/sql"
	"udup/internal/client/driver/mysql"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	gomysql "github.com/siddontang/go-mysql/mysql"
	gogtid "github.com/ikarishinjieva/go-gtid"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/ast"
)

const (
	TaskStateComplete int = iota
	TaskStateRestart
	TaskStateDead
	TaskStateShutdown
)

type Connector struct {
	logger                   *logger.Entry
	subject                  string
	mysqlContext             *config.MySQLDriverConfig
	kafkaContext             *config.KafkaDriverConfig
	db                       *gosql.DB
	singletonDB              *gosql.DB
	dumpers                  []*dumper
	replicateDoDb            []*config.DataSource
	dataChannel              chan *binlog.BinlogEntry
	inspector                *mysql.Inspector
	binlogReader             *binlog.BinlogReader
	initialBinlogCoordinates *base.BinlogCoordinates
	currentBinlogCoordinates *base.BinlogCoordinates
	tableCount               int

	waitCh chan *models.WaitResult

	shutdown     bool
	shutdownCh   chan struct{}
	shutdownLock sync.Mutex

	dbServerId        int64
	initialSourceInfo *Schema
	producer          *kafka.Producer
}

func NewConnector(subject string, cfg *config.MySQLDriverConfig, kcfg *config.KafkaDriverConfig, log *logger.Logger) *Connector {
	cfg = cfg.SetDefault()
	entry := logger.NewEntry(log).WithFields(logger.Fields{
		"job": subject,
	})
	return &Connector{
		logger:       entry,
		subject:      subject,
		mysqlContext: cfg,
		kafkaContext: kcfg,
		dataChannel:  make(chan *binlog.BinlogEntry, cfg.ReplChanBufferSize),
		waitCh:       make(chan *models.WaitResult, 1),
		shutdownCh:   make(chan struct{}),
		inspector:    mysql.NewInspector(cfg, entry),
	}

}

func (e *Connector) ID() string {
	id := config.DriverCtx{
		DriverConfig: &config.MySQLDriverConfig{
			TotalTransferredBytes: e.mysqlContext.TotalTransferredBytes,
			ReplicateDoDb:         e.mysqlContext.ReplicateDoDb,
			ReplicateIgnoreDb:     e.mysqlContext.ReplicateIgnoreDb,
			Gtid:                  e.mysqlContext.Gtid,
			ConnectionConfig:      e.mysqlContext.ConnectionConfig,
		},
		KafkaConfig: &config.KafkaDriverConfig{
			Broker: e.kafkaContext.Broker,
		},
	}

	data, err := json.Marshal(id)
	if err != nil {
		e.logger.Errorf("kafka.connector: Failed to marshal ID to JSON: %s", err)
	}
	return string(data)
}

// Run executes the complete extract logic.
func (e *Connector) Run() {
	e.logger.Infof("kafka.connector: connect mysql %s.%d", e.mysqlContext.ConnectionConfig.Host, e.mysqlContext.ConnectionConfig.Port)

	if err := e.initProducer(); err != nil {
		e.onError(TaskStateDead, err)
		return
	}
	if err := e.initiateInspector(); err != nil {
		e.onError(TaskStateDead, err)
		return
	}

	if err := e.initDB(); err != nil {
		e.onError(TaskStateDead, err)
		return
	}
	if status, err := e.mysqlDump(); nil != err {
		e.onError(TaskStateDead, err)
		return
	} else if SHUTDOWN == status {
		e.onError(TaskStateShutdown, err)
		return
	}

	if e.mysqlContext.Gtid != "" {
		_, err := gomysql.ParseMysqlGTIDSet(e.mysqlContext.Gtid)
		if err != nil {
			e.onError(TaskStateDead, err)
			return
		}

		e.logger.Infof("kafka.connector: begin streaming of binary log events at: %v", e.mysqlContext.Gtid)
	} else {
		e.mysqlContext.Gtid = e.initialBinlogCoordinates.GtidSet
	}
	if err := e.initBinlogReader(&base.BinlogCoordinates{GtidSet: e.mysqlContext.Gtid}); err != nil {
		e.onError(TaskStateDead, err)
		return
	}

	if err := e.initiateStreaming(); err != nil {
		e.onError(TaskStateDead, err)
		return
	}
}

func (e *Connector) initProducer() (err error) {
	e.logger.Infof("kafka.connector: connect kafka broker: %v", e.kafkaContext.Broker)
	e.producer, err = kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": e.kafkaContext.Broker})
	if nil != err {
		return fmt.Errorf("kafka.connector: init kafka producer error: %v", err)
	}
	return nil
}

// TODO: refactor: move to base util package
func (e *Connector) initiateInspector() (err error) {
	e.inspector = mysql.NewInspector(e.mysqlContext, e.logger)
	return e.inspector.InitDBConnections()
}

func (e *Connector) inspectTables() (err error) {
	// Creates a MYSQL Dump based on the options supplied through the dumper.
	if len(e.mysqlContext.ReplicateDoDb) > 0 {
		// TODO: consider whether bailing out when database name = 'mysql' or table type = 'view'
		for _, doDb := range e.mysqlContext.ReplicateDoDb {
			if doDb.TableSchema == "" {
				continue
			}
			db := &config.DataSource{
				TableSchema: doDb.TableSchema,
			}

			if len(doDb.Tables) == 0 {
				tbs, err := sql.ShowTables(e.db, doDb.TableSchema, true)
				if err != nil {
					return err
				}
				for _, doTb := range tbs {
					doTb.TableSchema = doDb.TableSchema
					if err := e.inspector.ValidateOriginalTable(doDb.TableSchema, doTb.TableName, doTb); err != nil {
						e.logger.Warnf("kafka.connector: %v", err)
						continue
					}
					db.Tables = append(db.Tables, doTb)
				}
			} else {
				for _, doTb := range doDb.Tables {
					doTb.TableSchema = doDb.TableSchema
					if err := e.inspector.ValidateOriginalTable(doDb.TableSchema, doTb.TableName, doTb); err != nil {
						e.logger.Warnf("kafka.connector: %v", err)
						continue
					}
					db.Tables = append(db.Tables, doTb)
				}
			}
			e.replicateDoDb = append(e.replicateDoDb, db)
		}
	}

	return nil
}

func (e *Connector) applyColumns() (err error) {
	e.logger.Printf("kafka.connector: Examining table structure on extractor")
	for _, doDb := range e.replicateDoDb {
		for _, doTb := range doDb.Tables {
			if err := base.ApplyColumnTypes(e.db, doTb.TableSchema, doTb.TableName, doTb.OriginalTableColumns); err != nil {
				e.logger.Errorf("kafka.connector: unexpected error on ApplyColumnTypes, got %v", err)
				return err
			}
		}
	}
	return nil
}

func (e *Connector) initDB() (err error) {
	if e.db, err = sql.CreateDB(e.mysqlContext.ConnectionConfig.GetDBUri()); nil != err {
		return err
	}

	if err = e.db.QueryRow(`SELECT @@SERVER_ID`).Scan(&e.dbServerId); nil != err {
		return err
	}

	return nil
}

type status int

const (
	SHUTDOWN status = iota + 1
	DONE
	ERROR
)

func (e *Connector) mysqlDump() (status, error) {
	var err error
	var setSystemVariablesStatement string
	step := 0

	// open db with REPEATABLE READ
	// get system variables
	{
		e.logger.Printf("kafka.connector: Step %d: disabling autocommit and enabling repeatable read transactions", step)
		dumpUri := fmt.Sprintf("%s&tx_isolation='REPEATABLE-READ'", e.mysqlContext.ConnectionConfig.GetSingletonDBUri())
		if e.singletonDB, err = sql.CreateDB(dumpUri); err != nil {
			return ERROR, err
		}
		defer e.singletonDB.Close()

		if err := e.readMySqlCharsetSystemVariables(); err != nil {
			return ERROR, err
		}
		setSystemVariablesStatement = e.setStatementForMySqlSystemVariables()
		// TODO: consider whether to get sql mode

		if e.shutdown {
			return SHUTDOWN, nil
		}
	}

	step++
	// Obtain read lock on all tables
	{
		e.logger.Printf("kafka.connector: Step %d: flush and obtain global read lock (preventing writes to database)", step)
		query := "FLUSH TABLES WITH READ LOCK"
		_, err = e.singletonDB.Exec(query)
		if err != nil {
			e.logger.Printf("[ERR] kafka.connector: exec %+v, error: %v", query, err)
			return ERROR, err
		}
		if e.shutdown {
			return SHUTDOWN, nil
		}
	}

	step++
	var tx *gosql.Tx
	// start transaction
	{
		e.logger.Printf("kafka.connector: Step %d: start transaction with consistent snapshot", step)
		tx, err = e.singletonDB.Begin()
		if err != nil {
			return ERROR, err
		}
		query := "START TRANSACTION WITH CONSISTENT SNAPSHOT"
		_, err = tx.Exec(query)
		if err != nil {
			e.logger.Printf("[ERR] kafka.connector: exec %+v, error: %v", query, err)
			return ERROR, err
		}
	}

	defer func() {
		e.logger.Printf("kafka.connector: Step %d: committing transaction", step)
		if err := tx.Commit(); err != nil {
			e.onError(TaskStateDead, err)
		}
	}()
	if e.shutdown {
		return SHUTDOWN, nil
	}

	step++
	// read binlog position
	{
		rows, err := e.singletonDB.Query("show master status")
		if err != nil {
			return ERROR, err
		}
		e.initialBinlogCoordinates, err = base.ParseBinlogCoordinatesFromRows(rows)
		if err != nil {
			return ERROR, err
		}
		e.initialSourceInfo = e.initSourceInfo()
		if err := e.transformDDL("", setSystemVariablesStatement, e.initialSourceInfo); nil != err {
			return ERROR, err
		}
		e.logger.Printf("kafka.connector: Step %d: read binlog coordinates of MySQL master: %+v", step, *e.initialBinlogCoordinates)
		if e.shutdown {
			return SHUTDOWN, nil
		}
	}

	step++
	// read tables struct
	{
		e.logger.Printf("kafka.connector: Step %d: read list of available tables in each database", step)
		if err := e.inspectTables(); err != nil {
			return ERROR, err
		}
		if err := e.applyColumns(); err != nil {
			return ERROR, err
		}
		e.logger.Printf("kafka.connector: Step %d: read tables: %+v", step, e.replicateDoDb)
		if e.shutdown {
			return SHUTDOWN, nil
		}
	}
	if "" != e.mysqlContext.Gtid {
		return DONE, nil
	}

	step++
	// transform the current schema
	{
		e.logger.Printf("kafka.connector: Step %d: - generating DROP and CREATE statements to reflect current database schemas:%v", step, e.replicateDoDb)
		for _, db := range e.replicateDoDb {
			dbSQL := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", db.TableSchema)
			if err := e.transformDDL(db.TableSchema, dbSQL, e.initialSourceInfo); nil != err {
				return ERROR, err
			}
			for _, tb := range db.Tables {
				if tb.TableSchema != db.TableSchema {
					continue
				}

				tbSQL, err := base.ShowCreateTable(e.singletonDB, tb.TableSchema, tb.TableName, e.mysqlContext.DropTableIfExists)
				if err != nil {
					return ERROR, err
				}
				for _, sql := range tbSQL {
					if err := e.transformDDL(db.TableSchema, sql, e.initialSourceInfo); nil != err {
						return ERROR, err
					}
				}
			}
			e.tableCount += len(db.Tables)
		}
		if e.shutdown {
			return SHUTDOWN, nil
		}
	}

	step++
	// unlock global read lock
	{
		e.logger.Printf("kafka.connector: Step %d: releasing global read lock to enable MySQL writes", step)
		query := "UNLOCK TABLES"
		if _, err := tx.Exec(query); nil != err {
			e.logger.Printf("[ERR] kafka.connector: exec %+v, error: %v", query, err)
			return ERROR, err
		}
	}

	step++
	// Dump all of the tables and generate source records ...
	{
		e.logger.Printf("kafka.connector: Step %d: scanning contents of %d tables", step, e.tableCount)
		startScan := time.Now()
		counter := 0
		for _, db := range e.replicateDoDb {
			for _, t := range db.Tables {
				counter++
				e.logger.Printf("kafka.connector: Step %d: - scanning table '%s.%s' (%d of %d tables)",
					step, t.TableSchema, t.TableName, counter, e.tableCount)
				if t.Counter, err = e.CountTableRows(t); nil != err {
					return ERROR, err
				}
				d := NewDumper(tx, t, e.mysqlContext.ChunkSize, e.logger)
				if err := d.Dump(); err != nil {
					return ERROR, err
				}
				defer d.Close()
				schema := newTableDMLRecord(fmt.Sprintf("%d", e.dbServerId), t, e.initialSourceInfo)

				// Scan the rows in the table ...
				for i := 0; i < d.entriesCount; i++ {
					select {
					case entry := <-d.resultsChannel:
						if nil == entry {
							e.logger.Warn("A bug may be triggered: nil dump entry")
							continue
						}
						if entry.err != nil {
							return ERROR, entry.err
						}
						for _, v := range entry.Values {
							columnMap := make(columnValue)
							for idx, col := range entry.Columns.ColumnList() {
								columnMap[col.Name] = v[idx]
							}
							if err := e.transformSnapshotData(schema, columnMap); nil != err {
								return ERROR, err
							}
						}
						atomic.AddInt64(&e.mysqlContext.TotalRowsCopied, entry.RowsCount)
					case <-e.shutdownCh:
						return SHUTDOWN, nil
					}
				}
			}
		}
		e.logger.Printf("kafka.connector: Step %d: scanned %d rows in %d tables in %v",
			step, e.mysqlContext.TotalRowsCopied, e.tableCount, time.Since(startScan))
	}

	return DONE, nil
}

func (e *Connector) initSourceInfo() sourceInfo {
	// TODO: fill right value
	return newSourceInfoRecord(e.subject,
		e.dbServerId,
		0,
		"",
		e.initialBinlogCoordinates.LogFile,
		e.initialBinlogCoordinates.LogPos,
		0,
		true,
		"", "", "")
}

// TODO: refactor: move to base util package
func (e *Connector) readMySqlCharsetSystemVariables() error {
	query := `show variables where Variable_name IN ('character_set_server','collation_server')`
	rows, err := e.db.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	// Show an example.
	/*
		mysql> SHOW VARIABLES WHERE Variable_name IN ('character_set_server','collation_server');
		+----------------------+-----------------+
		| Variable_name        | Value           |
		+----------------------+-----------------+
		| character_set_server | utf8            |
		| collation_server     | utf8_general_ci |
		+----------------------+-----------------+
	*/
	e.mysqlContext.SystemVariables = make(map[string]string)
	for rows.Next() {
		var (
			variable string
			value    string
		)

		err = rows.Scan(&variable, &value)

		if err != nil {
			return err
		}
		e.mysqlContext.SystemVariables[variable] = value
	}

	if rows.Err() != nil {
		return rows.Err()
	}

	e.logger.Printf("kafka.connector: Reading MySQL charset-related system variables.")

	return nil
}

// Generate the DDL statements that set the charset-related system variables ...
func (e *Connector) setStatementForMySqlSystemVariables() string {
	var buffer bytes.Buffer
	first := true
	buffer.WriteString("SET ")
	for valName, value := range e.mysqlContext.SystemVariables {
		if first {
			first = false
		} else {
			buffer.WriteString(", ")
		}
		buffer.WriteString(valName + " = ")
		if strings.Contains(value, ",") || strings.Contains(value, ";") {
			value = "'" + value + "'"
		}
		buffer.WriteString(value)
	}
	return buffer.String()
}

// TODO: refactor: move to base util package
// CountTableRows counts exact number of rows on the original table
func (e *Connector) CountTableRows(table *config.Table) (int64, error) {
	atomic.StoreInt64(&e.mysqlContext.CountingRowsFlag, 1)
	defer atomic.StoreInt64(&e.mysqlContext.CountingRowsFlag, 0)
	//e.logger.Debugf("kafka.connector: As instructed, I'm issuing a SELECT COUNT(*) on the table. This may take a while")

	query := fmt.Sprintf(`select count(*) as rows from %s.%s where (%s)`,
		sql.EscapeName(table.TableSchema), sql.EscapeName(table.TableName), table.Where)
	var rowsEstimate int64
	if err := e.db.QueryRow(query).Scan(&rowsEstimate); err != nil {
		return 0, err
	}
	atomic.AddInt64(&e.mysqlContext.RowsEstimate, rowsEstimate)

	e.mysqlContext.Stage = models.StageSearchingRowsForUpdate
	e.logger.Debugf("kafka.connector: Exact number of rows(%s.%s) via COUNT: %d", table.TableSchema, table.TableName, rowsEstimate)
	return rowsEstimate, nil
}

// initBinlogReader creates and connects the reader: we hook up to a MySQL server as a replica
func (e *Connector) initBinlogReader(binlogCoordinates *base.BinlogCoordinates) error {
	binlogReader, err := binlog.NewMySQLReader(e.mysqlContext, e.logger)
	if err != nil {
		return err
	}
	if err := binlogReader.ConnectBinlogStreamer(*binlogCoordinates); err != nil {
		return err
	}
	e.binlogReader = binlogReader
	return nil
}

// initiateStreaming begins streaming of binary log events and registers listeners for such events
func (e *Connector) initiateStreaming() error {
	e.logger.Infof("kafka.connector: Beginning streaming")
	go func() {
		if err := e.binlogReader.DataStreamEvents(e.dataChannel); err != nil {
			e.logger.Errorf("kafka.connector: StreamEvents encountered unexpected error: %+v", err)
			e.onError(TaskStateDead, err)
		}
	}()

	go func() {
		for binlogEntry := range e.dataChannel {
			if nil != binlogEntry {
				if err := e.handleBinlogEvent(binlogEntry); nil != err {
					e.logger.Errorf("kafka.connector: handleBinlogEvent encountered unexpected error: %+v", err)
					e.onError(TaskStateDead, err)
					return
				}
			}
		}
	}()

	return nil
}

func (e *Connector) transformDDL(dbName, ddl string, source sourceInfo) error {
	record := newSchemaChangeRecord()
	record.GenerateKey(dbName)
	record.GenerateValue(dbName, ddl, source)
	key, value, err := record.serialization()
	if nil != err {
		return fmt.Errorf("connector.serialization error: %v", err)
	}
	topic := fmt.Sprintf("%v.%v", e.dbServerId, dbName)
	e.logger.Debugf("kafka.connector: ready to transform DDL (%v) to topic %v",
		ddl, topic)
	if err := e.pushKafkaMessage(&kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny}, Key: key, Value: value}); nil != err {
		return err
	}
	return nil
}

func (e *Connector) transformSnapshotData(table *tableDMLRecord, value columnValue) error {
	e.logger.Debugf("kafka.connector: snapshot row value: %v", value)
	after, err := tableColumnsSchemaFill(table.columnSchema, value)
	if nil != err {
		return fmt.Errorf("connector.tableColumnsSchemaFill error: %v", err)
	}
	table.GenerateKey(value)
	table.GenerateValue(nil, after, RECORD_OP_INSERT)
	topic := table.name.topicStr()
	e.logger.Debugf("kafka.connector: ready to transform snapshot data to topic %v table %v.%v key fields(%v) value fields(%v)",
		topic, table.table.TableSchema, table.table.TableName, table.Key.printFields(), table.columnSchema.printFields())
	k, v, err := table.serialization()
	if nil != err {
		return fmt.Errorf("connector.serialization error: %v", err)
	}
	if err := e.pushKafkaMessage(&kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny}, Key: k, Value: v}); nil != err {
		return err
	}
	return nil
}

func (e *Connector) handleBinlogEvent(binlogEntry *binlog.BinlogEntry) error {
	e.currentBinlogCoordinates = &binlogEntry.Coordinates

	for _, event := range binlogEntry.Events {
		switch event.DML {
		case binlog.NotDML:
			stmt, err := parser.New().ParseOneStmt(event.Query, "", "")
			if err == nil {
				switch stmt.(type) {
				case *ast.CreateDatabaseStmt:
				case *ast.DropDatabaseStmt:
				case *ast.CreateIndexStmt:
				case *ast.CreateTableStmt:
				case *ast.DropIndexStmt:
				case *ast.TruncateTableStmt:
				case *ast.AlterTableStmt:
				case *ast.CreateUserStmt:
				case *ast.GrantStmt:
				case *ast.DropTableStmt:
				default:
					e.logger.Debugf("kafka.Connector: receive a not dml (%v) in database %v, ignore", event.Query, event.DatabaseName)
					continue
				}
			}
			return fmt.Errorf("kafka.Connector: receive ddl (%v) in database %v, not yet support(%v)", event.Query, event.DatabaseName, err)

		default:
			err := e.transformDMLEventQuery(event)
			if nil != err {
				e.logger.Errorf("kafka.Connector: transform dml event error: %v", err)
				return err
			}
			binlogGtid := fmt.Sprintf("%s:%d", binlogEntry.Coordinates.SID, binlogEntry.Coordinates.GNO)
			if isExecuted, err := gogtid.GtidContain(e.mysqlContext.Gtid, binlogGtid); nil != err {
				e.logger.Errorf("kafka.Connector: gtid contain error: %v", err)
				return err
			} else if isExecuted {
				e.logger.Debugf("kafka.Connector: gtid(%v) already executed, ignore", binlogGtid)
				return nil
			}
			if e.mysqlContext.Gtid, err = gogtid.GtidAdd(e.mysqlContext.Gtid, binlogGtid); nil != err {
				e.logger.Errorf("kafka.Connector: gtid add error: %v", err)
				return err
			}
			//if err := e.updateExecutedGtid(); nil != err {
			//	e.logger.Errorf("kafka.Connector: update executed gtid error: %v", err)
			//	return err
			//}
			e.logger.Debugf("kafka.Connector: update executed gtid: %v", e.mysqlContext.Gtid)
		}
	}

	return nil
}

func (e *Connector) transformDMLEventQuery(dmlEvent binlog.DataEvent) (err error) {
	var table *config.Table
	{ // get table
		for _, db := range e.replicateDoDb {
			if dmlEvent.DatabaseName == db.TableSchema {
				for _, t := range db.Tables {
					if dmlEvent.TableName == t.TableName {
						table = t
					}
				}
			}
		}
		if nil == table {
			return fmt.Errorf("kafka.connector: can not find event table %v.%v in connector config", dmlEvent.DatabaseName, dmlEvent.TableName)
		}
	}

	// get source info
	source := newSourceInfoRecord(e.subject, e.dbServerId, 0 /*todo*/ , e.currentBinlogCoordinates.GtidSet, e.currentBinlogCoordinates.LogFile,
		e.currentBinlogCoordinates.LogPos, 0                 /*todo*/ , false, "", dmlEvent.DatabaseName, dmlEvent.TableName)

	tableSchema := newTableDMLRecord(fmt.Sprintf("%v", e.dbServerId), table, source)
	columnMap := make(columnValue)
	var before, after *Schema
	{ // generate table schema
		if nil == dmlEvent.WhereColumnValues {
			before = nil
		} else if colLen := len(dmlEvent.WhereColumnValues.GetAbstractValues()); len(table.OriginalTableColumns.ColumnList()) != colLen {
			return fmt.Errorf("[ERR] table column list(len:%v) in memory is not same as table column list in binlog(len:%v)",
				len(table.OriginalTableColumns.ColumnList()), colLen)
		} else {
			for i := 0; i < colLen; i++ {
				columnMap[table.OriginalTableColumns.ColumnList()[i].Name] = dmlEvent.WhereColumnValues.StringColumn(i)
			}

			before, err = tableColumnsSchemaFill(tableSchema.columnSchema, columnMap)
			if nil != err {
				return fmt.Errorf("[ERR] transformDMLEventQuery.tableColumnsSchemaFill.before error: %v", err)
			}
		}

		if nil == dmlEvent.NewColumnValues {
			after = nil
		} else if colLen := len(dmlEvent.NewColumnValues.GetAbstractValues()); len(table.OriginalTableColumns.ColumnList()) != colLen {
			return fmt.Errorf("[ERR] table column list(len:%v) in memory is not same as table column list in binlog(len:%v)",
				len(table.OriginalTableColumns.ColumnList()), colLen)
		} else {
			for i := 0; i < colLen; i++ {
				columnMap[table.OriginalTableColumns.ColumnList()[i].Name] = dmlEvent.NewColumnValues.StringColumn(i)
			}

			after, err = tableColumnsSchemaFill(tableSchema.columnSchema, columnMap)
			if nil != err {
				return fmt.Errorf("[ERR] transformDMLEventQuery.tableColumnsSchemaFill.after error: %v", err)
			}
		}
	}

	switch dmlEvent.DML {
	case binlog.DeleteDML:
		{
			tableSchema.GenerateKey(columnMap)
			tableSchema.GenerateValue(before, nil, RECORD_OP_DELETE)
		}
	case binlog.InsertDML:
		{
			tableSchema.GenerateKey(columnMap)
			tableSchema.GenerateValue(nil, after, RECORD_OP_INSERT)
		}
	case binlog.UpdateDML:
		{
			tableSchema.GenerateKey(columnMap)
			tableSchema.GenerateValue(before, after, RECORD_OP_UPDATE)
		}
	default:
		return fmt.Errorf("Unknown dml event type: %v ", dmlEvent.DML)
	}

	topic := tableSchema.name.topicStr()
	key, value, err := tableSchema.serialization()
	if nil != err {
		return fmt.Errorf("kafka.connector: tableSchema.serialization error: %v", err)
	}
	e.logger.Debugf("kafka.connector: ready to transform dml to topic %v table %v.%v key fields(%v) value fields(%v)",
		topic, tableSchema.table.TableSchema, tableSchema.table.TableName, tableSchema.Key.printFields(), tableSchema.columnSchema.printFields())
	if err := e.pushKafkaMessage(&kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny}, Key: key, Value: value}); nil != err {
		return err
	}
	return nil
}

// TODO: synchronization way has poor performance, fix it
func (e *Connector) pushKafkaMessage(msg *kafka.Message) (err error) {
	e.producer.ProduceChannel() <- msg
	for {
		select {
		case event := <-e.producer.Events():

			switch ev := event.(type) {
			case *kafka.Message:
				m := ev
				if m.TopicPartition.Error != nil {
					return fmt.Errorf("kafka.connector: Delivery failed: %v", m.TopicPartition.Error)
				} else {
					e.logger.Debugf("kafka.connector: Delivered message to topic %s [%d] at offset %v",
						*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
					return nil
				}
			default:
				e.logger.Debugf("kafka.connector: Ignored event: %s", ev)
			}
		case <-time.After(5 * time.Second):
			return fmt.Errorf("kafka.connector: Delivered message timeout")
		}
	}

	return nil
}

func (e *Connector) onError(state int, err error) {
	if e.shutdown {
		return
	}
	e.waitCh <- models.NewWaitResult(state, err)
	e.Shutdown()
}

func (e *Connector) WaitCh() chan *models.WaitResult {
	return e.waitCh
}

// Shutdown is used to tear down the extractor
func (e *Connector) Shutdown() error {
	e.shutdownLock.Lock()
	defer e.shutdownLock.Unlock()

	if e.shutdown {
		return nil
	}
	e.shutdown = true
	close(e.shutdownCh)

	if err := sql.CloseDB(e.singletonDB); err != nil {
		return err
	}

	if e.binlogReader != nil {
		if err := e.binlogReader.Close(); err != nil {
			return err
		}
	}

	if err := sql.CloseDB(e.db); err != nil {
		return err
	}

	if nil != e.producer {
		e.producer.Close()
	}

	close(e.dataChannel)

	e.logger.Printf("kafka.connecor: Shutting down")
	return nil
}

func (e *Connector) Stats() (*models.TaskStatistics, error) {
	totalRowsCopied := e.mysqlContext.GetTotalRowsCopied()
	rowsEstimate := atomic.LoadInt64(&e.mysqlContext.RowsEstimate)
	var progressPct float64
	if rowsEstimate == 0 {
		progressPct = 100.0
	} else {
		progressPct = 100.0 * float64(totalRowsCopied) / float64(rowsEstimate)
	}

	taskResUsage := models.TaskStatistics{
		ProgressPct: strconv.FormatFloat(progressPct, 'f', 1, 64),
		Backlog:     fmt.Sprintf("%d/%d", len(e.dataChannel), cap(e.dataChannel)),
		Timestamp:   time.Now().UTC().UnixNano(),
	}

	currentBinlogCoordinates := &base.BinlogCoordinates{}
	if e.binlogReader != nil {
		currentBinlogCoordinates = e.binlogReader.GetCurrentBinlogCoordinates()
		taskResUsage.CurrentCoordinates = &models.CurrentCoordinates{
			File:     currentBinlogCoordinates.LogFile,
			Position: currentBinlogCoordinates.LogPos,
			GtidSet:  fmt.Sprintf("%s:%d", currentBinlogCoordinates.SID, currentBinlogCoordinates.GNO),
		}
	} else {
		taskResUsage.CurrentCoordinates = &models.CurrentCoordinates{
			File:     "",
			Position: 0,
			GtidSet:  "",
		}
	}

	return &taskResUsage, nil
}
