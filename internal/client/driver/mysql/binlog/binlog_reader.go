package binlog

import (
	"bytes"
	gosql "database/sql"
	//"encoding/hex"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	//"os"

	"github.com/issuj/gofaster/base64"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/parser"
	"github.com/satori/go.uuid"
	gomysql "github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"golang.org/x/net/context"

	"udup/internal/client/driver/mysql/base"
	"udup/internal/client/driver/mysql/sql"
	"udup/internal/client/driver/mysql/util"
	"udup/internal/config"
	"udup/internal/config/mysql"
	log "udup/internal/logger"
	"udup/internal/models"
	"udup/utils"
)

// BinlogReader is a general interface whose implementations can choose their methods of reading
// a binary log file and parsing it into binlog entries
type BinlogReader struct {
	logger                   *log.Entry
	connectionConfig         *mysql.ConnectionConfig
	db                       *gosql.DB
	binlogSyncer             *replication.BinlogSyncer
	binlogStreamer           *replication.BinlogStreamer
	currentCoordinates       base.BinlogCoordinates
	currentCoordinatesMutex  *sync.Mutex
	LastAppliedRowsEventHint base.BinlogCoordinates
	// raw config, whose ReplicateDoDB is same as config file (empty-is-all & no dynamically created tables)
	mysqlContext             *config.MySQLDriverConfig
	// dynamic config, include all tables (implicitly assigned or dynamically created)
	tables                   map[string](map[string]*config.TableContext)

	currentTx          *BinlogTx
	currentBinlogEntry *BinlogEntry
	txCount            int
	currentFde         string
	currentQuery       *bytes.Buffer
	currentSqlB64      *bytes.Buffer
	appendB64SqlBs     []byte
	ReMap              map[string]*regexp.Regexp

	wg           sync.WaitGroup
	shutdown     bool
	shutdownCh   chan struct{}
	shutdownLock sync.Mutex
}

func NewMySQLReader(cfg *config.MySQLDriverConfig, logger *log.Entry, replicateDoDb []*config.DataSource) (binlogReader *BinlogReader, err error) {
	binlogReader = &BinlogReader{
		logger:                  logger,
		currentCoordinates:      base.BinlogCoordinates{},
		currentCoordinatesMutex: &sync.Mutex{},
		mysqlContext:            cfg,
		appendB64SqlBs:          make([]byte, 1024*1024),
		ReMap:                   make(map[string]*regexp.Regexp),
		shutdownCh:              make(chan struct{}),
		tables:                  make(map[string](map[string]*config.TableContext)),
	}

	for _, db := range replicateDoDb {
		tableMap := binlogReader.getDbTableMap(db.TableSchema)
		for _, table := range db.Tables {
			if err := binlogReader.addTableToTableMap(tableMap, table); err != nil {
				return nil, err
			}
		}
	}

	uri := cfg.ConnectionConfig.GetDBUri()
	if binlogReader.db, err = sql.CreateDB(uri); err != nil {
		return nil, err
	}

	id, err := util.NewIdWorker(2, 3, util.SnsEpoch)
	if err != nil {
		return nil, err
	}
	sid, err := id.NextId()
	if err != nil {
		return nil, err
	}

	bid := []byte(strconv.FormatUint(uint64(sid), 10))
	serverId, err := strconv.ParseUint(string(bid), 10, 32)
	if err != nil {
		return nil, err
	}

	// support regex
	binlogReader.genRegexMap()

	binlogSyncerConfig := &replication.BinlogSyncerConfig{
		ServerID:       uint32(serverId),
		Flavor:         "mysql",
		Host:           cfg.ConnectionConfig.Host,
		Port:           uint16(cfg.ConnectionConfig.Port),
		User:           cfg.ConnectionConfig.User,
		Password:       cfg.ConnectionConfig.Password,
		RawModeEnabled: false,
	}
	binlogReader.binlogSyncer = replication.NewBinlogSyncer(binlogSyncerConfig)
	binlogReader.mysqlContext.Stage = models.StageRegisteringSlaveOnMaster

	return binlogReader, err
}

func (b *BinlogReader) getDbTableMap(schemaName string) map[string]*config.TableContext {
	tableMap, ok := b.tables[schemaName]
	if !ok {
		tableMap = make(map[string]*config.TableContext)
		b.tables[schemaName] = tableMap
	}
	return tableMap
}
func (b *BinlogReader) addTableToTableMap(dbMap map[string]*config.TableContext, table *config.Table) error {
	if table.Where == "" {
		b.logger.Warnf("UDUP_BUG: NewMySQLReader: table.Where is empty (#177 like)")
		table.Where = "true"
	}
	whereCtx, err := config.NewWhereCtx(table.Where, table)
	if err != nil {
		b.logger.Errorf("mysql.reader: Error parse where '%v'", table.Where)
		return err
	}
	dbMap[table.TableName] = &config.TableContext{
		Table:    table,
		WhereCtx: whereCtx,
	}
	return nil
}
// ConnectBinlogStreamer
func (b *BinlogReader) ConnectBinlogStreamer(coordinates base.BinlogCoordinates) (err error) {
	if coordinates.IsEmpty() {
		b.logger.Warnf("mysql.reader: Emptry coordinates at ConnectBinlogStreamer")
	}

	b.currentCoordinates = coordinates
	b.logger.Printf("mysql.reader: Connecting binlog streamer at %+v", b.currentCoordinates)

	// Start sync with sepcified binlog gtid
	b.logger.Debugf("mysql.reader: GtidSet: %v", b.currentCoordinates.GtidSet)
	gtidSet, err := gomysql.ParseMysqlGTIDSet(b.currentCoordinates.GtidSet)
	if err != nil {
		b.logger.Errorf("mysql.reader: err: %v", err)
	}
	b.binlogStreamer, err = b.binlogSyncer.StartSyncGTID(gtidSet)
	if err != nil {
		b.logger.Debugf("mysql.reader: err at StartSyncGTID: %v", err)
	}
	b.mysqlContext.Stage = models.StageRequestingBinlogDump

	return err
}

func (b *BinlogReader) GetCurrentBinlogCoordinates() *base.BinlogCoordinates {
	b.currentCoordinatesMutex.Lock()
	defer b.currentCoordinatesMutex.Unlock()
	returnCoordinates := b.currentCoordinates
	return &returnCoordinates
}

func ToColumnValuesV2(abstractValues []interface{}, table *config.TableContext) *mysql.ColumnValues {
	result := &mysql.ColumnValues{
		AbstractValues: make([]*interface{}, len(abstractValues)),
		ValuesPointers: make([]*interface{}, len(abstractValues)),
	}

	for i := 0; i < len(abstractValues); i++ {
		if table != nil {
			columns := table.Table.OriginalTableColumns.Columns
			if i < len(columns) && columns[i].IsUnsigned {
				// len(columns) might less than len(abstractValues), esp on AliRDS. See #192.
				switch v := abstractValues[i].(type) {
				case int8:
					abstractValues[i] = uint8(v)
				case int16:
					abstractValues[i] = uint16(v)
				case int32:
					if columns[i].Type == mysql.MediumIntColumnType {
						abstractValues[i] = uint32(v) & 0x00FFFFFF
					} else {
						abstractValues[i] = uint32(v)
					}
				case int64:
					abstractValues[i] = uint64(v)
				}
			}
		}
		result.AbstractValues[i] = &abstractValues[i]
		result.ValuesPointers[i] = result.AbstractValues[i]
	}

	return result
}

type DDLType int
const (
	DDLOther DDLType = iota
	DDLAlterTable
	DDLCreateTable
	DDLCreateSchema
	DDLDropSchema
)
// If isDDL, a sql correspond to a table item, aka len(tables) == len(sqls).
type parseDDLResult struct {
	isDDL   bool
	ddlType DDLType
	tables  []SchemaTable
	sqls    []string
}

// StreamEvents
func (b *BinlogReader) handleEvent(ev *replication.BinlogEvent, entriesChannel chan<- *BinlogEntry) error {
	if b.currentCoordinates.SmallerThanOrEquals(&b.LastAppliedRowsEventHint) {
		b.logger.Debugf("mysql.reader: Skipping handled query at %+v", b.currentCoordinates)
		return nil
	}

	// b.currentBinlogEntry is created on GtidEvent
	// Size of GtidEvent is ignored.
	if b.currentBinlogEntry != nil {
		b.currentBinlogEntry.OriginalSize += len(ev.RawData)
	}

	switch ev.Header.EventType {
	case replication.GTID_EVENT:
		if strings.HasPrefix(b.mysqlContext.MySQLVersion, "5.7") {
			evt := ev.Event.(*replication.GTIDEventV57)
			b.currentCoordinatesMutex.Lock()
			defer b.currentCoordinatesMutex.Unlock()
			u, _ := uuid.FromBytes(evt.GTID.SID)
			b.currentCoordinates.SID = u.String()
			b.currentCoordinates.GNO = evt.GTID.GNO
			b.currentCoordinates.LastCommitted = evt.GTID.LastCommitted
			b.currentBinlogEntry = NewBinlogEntryAt(b.currentCoordinates)
		} else {
			evt := ev.Event.(*replication.GTIDEvent)
			b.currentCoordinatesMutex.Lock()
			defer b.currentCoordinatesMutex.Unlock()
			u, _ := uuid.FromBytes(evt.SID)
			b.currentCoordinates.SID = u.String()
			b.currentCoordinates.GNO = evt.GNO
			b.currentBinlogEntry = NewBinlogEntryAt(b.currentCoordinates)
		}
	case replication.QUERY_EVENT:
		evt := ev.Event.(*replication.QueryEvent)
		query := string(evt.Query)

		b.logger.Debugf("mysql.reader: query event: schema: %s, query: %s", evt.Schema, query)

		if strings.ToUpper(query) == "BEGIN" {
			b.currentBinlogEntry.hasBeginQuery = true
		} else {
			if strings.ToUpper(query) == "COMMIT" || !b.currentBinlogEntry.hasBeginQuery {
				currentSchema := string(evt.Schema)
				if b.mysqlContext.SkipCreateDbTable {
					if skipCreateDbTable(query) {
						b.logger.Warnf("mysql.reader: skip create db/table %s", query)
						return nil
					}
				}

				if !b.mysqlContext.ExpandSyntaxSupport {
					if skipQueryEvent(query) {
						b.logger.Warnf("mysql.reader: skip query %s", query)
						return nil
					}
				}

				ddlInfo, err := resolveDDLSQL(query)
				if err != nil {
					b.logger.Debugf("mysql.reader: Parse query [%v] event failed: %v", query, err)
					if b.skipQueryDDL(query, currentSchema, "") {
						b.logger.Debugf("mysql.reader: skip QueryEvent at schema: %s,sql: %s", currentSchema, query)
						return nil
					}
				}
				if !ddlInfo.isDDL {
					event := NewQueryEvent(
						currentSchema,
						query,
						NotDML,
					)
					b.currentBinlogEntry.Events = append(b.currentBinlogEntry.Events, event)
					entriesChannel <- b.currentBinlogEntry
					b.LastAppliedRowsEventHint = b.currentCoordinates
					return nil
				}

				for i, sql := range ddlInfo.sqls {
					realSchema := utils.StringElse(ddlInfo.tables[i].Schema, currentSchema)
					tableName := ddlInfo.tables[i].Table

					if b.skipQueryDDL(sql, realSchema, tableName) {
						//b.logger.Debugf("mysql.reader: Skip QueryEvent at schema: %s,sql: %s", fmt.Sprintf("%s", evt.Schema), sql)
						return nil
					}

					switch ddlInfo.ddlType {
					case DDLCreateTable, DDLAlterTable:
						// create table is not ignored
						b.logger.Debugf("mysql.reader: ddl is create table")
						columns, err := base.GetTableColumns(b.db, realSchema, tableName)
						if err != nil {
							b.logger.Warnf("error handle create table in binlog: GetTableColumns: %v", err.Error())
						}
						err = base.ApplyColumnTypes(b.db, realSchema, tableName, columns)
						if err != nil {
							b.logger.Warnf("error handle create table in binlog: ApplyColumnTypes: %v", err.Error())
						}
						table := config.NewTable(realSchema, tableName)
						table.TableType = "BASE TABLE"
						table.OriginalTableColumns = columns

						tableMap := b.getDbTableMap(realSchema)
						b.addTableToTableMap(tableMap, table)
					}

					event := NewQueryEventAffectTable(
						currentSchema,
						sql,
						NotDML,
						ddlInfo.tables[i],
					)
					b.currentBinlogEntry.Events = append(b.currentBinlogEntry.Events, event)
				}
				entriesChannel <- b.currentBinlogEntry
				b.LastAppliedRowsEventHint = b.currentCoordinates
			}
		}
	case replication.XID_EVENT:
		entriesChannel <- b.currentBinlogEntry
		b.LastAppliedRowsEventHint = b.currentCoordinates
	default:
		if rowsEvent, ok := ev.Event.(*replication.RowsEvent); ok {
			skip, table := b.skipRowEvent(rowsEvent)
			if skip {
				//b.logger.Debugf("mysql.reader: skip rowsEvent [%s-%s]", rowsEvent.Table.Schema, rowsEvent.Table.Table)
				return nil
			}

			dml := ToEventDML(ev.Header.EventType.String())
			if dml == NotDML {
				return fmt.Errorf("Unknown DML type: %s", ev.Header.EventType.String())
			}
			dmlEvent := NewDataEvent(
				string(rowsEvent.Table.Schema),
				string(rowsEvent.Table.Table),
				dml,
				int(rowsEvent.ColumnCount),
			)
			dmlEvent.LogPos = int64(ev.Header.LogPos - ev.Header.EventSize)
			dmlEvent.Table = table.Table
			/*originalTableColumns, _, err := b.InspectTableColumnsAndUniqueKeys(string(rowsEvent.Table.Schema), string(rowsEvent.Table.Table))
			if err != nil {
				return err
			}
			dmlEvent.OriginalTableColumns = originalTableColumns*/

			for i, row := range rowsEvent.Rows {
				b.logger.Debugf("mysql.reader: row values: %v", row)
				if dml == UpdateDML && i%2 == 1 {
					// An update has two rows (WHERE+SET)
					// We do both at the same time
					continue
				}
				switch dml {
				case InsertDML:
					{
						dmlEvent.NewColumnValues = ToColumnValuesV2(row, table)
					}
				case UpdateDML:
					{
						dmlEvent.WhereColumnValues = ToColumnValuesV2(row, table)
						dmlEvent.NewColumnValues = ToColumnValuesV2(rowsEvent.Rows[i+1], table)
					}
				case DeleteDML:
					{
						dmlEvent.WhereColumnValues = ToColumnValuesV2(row, table)
					}
				}

				//b.logger.Debugf("event before row: %v", dmlEvent.WhereColumnValues)
				//b.logger.Debugf("event after row: %v", dmlEvent.NewColumnValues)
				whereTrue := true
				var err error
				if table != nil && !table.WhereCtx.IsDefault {
					switch dml {
					case InsertDML:
						whereTrue, err = table.WhereTrue(dmlEvent.NewColumnValues)
						if err != nil {
							return err
						}
					case UpdateDML:
						before, err := table.WhereTrue(dmlEvent.WhereColumnValues)
						if err != nil {
							return err
						}
						after, err := table.WhereTrue(dmlEvent.NewColumnValues)
						if err != nil {
							return err
						}
						if before != after {
							return fmt.Errorf("update on 'where columns' cause inconsistency")
							// TODO split it to delete + insert to allow such update
						} else {
							whereTrue = before
						}
					case DeleteDML:
						whereTrue, err = table.WhereTrue(dmlEvent.WhereColumnValues)
						if err != nil {
							return err
						}
					}
				}

				if !whereTrue {
					b.logger.Debugf("event has not passed 'where'")
					return nil
				}

				// The channel will do the throttling. Whoever is reding from the channel
				// decides whether action is taken sycnhronously (meaning we wait before
				// next iteration) or asynchronously (we keep pushing more events)
				// In reality, reads will be synchronous
				b.currentBinlogEntry.Events = append(b.currentBinlogEntry.Events, dmlEvent)
			}
			return nil
		}
	}
	return nil
}

// StreamEvents
func (b *BinlogReader) DataStreamEvents(entriesChannel chan<- *BinlogEntry) error {
	for {
		// Check for shutdown
		if b.shutdown {
			break
		}

		ev, err := b.binlogStreamer.GetEvent(context.Background())
		if err != nil {
			return err
		}
		if ev.Header.EventType == replication.HEARTBEAT_EVENT {
			continue
		}
		//ev.Dump(os.Stdout)

		func() {
			b.currentCoordinatesMutex.Lock()
			defer b.currentCoordinatesMutex.Unlock()
			b.currentCoordinates.LogPos = int64(ev.Header.LogPos)
		}()

		if rotateEvent, ok := ev.Event.(*replication.RotateEvent); ok {
			func() {
				b.currentCoordinatesMutex.Lock()
				defer b.currentCoordinatesMutex.Unlock()
				b.currentCoordinates.LogFile = string(rotateEvent.NextLogName)
			}()
			b.mysqlContext.Stage = models.StageFinishedReadingOneBinlogSwitchingToNextBinlog
			b.logger.Printf("mysql.reader: Rotate to next log name: %s", rotateEvent.NextLogName)
		} else {
			if err := b.handleEvent(ev, entriesChannel); err != nil {
				return err
			}
		}
	}

	return nil
}

func (b *BinlogReader) BinlogStreamEvents(txChannel chan<- *BinlogTx) error {
	for {
		// Check for shutdown
		if b.shutdown {
			break
		}

		ev, err := b.binlogStreamer.GetEvent(context.Background())
		if err != nil {
			return err
		}

		/*switch ev.Header.EventType {
		case replication.TABLE_MAP_EVENT:
			evt := ev.Event.(*replication.TableMapEvent)
			b.logger.Debugf("mysql.reader: TableID: %d", evt.TableID)
			//b.logger.Printf("TableID size: %d\n", evt.tableIDSize)
			b.logger.Debugf("mysql.reader: Flags: %d", evt.Flags)
			b.logger.Debugf("mysql.reader: Schema: %s", evt.Schema)
			b.logger.Debugf("mysql.reader: Table: %s", evt.Table)
			b.logger.Debugf("mysql.reader: Column count: %d", evt.ColumnCount)
			b.logger.Debugf("mysql.reader: Column type: \n%s", hex.Dump(evt.ColumnType))
			b.logger.Debugf("mysql.reader: NULL bitmap: \n%s", hex.Dump(evt.NullBitmap))
		case replication.GTID_EVENT:
			evt := ev.Event.(*replication.GTIDEventV57)
			u, _ := uuid.FromBytes(evt.GTID.SID)
			b.logger.Debugf("mysql.reader: Last committed: %d", evt.GTID.LastCommitted)
			b.logger.Debugf("mysql.reader: Sequence number: %d", evt.GTID.SequenceNumber)
			b.logger.Debugf("mysql.reader: Commit flag: %d", evt.GTID.CommitFlag)
			b.logger.Debugf("mysql.reader: GTID_NEXT: %s:%d", u.String(), evt.GTID.GNO)
		case replication.QUERY_EVENT:
			evt := ev.Event.(*replication.QueryEvent)
			b.logger.Debugf("mysql.reader: Slave proxy ID: %d", evt.SlaveProxyID)
			b.logger.Debugf("mysql.reader: Execution time: %d", evt.ExecutionTime)
			b.logger.Debugf("mysql.reader: Error code: %d", evt.ErrorCode)
			//b.logger.Printf( "Status vars: \n%s", hex.Dump(e.StatusVars))
			b.logger.Debugf("mysql.reader: Schema: %s", evt.Schema)
			b.logger.Debugf("mysql.reader: Query: %s", evt.Query)
		case replication.WRITE_ROWS_EVENTv2, replication.UPDATE_ROWS_EVENTv2, replication.DELETE_ROWS_EVENTv2:
			evt := ev.Event.(*replication.RowsEvent)
			b.logger.Debugf("mysql.reader: TableID: %d", evt.TableID)
			b.logger.Debugf("mysql.reader: Flags: %d", evt.Flags)
			b.logger.Debugf("mysql.reader: Column count: %d", evt.ColumnCount)

			b.logger.Debugf("mysql.reader: Values:")
			for _, rows := range evt.Rows {
				b.logger.Debugf("mysql.reader: --")
				for j, d := range rows {
					if _, ok := d.([]byte); ok {
						b.logger.Debugf("mysql.reader: %d:%q", j, d)
					} else {
						b.logger.Debugf("mysql.reader: %d:%#v", j, d)
					}
				}
			}
		case replication.XID_EVENT:
			evt := ev.Event.(*replication.XIDEvent)
			b.logger.Debugf("mysql.reader: XID: %d", evt.XID)
		}*/
		//--------------------------------

		func() {
			b.currentCoordinatesMutex.Lock()
			defer b.currentCoordinatesMutex.Unlock()
			b.currentCoordinates.LogPos = int64(ev.Header.LogPos)
		}()
		if rotateEvent, ok := ev.Event.(*replication.RotateEvent); ok {
			func() {
				b.currentCoordinatesMutex.Lock()
				defer b.currentCoordinatesMutex.Unlock()
				b.currentCoordinates.LogFile = string(rotateEvent.NextLogName)
			}()
			b.logger.Printf("mysql.reader: Rotate to next log name: %s", rotateEvent.NextLogName)
		} else {
			if err := b.handleBinlogRowsEvent(ev, txChannel); err != nil {
				return err
			}
		}
	}

	return nil
}

func (b *BinlogReader) handleBinlogRowsEvent(ev *replication.BinlogEvent, txChannel chan<- *BinlogTx) error {
	//bp.logger.Printf("[TEST] currentCoordinates: %v,LastAppliedRowsEventHint: %v",bp.currentCoordinates,bp.LastAppliedRowsEventHint)
	if b.currentCoordinates.SmallerThanOrEquals(&b.LastAppliedRowsEventHint) {
		b.logger.Debugf("mysql.reader: skipping handled query at %+v", b.currentCoordinates)
		return nil
	}

	if b.currentTx != nil {
		b.currentTx.eventCount++
		b.currentTx.EventSize += uint64(ev.Header.EventSize)
	}

	switch ev.Header.EventType {
	case replication.FORMAT_DESCRIPTION_EVENT:
		b.currentFde = "BINLOG '\n" + base64.StdEncoding.EncodeToString(ev.RawData) + "\n'"

	case replication.GTID_EVENT:
		if b.currentTx != nil {
			return fmt.Errorf("unfinished transaction %v@%v", b.currentCoordinates.LogFile, uint32(ev.Header.LogPos)-ev.Header.EventSize)
		}

		// Match the version string (from SELECT VERSION()).
		if strings.HasPrefix(b.mysqlContext.MySQLVersion, "5.7") {
			evt := ev.Event.(*replication.GTIDEventV57)
			u, _ := uuid.FromBytes(evt.GTID.SID)

			b.currentTx = &BinlogTx{
				SID:           u.String(),
				GNO:           evt.GTID.GNO,
				LastCommitted: evt.GTID.LastCommitted,
				//Gtid:           fmt.Sprintf("%s:%d", u.String(), evt.GNO),
				Impacting: map[uint64]([]string){},
				EventSize: uint64(ev.Header.EventSize),
			}

			b.currentSqlB64 = new(bytes.Buffer)
			b.currentSqlB64.WriteString("BINLOG '\n")
		} else {
			evt := ev.Event.(*replication.GTIDEvent)
			u, _ := uuid.FromBytes(evt.SID)

			b.currentTx = &BinlogTx{
				SID:       u.String(),
				GNO:       evt.GNO,
				Impacting: map[uint64]([]string){},
				EventSize: uint64(ev.Header.EventSize),
			}

			b.currentSqlB64 = new(bytes.Buffer)
			b.currentSqlB64.WriteString("BINLOG '\n")
		}

	case replication.QUERY_EVENT:
		event := &BinlogEvent{
			BinlogFile: b.currentCoordinates.LogFile,
			Header:     ev.Header,
			RealPos:    uint32(ev.Header.LogPos) - ev.Header.EventSize,
			Evt:        ev.Event,
		}

		if b.currentTx == nil {
			return newTxWithoutGTIDError(event)
		}

		evt := ev.Event.(*replication.QueryEvent)
		query := string(evt.Query)

		// a tx should contain one DDL at most
		if b.currentTx.ErrorCode != evt.ErrorCode {
			if b.currentTx.ErrorCode != 0 {
				return fmt.Errorf("multiple error code in a tx. see txBuilder.onQueryEvent()")
			}
			b.currentTx.ErrorCode = evt.ErrorCode
		}

		if strings.ToUpper(query) == "BEGIN" {
			b.currentTx.hasBeginQuery = true
			b.appendQuery(query)
		} else {
			// DDL or statement/mixed binlog format
			b.clearB64Sql()
			if strings.ToUpper(query) == "COMMIT" || !b.currentTx.hasBeginQuery {
				currentSchema := string(evt.Schema)

				if b.mysqlContext.SkipCreateDbTable {
					if skipCreateDbTable(query) {
						b.logger.Warnf("mysql.reader: skip create db/table %s", query)
						return nil
					}
				}

				if !b.mysqlContext.ExpandSyntaxSupport {
					if skipQueryEvent(query) {
						b.logger.Warnf("skip query %s", query)
						return nil
					}
				}

				ddlInfo, err := resolveDDLSQL(query)
				if err != nil {
					b.logger.Debugf("mysql.reader: Parse query [%v] event failed: %v", query, err)
				}
				if !ddlInfo.isDDL {
					b.appendQuery(query)
					b.onCommit(event, txChannel)
					return nil
				}


				for i, sql := range ddlInfo.sqls {
					realSchema := utils.StringElse(ddlInfo.tables[i].Schema, currentSchema)
					tableName := ddlInfo.tables[i].Table

					if b.skipQueryDDL(sql, realSchema, tableName) {
						b.logger.Debugf("mysql.reader: skip QueryEvent at schema: %s,sql: %s", fmt.Sprintf("%s", evt.Schema), sql)
						continue
					}

					sql, err = GenDDLSQL(sql, realSchema)
					if err != nil {
						return err
					}
					b.appendQuery(sql)
				}

				b.onCommit(event, txChannel)
			}
		}

	case replication.TABLE_MAP_EVENT:
		evt := ev.Event.(*replication.TableMapEvent)

		if b.skipEvent(string(evt.Schema), string(evt.Table)) {
			//b.logger.Debugf("mysql.reader: skip TableMapEvent at schema: %s,table: %s", fmt.Sprintf("%s", evt.Schema), fmt.Sprintf("%s", evt.Table))
			return nil
		}

		b.appendB64Sql(&BinlogEvent{
			BinlogFile: b.currentCoordinates.LogFile,
			Header:     ev.Header,
			RealPos:    uint32(ev.Header.LogPos) - ev.Header.EventSize,
			Evt:        ev.Event,
			RawBs:      ev.RawData,
		})

	case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		evt := ev.Event.(*replication.RowsEvent)
		if b.skipEvent(string(evt.Table.Schema), string(evt.Table.Table)) {
			//b.logger.Debugf("mysql.reader: skip RowsEvent at schema: %s,table: %s", fmt.Sprintf("%s", evt.Table.Schema), fmt.Sprintf("%s", evt.Table.Table))
			return nil
		}

		b.appendB64Sql(&BinlogEvent{
			BinlogFile: b.currentCoordinates.LogFile,
			Header:     ev.Header,
			RealPos:    uint32(ev.Header.LogPos) - ev.Header.EventSize,
			Evt:        ev.Event,
			RawBs:      ev.RawData,
		})
		//tb.addCount(Insert)

	case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		evt := ev.Event.(*replication.RowsEvent)
		if b.skipEvent(string(evt.Table.Schema), string(evt.Table.Table)) {
			//b.logger.Debugf("mysql.reader: skip RowsEvent at schema: %s,table: %s", fmt.Sprintf("%s", evt.Table.Schema), fmt.Sprintf("%s", evt.Table.Table))
			return nil
		}

		b.appendB64Sql(&BinlogEvent{
			BinlogFile: b.currentCoordinates.LogFile,
			Header:     ev.Header,
			RealPos:    uint32(ev.Header.LogPos) - ev.Header.EventSize,
			Evt:        ev.Event,
			RawBs:      ev.RawData,
		})
		//tb.addCount(Update)

	case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		evt := ev.Event.(*replication.RowsEvent)
		if b.skipEvent(string(evt.Table.Schema), string(evt.Table.Table)) {
			//b.logger.Debugf("mysql.reader: skip RowsEvent at schema: %s,table: %s", fmt.Sprintf("%s", evt.Table.Schema), fmt.Sprintf("%s", evt.Table.Table))
			return nil
		}

		b.appendB64Sql(&BinlogEvent{
			BinlogFile: b.currentCoordinates.LogFile,
			Header:     ev.Header,
			RealPos:    uint32(ev.Header.LogPos) - ev.Header.EventSize,
			Evt:        ev.Event,
			RawBs:      ev.RawData,
		})

	case replication.XID_EVENT:
		event := &BinlogEvent{
			BinlogFile: b.currentCoordinates.LogFile,
			Header:     ev.Header,
			RealPos:    uint32(ev.Header.LogPos) - ev.Header.EventSize,
			Evt:        ev.Event,
			RawBs:      ev.RawData,
		}

		if b.currentTx == nil {
			return newTxWithoutGTIDError(event)
		}
		b.onCommit(event, txChannel)
	default:
		//ignore
	}
	b.LastAppliedRowsEventHint = b.currentCoordinates
	return nil
}
func (b *BinlogReader) appendQuery(query string) {
	if b.currentQuery == nil {
		b.currentQuery = new(bytes.Buffer)
	}
	if b.currentQuery.String() != "" {
		b.currentQuery.WriteString(";")
	}
	b.currentQuery.WriteString(query)
}

func newTxWithoutGTIDError(event *BinlogEvent) error {
	return fmt.Errorf("transaction without GTID_EVENT %v@%v", event.BinlogFile, event.RealPos)
}

func (b *BinlogReader) clearB64Sql() {
	b.currentSqlB64 = nil
}

func (b *BinlogReader) appendB64Sql(event *BinlogEvent) {
	n := base64.StdEncoding.EncodedLen(len(event.RawBs))
	// enlarge only
	if len(b.appendB64SqlBs) < n {
		b.appendB64SqlBs = make([]byte, n)
	}
	base64.StdEncoding.Encode(b.appendB64SqlBs, event.RawBs)
	b.currentSqlB64.Write(b.appendB64SqlBs[0:n])

	b.currentSqlB64.WriteString("\n")
}

func (b *BinlogReader) onCommit(lastEvent *BinlogEvent, txChannel chan<- *BinlogTx) {
	b.wg.Add(1)
	defer b.wg.Done()
	if nil != b.currentSqlB64 && !strings.HasSuffix(b.currentSqlB64.String(), "BINLOG '") {
		b.currentSqlB64.WriteString("'")
		b.appendQuery(b.currentSqlB64.String())
	}

	if nil != b.currentQuery {
		b.currentTx.Fde = b.currentFde
		b.currentTx.Query = b.currentQuery.String()
	}

	if !b.shutdown {
		txChannel <- b.currentTx
	}

	b.currentQuery = nil
	b.currentTx = nil
}

func GenDDLSQL(sql string, schema string) (string, error) {
	stmt, err := parser.New().ParseOneStmt(sql, "", "")
	if err != nil {
		return "", err
	}
	_, isCreateDatabase := stmt.(*ast.CreateDatabaseStmt)
	if isCreateDatabase {
		return sql, nil
	}
	if schema == "" {
		return sql, nil
	}

	return fmt.Sprintf("USE %s;%s", schema, sql), nil
}

// resolveDDLSQL resolve to one ddl sql
// example: drop table test.a,test2.b -> drop table test.a; drop table test2.b;
//
// schemaTables is the schema.table that the query has invalidated. For err or non-DDL, it is nil.
// For DDL, it size equals len(sqls).
func resolveDDLSQL(sql string) (result parseDDLResult, err error) {
	result.ddlType = DDLOther

	stmt, err := parser.New().ParseOneStmt(sql, "", "")
	if err != nil {
		result.sqls = append(result.sqls, sql)
		return result, err
	}

	_, result.isDDL = stmt.(ast.DDLNode)
	if !result.isDDL {
		result.sqls = append(result.sqls, sql)
		return result, nil
	}

	appendSql := func(sql string, schema string, table string) {
		result.tables = append(result.tables, SchemaTable{Schema:schema, Table:table})
		result.sqls = append(result.sqls, sql)
	}

	switch v := stmt.(type) {
	case *ast.CreateDatabaseStmt:
		result.ddlType = DDLCreateSchema
		appendSql(sql, strings.ToLower(v.Name), "")
	case *ast.DropDatabaseStmt:
		result.ddlType = DDLDropSchema
		appendSql(sql, strings.ToLower(v.Name), "")
	case *ast.CreateIndexStmt:
		appendSql(sql, v.Table.Schema.L, v.Table.Name.L)
	case *ast.DropIndexStmt:
		appendSql(sql, v.Table.Schema.L, v.Table.Name.L)
	case *ast.TruncateTableStmt:
		appendSql(sql, v.Table.Schema.L, v.Table.Name.L)
	case *ast.CreateTableStmt:
		result.ddlType = DDLCreateTable
		appendSql(sql, v.Table.Schema.L, v.Table.Name.L)
	case *ast.AlterTableStmt:
		appendSql(sql, v.Table.Schema.L, v.Table.Name.L)
		result.ddlType = DDLAlterTable
	case *ast.DropTableStmt:
		var ex string
		if v.IfExists {
			ex = "if exists"
		}
		for _, t := range v.Tables {
			var db string
			if t.Schema.O != "" {
				db = fmt.Sprintf("%s.", t.Schema.O)
			}
			s := fmt.Sprintf("drop table %s %s`%s`", ex, db, t.Name.L)
			appendSql(s, t.Schema.L, t.Name.L)
		}
	case *ast.CreateUserStmt, *ast.GrantStmt:
		appendSql(sql, "mysql", "user")
	default:
		return result, fmt.Errorf("unknown DDL type")
	}

	return result, nil
}

func (b *BinlogReader) skipQueryDDL(sql string, schema string, tableName string) bool {
	switch strings.ToLower(schema) {
	case "mysql":
		if b.mysqlContext.ExpandSyntaxSupport {
			return false
		} else {
			return true
		}
	case "sys", "information_schema", "performance_schema", "actiontech_udup":
		return true
	default:
		if len(b.mysqlContext.ReplicateDoDb) > 0 {
			return !b.matchTable(b.mysqlContext.ReplicateDoDb, schema, tableName)
		}
		if len(b.mysqlContext.ReplicateIgnoreDb) > 0 {
			return b.matchTable(b.mysqlContext.ReplicateIgnoreDb, schema, tableName)
		}
		return false
	}
}

func skipCreateDbTable(sql string) bool {
	sql = strings.ToLower(sql)

	if strings.HasPrefix(sql, "create database") {
		return true
	}
	if strings.HasPrefix(sql, "create table") {
		return true
	}

	return false
}

func skipQueryEvent(sql string) bool {
	sql = strings.ToLower(sql)

	if strings.HasPrefix(sql, "flush privileges") {
		return true
	}
	if strings.HasPrefix(sql, "alter user") {
		return true
	}
	if strings.HasPrefix(sql, "create user") {
		return true
	}
	if strings.HasPrefix(sql, "create function") {
		return true
	}
	if strings.HasPrefix(sql, "create procedure") {
		return true
	}
	if strings.HasPrefix(sql, "drop user") {
		return true
	}
	if strings.HasPrefix(sql, "delete from mysql.user") {
		return true
	}
	if strings.HasPrefix(sql, "grant") {
		return true
	}
	if strings.HasPrefix(sql, "rename user") {
		return true
	}
	if strings.HasPrefix(sql, "revoke") {
		return true
	}
	if strings.HasPrefix(sql, "set password") {
		return true
	}

	return false
}

func (b *BinlogReader) skipEvent(schema string, table string) bool {
	switch strings.ToLower(schema) {
	case "mysql":
		if b.mysqlContext.ExpandSyntaxSupport {
			return skipMysqlSchemaEvent(strings.ToLower(table));
		} else {
			return true
		}
	case "sys", "information_schema", "performance_schema", "actiontech_udup":
		return true
	default:
		if len(b.mysqlContext.ReplicateDoDb) > 0 {
			table = strings.ToLower(table)
			//if table in tartget Table, do this event
			for _, d := range b.mysqlContext.ReplicateDoDb {
				if b.matchString(d.TableSchema, schema) || d.TableSchema == "" {
					if len(d.Tables) == 0 {
						return false
					}
					for _, dt := range d.Tables {
						if b.matchString(dt.TableName, table) {
							return false
						}
					}
				}
			}
			return true
		}
		if len(b.mysqlContext.ReplicateIgnoreDb) > 0 {
			table = strings.ToLower(table)
			//if table in tartget Table, do this event
			for _, d := range b.mysqlContext.ReplicateIgnoreDb {
				if b.matchString(d.TableSchema, schema) || d.TableSchema == "" {
					if len(d.Tables) == 0 {
						return true
					}
					for _, dt := range d.Tables {
						if b.matchString(dt.TableName, table) {
							return true
						}
					}
				}
			}
			return false
		}
	}
	return false
}

func skipMysqlSchemaEvent(tableLower string) bool {
	switch tableLower {
	case "event", "func", "proc", "tables_priv", "columns_priv", "procs_priv", "user":
		return false
	default:
		return true
	}
}

func (b *BinlogReader) skipRowEvent(rowsEvent *replication.RowsEvent) (bool, *config.TableContext) {
	tableLower := strings.ToLower(string(rowsEvent.Table.Table))
	switch strings.ToLower(string(rowsEvent.Table.Schema)) {
	case "actiontech_udup":
		b.currentBinlogEntry.Coordinates.OSID = mysql.ToColumnValues(rowsEvent.Rows[0]).StringColumn(0)
		return true, nil
	case "mysql":
		if b.mysqlContext.ExpandSyntaxSupport {
			return skipMysqlSchemaEvent(tableLower), nil
		} else {
			return true, nil
		}
	case "sys", "information_schema", "performance_schema":
		return true, nil
	default:
		if len(b.tables) > 0 {
			//if table in tartget Table, do this event
			for schemaName, tableMap := range b.tables {
				if b.matchString(schemaName, string(rowsEvent.Table.Schema)) || schemaName == "" {
					if len(tableMap) == 0 {
						return false, nil // TODO not skipping but TableContext
					}
					for tableName, tableCtx := range tableMap {
						if b.matchString(tableName, tableLower) {
							return false, tableCtx
						}
					}
				}
			}
			return true, nil
		}
		if len(b.mysqlContext.ReplicateIgnoreDb) > 0 {
			table := strings.ToLower(string(rowsEvent.Table.Table))
			//if table in tartget Table, do this event
			for _, d := range b.mysqlContext.ReplicateIgnoreDb {
				if b.matchString(d.TableSchema, string(rowsEvent.Table.Schema)) || d.TableSchema == "" {
					if len(d.Tables) == 0 {
						return true, nil
					}
					for _, dt := range d.Tables {
						if b.matchString(dt.TableName, table) {
							return true, nil
						}
					}
				}
			}
			return false, nil
		}
	}
	return false, nil
}

func (b *BinlogReader) matchString(pattern string, t string) bool {
	if re, ok := b.ReMap[pattern]; ok {
		return re.MatchString(t)
	}
	return pattern == t
}

func (b *BinlogReader) matchDB(patternDBS []*config.DataSource, a string) bool {
	for _, pdb := range patternDBS {
		if b.matchString(pdb.TableSchema, a) {
			return true
		}
	}
	return false
}

func (b *BinlogReader) matchTable(patternTBS []*config.DataSource, schemaName string, tableName string) bool {
	for _, pdb := range patternTBS {
		if len(pdb.Tables) == 0 && schemaName == pdb.TableSchema {
			return true
		}
		redb, okdb := b.ReMap[pdb.TableSchema]
		for _, ptb := range pdb.Tables {
			retb, oktb := b.ReMap[ptb.TableName]
			if oktb && okdb {
				if redb.MatchString(schemaName) && retb.MatchString(tableName) {
					return true
				}
			}
			if oktb {
				if retb.MatchString(tableName) && schemaName == pdb.TableSchema {
					return true
				}
			}
			if okdb {
				if redb.MatchString(schemaName) && tableName == ptb.TableName {
					return true
				}
			}

			//create database or drop database
			if tableName == "" {
				if schemaName == pdb.TableSchema {
					return true
				}
			}

			if ptb.TableName == tableName {
				return true
			}
		}
	}

	return false
}

func (b *BinlogReader) genRegexMap() {
	for _, db := range b.mysqlContext.ReplicateDoDb {
		if db.TableSchema[0] != '~' {
			continue
		}
		if _, ok := b.ReMap[db.TableSchema]; !ok {
			b.ReMap[db.TableSchema] = regexp.MustCompile(db.TableSchema[1:])
		}

		for _, tb := range db.Tables {
			if tb.TableName[0] == '~' {
				if _, ok := b.ReMap[tb.TableName]; !ok {
					b.ReMap[tb.TableName] = regexp.MustCompile(tb.TableName[1:])
				}
			}
			if tb.TableSchema[0] == '~' {
				if _, ok := b.ReMap[tb.TableSchema]; !ok {
					b.ReMap[tb.TableSchema] = regexp.MustCompile(tb.TableSchema[1:])
				}
			}
		}
	}
}

func (b *BinlogReader) Close() error {
	b.shutdownLock.Lock()
	defer b.shutdownLock.Unlock()

	if b.shutdown {
		return nil
	}
	b.shutdown = true
	close(b.shutdownCh)

	b.wg.Wait()
	if err := sql.CloseDB(b.db); err != nil {
		return err
	}
	// Historically there was a:
	b.binlogSyncer.Close()
	// here. A new go-mysql version closes the binlog syncer connection independently.
	// I will go against the sacred rules of comments and just leave this here.
	// This is the year 2017. Let's see what year these comments get deleted.
	return nil
}
