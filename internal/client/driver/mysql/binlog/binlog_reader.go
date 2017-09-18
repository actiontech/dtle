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
	mysqlContext             *config.MySQLDriverConfig

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

func NewMySQLReader(cfg *config.MySQLDriverConfig, logger *log.Entry) (binlogReader *BinlogReader, err error) {
	binlogReader = &BinlogReader{
		logger:                  logger,
		currentCoordinates:      base.BinlogCoordinates{},
		currentCoordinatesMutex: &sync.Mutex{},
		mysqlContext:            cfg,
		appendB64SqlBs:          make([]byte, 1024*1024),
		ReMap:                   make(map[string]*regexp.Regexp),
		shutdownCh:              make(chan struct{}),
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

// ConnectBinlogStreamer
func (b *BinlogReader) ConnectBinlogStreamer(coordinates base.BinlogCoordinates) (err error) {
	if coordinates.IsEmpty() {
		b.logger.Warnf("mysql.reader: Emptry coordinates at ConnectBinlogStreamer")
	}

	b.currentCoordinates = coordinates
	b.logger.Debugf("mysql.reader: Connecting binlog streamer at %+v", b.currentCoordinates)

	// Start sync with sepcified binlog gtid
	gtidSet, err := gomysql.ParseMysqlGTIDSet(b.currentCoordinates.GtidSet)
	if err != nil {
		b.logger.Errorf("mysql.reader: err: %v", err)
	}
	b.binlogStreamer, err = b.binlogSyncer.StartSyncGTID(gtidSet)
	b.mysqlContext.Stage = models.StageRequestingBinlogDump

	return err
}

func (b *BinlogReader) GetCurrentBinlogCoordinates() *base.BinlogCoordinates {
	b.currentCoordinatesMutex.Lock()
	defer b.currentCoordinatesMutex.Unlock()
	returnCoordinates := b.currentCoordinates
	return &returnCoordinates
}

// StreamEvents
func (b *BinlogReader) handleEvent(ev *replication.BinlogEvent, entriesChannel chan<- *BinlogEntry) error {
	if b.currentCoordinates.SmallerThanOrEquals(&b.LastAppliedRowsEventHint) {
		b.logger.Debugf("mysql.reader: Skipping handled query at %+v", b.currentCoordinates)
		return nil
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

		if strings.ToUpper(query) == "BEGIN" {
			b.currentBinlogEntry.hasBeginQuery = true
		} else {
			if strings.ToUpper(query) == "COMMIT" || !b.currentBinlogEntry.hasBeginQuery {
				if skipQueryEvent(query) {
					b.logger.Warnf("skip query %s", query)
					return nil
				}

				sqls, ok, err := resolveDDLSQL(query)
				if err != nil {
					b.logger.Debugf("mysql.reader: Parse query [%v] event failed: %v", query, err)
				}
				if !ok {
					event := NewQueryEvent(
						string(evt.Schema),
						query,
						NotDML,
					)
					b.currentBinlogEntry.Events = append(b.currentBinlogEntry.Events, event)
					entriesChannel <- b.currentBinlogEntry
					b.LastAppliedRowsEventHint = b.currentCoordinates
					return nil
				}

				for _, sql := range sqls {
					if b.skipQueryDDL(sql, string(evt.Schema)) {
						//b.logger.Debugf("mysql.reader: Skip QueryEvent at schema: %s,sql: %s", fmt.Sprintf("%s", evt.Schema), sql)
						continue
					}

					event := NewQueryEvent(
						string(evt.Schema),
						sql,
						NotDML,
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
			if b.skipRowEvent(rowsEvent) {
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

			/*originalTableColumns, _, err := b.InspectTableColumnsAndUniqueKeys(string(rowsEvent.Table.Schema), string(rowsEvent.Table.Table))
			if err != nil {
				return err
			}
			dmlEvent.OriginalTableColumns = originalTableColumns*/

			for i, row := range rowsEvent.Rows {
				if dml == UpdateDML && i%2 == 1 {
					// An update has two rows (WHERE+SET)
					// We do both at the same time
					continue
				}
				switch dml {
				case InsertDML:
					{
						//dmlEvent.NewColumnValues = mysql.ToColumnValues(row)
						dmlEvent.NewColumnValues = append(dmlEvent.NewColumnValues, mysql.ToColumnValues(row))
					}
				case UpdateDML:
					{
						dmlEvent.WhereColumnValues = mysql.ToColumnValues(row)
						//dmlEvent.NewColumnValues = mysql.ToColumnValues(rowsEvent.Rows[i+1])
						dmlEvent.NewColumnValues = append(dmlEvent.NewColumnValues, mysql.ToColumnValues(rowsEvent.Rows[i+1]))
					}
				case DeleteDML:
					{
						dmlEvent.WhereColumnValues = mysql.ToColumnValues(row)
					}
				}
				// The channel will do the throttling. Whoever is reding from the channel
				// decides whether action is taken sycnhronously (meaning we wait before
				// next iteration) or asynchronously (we keep pushing more events)
				// In reality, reads will be synchronous
			}
			b.currentBinlogEntry.Events = append(b.currentBinlogEntry.Events, dmlEvent)
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
			b.logger.Debugf("mysql.reader: Rotate to next log name: %s", rotateEvent.NextLogName)
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
			b.logger.Debugf("mysql.reader: Rotate to next log name: %s", rotateEvent.NextLogName)
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
				if skipQueryEvent(query) {
					b.logger.Warnf("skip query %s", query)
					return nil
				}

				sqls, ok, err := resolveDDLSQL(query)
				if err != nil {
					b.logger.Debugf("mysql.reader: Parse query [%v] event failed: %v", query, err)
				}
				if !ok {
					b.appendQuery(query)
					b.onCommit(event, txChannel)
					return nil
				}

				for _, sql := range sqls {
					if b.skipQueryDDL(sql, string(evt.Schema)) {
						//b.logger.Debugf("mysql.reader: skip QueryEvent at schema: %s,sql: %s", fmt.Sprintf("%s", evt.Schema), sql)
						continue
					}

					sql, err = GenDDLSQL(sql, string(evt.Schema))
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

func (b *BinlogReader) InspectTableColumnsAndUniqueKeys(databaseName, tableName string) (columns *mysql.ColumnList, uniqueKeys [](*mysql.UniqueKey), err error) {
	/*uniqueKeys, err = b.getCandidateUniqueKeys(databaseName, tableName)
	if err != nil {
		return columns, uniqueKeys, err
	}*/
	/*if len(uniqueKeys) == 0 {
		return columns, uniqueKeys, fmt.Errorf("No PRIMARY nor UNIQUE key found in table! Bailing out")
	}*/
	columns, err = base.GetTableColumns(b.db, databaseName, tableName)
	if err != nil {
		return columns, uniqueKeys, err
	}
	t := &config.Table{
		TableName:            tableName,
		OriginalTableColumns: columns,
	}
	if err := base.InspectTables(b.db, databaseName, t, b.mysqlContext.TimeZone); err != nil {
		return columns, uniqueKeys, err
	}
	columns = t.OriginalTableColumns
	uniqueKeys = t.OriginalTableUniqueKeys

	return columns, uniqueKeys, nil
}

// getCandidateUniqueKeys investigates a table and returns the list of unique keys
// candidate for chunking
func (b *BinlogReader) getCandidateUniqueKeys(databaseName, tableName string) (uniqueKeys [](*mysql.UniqueKey), err error) {
	query := `
    SELECT
      COLUMNS.TABLE_SCHEMA,
      COLUMNS.TABLE_NAME,
      COLUMNS.COLUMN_NAME,
      UNIQUES.INDEX_NAME,
      UNIQUES.COLUMN_NAMES,
      UNIQUES.COUNT_COLUMN_IN_INDEX,
      COLUMNS.DATA_TYPE,
      COLUMNS.CHARACTER_SET_NAME,
			LOCATE('auto_increment', EXTRA) > 0 as is_auto_increment,
      has_nullable
    FROM INFORMATION_SCHEMA.COLUMNS INNER JOIN (
      SELECT
        TABLE_SCHEMA,
        TABLE_NAME,
        INDEX_NAME,
        COUNT(*) AS COUNT_COLUMN_IN_INDEX,
        GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX ASC) AS COLUMN_NAMES,
        SUBSTRING_INDEX(GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX ASC), ',', 1) AS FIRST_COLUMN_NAME,
        SUM(NULLABLE='YES') > 0 AS has_nullable
      FROM INFORMATION_SCHEMA.STATISTICS
      WHERE
				NON_UNIQUE=0
				AND TABLE_SCHEMA = ?
      	AND TABLE_NAME = ?
      GROUP BY TABLE_SCHEMA, TABLE_NAME, INDEX_NAME
    ) AS UNIQUES
    ON (
      COLUMNS.TABLE_SCHEMA = UNIQUES.TABLE_SCHEMA AND
      COLUMNS.TABLE_NAME = UNIQUES.TABLE_NAME AND
      COLUMNS.COLUMN_NAME = UNIQUES.FIRST_COLUMN_NAME
    )
    WHERE
      COLUMNS.TABLE_SCHEMA = ?
      AND COLUMNS.TABLE_NAME = ?
    ORDER BY
      COLUMNS.TABLE_SCHEMA, COLUMNS.TABLE_NAME,
      CASE UNIQUES.INDEX_NAME
        WHEN 'PRIMARY' THEN 0
        ELSE 1
      END,
      CASE has_nullable
        WHEN 0 THEN 0
        ELSE 1
      END,
      CASE IFNULL(CHARACTER_SET_NAME, '')
          WHEN '' THEN 0
          ELSE 1
      END,
      CASE DATA_TYPE
        WHEN 'tinyint' THEN 0
        WHEN 'smallint' THEN 1
        WHEN 'int' THEN 2
        WHEN 'bigint' THEN 3
        ELSE 100
      END,
      COUNT_COLUMN_IN_INDEX
  `
	err = sql.QueryRowsMap(b.db, query, func(m sql.RowMap) error {
		uniqueKey := &mysql.UniqueKey{
			Name:            m.GetString("INDEX_NAME"),
			Columns:         *mysql.ParseColumnList(m.GetString("COLUMN_NAMES")),
			HasNullable:     m.GetBool("has_nullable"),
			IsAutoIncrement: m.GetBool("is_auto_increment"),
		}
		uniqueKeys = append(uniqueKeys, uniqueKey)
		return nil
	}, databaseName, tableName, databaseName, tableName)
	if err != nil {
		return uniqueKeys, err
	}
	//b.logger.Debugf("mysql.reader: potential unique keys in %+v.%+v: %+v", databaseName, tableName, uniqueKeys)
	return uniqueKeys, nil
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
func resolveDDLSQL(sql string) (sqls []string, ok bool, err error) {
	stmt, err := parser.New().ParseOneStmt(sql, "", "")
	if err != nil {
		sqls = append(sqls, sql)
		return sqls, false, err
	}

	_, isDDL := stmt.(ast.DDLNode)
	if !isDDL {
		sqls = append(sqls, sql)
		return
	}

	switch v := stmt.(type) {
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
			sqls = append(sqls, s)
		}

	default:
		sqls = append(sqls, sql)
	}
	return sqls, true, nil
}

func genTableName(schema string, table string) config.Table {
	return config.Table{TableSchema: schema, TableName: table}

}

func parserDDLTableName(sql string) (config.Table, error) {
	stmt, err := parser.New().ParseOneStmt(sql, "", "")
	if err != nil {
		return config.Table{}, err
	}

	var res config.Table
	switch v := stmt.(type) {
	case *ast.CreateDatabaseStmt:
		res = genTableName(v.Name, "")
	case *ast.DropDatabaseStmt:
		res = genTableName(v.Name, "")
	case *ast.CreateIndexStmt:
		res = genTableName(v.Table.Schema.O, v.Table.Name.L)
	case *ast.CreateTableStmt:
		res = genTableName(v.Table.Schema.O, v.Table.Name.L)
	case *ast.DropIndexStmt:
		res = genTableName(v.Table.Schema.O, v.Table.Name.L)
	case *ast.TruncateTableStmt:
		res = genTableName(v.Table.Schema.O, v.Table.Name.L)
	case *ast.AlterTableStmt:
		res = genTableName(v.Table.Schema.O, v.Table.Name.L)
	case *ast.CreateUserStmt:
		res = genTableName("mysql", "user")
	case *ast.GrantStmt:
		res = genTableName("mysql", "user")
	case *ast.DropTableStmt:
		if len(v.Tables) != 1 {
			return res, fmt.Errorf("may resovle DDL sql failed")
		}
		res = genTableName(v.Tables[0].Schema.O, v.Tables[0].Name.L)
	default:
		return res, fmt.Errorf("unkown DDL type")
	}

	return res, nil
}

func (b *BinlogReader) skipQueryDDL(sql string, schema string) bool {
	t, err := parserDDLTableName(sql)
	if err != nil {
		return false
	}

	switch strings.ToLower(schema) {
	case "sys", "mysql", "information_schema", "performance_schema":
		return true
	default:
		if len(b.mysqlContext.ReplicateDoDb) > 0 {
			//if table in target Table, do this sql
			if t.TableSchema == "" {
				t.TableSchema = schema
			}

			if b.matchTable(b.mysqlContext.ReplicateDoDb, t) {
				return false
			}

			return true
		}
	}
	return false
}

func skipQueryEvent(sql string) bool {
	sql = strings.ToLower(sql)

	if strings.HasPrefix(sql, "alter user") {
		return true
	}
	if strings.HasPrefix(sql, "create user") {
		return true
	}
	if strings.HasPrefix(sql, "drop user") {
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
	case "sys", "mysql", "information_schema", "performance_schema":
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
	}
	return false
}

func (b *BinlogReader) skipRowEvent(rowsEvent *replication.RowsEvent) bool {
	switch strings.ToLower(string(rowsEvent.Table.Schema)) {
	case "actiontech_udup":
		b.currentBinlogEntry.Coordinates.OSID = mysql.ToColumnValues(rowsEvent.Rows[0]).StringColumn(0)
		return true
	case "sys", "mysql", "information_schema", "performance_schema":
		return true
	default:
		if len(b.mysqlContext.ReplicateDoDb) > 0 {
			table := strings.ToLower(string(rowsEvent.Table.Table))
			//if table in tartget Table, do this event
			for _, d := range b.mysqlContext.ReplicateDoDb {
				if b.matchString(d.TableSchema, string(rowsEvent.Table.Schema)) || d.TableSchema == "" {
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
	}
	return false
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

func (b *BinlogReader) matchTable(patternTBS []*config.DataSource, t config.Table) bool {
	for _, pdb := range patternTBS {
		if len(pdb.Tables) == 0 && t.TableSchema == pdb.TableSchema {
			return true
		}
		redb, okdb := b.ReMap[pdb.TableSchema]
		for _, ptb := range pdb.Tables {
			retb, oktb := b.ReMap[ptb.TableName]
			if oktb && okdb {
				if redb.MatchString(t.TableSchema) && retb.MatchString(t.TableName) {
					return true
				}
			}
			if oktb {
				if retb.MatchString(t.TableName) && t.TableSchema == pdb.TableSchema {
					return true
				}
			}
			if okdb {
				if redb.MatchString(t.TableSchema) && t.TableName == ptb.TableName {
					return true
				}
			}

			//create database or drop database
			if t.TableName == "" {
				if t.TableSchema == pdb.TableSchema {
					return true
				}
			}

			if ptb.TableName == t.TableName {
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
