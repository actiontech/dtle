package binlog

import (
	"bytes"
	gosql "database/sql"
	//"encoding/hex"
	"fmt"
	"log"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

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
)

// BinlogReader is a general interface whose implementations can choose their methods of reading
// a binary log file and parsing it into binlog entries
type BinlogReader struct {
	logger                   *log.Logger
	connectionConfig         *mysql.ConnectionConfig
	db                       *gosql.DB
	binlogSyncer             *replication.BinlogSyncer
	binlogStreamer           *replication.BinlogStreamer
	currentCoordinates       base.BinlogCoordinates
	currentCoordinatesMutex  *sync.Mutex
	LastAppliedRowsEventHint base.BinlogCoordinates
	MysqlContext             *config.MySQLDriverConfig
	waitGroup                *sync.WaitGroup

	EvtChan chan *BinlogEvent

	currentTx     *BinlogTx
	txCount       int
	currentFde    string
	currentSqlB64 *bytes.Buffer
	ReMap         map[string]*regexp.Regexp
}

func NewMySQLReader(cfg *config.MySQLDriverConfig, logger *log.Logger) (binlogReader *BinlogReader, err error) {
	binlogReader = &BinlogReader{
		logger:                  logger,
		currentCoordinates:      base.BinlogCoordinates{},
		currentCoordinatesMutex: &sync.Mutex{},
		binlogSyncer:            nil,
		binlogStreamer:          nil,
		MysqlContext:            cfg,
		waitGroup:               &sync.WaitGroup{},
	}

	uri := cfg.ConnectionConfig.GetDBUri()
	if binlogReader.db, _, err = sql.GetDB(uri); err != nil {
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

	binlogSyncerConfig := &replication.BinlogSyncerConfig{
		ServerID: uint32(serverId),
		Flavor:   "mysql",
		Host:     cfg.ConnectionConfig.Key.Host,
		Port:     uint16(cfg.ConnectionConfig.Key.Port),
		User:     cfg.ConnectionConfig.User,
		Password: cfg.ConnectionConfig.Password,
	}
	binlogReader.binlogSyncer = replication.NewBinlogSyncer(binlogSyncerConfig)

	return binlogReader, err
}

// ConnectBinlogStreamer
func (b *BinlogReader) ConnectBinlogStreamer(coordinates base.BinlogCoordinates) (err error) {
	if coordinates.IsEmpty() {
		return fmt.Errorf("Emptry coordinates at ConnectBinlogStreamer()")
	}

	b.currentCoordinates = coordinates
	b.logger.Printf("[DEBUG] mysql.reader: connecting binlog streamer at %+v", b.currentCoordinates)
	// Start sync with sepcified binlog file and position
	//b.binlogStreamer, err = b.binlogSyncer.StartSync(gomysql.Position{b.currentCoordinates.LogFile, uint32(b.currentCoordinates.LogPos)})
	gtidSet, err := gomysql.ParseMysqlGTIDSet(b.currentCoordinates.GtidSet)
	if err != nil {
		b.logger.Printf("[ERR] mysql.reader: err: %v", err)
	}
	b.binlogStreamer, err = b.binlogSyncer.StartSyncGTID(gtidSet)

	return err
}

func (b *BinlogReader) GetCurrentBinlogCoordinates() *base.BinlogCoordinates {
	b.currentCoordinatesMutex.Lock()
	defer b.currentCoordinatesMutex.Unlock()
	returnCoordinates := b.currentCoordinates
	return &returnCoordinates
}

// StreamEvents
func (b *BinlogReader) handleRowsEvent(ev *replication.BinlogEvent, be *BinlogEntry, entriesChannel chan<- *BinlogEntry) error {
	if b.currentCoordinates.SmallerThanOrEquals(&b.LastAppliedRowsEventHint) {
		b.logger.Printf("[DEBUG] mysql.reader: Skipping handled query at %+v", b.currentCoordinates)
		return nil
	}

	switch ev.Header.EventType {
	case replication.QUERY_EVENT:
		evt := ev.Event.(*replication.QueryEvent)
		query := string(evt.Query)

		if skipQueryEvent(query) {
			//b.logger.Printf("[WARN] skip query %s", query)
			return nil
		}

		sqls, ok, err := resolveDDLSQL(query)
		if err != nil {
			return fmt.Errorf("parse query event failed: %v", err)
		}
		if !ok {
			return nil
		}

		for _, sql := range sqls {
			if b.skipQueryDDL(sql, string(evt.Schema)) {
				//b.logger.Printf("[DEBUG] mysql.reader: skip query-ddl-sql,schema:%s,sql:%s", evt.Schema, sql)
				continue
			}

			sql, err = GenDDLSQL(sql, string(evt.Schema))
			if err != nil {
				return err
			}
			event := NewDataEvent(
				sql,
				"",
				"",
				NotDML,
				nil,
				nil,
			)
			be.Events = append(be.Events, event)
		}
		entriesChannel <- be
	case replication.WRITE_ROWS_EVENTv2:
		rowsEvent := ev.Event.(*replication.RowsEvent)
		if b.skipRowEvent(string(rowsEvent.Table.Schema), string(rowsEvent.Table.Table)) {
			//b.logger.Printf("[DEBUG] mysql.reader: skip WRITE_ROWS_EVENTv2[%s-%s]", rowsEvent.Table.Schema, rowsEvent.Table.Table)
			return nil
		}
		originalTableColumns, originalTableUniqueKeys, err := b.InspectTableColumnsAndUniqueKeys(string(rowsEvent.Table.Schema), string(rowsEvent.Table.Table))
		if err != nil {
			return err
		}
		dmlEvent := NewDataEvent(
			"",
			string(rowsEvent.Table.Schema),
			string(rowsEvent.Table.Table),
			InsertDML,
			originalTableColumns,
			originalTableUniqueKeys,
		)
		for _, row := range rowsEvent.Rows {
			//dmlEvent.NewColumnValues = mysql.ToColumnValues(row)
			dmlEvent.NewColumnValues = append(dmlEvent.NewColumnValues, mysql.ToColumnValues(row))
		}
		be.Events = append(be.Events, dmlEvent)
	case replication.UPDATE_ROWS_EVENTv2:
		rowsEvent := ev.Event.(*replication.RowsEvent)
		if b.skipRowEvent(string(rowsEvent.Table.Schema), string(rowsEvent.Table.Table)) {
			//b.logger.Printf("[DEBUG] mysql.reader: skip WRITE_ROWS_EVENTv2[%s-%s]", rowsEvent.Table.Schema, rowsEvent.Table.Table)
			return nil
		}
		originalTableColumns, originalTableUniqueKeys, err := b.InspectTableColumnsAndUniqueKeys(string(rowsEvent.Table.Schema), string(rowsEvent.Table.Table))
		if err != nil {
			return err
		}
		dmlEvent := NewDataEvent(
			"",
			string(rowsEvent.Table.Schema),
			string(rowsEvent.Table.Table),
			UpdateDML,
			originalTableColumns,
			originalTableUniqueKeys,
		)
		for i, row := range rowsEvent.Rows {
			if i%2 == 1 {
				// An update has two rows (WHERE+SET)
				// We do both at the same time
				continue
			}

			dmlEvent.WhereColumnValues = mysql.ToColumnValues(row)
			dmlEvent.NewColumnValues = append(dmlEvent.NewColumnValues, mysql.ToColumnValues(rowsEvent.Rows[i+1]))
		}
		be.Events = append(be.Events, dmlEvent)
	case replication.DELETE_ROWS_EVENTv2:
		rowsEvent := ev.Event.(*replication.RowsEvent)
		if b.skipRowEvent(string(rowsEvent.Table.Schema), string(rowsEvent.Table.Table)) {
			//b.logger.Printf("[DEBUG] mysql.reader: skip WRITE_ROWS_EVENTv2[%s-%s]", rowsEvent.Table.Schema, rowsEvent.Table.Table)
			return nil
		}
		originalTableColumns, originalTableUniqueKeys, err := b.InspectTableColumnsAndUniqueKeys(string(rowsEvent.Table.Schema), string(rowsEvent.Table.Table))
		if err != nil {
			return err
		}
		dmlEvent := NewDataEvent(
			"",
			string(rowsEvent.Table.Schema),
			string(rowsEvent.Table.Table),
			DeleteDML,
			originalTableColumns,
			originalTableUniqueKeys,
		)
		for _, row := range rowsEvent.Rows {
			dmlEvent.WhereColumnValues = mysql.ToColumnValues(row)
			be.Events = append(be.Events, dmlEvent)
		}
	case replication.XID_EVENT:
		entriesChannel <- be
	default:
		//ignore
	}

	b.LastAppliedRowsEventHint = b.currentCoordinates
	return nil
}

// StreamEvents
func (b *BinlogReader) DataStreamEvents(canStopStreaming func() bool, entriesChannel chan<- *BinlogEntry) error {
	if canStopStreaming() {
		return nil
	}
	var be *BinlogEntry
	for {
		if canStopStreaming() {
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
			b.logger.Printf("[DEBUG] mysql.reader: === %s ===", replication.EventType(ev.Header.EventType))
			b.logger.Printf("[DEBUG] mysql.reader: Date: %s", time.Unix(int64(ev.Header.Timestamp), 0).Format(gomysql.TimeFormat))
			b.logger.Printf("[DEBUG] mysql.reader: Position: %d", rotateEvent.Position)
			b.logger.Printf("[DEBUG] mysql.reader: Event size: %d", ev.Header.EventSize)
			b.logger.Printf("[DEBUG] mysql.reader: rotate to next log name: %s", rotateEvent.NextLogName)
		} else if gtidEvent, ok := ev.Event.(*replication.GTIDEvent); ok {
			func() {
				b.currentCoordinatesMutex.Lock()
				defer b.currentCoordinatesMutex.Unlock()
				u, _ := uuid.FromBytes(gtidEvent.GTID.SID)
				gtidSet, err := gomysql.ParseMysqlGTIDSet(fmt.Sprintf("%s:%d", u.String(), gtidEvent.GTID.GNO))
				if err != nil {
					b.logger.Printf("[ERR] mysql.reader: err: %v", err)
				}
				b.currentCoordinates.GtidSet = gtidSet.String()
				be = NewBinlogEntryAt(b.currentCoordinates)
			}()
		} else {
			if err := b.handleRowsEvent(ev, be, entriesChannel); err != nil {
				return err
			}
		}
	}

	return nil
}

func (b *BinlogReader) BinlogStreamEvents(txChannel chan<- *BinlogTx) error {
	//OUTER:
	for {
		if atomic.LoadInt64(&b.MysqlContext.ShutdownFlag) > 0 {
			//break OUTER
			return nil
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
			/*b.logger.Printf("[DEBUG] mysql.reader: === %s ===", replication.EventType(ev.Header.EventType))
			b.logger.Printf("[DEBUG] mysql.reader: Date: %s", time.Unix(int64(ev.Header.Timestamp), 0).Format(gomysql.TimeFormat))
			b.logger.Printf("[DEBUG] mysql.reader: Position: %d", rotateEvent.Position)
			b.logger.Printf("[DEBUG] mysql.reader: Event size: %d", ev.Header.EventSize)*/
			b.logger.Printf("[DEBUG] mysql.reader: rotate to next log name: %s", rotateEvent.NextLogName)
		} else {
			if err := b.handleBinlogRowsEvent(ev, txChannel); err != nil {
				return err
			}
		}
	}

	return nil
}

func (b *BinlogReader) handleBinlogRowsEvent(ev *replication.BinlogEvent, txChannel chan<- *BinlogTx) error {
	//bp.logger.Printf("---c:%v----:%v",bp.currentCoordinates,bp.LastAppliedRowsEventHint)
	if b.currentCoordinates.SmallerThanOrEquals(&b.LastAppliedRowsEventHint) {
		b.logger.Printf("[DEBUG] mysql.reader: skipping handled query at %+v", b.currentCoordinates)
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

		evt := ev.Event.(*replication.GTIDEvent)
		u, _ := uuid.FromBytes(evt.GTID.SID)

		b.currentTx = &BinlogTx{
			ServerId:      fmt.Sprintf("%d", ev.Header.ServerID),
			SID:           u.String(),
			GNO:           evt.GTID.GNO,
			LastCommitted: evt.GTID.LastCommitted,
			//Gtid:           fmt.Sprintf("%s:%d", u.String(), evt.GNO),
			StartEventFile: b.currentCoordinates.LogFile,
			StartEventPos:  uint32(ev.Header.LogPos) - ev.Header.EventSize,
			Impacting:      map[uint64]([]string){},
			EventSize:      uint64(ev.Header.EventSize),
			Query:          []*BinlogQuery{},
		}

		b.currentSqlB64 = new(bytes.Buffer)
		b.currentSqlB64.WriteString("BINLOG '")

		event := &BinlogEvent{
			BinlogFile: b.currentCoordinates.LogFile,
			Header:     ev.Header,
			RealPos:    uint32(ev.Header.LogPos) - ev.Header.EventSize,
			Evt:        ev.Event,
		}

		b.currentTx.events = append(b.currentTx.events, event)

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
			b.currentTx.Query = append(b.currentTx.Query, &BinlogQuery{query, NotDML})
		} else {
			// DDL or statement/mixed binlog format
			b.setImpactOnAll()
			if strings.ToUpper(query) == "COMMIT" || !b.currentTx.hasBeginQuery {
				if skipQueryEvent(query) {
					b.logger.Printf("[WARN] skip query %s", query)
					return nil
				}

				sqls, ok, err := resolveDDLSQL(query)
				if err != nil {
					return fmt.Errorf("parse query [%v] event failed: %v", query, err)
				}
				if !ok {
					b.currentTx.Query = append(b.currentTx.Query, &BinlogQuery{query, NotDML})
					b.onCommit(event, txChannel)
					return nil
				}

				for _, sql := range sqls {
					if b.skipQueryDDL(sql, string(evt.Schema)) {
						/*ulog.Logger.WithFields(logrus.Fields{
							"schema": fmt.Sprintf("%s", evt.Schema),
							"sql":    fmt.Sprintf("%s", sql),
						}).Debug("builder: skip query-ddl-sql")*/
						continue
					}

					sql, err = GenDDLSQL(sql, string(evt.Schema))
					if err != nil {
						return err
					}
					b.currentTx.Query = append(b.currentTx.Query, &BinlogQuery{sql, NotDML})
				}

				b.currentTx.events = append(b.currentTx.events, event)
				b.onCommit(event, txChannel)
			}
		}

	case replication.TABLE_MAP_EVENT:
		event := &BinlogEvent{
			BinlogFile: b.currentCoordinates.LogFile,
			Header:     ev.Header,
			RealPos:    uint32(ev.Header.LogPos) - ev.Header.EventSize,
			Evt:        ev.Event,
			RawBs:      ev.RawData,
		}

		if b.currentTx.ImpactingAll {
			return nil
		}
		evt := ev.Event.(*replication.TableMapEvent)

		if b.skipRowEvent(string(evt.Schema), string(evt.Table)) {
			/*ulog.Logger.WithFields(logrus.Fields{
				"schema": fmt.Sprintf("%s", ev.Schema),
				"table":  fmt.Sprintf("%s", ev.Table),
			}).Debug("builder: skip TableMapEvent")*/
			return nil
		}

		b.appendB64Sql(&BinlogEvent{
			BinlogFile: b.currentCoordinates.LogFile,
			Header:     ev.Header,
			RealPos:    uint32(ev.Header.LogPos) - ev.Header.EventSize,
			Evt:        ev.Event,
			RawBs:      ev.RawData,
		})
		b.currentTx.events = append(b.currentTx.events, event)

	case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		event := &BinlogEvent{
			BinlogFile: b.currentCoordinates.LogFile,
			Header:     ev.Header,
			RealPos:    uint32(ev.Header.LogPos) - ev.Header.EventSize,
			Evt:        ev.Event,
			RawBs:      ev.RawData,
		}

		if b.currentTx.ImpactingAll {
			return nil
		}
		evt := ev.Event.(*replication.RowsEvent)
		if b.skipRowEvent(string(evt.Table.Schema), string(evt.Table.Table)) {
			/*ulog.Logger.WithFields(logrus.Fields{
				"schema": fmt.Sprintf("%s", ev.Table.Schema),
				"table":  fmt.Sprintf("%s", ev.Table.Table),
			}).Debug("builder: skip RowsEvent")*/
			return nil
		}

		b.appendB64Sql(&BinlogEvent{
			BinlogFile: b.currentCoordinates.LogFile,
			Header:     ev.Header,
			RealPos:    uint32(ev.Header.LogPos) - ev.Header.EventSize,
			Evt:        ev.Event,
			RawBs:      ev.RawData,
		})
		b.currentTx.events = append(b.currentTx.events, event)
		//tb.addCount(Insert)
	case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		if b.currentTx.ImpactingAll {
			return nil
		}
		event := &BinlogEvent{
			BinlogFile: b.currentCoordinates.LogFile,
			Header:     ev.Header,
			RealPos:    uint32(ev.Header.LogPos) - ev.Header.EventSize,
			Evt:        ev.Event,
			RawBs:      ev.RawData,
		}

		evt := ev.Event.(*replication.RowsEvent)
		if b.skipRowEvent(string(evt.Table.Schema), string(evt.Table.Table)) {
			/*ulog.Logger.WithFields(logrus.Fields{
				"schema": fmt.Sprintf("%s", ev.Table.Schema),
				"table":  fmt.Sprintf("%s", ev.Table.Table),
			}).Debug("builder: skip RowsEvent")*/
			return nil
		}

		b.appendB64Sql(&BinlogEvent{
			BinlogFile: b.currentCoordinates.LogFile,
			Header:     ev.Header,
			RealPos:    uint32(ev.Header.LogPos) - ev.Header.EventSize,
			Evt:        ev.Event,
			RawBs:      ev.RawData,
		})
		b.currentTx.events = append(b.currentTx.events, event)
		//tb.addCount(Update)
	case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		if b.currentTx.ImpactingAll {
			return nil
		}
		event := &BinlogEvent{
			BinlogFile: b.currentCoordinates.LogFile,
			Header:     ev.Header,
			RealPos:    uint32(ev.Header.LogPos) - ev.Header.EventSize,
			Evt:        ev.Event,
			RawBs:      ev.RawData,
		}

		evt := ev.Event.(*replication.RowsEvent)
		if b.skipRowEvent(string(evt.Table.Schema), string(evt.Table.Table)) {
			/*ulog.Logger.WithFields(logrus.Fields{
				"schema": fmt.Sprintf("%s", ev.Table.Schema),
				"table":  fmt.Sprintf("%s", ev.Table.Table),
			}).Debug("builder: skip RowsEvent")*/
			return nil
		}

		b.appendB64Sql(&BinlogEvent{
			BinlogFile: b.currentCoordinates.LogFile,
			Header:     ev.Header,
			RealPos:    uint32(ev.Header.LogPos) - ev.Header.EventSize,
			Evt:        ev.Event,
			RawBs:      ev.RawData,
		})
		b.currentTx.events = append(b.currentTx.events, event)

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
		b.currentTx.events = append(b.currentTx.events, event)
		b.onCommit(event, txChannel)
	default:
		//ignore
	}
	b.LastAppliedRowsEventHint = b.currentCoordinates
	return nil
}

func newTxWithoutGTIDError(event *BinlogEvent) error {
	return fmt.Errorf("transaction without GTID_EVENT %v@%v", event.BinlogFile, event.RealPos)
}

func (b *BinlogReader) setImpactOnAll() {
	b.currentTx.ImpactingAll = true
	b.clearB64Sql()
}
func (b *BinlogReader) clearB64Sql() {
	b.currentSqlB64 = nil
}

var appendB64SqlBs []byte = make([]byte, 1024*1024)

func (b *BinlogReader) appendB64Sql(event *BinlogEvent) {
	n := base64.StdEncoding.EncodedLen(len(event.RawBs))
	// enlarge only
	if len(appendB64SqlBs) < n {
		appendB64SqlBs = make([]byte, n)
	}
	base64.StdEncoding.Encode(appendB64SqlBs, event.RawBs)
	b.currentSqlB64.Write(appendB64SqlBs[0:n])

	b.currentSqlB64.WriteString("\n")
}

func (b *BinlogReader) onCommit(lastEvent *BinlogEvent, txChannel chan<- *BinlogTx) {
	b.waitGroup.Add(1)
	defer b.waitGroup.Done()

	if nil != b.currentSqlB64 && !strings.HasSuffix(b.currentSqlB64.String(), "BINLOG '") {
		b.currentSqlB64.WriteString("'")
		b.currentTx.Query = append(b.currentTx.Query, &BinlogQuery{b.currentSqlB64.String(), InsertDML})
		b.currentTx.Fde = b.currentFde
	}

	b.currentTx.EndEventFile = lastEvent.BinlogFile
	b.currentTx.EndEventPos = lastEvent.RealPos

	txChannel <- b.currentTx

	/*for _, ev := range b.currentTx.events {
		if len(b.currentTx.Query) > 0 {
			b.logger.Printf("[DEBUG] mysql.reader: === %s ===", replication.EventType(ev.Header.EventType))
			b.logger.Printf("[DEBUG] mysql.reader: Date: %s", time.Unix(int64(ev.Header.Timestamp), 0).Format(gomysql.TimeFormat))
			b.logger.Printf("[DEBUG] mysql.reader: Log position: %d", ev.Header.LogPos)
			b.logger.Printf("[DEBUG] mysql.reader: Event size: %d", ev.Header.EventSize)

			switch ev.Header.EventType {
			case replication.TABLE_MAP_EVENT:
				evt := ev.Evt.(*replication.TableMapEvent)
				b.logger.Printf("[DEBUG] mysql.reader: TableID: %d", evt.TableID)
				//b.logger.Printf("TableID size: %d\n", evt.tableIDSize)
				b.logger.Printf("[DEBUG] mysql.reader: Flags: %d", evt.Flags)
				b.logger.Printf("[DEBUG] mysql.reader: Schema: %s", evt.Schema)
				b.logger.Printf("[DEBUG] mysql.reader: Table: %s", evt.Table)
				b.logger.Printf("[DEBUG] mysql.reader: Column count: %d", evt.ColumnCount)
				b.logger.Printf("[DEBUG] mysql.reader: Column type: \n%s", hex.Dump(evt.ColumnType))
				b.logger.Printf("[DEBUG] mysql.reader: NULL bitmap: \n%s", hex.Dump(evt.NullBitmap))
			case replication.GTID_EVENT:
				evt := ev.Evt.(*replication.GTIDEvent)
				u, _ := uuid.FromBytes(evt.GTID.SID)
				b.logger.Printf("[DEBUG] mysql.reader: Last committed: %d", evt.GTID.LastCommitted)
				b.logger.Printf("[DEBUG] mysql.reader: Sequence number: %d", evt.GTID.SequenceNumber)
				b.logger.Printf("[DEBUG] mysql.reader: Commit flag: %d", evt.GTID.CommitFlag)
				b.logger.Printf("[DEBUG] mysql.reader: GTID_NEXT: %s:%d", u.String(), evt.GTID.GNO)
			case replication.QUERY_EVENT:
				evt := ev.Evt.(*replication.QueryEvent)
				b.logger.Printf("[DEBUG] mysql.reader: Slave proxy ID: %d", evt.SlaveProxyID)
				b.logger.Printf("[DEBUG] mysql.reader: Execution time: %d", evt.ExecutionTime)
				b.logger.Printf("[DEBUG] mysql.reader: Error code: %d", evt.ErrorCode)
				//b.logger.Printf( "Status vars: \n%s", hex.Dump(e.StatusVars))
				b.logger.Printf("[DEBUG] mysql.reader: Schema: %s", evt.Schema)
				b.logger.Printf("[DEBUG] mysql.reader: Query: %s", evt.Query)
			case replication.WRITE_ROWS_EVENTv2, replication.UPDATE_ROWS_EVENTv2, replication.DELETE_ROWS_EVENTv2:
				evt := ev.Evt.(*replication.RowsEvent)
				b.logger.Printf("[DEBUG] mysql.reader: TableID: %d", evt.TableID)
				b.logger.Printf("[DEBUG] mysql.reader: Flags: %d", evt.Flags)
				b.logger.Printf("[DEBUG] mysql.reader: Column count: %d", evt.ColumnCount)

				b.logger.Printf("[DEBUG] mysql.reader: Values:")
				for _, rows := range evt.Rows {
					b.logger.Printf("[DEBUG] mysql.reader: --")
					for j, d := range rows {
						if _, ok := d.([]byte); ok {
							b.logger.Printf("[DEBUG] mysql.reader: %d:%q", j, d)
						} else {
							b.logger.Printf("[DEBUG] mysql.reader: %d:%#v", j, d)
						}
					}
				}
			case replication.XID_EVENT:
				evt := ev.Evt.(*replication.XIDEvent)
				b.logger.Printf("[DEBUG] mysql.reader: XID: %d", evt.XID)
			}
		}
	}*/

	b.currentTx = nil
}

func (b *BinlogReader) InspectTableColumnsAndUniqueKeys(databaseName, tableName string) (columns *mysql.ColumnList, uniqueKeys [](*mysql.UniqueKey), err error) {
	uniqueKeys, err = b.getCandidateUniqueKeys(databaseName, tableName)
	if err != nil {
		return columns, uniqueKeys, err
	}
	/*if len(uniqueKeys) == 0 {
		return columns, uniqueKeys, fmt.Errorf("No PRIMARY nor UNIQUE key found in table! Bailing out")
	}*/
	columns, err = base.GetTableColumns(b.db, databaseName, tableName)
	if err != nil {
		return columns, uniqueKeys, err
	}

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
	//b.logger.Printf("[DEBUG] mysql.reader: potential unique keys in %+v.%+v: %+v", databaseName, tableName, uniqueKeys)
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

	return fmt.Sprintf("use %s; %s", schema, sql), nil
}

// resolveDDLSQL resolve to one ddl sql
// example: drop table test.a,test2.b -> drop table test.a; drop table test2.b;
func resolveDDLSQL(sql string) (sqls []string, ok bool, err error) {
	stmt, err := parser.New().ParseOneStmt(sql, "", "")
	if err != nil {
		return nil, false, err
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
			s := fmt.Sprintf("drop table %s %s%s", ex, db, t.Name.L)
			sqls = append(sqls, s)
		}

	default:
		sqls = append(sqls, sql)
	}
	return sqls, true, nil
}

func genTableName(schema string, table string) config.TableName {
	return config.TableName{Database: schema, Name: table}

}

func parserDDLTableName(sql string) (config.TableName, error) {
	stmt, err := parser.New().ParseOneStmt(sql, "", "")
	if err != nil {
		return config.TableName{}, err
	}

	var res config.TableName
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
		b.logger.Printf("[DEBUG] mysql.tx_builder: get table failure")
		return false
	}

	switch strings.ToLower(schema) {
	case "sys", "mysql", "information_schema", "performance_schema":
		return true
	default:
		if len(b.MysqlContext.ReplicateDoDb) > 0 {
			//if table in target Table, do this sql
			if t.Database == "" {
				t.Database = schema
			}
			/*if tb.matchTable(tb.cfg.ReplicateDoDb, t.Database) {
				return false
			}*/
			if b.matchTable(b.MysqlContext.ReplicateDoDb, t) {
				return false
			}

			// if  schema in target DB, do this sql
			if b.matchDB(b.MysqlContext.ReplicateDoDb, t.Database) {
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

func (b *BinlogReader) skipRowEvent(schema string, table string) bool {
	switch strings.ToLower(schema) {
	case "sys", "mysql", "information_schema", "performance_schema":
		return true
	default:
		if len(b.MysqlContext.ReplicateDoDb) > 0 {
			table = strings.ToLower(table)
			//if table in tartget Table, do this event
			for _, d := range b.MysqlContext.ReplicateDoDb {
				if b.matchString(d.Database, schema) || d.Database == "" {
					if len(d.Table) == 0 {
						return false
					}
					for _, dt := range d.Table {
						if b.matchString(dt.Name, table) {
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
		if b.matchString(pdb.Database, a) {
			return true
		}
	}
	return false
}

func (b *BinlogReader) matchTable(patternTBS []*config.DataSource, t config.TableName) bool {
	for _, pdb := range patternTBS {
		redb, okdb := b.ReMap[pdb.Database]
		for _, ptb := range pdb.Table {
			retb, oktb := b.ReMap[ptb.Name]

			if oktb && okdb {
				if redb.MatchString(t.Database) && retb.MatchString(t.Name) {
					return true
				}
			}
			if oktb {
				if retb.MatchString(t.Name) && t.Database == pdb.Database {
					return true
				}
			}
			if okdb {
				if redb.MatchString(t.Database) && t.Name == ptb.Name {
					return true
				}
			}

			//create database or drop database
			if t.Name == "" {
				if t.Database == pdb.Database {
					return true
				}
			}

			/*if ptb == t {
				return true
			}*/
		}
	}

	return false
}

func (b *BinlogReader) Close() error {
	atomic.StoreInt64(&b.MysqlContext.ShutdownFlag, 1)
	// Historically there was a:
	b.binlogSyncer.Close()
	// here. A new go-mysql version closes the binlog syncer connection independently.
	// I will go against the sacred rules of comments and just leave this here.
	// This is the year 2017. Let's see what year these comments get deleted.
	b.waitGroup.Wait()
	return nil
}
