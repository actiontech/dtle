/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package binlog

import (
	"bytes"
	"github.com/actiontech/dtle/drivers/mysql/common"
	"github.com/pingcap/parser/format"
	"github.com/pkg/errors"
	"github.com/shirou/gopsutil/mem"
	"os"
	"path"
	"path/filepath"
	"runtime/debug"
	"sync/atomic"
	"time"

	"github.com/cznic/mathutil"

	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/gtid"
	"github.com/pingcap/dm/pkg/streamer"

	"fmt"
	"github.com/actiontech/dtle/g"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	_ "github.com/pingcap/tidb/types/parser_driver"

	"github.com/actiontech/dtle/drivers/mysql/mysql/base"
	"github.com/actiontech/dtle/drivers/mysql/mysql/mysqlconfig"
	sqle "github.com/actiontech/dtle/drivers/mysql/mysql/sqle/inspector"
	"github.com/actiontech/dtle/drivers/mysql/mysql/util"
	hclog "github.com/hashicorp/go-hclog"
	"github.com/opentracing/opentracing-go"
	dmrelay "github.com/pingcap/dm/relay"
	uuid "github.com/satori/go.uuid"
	gomysql "github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"golang.org/x/net/context"
)

const (
	StageFinishedReadingOneBinlogSwitchingToNextBinlog = "Finished reading one binlog; switching to next binlog"
	StageRegisteringSlaveOnMaster                      = "Registering slave on master"
	StageRequestingBinlogDump                          = "Requesting binlog dump"

)

// BinlogReader is a general interface whose implementations can choose their methods of reading
// a binary log file and parsing it into binlog entries
type BinlogReader struct {
	serverId         uint64
	execCtx          *common.ExecContext
	logger           hclog.Logger

	relay            dmrelay.Process
	relayCancelF     context.CancelFunc
	// for direct streaming
	binlogSyncer *replication.BinlogSyncer
	binlogStreamer0 *replication.BinlogStreamer // for the functions added by us.
	// for relay & streaming
	binlogStreamer streamer.Streamer
	// for relay
	binlogReader             *streamer.BinlogReader
	currentCoordinates       base.BinlogCoordinatesX
	currentCoordinatesMutex  *sync.Mutex
	LastAppliedRowsEventHint base.BinlogCoordinatesX
	// raw config, whose ReplicateDoDB is same as config file (empty-is-all & no dynamically created tables)
	mysqlContext         *common.MySQLDriverConfig
	currentReplicateDoDb []*common.DataSource
	// dynamic config, include all tables (implicitly assigned or dynamically created)
	tables map[string](map[string]*common.TableContext)

	currentBinlogEntry *common.BinlogEntry
	hasBeginQuery      bool
	entryContext       *common.BinlogEntryContext
	ReMap              map[string]*regexp.Regexp // This is a cache for regexp.

	shutdown     bool
	shutdownCh   chan struct{}
	shutdownLock sync.Mutex

	sqlFilter *SqlFilter

	maybeSqleContext *sqle.Context
	memory           *int64
}

type SqlFilter struct {
	NoDML       bool
	NoDMLDelete bool
	NoDMLInsert bool
	NoDMLUpdate bool

	NoDDL            bool
	NoDDLCreateTable bool
	NoDDLDropTable   bool
	NoDDLAlterTable  bool

	NoDDLAlterTableAddColumn    bool
	NoDDLAlterTableDropColumn   bool
	NoDDLAlterTableModifyColumn bool
	NoDDLAlterTableChangeColumn bool
	NoDDLAlterTableAlterColumn  bool
	NoDDLCreateSchema           bool
}

func parseSqlFilter(strs []string) (*SqlFilter, error) {
	s := &SqlFilter{}
	for i := range strs {
		switch strings.ToLower(strs[i]) {
		case "nodml":
			s.NoDML = true
		case "nodmldelete":
			s.NoDMLDelete = true
		case "nodmlinsert":
			s.NoDMLInsert = true
		case "nodmlupdate":
			s.NoDMLUpdate = true

		case "noddl":
			s.NoDDL = true
		case "noddlcreateschema":
			s.NoDDLCreateSchema = true
		case "noddlcreatetable":
			s.NoDDLCreateTable = true
		case "noddldroptable":
			s.NoDDLDropTable = true
		case "noddlaltertable":
			s.NoDDLAlterTable = true

		case "noddlaltertableaddcolumn":
			s.NoDDLAlterTableAddColumn = true
		case "noddlaltertabledropcolumn":
			s.NoDDLAlterTableDropColumn = true
		case "noddlaltertablemodifycolumn":
			s.NoDDLAlterTableModifyColumn = true
		case "noddlaltertablechangecolumn":
			s.NoDDLAlterTableChangeColumn = true
		case "noddlaltertablealtercolumn":
			s.NoDDLAlterTableAlterColumn = true

		default:
			return nil, fmt.Errorf("unknown sql filter item: %v", strs[i])
		}
	}
	return s, nil
}

func NewMySQLReader(execCtx *common.ExecContext, cfg *common.MySQLDriverConfig, logger hclog.Logger,
	replicateDoDb []*common.DataSource, sqleContext *sqle.Context, memory *int64,
) (binlogReader *BinlogReader, err error) {

	sqlFilter, err := parseSqlFilter(cfg.SqlFilter)
	if err != nil {
		return nil, err
	}

	binlogReader = &BinlogReader{
		execCtx:                 execCtx,
		logger:                  logger,
		currentCoordinates:      base.BinlogCoordinatesX{},
		currentCoordinatesMutex: &sync.Mutex{},
		mysqlContext:            cfg,
		currentReplicateDoDb:    replicateDoDb,
		ReMap:                   make(map[string]*regexp.Regexp),
		shutdownCh:              make(chan struct{}),
		tables:                  make(map[string](map[string]*common.TableContext)),
		sqlFilter:               sqlFilter,
		maybeSqleContext:        sqleContext,
		memory:                  memory,
	}

	for _, db := range replicateDoDb {
		tableMap := binlogReader.getDbTableMap(db.TableSchema)
		for _, table := range db.Tables {
			if err := binlogReader.addTableToTableMap(tableMap, table); err != nil {
				return nil, err
			}
		}
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
	binlogReader.serverId, err = strconv.ParseUint(string(bid), 10, 32)
	if err != nil {
		return nil, err
	}
	logger.Debug("got replication serverId", "id", binlogReader.serverId)

	if binlogReader.mysqlContext.BinlogRelay {
		// init when connecting
	} else {
		binlogSyncerConfig := replication.BinlogSyncerConfig{
			ServerID:       uint32(binlogReader.serverId),
			Flavor:         "mysql",
			Host:           cfg.ConnectionConfig.Host,
			Port:           uint16(cfg.ConnectionConfig.Port),
			User:           cfg.ConnectionConfig.User,
			Password:       cfg.ConnectionConfig.Password,
			RawModeEnabled: false,
			UseDecimal:     true, // my mod: use string instead of Decimal if UseDecimal = true

			MaxReconnectAttempts: 10,
			HeartbeatPeriod:      3 * time.Second,
			ReadTimeout:          12 * time.Second,

			ParseTime: false, // must be false, or gencode will complain.
		}
		binlogReader.binlogSyncer = replication.NewBinlogSyncer(binlogSyncerConfig)
	}

	binlogReader.mysqlContext.Stage = StageRegisteringSlaveOnMaster

	return binlogReader, err
}

func (b *BinlogReader) getDbTableMap(schemaName string) map[string]*common.TableContext {
	tableMap, ok := b.tables[schemaName]
	if !ok {
		tableMap = make(map[string]*common.TableContext)
		b.tables[schemaName] = tableMap
	}
	return tableMap
}
func (b *BinlogReader) addTableToTableMap(tableMap map[string]*common.TableContext, table *common.Table) error {
	whereCtx, err := common.NewWhereCtx(table.GetWhere(), table)
	if err != nil {
		b.logger.Error("Error parsing where", "where", table.GetWhere(), "err", err)
		return err
	}

	tableMap[table.TableName] = common.NewTableContext(table, whereCtx)
	return nil
}

func (b *BinlogReader) getBinlogDir() string {
	return path.Join(b.execCtx.StateDir, "binlog", b.execCtx.Subject)
}

// ConnectBinlogStreamer
func (b *BinlogReader) ConnectBinlogStreamer(coordinates base.BinlogCoordinatesX) (err error) {
	if coordinates.IsEmpty() {
		b.logger.Warn("Emptry coordinates at ConnectBinlogStreamer")
	}

	b.currentCoordinates = coordinates
	b.logger.Info("Connecting binlog streamer",
		"file", coordinates.LogFile, "pos", coordinates.LogPos, "gtid", coordinates.GtidSet)

	if b.mysqlContext.BinlogRelay {
		startPos := gomysql.Position{Pos: uint32(coordinates.LogPos), Name: coordinates.LogFile}

		dbConfig := dmrelay.DBConfig{
			Host:     b.mysqlContext.ConnectionConfig.Host,
			Port:     b.mysqlContext.ConnectionConfig.Port,
			User:     b.mysqlContext.ConnectionConfig.User,
			Password: b.mysqlContext.ConnectionConfig.Password,
		}

		// Default to replay position. Change to local relay position if the relay log exists.
		var relayGtid string = coordinates.GtidSet

		relayConfig := &dmrelay.Config{
			ServerID:   int(b.serverId),
			Flavor:     "mysql",
			From:       dbConfig,
			RelayDir:   b.getBinlogDir(),
			BinLogName: "",
			EnableGTID: true,
			BinlogGTID: relayGtid,
		}
		b.relay = dmrelay.NewRelay(relayConfig)
		err = b.relay.Init()
		if err != nil {
			return err
		}

		meta := b.relay.GetMeta()

		{ // Update relayGtid if metaGtid is not empty, aka if there is local relaylog.
			_, metaGtid := meta.GTID()
			metaGtidStr := metaGtid.String()
			if metaGtidStr != "" {
				relayGtid = metaGtidStr
			}
		}

		changed, err := meta.AdjustWithStartPos(coordinates.LogFile, relayGtid, true)
		if err != nil {
			return err
		}
		b.logger.Debug("AdjustWithStartPos.after", "changed", changed)

		processResultCh := make(chan pb.ProcessResult)
		var ctx context.Context
		ctx, b.relayCancelF = context.WithCancel(context.Background())

		go b.relay.Process(ctx, processResultCh)

		loc, err := time.LoadLocation("Local") // TODO

		brConfig := &streamer.BinlogReaderConfig{
			RelayDir: b.getBinlogDir(),
			Timezone: loc,
		}
		b.binlogReader = streamer.NewBinlogReader(brConfig)

		go func() {
			select {
			case <-b.shutdownCh:
			case pr := <-processResultCh:
				b.logger.Warn("relay.Process stopped", "isCancelled", pr.IsCanceled, "deail", string(pr.Detail))
				for _, prErr := range pr.Errors {
					b.logger.Error("relay.Process error", "err", prErr)
				}

				_ = b.binlogReader.Close()
				b.relay.Close()
			}
		}()

		targetGtid, err := gtid.ParserGTID("mysql", coordinates.GtidSet)
		if err != nil {
			return err
		}

		chWait := make(chan struct{})
		go func() {
			for {
				if b.shutdown {
					break
				}
				if b.relay.IsClosed() {
					b.logger.Info("Relay: closed. stop waiting")
					break
				}
				_, p := meta.Pos()
				_, gs := meta.GTID()

				if targetGtid.Contain(gs) {
					b.logger.Debug("Relay: keep waiting.", "pos", p, "gs", gs)
				} else {
					b.logger.Debug("Relay: stop waiting.", "pos", p, "gs", gs)
					break
				}

				time.Sleep(1 * time.Second)
			}
			close(chWait)
		}()

		<-chWait
		if b.relay.IsClosed() {
			return fmt.Errorf("relay has been closed")
		}
		b.binlogStreamer, err = b.binlogReader.StartSync(startPos)
		if err != nil {
			b.logger.Debug("err at StartSync", "err", err)
			return err
		}
	} else {
		// Start sync with sepcified binlog gtid
		//	b.logger.WithField("coordinate", coordinates).Debugf("will start sync")
		b.logger.Debug("will start sync coordinate", "coordinates", coordinates)
		if coordinates.GtidSet == "" {
			b.binlogStreamer0, err = b.binlogSyncer.StartSync(
				gomysql.Position{Name: coordinates.LogFile, Pos: uint32(coordinates.LogPos)})
			b.binlogStreamer = b.binlogStreamer0
		} else {
			gtidSet, err := gomysql.ParseMysqlGTIDSet(coordinates.GtidSet)
			if err != nil {
				b.logger.Error("ParseMysqlGTIDSet error", "err", err)
				return err
			}

			b.binlogStreamer0, err = b.binlogSyncer.StartSyncGTID(gtidSet)
			b.binlogStreamer = b.binlogStreamer0
		}
		if err != nil {
			b.logger.Debug("StartSyncGTID error", "err", err)
			return err
		}
	}
	b.mysqlContext.Stage = StageRequestingBinlogDump

	return nil
}

func (b *BinlogReader) GetCurrentBinlogCoordinates() *base.BinlogCoordinatesX {
	b.currentCoordinatesMutex.Lock()
	defer b.currentCoordinatesMutex.Unlock()
	returnCoordinates := b.currentCoordinates
	return &returnCoordinates
}

func ToColumnValuesV2(abstractValues []interface{}) *common.ColumnValues {
	return &common.ColumnValues{
		AbstractValues: abstractValues,
	}
}

// If isDDL, a sql correspond to a table item, aka len(tables) == len(sqls).
type parseDDLResult struct {
	isDDL       bool
	table       common.SchemaTable
	extraTables []common.SchemaTable
	sql         string
	ast         ast.StmtNode
	isExpand    bool
}

// StreamEvents
func (b *BinlogReader) handleEvent(ev *replication.BinlogEvent, entriesChannel chan<- *common.BinlogEntryContext) error {
	spanContext := ev.SpanContest
	trace := opentracing.GlobalTracer()
	span := trace.StartSpan("incremental binlogEvent translation to sql", opentracing.ChildOf(spanContext))
	span.SetTag("begin to translation", time.Now().Unix())
	defer span.Finish()
	if b.currentCoordinates.CompareFilePos(&b.LastAppliedRowsEventHint) <= 0 {
		b.logger.Debug("Skipping handled query", "coordinate", b.currentCoordinates)
		return nil
	}

	switch ev.Header.EventType {
	case replication.GTID_EVENT:
		evt := ev.Event.(*replication.GTIDEvent)
		b.currentCoordinatesMutex.Lock()
		// TODO this does not unlock until function return. wrap with func() if needed
		defer b.currentCoordinatesMutex.Unlock()
		u, _ := uuid.FromBytes(evt.SID)

		entry := common.NewBinlogEntry()
		entry.Coordinates.SID = u
		entry.Coordinates.GNO = evt.GNO
		entry.Coordinates.LastCommitted = evt.LastCommitted
		entry.Coordinates.SeqenceNumber = evt.SequenceNumber

		b.currentBinlogEntry = entry
		b.hasBeginQuery = false
		b.entryContext = &common.BinlogEntryContext{
			Entry:       b.currentBinlogEntry,
			SpanContext: nil,
			TableItems:  nil,
			OriginalSize: 1, // GroupMaxSize is default to 1 and we send on EntriesSize >= GroupMaxSize
		}
	case replication.QUERY_EVENT:
		evt := ev.Event.(*replication.QueryEvent)
		query := string(evt.Query)

		if evt.ErrorCode != 0 {
			b.logger.Error("DTLE_BUG: found query_event with error code, which is not handled.",
				"ErrorCode", evt.ErrorCode, "query", query)
		}
		currentSchema := string(evt.Schema)

		b.logger.Debug("query event", "schema", currentSchema, "query", query)

		upperQuery := strings.ToUpper(query)
		if upperQuery == "BEGIN" {
			b.hasBeginQuery = true
		} else {
			if upperQuery == "COMMIT" || !b.hasBeginQuery {
				osid, err := checkDtleQuery(query)
				if err != nil {
					return errors.Wrap(err, "checkDtleQuery")
				}
				b.logger.Debug("query osid", "osid", osid)
				b.currentBinlogEntry.Coordinates.OSID = osid

				if osid == "" {
					query = b.setDtleQuery(query, upperQuery)
				}

				ddlInfo, err := resolveDDLSQL(currentSchema, query, b.skipQueryDDL)

				var skipExpandSyntax bool
				if b.mysqlContext.ExpandSyntaxSupport {
					skipExpandSyntax = false
				} else {
					skipExpandSyntax = isExpandSyntaxQuery(query) || ddlInfo.isExpand
				}

				currentSchemaRename := currentSchema
				schema := b.findCurrentSchema(currentSchema)
				if schema != nil && schema.TableSchemaRename != "" {
					currentSchemaRename = schema.TableSchemaRename
				}

				if skipExpandSyntax || err != nil || !ddlInfo.isDDL {
					if err != nil {
						b.logger.Warn("Parse query event failed. will execute", "query", query, "err", err, "gno", b.currentBinlogEntry.Coordinates.GNO)
					} else if !ddlInfo.isDDL {
						b.logger.Debug("mysql.reader: QueryEvent is not a DDL", "query", query, "gno", b.currentBinlogEntry.Coordinates.GNO)
					}
					if skipExpandSyntax {
						b.logger.Warn("skip query", "query", query, "gno", b.currentBinlogEntry.Coordinates.GNO)
					} else {
						event := common.NewQueryEvent(
							currentSchemaRename,
							query,
							common.NotDML,
							ev.Header.Timestamp,
						)

						b.currentBinlogEntry.Events = append(b.currentBinlogEntry.Events, event)
						b.entryContext.SpanContext = span.Context()
						b.entryContext.OriginalSize += len(ev.RawData)
					}
					b.sendEntry(entriesChannel)
					b.LastAppliedRowsEventHint = b.currentCoordinates
					return nil
				}

				skipEvent := false

				b.sqleExecDDL(currentSchema, ddlInfo.ast)

				if b.sqlFilter.NoDDL {
					skipEvent = true
				}

				sql := ddlInfo.sql
				if sql != "" {
					realSchema := common.StringElse(ddlInfo.table.Schema, currentSchema)
					tableName := ddlInfo.table.Table
					err = b.updateCurrentReplicateDoDb(realSchema, tableName)
					if err != nil {
						return errors.Wrap(err, "updateCurrentReplicateDoDb")
					}

					if b.skipQueryDDL(realSchema, tableName) {
						b.logger.Info("Skip QueryEvent", "currentSchema", currentSchema, "sql", sql,
							"realSchema", realSchema, "tableName", tableName, "gno", b.currentBinlogEntry.Coordinates.GNO)
					} else {
						if realSchema != currentSchema {
							schema = b.findCurrentSchema(realSchema)
						}
						table := b.findCurrentTable(schema, tableName)

						skipEvent = skipBySqlFilter(ddlInfo.ast, b.sqlFilter)
						switch realAst := ddlInfo.ast.(type) {
						case *ast.CreateDatabaseStmt:
							b.sqleAfterCreateSchema(ddlInfo.table.Schema)
						case *ast.CreateTableStmt:
							b.logger.Debug("ddl is create table")
							err := b.updateTableMeta(table, realSchema, tableName)
							if err != nil {
								return err
							}
						//case *ast.DropTableStmt:
						case *ast.AlterTableStmt:
							b.logger.Debug("ddl is alter table.", "specs", realAst.Specs)

							fromTable := table
							tableNameX := tableName

							for iSpec := range realAst.Specs {
								switch realAst.Specs[iSpec].Tp {
								case ast.AlterTableRenameTable:
									fromTable = nil
									tableNameX = realAst.Specs[iSpec].NewTable.Name.O
								default:
									// do nothing
								}
							}
							err := b.updateTableMeta(fromTable, realSchema, tableNameX)
							if err != nil {
								return err
							}
						case *ast.RenameTableStmt:
							for _, tt := range realAst.TableToTables {
								newSchemaName := common.StringElse(tt.NewTable.Schema.O, currentSchema)
								tableName := tt.NewTable.Name.O
								b.logger.Debug("updating meta for rename table", "newSchema", newSchemaName,
									"newTable", tableName)
								if !b.skipQueryDDL(newSchemaName, tableName) {
									err := b.updateTableMeta(nil, newSchemaName, tableName)
									if err != nil {
										return err
									}
								} else {
									b.logger.Debug("not updating meta for rename table", "newSchema", newSchemaName,
										"newTable", tableName)
								}
							}
						}

						if schema != nil && schema.TableSchemaRename != "" {
							ddlInfo.table.Schema = schema.TableSchemaRename
							realSchema = schema.TableSchemaRename
						} else {
							// schema == nil means it is not explicit in ReplicateDoDb, thus no renaming
							// or schema.TableSchemaRename == "" means no renaming
						}

						if table != nil && table.TableRename != "" {
							ddlInfo.table.Table = table.TableRename
						}
						// mapping
						schemaRenameMap, schemaNameToTablesRenameMap := b.generateRenameMaps()
						if len(schemaRenameMap) > 0 || len(schemaNameToTablesRenameMap) > 0 {
							sql, err = b.loadMapping(sql, currentSchema, schemaRenameMap, schemaNameToTablesRenameMap, ddlInfo.ast)
							if nil != err {
								return fmt.Errorf("ddl mapping failed: %v", err)
							}
						}

						if skipEvent {
							b.logger.Debug("skipped a ddl event.", "query", query)
						} else {
							if realSchema == "" || ddlInfo.table.Table == "" {
								b.logger.Info("NewQueryEventAffectTable. found empty schema or table.",
									"schema", realSchema, "table", ddlInfo.table.Table, "query", sql)
							}

							event := common.NewQueryEventAffectTable(
								currentSchemaRename,
								sql,
								common.NotDML,
								ddlInfo.table,
								ev.Header.Timestamp,
							)
							if table != nil {
								tableBs, err := common.GobEncode(table)
								if err != nil {
									return errors.Wrap(err, "GobEncode(table)")
								}
								event.Table = tableBs
							}
							b.currentBinlogEntry.Events = append(b.currentBinlogEntry.Events, event)
						}
					}
				}
				b.entryContext.SpanContext = span.Context()
				b.entryContext.OriginalSize += len(ev.RawData)
				b.sendEntry(entriesChannel)
				b.LastAppliedRowsEventHint = b.currentCoordinates
			}
		}
	case replication.XID_EVENT:
		evt := ev.Event.(*replication.XIDEvent)
		b.entryContext.SpanContext = span.Context()
		b.currentCoordinates.LogPos = int64(ev.Header.LogPos)
		b.currentCoordinates.GtidSet = evt.GSet.String()
		// TODO is the pos the start or the end of a event?
		// pos if which event should be use? Do we need +1?
		b.currentBinlogEntry.Coordinates.LogPos = b.currentCoordinates.LogPos

		b.sendEntry(entriesChannel)
		b.LastAppliedRowsEventHint = b.currentCoordinates
	default:
		if rowsEvent, ok := ev.Event.(*replication.RowsEvent); ok {
			schemaName := string(rowsEvent.Table.Schema)
			tableName := string(rowsEvent.Table.Table)

			dml := common.ToEventDML(ev.Header.EventType)
			skip, table := b.skipRowEvent(rowsEvent, dml)
			if skip {
				b.logger.Debug("skip rowsEvent", "schema", schemaName, "table", tableName,
					"gno", b.currentBinlogEntry.Coordinates.GNO)
				return nil
			}

			if b.sqlFilter.NoDML ||
				(b.sqlFilter.NoDMLDelete && dml == common.DeleteDML) ||
				(b.sqlFilter.NoDMLInsert && dml == common.InsertDML) ||
				(b.sqlFilter.NoDMLUpdate && dml == common.UpdateDML) {

				b.logger.Debug("skipped_a_dml_event.", "type", dml, "schema", schemaName, "table", tableName)
				return nil
			}

			if dml == common.NotDML {
				return fmt.Errorf("unknown DML type: %s", ev.Header.EventType.String())
			}
			dmlEvent := common.NewDataEvent(
				schemaName,
				tableName,
				dml,
				rowsEvent.ColumnCount,
				ev.Header.Timestamp,
			)
			dmlEvent.LogPos = int64(ev.Header.LogPos - ev.Header.EventSize)

			/*originalTableColumns, _, err := b.InspectTableColumnsAndUniqueKeys(string(rowsEvent.Table.Schema), string(rowsEvent.Table.Table))
			if err != nil {
				return err
			}
			dmlEvent.OriginalTableColumns = originalTableColumns*/

			// It is hard to calculate exact row size. We use estimation.
			avgRowSize := len(ev.RawData) / len(rowsEvent.Rows)

			for i, row := range rowsEvent.Rows {
				//for _, col := range row {
				//	b.logger.Debug("*** column type", "col", col, "type", hclog.Fmt("%T", col))
				//}
				b.logger.Trace("a row", "value", row[:mathutil.Min(len(row), g.LONG_LOG_LIMIT)])
				if dml == common.UpdateDML && i%2 == 1 {
					// An update has two rows (WHERE+SET)
					// We do both at the same time
					continue
				}
				switch dml {
				case common.InsertDML:
					{
						dmlEvent.NewColumnValues = ToColumnValuesV2(row)
					}
				case common.UpdateDML:
					{
						dmlEvent.WhereColumnValues = ToColumnValuesV2(row)
						dmlEvent.NewColumnValues = ToColumnValuesV2(rowsEvent.Rows[i+1])
					}
				case common.DeleteDML:
					{
						dmlEvent.WhereColumnValues = ToColumnValuesV2(row)
					}
				}

				//b.logger.Debug("event before row", "values", dmlEvent.WhereColumnValues)
				//b.logger.Debug("event after row", "values", dmlEvent.NewColumnValues)
				whereTrue := true
				var err error
				if table != nil && !table.WhereCtx.IsDefault {
					switch dml {
					case common.InsertDML:
						whereTrue, err = table.WhereTrue(dmlEvent.NewColumnValues)
						if err != nil {
							return err
						}
					case common.UpdateDML:
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
					case common.DeleteDML:
						whereTrue, err = table.WhereTrue(dmlEvent.WhereColumnValues)
						if err != nil {
							return err
						}
					}
				}
				if table != nil && table.Table.TableRename != "" {
					dmlEvent.TableName = table.Table.TableRename
					b.logger.Debug("dml table mapping", "from", dmlEvent.TableName, "to", table.Table.TableRename)
				}
				schema := b.findCurrentSchema(schemaName)
				if schema != nil && schema.TableSchemaRename != "" {
					b.logger.Debug("dml schema mapping", "from", dmlEvent.DatabaseName, "to", schema.TableSchemaRename)
					dmlEvent.DatabaseName = schema.TableSchemaRename
				}

				if table != nil && !table.DefChangedSent {
					b.logger.Debug("send table structure", "schema", schemaName, "table", tableName)
					if table.Table == nil {
						b.logger.Warn("DTLE_BUG binlog_reader: table.Table is nil",
							"schema", schemaName, "table", tableName)
					} else {
						tableBs, err := common.GobEncode(table.Table)
						if err != nil {
							return errors.Wrap(err, "GobEncode(table)")
						}
						dmlEvent.Table = tableBs
					}

					table.DefChangedSent = true
				}

				if whereTrue {
					if dmlEvent.WhereColumnValues != nil {
						b.entryContext.OriginalSize += avgRowSize
					}
					if dmlEvent.NewColumnValues != nil {
						b.entryContext.OriginalSize += avgRowSize
					}
					// The channel will do the throttling. Whoever is reding from the channel
					// decides whether action is taken sycnhronously (meaning we wait before
					// next iteration) or asynchronously (we keep pushing more events)
					// In reality, reads will be synchronous
					if table != nil && len(table.Table.ColumnMap) > 0 {
						if dmlEvent.NewColumnValues != nil {
							newRow := make([]interface{}, len(table.Table.ColumnMap))
							for i := range table.Table.ColumnMap {
								idx := table.Table.ColumnMap[i]
								newRow[i] = dmlEvent.NewColumnValues.AbstractValues[idx]
							}
							dmlEvent.NewColumnValues.AbstractValues = newRow
						}

						if dmlEvent.WhereColumnValues != nil {
							newRow := make([]interface{}, len(table.Table.ColumnMap))
							for i := range table.Table.ColumnMap {
								idx := table.Table.ColumnMap[i]
								newRow[i] = dmlEvent.WhereColumnValues.AbstractValues[idx]
							}
							dmlEvent.WhereColumnValues.AbstractValues = newRow
						}
					}
					b.currentBinlogEntry.Events = append(b.currentBinlogEntry.Events, dmlEvent)
				} else {
					b.logger.Debug("event has not passed 'where'")
				}
			}
			return nil
		}
	}
	return nil
}

const (
	dtleQueryPrefix = "/*dtle_gtid1 "
	dtleQuerySuffix = " dtle_gtid*/"
)
func checkDtleQuery(query string) (string, error) {

	start := strings.Index(query, dtleQueryPrefix)
	if start == -1 {
		return "", nil
	}
	start += len(dtleQueryPrefix)

	end := strings.Index(query, dtleQuerySuffix)
	if end == -1 {
		return "", fmt.Errorf("incomplete dtle_gtid for query %v", query)
	}
	if end < start {
		return "", fmt.Errorf("bad dtle_gtid for query %v", query)
	}

	dtleItem := query[start:end]
	ss := strings.Split(dtleItem, " ")
	if len(ss) != 3 {
		return "", fmt.Errorf("bad dtle_gtid splitted for query %v", query)
	}

	return ss[1], nil
}
func (b *BinlogReader) setDtleQuery(query string, upperQuery string) string {
	uuidStr := uuid.UUID(b.currentBinlogEntry.Coordinates.SID).String()
	tag := fmt.Sprintf("/*dtle_gtid1 %v %v %v dtle_gtid*/", b.execCtx.Subject, uuidStr, b.currentBinlogEntry.Coordinates.GNO)

	if strings.HasPrefix(upperQuery, "CREATE DEFINER=") {
		if strings.HasSuffix(upperQuery, "END") {
			return fmt.Sprintf("%v %v END", query[:len(query)-3], tag)
		}
	}

	return fmt.Sprintf("%v %v", query, tag)
}

func (b *BinlogReader) sendEntry(entriesChannel chan<- *common.BinlogEntryContext) {
	b.logger.Debug("sendEntry", "gno", b.currentBinlogEntry.Coordinates.GNO)
	atomic.AddInt64(b.memory, int64(b.entryContext.Entry.Size()))
	entriesChannel <- b.entryContext
}

func (b *BinlogReader) loadMapping(sql, currentSchema string,
	schemasRenameMap map[string]string /* map[oldSchemaName]newSchemaName */, oldSchemaNameToTablesRenameMap map[string]map[string]string, /* map[oldSchemaName]map[oldTableName]newTableName */
	stmt ast.StmtNode) (string, error) {

	logMapping := func(oldName, newName, mappingType string) {
		msg := fmt.Sprintf("ddl %s mapping", mappingType)
		b.logger.Debug(msg, "from", oldName, "to", newName)
	}

	renameAstTableFn := func(table *ast.TableName) {
		table.Schema.O = common.StringElse(table.Schema.O, currentSchema)
		newSchemaName := schemasRenameMap[table.Schema.O]
		tableNameMap := oldSchemaNameToTablesRenameMap[table.Schema.O]
		newTableName := tableNameMap[table.Name.O]

		if newSchemaName != "" {
			logMapping(table.Schema.O, newSchemaName, "schema")
			table.Schema.O = newSchemaName
			table.Schema.L = strings.ToLower(table.Schema.O)
		}

		if newTableName != "" {
			logMapping(table.Name.O, newTableName, "table")
			table.Name.O = newTableName
			table.Name.L = strings.ToLower(newTableName)
		}

	}

	renameSchemaFn := func(oldSchema *string) {
		newSchemaName := schemasRenameMap[*oldSchema]
		if newSchemaName != "" {
			logMapping(*oldSchema, newSchemaName, "schema")
			*oldSchema = newSchemaName
		}
	}

	switch v := stmt.(type) {
	case *ast.DropTableStmt:
		for _, table := range v.Tables {
			renameAstTableFn(table)
		}
	case *ast.RenameTableStmt:
		for _, tt := range v.TableToTables {
			renameAstTableFn(tt.OldTable)
			renameAstTableFn(tt.NewTable)
		}
	case *ast.CreateDatabaseStmt:
		renameSchemaFn(&v.Name)
	case *ast.DropDatabaseStmt:
		renameSchemaFn(&v.Name)
	case *ast.AlterDatabaseStmt:
		renameSchemaFn(&v.Name)
	case *ast.CreateIndexStmt:
		renameAstTableFn(v.Table)
	case *ast.DropIndexStmt:
		renameAstTableFn(v.Table)
	case *ast.TruncateTableStmt:
		renameAstTableFn(v.Table)
	case *ast.CreateTableStmt:
		renameAstTableFn(v.Table)
	case *ast.AlterTableStmt:
		renameAstTableFn(v.Table)
		for _, spec := range v.Specs {
			if nil != spec.NewTable {
				renameAstTableFn(spec.NewTable)
			}
		}
	case *ast.FlushStmt:
		if v.Tp != ast.FlushTables {
			b.logger.Debug("skip mapping ddl", "sql", sql)
			return sql, nil
		}

		for _, table := range v.Tables {
			renameAstTableFn(table)
		}
	default:
		b.logger.Debug("skip mapping ddl", "sql", sql)
		return sql, nil
	}

	bs := bytes.NewBuffer(nil)
	err := stmt.Restore(&format.RestoreCtx{
		Flags: format.DefaultRestoreFlags,
		In:    bs,
	})
	if err != nil {
		return "", fmt.Errorf("restore stmt failed: %v", err)
	}
	return bs.String(), nil
}

// StreamEvents
func (b *BinlogReader) DataStreamEvents(entriesChannel chan<- *common.BinlogEntryContext) error {
	lowMemory := false // TODO atomicity?
	go func() {
		t := time.NewTicker(1 * time.Second)
		i := 0
		for !b.shutdown {
			<-t.C
			memory, err := mem.VirtualMemory()
			if err != nil {
				lowMemory = false
			} else {
				if (float64(memory.Available)/float64(memory.Total) < 0.2) && (memory.Available < 1*1024*1024*1024) {
					if i%30 == 0 { // suppress log
						b.logger.Warn("memory is less than 20% and 1GiB. pause parsing binlog for 1s",
							"available", memory.Available, "total", memory.Total)
					} else {
						b.logger.Debug("memory is less than 20% and 1GiB. pause parsing binlog for 1s",
							"available", memory.Available, "total", memory.Total)
					}
					lowMemory = true
					debug.FreeOSMemory()
					i += 1
				} else {
					lowMemory = false
					if i != 0 {
						b.logger.Info("memory is greater than 20%. continue parsing binlog",
							"available", memory.Available, "total", memory.Total)
					}
					i = 0
				}
			}
		}
		t.Stop()
	}()

	for {
		// Check for shutdown
		if b.shutdown {
			break
		}

		trace := opentracing.GlobalTracer()
		ev, err := b.binlogStreamer.GetEvent(context.Background())
		if err != nil {
			b.logger.Error("error GetEvent.", "err", err)
			return err
		}
		for lowMemory {
			time.Sleep(900 * time.Millisecond)
		}
		if b.shutdown {
			return nil
		}

		spanContext := ev.SpanContest
		span := trace.StartSpan("DataStreamEvents()  get binlogEvent  from mysql-go ", opentracing.FollowsFrom(spanContext))
		span.SetTag("time", time.Now().Unix())
		ev.SpanContest = span.Context()

		if ev.Header.EventType == replication.HEARTBEAT_EVENT {
			continue
		}
		//ev.Dump(os.Stdout)

		func() {
			b.currentCoordinatesMutex.Lock()
			defer b.currentCoordinatesMutex.Unlock()
			b.currentCoordinates.LogPos = int64(ev.Header.LogPos)
		}()

		if ev.Header.EventType == replication.ROTATE_EVENT {
			if rotateEvent, ok := ev.Event.(*replication.RotateEvent); ok {
				func() {
					b.currentCoordinatesMutex.Lock()
					defer b.currentCoordinatesMutex.Unlock()
					b.currentCoordinates.LogFile = string(rotateEvent.NextLogName)
				}()
				b.mysqlContext.Stage = StageFinishedReadingOneBinlogSwitchingToNextBinlog
				b.logger.Info("Rotate to next binlog", "name", string(rotateEvent.NextLogName))
			} else {
				b.logger.Warn("fake rotate_event.")
			}
		} else {
			if err := b.handleEvent(ev, entriesChannel); err != nil {
				return err
			}
		}
		span.Finish()
	}

	return nil
}

// resolveDDLSQL resolve to one ddl sql
// example: drop table test.a,test2.b -> drop table test.a; drop table test2.b;
//
// schemaTables is the schema.table that the query has invalidated. For err or non-DDL, it is nil.
// For DDL, it size equals len(sqls).
func resolveDDLSQL(currentSchema string, sql string,
	skipFunc func(schema string, tableName string) bool) (result parseDDLResult, err error) {

	result.sql = sql

	stmt, err := parser.New().ParseOneStmt(sql, "", "")
	if err != nil {
		return result, err
	}
	result.ast = stmt

	_, result.isDDL = stmt.(ast.DDLNode)
	if !result.isDDL {
		return result, nil
	}

	setTable := func(schema string, table string) {
		result.table = common.SchemaTable{Schema: schema, Table: table}
	}

	switch v := stmt.(type) {
	case *ast.CreateDatabaseStmt:
		setTable(v.Name, "")
	case *ast.DropDatabaseStmt:
		setTable(v.Name, "")
	case *ast.AlterDatabaseStmt:
		setTable(v.Name, "")
	case *ast.CreateIndexStmt:
		setTable(v.Table.Schema.O, v.Table.Name.O)
	case *ast.DropIndexStmt:
		setTable(v.Table.Schema.O, v.Table.Name.O)
	case *ast.TruncateTableStmt:
		setTable(v.Table.Schema.O, v.Table.Name.O)
	case *ast.CreateTableStmt:
		setTable(v.Table.Schema.O, v.Table.Name.O)
	case *ast.AlterTableStmt:
		setTable(v.Table.Schema.O, v.Table.Name.O)
	case *ast.RevokeStmt, *ast.RevokeRoleStmt:
		result.isExpand = true
	case *ast.SetPwdStmt:
		result.isExpand = true
	case *ast.FlushStmt:
		switch v.Tp {
		case ast.FlushPrivileges:
			result.isExpand = true
		}
	case *ast.DropTableStmt:
		var newTables []*ast.TableName
		for i, t := range v.Tables {
			schema := common.StringElse(t.Schema.O, currentSchema)
			if !skipFunc(schema, t.Name.O) {
				if i == 0 {
					setTable(t.Schema.O, t.Name.O)
				} else {
					result.extraTables = append(result.extraTables,
						common.SchemaTable{Schema: t.Schema.O, Table: t.Name.O})
				}
				newTables = append(newTables, t)
			}
		}
		v.Tables = newTables

		if len(v.Tables) == 0 {
			result.sql = "drop table if exists dtle-dummy-never-exists.dtle-dummy-never-exists"
			setTable("dtle-dummy-never-exists", "dtle-dummy-never-exists")
		} else {
			bs := bytes.NewBuffer(nil)
			r := &format.RestoreCtx{
				Flags:     format.DefaultRestoreFlags,
				In:        bs,
				JoinLevel: 0,
			}
			err = v.Restore(r)
			if err != nil {
				return result, err
			}
			result.sql = bs.String()
		}
	case *ast.CreateUserStmt, *ast.GrantStmt, *ast.DropUserStmt, *ast.AlterUserStmt:
		setTable("mysql", "user")
		result.isExpand = true
	case *ast.RenameTableStmt:
		setTable(v.OldTable.Schema.O, v.OldTable.Name.O)
		// TODO handle extra tables in v.TableToTables[1:]
	default:
		return result, fmt.Errorf("unknown DDL type")
	}

	return result, nil
}

func (b *BinlogReader) skipQueryDDL(schema string, tableName string) bool {
	switch strings.ToLower(schema) {
	case "mysql":
		if b.mysqlContext.ExpandSyntaxSupport {
			return false
		} else {
			return true
		}
	case "sys", "information_schema", "performance_schema", g.DtleSchemaName:
		return true
	default:
		return b.skipSchemaOrTable(schema, tableName)
	}
}

func (b *BinlogReader) skipSchemaOrTable(schemaName, tableName string) bool {
	if len(b.mysqlContext.ReplicateDoDb) > 0 { // replicate specified schema
		schemaConfig := b.findSchemaConfig(b.mysqlContext.ReplicateDoDb, schemaName)
		if nil == schemaConfig {
			return true
		}
		if tableName == "" { // match schema
			return false
		}

		if len(schemaConfig.Tables) > 0 { // skip the table if it isn't specified in replicateDoDb config
			return nil == b.findTableConfig(schemaConfig, tableName)
		} else { // ignore table specified in ReplicateIgnoreDb if there is no table specified in replicateDoDb config
			return b.matchTable(b.mysqlContext.ReplicateIgnoreDb, schemaName, tableName)
		}
	} else if len(b.mysqlContext.ReplicateIgnoreDb) > 0 { // replicate all schemas and tables except schema and table specified in ReplicateIgnoreDb
		return b.matchTable(b.mysqlContext.ReplicateIgnoreDb, schemaName, tableName)
	}
	return false
}

func isExpandSyntaxQuery(sql string) bool {
	sql = strings.ToLower(sql)

	// TODO mod pingcap/parser to support these DDLs
	//  and use ast instead of string-matching
	if strings.HasPrefix(sql, "create function") {
		return true
	}
	if strings.HasPrefix(sql, "create procedure") {
		return true
	}
	if strings.HasPrefix(sql, "rename user") {
		return true
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

func (b *BinlogReader) skipRowEvent(rowsEvent *replication.RowsEvent, dml int8) (bool, *common.TableContext) {
	tableOrigin := string(rowsEvent.Table.Table)
	tableLower := strings.ToLower(tableOrigin)
	switch strings.ToLower(string(rowsEvent.Table.Schema)) {
	case g.DtleSchemaName:
		if  strings.HasPrefix(strings.ToLower(string(rowsEvent.Table.Table)), g.GtidExecutedTablePrefix) {
			// cases: 1. delete for compaction; 2. insert for compaction (gtid interval); 3. normal insert for tx (single gtid)
			// We make no special treat for case 2. That tx has only one insert, which should be ignored.
			if dml == common.InsertDML {
				if len(rowsEvent.Rows) == 1 {
					sidValue := rowsEvent.Rows[0][1]
					sidByte, ok := sidValue.(string)
					if !ok {
						b.logger.Error("cycle-prevention: unrecognized gtid_executed table sid type",
							"type", hclog.Fmt("%T", sidValue))
					} else {
						sid, err := uuid.FromBytes([]byte(sidByte))
						if err != nil {
							b.logger.Error("cycle-prevention: cannot convert sid to uuid", "err", err)
						} else {
							b.currentBinlogEntry.Coordinates.OSID = sid.String()
							b.logger.Debug("found an osid", "osid", b.currentBinlogEntry.Coordinates.OSID)
						}
					}
				}
				// If OSID is target mysql SID, skip applying the binlogEntry.
				// - Plan B: skip sending at applier: unnecessary sending
				// - Plan A: skip sending at extractor: currently extractor does not know target mysql SID
			}
		}
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
		//if table in tartget Table, do this event
		for schemaName, tableMap := range b.tables {
			if b.matchString(schemaName, string(rowsEvent.Table.Schema)) || schemaName == "" {
				if len(tableMap) == 0 {
					return false, nil // TODO not skipping but TableContext
				}
				for tableName, tableCtx := range tableMap {
					if b.matchString(tableName, tableOrigin) {
						return false, tableCtx
					}
				}
			}
		}
		return true, nil
	}
}

func (b *BinlogReader) matchString(pattern string, t string) bool {
	/*if re, ok := b.ReMap[pattern]; ok {
		return re.MatchString(t)
	}*/
	return pattern == t
}

func (b *BinlogReader) matchTable(patternTBS []*common.DataSource, schemaName string, tableName string) bool {
	if schemaName == "" {
		return false
	}

	for _, pdb := range patternTBS {
		if pdb.TableSchema == "" && pdb.TableSchemaRegex == "" { // invalid pattern
			continue
		}
		if pdb.TableSchema != "" && pdb.TableSchema != schemaName {
			continue
		}
		if pdb.TableSchemaRegex != "" {
			reg := regexp.MustCompile(pdb.TableSchemaRegex)
			if !reg.MatchString(schemaName) {
				continue
			}
		}

		if tableName == "" || len(pdb.Tables) == 0 { // match all tables within the db if length of pdb.Tables is 0
			return true
		}

		for _, ptb := range pdb.Tables {
			if ptb.TableName == "" && ptb.TableRegex == "" { // invalid pattern
				continue
			}
			if ptb.TableName != "" && ptb.TableName == tableName {
				return true
			}
			if ptb.TableRegex != "" {
				reg := regexp.MustCompile(ptb.TableRegex)
				if reg.MatchString(tableName) {
					return true
				}
			}
		}
		return false
	}
	return false
}

func (b *BinlogReader) Close() error {
	b.logger.Debug("BinlogReader.Close")
	b.shutdownLock.Lock()
	defer b.shutdownLock.Unlock()

	if b.shutdown {
		return nil
	}
	b.shutdown = true
	close(b.shutdownCh)

	// Historically there was a:
	if b.mysqlContext.BinlogRelay {
		b.binlogReader.Close()
		b.relayCancelF()
		b.relay.Close()
	} else {
		b.binlogSyncer.Close()
	}
	// here. A new go-mysql version closes the binlog syncer connection independently.
	// I will go against the sacred rules of comments and just leave this here.
	// This is the year 2017. Let's see what year these comments get deleted.
	return nil
}
func (b *BinlogReader) updateTableMeta(table *common.Table, realSchema string, tableName string) error {
	var err error

	if b.maybeSqleContext == nil {
		b.logger.Debug("ignore updating table meta", "schema", realSchema, "table", tableName)
		return nil
	}

	columns, err := base.GetTableColumnsSqle(b.maybeSqleContext, realSchema, tableName)
	if err != nil {
		b.logger.Warn("updateTableMeta: cannot get table info after ddl.", "err", err, "realSchema", realSchema, "tableName", tableName)
		return err
	}
	b.logger.Debug("binlog_reader. new columns.",
		"schema", realSchema, "table", tableName, "columns", columns.String())

	//var table *config.Table

	if table == nil {
		// a new table (it might be in all db copy since it is not ignored).
		table = common.NewTable(realSchema, tableName)
		table.TableType = "BASE TABLE"
	}
	table.OriginalTableColumns = columns
	table.ColumnMap = mysqlconfig.BuildColumnMapIndex(table.ColumnMapFrom, table.OriginalTableColumns.Ordinals)
	tableMap := b.getDbTableMap(realSchema)
	err = b.addTableToTableMap(tableMap, table)
	if err != nil {
		b.logger.Error("failed to make table context", "err", err)
		return err
	}

	return nil
}

func (b *BinlogReader) updateCurrentReplicateDoDb(schemaName string, tableName string) error {
	if "" == schemaName {
		return fmt.Errorf("schema name should not be empty")
	}

	var currentSchemaReplConfig *common.DataSource
	currentSchema := b.findCurrentSchema(schemaName)
	currentSchemaReplConfig = b.findSchemaConfig(b.mysqlContext.ReplicateDoDb, schemaName)
	if nil == currentSchema { // update current schema
		if len(b.mysqlContext.ReplicateDoDb) > 0 {
			if nil == currentSchemaReplConfig {
				//  the schema doesn't need to be replicated
				return nil
			}

			schemaRename := ""
			schemaRenameRegex := currentSchemaReplConfig.TableSchemaRename
			if currentSchemaReplConfig.TableSchema != "" { // match currentSchemaReplConfig.TableSchema and currentSchemaReplConfig.TableSchemaRename
				schemaRename = currentSchemaReplConfig.TableSchemaRename
			} else if currentSchemaReplConfig.TableSchemaRegex != "" { // match currentSchemaReplConfig.TableSchemaRegex and currentSchemaReplConfig.TableSchemaRename
				// TODO check & compile one time
				schemaNameRegex, err := regexp.Compile(currentSchemaReplConfig.TableSchemaRegex)
				if err != nil {
					return fmt.Errorf("compile TableSchemaRegex %v failed: %v", currentSchemaReplConfig.TableSchemaRegex, err)
				}

				match := schemaNameRegex.FindStringSubmatchIndex(schemaName)
				schemaRename = string(schemaNameRegex.ExpandString(nil, schemaRenameRegex, schemaName, match))

			} else {
				return fmt.Errorf("schema configuration error. schemaName=%v ", schemaName)
			}

			// add schema to currentReplicateDoDb
			currentSchema = &common.DataSource{
				TableSchema:      schemaName,
				TableSchemaRegex: currentSchemaReplConfig.TableSchemaRegex,
				TableSchemaRename: schemaRename,
			}
		} else { // replicate all schemas and tables
			currentSchema = &common.DataSource{
				TableSchema: schemaName,
			}
		}
		b.currentReplicateDoDb = append(b.currentReplicateDoDb, currentSchema)
	}

	if "" == tableName {
		return nil
	}

	currentTable := b.findCurrentTable(currentSchema, tableName)
	if nil != currentTable {
		// table already exists
		return nil
	}

	// update current table
	var newTable *common.Table
	if nil != currentSchemaReplConfig && len(currentSchemaReplConfig.Tables) > 0 {
		currentTableConfig := b.findTableConfig(currentSchemaReplConfig, tableName)
		if nil == currentTableConfig {
			// the table doesn't need to be replicated
			return nil
		}

		if currentTableConfig.TableName == tableName { // match tableConfig.TableName and tableConfig.TableRename
			// TODO validateTable. refer to '(i *Inspector) ValidateOriginalTable'
			newTable = &common.Table{
				TableName:            tableName,
				TableRegex:           currentTableConfig.TableRegex,
				TableRename:          currentTableConfig.TableRename,
				TableSchema:          schemaName,
				TableSchemaRename:    currentSchema.TableSchemaRename,
				ColumnMapFrom:        currentTableConfig.ColumnMapFrom,
				OriginalTableColumns: nil, //todo
				UseUniqueKey:         nil, //todo
				ColumnMap:            nil, //todo
				TableType:            "",  //todo
				Where:                currentTableConfig.GetWhere(),
			}
		} else if currentTableConfig.TableRegex != "" { // match tableConfig.TableRegex and tableConfig.TableRename
			// TODO check & compile one time
			tableNameRegex, err := regexp.Compile(currentTableConfig.TableRegex)
			if err != nil {
				return fmt.Errorf("compile TableRegex %v failed: %v", currentTableConfig.TableRegex, err)
			}
			// TODO validateTable
			match := tableNameRegex.FindStringSubmatchIndex(tableName)
			newTable = &common.Table{
				TableName:            tableName,
				TableRegex:           currentTableConfig.TableRegex,
				TableRename:          string(tableNameRegex.ExpandString(nil, currentTableConfig.TableRename, tableName, match)),
				TableSchema:          schemaName,
				TableSchemaRename:    currentSchema.TableSchemaRename,
				ColumnMapFrom:        currentTableConfig.ColumnMapFrom,
				OriginalTableColumns: nil, //todo
				UseUniqueKey:         nil, //todo
				ColumnMap:            nil, //todo
				TableType:            "",  //todo
				Where:                currentTableConfig.GetWhere(),
			}
		} else {
			return fmt.Errorf("table configuration error. schemaName=%v tableName=%v", schemaName, tableName)
		}
	} else { // replicate all tables within current schema
		// TODO validateTable
		newTable = common.NewTable(schemaName, tableName)
		newTable.TableSchemaRename = currentSchema.TableSchemaRename
	}

	if nil != newTable {
		currentSchema.Tables = append(currentSchema.Tables, newTable)
	}
	return nil
}

func (b *BinlogReader) OnApplierRotate(binlogFile string) {
	logger := b.logger.Named("OnApplierRotate")
	if !b.mysqlContext.BinlogRelay {
		// do nothing if BinlogRelay is not enabled
		return
	}

	wrappingDir := b.getBinlogDir()
	fs, err := streamer.ReadDir(wrappingDir)
	if err != nil {
		logger.Error("ReadDir error", "dir", wrappingDir, "err", err)
		return
	}

	dir := ""

	for i := range fs {
		// https://pingcap.com/docs-cn/v3.0/reference/tools/data-migration/relay-log/
		// <server-uuid>.<subdir-seq-number>
		// currently there should only be .000001, but we loop to the last one.
		subdir := filepath.Join(wrappingDir, fs[i])
		stat, err := os.Stat(subdir)
		if err != nil {
			logger.Error("err at stat", "err", err)
			return
		} else {
			if stat.IsDir() {
				dir = subdir
			} else {
				// meta-files like server-uuid.index
			}
		}
	}

	if dir == "" {
		logger.Warn("empty dir")
		return
	}

	realBinlogFile := normalizeBinlogFilename(binlogFile)

	cmp, err := streamer.CollectBinlogFilesCmp(dir, realBinlogFile, streamer.FileCmpLess)
	if err != nil {
		logger.Error("err at cmp", "err", err)
	}
	b.logger.Debug("cmp", "cmp", cmp)
	for i := range cmp {
		f := filepath.Join(dir, cmp[i])
		b.logger.Info("will remove", "file", f)
		err := os.Remove(f)
		if err != nil {
			b.logger.Error("error when removing binlog", "file", f)
		}
	}
}

func normalizeBinlogFilename(name string) string {
	// See `posUUIDSuffixSeparator` in pingcap/dm.
	re := regexp.MustCompile("^(.*)\\|.+(\\..*)$")
	sm := re.FindStringSubmatch(name)
	if len(sm) == 3 {
		return sm[1] + sm[2]
	} else {
		return name
	}
}

func (b *BinlogReader) GetQueueSize() int {
	if b != nil && b.binlogStreamer0 != nil {
		return b.binlogStreamer0.QueueSize()
	} else {
		return 0
	}
}

func (b *BinlogReader) GetQueueMem() int64 {
	if b != nil && b.binlogStreamer0 != nil {
		return b.binlogStreamer0.QueueMem()
	} else {
		return 0
	}
}

func (b *BinlogReader) sqleExecDDL(currentSchema string, ast ast.Node) {
	if b.maybeSqleContext != nil {
		b.maybeSqleContext.LoadSchemas(nil)
		if currentSchema != "" {
			b.maybeSqleContext.UseSchema(currentSchema)
		}
		b.maybeSqleContext.UpdateContext(ast, "mysql")
	}
}

func (b *BinlogReader) sqleAfterCreateSchema(schema string) {
	if b.maybeSqleContext != nil {
		b.maybeSqleContext.LoadTables(schema, nil)
	}
}
func (b *BinlogReader) findCurrentSchema(schemaName string) *common.DataSource {
	if schemaName != "" {
		for i := range b.currentReplicateDoDb {
			if b.currentReplicateDoDb[i].TableSchema == schemaName {
				return b.currentReplicateDoDb[i]
			}
		}
	}
	return nil
}

func (b *BinlogReader) findSchemaConfig(schemaConfigs []*common.DataSource, schemaName string) *common.DataSource {
	if schemaName == "" {
		return nil
	}

	for i := range schemaConfigs {
		if schemaConfigs[i].TableSchema == schemaName {
			return schemaConfigs[i]
		} else if schemaConfigs[i].TableSchemaRegex != "" {
			reg, err := regexp.Compile(schemaConfigs[i].TableSchemaRegex)
			if nil != err {
				b.logger.Warn("compile regexp failed", "schema", schemaName, "err", err)
				continue
			}
			if reg.MatchString(schemaName) {
				return schemaConfigs[i]
			}
		}
	}
	return nil
}

func (b *BinlogReader) findCurrentTable(maybeSchema *common.DataSource, tableName string) *common.Table {
	if maybeSchema != nil {
		for j := range maybeSchema.Tables {
			if maybeSchema.Tables[j].TableName == tableName {
				return maybeSchema.Tables[j]
			}
		}
	}
	return nil
}

func (b *BinlogReader) findTableConfig(maybeSchemaConfig *common.DataSource, tableName string) *common.Table {
	if nil == maybeSchemaConfig {
		return nil
	}

	for j := range maybeSchemaConfig.Tables {
		if maybeSchemaConfig.Tables[j].TableName == tableName {
			return maybeSchemaConfig.Tables[j]
		} else if maybeSchemaConfig.Tables[j].TableRegex != "" {
			reg, err := regexp.Compile(maybeSchemaConfig.Tables[j].TableRegex)
			if nil != err {
				b.logger.Warn("compile regexp failed", "schemaName", maybeSchemaConfig.TableSchema, "tableName", tableName, "err", err)
				continue
			}
			if reg.MatchString(tableName) {
				return maybeSchemaConfig.Tables[j]
			}
		}
	}
	return nil
}

func (b *BinlogReader) generateRenameMaps() (oldSchemaToNewSchema map[string]string, oldSchemaToTablesRenameMap map[string]map[string]string) {
	oldSchemaToNewSchema = map[string]string{}
	oldSchemaToTablesRenameMap = map[string]map[string]string{}

	for _, db := range b.currentReplicateDoDb {
		if db.TableSchemaRename != "" {
			oldSchemaToNewSchema[db.TableSchema] = db.TableSchemaRename
		}

		tablesRenameMap := map[string]string{}
		for _, tb := range db.Tables {
			if tb.TableRename == "" {
				continue
			}
			tablesRenameMap[tb.TableName] = tb.TableRename
		}
		if len(tablesRenameMap)>0{
			oldSchemaToTablesRenameMap[db.TableSchema] = tablesRenameMap
		}
	}
	return
}

func skipBySqlFilter(ddlAst ast.StmtNode, sqlFilter *SqlFilter) bool {
	switch realAst := ddlAst.(type) {
	case *ast.CreateDatabaseStmt:
		if sqlFilter.NoDDLCreateSchema {
			return true
		}
	case *ast.CreateTableStmt:
		if sqlFilter.NoDDLCreateTable {
			return true
		}
	case *ast.DropTableStmt:
		if sqlFilter.NoDDLDropTable {
			return true
		}
	case *ast.AlterTableStmt:
		if sqlFilter.NoDDLAlterTable {
			return true
		} else {
			for i := range realAst.Specs {
				switch realAst.Specs[i].Tp {
				case ast.AlterTableAddColumns:
					if sqlFilter.NoDDLAlterTableAddColumn {
						return true
					}
				case ast.AlterTableDropColumn:
					if sqlFilter.NoDDLAlterTableDropColumn {
						return true
					}
				case ast.AlterTableModifyColumn:
					if sqlFilter.NoDDLAlterTableModifyColumn {
						return true
					}
				case ast.AlterTableChangeColumn:
					if sqlFilter.NoDDLAlterTableChangeColumn {
						return true
					}
				case ast.AlterTableAlterColumn:
					if sqlFilter.NoDDLAlterTableAlterColumn {
						return true
					}
				default:
					// other case
				}
			}
		}
	}

	return false
}
