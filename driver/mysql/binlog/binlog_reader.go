/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package binlog

import (
	"bytes"
	gosql "database/sql"
	"encoding/binary"
	gmclient "github.com/go-mysql-org/go-mysql/client"
	"os"
	"path"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/actiontech/dtle/driver/common"
	"github.com/actiontech/dtle/driver/mysql/base"
	"github.com/actiontech/dtle/driver/mysql/mysqlconfig"
	"github.com/actiontech/dtle/driver/mysql/sql"
	"github.com/actiontech/dtle/driver/mysql/util"
	parserformat "github.com/pingcap/tidb/parser/format"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pkg/errors"

	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/actiontech/dtle/g"

	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	_ "github.com/pingcap/tidb/types/parser_driver"

	sqle "github.com/actiontech/dtle/driver/mysql/sqle/inspector"
	hclog "github.com/hashicorp/go-hclog"

	dmconfig "github.com/pingcap/dm/dm/config"
	dmpb "github.com/pingcap/dm/dm/pb"
	dmlog "github.com/pingcap/dm/pkg/log"
	dmstreamer "github.com/pingcap/dm/pkg/streamer"
	dmrelay "github.com/pingcap/dm/relay"
	dmretry "github.com/pingcap/dm/relay/retry"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	uuid "github.com/satori/go.uuid"
	"golang.org/x/net/context"
)

const (
	StageFinishedReadingOneBinlogSwitchingToNextBinlog = "Finished reading one binlog; switching to next binlog"
	StageRegisteringSlaveOnMaster                      = "Registering slave on master"
	StageRequestingBinlogDump                          = "Requesting binlog dump"
)

type BinlogReader struct {
	serverId uint64
	execCtx  *common.ExecContext
	logger   g.LoggerType

	relay        dmrelay.Process
	relayCancelF context.CancelFunc
	// for direct streaming
	binlogSyncer    *replication.BinlogSyncer
	binlogStreamer0 *replication.BinlogStreamer // for the functions added by us.
	// for relay & streaming
	binlogStreamer dmstreamer.Streamer
	// for relay
	binlogReader      *dmstreamer.BinlogReader
	currentCoord      common.MySQLCoordinates
	currentCoordMutex *sync.Mutex
	// raw config, whose ReplicateDoDB is same as config file (empty-is-all & no dynamically created tables)
	mysqlContext *common.MySQLDriverConfig
	// dynamic config, include all tables (implicitly assigned or dynamically created)
	tables map[string]*common.SchemaContext

	hasBeginQuery bool
	entryContext  *common.EntryContext
	ReMap         map[string]*regexp.Regexp // This is a cache for regexp.

	ctx          context.Context
	shutdown     bool
	shutdownCh   chan struct{}
	shutdownLock sync.Mutex

	sqlFilter *SqlFilter

	maybeSqleContext *sqle.Context
	memory           *int64
	extractedTxCount uint32
	db               *gosql.DB

	serverUUID          string
	lowerCaseTableNames mysqlconfig.LowerCaseTableNamesValue

	targetGtid          gomysql.GTIDSet
	currentGtidSet      gomysql.GTIDSet
	currentGtidSetMutex sync.RWMutex

	BigTxCount int32
}

type SqlFilter struct {
	NoDML       bool
	NoDMLInsert bool
	NoDMLDelete bool
	NoDMLUpdate bool

	NoDDL             bool
	NoDDLCreateSchema bool
	NoDDLCreateTable  bool
	NoDDLDropSchema   bool
	NoDDLDropTable    bool
	NoDDLDropIndex    bool
	NoDDLAlterTable   bool
	NoDDLTruncate     bool

	NoDDLAlterTableAddColumn    bool
	NoDDLAlterTableDropColumn   bool
	NoDDLAlterTableModifyColumn bool
	NoDDLAlterTableChangeColumn bool
	NoDDLAlterTableAlterColumn  bool
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
		case "noddldropschema":
			s.NoDDLDropSchema = true
		case "noddldroptable":
			s.NoDDLDropTable = true
		case "noddldropindex":
			s.NoDDLDropIndex = true
		case "noddltruncate":
			s.NoDDLTruncate = true
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

func NewBinlogReader(
	execCtx *common.ExecContext, cfg *common.MySQLDriverConfig, logger g.LoggerType,
	replicateDoDb map[string]*common.SchemaContext, sqleContext *sqle.Context,
	memory *int64, db *gosql.DB, targetGtid string, lctn mysqlconfig.LowerCaseTableNamesValue,
	ctx context.Context) (binlogReader *BinlogReader, err error) {

	sqlFilter, err := parseSqlFilter(cfg.SqlFilter)
	if err != nil {
		return nil, err
	}

	binlogReader = &BinlogReader{
		ctx:                 ctx,
		execCtx:             execCtx,
		logger:              logger,
		currentCoord:        common.MySQLCoordinates{},
		currentCoordMutex:   &sync.Mutex{},
		mysqlContext:        cfg,
		ReMap:               make(map[string]*regexp.Regexp),
		shutdownCh:          make(chan struct{}),
		tables:              make(map[string]*common.SchemaContext),
		sqlFilter:           sqlFilter,
		maybeSqleContext:    sqleContext,
		memory:              memory,
		db:                  db,
		lowerCaseTableNames: lctn,
	}

	binlogReader.serverUUID, err = sql.GetServerUUID(db)
	if err != nil {
		return nil, err
	}

	binlogReader.currentGtidSet, _ = gomysql.ParseMysqlGTIDSet("")
	if targetGtid != "" {
		binlogReader.targetGtid, err = gomysql.ParseMysqlGTIDSet(targetGtid)
		if err != nil {
			return nil, errors.Wrap(err, "ParseMysqlGTIDSet")
		}
	}

	binlogReader.tables = replicateDoDb

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

			ParseTime:               false, // must be false, or gencode will complain.
			TimestampStringLocation: time.UTC,

			MemLimitSize:    int64(g.MemAvailable / 5),
			MemLimitSeconds: 2,

			Option: func(conn *gmclient.Conn) error {
				_, err := conn.Execute("set session net_write_timeout = ?", cfg.SlaveNetWriteTimeout)
				if err != nil {
					return err
				}
				return nil
			},
		}
		binlogReader.binlogSyncer = replication.NewBinlogSyncer(binlogSyncerConfig)
	}

	binlogReader.mysqlContext.Stage = StageRegisteringSlaveOnMaster

	return binlogReader, err
}

func (b *BinlogReader) getBinlogDir() string {
	return path.Join(b.execCtx.StateDir, "binlog", b.execCtx.Subject)
}

func (b *BinlogReader) ConnectBinlogStreamer(coordinates common.MySQLCoordinates) (err error) {
	if coordinates.IsEmpty() {
		b.logger.Warn("Emptry coordinates at ConnectBinlogStreamer")
	}

	b.currentCoord = coordinates
	b.logger.Info("Connecting binlog streamer",
		"file", coordinates.LogFile, "pos", coordinates.LogPos, "gtid", coordinates.GtidSet)

	if b.mysqlContext.BinlogRelay {
		dbConfig := dmconfig.DBConfig{
			Host:     b.mysqlContext.ConnectionConfig.Host,
			Port:     b.mysqlContext.ConnectionConfig.Port,
			User:     b.mysqlContext.ConnectionConfig.User,
			Password: b.mysqlContext.ConnectionConfig.Password,
		}

		relayConfig := &dmrelay.Config{
			EnableGTID: true,
			RelayDir:   b.getBinlogDir(),
			ServerID:   uint32(b.serverId),
			Flavor:     "mysql",
			From:       dbConfig,
			BinLogName: "",
			BinlogGTID: coordinates.GtidSet,
			ReaderRetry: dmretry.ReaderRetryConfig{
				// value from dm/relay/relay_test.go
				BackoffRollback: 200 * time.Millisecond,
				BackoffMax:      1 * time.Second,
				BackoffMin:      1 * time.Millisecond,
				BackoffJitter:   true,
				BackoffFactor:   2,
			},
		}
		b.relay = dmrelay.NewRelay(relayConfig)
		err = b.relay.Init(b.ctx)
		if err != nil {
			return err
		}

		var ctx context.Context
		ctx, b.relayCancelF = context.WithCancel(b.ctx)

		{
			brConfig := &dmstreamer.BinlogReaderConfig{
				RelayDir: b.getBinlogDir(),
				Timezone: time.UTC,
			}
			b.binlogReader = dmstreamer.NewBinlogReader(dmlog.L(), brConfig)
		}

		go func() {
			b.logger.Info("starting BinlogRelay", "gtidSet", coordinates.GtidSet)
			pr := b.relay.Process(ctx)
			b.logger.Warn("relay.Process stopped", "isCancelled", pr.IsCanceled, "deail", string(pr.Detail))
			for _, prErr := range pr.Errors {
				b.logger.Error("relay.Process error", "err", prErr)
			}

			b.binlogReader.Close()
			b.relay.Close()
		}()

		targetGtid, err := gomysql.ParseMysqlGTIDSet(coordinates.GtidSet)
		if err != nil {
			return err
		}

		for {
			if b.shutdown {
				break
			}
			if b.relay.IsClosed() {
				b.logger.Info("Relay: closed. stop waiting")
				break
			}
			s := b.relay.Status(nil).(*dmpb.RelayStatus)
			gsStr := s.GetRelayBinlogGtid()
			p := s.GetRelayBinlog()

			gs, err := gomysql.ParseMysqlGTIDSet(gsStr)
			if err != nil {
				b.logger.Error("waiting relay. err at ParseMysqlGTIDSet", "err", err)
				b.relay.Close()
				break
			}

			if gs.Contain(targetGtid) {
				b.logger.Debug("Relay: stop waiting.", "pos", p, "gs", gsStr)
				break
			} else {
				b.logger.Debug("Relay: keep waiting.", "pos", p, "gs", gsStr)
			}

			time.Sleep(1 * time.Second)
		}

		if b.relay.IsClosed() {
			return fmt.Errorf("relay has been closed")
		}
		b.binlogStreamer, err = b.binlogReader.StartSyncByGTID(targetGtid)
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
				err = errors.Wrapf(err, "ParseMysqlGTIDSet. %v", coordinates.GtidSet)
				b.logger.Error(err.Error())
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

func (b *BinlogReader) GetCurrentBinlogCoordinates() *common.MySQLCoordinates {
	b.currentCoordMutex.Lock()
	defer b.currentCoordMutex.Unlock()
	returnCoordinates := b.currentCoord
	return &returnCoordinates
}

type parseQueryResult struct {
	isRecognized bool
	table        common.SchemaTable
	// for multi-table DDL, i.e. drop table. Not really used yet.
	extraTables []common.SchemaTable
	sql         string
	ast         ast.StmtNode
	isExpand    bool
	isSkip      bool
}

func (b *BinlogReader) handleEvent(ev *replication.BinlogEvent, entriesChannel chan<- *common.EntryContext) error {
	switch ev.Header.EventType {
	case replication.GTID_EVENT:
		evt := ev.Event.(*replication.GTIDEvent)
		b.currentCoordMutex.Lock()
		// TODO this does not unlock until function return. wrap with func() if needed
		defer b.currentCoordMutex.Unlock()
		u, _ := uuid.FromBytes(evt.SID)

		entry := common.NewBinlogEntry()
		entry.Coordinates = &common.MySQLCoordinateTx{
			LogFile:       b.currentCoord.LogFile,
			LogPos:        int64(ev.Header.LogPos),
			SID:           u,
			GNO:           evt.GNO,
			LastCommitted: evt.LastCommitted,
			SeqenceNumber: evt.SequenceNumber,
		}
		entry.Index = 0
		entry.Final = true

		b.hasBeginQuery = false
		b.entryContext = &common.EntryContext{
			Entry:        entry,
			TableItems:   nil,
			OriginalSize: 1, // GroupMaxSize is default to 1 and we send on EntriesSize >= GroupMaxSize
			Rows:         0,
		}
	case replication.QUERY_EVENT:
		return b.handleQueryEvent(ev, entriesChannel)
	case replication.XID_EVENT:
		evt := ev.Event.(*replication.XIDEvent)
		b.currentCoord.LogPos = int64(ev.Header.LogPos)
		meetTarget := false
		if evt.GSet != nil {
			b.currentCoord.GtidSet = evt.GSet.String()
			b.currentGtidSet = evt.GSet
			if b.targetGtid != nil {
				if b.currentGtidSet.Contain(b.targetGtid) {
					meetTarget = true
				}
			}
		}
		// TODO is the pos the start or the end of a event?
		// pos if which event should be use? Do we need +1?
		mysqlCoordinates := b.entryContext.Entry.Coordinates.(*common.MySQLCoordinateTx)
		mysqlCoordinates.LogPos = b.currentCoord.LogPos

		b.sendEntry(entriesChannel)
		if meetTarget {
			b.onMeetTarget()
		}
	default:
		if rowsEvent, ok := ev.Event.(*replication.RowsEvent); ok {
			return b.handleRowsEvent(ev, rowsEvent, entriesChannel)
		}
	}
	return nil
}

func queryIsBegin(query string) bool {
	return len(query) == 5 && strings.ToUpper(query) == "BEGIN"
}

func queryIsCommit(query string) bool {
	return len(query) == 6 && strings.ToUpper(query) == "COMMIT"
}

func (b *BinlogReader) handleQueryEvent(ev *replication.BinlogEvent,
	entriesChannel chan<- *common.EntryContext) error {
	mysqlCoordinateTx := b.entryContext.Entry.Coordinates.(*common.MySQLCoordinateTx)
	gno := mysqlCoordinateTx.GNO
	evt := ev.Event.(*replication.QueryEvent)
	query0 := string(evt.Query)

	queryEventFlags, err := common.ParseQueryEventFlags(evt.StatusVars, b.logger)
	if err != nil {
		return errors.Wrap(err, "ParseQueryEventFlags")
	}

	if evt.ErrorCode != 0 {
		b.logger.Error("DTLE_BUG: found query_event with error code, which is not handled.",
			"ErrorCode", evt.ErrorCode, "query", query0, "gno", gno)
	}
	currentSchema := string(evt.Schema)

	b.logger.Debug("query event", "schema", currentSchema, "query", query0)

	if queryIsBegin(query0) {
		b.hasBeginQuery = true
	} else if queryIsCommit(query0) || !b.hasBeginQuery {
		// not hasBeginQuery: a single-query transaction.

		var query8 string
		var errConvertToUTF8 error
		if g.IsUTF8OrMB4(queryEventFlags.CharacterSetClient) {
			query8 = query0
		} else {
			b.logger.Info("transcode a DDL to UTF8", "from", queryEventFlags.CharacterSetClient)
			query8, errConvertToUTF8 = mysqlconfig.ConvertToUTF8(query0, queryEventFlags.CharacterSetClient)
		}

		err := b.checkDtleQueryOSID(query8)
		if err != nil {
			return errors.Wrap(err, "checkDtleQueryOSID")
		}

		queryInfo, err := b.resolveQuery(currentSchema, query8, b.skipQueryDDL)
		if err != nil {
			return errors.Wrap(err, "resolveQuery")
		}

		skipSql := false
		if queryInfo.isSkip || isSkipQuery(query8) {
			// queries that should be skipped regardless of ExpandSyntaxSupport
			skipSql = true
		} else {
			if !b.mysqlContext.ExpandSyntaxSupport {
				skipSql = queryInfo.isExpand || isExpandSyntaxQuery(query8)
			}
		}

		currentSchemaRename := currentSchema
		schema := b.findCurrentSchema(currentSchema)
		if schema != nil && schema.TableSchemaRename != "" {
			currentSchemaRename = schema.TableSchemaRename
		}

		if skipSql || !queryInfo.isRecognized {
			if skipSql {
				b.logger.Warn("skipQuery", "query", query8, "gno", gno)
			} else {
				if !queryInfo.isRecognized {
					b.logger.Warn("mysql.reader: QueryEvent is not recognized. will still execute", "query", query8, "gno", gno)
				}

				if errConvertToUTF8 != nil {
					return errors.Wrap(errConvertToUTF8, "DDL ConvertToUTF8")
				}

				event := common.NewQueryEvent(
					currentSchemaRename,
					b.setDtleQuery(query8),
					common.NotDML,
					ev.Header.Timestamp,
					evt.StatusVars,
				)

				b.entryContext.Entry.Events = append(b.entryContext.Entry.Events, event)
				b.entryContext.OriginalSize += len(ev.RawData)
			}
			b.sendEntry(entriesChannel)
			return nil
		}

		b.sqleExecDDL(currentSchema, queryInfo.ast)

		sql1 := queryInfo.sql
		realSchema := g.StringElse(queryInfo.table.Schema, currentSchema)
		tableName := queryInfo.table.Table

		if b.skipQueryDDL(realSchema, tableName) {
			b.logger.Info("Skip QueryEvent", "currentSchema", currentSchema, "sql", sql1,
				"realSchema", realSchema, "tableName", tableName, "gno", gno)
		} else {
			var table *common.Table
			if realSchema != "" {
				err = b.updateCurrentReplicateDoDb(realSchema, tableName)
				if err != nil {
					return errors.Wrap(err, "updateCurrentReplicateDoDb")
				}

				if realSchema != currentSchema {
					schema = b.findCurrentSchema(realSchema)
				}
				tableCtx := b.findCurrentTable(schema, tableName)
				if tableCtx != nil {
					table = tableCtx.Table
				}
			}

			switch realAst := queryInfo.ast.(type) {
			case *ast.CreateDatabaseStmt:
				b.sqleAfterCreateSchema(queryInfo.table.Schema)
				// For `create schema` stmt, currentSchema makes no sense, and is actually buggy.
				currentSchema = ""
				currentSchemaRename = ""
			case *ast.DropDatabaseStmt:
				b.removeFKChildSchema(queryInfo.table.Schema)
			case *ast.CreateTableStmt:
				b.logger.Debug("ddl is create table")
				_, err := b.updateTableMeta(currentSchema, table, realSchema, tableName, gno, sql1)
				if err != nil {
					return err
				}
			case *ast.DropTableStmt:
				if realAst.IsView {
					// do nothing
				} else {
					for _, t := range realAst.Tables {
						dropTableSchema := g.StringElse(t.Schema.String(), currentSchema)
						b.removeFKChild(dropTableSchema, t.Name.String())
					}
				}
			case *ast.DropIndexStmt:
				_, err := b.updateTableMeta(currentSchema, table, realSchema, tableName, gno, sql1)
				if err != nil {
					return err
				}
			case *ast.AlterTableStmt:
				b.logger.Debug("ddl is alter table.", "specs", realAst.Specs)

				fromTable := table
				newSchemaName := realSchema
				newTableName := tableName

				for _, spec := range realAst.Specs {
					switch spec.Tp {
					case ast.AlterTableRenameTable:
						fromTable = nil
						newSchemaName = g.StringElse(spec.NewTable.Schema.String(), currentSchema)
						newTableName = spec.NewTable.Name.String()
					case ast.AlterTableAddConstraint, ast.AlterTableDropForeignKey:
						b.removeFKChild(realSchema, fromTable.TableName)
					default:
						// do nothing
					}
				}

				if !b.skipQueryDDL(newSchemaName, newTableName) {
					_, err := b.updateTableMeta(currentSchema, fromTable, newSchemaName, newTableName, gno, sql1)
					if err != nil {
						return err
					}
				}
			case *ast.RenameTableStmt:
				for _, tt := range realAst.TableToTables {
					newSchemaName := g.StringElse(tt.NewTable.Schema.String(), currentSchema)
					newTableName := tt.NewTable.Name.String()
					b.logger.Debug("updating meta for rename table", "newSchema", newSchemaName,
						"newTable", newTableName)
					if !b.skipQueryDDL(newSchemaName, newTableName) {
						_, err := b.updateTableMeta(currentSchema, nil, newSchemaName, newTableName, gno, sql1)
						if err != nil {
							return err
						}
					} else {
						b.logger.Debug("not updating meta for rename table", "newSchema", newSchemaName,
							"newTable", newTableName)
					}
				}
			}

			if schema != nil && schema.TableSchemaRename != "" {
				queryInfo.table.Schema = schema.TableSchemaRename
				realSchema = schema.TableSchemaRename
			} else {
				// schema == nil means it is not explicit in ReplicateDoDb, thus no renaming
				// or schema.TableSchemaRename == "" means no renaming
			}

			if table != nil && table.TableRename != "" {
				queryInfo.table.Table = table.TableRename
			}
			var columnMapFrom []string
			if table != nil {
				columnMapFrom = table.ColumnMapFrom
			}
			// mapping
			schemaRenameMap, schemaNameToTablesRenameMap := b.generateRenameMaps()
			if len(schemaRenameMap) > 0 || len(schemaNameToTablesRenameMap) > 0 ||
				len(columnMapFrom) > 0 {
				sql1, err = b.loadMapping(sql1, currentSchema, schemaRenameMap, schemaNameToTablesRenameMap, queryInfo.ast, columnMapFrom)
				if nil != err {
					return fmt.Errorf("ddl mapping failed: %v", err)
				}
			}

			if skipBySqlFilter(queryInfo.ast, b.sqlFilter) {
				b.logger.Debug("skipped a ddl event.", "query", sql1)
			} else {
				if realSchema == "" || queryInfo.table.Table == "" {
					b.logger.Info("NewQueryEventAffectTable. found empty schema or table.",
						"schema", realSchema, "table", queryInfo.table.Table, "query", sql1)
				}

				if errConvertToUTF8 != nil {
					return errors.Wrap(errConvertToUTF8, "DDL ConvertToUTF8")
				}

				event := common.NewQueryEventAffectTable(
					currentSchemaRename,
					b.setDtleQuery(sql1),
					common.NotDML,
					queryInfo.table,
					ev.Header.Timestamp,
					evt.StatusVars,
				)
				if table != nil {
					tableBs, err := common.EncodeTable(table)
					if err != nil {
						return errors.Wrap(err, "EncodeTable(table)")
					}
					event.Table = tableBs
				}
				b.entryContext.Entry.Events = append(b.entryContext.Entry.Events, event)
			}
		}
		b.entryContext.OriginalSize += len(ev.RawData)
		b.sendEntry(entriesChannel)
	}
	return nil
}

const (
	dtleQueryPrefix = "/*dtle_gtid1 "
	dtleQuerySuffix = " dtle_gtid*/"
)

func (b *BinlogReader) checkDtleQueryOSID(query string) error {

	start := strings.Index(query, dtleQueryPrefix)
	if start == -1 {
		return nil
	}
	start += len(dtleQueryPrefix)

	end := strings.Index(query, dtleQuerySuffix)
	if end == -1 {
		return fmt.Errorf("incomplete dtle_gtid for query %v", query)
	}
	if end < start {
		return fmt.Errorf("bad dtle_gtid for query %v", query)
	}

	dtleItem := query[start:end]
	ss := strings.Split(dtleItem, " ")
	if len(ss) != 3 {
		return fmt.Errorf("bad dtle_gtid splitted for query %v", query)
	}

	b.logger.Debug("query osid", "osid", ss[1], "gno", b.entryContext.Entry.Coordinates.GetFieldValue("GNO"))

	b.entryContext.Entry.Coordinates.SetField("OSID", ss[1])
	return nil
}
func (b *BinlogReader) setDtleQuery(query string) string {
	coordinate := b.entryContext.Entry.Coordinates.(*common.MySQLCoordinateTx)
	if coordinate.OSID == "" {
		uuidStr := uuid.UUID(coordinate.SID).String()
		tag := fmt.Sprintf("/*dtle_gtid1 %v %v %v dtle_gtid*/", b.execCtx.Subject, uuidStr, coordinate.GNO)

		upperQuery := strings.ToUpper(query)
		if strings.HasPrefix(upperQuery, "CREATE DEFINER=") {
			if strings.HasSuffix(upperQuery, "END") {
				return fmt.Sprintf("%v %v END", query[:len(query)-3], tag)
			}
		}

		return fmt.Sprintf("%v %v", query, tag)
	} else {
		return query
	}
}

func (b *BinlogReader) sendEntry(entriesChannel chan<- *common.EntryContext) {
	isBig := b.entryContext.Entry.IsPartOfBigTx()
	coordinate := b.entryContext.Entry.Coordinates.(*common.MySQLCoordinateTx)
	if isBig {
		newVal := atomic.AddInt32(&b.BigTxCount, 1)
		if newVal == 1 {
			g.AddBigTxJob()
		}
		b.logger.Info("send bigtx entry", "gno", coordinate.GNO,
			"index", b.entryContext.Entry.Index, "final", b.entryContext.Entry.Final, "count", newVal,
			"rows", b.entryContext.Rows)
	}
	b.logger.Debug("sendEntry", "gno", coordinate.GNO, "events", len(b.entryContext.Entry.Events),
		"isBig", isBig, "index", b.entryContext.Entry.Index, "final", b.entryContext.Entry.Final,
		"rows", b.entryContext.Rows)
	atomic.AddInt64(b.memory, int64(b.entryContext.Entry.Size()))
	select {
	case <-b.shutdownCh:
		return
	case entriesChannel <- b.entryContext:
		if b.entryContext.Entry.Final {
			atomic.AddUint32(&b.extractedTxCount, 1)
		}
	}
}

func (b *BinlogReader) loadMapping(sql, currentSchema string, schemasRenameMap map[string]string,
	oldSchemaNameToTablesRenameMap map[string]map[string]string, stmt ast.StmtNode,
	columnMap []string) (string, error) {

	logMapping := func(oldName, newName, mappingType string) {
		msg := fmt.Sprintf("ddl %s mapping", mappingType)
		b.logger.Debug(msg, "from", oldName, "to", newName)
	}

	// will do nothing if `table` is nil
	renameAstTableFn := func(table *ast.TableName) {
		if table == nil {
			return
		}
		table.Schema = model.NewCIStr(g.StringElse(table.Schema.String(), currentSchema))
		newSchemaName := schemasRenameMap[table.Schema.String()]
		tableNameMap := oldSchemaNameToTablesRenameMap[table.Schema.String()]
		newTableName := tableNameMap[table.Name.String()]

		if newSchemaName != "" {
			logMapping(table.Schema.String(), newSchemaName, "schema")
			table.Schema = model.NewCIStr(newSchemaName)
		}

		if newTableName != "" {
			logMapping(table.Name.String(), newTableName, "table")
			table.Name = model.NewCIStr(newTableName)
		}

	}

	renameTableFn := func(schemaName string, oldTableName *string) {
		tableNameMap := oldSchemaNameToTablesRenameMap[schemaName]
		newTableName := tableNameMap[*oldTableName]
		if newTableName != "" {
			logMapping(*oldTableName, newTableName, "table")
			*oldTableName = newTableName
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
		v.Cols = base.BuildCreateTableColsFromMap(v.Cols, columnMap)
		renameAstTableFn(v.Table)
		renameAstTableFn(v.ReferTable)
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
	case *ast.GrantStmt:
		renameTableFn(v.Level.DBName, &v.Level.TableName)
		renameSchemaFn(&v.Level.DBName)
	case *ast.RevokeStmt:
		renameTableFn(v.Level.DBName, &v.Level.TableName)
		renameSchemaFn(&v.Level.DBName)
	default:
		b.logger.Debug("skip mapping ddl", "sql", sql)
		return sql, nil
	}

	bs := bytes.NewBuffer(nil)
	err := stmt.Restore(&parserformat.RestoreCtx{
		Flags: common.ParserRestoreFlag,
		In:    bs,
	})
	if err != nil {
		return "", fmt.Errorf("restore stmt failed: %v", err)
	}
	return bs.String(), nil
}

func (b *BinlogReader) DataStreamEvents(entriesChannel chan<- *common.EntryContext) error {
	bigTxThrottlingCount := 0
	for {
		if b.shutdown {
			break
		}

		b.logger.Trace("b.HasBigTx.Wait. before")
		// Throttle if this job has un-acked big tx, or
		// there are too much global jobs with big tx.
		for !b.shutdown {
			localCount := atomic.LoadInt32(&b.BigTxCount)
			globalLimit := g.BigTxReachMax()
			if localCount <= b.mysqlContext.BigTxSrcQueue && !globalLimit {
				bigTxThrottlingCount = 0
				break
			}

			bigTxThrottlingCount += 1
			sleepMs := 10
			if bigTxThrottlingCount%(1000/sleepMs) == 0 {
				// Force to read an event every 1000ms.
				break
			}
			time.Sleep(time.Duration(sleepMs) * time.Millisecond)
			if bigTxThrottlingCount * sleepMs >= 15 * 1000 {
				b.logger.Warn("reader big tx throttling for 15s", "local", localCount, "global", globalLimit)
				bigTxThrottlingCount = 0
			}
		}
		b.logger.Trace("b.HasBigTx.Wait. after")

		ev, err := b.binlogStreamer.GetEvent(b.ctx)
		if err != nil {
			b.logger.Error("error GetEvent.", "err", err)
			return err
		}
		for g.IsLowMemory() {
			time.Sleep(900 * time.Millisecond)
		}
		if b.shutdown {
			return nil
		}

		if ev.Header.EventType == replication.HEARTBEAT_EVENT {
			continue
		}

		b.currentCoordMutex.Lock()
		b.currentCoord.LogPos = int64(ev.Header.LogPos)
		b.currentCoordMutex.Unlock()

		if ev.Header.EventType == replication.ROTATE_EVENT {
			serverUUID, err := sql.GetServerUUID(b.db)
			if err != nil {
				return errors.Wrap(err, "on rotate_event. GetServerUUID")
			}
			if serverUUID != b.serverUUID {
				return fmt.Errorf("serverUUID changed from %v to %v. job should restart",
					b.serverUUID, serverUUID)
			}

			rotateEvent := ev.Event.(*replication.RotateEvent)
			nextLogName := string(rotateEvent.NextLogName)
			b.currentCoordMutex.Lock()
			b.currentCoord.LogFile = nextLogName
			b.currentCoordMutex.Unlock()
			b.mysqlContext.Stage = StageFinishedReadingOneBinlogSwitchingToNextBinlog
			b.logger.Info("Rotate to next binlog", "name", nextLogName)
		} else {
			if err := b.handleEvent(ev, entriesChannel); err != nil {
				return err
			}
		}
	}

	return nil
}

// schemaTables is the schema.table that the query has invalidated. For unrecognized query, it is nil.
func (b *BinlogReader) resolveQuery(currentSchema string, sql string,
	skipFunc func(schema string, tableName string) bool) (result parseQueryResult, err error) {

	rewrite := false

	result.sql = sql
	result.isRecognized = true
	result.isSkip = false

	if b.lowerCaseTableNames != mysqlconfig.LowerCaseTableNames0 {
		rewrite = true
	}

	stmt, err := parser.New().ParseOneStmt(sql, "", "")
	if err != nil {
		result.isRecognized = false
		return result, nil
	}

	setSchema := func(schema *string) {
		if b.lowerCaseTableNames != mysqlconfig.LowerCaseTableNames0 {
			g.LowerString(schema)
		}
		result.table = common.SchemaTable{Schema: *schema, Table: ""}
	}
	mayLowerTable := func(tn *ast.TableName) {
		if b.lowerCaseTableNames != mysqlconfig.LowerCaseTableNames0 {
			tn.Schema = model.NewCIStr(tn.Schema.L)
			tn.Name = model.NewCIStr(tn.Name.L)
		}
	}
	setTable := func(tn *ast.TableName, extra bool) {
		mayLowerTable(tn)
		item := common.SchemaTable{Schema: tn.Schema.String(), Table: tn.Name.String()}
		if extra {
			result.extraTables = append(result.extraTables, item)
		} else {
			result.table = item
		}
	}

	switch v := stmt.(type) {
	case *ast.CreateDatabaseStmt:
		setSchema(&v.Name)
	case *ast.DropDatabaseStmt:
		setSchema(&v.Name)
	case *ast.AlterDatabaseStmt:
		setSchema(&v.Name)
	case *ast.CreateIndexStmt:
		setTable(v.Table, false)
	case *ast.DropIndexStmt:
		setTable(v.Table, false)
	case *ast.TruncateTableStmt:
		setTable(v.Table, false)
	case *ast.CreateTableStmt:
		setTable(v.Table, false)
	case *ast.CreateViewStmt:
		result.isSkip = true
	case *ast.AlterViewStmt:
		result.isSkip = true
	case *ast.AlterTableStmt:
		setTable(v.Table, false)
	case *ast.RevokeStmt, *ast.RevokeRoleStmt:
		result.isExpand = true
	case *ast.SetPwdStmt:
		result.isExpand = true
	case *ast.FlushStmt:
		result.isExpand = true
		//switch v.Tp {
		//case ast.FlushPrivileges:
		//case ast.FlushTables:
		//case ast.FlushStatus:
		//}
	case *ast.DropProcedureStmt:
		result.isExpand = true
	case *ast.AlterProcedureStmt:
		result.isExpand = true
	case *ast.DropTableStmt:
		if v.IsView {
			result.isSkip = true
		} else {
			var newTables []*ast.TableName
			for _, t := range v.Tables {
				mayLowerTable(t)
				schema := g.StringElse(t.Schema.String(), currentSchema)
				if !skipFunc(schema, t.Name.String()) {
					isExtraTable := len(newTables) > 0
					setTable(t, isExtraTable)
					newTables = append(newTables, t)
					b.logger.Debug("resolveQuery. DropTableStmt. include",
						"schema", t.Schema, "table", t.Name, "use", currentSchema)
				}
			}

			if len(newTables) == 0 {
				// No tables included. Add the first table and ignore the whole stmt.
				newTables = v.Tables[:1]
				setTable(v.Tables[0], false)
			}

			if rewrite || len(newTables) != len(v.Tables) {
				v.Tables = newTables
				rewrite = true
			}
		}
	case *ast.CreateUserStmt, *ast.GrantStmt, *ast.DropUserStmt, *ast.AlterUserStmt:
		setTable(&ast.TableName{
			Schema: model.NewCIStr("mysql"),
			Name:   model.NewCIStr("user"),
		}, false)
		result.isExpand = true
	case *ast.RenameTableStmt:
		for i, t2t := range v.TableToTables {
			// TODO handle extra tables in v.TableToTables[1:]
			if i == 0 {
				setTable(t2t.OldTable, false)
			}
			mayLowerTable(t2t.OldTable)
			mayLowerTable(t2t.NewTable)
		}
	case *ast.DropTriggerStmt:
		result.isSkip = true
	default:
		result.isRecognized = false
	}

	if rewrite {
		bs := bytes.NewBuffer(nil)
		r := &parserformat.RestoreCtx{
			Flags: common.ParserRestoreFlag,
			In:    bs,
		}
		err = stmt.Restore(r)
		if err != nil {
			return result, err
		}
		result.sql = bs.String()
		b.logger.Debug("resolveQuery. rewrite", "sql", result.sql)

		// Re-generate ast. See #1036.
		stmt, err = parser.New().ParseOneStmt(result.sql, "", "")
		if err != nil {
			return result, err
		}
	}

	result.ast = stmt

	return result, nil
}

// schema and tableName should be processed according to lower_case_table_names in advance
func (b *BinlogReader) skipQueryDDL(schema string, tableName string) bool {
	switch schema {
	case "mysql":
		if b.mysqlContext.ExpandSyntaxSupport {
			return false
		} else {
			return true
		}
	case "sys", "information_schema", "performance_schema", g.DtleSchemaName:
		return true
	default:
		if len(b.mysqlContext.ReplicateIgnoreDb) > 0 {
			if common.IgnoreDbByReplicateIgnoreDb(b.mysqlContext.ReplicateIgnoreDb, schema) {
				return true
			}
			if common.IgnoreTbByReplicateIgnoreDb(b.mysqlContext.ReplicateIgnoreDb, schema, tableName) {
				return true
			}
		}
		if len(b.mysqlContext.ReplicateDoDb) > 0 {
			return !b.matchTable(b.mysqlContext.ReplicateDoDb, schema, tableName)
		} else {
			// replicate all dbs and tbs
			return false
		}
	}
}

var (
	// > A Regexp is safe for concurrent use by multiple goroutines...
	regexCreateTrigger   = regexp.MustCompile(`(?is)CREATE\b.+?TRIGGER\b.+?(?:BEFORE|AFTER)\b.+?(?:INSERT|UPDATE|DELETE)\b.+?ON\b.+?FOR\b.+?EACH\b.+?ROW\b`)
	regexCreateEvent     = regexp.MustCompile(`(?is)CREATE\b.+?EVENT\b.+?ON\b.+?SCHEDULE\b.+?(?:AT|EVERY)\b.+?DO\b`)
	regexAlterEvent      = regexp.MustCompile(`(?is)ALTER\b.+?EVENT\b.+?(?:ON SCHEDULE|ON COMPLETION|RENAME TO|ENABLE|DISABLE|COMMENT|DO)\b`)
	regexCreateProcedure = regexp.MustCompile(`(?is)CREATE DEFINER=.+?(?:PROCEDURE|FUNCTION)\b.+?`)
)

func isSkipQuery(sql string) bool {
	if regexCreateTrigger.MatchString(sql) {
		return true
	}

	if regexCreateEvent.MatchString(sql) {
		return true
	}

	if regexAlterEvent.MatchString(sql) {
		return true
	}

	return false
}
func isExpandSyntaxQuery(sql string) bool {
	sql = strings.ToLower(sql)

	// TODO mod pingcap/parser to support these DDLs
	//  and use ast instead of string-matching
	if strings.HasPrefix(sql, "rename user") {
		return true
	}

	if regexCreateProcedure.MatchString(sql) {
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
		if strings.HasPrefix(strings.ToLower(string(rowsEvent.Table.Table)), g.GtidExecutedTablePrefix) {
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
						var sid uuid.UUID     // will be initialized to 0
						copy(sid[:], sidByte) // len(sidByte) might be less than 16 #1034

						coordinate := b.entryContext.Entry.Coordinates.(*common.MySQLCoordinateTx)
						coordinate.OSID = sid.String()
						b.logger.Debug("found an osid", "osid", coordinate.OSID)
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
		if schemaContext, ok := b.tables[string(rowsEvent.Table.Schema)]; ok {
			if tableCtx, ok := schemaContext.TableMap[tableOrigin]; ok {
				return false, tableCtx
			}
		}
		// TODO when will schemaName be empty?
		if schemaContext, ok := b.tables[""]; ok {
			if tableCtx, ok := schemaContext.TableMap[tableOrigin]; ok {
				return false, tableCtx
			}
		}
		return true, nil
	}
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
		if b.binlogReader != nil {
			b.binlogReader.Close()
		}
		if b.relayCancelF != nil {
			b.relayCancelF()
		}
		if b.relay != nil {
			b.relay.Close()
		}
	} else {
		b.binlogSyncer.Close()
	}
	// here. A new go-mysql version closes the binlog syncer connection independently.
	// I will go against the sacred rules of comments and just leave this here.
	// This is the year 2017. Let's see what year these comments get deleted.
	return nil
}

// gno, query: for debug use
func (b *BinlogReader) updateTableMeta(currentSchema string, table *common.Table, realSchema string, tableName string,
	gno int64, query string) (*common.TableContext, error) {

	var err error

	if b.maybeSqleContext == nil {
		b.logger.Debug("ignore updating table meta", "schema", realSchema, "table", tableName)
		return nil, nil
	}

	columns, fkParent, err := base.GetTableColumnsSqle(b.maybeSqleContext, realSchema, tableName)
	if err != nil {
		b.logger.Warn("updateTableMeta: cannot get table info after ddl.", "err", err,
			"realSchema", realSchema, "tableName", tableName, "gno", gno, "query", query)
		return nil, err
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
	table.ColumnMap, err = mysqlconfig.BuildColumnMapIndex(table.ColumnMapFrom, table.OriginalTableColumns.Ordinals)
	if err != nil {
		return nil, err
	}

	schemaContext := b.findCurrentSchema(realSchema)
	tableCtx, err := common.NewTableContext(table)
	if err != nil {
		return nil, err
	}
	schemaContext.TableMap[table.TableName] = tableCtx
	b.addFKChildren(currentSchema, fkParent, realSchema, tableName)

	return tableCtx, nil
}

func (b *BinlogReader) updateCurrentReplicateDoDb(schemaName string, tableName string) error {
	if schemaName == "" {
		return fmt.Errorf("schema name should not be empty. table %v", tableName)
	}

	var currentSchemaReplConfig *common.DataSource
	currentSchema := b.findCurrentSchema(schemaName)
	currentSchemaReplConfig = b.findSchemaConfig(b.mysqlContext.ReplicateDoDb, schemaName)
	if currentSchema == nil { // update current schema
		if len(b.mysqlContext.ReplicateDoDb) > 0 {
			if currentSchemaReplConfig == nil {
				//  the schema doesn't need to be replicated
				return nil
			}

			schemaRename := ""
			schemaRenameRegex := currentSchemaReplConfig.TableSchemaRename
			if currentSchemaReplConfig.TableSchema != "" {
				// match currentSchemaReplConfig.TableSchema and currentSchemaReplConfig.TableSchemaRename
				schemaRename = currentSchemaReplConfig.TableSchemaRename
			} else if currentSchemaReplConfig.TableSchemaRegex != "" {
				// match currentSchemaReplConfig.TableSchemaRegex and currentSchemaReplConfig.TableSchemaRename
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
			currentSchema = common.NewSchemaContext(schemaName)
			currentSchema.TableSchemaRename = schemaRename
		} else { // replicate all schemas and tables
			currentSchema = common.NewSchemaContext(schemaName)
		}
		b.tables[schemaName] = currentSchema
	}

	if tableName == "" {
		return nil
	}

	currentTable := b.findCurrentTable(currentSchema, tableName)
	if currentTable != nil {
		// table already exists
		return nil
	}

	// update current table
	var newTable *common.Table
	if currentSchemaReplConfig != nil && len(currentSchemaReplConfig.Tables) > 0 {
		currentTableConfig := b.findTableConfig(currentSchemaReplConfig, tableName)
		if currentTableConfig == nil {
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

	if newTable != nil {
		tableCtx, err := common.NewTableContext(newTable)
		if err != nil {
			return err
		}
		currentSchema.TableMap[tableName] = tableCtx
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
	fs, err := os.ReadDir(wrappingDir)
	if err != nil {
		logger.Error("ReadDir error", "dir", wrappingDir, "err", err)
		return
	}

	dir := ""

	for i := range fs {
		// https://pingcap.com/docs-cn/v3.0/reference/tools/data-migration/relay-log/
		// <server-uuid>.<subdir-seq-number>
		// currently there should only be .000001, but we loop to the last one.
		if fs[i].IsDir() {
			dir = filepath.Join(wrappingDir, fs[i].Name())
		} else {
			// meta-files like server-uuid.index
		}
	}

	if dir == "" {
		logger.Warn("OnApplierRotate: no sub dir", "wrappingDir", wrappingDir)
		return
	}

	realBinlogFile := normalizeBinlogFilename(binlogFile)

	cmp, err := dmstreamer.CollectBinlogFilesCmp(dir, realBinlogFile, dmstreamer.FileCmpLess)
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

func (b *BinlogReader) GetExtractedTxCount() uint32 {
	if b != nil {
		return b.extractedTxCount
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
func (b *BinlogReader) findCurrentSchema(schemaName string) *common.SchemaContext {
	if schemaName == "" {
		return nil
	}
	schemaContext, ok := b.tables[schemaName]
	if !ok {
		return nil
	}

	return schemaContext
}

func (b *BinlogReader) findSchemaConfig(schemaConfigs []*common.DataSource, schemaName string) *common.DataSource {
	if schemaName == "" {
		return nil
	}

	for i := range schemaConfigs {
		if schemaConfigs[i].TableSchema == schemaName {
			return schemaConfigs[i]
		} else if regStr := schemaConfigs[i].TableSchemaRegex; regStr != "" {
			reg, err := regexp.Compile(regStr)
			if nil != err {
				b.logger.Warn("compile regexp failed", "regex", regStr,
					"schema", schemaName, "err", err)
				continue
			}
			if reg.MatchString(schemaName) {
				return schemaConfigs[i]
			}
		}
	}
	return nil
}

func (b *BinlogReader) findCurrentTable(maybeSchema *common.SchemaContext, tableName string) *common.TableContext {
	if maybeSchema == nil {
		return nil
	}
	table, ok := maybeSchema.TableMap[tableName]
	if !ok {
		return nil
	}
	return table
}

func (b *BinlogReader) findTableConfig(maybeSchemaConfig *common.DataSource, tableName string) *common.Table {
	if nil == maybeSchemaConfig {
		return nil
	}

	for j := range maybeSchemaConfig.Tables {
		if maybeSchemaConfig.Tables[j].TableName == tableName {
			return maybeSchemaConfig.Tables[j]
		} else if regStr := maybeSchemaConfig.Tables[j].TableRegex; regStr != "" {
			reg, err := regexp.Compile(regStr)
			if nil != err {
				b.logger.Warn("compile regexp failed", "regex", regStr,
					"schemaName", maybeSchemaConfig.TableSchema, "tableName", tableName, "err", err)
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

	for _, db := range b.tables {
		if db.TableSchemaRename != "" {
			oldSchemaToNewSchema[db.TableSchema] = db.TableSchemaRename
		}

		tablesRenameMap := map[string]string{}
		for _, tb := range db.TableMap {
			if tb.Table.TableRename == "" {
				continue
			}
			tablesRenameMap[tb.Table.TableName] = tb.Table.TableRename
		}
		if len(tablesRenameMap) > 0 {
			oldSchemaToTablesRenameMap[db.TableSchema] = tablesRenameMap
		}
	}
	return
}

func skipBySqlFilter(ddlAst ast.StmtNode, sqlFilter *SqlFilter) bool {
	if sqlFilter.NoDDL {
		return true
	}

	switch realAst := ddlAst.(type) {
	case *ast.CreateDatabaseStmt:
		if sqlFilter.NoDDLCreateSchema {
			return true
		}
	case *ast.DropDatabaseStmt:
		if sqlFilter.NoDDLDropSchema {
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
	case *ast.DropIndexStmt:
		if sqlFilter.NoDDLDropIndex {
			return true
		}
	case *ast.TruncateTableStmt:
		if sqlFilter.NoDDLTruncate {
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

func (b *BinlogReader) SetTargetGtid(gtid string) (err error) {
	b.targetGtid, err = gomysql.ParseMysqlGTIDSet(gtid)
	if err != nil {
		return errors.Wrap(err, "ParseMysqlGTIDSet")
	}

	b.currentGtidSetMutex.RLock()
	defer b.currentGtidSetMutex.RUnlock()
	if b.currentGtidSet.Contain(b.targetGtid) {
		b.onMeetTarget()
	}
	return nil
}

func (b *BinlogReader) onMeetTarget() {
	b.logger.Info("onMeetTarget", "target", b.targetGtid.String(), "current", b.currentGtidSet.String())
	_ = b.Close()
}

func (b *BinlogReader) handleRowsEvent(ev *replication.BinlogEvent, rowsEvent *replication.RowsEvent,
	entriesChannel chan<- *common.EntryContext) (err error) {

	schemaName := string(rowsEvent.Table.Schema)
	tableName := string(rowsEvent.Table.Table)
	coordinate := b.entryContext.Entry.Coordinates.(*common.MySQLCoordinateTx)
	b.logger.Trace("got rowsEvent", "schema", schemaName, "table", tableName,
		"gno", coordinate.GNO, "flags", rowsEvent.Flags, "tableFlags", rowsEvent.Table.Flags,
		"nRows", len(rowsEvent.Rows))

	dml := common.ToEventDML(ev.Header.EventType)
	skip, table := b.skipRowEvent(rowsEvent, dml)
	if skip {
		b.logger.Debug("skip rowsEvent", "schema", schemaName, "table", tableName,
			"gno", coordinate.GNO)
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
	if table != nil {
		dmlEvent.FKParent = len(table.FKChildren) > 0
	}
	dmlEvent.Flags = make([]byte, 2)
	binary.LittleEndian.PutUint16(dmlEvent.Flags, rowsEvent.Flags)
	dmlEvent.LogPos = int64(ev.Header.LogPos - ev.Header.EventSize)

	/*originalTableColumns, _, err := b.InspectTableColumnsAndUniqueKeys(string(rowsEvent.Table.Schema), string(rowsEvent.Table.Table))
	if err != nil {
		return err
	}
	dmlEvent.OriginalTableColumns = originalTableColumns*/

	// It is hard to calculate exact row size. We use estimation.
	avgRowSize := len(ev.RawData) / len(rowsEvent.Rows)

	if table != nil && table.Table.TableRename != "" {
		dmlEvent.TableName = table.Table.TableRename
		b.logger.Debug("dml table mapping", "from", dmlEvent.TableName, "to", table.Table.TableRename)
	}
	schemaContext := b.findCurrentSchema(schemaName)
	if schemaContext != nil && schemaContext.TableSchemaRename != "" {
		b.logger.Debug("dml schema mapping", "from", dmlEvent.DatabaseName, "to", schemaContext.TableSchemaRename)
		dmlEvent.DatabaseName = schemaContext.TableSchemaRename
	}

	if table != nil && !table.DefChangedSent {
		b.logger.Debug("send table structure", "schema", schemaName, "table", tableName)
		if table.Table == nil {
			b.logger.Warn("DTLE_BUG binlog_reader: table.Table is nil",
				"schema", schemaName, "table", tableName)
		} else {
			tableBs, err := common.EncodeTable(table.Table)
			if err != nil {
				return errors.Wrap(err, "EncodeTable(table)")
			}
			dmlEvent.Table = tableBs
		}

		table.DefChangedSent = true
	}

	checkWhere := func(row []interface{}) (bool, error) {
		if table != nil && !table.WhereCtx.IsDefault {
			return table.WhereTrue(row)
		} else {
			return true, nil
		}
	}

	if dml == common.UpdateDML && len(rowsEvent.Rows) % 2 != 0 {
		return fmt.Errorf("bad RowsEvent. expect 2N rows for an update event. got %v. gno %v",
			len(rowsEvent.Rows), coordinate.GNO)
	}
	for i := 0; i < len(rowsEvent.Rows); i++ {
		row0 := rowsEvent.Rows[i]
		whereTrue0, err := checkWhere(row0)
		if err != nil {
			return err
		}

		switch dml {
		case common.InsertDML, common.DeleteDML:
			if whereTrue0 {
				b.entryContext.Rows += 1
				b.entryContext.OriginalSize += avgRowSize
				dmlEvent.Rows = append(dmlEvent.Rows, row0)
			} else {
				b.logger.Debug("event has not passed 'where'")
			}
		case common.UpdateDML:
			i += 1
			row1 := rowsEvent.Rows[i]
			whereTrue1, err := checkWhere(row1)
			if err != nil {
				return err
			}

			if !whereTrue0 && !whereTrue1 {
				// append no rows
			} else {
				b.entryContext.Rows += 1
				if whereTrue0 {
					b.entryContext.OriginalSize += avgRowSize
					dmlEvent.Rows = append(dmlEvent.Rows, row0)
				} else {
					dmlEvent.Rows = append(dmlEvent.Rows, nil)
					b.logger.Debug("event has not passed 'where' update.from")
				}
				if whereTrue1 {
					b.entryContext.OriginalSize += avgRowSize
					dmlEvent.Rows = append(dmlEvent.Rows, row1)
				} else {
					dmlEvent.Rows = append(dmlEvent.Rows, nil)
					b.logger.Debug("event has not passed 'where' update.to")
				}
			}
		}
	}

	if table != nil && len(table.Table.ColumnMap) > 0 {
		for iRow := range dmlEvent.Rows {
			if len(dmlEvent.Rows[iRow]) == 0 {
				continue
			}

			newRow := make([]interface{}, len(table.Table.ColumnMap))
			for iCol := range table.Table.ColumnMap {
				idx := table.Table.ColumnMap[iCol]
				newRow[iCol] = dmlEvent.Rows[iRow][idx]
			}
			dmlEvent.Rows[iRow] = newRow
		}
	}

	if len(dmlEvent.Rows) > 0 {
		// return 0 if the last event could be reused.
		reuseLast := func() int {
			if len(b.entryContext.Entry.Events) == 0 {
				return 1
			}
			lastEvent := &b.entryContext.Entry.Events[len(b.entryContext.Entry.Events)-1]
			if dml != common.InsertDML || lastEvent.DML != common.InsertDML {
				return 2
			}
			if lastEvent.DatabaseName != dmlEvent.DatabaseName || lastEvent.TableName != dmlEvent.TableName {
				return 3
			}
			if bytes.Compare(lastEvent.Flags, dmlEvent.Flags) != 0 {
				return 4
			}
			if dmlEvent.FKParent != lastEvent.FKParent {
				return 5
			}

			lastEvent.Rows = append(lastEvent.Rows, dmlEvent.Rows...)
			b.logger.Debug("reuseLast. reusing", "nRows", len(lastEvent.Rows))
			return 0
		}()

		if reuseLast != 0 {
			b.logger.Debug("reuseLast. not reusing", "step", reuseLast)
			b.entryContext.Entry.Events = append(b.entryContext.Entry.Events, *dmlEvent)
		}

		if b.entryContext.OriginalSize >= b.mysqlContext.DumpEntryLimit {
			b.logger.Debug("splitting big tx", "index", b.entryContext.Entry.Index)
			b.entryContext.Entry.Final = false
			b.sendEntry(entriesChannel)
			entry := common.NewBinlogEntry()
			entry.Coordinates = b.entryContext.Entry.Coordinates
			entry.Index = b.entryContext.Entry.Index + 1
			entry.Final = true
			b.entryContext = &common.EntryContext{
				Entry:        entry,
				TableItems:   nil,
				OriginalSize: 1,
				Rows:         0,
			}
		}
	}

	return nil
}

func (b *BinlogReader) removeFKChildSchema(schema string) {
	for _, schemaContext := range b.tables {
		for _, tableContext := range schemaContext.TableMap {
			for k, _ := range tableContext.FKChildren {
				if k.Schema == schema {
					delete(tableContext.FKChildren, k)
				}
			}

		}
	}
}

func (b *BinlogReader) removeFKChild(childSchema string, childTable string) {
	for _, schemaContext := range b.tables {
		for _, tableContext := range schemaContext.TableMap {
			// NOTE it is ok to delete not existing items
			st := common.SchemaTable{childSchema, childTable}
			if _, ok := tableContext.FKChildren[st]; ok {
				b.logger.Debug("remove fk child", "parent", tableContext.Table.TableName, "child", st.Table)
				delete(tableContext.FKChildren, st)
			}
		}
	}
}

func (b *BinlogReader) addFKChildren(currentSchema string, fkParents []*ast.TableName,
	childSchema string, childTable string) {

	childST := common.SchemaTable{childSchema, childTable}
	for _, fkp := range fkParents {
		parentSchema := g.StringElse(fkp.Schema.String(), currentSchema)
		parentTable := fkp.Name.String()
		schemaContext := b.findCurrentSchema(parentSchema)
		if schemaContext == nil {
			b.logger.Warn("FK parent not found 1", "schema", parentSchema, "table", parentTable)
			return
		}
		tableContext := b.findCurrentTable(schemaContext, parentTable)
		if tableContext == nil {
			b.logger.Warn("FK parent not found 2", "schema", parentSchema, "table", parentTable)
			return
		}
		tableContext.FKChildren[childST] = struct{}{}
	}
}
