package common

import (
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"github.com/actiontech/dtle/g"
	"github.com/go-mysql-org/go-mysql/replication"
	parsercharset "github.com/pingcap/tidb/parser/charset"
	uuid "github.com/satori/go.uuid"
)

func (b *MySQLCoordinateTx) GetSid() interface{} {
	return uuid.UUID(b.SID)
}

// Do not call this frequently. Cache your result.
func (b *MySQLCoordinateTx) GetSidStr() string {
	return uuid.UUID(b.SID).String()
}

func (b *MySQLCoordinateTx) GetGtidForThisTx() string {
	return fmt.Sprintf("%s:%d", b.GetSid(), b.GNO)
}

 func (b *MySQLCoordinateTx)GetLogPos()int64{
	return b.LogPos
 }

 func (b *MySQLCoordinateTx)GetLastCommit()int64{
	return b.LastCommitted
 }

 func (b *MySQLCoordinateTx)GetGNO()int64{
	return b.GNO
 }

 func (b *MySQLCoordinateTx)GetLogFile()string{
	return b.LogFile
 }

 func (b *MySQLCoordinateTx)GetSequenceNumber()int64{
	return b.SeqenceNumber
 }
type EntryContext struct {
	Entry       *DataEntry
	// Only a DML has a tableItem. For a DDL, its tableItem is nil.
	TableItems  []*ApplierTableItem
	OriginalSize  int // size of binlog entry
}

func NewBinlogEntry() *DataEntry {
	binlogEntry := &DataEntry{
		Events:       make([]DataEvent, 0),
	}
	return binlogEntry
}

func (b *DataEntry) HasDDL() bool {
	for i := range b.Events {
		switch b.Events[i].DML {
		case NotDML:
			return true
		default:
		}
	}
	return false
}

func (b *DataEntry) IsOneStmtDDL() bool {
	// most DDL is implicit committing
	return len(b.Events) == 1 && b.Events[0].DML == NotDML
}

func (b *DataEntry) IsPartOfBigTx() bool {
	return !(b.Index == 0 && b.Final)
}

// Duplicate creates and returns a new binlog entry, with some of the attributes pre-assigned
func (b *DataEntry) String() string {
	return fmt.Sprintf("[BinlogEntry at %+v]", b.Coordinates)
}

const (
	NotDML    int8 = iota
	InsertDML
	UpdateDML
	DeleteDML
)

func ToEventDML(eventType replication.EventType) int8 {
	switch eventType {
	case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		return InsertDML
	case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		return UpdateDML
	case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		return DeleteDML
	default:
		return NotDML
	}
}

type SchemaTable struct {
	Schema string
	Table  string
}

func NewDataEvent(databaseName, tableName string, dml int8, columnCount uint64, timestamp uint32) *DataEvent {
	event := &DataEvent{
		DatabaseName: databaseName,
		TableName:    tableName,
		DML:          dml,
		ColumnCount:  columnCount,
		Timestamp:    timestamp,
	}
	return event
}

func NewQueryEvent(currentSchema, query string, dml int8, timestamp uint32, flags []byte) DataEvent {
	event := DataEvent{
		CurrentSchema: currentSchema,
		Query:         query,
		DML:           dml,
		Timestamp:     timestamp,
		Flags:         flags,
	}
	return event
}
func NewQueryEventAffectTable(currentSchema, query string, dml int8, affectedTable SchemaTable, timestamp uint32, flags []byte) DataEvent {

	event := DataEvent{
		CurrentSchema: currentSchema,
		DatabaseName:  affectedTable.Schema,
		TableName:     affectedTable.Table,
		Query:         query,
		DML:           dml,
		Timestamp:     timestamp,
		Flags:         flags,
	}
	return event
}

func (b *DataEvent) String() string {
	return fmt.Sprintf("[%+v on %s:%s]", b.DML, b.DatabaseName, b.TableName)
}

type ApplierTableItem struct {
	Columns   *ColumnList
	PsInsert0 []*sql.Stmt
	PsInsert1 []*sql.Stmt
	PsInsert2 []*sql.Stmt
	PsInsert3 []*sql.Stmt
	PsDelete  []*sql.Stmt
	PsUpdate    []*sql.Stmt
	ColumnMapTo []string
}

func NewApplierTableItem(parallelWorkers int) *ApplierTableItem {
	return &ApplierTableItem{
		Columns:   nil,
		PsInsert0: make([]*sql.Stmt, parallelWorkers),
		PsInsert1: make([]*sql.Stmt, parallelWorkers),
		PsInsert2: make([]*sql.Stmt, parallelWorkers),
		PsInsert3: make([]*sql.Stmt, parallelWorkers),
		PsDelete:  make([]*sql.Stmt, parallelWorkers),
		PsUpdate:  make([]*sql.Stmt, parallelWorkers),
	}
}
func (ait *ApplierTableItem) Reset() {
	// TODO handle err of `.Close()`?
	closeStmts := func(stmts []*sql.Stmt) {
		for i := range stmts {
			if stmts[i] != nil {
				stmts[i].Close()
				stmts[i] = nil
			}
		}
	}
	closeStmts(ait.PsInsert0)
	closeStmts(ait.PsInsert1)
	closeStmts(ait.PsInsert2)
	closeStmts(ait.PsInsert3)
	closeStmts(ait.PsDelete)
	closeStmts(ait.PsUpdate)

	ait.Columns = nil
}

// String returns a user-friendly string representation of these coordinates
func (b MySQLCoordinates) String() string {
	return fmt.Sprintf("%v", b.GtidSet)
}

// IsEmpty returns true if the log file is empty, unnamed
func (b *MySQLCoordinates) IsEmpty() bool {
	return b.GtidSet == "" && b.LogFile == ""
}

func (b *MySQLCoordinates) CompareFilePos(other *MySQLCoordinates) int {
	if b.LogFile < other.LogFile {
		return -1
	} else if b.LogFile == other.LogFile {
		if b.LogPos < other.LogPos {
			return -1
		} else if b.LogPos == other.LogPos {
			return 0
		} else {
			return 1
		}
	} else {
		return 1
	}
}

func (b *MySQLCoordinates) GetLogPos() int64 {
	return b.LogPos
}

func (b *MySQLCoordinates) GetTxSet() string {
	return b.GtidSet
}

func (b *MySQLCoordinates) GetLogFile() string {
	return b.LogFile
}
type QueryEventFlags struct {
	NoForeignKeyChecks  bool

	// The query is converted to utf8 in dtle-src. Ignore charset/collation flags on dtle-dest.
	CharacterSetClient  string
	CollationConnection string
	CollationServer     string
}

func ParseQueryEventFlags(bs []byte, logger g.LoggerType) (r QueryEventFlags, err error) {
	logger = logger.Named("ParseQueryEventFlags")

	if logger.IsDebug() {
		logger.Debug("ParseQueryEventFlags", "bytes", hex.EncodeToString(bs))
	}

	// https://dev.mysql.com/doc/internals/en/query-event.html
	for i := 0; i < len(bs); {
		flag := bs[i]
		i += 1
		switch flag {
		case Q_FLAGS2_CODE: // Q_FLAGS2_CODE
			v := binary.LittleEndian.Uint32(bs[i : i+4])
			i += 4
			if v&OPTION_AUTO_IS_NULL != 0 {
				//log.Printf("OPTION_AUTO_IS_NULL")
			}
			if v&OPTION_NOT_AUTOCOMMIT != 0 {
				//log.Printf("OPTION_NOT_AUTOCOMMIT")
			}
			if v&OPTION_NO_FOREIGN_KEY_CHECKS != 0 {
				r.NoForeignKeyChecks = true
			}
			if v&OPTION_RELAXED_UNIQUE_CHECKS != 0 {
				//log.Printf("OPTION_RELAXED_UNIQUE_CHECKS")
			}
		case Q_SQL_MODE_CODE:
			_ = binary.LittleEndian.Uint64(bs[i : i+8])
			i += 8
		case Q_CATALOG:
			n := int(bs[i])
			i += 1
			i += n
			i += 1 // TODO What does 'only written length > 0' mean?
		case Q_AUTO_INCREMENT:
			i += 2 + 2
		case Q_CHARSET_CODE:
			cid := binary.LittleEndian.Uint16(bs[i:])
			i += 2
			c, err := parsercharset.GetCollationByID(int(cid))
			if err != nil {
				return r, err
			}
			r.CharacterSetClient = c.CharsetName

			cid = binary.LittleEndian.Uint16(bs[i:])
			i += 2
			c, err = parsercharset.GetCollationByID(int(cid))
			if err != nil {
				return r, err
			}
			r.CollationConnection = c.Name

			cid = binary.LittleEndian.Uint16(bs[i:])
			i += 2
			c, err = parsercharset.GetCollationByID(int(cid))
			if err != nil {
				return r, err
			}
			r.CollationServer = c.Name

		case Q_TIME_ZONE_CODE:
			n := int(bs[i])
			i += 1
			i += n
		case Q_CATALOG_NZ_CODE:
			length := int(bs[i])
			i += 1
			_ = string(bs[i : i+length])
			i += length
		case Q_LC_TIME_NAMES_CODE:
			i += 2
		case Q_CHARSET_DATABASE_CODE:
			i += 2
		case Q_TABLE_MAP_FOR_UPDATE_CODE:
			i += 8
		case Q_MASTER_DATA_WRITTEN_CODE:
			i += 4
		case Q_INVOKERS:
			n := int(bs[i])
			i += 1
			username := string(bs[i : i+n])
			i += n
			n = int(bs[i])
			i += 1
			hostname := string(bs[i : i+n])
			i += n
			logger.Debug("Q_INVOKERS", "username", username, "hostname", hostname)
		case Q_UPDATED_DB_NAMES:
			count := bs[i]
			i += 1
			const OVER_MAX_DBS_IN_EVENT_MTS = 254 // See #926
			if count != OVER_MAX_DBS_IN_EVENT_MTS {
				for j := uint8(0); j < count; j++ {
					i0 := i
					for bs[i] != 0 {
						i += 1
					}
					schemaName := string(bs[i0:i])
					logger.Debug("Q_UPDATED_DB_NAMES", "schema", schemaName, "i", i, "j", j)
					i += 1 // nul-terminated
				}
			}
		case Q_MICROSECONDS:
			i += 3
		case Q_COMMIT_TS:
			// not used in mysql
		case Q_COMMIT_TS2:
			// not used in mysql
		case Q_EXPLICIT_DEFAULTS_FOR_TIMESTAMP:
			i += 1
		case Q_DDL_LOGGED_WITH_XID:
			i += 8
		case Q_DEFAULT_COLLATION_FOR_UTF8MB4:
			i += 2
		case Q_SQL_REQUIRE_PRIMARY_KEY:
			i += 1
		case Q_DEFAULT_TABLE_ENCRYPTION:
			i += 1
		default:
			return r, fmt.Errorf("ParseQueryEventFlags. unknown flag 0x%x bytes %v",
				flag, hex.EncodeToString(bs))
		}
	}

	return r, nil
}

const (
	OPTION_AUTO_IS_NULL uint32 = 0x00004000
	OPTION_NOT_AUTOCOMMIT uint32 = 0x00080000
	OPTION_NO_FOREIGN_KEY_CHECKS uint32 = 0x04000000
	OPTION_RELAXED_UNIQUE_CHECKS uint32 = 0x08000000
)

const (
	// see mysql-server/libbinlogevents/include/statement_events.h
	Q_FLAGS2_CODE byte = iota
	Q_SQL_MODE_CODE
	Q_CATALOG
	Q_AUTO_INCREMENT
	Q_CHARSET_CODE
	Q_TIME_ZONE_CODE
	Q_CATALOG_NZ_CODE
	Q_LC_TIME_NAMES_CODE
	Q_CHARSET_DATABASE_CODE
	Q_TABLE_MAP_FOR_UPDATE_CODE
	Q_MASTER_DATA_WRITTEN_CODE
	Q_INVOKERS
	Q_UPDATED_DB_NAMES
	Q_MICROSECONDS
	Q_COMMIT_TS
	Q_COMMIT_TS2
	Q_EXPLICIT_DEFAULTS_FOR_TIMESTAMP
	Q_DDL_LOGGED_WITH_XID
	Q_DEFAULT_COLLATION_FOR_UTF8MB4
	Q_SQL_REQUIRE_PRIMARY_KEY
	Q_DEFAULT_TABLE_ENCRYPTION
)

const (
	RowsEventFlagEndOfStatement uint16 = 1
	RowsEventFlagNoForeignKeyChecks uint16 = 2
	RowsEventFlagNoUniqueKeyChecks uint16 = 4
	RowsEventFlagRowHasAColumns uint16 = 8
)
