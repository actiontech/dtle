package common

import (
	"database/sql"
	"fmt"
	"github.com/actiontech/dtle/drivers/mysql/mysql/mysqlconfig"
	"github.com/opentracing/opentracing-go"
	"github.com/satori/go.uuid"
	"github.com/siddontang/go-mysql/replication"
)

// Do not call this frequently. Cache your result.
func (b *BinlogCoordinateTx) GetSid() string {
	return uuid.UUID(b.SID).String()
}

// Equals tests equality of this corrdinate and another one.
func (b *BinlogCoordinateTx) Equals(other *BinlogCoordinateTx) bool {
	if other == nil {
		return false
	}
	return b.LogFile == other.LogFile && b.LogPos == other.LogPos
}

// SmallerThan returns true if this coordinate is strictly smaller than the other.
func (b *BinlogCoordinateTx) SmallerThan(other *BinlogCoordinateTx) bool {
	if b.LogFile < other.LogFile {
		return true
	}
	if b.LogFile == other.LogFile && b.LogPos < other.LogPos {
		return true
	}
	return false
}

// SmallerThanOrEquals returns true if this coordinate is the same or equal to the other one.
// We do NOT compare the type so we can not use b.Equals()
func (b *BinlogCoordinateTx) SmallerThanOrEquals(other *BinlogCoordinateTx) bool {
	if b.SmallerThan(other) {
		return true
	}
	return b.LogFile == other.LogFile && b.LogPos == other.LogPos // No Type comparison
}

func (b *BinlogCoordinateTx) GetGtidForThisTx() string {
	return fmt.Sprintf("%s:%d", b.GetSid(), b.GNO)
}

type BinlogEntries struct {
	Entries []*BinlogEntry
	BigTx   bool
	TxNum   int
	TxLen   int
}

// BinlogEntry describes an entry in the binary log
type BinlogEntry struct {
	Coordinates   BinlogCoordinateTx
	Events        []DataEvent
}

type BinlogEntryContext struct {
	Entry       *BinlogEntry
	SpanContext opentracing.SpanContext
	TableItems  []*ApplierTableItem
	OriginalSize  int // size of binlog entry
}

// NewBinlogEntry creates an empty, ready to go BinlogEntry object
func NewBinlogEntryAt(coordinates BinlogCoordinateTx) *BinlogEntry {
	binlogEntry := &BinlogEntry{
		Coordinates:  coordinates,
		Events:       make([]DataEvent, 0),
	}
	return binlogEntry
}

// Duplicate creates and returns a new binlog entry, with some of the attributes pre-assigned
func (b *BinlogEntry) String() string {
	return fmt.Sprintf("[BinlogEntry at %+v]", b.Coordinates)
}

type EventDML string

// TODO string vs int in serialized struct?
const (
	NotDML    EventDML = "NoDML"
	InsertDML          = "Insert"
	UpdateDML          = "Update"
	DeleteDML          = "Delete"
)

func ToEventDML(eventType replication.EventType) EventDML {
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

// BinlogDMLEvent is a binary log rows (DML) event entry, with data
type DataEvent struct {
	Query             string
	CurrentSchema     string
	DatabaseName      string
	TableName         string
	DML               EventDML
	ColumnCount       int
	WhereColumnValues *ColumnValues
	NewColumnValues   *ColumnValues
	Table             []byte // TODO tmp solution
	LogPos            int64  // for kafka. The pos of WRITE_ROW_EVENT
	Timestamp         uint32
}

func NewDataEvent(databaseName, tableName string, dml EventDML, columnCount int, timestamp uint32) DataEvent {
	event := DataEvent{
		DatabaseName: databaseName,
		TableName:    tableName,
		DML:          dml,
		ColumnCount:  columnCount,
		Timestamp:    timestamp,
	}
	return event
}

func NewQueryEvent(currentSchema, query string, dml EventDML, timestamp uint32) DataEvent {
	event := DataEvent{
		CurrentSchema: currentSchema,
		Query:         query,
		DML:           dml,
		Timestamp:     timestamp,
	}
	return event
}
func NewQueryEventAffectTable(currentSchema, query string, dml EventDML, affectedTable SchemaTable,
	timestamp uint32) DataEvent {

	event := DataEvent{
		CurrentSchema: currentSchema,
		DatabaseName:  affectedTable.Schema,
		TableName:     affectedTable.Table,
		Query:         query,
		DML:           dml,
		Timestamp:     timestamp,
	}
	return event
}

func (b *DataEvent) String() string {
	return fmt.Sprintf("[%+v on %s:%s]", b.DML, b.DatabaseName, b.TableName)
}

type ApplierTableItem struct {
	Columns  *mysqlconfig.ColumnList
	PsInsert []*sql.Stmt
	PsDelete []*sql.Stmt
	PsUpdate []*sql.Stmt
}

func NewApplierTableItem(parallelWorkers int) *ApplierTableItem {
	return &ApplierTableItem{
		Columns:  nil,
		PsInsert: make([]*sql.Stmt, parallelWorkers),
		PsDelete: make([]*sql.Stmt, parallelWorkers),
		PsUpdate: make([]*sql.Stmt, parallelWorkers),
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
	closeStmts(ait.PsInsert)
	closeStmts(ait.PsDelete)
	closeStmts(ait.PsUpdate)

	ait.Columns = nil
}
