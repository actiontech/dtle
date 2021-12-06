package common

import (
	"database/sql"
	"fmt"
	"github.com/satori/go.uuid"
	"github.com/go-mysql-org/go-mysql/replication"
)

// Do not call this frequently. Cache your result.
func (b *BinlogCoordinateTx) GetSid() string {
	return uuid.UUID(b.SID).String()
}

func (b *BinlogCoordinateTx) GetGtidForThisTx() string {
	return fmt.Sprintf("%s:%d", b.GetSid(), b.GNO)
}

type BinlogEntryContext struct {
	Entry       *BinlogEntry
	// Only a DML has a tableItem. For a DDL, its tableItem is nil.
	TableItems  []*ApplierTableItem
	OriginalSize  int // size of binlog entry
}

func NewBinlogEntry() *BinlogEntry {
	binlogEntry := &BinlogEntry{
		Events:       make([]DataEvent, 0),
	}
	return binlogEntry
}

func (b *BinlogEntry) HasDDL() bool {
	for i := range b.Events {
		switch b.Events[i].DML {
		case NotDML:
			return true
		default:
		}
	}
	return false
}

func (b *BinlogEntry) IsPartOfBigTx() bool {
	return !(b.Index == 0 && b.Final)
}

// Duplicate creates and returns a new binlog entry, with some of the attributes pre-assigned
func (b *BinlogEntry) String() string {
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

func NewDataEvent(databaseName, tableName string, dml int8, columnCount uint64, timestamp uint32) DataEvent {
	event := DataEvent{
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
	Columns  *ColumnList
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

// String returns a user-friendly string representation of these coordinates
func (b BinlogCoordinatesX) String() string {
	return fmt.Sprintf("%v", b.GtidSet)
}

// IsEmpty returns true if the log file is empty, unnamed
func (b *BinlogCoordinatesX) IsEmpty() bool {
	return b.GtidSet == "" && b.LogFile == ""
}

func (b *BinlogCoordinatesX) CompareFilePos(other *BinlogCoordinatesX) int {
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

