package binlog

import (
	"fmt"

	"github.com/siddontang/go-mysql/replication"

	"udup/internal/config/mysql"
)

type EventDML string

const (
	NotDML    EventDML = "NoDML"
	InsertDML          = "Insert"
	UpdateDML          = "Update"
	DeleteDML          = "Delete"
)

type BinlogTx struct {
	SID            string
	GNO            int64
	LastCommitted  int64
	//Gtid           string
	// table -> [row]. row is identified by a hash of pk values.
	hasBeginQuery bool
	Impacting     map[uint64]([]string)
	Query         string
	Fde           string
	eventCount    int //for evaluate
	EventSize     uint64
	ErrorCode     uint16
}

type BinlogQuery struct {
	Sql string
	DML EventDML
}

type BinlogEvent struct {
	BinlogFile string
	RealPos    uint32
	Header     *replication.EventHeader
	Evt        replication.Event
	RawBs      []byte
	Query      []*BinlogQuery

	Err error
}

// BinlogDMLEvent is a binary log rows (DML) event entry, with data
type DataEvent struct {
	Query                   string
	DatabaseName            string
	TableName               string
	DML                     EventDML
	OriginalTableColumns    *mysql.ColumnList
	OriginalTableUniqueKeys [](*mysql.UniqueKey)
	WhereColumnValues       *mysql.ColumnValues
	NewColumnValues         []*mysql.ColumnValues
}

func NewDataEvent(query, databaseName, tableName string, dml EventDML, tableColumns *mysql.ColumnList, tableUniqueKey [](*mysql.UniqueKey)) DataEvent {
	event := DataEvent{
		Query:                   query,
		DatabaseName:            databaseName,
		TableName:               tableName,
		DML:                     dml,
		OriginalTableColumns:    tableColumns,
		OriginalTableUniqueKeys: tableUniqueKey,
	}
	return event
}

func (b *DataEvent) String() string {
	return fmt.Sprintf("[%+v on %s:%s]", b.DML, b.DatabaseName, b.TableName)
}
