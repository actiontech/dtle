package binlog

import (
	"fmt"

	"github.com/siddontang/go-mysql/replication"

	"strings"
	"udup/internal/config/mysql"
)

type EventDML string

const (
	NotDML    EventDML = "NoDML"
	InsertDML          = "Insert"
	UpdateDML          = "Update"
	DeleteDML          = "Delete"
)

func ToEventDML(description string) EventDML {
	// description can be a statement (`UPDATE my_table ...`) or a RBR event name (`UpdateRowsEventV2`)
	description = strings.TrimSpace(strings.Split(description, " ")[0])
	switch strings.ToLower(description) {
	case "insert":
		return InsertDML
	case "update":
		return UpdateDML
	case "delete":
		return DeleteDML
	}
	if strings.HasPrefix(description, "WriteRows") {
		return InsertDML
	}
	if strings.HasPrefix(description, "UpdateRows") {
		return UpdateDML
	}
	if strings.HasPrefix(description, "DeleteRows") {
		return DeleteDML
	}
	return NotDML
}

type BinlogTx struct {
	SID           string
	GNO           int64
	LastCommitted int64
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
	isDbExist               bool
	Query                   string
	DatabaseName            string
	TableName               string
	DML                     EventDML
	OriginalTableColumns    *mysql.ColumnList
	OriginalTableUniqueKeys [](*mysql.UniqueKey)
	WhereColumnValues       *mysql.ColumnValues
	NewColumnValues         *mysql.ColumnValues
}

func NewDataEvent(databaseName, tableName string, dml EventDML) DataEvent {
	event := DataEvent{
		DatabaseName: databaseName,
		TableName:    tableName,
		DML:          dml,
	}
	return event
}

func NewQueryEvent(dbName, query string, dml EventDML) DataEvent {
	event := DataEvent{
		DatabaseName: dbName,
		Query:        query,
		DML:          dml,
	}
	return event
}

func (b *DataEvent) String() string {
	return fmt.Sprintf("[%+v on %s:%s]", b.DML, b.DatabaseName, b.TableName)
}
