/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/actiontech/kafkas, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package binlog

import (
	"fmt"

	"github.com/siddontang/go-mysql/replication"

	config "github.com/actiontech/dtle/drivers/mysql/mysql/mysqlconfig"
)

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
	WhereColumnValues *config.ColumnValues
	NewColumnValues   *config.ColumnValues
	Table             *config.Table // TODO tmp solution
	LogPos            int64         // for kafkas. The pos of WRITE_ROW_EVENT
	TableItem         interface{}
}

func NewDataEvent(databaseName, tableName string, dml EventDML, columnCount int) DataEvent {
	event := DataEvent{
		DatabaseName: databaseName,
		TableName:    tableName,
		DML:          dml,
		ColumnCount:  columnCount,
	}
	return event
}

func NewQueryEvent(currentSchema, query string, dml EventDML) DataEvent {
	event := DataEvent{
		CurrentSchema: currentSchema,
		Query:         query,
		DML:           dml,
	}
	return event
}
func NewQueryEventAffectTable(currentSchema, query string, dml EventDML, affectedTable SchemaTable) DataEvent {
	event := DataEvent{
		CurrentSchema: currentSchema,
		DatabaseName:  affectedTable.Schema,
		TableName:     affectedTable.Table,
		Query:         query,
		DML:           dml,
	}
	return event
}

func (b *DataEvent) String() string {
	return fmt.Sprintf("[%+v on %s:%s]", b.DML, b.DatabaseName, b.TableName)
}
