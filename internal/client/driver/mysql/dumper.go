/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package mysql

import (
	"bytes"
	"fmt"
	"math"
	"strings"
	"sync"

	"time"
	ubase "udup/internal/client/driver/mysql/base"
	usql "udup/internal/client/driver/mysql/sql"
	"udup/internal/config"
	umconf "udup/internal/config/mysql"
	log "udup/internal/logger"
)

type dumper struct {
	logger         *log.Entry
	chunkSize      int64
	total          int64
	TableSchema    string
	TableName      string
	table          *config.Table
	columns        string
	entriesCount   int
	resultsChannel chan *DumpEntry
	entriesChannel chan *DumpEntry
	shutdown       bool
	shutdownCh     chan struct{}
	shutdownLock   sync.Mutex

	// DB is safe for using in goroutines
	// http://golang.org/src/database/sql/sql.go?s=5574:6362#L201
	db usql.QueryAble
}

func NewDumper(db usql.QueryAble, table *config.Table, total, chunkSize int64,
	logger *log.Entry) *dumper {
	dumper := &dumper{
		logger:         logger,
		db:             db,
		TableSchema:    table.TableSchema,
		TableName:      table.TableName,
		table:          table,
		total:          total,
		resultsChannel: make(chan *DumpEntry, 24),
		entriesChannel: make(chan *DumpEntry),
		chunkSize:      chunkSize,
		shutdownCh:     make(chan struct{}),
	}
	return dumper
}

type dumpStatResult struct {
	Gtid       string
	TotalCount int64
}

type DumpEntry struct {
	SystemVariablesStatement string
	SqlMode                  string
	DbSQL                    string
	TableName                string
	TableSchema              string
	TbSQL                    []string
	ValuesX                  [][]*[]byte
	TotalCount               int64
	RowsCount                int64
	Offset                   uint64 // only for 'no PK' table
	colBuffer                bytes.Buffer
	err                      error
}

func (e *DumpEntry) incrementCounter() {
	e.RowsCount++
}

func (d *dumper) getDumpEntries() ([]*DumpEntry, error) {
	if d.total == 0 {
		return []*DumpEntry{}, nil
	}

	columnList, err := ubase.GetTableColumns(d.db, d.TableSchema, d.TableName)
	if err != nil {
		return []*DumpEntry{}, err
	}

	if err := ubase.ApplyColumnTypes(d.db, d.TableSchema, d.TableName, columnList); err != nil {
		return []*DumpEntry{}, err
	}

	needPm := false
	columns := make([]string, 0)
	for _, col := range columnList.Columns {
		switch col.Type {
		case umconf.FloatColumnType, umconf.DoubleColumnType,
			umconf.MediumIntColumnType, umconf.BigIntColumnType,
			umconf.DecimalColumnType:
			columns = append(columns, fmt.Sprintf("`%s`+0", col.Name))
			needPm = true
		default:
			columns = append(columns, fmt.Sprintf("`%s`", col.Name))
		}
	}
	if needPm {
		d.columns = strings.Join(columns, ", ")
	} else {
		d.columns = "*"
	}

	sliceCount := int(math.Ceil(float64(d.total) / float64(d.chunkSize)))
	if sliceCount == 0 {
		sliceCount = 1
	}
	entries := make([]*DumpEntry, sliceCount)
	for i := 0; i < sliceCount; i++ {
		offset := uint64(i) * uint64(d.chunkSize)
		entries[i] = &DumpEntry{
			Offset: offset,
		}
	}
	return entries, nil
}

func (d *dumper) buildQueryOldWay(e *DumpEntry) string {
	return fmt.Sprintf(`SELECT %s FROM %s.%s where (%s) LIMIT %d OFFSET %d`,
		d.columns,
		usql.EscapeName(d.TableSchema),
		usql.EscapeName(d.TableName),
		d.table.Where,
		d.chunkSize,
		e.Offset,
	)
}

func (d *dumper) buildQueryOnUniqueKey(e *DumpEntry) string {
	nCol := len(d.table.UseUniqueKey.Columns.Columns)
	uniqueKeyColumnAscending := make([]string, nCol, nCol)
	for i, col := range d.table.UseUniqueKey.Columns.Columns {
		colName := usql.EscapeName(col.Name)
		switch col.Type {
		case umconf.EnumColumnType:
			// TODO try mysql enum type
			uniqueKeyColumnAscending[i] = fmt.Sprintf("concat(%s) asc", colName)
		default:
			uniqueKeyColumnAscending[i] = fmt.Sprintf("%s asc", colName)
		}
	}

	var rangeStr string

	if d.table.Iteration == 0 {
		rangeStr = "true"
	} else {
		rangeItems := make([]string, nCol)

		// The form like: (A > a) or (A = a and B > b) or (A = a and B = b and C > c) or ...
		for x := 0; x < nCol; x++ {
			innerItems := make([]string, x+1)

			for y := 0; y < x; y++ {
				colName := usql.EscapeName(d.table.UseUniqueKey.Columns.Columns[y].Name)
				innerItems[y] = fmt.Sprintf("(%s = %s)", colName, d.table.UseUniqueKey.LastMaxVals[y])
			}

			colName := usql.EscapeName(d.table.UseUniqueKey.Columns.Columns[x].Name)
			innerItems[x] = fmt.Sprintf("(%s > %s)", colName, d.table.UseUniqueKey.LastMaxVals[x])

			rangeItems[x] = fmt.Sprintf("(%s)", strings.Join(innerItems, " and "))
		}

		rangeStr = strings.Join(rangeItems, " or ")
	}

	return fmt.Sprintf(`SELECT %s FROM %s.%s where %s and (%s) order by %s LIMIT %d`,
		d.columns,
		usql.EscapeName(d.TableSchema),
		usql.EscapeName(d.TableName),
		// where
		rangeStr, d.table.Where,
		// order by
		strings.Join(uniqueKeyColumnAscending, ", "),
		// limit
		d.chunkSize,
	)
}

// dumps a specific chunk, reading chunk info from the channel
func (d *dumper) getChunkData(e *DumpEntry) (err error) {
	entry := &DumpEntry{
		TableSchema: d.TableSchema,
		TableName:   d.TableName,
		RowsCount:   e.RowsCount,
		Offset:      e.Offset,
	}
	// TODO use PS
	// TODO escape schema/table/column name once and save
	defer func() {
		entry.err = err
		keepGoing := true
		for keepGoing {
			select {
			case d.resultsChannel <- entry:
				keepGoing = false
			case <-time.After(5 * time.Second):
				d.logger.Debugf("mysql.dumper: resultsChannel full. waiting")
			}
		}
		d.logger.Debugf("mysql.dumper: resultsChannel: %v", len(d.resultsChannel))
	}()

	query := ""
	if d.table.UseUniqueKey == nil {
		query = d.buildQueryOldWay(e)
	} else {
		query = d.buildQueryOnUniqueKey(e)
	}
	d.logger.Debugf("getChunkData. query: %s", query)

	d.table.Iteration += 1
	rows, err := d.db.Query(query)
	if err != nil {
		return fmt.Errorf("exec [%s] error: %v", query, err)
	}

	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	//packetLen := 0

	nRows := 0

	scanArgs := make([]interface{}, len(columns)) // tmp use, for casting `values` to `[]interface{}`

	for rows.Next() {
		rowValuesRaw := make([]*[]byte, len(columns))
		for i := range rowValuesRaw {
			scanArgs[i] = &rowValuesRaw[i]
		}

		err = rows.Scan(scanArgs...)
		if err != nil {
			return err
		}

		entry.ValuesX = append(entry.ValuesX, rowValuesRaw)

		nRows += 1
		entry.incrementCounter()
	}

	d.logger.Debugf("getChunkData. n_row: %d", nRows)

	// TODO getChunkData could get 0 rows. Esp after removing 'start transaction'.
	if nRows == 0 {
		return fmt.Errorf("getChunkData. GetLastMaxVal: no rows found")
	}

	if nRows > 0 {
		var lastVals []string

		for _, col := range entry.ValuesX[len(entry.ValuesX)-1] {
			lastVals = append(lastVals, usql.EscapeColRawToString(col))
		}

		if d.table.UseUniqueKey != nil {
			// lastVals must not be nil if len(data) > 0
			for i, col := range d.table.UseUniqueKey.Columns.Columns {
				// TODO save the idx
				idx := d.table.OriginalTableColumns.Ordinals[col.Name]
				if idx > len(lastVals) {
					return fmt.Errorf("getChunkData. GetLastMaxVal: column index %v > n_column %v", idx, len(lastVals))
				} else {
					d.table.UseUniqueKey.LastMaxVals[i] = lastVals[idx]
				}
			}
			d.logger.Debugf("GetLastMaxVal: got %v", d.table.UseUniqueKey.LastMaxVals)
		}
	}

	// ValuesX[i]: n-th row
	// ValuesX[i][j]: j-th col of n-th row
	// Values[i]: i-th chunk of rows
	// Values[i][j]: j-th row (in paren-wrapped string)

	return nil
}

func (d *dumper) worker() {
	for e := range d.entriesChannel {
		select {
		case <-d.shutdownCh:
			return
		default:
		}
		if e != nil {
			err := d.getChunkData(e)
			//FIXME: useless err
			if err != nil {
				e.err = err
			}
			//d.resultsChannel <- e
		}
	}
}

func (d *dumper) Dump(w int) error {
	entries, err := d.getDumpEntries()
	if err != nil {
		return err
	}

	if len(entries) == 0 {
		return nil
	}

	workersCount := int(math.Min(float64(w), float64(len(entries))))
	if workersCount < 1 {
		return nil
	}

	d.entriesCount = len(entries)
	for i := 0; i < workersCount; i++ {
		go d.worker()
	}

	go func() {
		for _, e := range entries {
			d.entriesChannel <- e
		}
		close(d.entriesChannel)
	}()

	return nil
}

func (d *dumper) Close() error {
	// Quit goroutine
	d.shutdownLock.Lock()
	defer d.shutdownLock.Unlock()

	if d.shutdown {
		return nil
	}
	d.shutdown = true
	close(d.shutdownCh)
	return nil
}
