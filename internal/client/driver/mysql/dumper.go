/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package mysql

import (
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/actiontech/dtle/internal/g"

	"time"

	usql "github.com/actiontech/dtle/internal/client/driver/mysql/sql"
	"github.com/actiontech/dtle/internal/config"
	umconf "github.com/actiontech/dtle/internal/config/mysql"
	log "github.com/actiontech/dtle/internal/logger"
)

type dumper struct {
	logger         *log.Entry
	chunkSize      int64
	TableSchema    string
	TableName      string
	table          *config.Table
	columns        string
	resultsChannel chan *DumpEntry
	shutdown       bool
	shutdownCh     chan struct{}
	shutdownLock   sync.Mutex

	// DB is safe for using in goroutines
	// http://golang.org/src/database/sql/sql.go?s=5574:6362#L201
	db usql.QueryAble

	// 0: don't checksum; 1: checksum once; 2: checksum every time
	doChecksum int
	oldWayDump bool
}

func NewDumper(db usql.QueryAble, table *config.Table, chunkSize int64,
	logger *log.Entry) *dumper {

	dumper := &dumper{
		logger:         logger,
		db:             db,
		TableSchema:    table.TableSchema,
		TableName:      table.TableName,
		table:          table,
		resultsChannel: make(chan *DumpEntry, 24),
		chunkSize:      chunkSize,
		shutdownCh:     make(chan struct{}),
	}
	switch os.Getenv(g.ENV_DUMP_CHECKSUM) {
	case "1":
		dumper.doChecksum = 1
	case "2":
		dumper.doChecksum = 2
	default:
		dumper.doChecksum = 0
	}
	if os.Getenv(g.ENV_DUMP_OLDWAY) != "" {
		dumper.oldWayDump = true
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
	// For each `*interface{}` item, it is ensured to be not nil.
	// If field is sql-NULL, *item is nil. Else, *item is a `[]byte`.
	// TODO can we just use interface{}? Make sure it is not copied again and again.
	ValuesX    [][]*interface{}
	TotalCount int64
	RowsCount  int64
	Err        error
	Table      *config.Table
}

func (e *DumpEntry) incrementCounter() {
	e.RowsCount++
}

func (d *dumper) prepareForDumping() error {
	needPm := false
	columns := make([]string, 0)
	for _, col := range d.table.OriginalTableColumns.Columns {
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

	return nil
}

func (d *dumper) buildQueryOldWay() string {
	return fmt.Sprintf(`SELECT %s FROM %s.%s where (%s) LIMIT %d OFFSET %d`,
		d.columns,
		usql.EscapeName(d.TableSchema),
		usql.EscapeName(d.TableName),
		d.table.Where,
		d.chunkSize,
		d.table.Iteration*d.chunkSize,
	)
}

func (d *dumper) buildQueryOnUniqueKey() string {
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

	return fmt.Sprintf(`SELECT %s FROM %s.%s where (%s) and (%s) order by %s LIMIT %d`,
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
func (d *dumper) getChunkData() (nRows int64, err error) {
	entry := &DumpEntry{
		TableSchema: d.TableSchema,
		TableName:   d.TableName,
		RowsCount:   0,
	}
	// TODO use PS
	// TODO escape schema/table/column name once and save
	defer func() {
		entry.Err = err
		if err == nil && entry.RowsCount == 0 {
			return
		}

		keepGoing := true
		timer := time.NewTimer(pingInterval)
		for keepGoing {
			select {
			case d.resultsChannel <- entry:
				if !timer.Stop() {
					<-timer.C
				}
				keepGoing = false
			case <-timer.C:
				timer.Reset(pingInterval)
				d.logger.Debugf("mysql.dumper: resultsChannel full. waiting and ping conn")
				var dummy int
				errPing := d.db.QueryRow("select 1").Scan(&dummy)
				if errPing != nil {
					d.logger.Debugf("mysql.dumper: ping query row got error. err: %v", errPing)
				}
			}
		}
		d.logger.Debugf("mysql.dumper: resultsChannel: %v", len(d.resultsChannel))
	}()

	query := ""
	if d.oldWayDump || d.table.UseUniqueKey == nil {
		query = d.buildQueryOldWay()
	} else {
		query = d.buildQueryOnUniqueKey()
	}
	d.logger.Debugf("getChunkData. query: %s", query)

	if d.doChecksum != 0 {
		if d.doChecksum == 2 || (d.doChecksum == 1 && d.table.Iteration == 0) {
			row := d.db.QueryRow(fmt.Sprintf("checksum table %v.%v", d.TableSchema, d.TableName))
			var table string
			var cs int64
			err := row.Scan(&table, &cs)
			if err != nil {
				d.logger.Debugf("getChunkData checksum_table_err %v %v", table, err)
			} else {
				d.logger.Debugf("getChunkData checksum_table %v %v", table, cs)
			}
		}
	}

	// this must be increased after building query
	d.table.Iteration += 1
	rows, err := d.db.Query(query)
	if err != nil {
		d.logger.Debugf("mysql.dumper. error at select chunk. query: ", query)
		newErr := fmt.Errorf("mysql.dumper. error at select chunk. err: %v", err)
		d.logger.Errorf(newErr.Error())
		return 0, err
	}

	columns, err := rows.Columns()
	if err != nil {
		return 0, err
	}

	scanArgs := make([]interface{}, len(columns)) // tmp use, for casting `values` to `[]interface{}`

	interfacePtrWithNil := new(interface{})

	for rows.Next() {
		rowValuesRaw := make([]*interface{}, len(columns))
		for i := range rowValuesRaw {
			scanArgs[i] = &rowValuesRaw[i]
		}

		err = rows.Scan(scanArgs...)
		if err != nil {
			return 0, err
		}

		for i := range rowValuesRaw {
			if rowValuesRaw[i] == nil {
				rowValuesRaw[i] = interfacePtrWithNil
			}
		}
		entry.ValuesX = append(entry.ValuesX, rowValuesRaw)

		entry.incrementCounter()
	}

	d.logger.Debugf("getChunkData. n_row: %d", entry.RowsCount)

	if entry.RowsCount > 0 {
		var lastVals []string

		for _, col := range entry.ValuesX[len(entry.ValuesX)-1] {
			lastVals = append(lastVals, usql.EscapeColRawToString(col))
		}

		if d.table.UseUniqueKey != nil {
			// lastVals must not be nil if len(data) > 0
			for i, col := range d.table.UseUniqueKey.Columns.Columns {
				// TODO save the idx
				idx := d.table.OriginalTableColumns.Ordinals[strings.ToLower(col.Name)]
				if idx > len(lastVals) {
					return entry.RowsCount, fmt.Errorf("getChunkData. GetLastMaxVal: column index %v > n_column %v", idx, len(lastVals))
				} else {
					d.table.UseUniqueKey.LastMaxVals[i] = lastVals[idx]
				}
			}
			d.logger.Debugf("GetLastMaxVal: got %v", d.table.UseUniqueKey.LastMaxVals)
		}
	}
	if d.table.TableRename != "" {
		entry.TableName = d.table.TableRename
	}
	if d.table.TableSchemaRename != "" {
		entry.TableSchema = d.table.TableSchemaRename
	}
	// ValuesX[i]: n-th row
	// ValuesX[i][j]: j-th col of n-th row
	// Values[i]: i-th chunk of rows
	// Values[i][j]: j-th row (in paren-wrapped string)

	return entry.RowsCount, nil
}

func (d *dumper) Dump() error {
	err := d.prepareForDumping()
	if err != nil {
		return err
	}

	go func() {
		for {
			select {
			case <-d.shutdownCh:
				return
			default:
			}

			nRows, err := d.getChunkData()
			if err != nil {
				d.logger.Errorf("mysql.dumper: error at dump %v", err)
				break
			}

			if nRows < d.chunkSize {
				// If nRows < d.chunkSize while there are still more rows, it is a possible mysql bug.
				d.logger.Infof("mysql.dumper: nRows < d.chunkSize. %v %v", nRows, d.chunkSize)
			}
			if nRows == 0 {
				d.logger.Infof("mysql.dumper: nRows == 0. dump finished. %v %v", nRows, d.chunkSize)
				break
			}
		}
		close(d.resultsChannel)
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
