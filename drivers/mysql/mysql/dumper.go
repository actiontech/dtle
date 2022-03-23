/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package mysql

import (
	"fmt"
	"github.com/actiontech/dtle/drivers/mysql/common"
	"os"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/actiontech/dtle/g"
	"time"

	umconf "github.com/actiontech/dtle/drivers/mysql/mysql/mysqlconfig"
	usql "github.com/actiontech/dtle/drivers/mysql/mysql/sql"
)

type dumper struct {
	logger             g.LoggerType
	chunkSize          int64
	TableSchema        string
	EscapedTableSchema string
	TableName          string
	EscapedTableName   string
	table              *common.Table
	iteration          int64
	columns            string
	resultsChannel     chan *common.DumpEntry
	shutdown           bool
	shutdownCh         chan struct{}
	shutdownLock       sync.Mutex

	// DB is safe for using in goroutines
	// http://golang.org/src/database/sql/sql.go?s=5574:6362#L201
	db usql.QueryAble

	// 0: don't checksum; 1: checksum once; 2: checksum every time
	doChecksum int
	oldWayDump bool

	sentTableDef bool

	memory *int64
}

func NewDumper(db usql.QueryAble, table *common.Table, chunkSize int64,
	logger g.LoggerType, memory *int64) *dumper {

	dumper := &dumper{
		logger:             logger,
		db:                 db,
		TableSchema:        table.TableSchema,
		EscapedTableSchema: umconf.EscapeName(table.TableSchema),
		TableName:          table.TableName,
		EscapedTableName:   umconf.EscapeName(table.TableName),
		table:              table,
		resultsChannel:     make(chan *common.DumpEntry, 24),
		chunkSize:          chunkSize,
		shutdownCh:         make(chan struct{}),
		sentTableDef:       false,
		memory:             memory,
	}
	switch os.Getenv(g.ENV_DUMP_CHECKSUM) {
	case "1":
		dumper.doChecksum = 1
	case "2":
		dumper.doChecksum = 2
	default:
		dumper.doChecksum = 0
	}
	if g.EnvIsTrue(g.ENV_DUMP_OLDWAY) {
		dumper.oldWayDump = true
	}

	return dumper
}

type DumpEntryOrig struct {
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
	Table      *common.Table
}

func (d *dumper) prepareForDumping() error {
	needPm := false
	columns := make([]string, 0)
	for _, col := range d.table.OriginalTableColumns.Columns {
		switch col.Type {
		case umconf.FloatColumnType, umconf.DoubleColumnType,
			umconf.MediumIntColumnType, umconf.BigIntColumnType,
			umconf.DecimalColumnType:
			columns = append(columns, fmt.Sprintf("%s+0", col.EscapedName))
			needPm = true
		default:
			columns = append(columns, col.EscapedName)
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
		d.EscapedTableSchema,
		d.EscapedTableName,
		d.table.GetWhere(),
		d.chunkSize,
		d.iteration * d.chunkSize,
	)
}

func (d *dumper) buildQueryOnUniqueKey() string {
	nCol := len(d.table.UseUniqueKey.Columns.Columns)
	uniqueKeyColumnAscending := make([]string, nCol, nCol)
	for i, col := range d.table.UseUniqueKey.Columns.Columns {
		colName := col.EscapedName
		switch col.Type {
		case umconf.EnumColumnType:
			// TODO try mysql enum type
			uniqueKeyColumnAscending[i] = fmt.Sprintf("concat(%s) asc", colName)
		default:
			uniqueKeyColumnAscending[i] = fmt.Sprintf("%s asc", colName)
		}
	}

	var rangeStr string

	if d.iteration == 0 {
		rangeStr = "true"
	} else {
		rangeItems := make([]string, nCol)

		// The form like: (A > a) or (A = a and B > b) or (A = a and B = b and C > c) or ...
		for x := 0; x < nCol; x++ {
			innerItems := make([]string, x+1)

			for y := 0; y < x; y++ {
				colName := d.table.UseUniqueKey.Columns.Columns[y].EscapedName
				innerItems[y] = fmt.Sprintf("(%s = %s)", colName, d.table.UseUniqueKey.LastMaxVals[y])
			}

			colName := d.table.UseUniqueKey.Columns.Columns[x].EscapedName
			innerItems[x] = fmt.Sprintf("(%s > %s)", colName, d.table.UseUniqueKey.LastMaxVals[x])

			rangeItems[x] = fmt.Sprintf("(%s)", strings.Join(innerItems, " and "))
		}

		rangeStr = strings.Join(rangeItems, " or ")
	}

	return fmt.Sprintf(`SELECT %s FROM %s.%s where (%s) and (%s) order by %s LIMIT %d`,
		d.columns,
		d.EscapedTableSchema,
		d.EscapedTableName,
		// where
		rangeStr, d.table.GetWhere(),
		// order by
		strings.Join(uniqueKeyColumnAscending, ", "),
		// limit
		d.chunkSize,
	)
}

// dumps a specific chunk, reading chunk info from the channel
func (d *dumper) getChunkData() (nRows int64, err error) {
	entry := &common.DumpEntry{
		TableSchema: d.TableSchema,
		TableName:   d.TableName,
	}
	defer func() {
		if err != nil {
			entry.Err = err.Error()
		}
		if err == nil && len(entry.ValuesX) == 0 {
			return
		}

		keepGoing := true
		timer := time.NewTicker(pingInterval)
		defer timer.Stop()
		for keepGoing {
			select {
			case <-d.shutdownCh:
				keepGoing = false
			case d.resultsChannel <- entry:
				atomic.AddInt64(d.memory, int64(entry.Size()))
				//d.logger.Debug("*** memory", "memory", atomic.LoadInt64(d.memory))
				keepGoing = false
			case <-timer.C:
				d.logger.Debug("resultsChannel full. waiting and ping conn")
				var dummy int
				errPing := d.db.QueryRow("select 1").Scan(&dummy)
				if errPing != nil {
					d.logger.Debug("ping query row got error.", "err", errPing)
				}
			}
		}
		d.logger.Debug("resultsChannel", "n", len(d.resultsChannel))
	}()

	query := ""
	if d.oldWayDump || d.table.UseUniqueKey == nil {
		query = d.buildQueryOldWay()
	} else {
		query = d.buildQueryOnUniqueKey()
	}
	d.logger.Debug("getChunkData.", "query", query)

	if d.doChecksum != 0 {
		if d.doChecksum == 2 || (d.doChecksum == 1 && d.iteration == 0) {
			row := d.db.QueryRow(fmt.Sprintf("checksum table %v.%v", d.TableSchema, d.TableName))
			var table string
			var cs int64
			err := row.Scan(&table, &cs)
			if err != nil {
				d.logger.Debug("getChunkData checksum_table_err", "table", table, "err", err)
			} else {
				d.logger.Debug("getChunkData checksum_table", "table", table, "cs", cs)
			}
		}
	}

	// this must be increased after building query
	d.iteration += 1
	rows, err := d.db.Query(query)
	if err != nil {
		d.logger.Debug("error at select chunk. query: ", query)
		newErr := fmt.Errorf("error at select chunk. err: %v", err)
		d.logger.Error(newErr.Error())
		return 0, err
	}

	columns, err := rows.Columns()
	if err != nil {
		return 0, err
	}

	scanArgs := make([]interface{}, len(columns)) // tmp use, for casting `values` to `[]interface{}`

	for rows.Next() {
		rowValuesRaw := make([]*[]byte, len(columns))
		for i := range rowValuesRaw {
			scanArgs[i] = &rowValuesRaw[i]
		}

		err = rows.Scan(scanArgs...)
		if err != nil {
			return 0, err
		}

		entry.ValuesX = append(entry.ValuesX, rowValuesRaw)
	}

	nRows = int64(len(entry.ValuesX))
	d.logger.Debug("getChunkData.", "n_row", nRows)

	if nRows > 0 {
		var lastVals []string

		for _, col := range entry.ValuesX[len(entry.ValuesX)-1] {
			lastVals = append(lastVals, usql.EscapeColRawToString(col))
		}

		if d.table.UseUniqueKey != nil {
			// lastVals must not be nil if len(data) > 0
			for i, col := range d.table.UseUniqueKey.Columns.Columns {
				// TODO save the idx
				idx := d.table.OriginalTableColumns.Ordinals[col.RawName]
				if idx > len(lastVals) {
					return nRows, fmt.Errorf("getChunkData. GetLastMaxVal: column index %v > n_column %v", idx, len(lastVals))
				} else {
					d.table.UseUniqueKey.LastMaxVals[i] = lastVals[idx]
				}
			}
			d.logger.Debug("GetLastMaxVal", "val", d.table.UseUniqueKey.LastMaxVals)
		}
	}
	if d.table.TableRename != "" {
		entry.TableName = d.table.TableRename
	}
	if d.table.TableSchemaRename != "" {
		entry.TableSchema = d.table.TableSchemaRename
	}
	if len(d.table.ColumnMap) > 0 {
		for i, oldRow := range entry.ValuesX {
			row := make([]*[]byte, len(d.table.ColumnMap))
			for i := range d.table.ColumnMap {
				fromIdx := d.table.ColumnMap[i]
				row[i] = oldRow[fromIdx]
			}
			entry.ValuesX[i] = row
		}
	}
	// ValuesX[i]: n-th row
	// ValuesX[i][j]: j-th col of n-th row
	// Values[i]: i-th chunk of rows
	// Values[i][j]: j-th row (in paren-wrapped string)

	return nRows, nil
}

func (d *dumper) Dump() error {
	err := d.prepareForDumping()
	if err != nil {
		return err
	}

	go func() {
		defer close(d.resultsChannel)
		for {
			select {
			case <-d.shutdownCh:
				return
			default:
			}

			nRows, err := d.getChunkData()
			if err != nil {
				d.logger.Error("error at dump", "err", err)
				break
			}

			if nRows < d.chunkSize {
				// If nRows < d.chunkSize while there are still more rows, it is a possible mysql bug.
				d.logger.Info("nRows < d.chunkSize.", "nRows", nRows, "chunkSize", d.chunkSize)
			}
			if nRows == 0 {
				d.logger.Info("nRows == 0. dump finished.", "nRows", nRows, "chunkSize", d.chunkSize)
				break
			}
		}
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
