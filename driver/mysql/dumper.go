/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package mysql

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"os"
	"strings"
	"sync/atomic"

	"github.com/actiontech/dtle/driver/common"

	"time"

	"github.com/actiontech/dtle/g"

	umconf "github.com/actiontech/dtle/driver/mysql/mysqlconfig"
	usql "github.com/actiontech/dtle/driver/mysql/sql"
)

type dumper struct {
	*common.Dumper
	EscapedTableSchema string
	EscapedTableName   string
	// DB is safe for using in goroutines
	// http://golang.org/src/database/sql/sql.go?s=5574:6362#L201
	db usql.QueryAble
	// 0: don't checksum; 1: checksum once; 2: checksum every time
	doChecksum int
	oldWayDump bool

	sentTableDef   bool
	dumpEntryLimit int
}

func NewDumper(ctx context.Context, db usql.QueryAble, table *common.Table, chunkSize int64,
	logger g.LoggerType, memory *int64, dumpEntryLimit int) *dumper {
	dumper := &dumper{
		common.NewDumper(ctx, table, chunkSize, logger, memory),
		umconf.EscapeName(table.TableSchema),
		umconf.EscapeName(table.TableName),
		db,
		0,
		false,
		false,
		dumpEntryLimit,
	}
	dumper.PrepareForDumping = dumper.prepareForDumping
	dumper.GetChunkData = dumper.getChunkData

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

func (d *dumper) prepareForDumping() error {
	needPm := false
	columns := make([]string, 0)
	for _, col := range d.Table.OriginalTableColumns.Columns {
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
		d.Columns = strings.Join(columns, ", ")
	} else {
		d.Columns = "*"
	}

	return nil
}

func (d *dumper) buildQueryOldWay() string {
	return fmt.Sprintf(`SELECT %s FROM %s.%s where (%s) LIMIT %d OFFSET %d`,
		d.Columns,
		d.EscapedTableSchema,
		d.EscapedTableName,
		d.Table.GetWhere(),
		d.ChunkSize,
		d.Iteration*d.ChunkSize,
	)
}

func (d *dumper) buildQueryOnUniqueKey() string {
	nCol := len(d.Table.UseUniqueKey.Columns.Columns)
	uniqueKeyColumnAscending := make([]string, nCol, nCol)
	for i, col := range d.Table.UseUniqueKey.Columns.Columns {
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

	if d.Iteration == 0 {
		rangeStr = "true"
	} else {
		rangeItems := make([]string, nCol)

		// The form like: (A > a) or (A = a and B > b) or (A = a and B = b and C > c) or ...
		for x := 0; x < nCol; x++ {
			innerItems := make([]string, x+1)

			for y := 0; y < x; y++ {
				colName := d.Table.UseUniqueKey.Columns.Columns[y].EscapedName
				innerItems[y] = fmt.Sprintf("(%s = %s)", colName, d.Table.UseUniqueKey.LastMaxVals[y])
			}

			colName := d.Table.UseUniqueKey.Columns.Columns[x].EscapedName
			innerItems[x] = fmt.Sprintf("(%s > %s)", colName, d.Table.UseUniqueKey.LastMaxVals[x])

			rangeItems[x] = fmt.Sprintf("(%s)", strings.Join(innerItems, " and "))
		}

		rangeStr = strings.Join(rangeItems, " or ")
	}

	return fmt.Sprintf(`SELECT %s FROM %s.%s where (%s) and (%s) order by %s LIMIT %d`,
		d.Columns,
		d.EscapedTableSchema,
		d.EscapedTableName,
		// where
		rangeStr, d.Table.GetWhere(),
		// order by
		strings.Join(uniqueKeyColumnAscending, ", "),
		// limit
		d.ChunkSize,
	)
}

// dumps a specific chunk, reading chunk info from the channel
func (d *dumper) getChunkData() (nRows int64, err error) {
	query := ""
	if d.oldWayDump || d.Table.UseUniqueKey == nil {
		query = d.buildQueryOldWay()
	} else {
		query = d.buildQueryOnUniqueKey()
	}
	d.Logger.Debug("getChunkData.", "query", query)

	if d.doChecksum != 0 {
		if d.doChecksum == 2 || (d.doChecksum == 1 && d.Iteration == 0) {
			row := d.db.QueryRow(fmt.Sprintf("checksum table %v.%v", d.TableSchema, d.TableName))
			var table string
			var cs int64
			err := row.Scan(&table, &cs)
			if err != nil {
				d.Logger.Debug("getChunkData checksum_table_err", "table", table, "err", err)
			} else {
				d.Logger.Debug("getChunkData checksum_table", "table", table, "cs", cs)
			}
		}
	}

	// this must be increased after building query
	d.Iteration += 1
	rows, err := d.db.Query(query)
	if err != nil {
		d.Logger.Error("error at select chunk", "query", query)
		return 0, errors.Wrap(err, "select chunk")
	}

	columns, err := rows.Columns()
	if err != nil {
		return 0, err
	}

	handleEntry := func(valuesX [][]*[]byte, last bool) error {
		if len(valuesX) == 0 {
			return nil
		}

		entry := &common.DumpEntry{
			TableSchema: g.StringElse(d.Table.TableSchemaRename, d.TableSchema),
			TableName:   g.StringElse(d.Table.TableRename, d.TableName),
			ColumnMapTo: d.Table.ColumnMapTo,
			ValuesX:     valuesX,
		}

		if last {
			var lastVals []string
			for _, col := range entry.ValuesX[len(entry.ValuesX)-1] {
				lastVals = append(lastVals, usql.EscapeColRawToString(col))
			}

			if d.Table.UseUniqueKey != nil {
				// lastVals must not be nil if len(data) > 0
				for i, col := range d.Table.UseUniqueKey.Columns.Columns {
					// TODO save the idx
					idx := d.Table.OriginalTableColumns.Ordinals[col.RawName]
					if idx > len(lastVals) {
						return fmt.Errorf("getChunkData. GetLastMaxVal: column index %v > n_column %v", idx, len(lastVals))
					} else {
						d.Table.UseUniqueKey.LastMaxVals[i] = lastVals[idx]
					}
				}
				d.Logger.Debug("GetLastMaxVal", "val", d.Table.UseUniqueKey.LastMaxVals)
			}
		}
		if len(d.Table.ColumnMap) > 0 {
			for i, oldRow := range entry.ValuesX {
				row := make([]*[]byte, len(d.Table.ColumnMap))
				for i := range d.Table.ColumnMap {
					fromIdx := d.Table.ColumnMap[i]
					row[i] = oldRow[fromIdx]
				}
				entry.ValuesX[i] = row
			}
		}

		keepGoing := true
		timer := time.NewTicker(pingInterval)
		defer timer.Stop()
		for keepGoing {
			select {
			case <-d.ShutdownCh:
				keepGoing = false
			case d.ResultsChannel <- entry:
				atomic.AddInt64(d.Memory, int64(entry.Size()))
				//d.logger.Debug("*** memory", "memory", atomic.LoadInt64(d.memory))
				keepGoing = false
			case <-timer.C:
				d.Logger.Debug("resultsChannel full. waiting and ping conn")
				var dummy int
				errPing := d.db.QueryRow("select 1").Scan(&dummy)
				if errPing != nil {
					d.Logger.Debug("ping query row got error.", "err", errPing)
				}
			}
		}
		d.Logger.Debug("resultsChannel", "n", len(d.ResultsChannel))

		return nil
	}

	entrySize := 0
	var valuesX [][]*[]byte
	scanArgs := make([]interface{}, len(columns)) // tmp use, for casting `values` to `[]interface{}`
	splitPoints := []int{0}
	hasRow := rows.Next()
	for hasRow {
		rowValuesRaw := make([]*[]byte, len(columns))
		for i := range rowValuesRaw {
			scanArgs[i] = &rowValuesRaw[i]
		}

		err = rows.Scan(scanArgs...)
		if err != nil {
			return 0, err
		}

		hasRow = rows.Next()

		valuesX = append(valuesX, rowValuesRaw)
		entrySize += getRowSize(rowValuesRaw)

		if !hasRow || entrySize >= d.dumpEntryLimit {
			d.Logger.Debug("reach dumpEntryLimit.", "size", entrySize, "point", len(valuesX))
			splitPoints = append(splitPoints, len(valuesX))
			entrySize = 0
		}
	}
	if err = rows.Err(); err != nil {
		return 0, err
	}
	nRows = int64(len(valuesX))

	for i := 1; i < len(splitPoints); i++ {
		err = handleEntry(valuesX[splitPoints[i-1]:splitPoints[i]], i == len(splitPoints)-1)
		if err != nil {
			return 0, err
		}
	}

	d.Logger.Debug("getChunkData.", "n_row", nRows)

	return nRows, nil
}

func getRowSize(row []*[]byte) (size int) {
	for i := range row {
		if row[i] == nil {
			size += 0
		} else {
			size += len(*row[i])
		}
	}
	return size
}
