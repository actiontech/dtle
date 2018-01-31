package kafka

import (
	"sync"
	"fmt"
	"strings"
)

import (
	"database/sql"
	"math"
	usql "udup/internal/client/driver/mysql/sql"
	"udup/internal/config"
	umconf "udup/internal/config/mysql"
	log "udup/internal/logger"
)

var (
	stringOfBackslashAndQuoteChars = "\u005c\u00a5\u0160\u20a9\u2216\ufe68uff3c\u0022\u0027\u0060\u00b4\u02b9\u02ba\u02bb\u02bc\u02c8\u02ca\u02cb\u02d9\u0300\u0301\u2018\u2019\u201a\u2032\u2035\u275b\u275c\uff07"
)

type dumper struct {
	logger         *log.Entry
	chunkSize      int64
	table          *config.Table
	columns        string
	entriesCount   int
	resultsChannel chan *dumpEntry
	entries        []*dumpEntry
	shutdown       bool
	shutdownCh     chan struct{}
	shutdownLock   sync.Mutex

	// DB is safe for using in goroutines
	// http://golang.org/src/database/sql/sql.go?s=5574:6362#L201
	db *sql.Tx
}

func NewDumper(db *sql.Tx, table *config.Table, chunkSize int64,
	logger *log.Entry) *dumper {
	dumper := &dumper{
		logger:         logger,
		db:             db,
		table:          table,
		resultsChannel: make(chan *dumpEntry, 50),
		entries:        make([]*dumpEntry, 0),
		chunkSize:      chunkSize,
		shutdownCh:     make(chan struct{}),
	}
	return dumper
}

type dumpStatResult struct {
	TotalCount int64
}

type dumpEntry struct {
	TableName   string
	TableSchema string
	Values      [][]string
	TotalCount  int64
	RowsCount   int64
	Offset      uint64 // only for 'no PK' table
	Columns     *umconf.ColumnList
	err         error
}

func (e *dumpEntry) incrementCounter() {
	e.RowsCount++
}

func (d *dumper) getDumpEntries() (entries []*dumpEntry, err error) {
	if d.table.Counter == 0 {
		return []*dumpEntry{}, nil
	} else {
		sliceCount := int(math.Ceil(float64(d.table.Counter) / float64(d.chunkSize)))
		if sliceCount == 0 {
			sliceCount = 1
		}
		entries = make([]*dumpEntry, sliceCount)
		for i := 0; i < sliceCount; i++ {
			offset := uint64(i) * uint64(d.chunkSize)
			entries[i] = &dumpEntry{
				TableName:   d.table.TableName,
				TableSchema: d.table.TableSchema,
				Offset:      offset,
			}
		}
	}

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

	return entries, nil
}

func (d *dumper) buildQueryOldWay(e *dumpEntry) string {
	return fmt.Sprintf(`SELECT %s FROM %s.%s where (%s) LIMIT %d OFFSET %d`,
		d.columns,
		usql.EscapeName(d.table.TableSchema),
		usql.EscapeName(d.table.TableName),
		d.table.Where,
		d.chunkSize,
		e.Offset,
	)
}

func (d *dumper) buildQueryOnUniqueKey(e *dumpEntry) string {
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
		usql.EscapeName(d.table.TableSchema),
		usql.EscapeName(d.table.TableName),
		// where
		rangeStr, d.table.Where,
		// order by
		strings.Join(uniqueKeyColumnAscending, ", "),
		// limit
		d.chunkSize,
	)
}

// dumps a specific chunk, reading chunk info from the channel
func (d *dumper) getChunkData(e *dumpEntry) {
	// TODO use PS
	// TODO escape schema/table/column name once and save
	var err error
	defer func() {
		d.resultsChannel <- e
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
		e.err = err
		return
	}

	columns, err := rows.Columns()
	if err != nil {
		e.err = err
		return
	}
	e.Columns = umconf.NewColumnList(columns)

	values := make([]*sql.RawBytes, len(columns))

	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	var lastVals *[]string

	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			e.err = err
			return
		}

		vals := make([]string, 0)
		for _, col := range values {
			// Here we can check if the value is nil (NULL value)
			if col != nil {
				vals = append(vals, string(*col))
			} else {
				vals = append(vals, "NULL")
			}
		}
		lastVals = &vals
		e.Values = append(e.Values, vals)
		e.incrementCounter()
	}

	d.logger.Debugf("getChunkData. n_row: %d", len(e.Values))

	// TODO getChunkData could get 0 rows. Esp after removing 'start transaction'.
	if len(e.Values) == 0 {
		e.err = fmt.Errorf("getChunkData. GetLastMaxVal: no rows found")
		return
	}

	if d.table.UseUniqueKey != nil {
		// lastVals must not be nil if len(data) > 0
		for i, col := range d.table.UseUniqueKey.Columns.Columns {
			// TODO save the idx
			idx := d.table.OriginalTableColumns.Ordinals[col.Name]
			if idx > len(*lastVals) {
				e.err = fmt.Errorf("getChunkData. GetLastMaxVal: column index %v > n_column %v", idx, len(*lastVals))
				return
			} else {
				d.table.UseUniqueKey.LastMaxVals[i] = (*lastVals)[idx]
			}
		}
		d.logger.Debugf("GetLastMaxVal: got %v", d.table.UseUniqueKey.LastMaxVals)
	}

	return
}

/*func (e *dumpEntry) escape(colValue string) string {
	e.colBuffer = *new(bytes.Buffer)
	if !strings.ContainsAny(colValue, stringOfBackslashAndQuoteChars) {
		return colValue
	} else {
		for _, char_c := range colValue {
			c := fmt.Sprintf("%c", char_c)
			if strings.ContainsAny(c, stringOfBackslashAndQuoteChars) {
				e.colBuffer.WriteString("\\")
			}
			e.colBuffer.WriteString(c)
		}
		return e.colBuffer.String()
	}
}*/

func (d *dumper) worker() {
	for _, e := range d.entries {
		if e != nil && !d.shutdown{
			d.getChunkData(e)
		}
	}
}

func (d *dumper) Dump() (err error) {
	d.entries, err = d.getDumpEntries()
	if err != nil {
		return err
	}

	if len(d.entries) == 0 {
		return nil
	}
	d.entriesCount = len(d.entries)
	go d.worker()

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
	close(d.resultsChannel)
	return nil
}
