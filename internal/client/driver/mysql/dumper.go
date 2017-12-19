package mysql

import (
	"bytes"
	"database/sql"
	"fmt"
	"math"
	"strings"
	"sync"

	ubase "udup/internal/client/driver/mysql/base"
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
	total          int64
	TableSchema    string
	TableName      string
	table          *config.Table
	columns        string
	entriesCount   int
	resultsChannel chan *dumpEntry
	entriesChannel chan *dumpEntry
	shutdown       bool
	shutdownCh     chan struct{}
	shutdownLock   sync.Mutex

	// DB is safe for using in goroutines
	// http://golang.org/src/database/sql/sql.go?s=5574:6362#L201
	db *sql.Tx
}

func NewDumper(db *sql.Tx, table *config.Table, total, chunkSize int64,
	logger *log.Entry) *dumper {
	dumper := &dumper{
		logger:         logger,
		db:             db,
		TableSchema:    table.TableSchema,
		TableName:      table.TableName,
		table:          table,
		total:          total,
		resultsChannel: make(chan *dumpEntry, 50),
		entriesChannel: make(chan *dumpEntry),
		chunkSize:      chunkSize,
		shutdownCh:     make(chan struct{}),
	}
	return dumper
}

type dumpStatResult struct {
	TotalCount int64
}

type dumpEntry struct {
	SystemVariablesStatement string
	SqlMode                  string
	DbSQL                    string
	TableName                string
	TableSchema              string
	TbSQL                    string
	Values                   [][]string
	TotalCount               int64
	RowsCount                int64
	Offset                   uint64 // only for 'no PK' table
	colBuffer                bytes.Buffer
	err                      error
}

func (e *dumpEntry) incrementCounter() {
	e.RowsCount++
}

func (d *dumper) getDumpEntries() ([]*dumpEntry, error) {
	if d.total == 0 {
		return []*dumpEntry{}, nil
	}

	columnList, err := ubase.GetTableColumns(d.db, d.TableSchema, d.TableName)
	if err != nil {
		return []*dumpEntry{}, err
	}

	if err := ubase.ApplyColumnTypes(d.db, d.TableSchema, d.TableName, columnList); err != nil {
		return []*dumpEntry{}, err
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
	entries := make([]*dumpEntry, sliceCount)
	for i := 0; i < sliceCount; i++ {
		offset := uint64(i) * uint64(d.chunkSize)
		entries[i] = &dumpEntry{
			Offset: offset,
		}
	}
	return entries, nil
}

func (d *dumper) buildQueryOldWay(e *dumpEntry) string {
	return fmt.Sprintf(`SELECT %s FROM %s.%s where (%s) LIMIT %d OFFSET %d`,
		d.columns,
		usql.EscapeName(d.TableSchema),
		usql.EscapeName(d.TableName),
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
func (d *dumper) getChunkData(e *dumpEntry) (err error) {
	entry := &dumpEntry{
		TableSchema: d.TableSchema,
		TableName:   d.TableName,
		RowsCount:   e.RowsCount,
		Offset:      e.Offset,
	}
	// TODO use PS
	// TODO escape schema/table/column name once and save
	defer func() {
		entry.err = err
		d.resultsChannel <- entry
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

	values := make([]*sql.RawBytes, len(columns))

	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	data := make([]string, 0)
	//packetLen := 0
	var lastVals *[]string

	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			return err
		}

		vals := make([]string, 0)
		for _, col := range values {
			// Here we can check if the value is nil (NULL value)
			if col != nil {
				vals = append(vals, fmt.Sprintf("'%s'", usql.EscapeValue(string(*col))))
				/*packetLen += len(usql.EscapeValue(string(*col)))
				if packetLen > 2000000 {
					entry.Values = append(entry.Values, data)
					packetLen = 0
					data = []string{}
				}*/
			} else {
				vals = append(vals, "NULL")
			}
		}
		lastVals = &vals
		data = append(data, fmt.Sprintf("( %s )", strings.Join(vals, ", ")))
		entry.incrementCounter()
	}

	d.logger.Debugf("getChunkData. n_row: %d", len(data))

	// TODO getChunkData could get 0 rows. Esp after removing 'start transaction'.
	if len(data) == 0 {
		return fmt.Errorf("getChunkData. GetLastMaxVal: no rows found")
	}

	if d.table.UseUniqueKey != nil {
		// lastVals must not be nil if len(data) > 0
		for i, col := range d.table.UseUniqueKey.Columns.Columns {
			// TODO save the idx
			idx := d.table.OriginalTableColumns.Ordinals[col.Name]
			if idx > len(*lastVals) {
				return fmt.Errorf("getChunkData. GetLastMaxVal: column index %d > n_column", idx, len(*lastVals))
			} else {
				d.table.UseUniqueKey.LastMaxVals[i] = (*lastVals)[idx]
			}
		}
		d.logger.Debugf("GetLastMaxVal: got %v", d.table.UseUniqueKey.LastMaxVals)
	}

	entry.Values = append(entry.Values, data)
	//d.resultsChannel <- entry
	/*query = fmt.Sprintf(`
			insert into %s.%s
				(%s)
			values
				%s
			on duplicate key update
				%s=VALUES(%s)
		`,
		usql.EscapeName(d.TableSchema),
		usql.EscapeName(d.TableName),
		strings.Join(columns, ","),
		strings.Join(data, ","),
		columns[0],
		columns[0],
	)
	entry.Values = query*/
	return nil
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

//LOCK TABLES {{ .Name }} WRITE;
//INSERT INTO {{ .Name }} VALUES {{ .Values }};
//UNLOCK TABLES;

func showDatabases(db *sql.DB) ([]string, error) {
	dbs := make([]string, 0)

	// Get table list
	rows, err := db.Query("SHOW DATABASES")
	if err != nil {
		return dbs, err
	}
	defer rows.Close()

	// Read result
	for rows.Next() {
		var database sql.NullString
		if err := rows.Scan(&database); err != nil {
			return dbs, err
		}
		switch strings.ToLower(database.String) {
		case "sys", "mysql", "information_schema", "performance_schema":
			continue
		default:
			dbs = append(dbs, database.String)
		}
	}
	return dbs, rows.Err()
}

func showTables(db *sql.DB, dbName string) (tables []*config.Table, err error) {
	// Get table list
	rows, err := db.Query(fmt.Sprintf("SHOW TABLES IN %s", dbName))
	if err != nil {
		return tables, err
	}
	defer rows.Close()

	// Read result
	for rows.Next() {
		var table sql.NullString
		if err := rows.Scan(&table); err != nil {
			return tables, err
		}
		tb := config.NewTable(dbName, table.String)
		tables = append(tables, tb)
	}
	return tables, rows.Err()
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
