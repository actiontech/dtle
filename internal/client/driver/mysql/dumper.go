package mysql

import (
	"database/sql"
	"fmt"
	"log"
	"math"
	"strconv"
	"strings"

	"bytes"
	usql "udup/internal/client/driver/mysql/sql"
	"udup/internal/config"
)

var (
	stringOfBackslashChars = []string{"\u005c", "\u00a5", "\u0160", "\u20a9", "\u2216", "\ufe68", "uff3c"}
	stringOfQuoteChars     = []string{"\u0022", "\u0027", "\u0060", "\u00b4", "\u02b9", "\u02ba", "\u02bb", "\u02bc",
		"\u02c8", "\u02ca", "\u02cb", "\u02d9", "\u0300", "\u0301", "\u2018", "\u2019", "\u201a", "\u2032", "\u2035",
		"\u275b", "\u275c", "\uff07"}
)

type dumper struct {
	logger      *log.Logger
	concurrency int
	chunkSize   int
	TableSchema string
	TableName   string

	// DB is safe for using in goroutines
	// http://golang.org/src/database/sql/sql.go?s=5574:6362#L201
	db *sql.DB
}

func NewDumper(db *sql.DB, dbName, tableName string, concurrency, chunkSize int, logger *log.Logger) *dumper {
	dumper := &dumper{
		logger:      logger,
		db:          db,
		TableSchema: dbName,
		TableName:   tableName,
		concurrency: concurrency,
		chunkSize:   chunkSize,
	}

	return dumper
}

func (d *dumper) getRowsCount() (uint64, error) {
	var res sql.NullString
	query := fmt.Sprintf(`SELECT COUNT(*) FROM %s.%s`,
		usql.EscapeName(d.TableSchema),
		usql.EscapeName(d.TableName),
	)
	err := d.db.QueryRow(query).Scan(&res)
	if err != nil {
		return 0, err
	}

	val, err := res.Value()
	if err != nil {
		return 0, err
	}

	return strconv.ParseUint(val.(string), 0, 64)
}

type dumpEntry struct {
	SystemVariablesStatement string
	DbSQL                    string
	TbSQL                    string
	Values                   string
	RowsCount                uint64
	Offset                   uint64
	Counter                  uint64
	Rows_cnt                 uint32
	Rows_crc32               uint32
	data_text                []string
	err                      error
}

func (j *dumpEntry) incrementCounter() {
	j.Counter++
}

func (d *dumper) getDumpEntries(systemVariablesStatement, dbSQL, tbSQL string) ([]*dumpEntry, error) {
	total, err := d.getRowsCount()
	if err != nil {
		return nil, err
	}

	sliceCount := int(math.Ceil(float64(total) / float64(d.chunkSize)))
	if sliceCount == 0 {
		sliceCount = 1
	}
	entries := make([]*dumpEntry, sliceCount)
	for i := 0; i < sliceCount; i++ {
		offset := uint64(i) * uint64(d.chunkSize)
		entries[i] = &dumpEntry{
			SystemVariablesStatement: systemVariablesStatement,
			DbSQL:     dbSQL,
			TbSQL:     tbSQL,
			RowsCount: total,
			Offset:    offset,
			Counter:   0}
	}
	return entries, nil
}

// dumps a specific chunk, reading chunk info from the channel
func (d *dumper) getChunkData(entry *dumpEntry) error {
	if entry.RowsCount == 0 {
		return nil
	}
	query := fmt.Sprintf(`SELECT * FROM %s.%s LIMIT %d OFFSET %d`,
		usql.EscapeName(d.TableSchema),
		usql.EscapeName(d.TableName),
		d.chunkSize,
		entry.Offset,
	)
	rows, err := d.db.Query(query)
	if err != nil {
		return err
	}

	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	/*queries := []string{}
	for _,column :=range columns {
		query = fmt.Sprintf(`SUM(CRC32(%s))`,
			column,
		)
		queries = append(queries,query)
	}

	query = fmt.Sprintf(`SELECT %v FROM (SELECT * FROM %s.%s LIMIT %d OFFSET %d) q`,
		strings.Join(queries, ","),
		usql.EscapeName(d.DbName),
		usql.EscapeName(d.TableName),
		d.chunkSize,
		entry.offset,
	)

	if err := d.db.QueryRow(query).Scan(&entry.rows_crc32); err != nil {
		return err
	}*/

	columnsCount := len(columns)
	values := make([]sql.RawBytes, columnsCount)

	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	entry.data_text = make([]string, 0)
	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			return err
		}

		var value string
		dataStrings := make([]string, columnsCount)
		for i, col := range values {
			// Here we can check if the value is nil (NULL value)
			if col == nil {
				value = "NULL"
			} else {
				colValue := string(col)
				if !needsQuoting(colValue) {
					value = colValue
				} else {
					colBuffer := new(bytes.Buffer)
					for _, char_c := range colValue {
						c := fmt.Sprintf("%c", char_c)
						if needsQuoting(c) {
							colBuffer.WriteString("\\")
						}
						colBuffer.WriteString(c)
					}
					value = colBuffer.String()
				}
				value = fmt.Sprintf("'%s'", value)
			}
			dataStrings[i] = value
		}
		entry.data_text = append(entry.data_text, "("+strings.Join(dataStrings, ",")+")")
		entry.incrementCounter()
	}
	//entry.Rows_crc32 = crc32.ChecksumIEEE([]byte(strings.Join(entry.data_text, ",")))
	entry.Values = fmt.Sprintf(`replace into %s.%s values %s`, d.TableSchema, d.TableName, strings.Join(entry.data_text, ","))
	return nil
}

func needsQuoting(s string) bool {
	for _, char_c := range s {
		for _, bc := range stringOfBackslashChars {
			if bc == fmt.Sprintf("%c", char_c) {
				return true
			}
		}
		for _, qc := range stringOfQuoteChars {
			if qc == fmt.Sprintf("%c", char_c) {
				return true
			}
		}
	}
	return false
}

// dumps a specific chunk, reading chunk info from the channel
/*func (d *dumper) getChunkData(entry *dumpEntry) error {
	query := fmt.Sprintf(`SELECT * FROM %s.%s LIMIT %d OFFSET %d LOCK IN SHARE MODE`,
		usql.EscapeName(d.DbName),
		usql.EscapeName(d.TableName),
		d.chunkSize,
		entry.Offset,
	)

	d.logger.Printf("query:%v",query)

	err := func() error {
		tx, err := d.db.Begin()
		if err != nil {
			return err
		}

		rollback := func(err error) error {
			tx.Rollback()
			return err
		}

		rows, err := tx.Query(query)
		if err != nil {
			return rollback(err)
		}

		columns, err := rows.Columns()
		if err != nil {
			return err
		}

		queries := []string{}
		for _,column :=range columns {
			query = fmt.Sprintf("'%s'",column)
			queries = append(queries,query)
		}

		query = fmt.Sprintf(`SELECT
			COUNT(*) AS cnt,
			COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(CONCAT_WS('#',
			%s)) AS UNSIGNED)), 10, 16)), 0)
			AS crc FROM %s.%s FORCE INDEX('PRIMARY')
			WHERE id >= %d AND id <= %d
			`,
			strings.Join(queries, ","),
			usql.EscapeName(d.DbName),
			usql.EscapeName(d.TableName),
			entry.Offset,
			d.chunkSize,
		)

		columnsCount := len(columns)
		values := make([]sql.RawBytes, columnsCount)

		scanArgs := make([]interface{}, len(values))
		for i := range values {
			scanArgs[i] = &values[i]
		}

		entry.data_text = make([]string, 0)
		for rows.Next() {
			err = rows.Scan(scanArgs...)
			if err != nil {
				return err
			}

			var value string
			dataStrings := make([]string, columnsCount)
			for i, col := range values {
				// Here we can check if the value is nil (NULL value)
				if col == nil {
					value = "NULL"
				} else {
					value = string(col)
				}
				dataStrings[i] = value
			}
			entry.data_text = append(entry.data_text, "('"+strings.Join(dataStrings, "','")+"')")
			entry.incrementCounter()
		}

		if err = tx.QueryRow(query).Scan(&entry.Rows_cnt,&entry.Rows_crc);err != nil {
			return rollback(err)
		}

		if err := tx.Commit(); err != nil {
			return err
		}
		return nil
	}()

	if err != nil {
		return err
	}

	entry.Values = fmt.Sprintf(`replace into %s.%s values %s`, d.DbName, d.TableName, strings.Join(entry.data_text, ","))
	return nil
}*/

func (d *dumper) worker(entriesChannel <-chan *dumpEntry, resultsChannel chan<- *dumpEntry) {
	for e := range entriesChannel {
		err := d.getChunkData(e)
		if err != nil {
			e.err = err
		}
		resultsChannel <- e
	}
}

func (d *dumper) Dump(systemVariablesStatement string) ([]*dumpEntry, error) {
	dbSQL := fmt.Sprintf("CREATE DATABASE %s", d.TableSchema)
	tbSQL, err := d.createTableSQL()
	if err != nil {
		return nil, err
	}
	entries, err := d.getDumpEntries(systemVariablesStatement, dbSQL, tbSQL)
	if err != nil {
		return nil, err
	}

	workersCount := int(math.Min(float64(d.concurrency), float64(len(entries))))
	if workersCount < 1 {
		return nil, nil
	}

	entriesCount := len(entries)
	entriesChannel := make(chan *dumpEntry)
	resultsChannel := make(chan *dumpEntry)
	for i := 0; i < workersCount; i++ {
		go d.worker(entriesChannel, resultsChannel)
	}

	go func() {
		for _, e := range entries {
			entriesChannel <- e
		}
		close(entriesChannel)
	}()

	result := make([]*dumpEntry, entriesCount)
	for i := 0; i < entriesCount; i++ {
		e := <-resultsChannel
		result[i] = e
	}
	close(resultsChannel)

	return result, nil
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
		tb := &config.Table{TableSchema: dbName, TableName: table.String}
		tables = append(tables, tb)
	}
	return tables, rows.Err()
}

func (d *dumper) createTableSQL() (string, error) {
	query := fmt.Sprintf(`SHOW CREATE TABLE %s.%s`,
		usql.EscapeName(d.TableSchema),
		usql.EscapeName(d.TableName),
	)
	// Get table creation SQL
	var table_return sql.NullString
	var table_sql sql.NullString
	err := d.db.QueryRow(query).Scan(&table_return, &table_sql)
	if err != nil {
		return "", err
	}
	if table_return.String != d.TableName {
		return "", fmt.Errorf("Returned table is not the same as requested table")
	}

	return fmt.Sprintf("USE %s;%s", d.TableSchema, table_sql.String), nil
}
