package mysql

import (
	"database/sql"
	"fmt"
	"math"
	"strconv"
	"strings"
)

type dumper struct {
	concurrency              int
	chunkSize                int
	SystemVariablesStatement string
	DbName                   string
	TableName                string
	DbSQL                    string
	TbSQL                    string
	Values                   string

	// DB is safe for using in goroutines
	// http://golang.org/src/database/sql/sql.go?s=5574:6362#L201
	db *sql.DB
}

type table struct {
	DbName string
	TbName string
}

func NewDumper(db *sql.DB, dbName, tableName, systemVariablesStatement string, concurrency, chunkSize int) *dumper {
	dumper := new(dumper)
	dumper.db = db
	dumper.SystemVariablesStatement = systemVariablesStatement
	dumper.DbName = dbName
	dumper.TableName = tableName
	dumper.DbSQL = fmt.Sprintf("CREATE DATABASE %s", dbName)
	dumper.concurrency = concurrency
	dumper.chunkSize = chunkSize

	return dumper
}

func (d *dumper) getRowsCount() (uint64, error) {
	var res sql.NullString
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s.%s", d.DbName, d.TableName)
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

type job struct {
	offset    uint64
	counter   uint64
	data_text []string
	err       error
}

func (j *job) incrementCounter() {
	j.counter++
}

func (d *dumper) getJobs() ([]*job, error) {
	total, err := d.getRowsCount()
	if err != nil {
		return nil, err
	}

	sliceCount := int(math.Ceil(float64(total) / float64(d.chunkSize)))
	jobs := make([]*job, sliceCount)
	for i := 0; i < sliceCount; i++ {
		offset := uint64(i) * uint64(d.chunkSize)
		jobs[i] = &job{offset: offset, counter: 0}
	}
	return jobs, nil
}

// dumps a specific chunk, reading chunk info from the channel
func (d *dumper) getChunkData(job *job) error {
	query := fmt.Sprintf(
		"SELECT * FROM %s.%s LIMIT %d OFFSET %d",
		d.DbName, d.TableName, d.chunkSize, job.offset)
	rows, err := d.db.Query(query)
	if err != nil {
		return err
	}

	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	columnsCount := len(columns)
	values := make([]sql.RawBytes, columnsCount)

	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	job.data_text = make([]string, 0)
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
		job.data_text = append(job.data_text, "('"+strings.Join(dataStrings, "','")+"')")
		job.incrementCounter()
	}

	return nil
}

func (d *dumper) worker(jobsChannel <-chan *job, resultsChannel chan<- *job) {
	for job := range jobsChannel {
		err := d.getChunkData(job)
		if err != nil {
			job.err = err
		}
		resultsChannel <- job
	}
}

func (d *dumper) Dump() ([]*job, error) {
	if err := d.createTableSQL(); err != nil {
		return nil, err
	}
	jobs, err := d.getJobs()
	if err != nil {
		return nil, err
	}

	workersCount := int(math.Min(float64(d.concurrency), float64(len(jobs))))
	if workersCount < 1 {
		return nil, nil
	}

	jobsCount := len(jobs)
	jobsChannel := make(chan *job)
	resultsChannel := make(chan *job)
	for i := 0; i < workersCount; i++ {
		go d.worker(jobsChannel, resultsChannel)
	}

	go func() {
		for _, job := range jobs {
			jobsChannel <- job
		}
		close(jobsChannel)
	}()

	result := make([]*job, jobsCount)
	for i := 0; i < jobsCount; i++ {
		job := <-resultsChannel
		result[i] = job
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

func showTables(db *sql.DB, dbName string) ([]string, error) {
	tables := make([]string, 0)

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
		tables = append(tables, table.String)
	}
	return tables, rows.Err()
}

func (d *dumper) createTableSQL() error {
	query := fmt.Sprintf("SHOW CREATE TABLE %s.%s", d.DbName, d.TableName)
	// Get table creation SQL
	var table_return sql.NullString
	var table_sql sql.NullString
	err := d.db.QueryRow(query).Scan(&table_return, &table_sql)
	if err != nil {
		return err
	}
	if table_return.String != d.TableName {
		return fmt.Errorf("Returned table is not the same as requested table")
	}
	d.TbSQL = fmt.Sprintf("USE %s;%s", d.DbName, table_sql.String)

	return nil
}
