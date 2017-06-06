package mysql

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

type table struct {
	DbName string
	TbName string
	SQL    string
	Values string
}

type dump struct {
	Tables       []*table
	CompleteTime string
}

//LOCK TABLES {{ .Name }} WRITE;
//INSERT INTO {{ .Name }} VALUES {{ .Values }};
//UNLOCK TABLES;

func getDatabases(db *sql.DB) ([]string, error) {
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
		dbs = append(dbs, database.String)
	}
	return dbs, rows.Err()
}

func getTables(db *sql.DB, dbName string) ([]string, error) {
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

func createTable(db *sql.DB, dbName, tbName string) (*table, error) {
	var err error
	t := &table{DbName: dbName, TbName: tbName}

	if t.SQL, err = createTableSQL(db, tbName); err != nil {
		return nil, err
	}

	if t.Values, err = createTableValues(db, tbName); err != nil {
		return nil, err
	}

	return t, nil
}

func createTableSQL(db *sql.DB, name string) (string, error) {
	// Get table creation SQL
	var table_return sql.NullString
	var table_sql sql.NullString
	err := db.QueryRow("SHOW CREATE TABLE "+name).Scan(&table_return, &table_sql)

	if err != nil {
		return "", err
	}
	if table_return.String != name {
		return "", errors.New("Returned table is not the same as requested table")
	}

	return table_sql.String, nil
}

func createTableValues(db *sql.DB, name string) (string, error) {
	// Get Data
	rows, err := db.Query("SELECT * FROM " + name)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	// Get columns
	columns, err := rows.Columns()
	if err != nil {
		return "", err
	}
	if len(columns) == 0 {
		return "", errors.New("No columns in table " + name + ".")
	}

	// Read data
	data_text := make([]string, 0)
	for rows.Next() {
		data := make([]*sql.NullString, len(columns))
		ptrs := make([]interface{}, len(columns))
		for i := range data {
			ptrs[i] = &data[i]
		}

		// Read data
		if err := rows.Scan(ptrs...); err != nil {
			return "", err
		}

		dataStrings := make([]string, len(columns))

		for key, value := range data {
			if value != nil && value.Valid {
				dataStrings[key] = value.String
			}
		}

		data_text = append(data_text, "('"+strings.Join(dataStrings, "','")+"')")
	}

	return strings.Join(data_text, ","), rows.Err()
}
