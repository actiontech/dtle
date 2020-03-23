package dbutil

import (
	"database/sql"

	"github.com/pingcap/errors"
)

// ScanRowsToInterfaces scans rows to interface arrary.
func ScanRowsToInterfaces(rows *sql.Rows) ([][]interface{}, error) {
	var rowsData [][]interface{}
	cols, err := rows.Columns()
	if err != nil {
		return nil, errors.Trace(err)
	}

	for rows.Next() {
		colVals := make([]interface{}, len(cols))

		err = rows.Scan(colVals...)
		if err != nil {
			return nil, errors.Trace(err)
		}
		rowsData = append(rowsData, colVals)
	}

	return rowsData, nil
}

// ColumnData saves column's data.
type ColumnData struct {
	Data   []byte
	IsNull bool
}

// ScanRow scans rows into a map.
func ScanRow(rows *sql.Rows) (map[string]*ColumnData, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, errors.Trace(err)
	}

	colVals := make([][]byte, len(cols))
	colValsI := make([]interface{}, len(colVals))
	for i := range colValsI {
		colValsI[i] = &colVals[i]
	}

	err = rows.Scan(colValsI...)
	if err != nil {
		return nil, errors.Trace(err)
	}

	result := make(map[string]*ColumnData)
	for i := range colVals {
		data := &ColumnData{
			Data:   colVals[i],
			IsNull: colVals[i] == nil,
		}
		result[cols[i]] = data
	}

	return result, nil
}
