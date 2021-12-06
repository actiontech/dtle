// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package binlog

import (
	"context"
	"database/sql"
	"time"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"

	"github.com/pingcap/dm/pkg/terror"
)

// in MySQL, we can set `max_binlog_size` to control the max size of a binlog file.
// but this is not absolute:
// > A transaction is written in one chunk to the binary log, so it is never split between several binary logs.
// > Therefore, if you have big transactions, you might see binary log files larger than max_binlog_size.
// ref: https://dev.mysql.com/doc/refman/5.7/en/replication-options-binary-log.html#sysvar_max_binlog_size
// The max value of `max_binlog_size` is 1073741824 (1GB)
// but the actual file size still can be larger, and it may exceed the range of an uint32
// so, if we use go-mysql.Position(with uint32 Pos) to store the binlog size, it may become out of range.
// ps, use go-mysql.Position to store a position of binlog event (position of the next event) is enough.
type binlogSize struct {
	name string
	size int64
}

// FileSizes is a list of binlog filename and size.
type FileSizes []binlogSize

// GetBinaryLogs returns binlog filename and size of upstream.
func GetBinaryLogs(ctx context.Context, db *sql.DB) (FileSizes, error) {
	query := "SHOW BINARY LOGS"
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, terror.DBErrorAdapt(err, terror.ErrDBDriverError)
	}
	defer rows.Close()

	rowColumns, err := rows.Columns()
	if err != nil {
		return nil, terror.DBErrorAdapt(err, terror.ErrDBDriverError)
	}
	files := make([]binlogSize, 0, 10)
	for rows.Next() {
		var file string
		var pos int64
		var nullPtr interface{}
		if len(rowColumns) == 2 {
			err = rows.Scan(&file, &pos)
		} else {
			err = rows.Scan(&file, &pos, &nullPtr)
		}
		if err != nil {
			return nil, terror.DBErrorAdapt(err, terror.ErrDBDriverError)
		}
		files = append(files, binlogSize{name: file, size: pos})
	}
	if rows.Err() != nil {
		return nil, terror.DBErrorAdapt(rows.Err(), terror.ErrDBDriverError)
	}
	return files, nil
}

// After returns the total size of binlog after `fromFile` in FileSizes.
func (b FileSizes) After(fromFile gmysql.Position) int64 {
	var total int64
	for _, file := range b {
		switch gmysql.CompareBinlogFileName(file.name, fromFile.Name) {
		case -1:
			continue
		case 1:
			total += file.size
		case 0:
			if file.size > int64(fromFile.Pos) {
				total += file.size - int64(fromFile.Pos)
			}
		}
	}

	return total
}

// SourceStatus collects all information of upstream.
type SourceStatus struct {
	Location   Location
	Binlogs    FileSizes
	UpdateTime time.Time
}
