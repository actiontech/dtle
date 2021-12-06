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

package conn

import (
	"database/sql"

	"github.com/DATA-DOG/go-sqlmock"
	check "github.com/pingcap/check"

	"github.com/pingcap/dm/dm/config"
)

type mockDBProvider struct {
	verDB *sql.DB // verDB user for show version.
	db    *sql.DB
}

// Apply will build BaseDB with DBConfig.
func (d *mockDBProvider) Apply(config config.DBConfig) (*BaseDB, error) {
	if d.verDB != nil {
		if err := d.verDB.Ping(); err == nil {
			// nolint:nilerr
			return NewBaseDB(d.verDB, func() {}), nil
		}
	}
	return NewBaseDB(d.db, func() {}), nil
}

// InitMockDB return a mocked db for unit test.
func InitMockDB(c *check.C) sqlmock.Sqlmock {
	db, mock, err := sqlmock.New()
	c.Assert(err, check.IsNil)
	if mdbp, ok := DefaultDBProvider.(*mockDBProvider); ok {
		mdbp.db = db
	} else {
		DefaultDBProvider = &mockDBProvider{db: db}
	}
	return mock
}

// InitVersionDB return a mocked db for unit test's show version.
func InitVersionDB(c *check.C) sqlmock.Sqlmock {
	db, mock, err := sqlmock.New()
	c.Assert(err, check.IsNil)
	if mdbp, ok := DefaultDBProvider.(*mockDBProvider); ok {
		mdbp.verDB = db
	} else {
		DefaultDBProvider = &mockDBProvider{verDB: db}
	}
	return mock
}
