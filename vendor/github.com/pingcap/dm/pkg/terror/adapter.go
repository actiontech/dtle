// Copyright 2019 PingCAP, Inc.
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

package terror

import (
	"database/sql/driver"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
)

// DBErrorAdaptArgs is an adapter to change raw database error to *Error object.
// If err is already an *Error object, return it directly.
func DBErrorAdaptArgs(err error, defaultErr *Error, args ...interface{}) error {
	if err == nil {
		return nil
	}
	if e, ok := err.(*Error); ok {
		return e
	}
	causeErr := errors.Cause(err)
	switch causeErr {
	case driver.ErrBadConn:
		return ErrDBBadConn.Delegate(err, args...)
	case mysql.ErrInvalidConn:
		return ErrDBInvalidConn.Delegate(err, args...)
	default:
		return defaultErr.Delegate(err, args...)
	}
}

// DBErrorAdapt is an adapter to change raw database error to *Error object.
// If err is already an *Error object, return it directly.
func DBErrorAdapt(err error, defaultErr *Error, args ...interface{}) error {
	return DBErrorAdaptArgs(err, defaultErr, args...)
}
