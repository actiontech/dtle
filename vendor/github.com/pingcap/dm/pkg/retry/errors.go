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

package retry

import (
	"database/sql/driver"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/errors"
	tmysql "github.com/pingcap/tidb/parser/mysql"

	"github.com/pingcap/dm/pkg/terror"
)

// some error reference: https://docs.pingcap.com/tidb/stable/tidb-limitations#limitations-on-a-single-table
var (
	// UnsupportedDDLMsgs list the error messages of some unsupported DDL in TiDB.
	UnsupportedDDLMsgs = []string{
		"can't drop column with index",
		"unsupported add column",
		"unsupported modify column",
		"unsupported modify charset",
		"unsupported modify collate",
		"unsupported drop integer primary key",
		"Unsupported collation",
		"Invalid default value for",
		"Unsupported drop primary key",
		"Error 1059: Identifier name", // Limitations on identifier length
		"Error 1117: Too many columns",
		"Error 1069: Too many keys specified",
	}

	// UnsupportedDMLMsgs list the error messages of some un-recoverable DML, which is used in task auto recovery.
	UnsupportedDMLMsgs = []string{
		"Error 1062: Duplicate",
		"Error 1406: Data too long for column",
		"Error 1366",
		"Error 8025: entry too large",
	}

	// ReplicationErrMsgs list the error message of un-recoverable replication error.
	ReplicationErrMsgs = []string{
		"Could not find first log file name in binary log index file",
	}

	// ParseRelayLogErrMsgs list the error messages of some un-recoverable relay log parsing error, which is used in task auto recovery.
	ParseRelayLogErrMsgs = []string{
		"binlog checksum mismatch, data may be corrupted",
		"get event err EOF",
	}

	// UnresumableErrCodes is a set of unresumeable err codes.
	UnresumableErrCodes = map[int32]struct{}{
		int32(terror.ErrSyncUnitDDLWrongSequence.Code()):    {},
		int32(terror.ErrDumpUnitGlobalLock.Code()):          {},
		int32(terror.ErrDumpUnitRuntime.Code()):             {},
		int32(terror.ErrSyncerUnitDMLColumnNotMatch.Code()): {},
	}

	// UnresumableRelayErrCodes is a set of unresumeable relay unit err codes.
	UnresumableRelayErrCodes = map[int32]struct{}{
		int32(terror.ErrRelayUUIDSuffixNotValid.Code()):     {},
		int32(terror.ErrRelayUUIDSuffixLessThanPrev.Code()): {},
		int32(terror.ErrRelayBinlogNameNotValid.Code()):     {},
		int32(terror.ErrRelayNoCurrentUUID.Code()):          {},
		int32(terror.ErrRelayLogDirpathEmpty.Code()):        {},
	}
)

// IsConnectionError tells whether this error should reconnect to Database.
func IsConnectionError(err error) bool {
	err = errors.Cause(err)
	switch err {
	case driver.ErrBadConn, tmysql.ErrBadConn, gmysql.ErrBadConn:
		return true
	}
	return false
}
