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

package utils

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	gmysql "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/errno"
	"go.uber.org/zap"
	"golang.org/x/net/http/httpproxy"

	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"
)

var (
	// OsExit is function placeholder for os.Exit.
	OsExit func(int)
	/*
		CREATE [TEMPORARY] TABLE [IF NOT EXISTS] tbl_name
			{ LIKE old_tbl_name | (LIKE old_tbl_name) }
	*/
	builtInSkipDDLs = []string{
		// transaction
		"^SAVEPOINT",

		// skip all flush sqls
		"^FLUSH",

		// table maintenance
		"^OPTIMIZE\\s+TABLE",
		"^ANALYZE\\s+TABLE",
		"^REPAIR\\s+TABLE",

		// temporary table
		"^DROP\\s+(\\/\\*\\!40005\\s+)?TEMPORARY\\s+(\\*\\/\\s+)?TABLE",

		// trigger
		"^CREATE\\s+(DEFINER\\s?=.+?)?TRIGGER",
		"^DROP\\s+TRIGGER",

		// procedure
		"^DROP\\s+PROCEDURE",
		"^CREATE\\s+(DEFINER\\s?=.+?)?PROCEDURE",
		"^ALTER\\s+PROCEDURE",

		// view
		"^CREATE\\s*(OR REPLACE)?\\s+(ALGORITHM\\s?=.+?)?(DEFINER\\s?=.+?)?\\s+(SQL SECURITY DEFINER)?VIEW",
		"^DROP\\s+VIEW",
		"^ALTER\\s+(ALGORITHM\\s?=.+?)?(DEFINER\\s?=.+?)?(SQL SECURITY DEFINER)?VIEW",

		// function
		// user-defined function
		"^CREATE\\s+(AGGREGATE)?\\s*?FUNCTION",
		// stored function
		"^CREATE\\s+(DEFINER\\s?=.+?)?FUNCTION",
		"^ALTER\\s+FUNCTION",
		"^DROP\\s+FUNCTION",

		// tableSpace
		"^CREATE\\s+TABLESPACE",
		"^ALTER\\s+TABLESPACE",
		"^DROP\\s+TABLESPACE",

		// event
		"^CREATE\\s+(DEFINER\\s?=.+?)?EVENT",
		"^ALTER\\s+(DEFINER\\s?=.+?)?EVENT",
		"^DROP\\s+EVENT",

		// account management
		"^GRANT",
		"^REVOKE",
		"^CREATE\\s+USER",
		"^ALTER\\s+USER",
		"^RENAME\\s+USER",
		"^DROP\\s+USER",
		"^SET\\s+PASSWORD",
	}
	builtInSkipDDLPatterns *regexp.Regexp

	passwordPatterns = `(password: (\\")?)(.*?)((\\")?\\n)`
	passwordRegexp   *regexp.Regexp
)

func init() {
	OsExit = os.Exit
	builtInSkipDDLPatterns = regexp.MustCompile("(?i)" + strings.Join(builtInSkipDDLs, "|"))
	passwordRegexp = regexp.MustCompile(passwordPatterns)
	pb.HidePwdFunc = HidePassword
}

// DecodeBinlogPosition parses a mysql.Position from string format.
func DecodeBinlogPosition(pos string) (*mysql.Position, error) {
	if len(pos) < 3 {
		return nil, terror.ErrInvalidBinlogPosStr.Generate(pos)
	}
	if pos[0] != '(' || pos[len(pos)-1] != ')' {
		return nil, terror.ErrInvalidBinlogPosStr.Generate(pos)
	}
	sp := strings.Split(pos[1:len(pos)-1], ",")
	if len(sp) != 2 {
		return nil, terror.ErrInvalidBinlogPosStr.Generate(pos)
	}
	position, err := strconv.ParseUint(strings.TrimSpace(sp[1]), 10, 32)
	if err != nil {
		return nil, terror.ErrInvalidBinlogPosStr.Delegate(err, pos)
	}
	return &mysql.Position{
		Name: strings.TrimSpace(sp[0]),
		Pos:  uint32(position),
	}, nil
}

// WaitSomething waits for something done with `true`.
func WaitSomething(backoff int, waitTime time.Duration, fn func() bool) bool {
	for i := 0; i < backoff; i++ {
		if fn() {
			return true
		}

		time.Sleep(waitTime)
	}

	return false
}

// IsContextCanceledError checks whether err is context.Canceled.
func IsContextCanceledError(err error) bool {
	return errors.Cause(err) == context.Canceled
}

// IgnoreErrorCheckpoint is used in checkpoint update.
func IgnoreErrorCheckpoint(err error) bool {
	err = errors.Cause(err) // check the original error
	mysqlErr, ok := err.(*gmysql.MySQLError)
	if !ok {
		return false
	}

	switch mysqlErr.Number {
	case errno.ErrDupFieldName:
		return true
	default:
		return false
	}
}

// IsBuildInSkipDDL return true when checked sql that will be skipped for syncer.
func IsBuildInSkipDDL(sql string) bool {
	return builtInSkipDDLPatterns.FindStringIndex(sql) != nil
}

// HidePassword replace password with ******.
func HidePassword(input string) string {
	output := passwordRegexp.ReplaceAllString(input, "$1******$4")
	return output
}

// UnwrapScheme removes http or https scheme from input.
func UnwrapScheme(s string) string {
	if strings.HasPrefix(s, "http://") {
		return s[len("http://"):]
	} else if strings.HasPrefix(s, "https://") {
		return s[len("https://"):]
	}
	return s
}

func wrapScheme(s string, https bool) string {
	if s == "" {
		return s
	}
	s = UnwrapScheme(s)
	if https {
		return "https://" + s
	}
	return "http://" + s
}

// WrapSchemes adds http or https scheme to input if missing. input could be a comma-separated list.
func WrapSchemes(s string, https bool) string {
	items := strings.Split(s, ",")
	output := make([]string, 0, len(items))
	for _, s := range items {
		output = append(output, wrapScheme(s, https))
	}
	return strings.Join(output, ",")
}

// WrapSchemesForInitialCluster acts like WrapSchemes, except input is "name=URL,...".
func WrapSchemesForInitialCluster(s string, https bool) string {
	items := strings.Split(s, ",")
	output := make([]string, 0, len(items))
	for _, item := range items {
		kv := strings.Split(item, "=")
		if len(kv) != 2 {
			output = append(output, item)
			continue
		}

		output = append(output, kv[0]+"="+wrapScheme(kv[1], https))
	}
	return strings.Join(output, ",")
}

// IsFakeRotateEvent return true if is this event is a fake rotate event
// If log pos equals zero then the received event is a fake rotate event and
// contains only a name of the next binlog file
// See https://github.com/mysql/mysql-server/blob/8e797a5d6eb3a87f16498edcb7261a75897babae/sql/rpl_binlog_sender.h#L235
// and https://github.com/mysql/mysql-server/blob/8cc757da3d87bf4a1f07dcfb2d3c96fed3806870/sql/rpl_binlog_sender.cc#L899
func IsFakeRotateEvent(header *replication.EventHeader) bool {
	return header.Timestamp == 0 || header.LogPos == 0
}

// LogHTTPProxies logs HTTP proxy relative environment variables.
func LogHTTPProxies(useLogger bool) {
	if fields := proxyFields(); len(fields) > 0 {
		if useLogger {
			log.L().Warn("using proxy config", fields...)
		} else {
			filedsStr := make([]string, 0, len(fields))
			for _, field := range fields {
				filedsStr = append(filedsStr, field.Key+"="+field.String)
			}
			fmt.Printf("\n[Warning] [using proxy config] [%v]\n", strings.Join(filedsStr, ", "))
		}
	}
}

func proxyFields() []zap.Field {
	proxyCfg := httpproxy.FromEnvironment()
	fields := make([]zap.Field, 0, 3)
	if proxyCfg.HTTPProxy != "" {
		fields = append(fields, zap.String("http_proxy", proxyCfg.HTTPProxy))
	}
	if proxyCfg.HTTPSProxy != "" {
		fields = append(fields, zap.String("https_proxy", proxyCfg.HTTPSProxy))
	}
	if proxyCfg.NoProxy != "" {
		fields = append(fields, zap.String("no_proxy", proxyCfg.NoProxy))
	}
	return fields
}
