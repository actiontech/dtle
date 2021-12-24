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

package common

import (
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"

	parserpkg "github.com/pingcap/dm/pkg/parser"
	"github.com/pingcap/dm/pkg/utils"
)

// CheckIsDDL checks input SQL whether is a valid DDL statement.
func CheckIsDDL(sql string, p *parser.Parser) bool {
	sql = utils.TrimCtrlChars(sql)

	if utils.IsBuildInSkipDDL(sql) {
		return false
	}

	// if parse error, treat it as not a DDL
	stmts, err := parserpkg.Parse(p, sql, "", "")
	if err != nil || len(stmts) == 0 {
		return false
	}

	stmt := stmts[0]
	switch stmt.(type) {
	case ast.DDLNode:
		return true
	default:
		// other thing this like `BEGIN`
		return false
	}
}
