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
	"database/sql"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb-tools/pkg/filter"
	router "github.com/pingcap/tidb-tools/pkg/table-router"
	"github.com/pingcap/tidb/parser/model"
	tmysql "github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"

	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"
)

// TrimCtrlChars returns a slice of the string s with all leading
// and trailing control characters removed.
func TrimCtrlChars(s string) string {
	f := func(r rune) bool {
		// All entries in the ASCII table below code 32 (technically the C0 control code set) are of this kind,
		// including CR and LF used to separate lines of text. The code 127 (DEL) is also a control character.
		// Reference: https://en.wikipedia.org/wiki/Control_character
		return r < 32 || r == 127
	}

	return strings.TrimFunc(s, f)
}

// TrimQuoteMark tries to trim leading and tailing quote(") mark if exists
// only trim if leading and tailing quote matched as a pair.
func TrimQuoteMark(s string) string {
	if len(s) > 2 && s[0] == '"' && s[len(s)-1] == '"' {
		return s[1 : len(s)-1]
	}
	return s
}

// FetchAllDoTables returns all need to do tables after filtered (fetches from upstream MySQL).
func FetchAllDoTables(ctx context.Context, db *sql.DB, bw *filter.Filter) (map[string][]string, error) {
	schemas, err := dbutil.GetSchemas(ctx, db)

	failpoint.Inject("FetchAllDoTablesFailed", func(val failpoint.Value) {
		err = tmysql.NewErr(uint16(val.(int)))
		log.L().Warn("FetchAllDoTables failed", zap.String("failpoint", "FetchAllDoTablesFailed"), zap.Error(err))
	})

	if err != nil {
		return nil, terror.WithScope(err, terror.ScopeUpstream)
	}

	ftSchemas := make([]*filter.Table, 0, len(schemas))
	for _, schema := range schemas {
		if filter.IsSystemSchema(schema) {
			continue
		}
		ftSchemas = append(ftSchemas, &filter.Table{
			Schema: schema,
			Name:   "", // schema level
		})
	}
	ftSchemas = bw.Apply(ftSchemas)
	if len(ftSchemas) == 0 {
		log.L().Warn("no schema need to sync")
		return nil, nil
	}

	schemaToTables := make(map[string][]string)
	for _, ftSchema := range ftSchemas {
		schema := ftSchema.Schema
		// use `GetTables` from tidb-tools, no view included
		tables, err := dbutil.GetTables(ctx, db, schema)
		if err != nil {
			return nil, terror.WithScope(terror.DBErrorAdapt(err, terror.ErrDBDriverError), terror.ScopeUpstream)
		}
		ftTables := make([]*filter.Table, 0, len(tables))
		for _, table := range tables {
			ftTables = append(ftTables, &filter.Table{
				Schema: schema,
				Name:   table,
			})
		}
		ftTables = bw.Apply(ftTables)
		if len(ftTables) == 0 {
			log.L().Info("no tables need to sync", zap.String("schema", schema))
			continue // NOTE: should we still keep it as an empty elem?
		}
		tables = tables[:0]
		for _, ftTable := range ftTables {
			tables = append(tables, ftTable.Name)
		}
		schemaToTables[schema] = tables
	}

	return schemaToTables, nil
}

// FetchTargetDoTables returns all need to do tables after filtered and routed (fetches from upstream MySQL).
func FetchTargetDoTables(ctx context.Context, db *sql.DB, bw *filter.Filter, router *router.Table) (map[string][]*filter.Table, error) {
	// fetch tables from source and filter them
	sourceTables, err := FetchAllDoTables(ctx, db, bw)

	failpoint.Inject("FetchTargetDoTablesFailed", func(val failpoint.Value) {
		err = tmysql.NewErr(uint16(val.(int)))
		log.L().Warn("FetchTargetDoTables failed", zap.String("failpoint", "FetchTargetDoTablesFailed"), zap.Error(err))
	})

	if err != nil {
		return nil, err
	}

	mapper := make(map[string][]*filter.Table)
	for schema, tables := range sourceTables {
		for _, table := range tables {
			targetSchema, targetTable, err := router.Route(schema, table)
			if err != nil {
				return nil, terror.ErrGenTableRouter.Delegate(err)
			}

			targetTableName := dbutil.TableName(targetSchema, targetTable)
			mapper[targetTableName] = append(mapper[targetTableName], &filter.Table{
				Schema: schema,
				Name:   table,
			})
		}
	}

	return mapper, nil
}

// LowerCaseTableNamesFlavor represents the type of db `lower_case_table_names` settings.
type LowerCaseTableNamesFlavor uint8

const (
	// LCTableNamesSensitive represent lower_case_table_names = 0, case sensitive.
	LCTableNamesSensitive LowerCaseTableNamesFlavor = 0
	// LCTableNamesInsensitive represent lower_case_table_names = 1, case insensitive.
	LCTableNamesInsensitive = 1
	// LCTableNamesMixed represent lower_case_table_names = 2, table names are case-sensitive, but case-insensitive in usage.
	LCTableNamesMixed = 2
)

// FetchLowerCaseTableNamesSetting return the `lower_case_table_names` setting of target db.
func FetchLowerCaseTableNamesSetting(ctx context.Context, conn *sql.Conn) (LowerCaseTableNamesFlavor, error) {
	query := "SELECT @@lower_case_table_names;"
	row := conn.QueryRowContext(ctx, query)
	if row.Err() != nil {
		return LCTableNamesSensitive, terror.ErrDBExecuteFailed.Delegate(row.Err(), query)
	}
	var res uint8
	if err := row.Scan(&res); err != nil {
		return LCTableNamesSensitive, terror.ErrDBExecuteFailed.Delegate(err, query)
	}
	if res > LCTableNamesMixed {
		return LCTableNamesSensitive, terror.ErrDBUnExpect.Generate(fmt.Sprintf("invalid `lower_case_table_names` value '%d'", res))
	}
	return LowerCaseTableNamesFlavor(res), nil
}

// GetDBCaseSensitive returns the case sensitive setting of target db.
func GetDBCaseSensitive(ctx context.Context, db *sql.DB) (bool, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return true, terror.DBErrorAdapt(err, terror.ErrDBDriverError)
	}
	lcFlavor, err := FetchLowerCaseTableNamesSetting(ctx, conn)
	if err != nil {
		return true, err
	}
	return lcFlavor == LCTableNamesSensitive, nil
}

// CompareShardingDDLs compares s and t ddls
// only concern in content, ignore order of ddl.
func CompareShardingDDLs(s, t []string) bool {
	if len(s) != len(t) {
		return false
	}

	ddls := make(map[string]struct{})
	for _, ddl := range s {
		ddls[ddl] = struct{}{}
	}

	for _, ddl := range t {
		if _, ok := ddls[ddl]; !ok {
			return false
		}
	}

	return true
}

// GenDDLLockID returns lock ID used in shard-DDL.
func GenDDLLockID(task, schema, table string) string {
	return fmt.Sprintf("%s-%s", task, dbutil.TableName(schema, table))
}

var lockIDPattern = regexp.MustCompile("(.*)\\-\\`(.*)\\`.\\`(.*)\\`")

// ExtractTaskFromLockID extract task from lockID.
func ExtractTaskFromLockID(lockID string) string {
	strs := lockIDPattern.FindStringSubmatch(lockID)
	// strs should be [full-lock-ID, task, db, table] if successful matched
	if len(strs) < 4 {
		return ""
	}
	return strs[1]
}

// ExtractDBAndTableFromLockID extract schema and table from lockID.
func ExtractDBAndTableFromLockID(lockID string) (string, string) {
	strs := lockIDPattern.FindStringSubmatch(lockID)
	// strs should be [full-lock-ID, task, db, table] if successful matched
	if len(strs) < 4 {
		return "", ""
	}
	return strs[2], strs[3]
}

// NonRepeatStringsEqual is used to compare two un-ordered, non-repeat-element string slice is equal.
func NonRepeatStringsEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	m := make(map[string]struct{}, len(a))
	for _, s := range a {
		m[s] = struct{}{}
	}
	for _, s := range b {
		if _, ok := m[s]; !ok {
			return false
		}
	}
	return true
}

// GenTableID generates table ID.
func GenTableID(table *filter.Table) string {
	return table.String()
}

// GenSchemaID generates schema ID.
func GenSchemaID(table *filter.Table) string {
	return "`" + table.Schema + "`"
}

// GenTableIDAndCheckSchemaOnly generates table ID and check if schema only.
func GenTableIDAndCheckSchemaOnly(table *filter.Table) (id string, isSchemaOnly bool) {
	return GenTableID(table), len(table.Name) == 0
}

// UnpackTableID unpacks table ID to <schema, table> pair.
func UnpackTableID(id string) *filter.Table {
	parts := strings.Split(id, "`.`")
	schema := strings.TrimLeft(parts[0], "`")
	table := strings.TrimRight(parts[1], "`")
	return &filter.Table{
		Schema: schema,
		Name:   table,
	}
}

type session struct {
	sessionctx.Context
	vars                 *variable.SessionVars
	values               map[fmt.Stringer]interface{}
	builtinFunctionUsage map[string]uint32
	mu                   sync.RWMutex
}

// GetSessionVars implements the sessionctx.Context interface.
func (se *session) GetSessionVars() *variable.SessionVars {
	return se.vars
}

// SetValue implements the sessionctx.Context interface.
func (se *session) SetValue(key fmt.Stringer, value interface{}) {
	se.mu.Lock()
	se.values[key] = value
	se.mu.Unlock()
}

// Value implements the sessionctx.Context interface.
func (se *session) Value(key fmt.Stringer) interface{} {
	se.mu.RLock()
	value := se.values[key]
	se.mu.RUnlock()
	return value
}

// GetInfoSchema implements the sessionctx.Context interface.
func (se *session) GetInfoSchema() sessionctx.InfoschemaMetaVersion {
	return nil
}

// GetBuiltinFunctionUsage implements the sessionctx.Context interface.
func (se *session) GetBuiltinFunctionUsage() map[string]uint32 {
	return se.builtinFunctionUsage
}

// UTCSession can be used as a sessionctx.Context, with UTC timezone.
var UTCSession *session

func init() {
	UTCSession = &session{}
	vars := variable.NewSessionVars()
	vars.StmtCtx.TimeZone = time.UTC
	UTCSession.vars = vars
	UTCSession.values = make(map[fmt.Stringer]interface{}, 1)
	UTCSession.builtinFunctionUsage = make(map[string]uint32)
}

// AdjustBinaryProtocolForDatum converts the data in binlog to TiDB datum.
func AdjustBinaryProtocolForDatum(data []interface{}, cols []*model.ColumnInfo) ([]types.Datum, error) {
	log.L().Debug("AdjustBinaryProtocolForChunk",
		zap.Any("data", data),
		zap.Any("columns", cols))
	ret := make([]types.Datum, 0, len(data))
	for i, d := range data {
		switch v := d.(type) {
		case int8:
			d = int64(v)
		case int16:
			d = int64(v)
		case int32:
			d = int64(v)
		case uint8:
			d = uint64(v)
		case uint16:
			d = uint64(v)
		case uint32:
			d = uint64(v)
		case uint:
			d = uint64(v)
		case decimal.Decimal:
			d = v.String()
		}
		datum := types.NewDatum(d)

		// TODO: should we use timezone of upstream?
		castDatum, err := table.CastValue(UTCSession, datum, cols[i], false, false)
		if err != nil {
			return nil, err
		}
		ret = append(ret, castDatum)
	}
	return ret, nil
}
