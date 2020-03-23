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

package filter

import "strings"

// DM heartbeat schema / table name
var (
	DMHeartbeatSchema = "dm_heartbeat"
	DMHeartbeatTable  = "heartbeat"
)

// mysql system schema
var systemSchemas = map[string]struct{}{
	"information_schema": {},
	"mysql":              {},
	"performance_schema": {},
	"sys":                {},
	DMHeartbeatSchema:    {}, // do not create table in it manually
}

// IsSystemSchema judge schema is system shema or not
// case insensitive
func IsSystemSchema(schema string) bool {
	schema = strings.ToLower(schema)
	_, ok := systemSchemas[schema]
	return ok
}
