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

package config

import (
	"bytes"
	"fmt"

	"github.com/pingcap/dm/pkg/terror"
)

// DM definition checking items
// refer github.com/pingcap/tidb-tools/pkg/check.
const (
	AllChecking                  = "all"
	DumpPrivilegeChecking        = "dump_privilege"
	ReplicationPrivilegeChecking = "replication_privilege"
	VersionChecking              = "version"
	ServerIDChecking             = "server_id"
	BinlogEnableChecking         = "binlog_enable"
	BinlogFormatChecking         = "binlog_format"
	BinlogRowImageChecking       = "binlog_row_image"
	TableSchemaChecking          = "table_schema"
	ShardTableSchemaChecking     = "schema_of_shard_tables"
	ShardAutoIncrementIDChecking = "auto_increment_ID"
)

// AllCheckingItems contains all checking items.
var AllCheckingItems = map[string]string{
	AllChecking:                  "all checking items",
	DumpPrivilegeChecking:        "dump privileges of source DB checking item",
	ReplicationPrivilegeChecking: "replication privileges of source DB checking item",
	VersionChecking:              "MySQL/MariaDB version checking item",
	ServerIDChecking:             "server_id checking item",
	BinlogEnableChecking:         "binlog enable checking item",
	BinlogFormatChecking:         "binlog format checking item",
	BinlogRowImageChecking:       "binlog row image checking item",
	TableSchemaChecking:          "table schema compatibility checking item",
	ShardTableSchemaChecking:     "consistent schema of shard tables checking item",
	ShardAutoIncrementIDChecking: "conflict auto increment ID of shard tables checking item",
}

// MaxSourceIDLength is the max length for dm-worker source id.
const MaxSourceIDLength = 32

// ValidateCheckingItem validates checking item.
func ValidateCheckingItem(item string) error {
	if _, ok := AllCheckingItems[item]; ok {
		return nil
	}

	return terror.ErrConfigCheckItemNotSupport.Generate(item, SupportCheckingItems())
}

// SupportCheckingItems returns all supporting checking item.
func SupportCheckingItems() string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "************ supporting checking items ************\n name:\t\tdescription\n")
	for name, desc := range AllCheckingItems {
		fmt.Fprintf(&buf, "%s:\t%s\n", name, desc)
	}
	fmt.Fprintf(&buf, "************ supporting checking items ************\n")
	return buf.String()
}

// FilterCheckingItems filters ignored items from all checking items.
func FilterCheckingItems(ignoredItems []string) map[string]string {
	checkingItems := make(map[string]string)
	for item, desc := range AllCheckingItems {
		checkingItems[item] = desc
	}
	delete(checkingItems, AllChecking)

	for _, item := range ignoredItems {
		if item == AllChecking {
			return nil
		}

		delete(checkingItems, item)
	}

	return checkingItems
}
