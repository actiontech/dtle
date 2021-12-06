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

package config

// TODO: move related struct to tidb-tools

// ExpressionFilter represents a filter that will be applied on row changes.
// one ExpressionFilter can only have one of (insert, update, delete) expressions.
// there are two update expressions, which form an AND logic. If user omits one expression, DM will use "TRUE" for it.
type ExpressionFilter struct {
	Schema             string `yaml:"schema" toml:"schema" json:"schema"`
	Table              string `yaml:"table" toml:"table" json:"table"`
	InsertValueExpr    string `yaml:"insert-value-expr" toml:"insert-value-expr" json:"insert-value-expr"`
	UpdateOldValueExpr string `yaml:"update-old-value-expr" toml:"update-old-value-expr" json:"update-old-value-expr"`
	UpdateNewValueExpr string `yaml:"update-new-value-expr" toml:"update-new-value-expr" json:"update-new-value-expr"`
	DeleteValueExpr    string `yaml:"delete-value-expr" toml:"delete-value-expr" json:"delete-value-expr"`
}
