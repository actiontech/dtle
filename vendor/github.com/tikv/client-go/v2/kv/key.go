// Copyright 2021 TiKV Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// NOTE: The code in this file is based on code from the
// TiDB project, licensed under the Apache License v 2.0
//
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/kv/key.go
//

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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kv

import (
	"bytes"
	"encoding/hex"
)

// NextKey returns the next key in byte-order.
func NextKey(k []byte) []byte {
	// add 0x0 to the end of key
	buf := make([]byte, len(k)+1)
	copy(buf, k)
	return buf
}

// PrefixNextKey returns the next prefix key.
//
// Assume there are keys like:
//
//   rowkey1
//   rowkey1_column1
//   rowkey1_column2
//   rowKey2
//
// If we seek 'rowkey1' NextKey, we will get 'rowkey1_column1'.
// If we seek 'rowkey1' PrefixNextKey, we will get 'rowkey2'.
func PrefixNextKey(k []byte) []byte {
	buf := make([]byte, len(k))
	copy(buf, k)
	var i int
	for i = len(k) - 1; i >= 0; i-- {
		buf[i]++
		if buf[i] != 0 {
			break
		}
	}
	if i == -1 {
		// Unlike TiDB, for the specific key 0xFF
		// we return empty slice instead of {0xFF, 0x0}
		buf = make([]byte, 0)
	}
	return buf
}

// CmpKey returns the comparison result of two key.
// The result will be 0 if a==b, -1 if a < b, and +1 if a > b.
func CmpKey(k, another []byte) int {
	return bytes.Compare(k, another)
}

// StrKey returns string for key.
func StrKey(k []byte) string {
	return hex.EncodeToString(k)
}

// KeyRange represents a range where StartKey <= key < EndKey.
type KeyRange struct {
	StartKey []byte
	EndKey   []byte
}
