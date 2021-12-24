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
	"fmt"
)

const (
	defaultStringLenLimit = 1024
)

// TruncateString returns a string with only the leading (at most) n runes of the input string.
// If the string is truncated, a `...` tail will be appended.
func TruncateString(s string, n int) string {
	if n < 0 {
		n = defaultStringLenLimit
	}
	if len(s) <= n {
		return s
	}
	return s[:n] + "..." // mark as some content is truncated
}

// TruncateInterface converts the interface to a string
// and returns a string with only the leading (at most) n runes of the input string.
// If the converted string is truncated, a `...` tail will be appended.
// It is not effective for large structure now.
func TruncateInterface(v interface{}, n int) string {
	return TruncateString(fmt.Sprintf("%+v", v), n)
}
