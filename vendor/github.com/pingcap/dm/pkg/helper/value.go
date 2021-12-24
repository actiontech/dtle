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

package helper

import "reflect"

// IsNil tests whether the passed in value is nil.
// ref https://github.com/golang/go/blob/87113f7eadf6d8b12279709f05c0359b54b194ea/src/reflect/value.go#L1049.
func IsNil(vi interface{}) (result bool) {
	if vi == nil {
		result = true
	} else {
		switch v := reflect.ValueOf(vi); v.Kind() {
		case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.UnsafePointer, reflect.Slice:
			return v.IsNil()
		}
	}
	return
}
