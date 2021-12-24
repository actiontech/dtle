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

// +build windows

package utils

import (
	"syscall"
	"unsafe"

	"github.com/pingcap/dm/pkg/terror"
)

var (
	kernel32            = syscall.MustLoadDLL("kernel32.dll")
	getDiskFreeSpaceExW = kernel32.MustFindProc("GetDiskFreeSpaceExW")
)

// GetStorageSize gets storage's capacity and available size
func GetStorageSize(dir string) (size StorageSize, err error) {
	r, _, e := getDiskFreeSpaceExW.Call(
		uintptr(unsafe.Pointer(syscall.StringToUTF16Ptr(dir))),
		uintptr(unsafe.Pointer(&size.Available)),
		uintptr(unsafe.Pointer(&size.Capacity)),
		0,
	)
	if r == 0 {
		err = terror.ErrStatFileSize.Delegate(e)
	}
	return
}
