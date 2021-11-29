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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/kv/kv.go
//

package kv

import (
	"math"
	"sync"
	"time"

	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/util"
)

// ReturnedValue pairs the Value and AlreadyLocked flag for PessimisticLock return values result.
type ReturnedValue struct {
	Value         []byte
	AlreadyLocked bool
}

// Used for pessimistic lock wait time
// these two constants are special for lock protocol with tikv
// math.MaxInt64 means always wait, 0 means nowait, others meaning lock wait in milliseconds
const (
	LockAlwaysWait = int64(math.MaxInt64)
	LockNoWait     = int64(0)
)

type lockWaitTimeInMs struct {
	value int64
}

func defaultLockWaitTime() *lockWaitTimeInMs {
	return &lockWaitTimeInMs{value: LockAlwaysWait}
}

// LockCtx contains information for LockKeys method.
type LockCtx struct {
	Killed                *uint32
	ForUpdateTS           uint64
	lockWaitTime          *lockWaitTimeInMs
	WaitStartTime         time.Time
	PessimisticLockWaited *int32
	LockKeysDuration      *int64
	LockKeysCount         *int32
	ReturnValues          bool
	Values                map[string]ReturnedValue
	ValuesLock            sync.Mutex
	LockExpired           *uint32
	Stats                 *util.LockKeysDetails
	ResourceGroupTag      []byte
	OnDeadlock            func(*tikverr.ErrDeadlock)
}

// LockWaitTime returns lockWaitTimeInMs
func (ctx *LockCtx) LockWaitTime() int64 {
	if ctx.lockWaitTime == nil {
		ctx.lockWaitTime = defaultLockWaitTime()
	}
	return ctx.lockWaitTime.value
}

// NewLockCtx creates a LockCtx.
func NewLockCtx(forUpdateTS uint64, lockWaitTime int64, waitStartTime time.Time) *LockCtx {
	return &LockCtx{
		ForUpdateTS:   forUpdateTS,
		lockWaitTime:  &lockWaitTimeInMs{value: lockWaitTime},
		WaitStartTime: waitStartTime,
	}
}

// InitReturnValues creates the map to store returned value.
func (ctx *LockCtx) InitReturnValues(valueLen int) {
	ctx.ReturnValues = true
	ctx.Values = make(map[string]ReturnedValue, valueLen)
}

// GetValueNotLocked returns a value if the key is not already locked.
// (nil, false) means already locked.
func (ctx *LockCtx) GetValueNotLocked(key []byte) ([]byte, bool) {
	rv := ctx.Values[string(key)]
	if !rv.AlreadyLocked {
		return rv.Value, true
	}
	return nil, false
}

// IterateValuesNotLocked applies f to all key-values that are not already
// locked.
func (ctx *LockCtx) IterateValuesNotLocked(f func([]byte, []byte)) {
	ctx.ValuesLock.Lock()
	defer ctx.ValuesLock.Unlock()
	for key, val := range ctx.Values {
		if !val.AlreadyLocked {
			f([]byte(key), val.Value)
		}
	}
}
