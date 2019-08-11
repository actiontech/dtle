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

package rollback

import (
	"sync"

	"github.com/pingcap/dm/pkg/log"
)

// FuncRollback records function used to rolling back some operations.
// It is currently used by units' Init to release resources when failing in the half-way.
type FuncRollback struct {
	Name string
	Fn   func()
}

// FuncRollbackHolder holds some RollbackFuncs.
type FuncRollbackHolder struct {
	mu    sync.Mutex
	owner string // used to make log clearer
	fns   []FuncRollback
}

// NewRollbackHolder creates a new FuncRollbackHolder instance.
func NewRollbackHolder(owner string) *FuncRollbackHolder {
	return &FuncRollbackHolder{
		owner: owner,
		fns:   make([]FuncRollback, 0),
	}
}

// Add adds a func to the holder.
func (h *FuncRollbackHolder) Add(fn FuncRollback) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.fns = append(h.fns, fn)
}

// RollbackReverseOrder executes rollback functions in reverse order
func (h *FuncRollbackHolder) RollbackReverseOrder() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for i := len(h.fns) - 1; i >= 0; i-- {
		fn := h.fns[i]
		log.Infof("[%s] rolling back %s", h.owner, fn.Name)
		fn.Fn()
	}
}
