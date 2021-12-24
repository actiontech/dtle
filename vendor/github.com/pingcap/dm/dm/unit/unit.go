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

package unit

import (
	"context"
	"strings"
	"time"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/binlog"
	"github.com/pingcap/dm/pkg/terror"
)

const (
	// DefaultInitTimeout represents the default timeout value when initializing a process unit.
	DefaultInitTimeout = time.Minute
)

// Unit defines interface for subtask process units, like syncer, loader, relay, etc.
// The Unit is not responsible to maintain its status like "pausing"/"paused". The caller should maintain the status,
// for example, know the Unit is "paused" and avoid call Pause again.
// All method is Unit interface can expect no concurrent invocation, the caller should guarantee this.
type Unit interface {
	// Init initializes the dm process unit
	// every unit does base initialization in `Init`, and this must pass before start running the subtask
	// other setups can be done in the beginning of `Process`, but this should be treated carefully to make it
	// compatible with Pause / Resume.
	// if initialing successfully, the outer caller should call `Close` when the unit (or the task) finished, stopped or canceled (because other units Init fail).
	// if initialing fail, Init itself should release resources it acquired before (rolling itself back).
	Init(ctx context.Context) error
	// Process does the main logic and its returning must send a result to pr channel.
	// When ctx.Done, stops the process and returns, otherwise the DM-worker will be blocked forever
	// When not in processing, call Process to continue or resume the process
	Process(ctx context.Context, pr chan pb.ProcessResult)
	// Close shuts down the process and closes the unit, after that can not call Process to resume
	// The implementation should not block for a long time.
	Close()
	// Pause does some cleanups and the unit can be resumed later. The caller will make sure Process has returned.
	// The implementation should not block for a long time.
	Pause()
	// Resume resumes the paused process and its returning must send a result to pr channel.
	Resume(ctx context.Context, pr chan pb.ProcessResult)
	// Update updates the configuration
	Update(cfg *config.SubTaskConfig) error

	// Status returns the unit's current status. The result may need calculation with source status, like estimated time
	// to catch up. If sourceStatus is nil, the calculation should be skipped.
	Status(sourceStatus *binlog.SourceStatus) interface{}
	// Type returns the unit's type
	Type() pb.UnitType
	// IsFreshTask return whether is a fresh task (not processed before)
	// it will be used to decide where the task should become restoring
	IsFreshTask(ctx context.Context) (bool, error)
}

// NewProcessError creates a new ProcessError
// we can refine to add error scope field if needed.
func NewProcessError(err error) *pb.ProcessError {
	if e, ok := err.(*terror.Error); ok {
		return &pb.ProcessError{
			ErrCode:    int32(e.Code()),
			ErrClass:   e.Class().String(),
			ErrScope:   e.Scope().String(),
			ErrLevel:   e.Level().String(),
			Message:    terror.Message(e),
			RawCause:   terror.Message(e.Cause()),
			Workaround: e.Workaround(),
		}
	}

	return &pb.ProcessError{
		ErrCode:    int32(terror.ErrNotSet.Code()),
		ErrClass:   terror.ErrNotSet.Class().String(),
		ErrScope:   terror.ErrNotSet.Scope().String(),
		ErrLevel:   terror.ErrNotSet.Level().String(),
		Message:    terror.Message(err),
		RawCause:   terror.Message(terror.ErrNotSet.Cause()),
		Workaround: terror.ErrNotSet.Workaround(),
	}
}

// IsCtxCanceledProcessErr returns true if the err's context canceled.
func IsCtxCanceledProcessErr(err *pb.ProcessError) bool {
	return strings.Contains(err.Message, "context canceled")
}

// JoinProcessErrors return the string of pb.ProcessErrors joined by ", ".
func JoinProcessErrors(errors []*pb.ProcessError) string {
	serrs := make([]string, 0, len(errors))
	for _, serr := range errors {
		serrs = append(serrs, serr.String())
	}
	return strings.Join(serrs, ", ")
}
