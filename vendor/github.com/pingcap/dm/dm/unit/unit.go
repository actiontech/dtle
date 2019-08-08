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

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/pb"
)

// Unit defines interface for sub task process units, like syncer, loader, relay, etc.
type Unit interface {
	// Init initializes the dm process unit
	// every unit does base initialization in `Init`, and this must pass before start running the sub task
	// other setups can be done in `Process`, but this should be treated carefully, let it's compatible with Pause / Resume
	// if initialing successfully, the outer caller should call `Close` when the unit (or the task) finished, stopped or canceled (because other units Init fail).
	// if initialing fail, Init itself should release resources it acquired before (rolling itself back).
	Init() error
	// Process processes sub task
	// When ctx.Done, stops the process and returns
	// When not in processing, call Process to continue or resume the process
	Process(ctx context.Context, pr chan pb.ProcessResult)
	// Close shuts down the process and closes the unit, after that can not call Process to resume
	Close()
	// Pause pauses the process, it can be resumed later
	Pause()
	// Resume resumes the paused process
	Resume(ctx context.Context, pr chan pb.ProcessResult)
	// Update updates the configuration
	Update(cfg *config.SubTaskConfig) error

	// Status returns the unit's current status
	Status() interface{}
	// Error returns the unit's error information
	Error() interface{}
	// Type returns the unit's type
	Type() pb.UnitType
	// IsFreshTask return whether is a fresh task (not processed before)
	// it will be used to decide where the task should become restoring
	IsFreshTask() (bool, error)
}

// NewProcessError creates a new ProcessError
// we can refine to add error scope field if needed
func NewProcessError(errorType pb.ErrorType, msg string) *pb.ProcessError {
	return &pb.ProcessError{
		Type: errorType,
		Msg:  msg,
	}
}
