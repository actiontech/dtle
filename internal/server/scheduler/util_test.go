/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package scheduler

import (
	"reflect"
	"testing"
	log "github.com/actiontech/udup/internal/logger"
	"github.com/actiontech/udup/internal/models"
)

func Test_materializeTasks(t *testing.T) {
	type args struct {
		job *models.Job
	}
	tests := []struct {
		name string
		args args
		want map[string]*models.Task
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := materializeTasks(tt.args.job); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("materializeTasks() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_diffResult_GoString(t *testing.T) {
	type fields struct {
		place   []allocTuple
		update  []allocTuple
		migrate []allocTuple
		resume  []allocTuple
		pause   []allocTuple
		stop    []allocTuple
		ignore  []allocTuple
		lost    []allocTuple
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &diffResult{
				place:   tt.fields.place,
				update:  tt.fields.update,
				migrate: tt.fields.migrate,
				resume:  tt.fields.resume,
				pause:   tt.fields.pause,
				stop:    tt.fields.stop,
				ignore:  tt.fields.ignore,
				lost:    tt.fields.lost,
			}
			if got := d.GoString(); got != tt.want {
				t.Errorf("diffResult.GoString() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_diffResult_Append(t *testing.T) {
	type fields struct {
		place   []allocTuple
		update  []allocTuple
		migrate []allocTuple
		resume  []allocTuple
		pause   []allocTuple
		stop    []allocTuple
		ignore  []allocTuple
		lost    []allocTuple
	}
	type args struct {
		other *diffResult
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &diffResult{
				place:   tt.fields.place,
				update:  tt.fields.update,
				migrate: tt.fields.migrate,
				resume:  tt.fields.resume,
				pause:   tt.fields.pause,
				stop:    tt.fields.stop,
				ignore:  tt.fields.ignore,
				lost:    tt.fields.lost,
			}
			d.Append(tt.args.other)
		})
	}
}

func Test_diffAllocs(t *testing.T) {
	type args struct {
		job            *models.Job
		taintedNodes   map[string]*models.Node
		required       map[string]*models.Task
		allocs         []*models.Allocation
		terminalAllocs map[string]*models.Allocation
	}
	tests := []struct {
		name string
		args args
		want *diffResult
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := diffAllocs(tt.args.job, tt.args.taintedNodes, tt.args.required, tt.args.allocs, tt.args.terminalAllocs); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("diffAllocs() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_readyNodesInDCs(t *testing.T) {
	type args struct {
		state State
		dcs   []string
	}
	tests := []struct {
		name    string
		args    args
		want    []*models.Node
		want1   map[string]int
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1, err := readyNodesInDCs(tt.args.state, tt.args.dcs)
			if (err != nil) != tt.wantErr {
				t.Errorf("readyNodesInDCs() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("readyNodesInDCs() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("readyNodesInDCs() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func Test_retryMax(t *testing.T) {
	type args struct {
		max   int
		cb    func() (bool, error)
		reset func() bool
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := retryMax(tt.args.max, tt.args.cb, tt.args.reset); (err != nil) != tt.wantErr {
				t.Errorf("retryMax() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_progressMade(t *testing.T) {
	type args struct {
		result *models.PlanResult
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := progressMade(tt.args.result); got != tt.want {
				t.Errorf("progressMade() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_taintedNodes(t *testing.T) {
	type args struct {
		state  State
		allocs []*models.Allocation
	}
	tests := []struct {
		name    string
		args    args
		want    map[string]*models.Node
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := taintedNodes(tt.args.state, tt.args.allocs)
			if (err != nil) != tt.wantErr {
				t.Errorf("taintedNodes() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("taintedNodes() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_shuffleNodes(t *testing.T) {
	type args struct {
		nodes []*models.Node
	}
	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			shuffleNodes(tt.args.nodes)
		})
	}
}

func Test_tasksUpdated(t *testing.T) {
	type args struct {
		jobA *models.Job
		jobB *models.Job
		task string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tasksUpdated(tt.args.jobA, tt.args.jobB, tt.args.task); got != tt.want {
				t.Errorf("tasksUpdated() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_setStatus(t *testing.T) {
	type args struct {
		logger         *log.Logger
		planner        Planner
		eval           *models.Evaluation
		nextEval       *models.Evaluation
		spawnedBlocked *models.Evaluation
		tgMetrics      map[string]*models.AllocMetric
		status         string
		desc           string
		queuedAllocs   map[string]int
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := setStatus(tt.args.logger, tt.args.planner, tt.args.eval, tt.args.nextEval, tt.args.spawnedBlocked, tt.args.tgMetrics, tt.args.status, tt.args.desc, tt.args.queuedAllocs); (err != nil) != tt.wantErr {
				t.Errorf("setStatus() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_inplaceUpdate(t *testing.T) {
	type args struct {
		ctx     Context
		eval    *models.Evaluation
		job     *models.Job
		updates []allocTuple
	}
	tests := []struct {
		name            string
		args            args
		wantDestructive []allocTuple
		wantInplace     []allocTuple
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotDestructive, gotInplace := inplaceUpdate(tt.args.ctx, tt.args.eval, tt.args.job, tt.args.updates)
			if !reflect.DeepEqual(gotDestructive, tt.wantDestructive) {
				t.Errorf("inplaceUpdate() gotDestructive = %v, want %v", gotDestructive, tt.wantDestructive)
			}
			if !reflect.DeepEqual(gotInplace, tt.wantInplace) {
				t.Errorf("inplaceUpdate() gotInplace = %v, want %v", gotInplace, tt.wantInplace)
			}
		})
	}
}

func Test_taskConstraints(t *testing.T) {
	type args struct {
		t *models.Task
	}
	tests := []struct {
		name string
		args args
		want taskConstrainTuple
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := taskConstraints(tt.args.t); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("taskConstraints() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_desiredUpdates(t *testing.T) {
	type args struct {
		diff               *diffResult
		inplaceUpdates     []allocTuple
		destructiveUpdates []allocTuple
	}
	tests := []struct {
		name string
		args args
		want map[string]*models.DesiredUpdates
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := desiredUpdates(tt.args.diff, tt.args.inplaceUpdates, tt.args.destructiveUpdates); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("desiredUpdates() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_adjustQueuedAllocations(t *testing.T) {
	type args struct {
		logger       *log.Logger
		result       *models.PlanResult
		queuedAllocs map[string]int
	}
	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			adjustQueuedAllocations(tt.args.logger, tt.args.result, tt.args.queuedAllocs)
		})
	}
}

func Test_updateNonTerminalAllocsToLost(t *testing.T) {
	type args struct {
		plan    *models.Plan
		tainted map[string]*models.Node
		allocs  []*models.Allocation
	}
	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			updateNonTerminalAllocsToLost(tt.args.plan, tt.args.tainted, tt.args.allocs)
		})
	}
}
