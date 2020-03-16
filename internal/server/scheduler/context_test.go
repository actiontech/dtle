/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package scheduler

import (
	"reflect"
	"regexp"
	"testing"
	log "github.com/actiontech/dts/internal/logger"
	"github.com/actiontech/dts/internal/models"

	version "github.com/hashicorp/go-version"
)

func TestEvalCache_RegexpCache(t *testing.T) {
	type fields struct {
		reCache         map[string]*regexp.Regexp
		constraintCache map[string]version.Constraints
	}
	tests := []struct {
		name   string
		fields fields
		want   map[string]*regexp.Regexp
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &EvalCache{
				reCache:         tt.fields.reCache,
				constraintCache: tt.fields.constraintCache,
			}
			if got := e.RegexpCache(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("EvalCache.RegexpCache() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEvalCache_ConstraintCache(t *testing.T) {
	type fields struct {
		reCache         map[string]*regexp.Regexp
		constraintCache map[string]version.Constraints
	}
	tests := []struct {
		name   string
		fields fields
		want   map[string]version.Constraints
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &EvalCache{
				reCache:         tt.fields.reCache,
				constraintCache: tt.fields.constraintCache,
			}
			if got := e.ConstraintCache(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("EvalCache.ConstraintCache() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewEvalContext(t *testing.T) {
	type args struct {
		s   State
		p   *models.Plan
		log *log.Logger
	}
	tests := []struct {
		name string
		args args
		want *EvalContext
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewEvalContext(tt.args.s, tt.args.p, tt.args.log); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewEvalContext() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEvalContext_State(t *testing.T) {
	type fields struct {
		EvalCache   EvalCache
		state       State
		plan        *models.Plan
		logger      *log.Logger
		metrics     *models.AllocMetric
		eligibility *EvalEligibility
	}
	tests := []struct {
		name   string
		fields fields
		want   State
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &EvalContext{
				EvalCache:   tt.fields.EvalCache,
				state:       tt.fields.state,
				plan:        tt.fields.plan,
				logger:      tt.fields.logger,
				metrics:     tt.fields.metrics,
				eligibility: tt.fields.eligibility,
			}
			if got := e.State(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("EvalContext.State() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEvalContext_Plan(t *testing.T) {
	type fields struct {
		EvalCache   EvalCache
		state       State
		plan        *models.Plan
		logger      *log.Logger
		metrics     *models.AllocMetric
		eligibility *EvalEligibility
	}
	tests := []struct {
		name   string
		fields fields
		want   *models.Plan
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &EvalContext{
				EvalCache:   tt.fields.EvalCache,
				state:       tt.fields.state,
				plan:        tt.fields.plan,
				logger:      tt.fields.logger,
				metrics:     tt.fields.metrics,
				eligibility: tt.fields.eligibility,
			}
			if got := e.Plan(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("EvalContext.Plan() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEvalContext_Logger(t *testing.T) {
	type fields struct {
		EvalCache   EvalCache
		state       State
		plan        *models.Plan
		logger      *log.Logger
		metrics     *models.AllocMetric
		eligibility *EvalEligibility
	}
	tests := []struct {
		name   string
		fields fields
		want   *log.Logger
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &EvalContext{
				EvalCache:   tt.fields.EvalCache,
				state:       tt.fields.state,
				plan:        tt.fields.plan,
				logger:      tt.fields.logger,
				metrics:     tt.fields.metrics,
				eligibility: tt.fields.eligibility,
			}
			if got := e.Logger(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("EvalContext.Logger() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEvalContext_Metrics(t *testing.T) {
	type fields struct {
		EvalCache   EvalCache
		state       State
		plan        *models.Plan
		logger      *log.Logger
		metrics     *models.AllocMetric
		eligibility *EvalEligibility
	}
	tests := []struct {
		name   string
		fields fields
		want   *models.AllocMetric
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &EvalContext{
				EvalCache:   tt.fields.EvalCache,
				state:       tt.fields.state,
				plan:        tt.fields.plan,
				logger:      tt.fields.logger,
				metrics:     tt.fields.metrics,
				eligibility: tt.fields.eligibility,
			}
			if got := e.Metrics(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("EvalContext.Metrics() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEvalContext_SetState(t *testing.T) {
	type fields struct {
		EvalCache   EvalCache
		state       State
		plan        *models.Plan
		logger      *log.Logger
		metrics     *models.AllocMetric
		eligibility *EvalEligibility
	}
	type args struct {
		s State
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
			e := &EvalContext{
				EvalCache:   tt.fields.EvalCache,
				state:       tt.fields.state,
				plan:        tt.fields.plan,
				logger:      tt.fields.logger,
				metrics:     tt.fields.metrics,
				eligibility: tt.fields.eligibility,
			}
			e.SetState(tt.args.s)
		})
	}
}

func TestEvalContext_Reset(t *testing.T) {
	type fields struct {
		EvalCache   EvalCache
		state       State
		plan        *models.Plan
		logger      *log.Logger
		metrics     *models.AllocMetric
		eligibility *EvalEligibility
	}
	tests := []struct {
		name   string
		fields fields
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &EvalContext{
				EvalCache:   tt.fields.EvalCache,
				state:       tt.fields.state,
				plan:        tt.fields.plan,
				logger:      tt.fields.logger,
				metrics:     tt.fields.metrics,
				eligibility: tt.fields.eligibility,
			}
			e.Reset()
		})
	}
}

func TestEvalContext_ProposedAllocs(t *testing.T) {
	type fields struct {
		EvalCache   EvalCache
		state       State
		plan        *models.Plan
		logger      *log.Logger
		metrics     *models.AllocMetric
		eligibility *EvalEligibility
	}
	type args struct {
		nodeID string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []*models.Allocation
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &EvalContext{
				EvalCache:   tt.fields.EvalCache,
				state:       tt.fields.state,
				plan:        tt.fields.plan,
				logger:      tt.fields.logger,
				metrics:     tt.fields.metrics,
				eligibility: tt.fields.eligibility,
			}
			got, err := e.ProposedAllocs(tt.args.nodeID)
			if (err != nil) != tt.wantErr {
				t.Errorf("EvalContext.ProposedAllocs() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("EvalContext.ProposedAllocs() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEvalContext_Eligibility(t *testing.T) {
	type fields struct {
		EvalCache   EvalCache
		state       State
		plan        *models.Plan
		logger      *log.Logger
		metrics     *models.AllocMetric
		eligibility *EvalEligibility
	}
	tests := []struct {
		name   string
		fields fields
		want   *EvalEligibility
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &EvalContext{
				EvalCache:   tt.fields.EvalCache,
				state:       tt.fields.state,
				plan:        tt.fields.plan,
				logger:      tt.fields.logger,
				metrics:     tt.fields.metrics,
				eligibility: tt.fields.eligibility,
			}
			if got := e.Eligibility(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("EvalContext.Eligibility() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewEvalEligibility(t *testing.T) {
	tests := []struct {
		name string
		want *EvalEligibility
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewEvalEligibility(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewEvalEligibility() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEvalEligibility_SetJob(t *testing.T) {
	type fields struct {
		job                  map[string]ComputedClassFeasibility
		jobEscaped           bool
		tasks                map[string]map[string]ComputedClassFeasibility
		tgEscapedConstraints map[string]bool
	}
	type args struct {
		job *models.Job
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
			e := &EvalEligibility{
				job:                  tt.fields.job,
				jobEscaped:           tt.fields.jobEscaped,
				tasks:                tt.fields.tasks,
				tgEscapedConstraints: tt.fields.tgEscapedConstraints,
			}
			e.SetJob(tt.args.job)
		})
	}
}

func TestEvalEligibility_HasEscaped(t *testing.T) {
	type fields struct {
		job                  map[string]ComputedClassFeasibility
		jobEscaped           bool
		tasks                map[string]map[string]ComputedClassFeasibility
		tgEscapedConstraints map[string]bool
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &EvalEligibility{
				job:                  tt.fields.job,
				jobEscaped:           tt.fields.jobEscaped,
				tasks:                tt.fields.tasks,
				tgEscapedConstraints: tt.fields.tgEscapedConstraints,
			}
			if got := e.HasEscaped(); got != tt.want {
				t.Errorf("EvalEligibility.HasEscaped() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEvalEligibility_GetClasses(t *testing.T) {
	type fields struct {
		job                  map[string]ComputedClassFeasibility
		jobEscaped           bool
		tasks                map[string]map[string]ComputedClassFeasibility
		tgEscapedConstraints map[string]bool
	}
	tests := []struct {
		name   string
		fields fields
		want   map[string]bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &EvalEligibility{
				job:                  tt.fields.job,
				jobEscaped:           tt.fields.jobEscaped,
				tasks:                tt.fields.tasks,
				tgEscapedConstraints: tt.fields.tgEscapedConstraints,
			}
			if got := e.GetClasses(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("EvalEligibility.GetClasses() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEvalEligibility_JobStatus(t *testing.T) {
	type fields struct {
		job                  map[string]ComputedClassFeasibility
		jobEscaped           bool
		tasks                map[string]map[string]ComputedClassFeasibility
		tgEscapedConstraints map[string]bool
	}
	type args struct {
		class string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   ComputedClassFeasibility
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &EvalEligibility{
				job:                  tt.fields.job,
				jobEscaped:           tt.fields.jobEscaped,
				tasks:                tt.fields.tasks,
				tgEscapedConstraints: tt.fields.tgEscapedConstraints,
			}
			if got := e.JobStatus(tt.args.class); got != tt.want {
				t.Errorf("EvalEligibility.JobStatus() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEvalEligibility_SetJobEligibility(t *testing.T) {
	type fields struct {
		job                  map[string]ComputedClassFeasibility
		jobEscaped           bool
		tasks                map[string]map[string]ComputedClassFeasibility
		tgEscapedConstraints map[string]bool
	}
	type args struct {
		eligible bool
		class    string
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
			e := &EvalEligibility{
				job:                  tt.fields.job,
				jobEscaped:           tt.fields.jobEscaped,
				tasks:                tt.fields.tasks,
				tgEscapedConstraints: tt.fields.tgEscapedConstraints,
			}
			e.SetJobEligibility(tt.args.eligible, tt.args.class)
		})
	}
}

func TestEvalEligibility_TaskStatus(t *testing.T) {
	type fields struct {
		job                  map[string]ComputedClassFeasibility
		jobEscaped           bool
		tasks                map[string]map[string]ComputedClassFeasibility
		tgEscapedConstraints map[string]bool
	}
	type args struct {
		tg    string
		class string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   ComputedClassFeasibility
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &EvalEligibility{
				job:                  tt.fields.job,
				jobEscaped:           tt.fields.jobEscaped,
				tasks:                tt.fields.tasks,
				tgEscapedConstraints: tt.fields.tgEscapedConstraints,
			}
			if got := e.TaskStatus(tt.args.tg, tt.args.class); got != tt.want {
				t.Errorf("EvalEligibility.TaskStatus() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEvalEligibility_SetTaskEligibility(t *testing.T) {
	type fields struct {
		job                  map[string]ComputedClassFeasibility
		jobEscaped           bool
		tasks                map[string]map[string]ComputedClassFeasibility
		tgEscapedConstraints map[string]bool
	}
	type args struct {
		eligible bool
		tg       string
		class    string
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
			e := &EvalEligibility{
				job:                  tt.fields.job,
				jobEscaped:           tt.fields.jobEscaped,
				tasks:                tt.fields.tasks,
				tgEscapedConstraints: tt.fields.tgEscapedConstraints,
			}
			e.SetTaskEligibility(tt.args.eligible, tt.args.tg, tt.args.class)
		})
	}
}
