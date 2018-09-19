/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package scheduler

import (
	"reflect"
	"sync"
	"testing"
	"github.com/actiontech/udup/internal/models"
	"github.com/actiontech/udup/internal/server/store"
)

func TestHarness_SubmitPlan(t *testing.T) {
	type fields struct {
		State         *store.StateStore
		Planner       Planner
		planLock      sync.Mutex
		Plans         []*models.Plan
		Evals         []*models.Evaluation
		CreateEvals   []*models.Evaluation
		ReblockEvals  []*models.Evaluation
		nextIndex     uint64
		nextIndexLock sync.Mutex
	}
	type args struct {
		plan *models.Plan
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *models.PlanResult
		want1   State
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &Harness{
				State:         tt.fields.State,
				Planner:       tt.fields.Planner,
				planLock:      tt.fields.planLock,
				Plans:         tt.fields.Plans,
				Evals:         tt.fields.Evals,
				CreateEvals:   tt.fields.CreateEvals,
				ReblockEvals:  tt.fields.ReblockEvals,
				nextIndex:     tt.fields.nextIndex,
				nextIndexLock: tt.fields.nextIndexLock,
			}
			got, got1, err := h.SubmitPlan(tt.args.plan)
			if (err != nil) != tt.wantErr {
				t.Errorf("Harness.SubmitPlan() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Harness.SubmitPlan() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("Harness.SubmitPlan() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestHarness_UpdateEval(t *testing.T) {
	type fields struct {
		State         *store.StateStore
		Planner       Planner
		planLock      sync.Mutex
		Plans         []*models.Plan
		Evals         []*models.Evaluation
		CreateEvals   []*models.Evaluation
		ReblockEvals  []*models.Evaluation
		nextIndex     uint64
		nextIndexLock sync.Mutex
	}
	type args struct {
		eval *models.Evaluation
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &Harness{
				State:         tt.fields.State,
				Planner:       tt.fields.Planner,
				planLock:      tt.fields.planLock,
				Plans:         tt.fields.Plans,
				Evals:         tt.fields.Evals,
				CreateEvals:   tt.fields.CreateEvals,
				ReblockEvals:  tt.fields.ReblockEvals,
				nextIndex:     tt.fields.nextIndex,
				nextIndexLock: tt.fields.nextIndexLock,
			}
			if err := h.UpdateEval(tt.args.eval); (err != nil) != tt.wantErr {
				t.Errorf("Harness.UpdateEval() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestHarness_CreateEval(t *testing.T) {
	type fields struct {
		State         *store.StateStore
		Planner       Planner
		planLock      sync.Mutex
		Plans         []*models.Plan
		Evals         []*models.Evaluation
		CreateEvals   []*models.Evaluation
		ReblockEvals  []*models.Evaluation
		nextIndex     uint64
		nextIndexLock sync.Mutex
	}
	type args struct {
		eval *models.Evaluation
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &Harness{
				State:         tt.fields.State,
				Planner:       tt.fields.Planner,
				planLock:      tt.fields.planLock,
				Plans:         tt.fields.Plans,
				Evals:         tt.fields.Evals,
				CreateEvals:   tt.fields.CreateEvals,
				ReblockEvals:  tt.fields.ReblockEvals,
				nextIndex:     tt.fields.nextIndex,
				nextIndexLock: tt.fields.nextIndexLock,
			}
			if err := h.CreateEval(tt.args.eval); (err != nil) != tt.wantErr {
				t.Errorf("Harness.CreateEval() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestHarness_ReblockEval(t *testing.T) {
	type fields struct {
		State         *store.StateStore
		Planner       Planner
		planLock      sync.Mutex
		Plans         []*models.Plan
		Evals         []*models.Evaluation
		CreateEvals   []*models.Evaluation
		ReblockEvals  []*models.Evaluation
		nextIndex     uint64
		nextIndexLock sync.Mutex
	}
	type args struct {
		eval *models.Evaluation
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &Harness{
				State:         tt.fields.State,
				Planner:       tt.fields.Planner,
				planLock:      tt.fields.planLock,
				Plans:         tt.fields.Plans,
				Evals:         tt.fields.Evals,
				CreateEvals:   tt.fields.CreateEvals,
				ReblockEvals:  tt.fields.ReblockEvals,
				nextIndex:     tt.fields.nextIndex,
				nextIndexLock: tt.fields.nextIndexLock,
			}
			if err := h.ReblockEval(tt.args.eval); (err != nil) != tt.wantErr {
				t.Errorf("Harness.ReblockEval() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestHarness_NextIndex(t *testing.T) {
	type fields struct {
		State         *store.StateStore
		Planner       Planner
		planLock      sync.Mutex
		Plans         []*models.Plan
		Evals         []*models.Evaluation
		CreateEvals   []*models.Evaluation
		ReblockEvals  []*models.Evaluation
		nextIndex     uint64
		nextIndexLock sync.Mutex
	}
	tests := []struct {
		name   string
		fields fields
		want   uint64
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &Harness{
				State:         tt.fields.State,
				Planner:       tt.fields.Planner,
				planLock:      tt.fields.planLock,
				Plans:         tt.fields.Plans,
				Evals:         tt.fields.Evals,
				CreateEvals:   tt.fields.CreateEvals,
				ReblockEvals:  tt.fields.ReblockEvals,
				nextIndex:     tt.fields.nextIndex,
				nextIndexLock: tt.fields.nextIndexLock,
			}
			if got := h.NextIndex(); got != tt.want {
				t.Errorf("Harness.NextIndex() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHarness_Snapshot(t *testing.T) {
	type fields struct {
		State         *store.StateStore
		Planner       Planner
		planLock      sync.Mutex
		Plans         []*models.Plan
		Evals         []*models.Evaluation
		CreateEvals   []*models.Evaluation
		ReblockEvals  []*models.Evaluation
		nextIndex     uint64
		nextIndexLock sync.Mutex
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
			h := &Harness{
				State:         tt.fields.State,
				Planner:       tt.fields.Planner,
				planLock:      tt.fields.planLock,
				Plans:         tt.fields.Plans,
				Evals:         tt.fields.Evals,
				CreateEvals:   tt.fields.CreateEvals,
				ReblockEvals:  tt.fields.ReblockEvals,
				nextIndex:     tt.fields.nextIndex,
				nextIndexLock: tt.fields.nextIndexLock,
			}
			if got := h.Snapshot(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Harness.Snapshot() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHarness_Scheduler(t *testing.T) {
	type fields struct {
		State         *store.StateStore
		Planner       Planner
		planLock      sync.Mutex
		Plans         []*models.Plan
		Evals         []*models.Evaluation
		CreateEvals   []*models.Evaluation
		ReblockEvals  []*models.Evaluation
		nextIndex     uint64
		nextIndexLock sync.Mutex
	}
	type args struct {
		factory Factory
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   Scheduler
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &Harness{
				State:         tt.fields.State,
				Planner:       tt.fields.Planner,
				planLock:      tt.fields.planLock,
				Plans:         tt.fields.Plans,
				Evals:         tt.fields.Evals,
				CreateEvals:   tt.fields.CreateEvals,
				ReblockEvals:  tt.fields.ReblockEvals,
				nextIndex:     tt.fields.nextIndex,
				nextIndexLock: tt.fields.nextIndexLock,
			}
			if got := h.Scheduler(tt.args.factory); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Harness.Scheduler() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHarness_Process(t *testing.T) {
	type fields struct {
		State         *store.StateStore
		Planner       Planner
		planLock      sync.Mutex
		Plans         []*models.Plan
		Evals         []*models.Evaluation
		CreateEvals   []*models.Evaluation
		ReblockEvals  []*models.Evaluation
		nextIndex     uint64
		nextIndexLock sync.Mutex
	}
	type args struct {
		factory Factory
		eval    *models.Evaluation
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &Harness{
				State:         tt.fields.State,
				Planner:       tt.fields.Planner,
				planLock:      tt.fields.planLock,
				Plans:         tt.fields.Plans,
				Evals:         tt.fields.Evals,
				CreateEvals:   tt.fields.CreateEvals,
				ReblockEvals:  tt.fields.ReblockEvals,
				nextIndex:     tt.fields.nextIndex,
				nextIndexLock: tt.fields.nextIndexLock,
			}
			if err := h.Process(tt.args.factory, tt.args.eval); (err != nil) != tt.wantErr {
				t.Errorf("Harness.Process() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestHarness_AssertEvalStatus(t *testing.T) {
	type fields struct {
		State         *store.StateStore
		Planner       Planner
		planLock      sync.Mutex
		Plans         []*models.Plan
		Evals         []*models.Evaluation
		CreateEvals   []*models.Evaluation
		ReblockEvals  []*models.Evaluation
		nextIndex     uint64
		nextIndexLock sync.Mutex
	}
	type args struct {
		t     *testing.T
		state string
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
			h := &Harness{
				State:         tt.fields.State,
				Planner:       tt.fields.Planner,
				planLock:      tt.fields.planLock,
				Plans:         tt.fields.Plans,
				Evals:         tt.fields.Evals,
				CreateEvals:   tt.fields.CreateEvals,
				ReblockEvals:  tt.fields.ReblockEvals,
				nextIndex:     tt.fields.nextIndex,
				nextIndexLock: tt.fields.nextIndexLock,
			}
			h.AssertEvalStatus(tt.args.t, tt.args.state)
		})
	}
}
