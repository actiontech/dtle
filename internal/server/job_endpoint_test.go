/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package server

import (
	"testing"
	"github.com/actiontech/dtle/internal/models"
)

func TestJob_Register(t *testing.T) {
	type fields struct {
		srv *Server
	}
	type args struct {
		args  *models.JobRegisterRequest
		reply *models.JobResponse
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
			j := &Job{
				srv: tt.fields.srv,
			}
			if err := j.Register(tt.args.args, tt.args.reply); (err != nil) != tt.wantErr {
				t.Errorf("Job.Register() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestJob_UpdateStatus(t *testing.T) {
	type fields struct {
		srv *Server
	}
	type args struct {
		args  *models.JobUpdateStatusRequest
		reply *models.JobResponse
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
			j := &Job{
				srv: tt.fields.srv,
			}
			if err := j.UpdateStatus(tt.args.args, tt.args.reply); (err != nil) != tt.wantErr {
				t.Errorf("Job.UpdateStatus() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestJob_Validate(t *testing.T) {
	type fields struct {
		srv *Server
	}
	type args struct {
		args  *models.JobValidateRequest
		reply *models.JobValidateResponse
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
			j := &Job{
				srv: tt.fields.srv,
			}
			if err := j.Validate(tt.args.args, tt.args.reply); (err != nil) != tt.wantErr {
				t.Errorf("Job.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestJob_Evaluate(t *testing.T) {
	type fields struct {
		srv *Server
	}
	type args struct {
		args  *models.JobEvaluateRequest
		reply *models.JobResponse
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
			j := &Job{
				srv: tt.fields.srv,
			}
			if err := j.Evaluate(tt.args.args, tt.args.reply); (err != nil) != tt.wantErr {
				t.Errorf("Job.Evaluate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestJob_Deregister(t *testing.T) {
	type fields struct {
		srv *Server
	}
	type args struct {
		args  *models.JobDeregisterRequest
		reply *models.JobResponse
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
			j := &Job{
				srv: tt.fields.srv,
			}
			if err := j.Deregister(tt.args.args, tt.args.reply); (err != nil) != tt.wantErr {
				t.Errorf("Job.Deregister() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestJob_GetJob(t *testing.T) {
	type fields struct {
		srv *Server
	}
	type args struct {
		args  *models.JobSpecificRequest
		reply *models.SingleJobResponse
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
			j := &Job{
				srv: tt.fields.srv,
			}
			if err := j.GetJob(tt.args.args, tt.args.reply); (err != nil) != tt.wantErr {
				t.Errorf("Job.GetJob() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestJob_List(t *testing.T) {
	type fields struct {
		srv *Server
	}
	type args struct {
		args  *models.JobListRequest
		reply *models.JobListResponse
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
			j := &Job{
				srv: tt.fields.srv,
			}
			if err := j.List(tt.args.args, tt.args.reply); (err != nil) != tt.wantErr {
				t.Errorf("Job.List() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestJob_Allocations(t *testing.T) {
	type fields struct {
		srv *Server
	}
	type args struct {
		args  *models.JobSpecificRequest
		reply *models.JobAllocationsResponse
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
			j := &Job{
				srv: tt.fields.srv,
			}
			if err := j.Allocations(tt.args.args, tt.args.reply); (err != nil) != tt.wantErr {
				t.Errorf("Job.Allocations() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestJob_Evaluations(t *testing.T) {
	type fields struct {
		srv *Server
	}
	type args struct {
		args  *models.JobSpecificRequest
		reply *models.JobEvaluationsResponse
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
			j := &Job{
				srv: tt.fields.srv,
			}
			if err := j.Evaluations(tt.args.args, tt.args.reply); (err != nil) != tt.wantErr {
				t.Errorf("Job.Evaluations() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestJob_Plan(t *testing.T) {
	type fields struct {
		srv *Server
	}
	type args struct {
		args  *models.JobPlanRequest
		reply *models.JobPlanResponse
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
			j := &Job{
				srv: tt.fields.srv,
			}
			if err := j.Plan(tt.args.args, tt.args.reply); (err != nil) != tt.wantErr {
				t.Errorf("Job.Plan() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_validateJob(t *testing.T) {
	type args struct {
		job *models.Job
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
			if err := validateJob(tt.args.job); (err != nil) != tt.wantErr {
				t.Errorf("validateJob() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
