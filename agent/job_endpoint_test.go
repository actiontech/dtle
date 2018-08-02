/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package agent

import (
	"log"
	"net"
	"net/http"
	"reflect"
	"testing"
	"udup/api"
	"udup/internal/models"
)

func TestHTTPServer_JobsRequest(t *testing.T) {
	type fields struct {
		agent    *Agent
		mux      *http.ServeMux
		listener net.Listener
		logger   *log.Logger
		addr     string
	}
	type args struct {
		resp http.ResponseWriter
		req  *http.Request
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    interface{}
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &HTTPServer{
				agent:    tt.fields.agent,
				mux:      tt.fields.mux,
				listener: tt.fields.listener,
				logger:   tt.fields.logger,
				addr:     tt.fields.addr,
			}
			got, err := s.JobsRequest(tt.args.resp, tt.args.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("HTTPServer.JobsRequest() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("HTTPServer.JobsRequest() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHTTPServer_jobListRequest(t *testing.T) {
	type fields struct {
		agent    *Agent
		mux      *http.ServeMux
		listener net.Listener
		logger   *log.Logger
		addr     string
	}
	type args struct {
		resp http.ResponseWriter
		req  *http.Request
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    interface{}
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &HTTPServer{
				agent:    tt.fields.agent,
				mux:      tt.fields.mux,
				listener: tt.fields.listener,
				logger:   tt.fields.logger,
				addr:     tt.fields.addr,
			}
			got, err := s.jobListRequest(tt.args.resp, tt.args.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("HTTPServer.jobListRequest() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("HTTPServer.jobListRequest() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHTTPServer_JobSpecificRequest(t *testing.T) {
	type fields struct {
		agent    *Agent
		mux      *http.ServeMux
		listener net.Listener
		logger   *log.Logger
		addr     string
	}
	type args struct {
		resp http.ResponseWriter
		req  *http.Request
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    interface{}
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &HTTPServer{
				agent:    tt.fields.agent,
				mux:      tt.fields.mux,
				listener: tt.fields.listener,
				logger:   tt.fields.logger,
				addr:     tt.fields.addr,
			}
			got, err := s.JobSpecificRequest(tt.args.resp, tt.args.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("HTTPServer.JobSpecificRequest() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("HTTPServer.JobSpecificRequest() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHTTPServer_jobAllocations(t *testing.T) {
	type fields struct {
		agent    *Agent
		mux      *http.ServeMux
		listener net.Listener
		logger   *log.Logger
		addr     string
	}
	type args struct {
		resp    http.ResponseWriter
		req     *http.Request
		jobName string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    interface{}
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &HTTPServer{
				agent:    tt.fields.agent,
				mux:      tt.fields.mux,
				listener: tt.fields.listener,
				logger:   tt.fields.logger,
				addr:     tt.fields.addr,
			}
			got, err := s.jobAllocations(tt.args.resp, tt.args.req, tt.args.jobName)
			if (err != nil) != tt.wantErr {
				t.Errorf("HTTPServer.jobAllocations() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("HTTPServer.jobAllocations() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHTTPServer_jobEvaluations(t *testing.T) {
	type fields struct {
		agent    *Agent
		mux      *http.ServeMux
		listener net.Listener
		logger   *log.Logger
		addr     string
	}
	type args struct {
		resp    http.ResponseWriter
		req     *http.Request
		jobName string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    interface{}
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &HTTPServer{
				agent:    tt.fields.agent,
				mux:      tt.fields.mux,
				listener: tt.fields.listener,
				logger:   tt.fields.logger,
				addr:     tt.fields.addr,
			}
			got, err := s.jobEvaluations(tt.args.resp, tt.args.req, tt.args.jobName)
			if (err != nil) != tt.wantErr {
				t.Errorf("HTTPServer.jobEvaluations() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("HTTPServer.jobEvaluations() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHTTPServer_jobCRUD(t *testing.T) {
	type fields struct {
		agent    *Agent
		mux      *http.ServeMux
		listener net.Listener
		logger   *log.Logger
		addr     string
	}
	type args struct {
		resp    http.ResponseWriter
		req     *http.Request
		jobName string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    interface{}
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &HTTPServer{
				agent:    tt.fields.agent,
				mux:      tt.fields.mux,
				listener: tt.fields.listener,
				logger:   tt.fields.logger,
				addr:     tt.fields.addr,
			}
			got, err := s.jobCRUD(tt.args.resp, tt.args.req, tt.args.jobName)
			if (err != nil) != tt.wantErr {
				t.Errorf("HTTPServer.jobCRUD() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("HTTPServer.jobCRUD() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHTTPServer_jobQuery(t *testing.T) {
	type fields struct {
		agent    *Agent
		mux      *http.ServeMux
		listener net.Listener
		logger   *log.Logger
		addr     string
	}
	type args struct {
		resp  http.ResponseWriter
		req   *http.Request
		jobId string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    interface{}
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &HTTPServer{
				agent:    tt.fields.agent,
				mux:      tt.fields.mux,
				listener: tt.fields.listener,
				logger:   tt.fields.logger,
				addr:     tt.fields.addr,
			}
			got, err := s.jobQuery(tt.args.resp, tt.args.req, tt.args.jobId)
			if (err != nil) != tt.wantErr {
				t.Errorf("HTTPServer.jobQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("HTTPServer.jobQuery() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHTTPServer_jobUpdate(t *testing.T) {
	type fields struct {
		agent    *Agent
		mux      *http.ServeMux
		listener net.Listener
		logger   *log.Logger
		addr     string
	}
	type args struct {
		resp    http.ResponseWriter
		req     *http.Request
		jobName string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    interface{}
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &HTTPServer{
				agent:    tt.fields.agent,
				mux:      tt.fields.mux,
				listener: tt.fields.listener,
				logger:   tt.fields.logger,
				addr:     tt.fields.addr,
			}
			got, err := s.jobUpdate(tt.args.resp, tt.args.req, tt.args.jobName)
			if (err != nil) != tt.wantErr {
				t.Errorf("HTTPServer.jobUpdate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("HTTPServer.jobUpdate() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHTTPServer_jobDelete(t *testing.T) {
	type fields struct {
		agent    *Agent
		mux      *http.ServeMux
		listener net.Listener
		logger   *log.Logger
		addr     string
	}
	type args struct {
		resp    http.ResponseWriter
		req     *http.Request
		jobName string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    interface{}
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &HTTPServer{
				agent:    tt.fields.agent,
				mux:      tt.fields.mux,
				listener: tt.fields.listener,
				logger:   tt.fields.logger,
				addr:     tt.fields.addr,
			}
			got, err := s.jobDelete(tt.args.resp, tt.args.req, tt.args.jobName)
			if (err != nil) != tt.wantErr {
				t.Errorf("HTTPServer.jobDelete() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("HTTPServer.jobDelete() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHTTPServer_jobResumeRequest(t *testing.T) {
	type fields struct {
		agent    *Agent
		mux      *http.ServeMux
		listener net.Listener
		logger   *log.Logger
		addr     string
	}
	type args struct {
		resp http.ResponseWriter
		req  *http.Request
		name string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    interface{}
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &HTTPServer{
				agent:    tt.fields.agent,
				mux:      tt.fields.mux,
				listener: tt.fields.listener,
				logger:   tt.fields.logger,
				addr:     tt.fields.addr,
			}
			got, err := s.jobResumeRequest(tt.args.resp, tt.args.req, tt.args.name)
			if (err != nil) != tt.wantErr {
				t.Errorf("HTTPServer.jobResumeRequest() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("HTTPServer.jobResumeRequest() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHTTPServer_jobPauseRequest(t *testing.T) {
	type fields struct {
		agent    *Agent
		mux      *http.ServeMux
		listener net.Listener
		logger   *log.Logger
		addr     string
	}
	type args struct {
		resp http.ResponseWriter
		req  *http.Request
		name string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    interface{}
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &HTTPServer{
				agent:    tt.fields.agent,
				mux:      tt.fields.mux,
				listener: tt.fields.listener,
				logger:   tt.fields.logger,
				addr:     tt.fields.addr,
			}
			got, err := s.jobPauseRequest(tt.args.resp, tt.args.req, tt.args.name)
			if (err != nil) != tt.wantErr {
				t.Errorf("HTTPServer.jobPauseRequest() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("HTTPServer.jobPauseRequest() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestApiJobToStructJob(t *testing.T) {
	type args struct {
		job *api.Job
	}
	tests := []struct {
		name string
		args args
		want *models.Job
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ApiJobToStructJob(tt.args.job); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ApiJobToStructJob() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestApiTaskToStructsTask(t *testing.T) {
	type args struct {
		apiTask     *api.Task
		structsTask *models.Task
	}
	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ApiTaskToStructsTask(tt.args.apiTask, tt.args.structsTask)
		})
	}
}
