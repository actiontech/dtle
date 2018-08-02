/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package server

import (
	"testing"
	"udup/internal/models"
)

func TestStatus_Version(t *testing.T) {
	type fields struct {
		srv *Server
	}
	type args struct {
		args  *models.GenericRequest
		reply *models.VersionResponse
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
			s := &Status{
				srv: tt.fields.srv,
			}
			if err := s.Version(tt.args.args, tt.args.reply); (err != nil) != tt.wantErr {
				t.Errorf("Status.Version() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestStatus_Ping(t *testing.T) {
	type fields struct {
		srv *Server
	}
	type args struct {
		args  struct{}
		reply *struct{}
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
			s := &Status{
				srv: tt.fields.srv,
			}
			if err := s.Ping(tt.args.args, tt.args.reply); (err != nil) != tt.wantErr {
				t.Errorf("Status.Ping() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestStatus_Leader(t *testing.T) {
	type fields struct {
		srv *Server
	}
	type args struct {
		args  *models.GenericRequest
		reply *string
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
			s := &Status{
				srv: tt.fields.srv,
			}
			if err := s.Leader(tt.args.args, tt.args.reply); (err != nil) != tt.wantErr {
				t.Errorf("Status.Leader() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestStatus_Peers(t *testing.T) {
	type fields struct {
		srv *Server
	}
	type args struct {
		args  *models.GenericRequest
		reply *[]string
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
			s := &Status{
				srv: tt.fields.srv,
			}
			if err := s.Peers(tt.args.args, tt.args.reply); (err != nil) != tt.wantErr {
				t.Errorf("Status.Peers() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestStatus_Members(t *testing.T) {
	type fields struct {
		srv *Server
	}
	type args struct {
		args  *models.GenericRequest
		reply *models.ServerMembersResponse
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
			s := &Status{
				srv: tt.fields.srv,
			}
			if err := s.Members(tt.args.args, tt.args.reply); (err != nil) != tt.wantErr {
				t.Errorf("Status.Members() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
