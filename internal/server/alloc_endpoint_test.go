/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package server

import (
	"testing"
	"github.com/actiontech/dts/internal/models"
)

func TestAlloc_List(t *testing.T) {
	type fields struct {
		srv *Server
	}
	type args struct {
		args  *models.AllocListRequest
		reply *models.AllocListResponse
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
			a := &Alloc{
				srv: tt.fields.srv,
			}
			if err := a.List(tt.args.args, tt.args.reply); (err != nil) != tt.wantErr {
				t.Errorf("Alloc.List() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestAlloc_GetAlloc(t *testing.T) {
	type fields struct {
		srv *Server
	}
	type args struct {
		args  *models.AllocSpecificRequest
		reply *models.SingleAllocResponse
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
			a := &Alloc{
				srv: tt.fields.srv,
			}
			if err := a.GetAlloc(tt.args.args, tt.args.reply); (err != nil) != tt.wantErr {
				t.Errorf("Alloc.GetAlloc() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestAlloc_GetAllocs(t *testing.T) {
	type fields struct {
		srv *Server
	}
	type args struct {
		args  *models.AllocsGetRequest
		reply *models.AllocsGetResponse
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
			a := &Alloc{
				srv: tt.fields.srv,
			}
			if err := a.GetAllocs(tt.args.args, tt.args.reply); (err != nil) != tt.wantErr {
				t.Errorf("Alloc.GetAllocs() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
