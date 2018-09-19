/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package client

import (
	"reflect"
	"testing"
	"github.com/actiontech/udup/internal/models"
)

func Test_diffResult_GoString(t *testing.T) {
	type fields struct {
		added   []*models.Allocation
		removed []*models.Allocation
		updated []allocTuple
		ignore  []*models.Allocation
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
				added:   tt.fields.added,
				removed: tt.fields.removed,
				updated: tt.fields.updated,
				ignore:  tt.fields.ignore,
			}
			if got := d.GoString(); got != tt.want {
				t.Errorf("diffResult.GoString() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_diffAllocs(t *testing.T) {
	type args struct {
		existing []*models.Allocation
		allocs   *allocUpdates
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
			if got := diffAllocs(tt.args.existing, tt.args.allocs); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("diffAllocs() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_persistState(t *testing.T) {
	type args struct {
		path string
		data interface{}
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
			if err := persistState(tt.args.path, tt.args.data); (err != nil) != tt.wantErr {
				t.Errorf("persistState() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
