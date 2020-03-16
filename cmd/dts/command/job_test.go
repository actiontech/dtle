/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package command

import (
	"io"
	"reflect"
	"testing"
	"github.com/actiontech/dts/api"

	"github.com/hashicorp/hcl/hcl/ast"
)

func TestJobGetter_ApiJob(t *testing.T) {
	type fields struct {
		testStdin io.Reader
	}
	type args struct {
		jpath string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *api.Job
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			j := &JobGetter{
				testStdin: tt.fields.testStdin,
			}
			got, err := j.ApiJob(tt.args.jpath)
			if (err != nil) != tt.wantErr {
				t.Errorf("JobGetter.ApiJob() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("JobGetter.ApiJob() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParse(t *testing.T) {
	type args struct {
		r io.Reader
	}
	tests := []struct {
		name    string
		args    args
		want    *api.Job
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Parse(tt.args.r)
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Parse() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParseFile(t *testing.T) {
	type args struct {
		path string
	}
	tests := []struct {
		name    string
		args    args
		want    *api.Job
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseFile(tt.args.path)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseFile() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ParseFile() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_parseJob(t *testing.T) {
	type args struct {
		result *api.Job
		list   *ast.ObjectList
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
			if err := parseJob(tt.args.result, tt.args.list); (err != nil) != tt.wantErr {
				t.Errorf("parseJob() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_parseTasks(t *testing.T) {
	type args struct {
		result *api.Job
		list   *ast.ObjectList
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
			if err := parseTasks(tt.args.result, tt.args.list); (err != nil) != tt.wantErr {
				t.Errorf("parseTasks() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_checkHCLKeys(t *testing.T) {
	type args struct {
		node  ast.Node
		valid []string
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
			if err := checkHCLKeys(tt.args.node, tt.args.valid); (err != nil) != tt.wantErr {
				t.Errorf("checkHCLKeys() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
