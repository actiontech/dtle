/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package command

import (
	"reflect"
	"testing"
	"github.com/actiontech/dtle/api"

	"github.com/mitchellh/colorstring"
)

func TestNodeStatusCommand_Help(t *testing.T) {
	type fields struct {
		Meta        Meta
		color       *colorstring.Colorize
		length      int
		verbose     bool
		list_allocs bool
		self        bool
		stats       bool
		json        bool
		tmpl        string
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
			c := &NodeStatusCommand{
				Meta:        tt.fields.Meta,
				color:       tt.fields.color,
				length:      tt.fields.length,
				verbose:     tt.fields.verbose,
				list_allocs: tt.fields.list_allocs,
				self:        tt.fields.self,
				stats:       tt.fields.stats,
				json:        tt.fields.json,
				tmpl:        tt.fields.tmpl,
			}
			if got := c.Help(); got != tt.want {
				t.Errorf("NodeStatusCommand.Help() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNodeStatusCommand_Synopsis(t *testing.T) {
	type fields struct {
		Meta        Meta
		color       *colorstring.Colorize
		length      int
		verbose     bool
		list_allocs bool
		self        bool
		stats       bool
		json        bool
		tmpl        string
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
			c := &NodeStatusCommand{
				Meta:        tt.fields.Meta,
				color:       tt.fields.color,
				length:      tt.fields.length,
				verbose:     tt.fields.verbose,
				list_allocs: tt.fields.list_allocs,
				self:        tt.fields.self,
				stats:       tt.fields.stats,
				json:        tt.fields.json,
				tmpl:        tt.fields.tmpl,
			}
			if got := c.Synopsis(); got != tt.want {
				t.Errorf("NodeStatusCommand.Synopsis() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNodeStatusCommand_Run(t *testing.T) {
	type fields struct {
		Meta        Meta
		color       *colorstring.Colorize
		length      int
		verbose     bool
		list_allocs bool
		self        bool
		stats       bool
		json        bool
		tmpl        string
	}
	type args struct {
		args []string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   int
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &NodeStatusCommand{
				Meta:        tt.fields.Meta,
				color:       tt.fields.color,
				length:      tt.fields.length,
				verbose:     tt.fields.verbose,
				list_allocs: tt.fields.list_allocs,
				self:        tt.fields.self,
				stats:       tt.fields.stats,
				json:        tt.fields.json,
				tmpl:        tt.fields.tmpl,
			}
			if got := c.Run(tt.args.args); got != tt.want {
				t.Errorf("NodeStatusCommand.Run() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_nodeDrivers(t *testing.T) {
	type args struct {
		n *api.Node
	}
	tests := []struct {
		name string
		args args
		want []string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := nodeDrivers(tt.args.n); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("nodeDrivers() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNodeStatusCommand_formatNode(t *testing.T) {
	type fields struct {
		Meta        Meta
		color       *colorstring.Colorize
		length      int
		verbose     bool
		list_allocs bool
		self        bool
		stats       bool
		json        bool
		tmpl        string
	}
	type args struct {
		client *api.Client
		node   *api.Node
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   int
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &NodeStatusCommand{
				Meta:        tt.fields.Meta,
				color:       tt.fields.color,
				length:      tt.fields.length,
				verbose:     tt.fields.verbose,
				list_allocs: tt.fields.list_allocs,
				self:        tt.fields.self,
				stats:       tt.fields.stats,
				json:        tt.fields.json,
				tmpl:        tt.fields.tmpl,
			}
			if got := c.formatNode(tt.args.client, tt.args.node); got != tt.want {
				t.Errorf("NodeStatusCommand.formatNode() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNodeStatusCommand_formatAttributes(t *testing.T) {
	type fields struct {
		Meta        Meta
		color       *colorstring.Colorize
		length      int
		verbose     bool
		list_allocs bool
		self        bool
		stats       bool
		json        bool
		tmpl        string
	}
	type args struct {
		node *api.Node
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
			c := &NodeStatusCommand{
				Meta:        tt.fields.Meta,
				color:       tt.fields.color,
				length:      tt.fields.length,
				verbose:     tt.fields.verbose,
				list_allocs: tt.fields.list_allocs,
				self:        tt.fields.self,
				stats:       tt.fields.stats,
				json:        tt.fields.json,
				tmpl:        tt.fields.tmpl,
			}
			c.formatAttributes(tt.args.node)
		})
	}
}

func TestNodeStatusCommand_formatMeta(t *testing.T) {
	type fields struct {
		Meta        Meta
		color       *colorstring.Colorize
		length      int
		verbose     bool
		list_allocs bool
		self        bool
		stats       bool
		json        bool
		tmpl        string
	}
	type args struct {
		node *api.Node
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
			c := &NodeStatusCommand{
				Meta:        tt.fields.Meta,
				color:       tt.fields.color,
				length:      tt.fields.length,
				verbose:     tt.fields.verbose,
				list_allocs: tt.fields.list_allocs,
				self:        tt.fields.self,
				stats:       tt.fields.stats,
				json:        tt.fields.json,
				tmpl:        tt.fields.tmpl,
			}
			c.formatMeta(tt.args.node)
		})
	}
}

func Test_getRunningAllocs(t *testing.T) {
	type args struct {
		client *api.Client
		nodeID string
	}
	tests := []struct {
		name    string
		args    args
		want    []*api.Allocation
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getRunningAllocs(tt.args.client, tt.args.nodeID)
			if (err != nil) != tt.wantErr {
				t.Errorf("getRunningAllocs() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getRunningAllocs() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getAllocs(t *testing.T) {
	type args struct {
		client *api.Client
		node   *api.Node
		length int
	}
	tests := []struct {
		name    string
		args    args
		want    []string
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getAllocs(tt.args.client, tt.args.node, tt.args.length)
			if (err != nil) != tt.wantErr {
				t.Errorf("getAllocs() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getAllocs() = %v, want %v", got, tt.want)
			}
		})
	}
}
