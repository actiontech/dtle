/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package command

import "testing"

func TestStopCommand_Help(t *testing.T) {
	type fields struct {
		Meta Meta
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
			c := &StopCommand{
				Meta: tt.fields.Meta,
			}
			if got := c.Help(); got != tt.want {
				t.Errorf("StopCommand.Help() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStopCommand_Synopsis(t *testing.T) {
	type fields struct {
		Meta Meta
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
			c := &StopCommand{
				Meta: tt.fields.Meta,
			}
			if got := c.Synopsis(); got != tt.want {
				t.Errorf("StopCommand.Synopsis() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStopCommand_Run(t *testing.T) {
	type fields struct {
		Meta Meta
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
			c := &StopCommand{
				Meta: tt.fields.Meta,
			}
			if got := c.Run(tt.args.args); got != tt.want {
				t.Errorf("StopCommand.Run() = %v, want %v", got, tt.want)
			}
		})
	}
}
