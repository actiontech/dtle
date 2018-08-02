/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package scheduler

import (
	"reflect"
	"testing"
	ulog "udup/internal/logger"
)

func TestNewScheduler(t *testing.T) {
	type args struct {
		name    string
		logger  *ulog.Logger
		state   State
		planner Planner
	}
	tests := []struct {
		name    string
		args    args
		want    Scheduler
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewScheduler(tt.args.name, tt.args.logger, tt.args.state, tt.args.planner)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewScheduler() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewScheduler() = %v, want %v", got, tt.want)
			}
		})
	}
}
