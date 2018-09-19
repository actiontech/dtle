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

func TestPlan_Submit(t *testing.T) {
	type fields struct {
		srv *Server
	}
	type args struct {
		args  *models.PlanRequest
		reply *models.PlanResponse
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
			p := &Plan{
				srv: tt.fields.srv,
			}
			if err := p.Submit(tt.args.args, tt.args.reply); (err != nil) != tt.wantErr {
				t.Errorf("Plan.Submit() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
