package server

import (
	"testing"
	"udup/internal/models"
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
