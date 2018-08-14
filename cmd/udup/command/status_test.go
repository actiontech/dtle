package command

import (
	"reflect"
	"testing"
	"udup/api"
)

func TestStatusCommand_Help(t *testing.T) {
	type fields struct {
		Meta      Meta
		length    int
		evals     bool
		allAllocs bool
		verbose   bool
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
			c := &StatusCommand{
				Meta:      tt.fields.Meta,
				length:    tt.fields.length,
				evals:     tt.fields.evals,
				allAllocs: tt.fields.allAllocs,
				verbose:   tt.fields.verbose,
			}
			if got := c.Help(); got != tt.want {
				t.Errorf("StatusCommand.Help() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStatusCommand_Synopsis(t *testing.T) {
	type fields struct {
		Meta      Meta
		length    int
		evals     bool
		allAllocs bool
		verbose   bool
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
			c := &StatusCommand{
				Meta:      tt.fields.Meta,
				length:    tt.fields.length,
				evals:     tt.fields.evals,
				allAllocs: tt.fields.allAllocs,
				verbose:   tt.fields.verbose,
			}
			if got := c.Synopsis(); got != tt.want {
				t.Errorf("StatusCommand.Synopsis() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStatusCommand_Run(t *testing.T) {
	type fields struct {
		Meta      Meta
		length    int
		evals     bool
		allAllocs bool
		verbose   bool
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
			c := &StatusCommand{
				Meta:      tt.fields.Meta,
				length:    tt.fields.length,
				evals:     tt.fields.evals,
				allAllocs: tt.fields.allAllocs,
				verbose:   tt.fields.verbose,
			}
			if got := c.Run(tt.args.args); got != tt.want {
				t.Errorf("StatusCommand.Run() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStatusCommand_outputJobInfo(t *testing.T) {
	type fields struct {
		Meta      Meta
		length    int
		evals     bool
		allAllocs bool
		verbose   bool
	}
	type args struct {
		client *api.Client
		job    *api.Job
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
			c := &StatusCommand{
				Meta:      tt.fields.Meta,
				length:    tt.fields.length,
				evals:     tt.fields.evals,
				allAllocs: tt.fields.allAllocs,
				verbose:   tt.fields.verbose,
			}
			if err := c.outputJobInfo(tt.args.client, tt.args.job); (err != nil) != tt.wantErr {
				t.Errorf("StatusCommand.outputJobInfo() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestStatusCommand_outputJobSummary(t *testing.T) {
	type fields struct {
		Meta      Meta
		length    int
		evals     bool
		allAllocs bool
		verbose   bool
	}
	type args struct {
		client *api.Client
		job    *api.Job
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
			c := &StatusCommand{
				Meta:      tt.fields.Meta,
				length:    tt.fields.length,
				evals:     tt.fields.evals,
				allAllocs: tt.fields.allAllocs,
				verbose:   tt.fields.verbose,
			}
			if err := c.outputJobSummary(tt.args.client, tt.args.job); (err != nil) != tt.wantErr {
				t.Errorf("StatusCommand.outputJobSummary() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestStatusCommand_outputFailedPlacements(t *testing.T) {
	type fields struct {
		Meta      Meta
		length    int
		evals     bool
		allAllocs bool
		verbose   bool
	}
	type args struct {
		failedEval *api.Evaluation
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
			c := &StatusCommand{
				Meta:      tt.fields.Meta,
				length:    tt.fields.length,
				evals:     tt.fields.evals,
				allAllocs: tt.fields.allAllocs,
				verbose:   tt.fields.verbose,
			}
			c.outputFailedPlacements(tt.args.failedEval)
		})
	}
}

func Test_sortedTaskFromMetrics(t *testing.T) {
	type args struct {
		groups map[string]*api.AllocationMetric
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
			if got := sortedTaskFromMetrics(tt.args.groups); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("sortedTaskFromMetrics() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_createStatusListOutput(t *testing.T) {
	type args struct {
		jobs []*api.JobListStub
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := createStatusListOutput(tt.args.jobs); got != tt.want {
				t.Errorf("createStatusListOutput() = %v, want %v", got, tt.want)
			}
		})
	}
}
