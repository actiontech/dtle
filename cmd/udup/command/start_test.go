package command

import (
	"reflect"
	"testing"
	"udup/api"
)

func TestStartCommand_Help(t *testing.T) {
	type fields struct {
		Meta      Meta
		JobGetter JobGetter
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
			c := &StartCommand{
				Meta:      tt.fields.Meta,
				JobGetter: tt.fields.JobGetter,
			}
			if got := c.Help(); got != tt.want {
				t.Errorf("StartCommand.Help() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStartCommand_Synopsis(t *testing.T) {
	type fields struct {
		Meta      Meta
		JobGetter JobGetter
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
			c := &StartCommand{
				Meta:      tt.fields.Meta,
				JobGetter: tt.fields.JobGetter,
			}
			if got := c.Synopsis(); got != tt.want {
				t.Errorf("StartCommand.Synopsis() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStartCommand_Run(t *testing.T) {
	type fields struct {
		Meta      Meta
		JobGetter JobGetter
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
			c := &StartCommand{
				Meta:      tt.fields.Meta,
				JobGetter: tt.fields.JobGetter,
			}
			if got := c.Run(tt.args.args); got != tt.want {
				t.Errorf("StartCommand.Run() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_parseCheckIndex(t *testing.T) {
	type args struct {
		input string
	}
	tests := []struct {
		name    string
		args    args
		want    uint64
		want1   bool
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1, err := parseCheckIndex(tt.args.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseCheckIndex() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("parseCheckIndex() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("parseCheckIndex() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestStartCommand_validateLocal(t *testing.T) {
	type fields struct {
		Meta      Meta
		JobGetter JobGetter
	}
	type args struct {
		aj *api.Job
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *api.JobValidateResponse
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &StartCommand{
				Meta:      tt.fields.Meta,
				JobGetter: tt.fields.JobGetter,
			}
			got, err := c.validateLocal(tt.args.aj)
			if (err != nil) != tt.wantErr {
				t.Errorf("StartCommand.validateLocal() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("StartCommand.validateLocal() = %v, want %v", got, tt.want)
			}
		})
	}
}
