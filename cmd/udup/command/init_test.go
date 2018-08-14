package command

import "testing"

func TestInitCommand_Help(t *testing.T) {
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
			c := &InitCommand{
				Meta: tt.fields.Meta,
			}
			if got := c.Help(); got != tt.want {
				t.Errorf("InitCommand.Help() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestInitCommand_Synopsis(t *testing.T) {
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
			c := &InitCommand{
				Meta: tt.fields.Meta,
			}
			if got := c.Synopsis(); got != tt.want {
				t.Errorf("InitCommand.Synopsis() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestInitCommand_Run(t *testing.T) {
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
			c := &InitCommand{
				Meta: tt.fields.Meta,
			}
			if got := c.Run(tt.args.args); got != tt.want {
				t.Errorf("InitCommand.Run() = %v, want %v", got, tt.want)
			}
		})
	}
}
