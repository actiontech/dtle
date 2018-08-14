package command

import (
	"testing"

	"github.com/mitchellh/cli"
)

func TestVersionCommand_Help(t *testing.T) {
	type fields struct {
		Version string
		Branch  string
		Commit  string
		Ui      cli.Ui
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		// TODO: Add test cases.
		{"V1", fields{"1.0", "master", "g94c569b", nil}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &VersionCommand{
				Version: tt.fields.Version,
				Branch:  tt.fields.Branch,
				Commit:  tt.fields.Commit,
				Ui:      tt.fields.Ui,
			}
			if got := c.Help(); got != tt.want {
				t.Errorf("VersionCommand.Help() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestVersionCommand_Run(t *testing.T) {
	type fields struct {
		Version string
		Branch  string
		Commit  string
		Ui      cli.Ui
	}
	type args struct {
		in0 []string
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
			c := &VersionCommand{
				Version: tt.fields.Version,
				Branch:  tt.fields.Branch,
				Commit:  tt.fields.Commit,
				Ui:      tt.fields.Ui,
			}
			if got := c.Run(tt.args.in0); got != tt.want {
				t.Errorf("VersionCommand.Run() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestVersionCommand_Synopsis(t *testing.T) {
	type fields struct {
		Version string
		Branch  string
		Commit  string
		Ui      cli.Ui
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
			c := &VersionCommand{
				Version: tt.fields.Version,
				Branch:  tt.fields.Branch,
				Commit:  tt.fields.Commit,
				Ui:      tt.fields.Ui,
			}
			if got := c.Synopsis(); got != tt.want {
				t.Errorf("VersionCommand.Synopsis() = %v, want %v", got, tt.want)
			}
		})
	}
}
