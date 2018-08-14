package command

import (
	"reflect"
	"testing"
	"udup/api"
)

func TestServerMembersCommand_Help(t *testing.T) {
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
			c := &ServerMembersCommand{
				Meta: tt.fields.Meta,
			}
			if got := c.Help(); got != tt.want {
				t.Errorf("ServerMembersCommand.Help() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestServerMembersCommand_Synopsis(t *testing.T) {
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
			c := &ServerMembersCommand{
				Meta: tt.fields.Meta,
			}
			if got := c.Synopsis(); got != tt.want {
				t.Errorf("ServerMembersCommand.Synopsis() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestServerMembersCommand_Run(t *testing.T) {
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
			c := &ServerMembersCommand{
				Meta: tt.fields.Meta,
			}
			if got := c.Run(tt.args.args); got != tt.want {
				t.Errorf("ServerMembersCommand.Run() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_standardOutput(t *testing.T) {
	type args struct {
		mem     []*api.AgentMember
		leaders map[string]string
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
			if got := standardOutput(tt.args.mem, tt.args.leaders); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("standardOutput() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_detailedOutput(t *testing.T) {
	type args struct {
		mem []*api.AgentMember
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
			if got := detailedOutput(tt.args.mem); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("detailedOutput() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_regionLeaders(t *testing.T) {
	type args struct {
		client *api.Client
		mem    []*api.AgentMember
	}
	tests := []struct {
		name    string
		args    args
		want    map[string]string
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := regionLeaders(tt.args.client, tt.args.mem)
			if (err != nil) != tt.wantErr {
				t.Errorf("regionLeaders() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("regionLeaders() = %v, want %v", got, tt.want)
			}
		})
	}
}
