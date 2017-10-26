package command

import (
	"flag"
	"reflect"
	"testing"
	"udup/api"

	"github.com/mitchellh/cli"
	"github.com/mitchellh/colorstring"
)

func TestMeta_FlagSet(t *testing.T) {
	type fields struct {
		Ui          cli.Ui
		flagAddress string
		noColor     bool
		region      string
	}
	type args struct {
		n  string
		fs FlagSetFlags
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *flag.FlagSet
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &Meta{
				Ui:          tt.fields.Ui,
				flagAddress: tt.fields.flagAddress,
				noColor:     tt.fields.noColor,
				region:      tt.fields.region,
			}
			if got := m.FlagSet(tt.args.n, tt.args.fs); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Meta.FlagSet() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMeta_Client(t *testing.T) {
	type fields struct {
		Ui          cli.Ui
		flagAddress string
		noColor     bool
		region      string
	}
	tests := []struct {
		name    string
		fields  fields
		want    *api.Client
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &Meta{
				Ui:          tt.fields.Ui,
				flagAddress: tt.fields.flagAddress,
				noColor:     tt.fields.noColor,
				region:      tt.fields.region,
			}
			got, err := m.Client()
			if (err != nil) != tt.wantErr {
				t.Errorf("Meta.Client() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Meta.Client() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMeta_Colorize(t *testing.T) {
	type fields struct {
		Ui          cli.Ui
		flagAddress string
		noColor     bool
		region      string
	}
	tests := []struct {
		name   string
		fields fields
		want   *colorstring.Colorize
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &Meta{
				Ui:          tt.fields.Ui,
				flagAddress: tt.fields.flagAddress,
				noColor:     tt.fields.noColor,
				region:      tt.fields.region,
			}
			if got := m.Colorize(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Meta.Colorize() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_generalOptionsUsage(t *testing.T) {
	tests := []struct {
		name string
		want string
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := generalOptionsUsage(); got != tt.want {
				t.Errorf("generalOptionsUsage() = %v, want %v", got, tt.want)
			}
		})
	}
}
