package agent

import (
	"io"
	"reflect"
	"testing"
	"udup/internal/config"

	"github.com/hashicorp/hcl/hcl/ast"
)

func TestParseConfigFile(t *testing.T) {
	type args struct {
		path string
	}
	tests := []struct {
		name    string
		args    args
		want    *Config
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseConfigFile(tt.args.path)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseConfigFile() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ParseConfigFile() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParseConfig(t *testing.T) {
	type args struct {
		r io.Reader
	}
	tests := []struct {
		name    string
		args    args
		want    *Config
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseConfig(tt.args.r)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseConfig() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ParseConfig() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_parseConfig(t *testing.T) {
	type args struct {
		result *Config
		list   *ast.ObjectList
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := parseConfig(tt.args.result, tt.args.list); (err != nil) != tt.wantErr {
				t.Errorf("parseConfig() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_parsePorts(t *testing.T) {
	type args struct {
		result **Ports
		list   *ast.ObjectList
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := parsePorts(tt.args.result, tt.args.list); (err != nil) != tt.wantErr {
				t.Errorf("parsePorts() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_parseAddresses(t *testing.T) {
	type args struct {
		result **Addresses
		list   *ast.ObjectList
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := parseAddresses(tt.args.result, tt.args.list); (err != nil) != tt.wantErr {
				t.Errorf("parseAddresses() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_parseAdvertise(t *testing.T) {
	type args struct {
		result **AdvertiseAddrs
		list   *ast.ObjectList
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := parseAdvertise(tt.args.result, tt.args.list); (err != nil) != tt.wantErr {
				t.Errorf("parseAdvertise() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_parseClient(t *testing.T) {
	type args struct {
		result **ClientConfig
		list   *ast.ObjectList
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := parseClient(tt.args.result, tt.args.list); (err != nil) != tt.wantErr {
				t.Errorf("parseClient() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_parseServer(t *testing.T) {
	type args struct {
		result **ServerConfig
		list   *ast.ObjectList
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := parseServer(tt.args.result, tt.args.list); (err != nil) != tt.wantErr {
				t.Errorf("parseServer() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_parseMetric(t *testing.T) {
	type args struct {
		result **Metric
		list   *ast.ObjectList
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := parseMetric(tt.args.result, tt.args.list); (err != nil) != tt.wantErr {
				t.Errorf("parseMetric() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_parseNetwork(t *testing.T) {
	type args struct {
		result **Network
		list   *ast.ObjectList
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := parseNetwork(tt.args.result, tt.args.list); (err != nil) != tt.wantErr {
				t.Errorf("parseNetwork() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_parseConsulConfig(t *testing.T) {
	type args struct {
		result **config.ConsulConfig
		list   *ast.ObjectList
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := parseConsulConfig(tt.args.result, tt.args.list); (err != nil) != tt.wantErr {
				t.Errorf("parseConsulConfig() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_checkHCLKeys(t *testing.T) {
	type args struct {
		node  ast.Node
		valid []string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := checkHCLKeys(tt.args.node, tt.args.valid); (err != nil) != tt.wantErr {
				t.Errorf("checkHCLKeys() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
