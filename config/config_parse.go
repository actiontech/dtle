package config

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/hashicorp/go-multierror"
	"github.com/hashicorp/hcl"
	"github.com/hashicorp/hcl/hcl/ast"
	"github.com/mitchellh/mapstructure"
)

// ParseConfigFile parses the given path as a config file.
func ParseConfigFile(path string) (*Config, error) {
	path, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	config, err := ParseConfig(f)
	if err != nil {
		return nil, err
	}

	return config, nil
}

// ParseConfig parses the config from the given io.Reader.
//
// Due to current internal limitations, the entire contents of the
// io.Reader will be copied into memory first before parsing.
func ParseConfig(r io.Reader) (*Config, error) {
	// Copy the reader into an in-memory buffer first since HCL requires it.
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, r); err != nil {
		return nil, err
	}

	// Parse the buffer
	root, err := hcl.Parse(buf.String())
	if err != nil {
		return nil, fmt.Errorf("error parsing: %s", err)
	}
	buf.Reset()

	// Top-level item should be a list
	list, ok := root.Node.(*ast.ObjectList)
	if !ok {
		return nil, fmt.Errorf("error parsing: root should be an object")
	}

	var config Config
	if err := parseConfig(&config, list); err != nil {
		return nil, fmt.Errorf("error parsing 'config': %v", err)
	}

	return &config, nil
}

func parseConfig(result *Config, list *ast.ObjectList) error {
	// Check for invalid keys
	valid := []string{
		"log_level",
		"log_file",
		"log_rotate",
		"region",
		"datacenter",
		"interface",
		"bind_addr",
		"http_addr",
		"rpc_port",
		"start_join",
		"server",
		"consul",
		"pid_file",
	}
	if err := checkHCLKeys(list, valid); err != nil {
		return multierror.Prefix(err, "config:")
	}

	// Decode the full thing into a map[string]interface for ease
	var m map[string]interface{}
	if err := hcl.DecodeObject(&m, list); err != nil {
		return err
	}

	delete(m, "consul")

	// Decode the rest
	if err := mapstructure.WeakDecode(m, result); err != nil {
		return err
	}

	// Parse the consul config
	if o := list.Filter("consul"); len(o.Items) > 0 {
		if err := parseConsulConfig(&result.Consul, o); err != nil {
			return multierror.Prefix(err, "consul ->")
		}
	}

	return nil
}

func parseConsulConfig(result **ConsulConfig, list *ast.ObjectList) error {
	list = list.Elem()
	if len(list.Items) > 1 {
		return fmt.Errorf("only one 'consul' block allowed")
	}

	// Get our Consul object
	listVal := list.Items[0].Val

	// Check for invalid keys
	valid := []string{
		"addrs",
		"client_auto_join",
		"server_auto_join",
	}

	if err := checkHCLKeys(listVal, valid); err != nil {
		return err
	}

	var m map[string]interface{}
	if err := hcl.DecodeObject(&m, listVal); err != nil {
		return err
	}

	var consulConfig ConsulConfig
	dec, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		DecodeHook:       mapstructure.StringToTimeDurationHookFunc(),
		WeaklyTypedInput: true,
		Result:           &consulConfig,
	})
	if err != nil {
		return err
	}
	if err := dec.Decode(m); err != nil {
		return err
	}

	*result = &consulConfig
	return nil
}

func checkHCLKeys(node ast.Node, valid []string) error {
	var list *ast.ObjectList
	switch n := node.(type) {
	case *ast.ObjectList:
		list = n
	case *ast.ObjectType:
		list = n.List
	default:
		return fmt.Errorf("cannot check HCL keys of type %T", n)
	}

	validMap := make(map[string]struct{}, len(valid))
	for _, v := range valid {
		validMap[v] = struct{}{}
	}

	var result error
	for _, item := range list.Items {
		key := item.Keys[0].Token.Value().(string)
		if _, ok := validMap[key]; !ok {
			result = multierror.Append(result, fmt.Errorf(
				"invalid key: %s", key))
		}
	}

	return result
}
