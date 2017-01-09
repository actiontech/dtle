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
		"nats_addr",
		"worker_count",
		"batch",
		"replicate_do_db",
		"replicate_do_table",
		"extract",
		"apply",
	}
	if err := checkHCLKeys(list, valid); err != nil {
		return multierror.Prefix(err, "config:")
	}

	// Decode the full thing into a map[string]interface for ease
	var m map[string]interface{}
	if err := hcl.DecodeObject(&m, list); err != nil {
		return err
	}

	delete(m, "extract")
	delete(m, "apply")

	// Decode the rest
	if err := mapstructure.WeakDecode(m, result); err != nil {
		return err
	}

	// Parse Extract config
	if o := list.Filter("extract"); len(o.Items) > 0 {
		if err := parseExtractor(&result.Extract, o); err != nil {
			return multierror.Prefix(err, "extract ->")
		}
	}

	// Parse Apply config
	if o := list.Filter("apply"); len(o.Items) > 0 {
		if err := parseApplier(&result.Apply, o); err != nil {
			return multierror.Prefix(err, "apply ->")
		}
	}

	return nil
}

func parseExtractor(result **ExtractorConfig, list *ast.ObjectList) error {
	list = list.Elem()
	if len(list.Items) > 1 {
		return fmt.Errorf("only one 'client' block allowed")
	}

	// Get our client object
	obj := list.Items[0]

	// Value should be an object
	var listVal *ast.ObjectList
	if ot, ok := obj.Val.(*ast.ObjectType); ok {
		listVal = ot.List
	} else {
		return fmt.Errorf("client value: should be an object")
	}

	// Check for invalid keys
	valid := []string{
		"enabled",
		"server_id",
		"conn_cfg",
	}
	if err := checkHCLKeys(listVal, valid); err != nil {
		return err
	}

	var m map[string]interface{}
	if err := hcl.DecodeObject(&m, listVal); err != nil {
		return err
	}

	delete(m, "conn_cfg")

	var config ExtractorConfig
	if err := mapstructure.WeakDecode(m, &config); err != nil {
		return err
	}

	if oo := listVal.Filter("conn_cfg"); len(oo.Items) > 0 {
		if err := parseConnCfg(&config.ConnCfg, oo); err != nil {
			return multierror.Prefix(err, "conn_cfg: ")
		}
	}

	*result = &config
	return nil
}

func parseApplier(result **ApplierConfig, list *ast.ObjectList) error {
	list = list.Elem()
	if len(list.Items) > 1 {
		return fmt.Errorf("only one 'server' block allowed")
	}

	// Get our server object
	obj := list.Items[0]

	// Value should be an object
	var listVal *ast.ObjectList
	if ot, ok := obj.Val.(*ast.ObjectType); ok {
		listVal = ot.List
	} else {
		return fmt.Errorf("client value: should be an object")
	}

	// Check for invalid keys
	valid := []string{
		"enabled",
		"conn_cfg",
	}
	if err := checkHCLKeys(listVal, valid); err != nil {
		return err
	}

	var m map[string]interface{}
	if err := hcl.DecodeObject(&m, listVal); err != nil {
		return err
	}

	delete(m, "conn_cfg")

	var config ApplierConfig
	if err := mapstructure.WeakDecode(m, &config); err != nil {
		return err
	}

	if oo := listVal.Filter("conn_cfg"); len(oo.Items) > 0 {
		if err := parseConnCfg(&config.ConnCfg, oo); err != nil {
			return multierror.Prefix(err, "conn_cfg: ")
		}
	}

	*result = &config
	return nil
}

func parseConnCfg(result **ConnectionConfig, list *ast.ObjectList) error {
	list = list.Elem()
	if len(list.Items) > 1 {
		return fmt.Errorf("only one 'cfg' block allowed per ConnectionConfig")
	}

	// Get our resource object
	o := list.Items[0]

	var m map[string]interface{}
	if err := hcl.DecodeObject(&m, o.Val); err != nil {
		return err
	}

	if err := mapstructure.WeakDecode(m, &result); err != nil {
		return err
	}

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
