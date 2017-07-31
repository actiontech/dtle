package command

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	gg "github.com/hashicorp/go-getter"
	"github.com/hashicorp/go-multierror"
	"github.com/hashicorp/hcl"
	"github.com/hashicorp/hcl/hcl/ast"
	"github.com/mitchellh/mapstructure"

	"udup/api"
	"udup/internal"
)

type JobGetter struct {
	// The fields below can be overwritten for tests
	testStdin io.Reader
}

// StructJob returns the Job struct from jobfile.
func (j *JobGetter) ApiJob(jpath string) (*api.Job, error) {
	var jobfile io.Reader
	switch jpath {
	case "-":
		if j.testStdin != nil {
			jobfile = j.testStdin
		} else {
			jobfile = os.Stdin
		}
	default:
		if len(jpath) == 0 {
			return nil, fmt.Errorf("Error jobfile path has to be specified.")
		}

		job, err := ioutil.TempFile("", "jobfile")
		if err != nil {
			return nil, err
		}
		defer os.Remove(job.Name())

		if err := job.Close(); err != nil {
			return nil, err
		}

		// Get the pwd
		pwd, err := os.Getwd()
		if err != nil {
			return nil, err
		}

		client := &gg.Client{
			Src: jpath,
			Pwd: pwd,
			Dst: job.Name(),
		}

		if err := client.Get(); err != nil {
			return nil, fmt.Errorf("Error getting jobfile from %q: %v", jpath, err)
		} else {
			file, err := os.Open(job.Name())
			defer file.Close()
			if err != nil {
				return nil, fmt.Errorf("Error opening file %q: %v", jpath, err)
			}
			jobfile = file
		}
	}

	// Parse the JobFile
	jobStruct, err := Parse(jobfile)
	if err != nil {
		return nil, fmt.Errorf("Error parsing job file from %s: %v", jpath, err)
	}

	return jobStruct, nil
}

// Parse parses the job spec from the given io.Reader.
//
// Due to current internal limitations, the entire contents of the
// io.Reader will be copied into memory first before parsing.
func Parse(r io.Reader) (*api.Job, error) {
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

	// Check for invalid keys
	valid := []string{
		"job",
	}
	if err := checkHCLKeys(list, valid); err != nil {
		return nil, err
	}

	var job api.Job

	// Parse the job out
	matches := list.Filter("job")
	if len(matches.Items) == 0 {
		return nil, fmt.Errorf("'job' stanza not found")
	}

	if err := parseJob(&job, matches); err != nil {
		return nil, fmt.Errorf("error parsing 'job': %s", err)
	}

	return &job, nil
}

// ParseFile parses the given path as a job spec.
func ParseFile(path string) (*api.Job, error) {
	path, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	return Parse(f)
}

func parseJob(result *api.Job, list *ast.ObjectList) error {
	if len(list.Items) != 1 {
		return fmt.Errorf("only one 'job' block allowed")
	}
	list = list.Children()
	if len(list.Items) != 1 {
		return fmt.Errorf("'job' block missing name")
	}

	// Get our job object
	obj := list.Items[0]

	// Decode the full thing into a map[string]interface for ease
	var m map[string]interface{}
	if err := hcl.DecodeObject(&m, obj.Val); err != nil {
		return err
	}

	// Set the ID and name to the object key
	result.ID = internal.StringToPtr(obj.Keys[0].Token.Value().(string))
	result.Name = internal.StringToPtr(*result.ID)

	// Decode the rest
	if err := mapstructure.WeakDecode(m, result); err != nil {
		return err
	}

	// Value should be an object
	var listVal *ast.ObjectList
	if ot, ok := obj.Val.(*ast.ObjectType); ok {
		listVal = ot.List
	} else {
		return fmt.Errorf("job '%s' value: should be an object", *result.ID)
	}

	// Check for invalid keys
	valid := []string{
		"region",
		"datacenters",
		"name",
		"task",
		"type",
	}
	if err := checkHCLKeys(listVal, valid); err != nil {
		return multierror.Prefix(err, "job:")
	}

	// Parse the task groups
	if o := listVal.Filter("task"); len(o.Items) > 0 {
		if err := parseTasks(result, o); err != nil {
			return multierror.Prefix(err, "task:")
		}
	}

	return nil
}

func parseTasks(result *api.Job, list *ast.ObjectList) error {
	list = list.Children()
	if len(list.Items) == 0 {
		return nil
	}

	// Go through each object and turn it into an actual result.
	collection := make([]*api.Task, 0, len(list.Items))
	seen := make(map[string]struct{})
	for _, item := range list.Items {
		n := item.Keys[0].Token.Value().(string)

		// Make sure we haven't already found this
		if _, ok := seen[n]; ok {
			return fmt.Errorf("task '%s' defined more than once", n)
		}
		seen[n] = struct{}{}

		// We need this later
		var listVal *ast.ObjectList
		if ot, ok := item.Val.(*ast.ObjectType); ok {
			listVal = ot.List
		} else {
			return fmt.Errorf("task '%s': should be an object", n)
		}

		// Check for invalid keys
		valid := []string{
			"node_id",
			"config",
			"driver",
		}
		if err := checkHCLKeys(listVal, valid); err != nil {
			return multierror.Prefix(err, fmt.Sprintf("'%s' ->", n))
		}

		var m map[string]interface{}
		if err := hcl.DecodeObject(&m, item.Val); err != nil {
			return err
		}
		delete(m, "config")

		// Build the group with the basic decode
		var t api.Task
		t.Type = *internal.StringToPtr(n)
		if err := mapstructure.WeakDecode(m, &t); err != nil {
			return err
		}

		// If we have config, then parse that
		if o := listVal.Filter("config"); len(o.Items) > 0 {
			for _, o := range o.Elem().Items {
				var m map[string]interface{}
				if err := hcl.DecodeObject(&m, o.Val); err != nil {
					return err
				}

				if err := mapstructure.WeakDecode(m, &t.Config); err != nil {
					return err
				}
			}
		}

		collection = append(collection, &t)
	}

	result.Tasks = append(result.Tasks, collection...)
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
