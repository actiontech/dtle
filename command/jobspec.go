package command

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/hashicorp/go-multierror"
	"github.com/hashicorp/hcl"
	"github.com/hashicorp/hcl/hcl/ast"
	"github.com/mitchellh/mapstructure"
	"udup/client/driver"
	"udup/server/structs"
)

var reDynamicPorts = regexp.MustCompile("^[a-zA-Z0-9_]+$")
var errPortLabel = fmt.Errorf("Port label does not conform to naming requirements %s", reDynamicPorts.String())

// Parse parses the job spec from the given io.Reader.
//
// Due to current internal limitations, the entire contents of the
// io.Reader will be copied into memory first before parsing.
func Parse(r io.Reader) (*structs.Job, error) {
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

	var job structs.Job

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
func ParseFile(path string) (*structs.Job, error) {
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

func parseJob(result *structs.Job, list *ast.ObjectList) error {
	list = list.Children()
	if len(list.Items) != 1 {
		return fmt.Errorf("only one 'job' block allowed")
	}

	// Get our job object
	obj := list.Items[0]

	// Decode the full thing into a map[string]interface for ease
	var m map[string]interface{}
	if err := hcl.DecodeObject(&m, obj.Val); err != nil {
		return err
	}
	delete(m, "meta")
	delete(m, "update")
	delete(m, "periodic")

	// Set the ID and name to the object key
	result.ID = obj.Keys[0].Token.Value().(string)
	result.Name = result.ID

	// Defaults
	result.Priority = 50
	result.Region = "global"
	result.Type = "service"

	// Decode the rest
	if err := mapstructure.WeakDecode(m, result); err != nil {
		return err
	}

	// Value should be an object
	var listVal *ast.ObjectList
	if ot, ok := obj.Val.(*ast.ObjectType); ok {
		listVal = ot.List
	} else {
		return fmt.Errorf("job '%s' value: should be an object", result.ID)
	}

	// Check for invalid keys
	valid := []string{
		"id",
		"name",
		"region",
		"all_at_once",
		"type",
		"priority",
		"datacenters",
		"update",
		"periodic",
		"meta",
		"task",
		"group",
	}
	if err := checkHCLKeys(listVal, valid); err != nil {
		return multierror.Prefix(err, "job:")
	}

	// If we have an update strategy, then parse that
	if o := listVal.Filter("update"); len(o.Items) > 0 {
		if err := parseUpdate(&result.Update, o); err != nil {
			return multierror.Prefix(err, "update ->")
		}
	}

	// If we have a periodic definition, then parse that
	if o := listVal.Filter("periodic"); len(o.Items) > 0 {
		if err := parsePeriodic(&result.Periodic, o); err != nil {
			return multierror.Prefix(err, "periodic ->")
		}
	}

	// Parse out meta fields. These are in HCL as a list so we need
	// to iterate over them and merge them.
	if metaO := listVal.Filter("meta"); len(metaO.Items) > 0 {
		for _, o := range metaO.Elem().Items {
			var m map[string]interface{}
			if err := hcl.DecodeObject(&m, o.Val); err != nil {
				return err
			}
			if err := mapstructure.WeakDecode(m, &result.Meta); err != nil {
				return err
			}
		}
	}

	// If we have tasks outside, create TaskGroups for them
	if o := listVal.Filter("task"); len(o.Items) > 0 {
		var tasks []*structs.Task
		if err := parseTasks(result.Name, "", &tasks, o); err != nil {
			return multierror.Prefix(err, "task:")
		}

		result.TaskGroups = make([]*structs.TaskGroup, len(tasks), len(tasks)*2)
		for i, t := range tasks {
			result.TaskGroups[i] = &structs.TaskGroup{
				Name:          t.Name,
				EphemeralDisk: structs.DefaultEphemeralDisk(),
				Tasks:         []*structs.Task{t},
			}
		}
	}

	// Parse the task groups
	if o := listVal.Filter("group"); len(o.Items) > 0 {
		if err := parseGroups(result, o); err != nil {
			return multierror.Prefix(err, "group:")
		}
	}

	return nil
}

func parseGroups(result *structs.Job, list *ast.ObjectList) error {
	list = list.Children()
	if len(list.Items) == 0 {
		return nil
	}

	// Go through each object and turn it into an actual result.
	collection := make([]*structs.TaskGroup, 0, len(list.Items))
	seen := make(map[string]struct{})
	for _, item := range list.Items {
		n := item.Keys[0].Token.Value().(string)

		// Make sure we haven't already found this
		if _, ok := seen[n]; ok {
			return fmt.Errorf("group '%s' defined more than once", n)
		}
		seen[n] = struct{}{}

		// We need this later
		var listVal *ast.ObjectList
		if ot, ok := item.Val.(*ast.ObjectType); ok {
			listVal = ot.List
		} else {
			return fmt.Errorf("group '%s': should be an object", n)
		}

		// Check for invalid keys
		valid := []string{
			"restart",
			"meta",
			"task",
			"ephemeral_disk",
		}
		if err := checkHCLKeys(listVal, valid); err != nil {
			return multierror.Prefix(err, fmt.Sprintf("'%s' ->", n))
		}

		var m map[string]interface{}
		if err := hcl.DecodeObject(&m, item.Val); err != nil {
			return err
		}
		delete(m, "meta")
		delete(m, "task")
		delete(m, "restart")
		delete(m, "ephemeral_disk")

		// Build the group with the basic decode
		var g structs.TaskGroup
		g.Name = n
		if err := mapstructure.WeakDecode(m, &g); err != nil {
			return err
		}

		// Parse restart policy
		if o := listVal.Filter("restart"); len(o.Items) > 0 {
			if err := parseRestartPolicy(&g.RestartPolicy, o); err != nil {
				return multierror.Prefix(err, fmt.Sprintf("'%s', restart ->", n))
			}
		}

		// Parse ephemeral disk
		g.EphemeralDisk = structs.DefaultEphemeralDisk()
		if o := listVal.Filter("ephemeral_disk"); len(o.Items) > 0 {
			if err := parseEphemeralDisk(&g.EphemeralDisk, o); err != nil {
				return multierror.Prefix(err, fmt.Sprintf("'%s', ephemeral_disk ->", n))
			}
		}

		// Parse out meta fields. These are in HCL as a list so we need
		// to iterate over them and merge them.
		if metaO := listVal.Filter("meta"); len(metaO.Items) > 0 {
			for _, o := range metaO.Elem().Items {
				var m map[string]interface{}
				if err := hcl.DecodeObject(&m, o.Val); err != nil {
					return err
				}
				if err := mapstructure.WeakDecode(m, &g.Meta); err != nil {
					return err
				}
			}
		}

		// Parse tasks
		if o := listVal.Filter("task"); len(o.Items) > 0 {
			if err := parseTasks(result.Name, g.Name, &g.Tasks, o); err != nil {
				return multierror.Prefix(err, fmt.Sprintf("'%s', task:", n))
			}
		}

		collection = append(collection, &g)
	}

	result.TaskGroups = append(result.TaskGroups, collection...)
	return nil
}

func parseRestartPolicy(final **structs.RestartPolicy, list *ast.ObjectList) error {
	list = list.Elem()
	if len(list.Items) > 1 {
		return fmt.Errorf("only one 'restart' block allowed")
	}

	// Get our job object
	obj := list.Items[0]

	// Check for invalid keys
	valid := []string{
		"attempts",
		"interval",
		"delay",
		"mode",
	}
	if err := checkHCLKeys(obj.Val, valid); err != nil {
		return err
	}

	var m map[string]interface{}
	if err := hcl.DecodeObject(&m, obj.Val); err != nil {
		return err
	}

	var result structs.RestartPolicy
	dec, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		DecodeHook:       mapstructure.StringToTimeDurationHookFunc(),
		WeaklyTypedInput: true,
		Result:           &result,
	})
	if err != nil {
		return err
	}
	if err := dec.Decode(m); err != nil {
		return err
	}

	*final = &result
	return nil
}

func parseEphemeralDisk(result **structs.EphemeralDisk, list *ast.ObjectList) error {
	list = list.Elem()
	if len(list.Items) > 1 {
		return fmt.Errorf("only one 'ephemeral_disk' block allowed")
	}

	// Get our ephemeral_disk object
	obj := list.Items[0]

	// Check for invalid keys
	valid := []string{
		"sticky",
		"size",
	}
	if err := checkHCLKeys(obj.Val, valid); err != nil {
		return err
	}

	var m map[string]interface{}
	if err := hcl.DecodeObject(&m, obj.Val); err != nil {
		return err
	}

	var ephemeralDisk structs.EphemeralDisk
	if err := mapstructure.WeakDecode(m, &ephemeralDisk); err != nil {
		return err
	}
	*result = &ephemeralDisk

	return nil
}

// parseBool takes an interface value and tries to convert it to a boolean and
// returns an error if the type can't be converted.
func parseBool(value interface{}) (bool, error) {
	var enabled bool
	var err error
	switch value.(type) {
	case string:
		enabled, err = strconv.ParseBool(value.(string))
	case bool:
		enabled = value.(bool)
	default:
		err = fmt.Errorf("%v couldn't be converted to boolean value", value)
	}

	return enabled, err
}

func parseTasks(jobName string, taskGroupName string, result *[]*structs.Task, list *ast.ObjectList) error {
	list = list.Children()
	if len(list.Items) == 0 {
		return nil
	}

	// Go through each object and turn it into an actual result.
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
			return fmt.Errorf("group '%s': should be an object", n)
		}

		// Check for invalid keys
		valid := []string{
			"config",
			"driver",
			"kill_timeout",
			"logs",
			"meta",
			"resources",
			"template",
		}
		if err := checkHCLKeys(listVal, valid); err != nil {
			return multierror.Prefix(err, fmt.Sprintf("'%s' ->", n))
		}

		var m map[string]interface{}
		if err := hcl.DecodeObject(&m, item.Val); err != nil {
			return err
		}
		delete(m, "config")
		delete(m, "logs")
		delete(m, "meta")
		delete(m, "resources")
		delete(m, "template")

		// Build the task
		var t structs.Task
		t.Name = n
		if taskGroupName == "" {
			taskGroupName = n
		}
		dec, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
			DecodeHook:       mapstructure.StringToTimeDurationHookFunc(),
			WeaklyTypedInput: true,
			Result:           &t,
		})
		if err != nil {
			return err
		}
		if err := dec.Decode(m); err != nil {
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

			// Instantiate a driver to validate the configuration
			_, err := driver.NewDriver(
				t.Driver,
				driver.NewEmptyDriverContext(),
			)

			if err != nil {
				return multierror.Prefix(err,
					fmt.Sprintf("'%s', config ->", n))
			}

			//Validate driver config
		}

		// Parse out meta fields. These are in HCL as a list so we need
		// to iterate over them and merge them.
		if metaO := listVal.Filter("meta"); len(metaO.Items) > 0 {
			for _, o := range metaO.Elem().Items {
				var m map[string]interface{}
				if err := hcl.DecodeObject(&m, o.Val); err != nil {
					return err
				}
				if err := mapstructure.WeakDecode(m, &t.Meta); err != nil {
					return err
				}
			}
		}

		// If we have resources, then parse that
		if o := listVal.Filter("resources"); len(o.Items) > 0 {
			var r structs.Resources
			if err := parseResources(&r, o); err != nil {
				return multierror.Prefix(err, fmt.Sprintf("'%s',", n))
			}

			t.Resources = &r
		}

		// If we have logs then parse that
		logConfig := structs.DefaultLogConfig()
		if o := listVal.Filter("logs"); len(o.Items) > 0 {
			if len(o.Items) > 1 {
				return fmt.Errorf("only one logs block is allowed in a Task. Number of logs block found: %d", len(o.Items))
			}
			var m map[string]interface{}
			logsBlock := o.Items[0]

			// Check for invalid keys
			valid := []string{
				"max_files",
				"max_file_size",
			}
			if err := checkHCLKeys(logsBlock.Val, valid); err != nil {
				return multierror.Prefix(err, fmt.Sprintf("'%s', logs ->", n))
			}

			if err := hcl.DecodeObject(&m, logsBlock.Val); err != nil {
				return err
			}

			if err := mapstructure.WeakDecode(m, &logConfig); err != nil {
				return err
			}
		}
		t.LogConfig = logConfig

		*result = append(*result, &t)
	}

	return nil
}

func parseResources(result *structs.Resources, list *ast.ObjectList) error {
	list = list.Elem()
	if len(list.Items) == 0 {
		return nil
	}
	if len(list.Items) > 1 {
		return fmt.Errorf("only one 'resource' block allowed per task")
	}

	// Get our resource object
	o := list.Items[0]

	// We need this later
	var listVal *ast.ObjectList
	if ot, ok := o.Val.(*ast.ObjectType); ok {
		listVal = ot.List
	} else {
		return fmt.Errorf("resource: should be an object")
	}

	// Check for invalid keys
	valid := []string{
		"cpu",
		"iops",
		"memory",
		"network",
	}
	if err := checkHCLKeys(listVal, valid); err != nil {
		return multierror.Prefix(err, "resources ->")
	}

	var m map[string]interface{}
	if err := hcl.DecodeObject(&m, o.Val); err != nil {
		return err
	}
	delete(m, "network")

	if err := mapstructure.WeakDecode(m, result); err != nil {
		return err
	}

	// Parse the network resources
	if o := listVal.Filter("network"); len(o.Items) > 0 {
		if len(o.Items) > 1 {
			return fmt.Errorf("only one 'network' resource allowed")
		}

		// Check for invalid keys
		valid := []string{
			"mbits",
			"port",
		}
		if err := checkHCLKeys(o.Items[0].Val, valid); err != nil {
			return multierror.Prefix(err, "resources, network ->")
		}

		var r structs.NetworkResource
		var m map[string]interface{}
		if err := hcl.DecodeObject(&m, o.Items[0].Val); err != nil {
			return err
		}
		if err := mapstructure.WeakDecode(m, &r); err != nil {
			return err
		}

		var networkObj *ast.ObjectList
		if ot, ok := o.Items[0].Val.(*ast.ObjectType); ok {
			networkObj = ot.List
		} else {
			return fmt.Errorf("resource: should be an object")
		}
		if err := parsePorts(networkObj, &r); err != nil {
			return multierror.Prefix(err, "resources, network, ports ->")
		}

		result.Networks = []*structs.NetworkResource{&r}
	}

	// Combine the parsed resources with a default resource block.
	min := structs.DefaultResources()
	min.Merge(result)
	*result = *min
	return nil
}

func parsePorts(networkObj *ast.ObjectList, nw *structs.NetworkResource) error {
	// Check for invalid keys
	valid := []string{
		"mbits",
		"port",
	}
	if err := checkHCLKeys(networkObj, valid); err != nil {
		return err
	}

	portsObjList := networkObj.Filter("port")
	knownPortLabels := make(map[string]bool)
	for _, port := range portsObjList.Items {
		if len(port.Keys) == 0 {
			return fmt.Errorf("ports must be named")
		}
		label := port.Keys[0].Token.Value().(string)
		if !reDynamicPorts.MatchString(label) {
			return errPortLabel
		}
		l := strings.ToLower(label)
		if knownPortLabels[l] {
			return fmt.Errorf("found a port label collision: %s", label)
		}
		var p map[string]interface{}
		var res structs.Port
		if err := hcl.DecodeObject(&p, port.Val); err != nil {
			return err
		}
		if err := mapstructure.WeakDecode(p, &res); err != nil {
			return err
		}
		res.Label = label
		if res.Value > 0 {
			nw.ReservedPorts = append(nw.ReservedPorts, res)
		} else {
			nw.DynamicPorts = append(nw.DynamicPorts, res)
		}
		knownPortLabels[l] = true
	}
	return nil
}

func parseUpdate(result *structs.UpdateStrategy, list *ast.ObjectList) error {
	list = list.Elem()
	if len(list.Items) > 1 {
		return fmt.Errorf("only one 'update' block allowed per job")
	}

	// Get our resource object
	o := list.Items[0]

	var m map[string]interface{}
	if err := hcl.DecodeObject(&m, o.Val); err != nil {
		return err
	}

	// Check for invalid keys
	valid := []string{
		"stagger",
		"max_parallel",
	}
	if err := checkHCLKeys(o.Val, valid); err != nil {
		return err
	}

	dec, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		DecodeHook:       mapstructure.StringToTimeDurationHookFunc(),
		WeaklyTypedInput: true,
		Result:           result,
	})
	if err != nil {
		return err
	}
	return dec.Decode(m)
}

func parsePeriodic(result **structs.PeriodicConfig, list *ast.ObjectList) error {
	list = list.Elem()
	if len(list.Items) > 1 {
		return fmt.Errorf("only one 'periodic' block allowed per job")
	}

	// Get our resource object
	o := list.Items[0]

	var m map[string]interface{}
	if err := hcl.DecodeObject(&m, o.Val); err != nil {
		return err
	}

	// Check for invalid keys
	valid := []string{
		"enabled",
		"cron",
		"prohibit_overlap",
	}
	if err := checkHCLKeys(o.Val, valid); err != nil {
		return err
	}

	// Enabled by default if the periodic block exists.
	if value, ok := m["enabled"]; !ok {
		m["Enabled"] = true
	} else {
		enabled, err := parseBool(value)
		if err != nil {
			return fmt.Errorf("periodic.enabled should be set to true or false; %v", err)
		}
		m["Enabled"] = enabled
	}

	// If "cron" is provided, set the type to "cron" and store the spec.
	if cron, ok := m["cron"]; ok {
		m["SpecType"] = structs.PeriodicSpecCron
		m["Spec"] = cron
	}

	// Build the constraint
	var p structs.PeriodicConfig
	if err := mapstructure.WeakDecode(m, &p); err != nil {
		return err
	}
	*result = &p
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
