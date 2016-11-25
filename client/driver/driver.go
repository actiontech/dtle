package driver

import (
	"fmt"
	"log"

	"udup/client/allocdir"
	"udup/client/config"
	"udup/client/fingerprint"
	"udup/server/structs"
)

// BuiltinDrivers contains the built in registered drivers
// which are available for allocation handling
var BuiltinDrivers = map[string]Factory{
	"mysql": NewMySQLDriver,
}

// NewDriver is used to instantiate and return a new driver
// given the name and a logger
func NewDriver(name string, ctx *DriverContext) (Driver, error) {
	// Lookup the factory function
	factory, ok := BuiltinDrivers[name]
	if !ok {
		return nil, fmt.Errorf("unknown driver '%s'", name)
	}

	// Instantiate the driver
	f := factory(ctx)
	return f, nil
}

// Factory is used to instantiate a new Driver
type Factory func(*DriverContext) Driver

// Driver is used for execution of tasks. This allows Udup
// to support many pluggable implementations of task drivers.
// Examples could include LXC, Docker, Qemu, etc.
type Driver interface {
	// Drivers must support the fingerprint interface for detection
	fingerprint.Fingerprint

	// Start is used to being task execution
	Repl(logger *log.Logger, ctx *ExecContext, task *structs.Task) error

	Migrate(logger *log.Logger, ctx *ExecContext, task *structs.Task) error

	Sub(logger *log.Logger, ctx *ExecContext, task *structs.Task) error
}

// DriverContext is a means to inject dependencies such as loggers, configs, and
// node attributes into a Driver without having to change the Driver interface
// each time we do it. Used in conjection with Factory, above.
type DriverContext struct {
	taskName string
	config   *config.Config
	logger   *log.Logger
	node     *structs.Node
}

// NewEmptyDriverContext returns a DriverContext with all fields set to their
// zero value.
func NewEmptyDriverContext() *DriverContext {
	return &DriverContext{
		taskName: "",
		config:   nil,
		node:     nil,
		logger:   nil,
	}
}

// NewDriverContext initializes a new DriverContext with the specified fields.
// This enables other packages to create DriverContexts but keeps the fields
// private to the driver. If we want to change this later we can gorename all of
// the fields in DriverContext.
func NewDriverContext(taskName string, config *config.Config, node *structs.Node,
	logger *log.Logger) *DriverContext {
	return &DriverContext{
		taskName: taskName,
		config:   config,
		node:     node,
		logger:   logger,
	}
}

// ExecContext is shared between drivers within an allocation
type ExecContext struct {
	// AllocDir contains information about the alloc directory structure.
	AllocDir *allocdir.AllocDir

	// Alloc ID
	AllocID string
}

// NewExecContext is used to create a new execution context
func NewExecContext(alloc *allocdir.AllocDir, allocID string) *ExecContext {
	return &ExecContext{AllocDir: alloc, AllocID: allocID}
}

func mapMergeStrInt(maps ...map[string]int) map[string]int {
	out := map[string]int{}
	for _, in := range maps {
		for key, val := range in {
			out[key] = val
		}
	}
	return out
}

func mapMergeStrStr(maps ...map[string]string) map[string]string {
	out := map[string]string{}
	for _, in := range maps {
		for key, val := range in {
			out[key] = val
		}
	}
	return out
}
