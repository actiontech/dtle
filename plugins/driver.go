package plugins

import (
	"fmt"

	uconf "udup/config"
)

const (
	MysqlDriverAttr = "mysql"
	ProcessorTypeExtract = "extract"
	ProcessorTypeApply = "apply"
)

var BuiltinProcessors = map[string]Factory{
	"mysql": NewMySQLDriver,
}

type Factory func(ctx *DriverContext) Driver

func DiscoverPlugins(name string, ctx *DriverContext) (Driver, error) {
	// Lookup the factory function
	factory, ok := BuiltinProcessors[name]
	if !ok {
		return nil, fmt.Errorf("unknown driver '%s'", name)
	}

	// Instantiate the driver
	f := factory(ctx)
	return f, nil
}

type Driver interface {
	// Start is used to being task execution
	Start(t string,driverCtx *uconf.DriverConfig) error

	// Plugins must validate their configuration
	Validate(map[string]interface{}) error
}

type DriverContext struct {
	taskName string
	config   *uconf.Config
}

func NewDriverContext(taskName string, config *uconf.Config) *DriverContext {
	return &DriverContext{
		taskName: taskName,
		config:   config,
	}
}
