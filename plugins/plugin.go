package plugins

import (
	"fmt"

	uconf "udup/config"
)

const (
	DriverTypeMySQL = "mysql"
)

var BuiltinProcessors = map[string]Factory{
	"mysql": NewMySQLDriver,
}

type Factory func(ctx *PluginContext) Plugin

func DiscoverPlugins(name string, ctx *PluginContext) (Plugin, error) {
	// Lookup the factory function
	factory, ok := BuiltinProcessors[name]
	if !ok {
		return nil, fmt.Errorf("unknown driver '%s'", name)
	}

	// Instantiate the driver
	f := factory(ctx)
	return f, nil
}

type Plugin interface {
	// Start is used to being task execution
	Start() error

	// Plugins must validate their configuration
	Validate(map[string]interface{}) error
}

type PluginContext struct {
	taskName string
	config   *uconf.Config
}

func NewPluginContext(taskName string, config *uconf.Config) *PluginContext {
	return &PluginContext{
		taskName: taskName,
		config:   config,
	}
}
