package plugins

import (
	"udup/agent"
	"fmt"
)

var BuiltinProcessors = map[string]Factory{
	"mysql":   NewMySQLDriver,
}

type Factory func(ctx *PluginContext) Plugin

type Plugins struct {
	Processors map[string]agent.Processor
}

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
	config   *agent.PluginConfig
}

func NewPluginContext(taskName string, config *agent.PluginConfig) *PluginContext {
	return &PluginContext{
		taskName: taskName,
		config:   config,
	}
}