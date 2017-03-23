package plugins

import (
	"fmt"

	uconf "udup/config"
	umysql "udup/plugins/mysql"
)

const (
	MysqlDriverAttr = "mysql"
)

const (
	DataSrc = "src"
	DataDest   = "dest"
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
	Start(jobName string, t string, driverCfg *uconf.DriverConfig) error

	Stop(t string) error

	// Plugins must validate their configuration
	Validate(map[string]interface{}) error
}

type DriverContext struct {
	taskName  string
	config    *uconf.DriverConfig
	Extractor *umysql.Extractor
	Applier   *umysql.Applier
}

func NewDriverContext(taskName string, config *uconf.DriverConfig) *DriverContext {
	return &DriverContext{
		taskName: taskName,
		config:   config,
	}
}
