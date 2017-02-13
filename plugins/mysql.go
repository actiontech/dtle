package plugins

import "github.com/ngaut/log"

type MySQLDriver struct {
	PluginContext
}

func NewMySQLDriver(ctx *PluginContext) Plugin {
	return &MySQLDriver{PluginContext: *ctx}
}

func (d *MySQLDriver) Start() error {
	log.Infof("------start mysql driver")
	return nil
}

func (d *MySQLDriver) Validate(config map[string]interface{}) error {
	return nil
}
