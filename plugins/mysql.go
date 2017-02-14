package plugins

import (
	"fmt"
	"github.com/ngaut/log"

	uconf "udup/config"
)

type MySQLDriver struct {
	DriverContext
}

func NewMySQLDriver(ctx *DriverContext) Driver {
	return &MySQLDriver{DriverContext: *ctx}
}

func (d *MySQLDriver) Start(t string, driverCtx uconf.DriverConfig) error {
	switch t {
	case ProcessorTypeExtract:
		{
			log.Infof("start mysql extract")
		}
	case ProcessorTypeApply:
		{
			log.Infof("start mysql apply")
		}
	default:
		{
			return fmt.Errorf("Unknown job type : %+v", t)
		}
	}
	return nil
}

func (d *MySQLDriver) Validate(config map[string]interface{}) error {
	return nil
}
