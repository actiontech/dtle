package plugins

import (
	"fmt"

	uconf "udup/config"
	umysql "udup/plugins/mysql"
)

type MySQLDriver struct {
	DriverContext
}

func NewMySQLDriver(ctx *DriverContext) Driver {
	return &MySQLDriver{DriverContext: *ctx}
}

func (d *MySQLDriver) Start(jobName string,t string, driverCfg *uconf.DriverConfig) error {
	switch t {
	case ProcessorTypeExtract:
		{
			// Create the extractor
			e := umysql.NewExtractor(driverCfg)
			d.Extractor = e

			if err := e.InitiateExtractor(jobName); err != nil {
				return fmt.Errorf("[Extractor]:%+v", err)
			}
		}
	case ProcessorTypeApply:
		{
			a := umysql.NewApplier(driverCfg)
			d.Applier = a
			if err := a.InitiateApplier(jobName); err != nil {
				return fmt.Errorf("[Applier]:%+v", err)
			}
		}
	default:
		{
			return fmt.Errorf("Unknown job type : %+v", t)
		}
	}
	return nil
}

func (d *MySQLDriver) Stop(t string) error {
	switch t {
	case ProcessorTypeExtract:
		{
			// Create the extractor
			if err := d.Extractor.Shutdown(); err != nil {
				return err
			}
		}
	case ProcessorTypeApply:
		{
			if err := d.Applier.Shutdown(); err != nil {
				return err
			}
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
