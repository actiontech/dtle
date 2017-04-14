package plugins

import (
	"fmt"

	uconf "udup/config"
	umysql "udup/internal/client/plugins/mysql"
)

type MySQLDriver struct {
	DriverContext
}

func NewMySQLDriver(ctx *DriverContext) Driver {
	return &MySQLDriver{DriverContext: *ctx}
}

func (d *MySQLDriver) Start(subject string, t string, driverCfg *uconf.DriverConfig) error {
	switch t {
	case DataSrc:
		{
			// Create the extractor
			e := umysql.NewExtractor(driverCfg, subject)
			d.Extractor = e

			if err := e.InitiateExtractor(); err != nil {
				return err
			}
		}
	case DataDest:
		{
			a := umysql.NewApplier(driverCfg, subject)
			d.Applier = a
			if err := a.InitiateApplier(); err != nil {
				return err
			}
		}
	default:
		{
			return fmt.Errorf("unknown processor type : %+v", t)
		}
	}
	return nil
}

func (d *MySQLDriver) Stop(t string) error {
	switch t {
	case DataSrc:
		{
			// Create the extractor
			if err := d.Extractor.Shutdown(); err != nil {
				return err
			}
		}
	case DataDest:
		{
			if err := d.Applier.Shutdown(); err != nil {
				return err
			}
		}
	default:
		{
			return fmt.Errorf("unknown processor type : %+v", t)
		}
	}
	return nil
}

func (d *MySQLDriver) Validate(config map[string]interface{}) error {
	return nil
}
