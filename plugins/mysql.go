package plugins

import (
	"fmt"
	"github.com/ngaut/log"

	uconf "udup/config"
	umysql "udup/plugins/mysql"
)

type MySQLDriver struct {
	DriverContext
}

func NewMySQLDriver(ctx *DriverContext) Driver {
	return &MySQLDriver{DriverContext: *ctx}
}

func (d *MySQLDriver) Start(t string, driverCtx *uconf.DriverConfig) error {
	switch t {
	case ProcessorTypeExtract:
		{
			log.Infof("start mysql extract")
			// Create the extractor
			extractor := umysql.NewExtractor(driverCtx)
			go func() {
				if err := extractor.InitiateExtractor(); err != nil {
					log.Errorf("extractor run failed: %v", err)
					driverCtx.ErrCh <- err
				}
			}()
		}
	case ProcessorTypeApply:
		{
			log.Infof("start mysql apply")
			applier := umysql.NewApplier(driverCtx)
			go func() {
				if err := applier.InitiateApplier(); err != nil {
					log.Errorf("applier run failed: %v", err)
					driverCtx.ErrCh <- err
				}
			}()
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
