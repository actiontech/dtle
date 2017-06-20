package driver

import (
	"fmt"

	"github.com/mitchellh/mapstructure"

	umysql "udup/internal/client/driver/mysql"
	uconf "udup/internal/config"
	"udup/internal/models"
)

type MySQLDriver struct {
	DriverContext
}

// NewMySQLDriver is used to create a new mysql driver
func NewMySQLDriver(ctx *DriverContext) Driver {
	return &MySQLDriver{DriverContext: *ctx}
}

func (m *MySQLDriver) Start(ctx *ExecContext, task *models.Task) (DriverHandle, error) {
	var driverConfig uconf.MySQLDriverConfig
	if err := mapstructure.WeakDecode(task.Config, &driverConfig); err != nil {
		return nil, err
	}

	switch ctx.Tp {
	case models.TaskTypeSrc:
		{
			// Create the extractor
			e := umysql.NewExtractor(ctx.Subject, &driverConfig, m.logger)
			go e.Run()
			return e, nil
		}
	case models.TaskTypeDest:
		{
			a, err := umysql.NewApplier(ctx.Subject, &driverConfig, m.logger)
			if err != nil {
				return nil, err
			}
			go a.Run()
			return a, nil
		}
	default:
		{
			return nil, fmt.Errorf("unknown processor type : %+v", ctx.Tp)
		}
	}
	return nil, nil
}
